from datetime import timedelta
from airflow.models import Variable
from pendulum import datetime as dt
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'npsg_disable_users_master_{config.instance}',
        description=f'NPSG - Disable Users Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=dt(2023, 1, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_enabled_users_report_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_enabled_users_report_details',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_enabled_users_report_details = rail.RepliconReportDetailsOperator(
            task_id = 'get_enabled_users_report_details',
            report_name=config.enabled_users_report
        )

        run_enabled_users_report = rail.run_report2(
            group_id='run_enabled_users_report',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_enabled_users_report_details')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                    }
                ]
            }
        )

        if_payload_in_report_not_present = rail.IfOperator(
            task_id = 'if_payload_in_report_not_present',
            test="{{result('run_enabled_users_report.get_report_result','has_data') | is_falsy}}",
            yes_task='log_to_sumo',
            no_task='if_payload_has_correct_columns'
        )

        if_payload_has_correct_columns = rail.IfOperator(
            task_id = 'if_payload_has_correct_columns',
            #pylint: disable = line-too-long
            test="{{result('run_enabled_users_report.get_report_result').reportGenerationResults[0].payload | starts_with('User Name,Login Name,Employee ID,UserUri,User End Date,daydiff')}}",
            yes_task='load_csv_report_result',
            no_task='stop_job_with_error'
        )

        load_csv_report_result = rail.LoadCSVFileOperator(
            task_id = 'load_csv_report_result',
            document="{{result('run_enabled_users_report.get_report_result').reportGenerationResults[0].payload}}",
            delimiter=','
        )

        create_collection_enabled_users = rail.CreateCollectionOperator(
            task_id = 'create_collection_enabled_users',
            source="{{result('load_csv_report_result')}}",
            name='enabledusers',
            columns={
                'User Name':'username',
                'Login Name':'loginname',
                'Employee ID':'employeeid',
                'UserUri':'useruri',
                'User End Date':'enddate',
                'daydiff':'daydiff'
            }
        )

        query_users_to_be_disabled = rail.QueryCollectionOperator(
            task_id = 'query_users_to_be_disabled',
            query="""SELECT * FROM enabledusers WHERE CAST(enabledusers.daydiff as FLOAT) < -7 AND NULLIF(daydiff,'') IS NOT NULL """
        )

        if_users_to_be_disabled_present = rail.IfOperator(
            task_id = 'if_users_to_be_disabled_present',
            test="{{result('query_users_to_be_disabled','length') > 0}}",
            yes_task='foreach_user_to_be_disabled',
            no_task='log_to_sumo'
        )

        foreach_user_to_be_disabled = rail.ForEachOperator(
            task_id = 'foreach_user_to_be_disabled',
            items="{{result('query_users_to_be_disabled')}}",
            start_task='disable_user',
            end_task='foreach_user_to_be_disabled_end'
        )

        disable_user = rail.RepliconServiceOperator(
            task_id = 'disable_user',
            endpoint='/services/securityService1.svc/DisableLogin',
            data={
                "userUri": "{{result('foreach_user_to_be_disabled').useruri}}"
            }
        )

        foreach_user_to_be_disabled_end = rail.EmptyOperator(
            task_id = 'foreach_user_to_be_disabled_end'
        )

        stop_job_with_error = rail.FailOperator(
            task_id = 'stop_job_with_error',
            message="Base report column order doesn't match"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_enabled_users_report_details
        get_enabled_users_report_details >> run_enabled_users_report >> if_payload_in_report_not_present
        if_payload_in_report_not_present >> rail.Label('Yes') >> log_to_sumo
        if_payload_in_report_not_present >> rail.Label('No') >> if_payload_has_correct_columns
        if_payload_has_correct_columns >> rail.Label('Yes') >> load_csv_report_result >> create_collection_enabled_users >> query_users_to_be_disabled
        query_users_to_be_disabled >> if_users_to_be_disabled_present
        if_users_to_be_disabled_present >> rail.Label('Yes') >> foreach_user_to_be_disabled >> disable_user >> foreach_user_to_be_disabled_end
        foreach_user_to_be_disabled >> foreach_user_to_be_disabled_end >> log_to_sumo
        if_users_to_be_disabled_present >> rail.Label('No') >> log_to_sumo
        if_payload_has_correct_columns >> rail.Label('No') >> stop_job_with_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
