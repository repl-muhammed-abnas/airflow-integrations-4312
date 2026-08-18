
from datetime import timedelta
from pendulum import datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'intercontinentalexchange_scheduleddisable_user_{config.instance}',
        description=f'IntercontinentalExchange_scheduled disable_user {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 5, 1, tz=config.pacific_timezone),
        schedule_interval=config.schedule_interval_daily,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_report_details_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_report_details_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_report_details_3 = rail.RepliconReportDetailsOperator(
            task_id='get_report_details_3',
            report_name=config.user_report_to_disable,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details_3').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='if_generate_report_34_payload_starts_with_nodata_6',
            no_task='log_to_sumo'
        )

        if_generate_report_34_payload_starts_with_nodata_6 = rail.IfOperator(
            task_id='if_generate_report_34_payload_starts_with_nodata_6',
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('No Data') }}",
            yes_task="stop_5",
            no_task="if_generate_report_34_payload_not_starts_with_column_6",
        )

        stop_5 = rail.FailOperator(
            task_id='stop_5',
            message='''No Data in the base report'''
        )

        if_generate_report_34_payload_not_starts_with_column_6 = rail.IfOperator(
            task_id='if_generate_report_34_payload_not_starts_with_column_6',
            # pylint: disable=line-too-long
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('User Name,Login Name,Employee ID,UserUri,User End Date,daydiff')}}",
            yes_task="load_report_data_7",
            no_task="stop_13",
        )

        load_report_data_7 = rail.LoadCSVFileOperator(
            task_id='load_report_data_7',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_user_collection_7 = rail.CreateCollectionOperator(
            task_id='create_user_collection_7',
            name='enabled_users',
            source="{{ result('load_report_data_7') }}",
            columns={
                'User Name': 'username',
                'Login Name': 'loginname',
                'Employee ID': 'employeeid',
                'UserUri': 'useruri',
                'User End Date': 'enddate',
                'daydiff': 'daydiff'}
        )

        query_list_8 = rail.QueryCollectionOperator(
            task_id='query_list_8',
            query="""SELECT * FROM  enabled_users WHERE enabled_users.daydiff != '' and enabled_users.daydiff <= 0""",
        )

        if_query_list_8_rows_greater_than_0_9 = rail.IfOperator(
            task_id='if_query_list_8_rows_greater_than_0_9',
            test='''{{ result("query_list_8", "length") > 0 }}''',
            yes_task="foreach_query_list_8_10",
            no_task="finish",
        )

        foreach_query_list_8_10 = rail.ForEachOperator(
            task_id='foreach_query_list_8_10',
            items="{{ result('query_list_8') }}",
            start_task='disable_login_11',
            end_task='foreach_query_list_8_10_end'
        )

        disable_login_11 = rail.RepliconServiceOperator(
            task_id='disable_login_11',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_query_list_8_10').useruri }}"
            }
        )

        foreach_query_list_8_10_end = rail.EmptyOperator(
            task_id='foreach_query_list_8_10_end',
        )

        stop_13 = rail.FailOperator(
            task_id='stop_13',
            message='''Base report column order doesn't match'''
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> get_report_details_3 >> run_report_group_entry
        run_report_group_exit >> report_has_data
        report_has_data >> rail.Label('No') >> log_to_sumo
        report_has_data >> rail.Label(
            'Yes') >> if_generate_report_34_payload_starts_with_nodata_6
        if_generate_report_34_payload_starts_with_nodata_6 >> rail.Label(
            'Yes') >> stop_5 >> log_to_sumo
        if_generate_report_34_payload_starts_with_nodata_6 >> rail.Label(
            'No') >> if_generate_report_34_payload_not_starts_with_column_6
        if_generate_report_34_payload_not_starts_with_column_6 >> rail.Label(
            'No') >> stop_13 >> log_to_sumo
        if_generate_report_34_payload_not_starts_with_column_6 >> rail.Label('Yes') >> load_report_data_7 >> \
            create_user_collection_7 >> query_list_8 >> if_query_list_8_rows_greater_than_0_9
        if_query_list_8_rows_greater_than_0_9 >> rail.Label(
            'Yes') >> foreach_query_list_8_10 >> disable_login_11 >> foreach_query_list_8_10_end
        foreach_query_list_8_10 >> foreach_query_list_8_10_end >> finish
        if_query_list_8_rows_greater_than_0_9 >> rail.Label(
            'No') >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
