from datetime import timedelta
from pendulum import datetime
import pendulum
from capgemini.optional_holidays_auto_population_india_new_users_v1.tasks.send_logs import get_send_logs
from capgemini.optional_holidays_auto_population_india_new_users_v1.utils import custom_methods, request_payload
from airflow.models import Variable
import rail

null = None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'Capgemini Auto Population of Optional Holidays India for New Users Master v1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 6, 1, tz=config.time_zone),
        schedule_interval=config.new_users_schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            'retries': 0
        }
    ) as dag:

        is_existing_users_setup_scheduled_today = rail.IfOperator(
            task_id='is_existing_users_setup_scheduled_today',
            test=lambda: pendulum.now(config.time_zone).strftime("%m/%d") == config.e1_schedule or
                pendulum.now(config.time_zone).strftime("%m/%d") == config.e2_schedule,
            yes_task='dagrun_log_to_sumo',
            no_task='process_tenant_wide_logs'
        )

        process_tenant_wide_logs = rail.EmptyOperator(
            task_id='process_tenant_wide_logs'
        )

        declare_log_artifacts_var = rail.SetVariableOperator(
            task_id='declare_log_artifacts_var',
            name='new_users_log_artifacts',
            value=[]
        )

        for_each_tenant_wide_log = rail.ForEachOperator(
            task_id='for_each_tenant_wide_log',
            items=config.tenant_wide_log_list,
            start_task='get_new_users_artifact',
            end_task='for_each_end'
        )

        get_new_users_artifact = rail.FilterLogEntriesOperator(
            task_id='get_new_users_artifact',
            log="{{ result('for_each_tenant_wide_log') }}",
            remove_filtered_entries=True
        )

        log_new_users_artifact = rail.SetVariableOperator(
            task_id='log_new_users_artifact',
            name='new_users_log_artifacts',
            value='{{ result("get_new_users_artifact") }}',
            append=True
        )

        for_each_end = rail.EmptyOperator(
            task_id='for_each_end'
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config.time_zone, config.states_optional_holiday_calendars, "new_user"]
        )

        get_new_users_artifacts = rail.GetVariableOperator(
            task_id='get_new_users_artifacts',
            name='new_users_log_artifacts'
        )

        merge_all_artifacts = rail.CreateCollectionOperator(
            task_id='merge_all_artifacts',
            source=custom_methods.get_new_users_artifact_data,
            columns=['user_login_name', 'user_name', 'user_uri'],
            name='final_tenant_wide_log_data'
        )

        has_any_entries = rail.IfOperator(
            task_id='has_any_entries',
            test='{{ result("merge_all_artifacts", "length") > 0 }}',
            yes_task='get_user_report_details',
            no_task='send_no_new_users_email',
        )

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name=config.user_details_report
        )

        run_user_details_report = rail.run_report2(
            group_id='run_user_details_report',
            report_params=request_payload.user_details_report_payload,
            target='artifact'
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test="{{ (result('run_user_details_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy}}",
            yes_task='fail_report_generation',
            no_task='report_has_data'
        )

        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message="{{ (result('run_user_details_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{result('run_user_details_report.get_report_result','has_data')}}",
            yes_task='is_report_has_expected_columns',
            no_task='dagrun_log_to_sumo'
        )

        is_report_has_expected_columns = rail.IfOperator(
            task_id='is_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ (result('run_user_details_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_user_details_report_columns,
            yes_task='load_csv',
            no_task='fail_no_expected_columns',
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id='load_csv',
            document="{{ (result('run_user_details_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}"
        )

        user_details_report_collection = rail.CreateCollectionOperator(
            task_id='user_details_report_collection',
            source='{{ result("load_csv") }}',
            columns={
                "Employee ID": "employeeid",
                "UserUri": "useruri",
                "User Start Date": "startdate",
                "Location (Current) (Full Path)": "locationfullpath"
            },
            name='user_details'
        )

        query_specific_users = rail.QueryCollectionOperator(
            task_id='query_specific_users',
            query="""SELECT ftwld.user_uri, ftwld.user_name
                FROM final_tenant_wide_log_data ftwld
                WHERE ftwld.user_uri IN (
                    SELECT ud.useruri
                    FROM user_details ud
                    WHERE ud.locationfullpath LIKE 'India%'
                    AND ud.startdate NOT LIKE :jandate
                    AND ud.startdate NOT LIKE :juldate
                )""",
            query_params={
                "jandate": "Jan%" + str(pendulum.now().year),
                "juldate": "Jul%" + str(pendulum.now().year)
            }
        )

        has_users_data = rail.IfOperator(
            task_id='has_users_data',
            test='{{ result("query_specific_users", "length") > 0 }}',
            yes_task='create_log',
            no_task='send_no_new_users_email'
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        process_new_users = rail.trigger_parallel_dagrun(
            task_id='process_new_users',
            items='{{ result("query_specific_users") }}',
            parallel_count=config.parallel_dags_count,
            trigger_dag_id=config.process_new_users_dagid,
            conf=lambda item: {
                "user_uri": item["user_uri"],
                "user_name": item["user_name"],
                "log_artifact": rail.result("create_log"),
                "states_optional_holiday_calendars": rail.result("logging_details")["states_optional_holiday_calendars"]
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        send_no_new_users_email = rail.EmailOperator(
            task_id='send_no_new_users_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | The Auto population of Optional holiday booking for New Users is '
                + 'skipped at {{ result("logging_details").process_start_time }}',
            html_content='/templates/emails/no_new_users.html'
        )

        process_logs = rail.EmptyOperator(
            task_id='process_logs'
        )

        send_logs_enter, send_logs_exit = get_send_logs(config)

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        is_existing_users_setup_scheduled_today >> rail.Label("Yes") >> dagrun_log_to_sumo
        is_existing_users_setup_scheduled_today >> rail.Label("No") >> process_tenant_wide_logs \
            >> declare_log_artifacts_var >> for_each_tenant_wide_log >> get_new_users_artifact \
                >> log_new_users_artifact >> for_each_end
        for_each_tenant_wide_log >> for_each_end >> logging_details >> get_new_users_artifacts \
            >> merge_all_artifacts >> has_any_entries
        has_any_entries >> rail.Label("Yes") >> get_user_report_details >> run_user_details_report >> is_report_failed
        is_report_failed >> rail.Label("Yes") >> fail_report_generation >> dagrun_log_to_sumo
        is_report_failed >> rail.Label("No") >> report_has_data
        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns
        report_has_data >> rail.Label("No") >> dagrun_log_to_sumo
        is_report_has_expected_columns >> rail.Label("Yes") >> load_csv >> user_details_report_collection \
            >> query_specific_users >> has_users_data
        has_users_data >> rail.Label("Yes") >> create_log >> process_new_users \
                >> process_logs >> send_logs_enter
        has_users_data >> rail.Label("No") >> send_no_new_users_email
        is_report_has_expected_columns >> rail.Label("No") >> fail_no_expected_columns >> dagrun_log_to_sumo
        send_logs_exit >> dagrun_log_to_sumo
        has_any_entries>> rail.Label("No") >> send_no_new_users_email >> dagrun_log_to_sumo
        dagrun_log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)
