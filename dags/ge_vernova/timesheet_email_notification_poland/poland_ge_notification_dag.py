from datetime import timedelta
from ge.timesheet_email_notification_poland.utils import custom_methods
from ge.timesheet_email_notification_poland.utils import request_payload
from rail.lib.ecid import get_dagrun_ecid
import rail
import pendulum
from airflow.models import Variable

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_timesheet_email_notification_master_{config.instance}_v1',
        description=f'GE(EY) Poland_GE Timesheet email notification Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
    ) as dag:

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=custom_methods.logging_details
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id='run_report',
            report_params=request_payload.get_report_generate_payload,
            replicon_conn_id=config.replicon_conn_id
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='is_report_has_expected_columns',
            no_task='finish'
        )

        is_report_has_expected_columns = rail.IfOperator(
            task_id='is_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_report_columns,
            yes_task="load_timehseets_csv",
            no_task="fail_no_expected_columns",
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_timehseets_csv = rail.LoadCSVFileOperator(
            task_id='load_timehseets_csv',
            document='{{ result("run_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        timesheets_data = rail.CreateCollectionOperator(
            task_id='timesheets_data',
            source='{{ result("load_timehseets_csv") }}',
            columns={
                "Timesheet URI": "timesheet_uri",
                "Timesheet Period": "date_range_value",
                "User Name": "username",
                "User URI": "user_uri",
                "Approval Status": "approval_status",
                "User Supervisor Name (Current)": "supervisor_name",
                "User Supervisor Email address": "supervisor_email",
                "Supervisor URI": "supervisor_uri",
                "Waiting on Approver": "waiting_on_approver",
                "Location (Current)": "location"
            },
            name='timesheetsdata'
        )

        query_unique_supervisors = rail.QueryCollectionOperator(
            task_id='query_unique_supervisors',
            query="SELECT DISTINCT supervisor_name FROM timesheetsdata",
            name='unique_supervisors'
        )

        load_timesheets_data = rail.PythonOperator(
            task_id='load_timesheets_data',
            python_callable=lambda: rail.load_all_records(rail.result("timesheets_data"))
        )

        supervisors_formatted_timesheets_data = rail.DataAdaptorOperator(
            task_id='supervisors_formatted_timesheets_data',
            source='{{ result("query_unique_supervisors") }}',
            columns=["supervisor_name", "date_range_value", "supervisor_email", "users", "locations"],
            data=custom_methods.get_supervisors_timesheet_data
        )

        formatted_timesheets_data_collection = rail.CreateCollectionOperator(
            task_id='formatted_timesheets_data_collection',
            source='{{ result("supervisors_formatted_timesheets_data") }}',
            name='formatted_timesheets_data'
        )

        query_supervisor_email_exist_records = rail.QueryCollectionOperator(
            task_id='query_supervisor_email_exist_records',
            query="SELECT * FROM formatted_timesheets_data WHERE NULLIF(supervisor_email, '') IS NOT NULL AND supervisor_email LIKE '%@%'"
        )

        is_supervisor_email_records_exist = rail.IfOperator(
            task_id='is_supervisor_email_records_exist',
            test='{{ result("query_supervisor_email_exist_records", "length") > 0 }}',
            yes_task='trigger_send_email_notifications',
            no_task='send_logs'
        )

        trigger_send_email_notifications = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_send_email_notifications',
            items='{{ result("query_supervisor_email_exist_records") }}',
            trigger_dag_id=f'ge_timesheet_email_notification_poland_send_email_notification_child_{config.instance}_v1',
            conf=lambda dag_run, item: {
                "supervisor_data": item,
                "parent_dag_run_ecid": get_dagrun_ecid(dag_run)
            }
        )

        wait_for_send_email_notifications = rail.WaitForDagRunsSensor(
            task_id='wait_for_send_email_notifications',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_send_email_notifications") }}'
        )

        query_supervisor_email_not_exist_records = rail.QueryCollectionOperator(
            task_id='query_supervisor_email_not_exist_records',
            query="SELECT * FROM formatted_timesheets_data WHERE NULLIF(supervisor_email, '') IS NULL OR supervisor_email NOT LIKE '%@%'"
        )

        is_supervisor_email_no_records_exist = rail.IfOperator(
            task_id='is_supervisor_email_no_records_exist',
            test='{{ result("query_supervisor_email_not_exist_records", "length") > 0 }}',
            yes_task='log_no_supervisor_email',
            no_task='send_logs'
        )

        log_no_supervisor_email = rail.WriteLogOperator(
            task_id='log_no_supervisor_email',
            items='{{ result("query_supervisor_email_not_exist_records") }}',
            message='Supervisor Email not Available',
            severity='Exception',
            properties=lambda item: {
                "Parentjobid": '{{ dag_run_ecid() }}',
                "username": item["supervisor_name"],
                "emailid": item["supervisor_email"],
                "status": "Not Processed",
                "reason": "Supervisor Email not Available",
                "childjobid": "",
                "date": pendulum.now().strftime("%m/%d/%Y")
            }
        )

        send_logs = rail.EmptyOperator(
            task_id='send_logs'
        )

        has_any_entries_in_log = rail.IfOperator(
            task_id='has_any_entries_in_log',
            test='{{ get_master_log() | load_all_records() | length > 0 }}',
            yes_task='render_logs_csv',
            no_task='finish',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source='{{ get_master_log() }}',
            header=["Parentjobid", "username", "emailid", "status", "reason", "childjobid", "date"],
            row=[
                '{{item.properties.Parentjobid}}',
                '{{item.properties.username}}',
                '{{item.properties.emailid}}',
                '{{item.properties.status}}',
                '{{item.properties.reason}}',
                '{{item.properties.childjobid}}',
                '{{item.properties.date}}'
            ]
        )

        upload_file_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_file_to_s3',
            source='{{ result("render_logs_csv") }}',
            key_name=config.s3_reference_key_name + '/EY_Poland_Notification_Logs_{{ result("get_logging_details").dag_run_start_time }}.csv',
            bucket_name=lambda: Variable.get(
                config.s3_bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_logging_details >> get_report_details >> run_report_entry
        run_report_exit >> is_report_failed

        is_report_failed >> rail.Label("No") >> report_has_data
        is_report_failed >> rail.Label("Yes") >> fail_report_generation

        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns
        report_has_data >> rail.Label("No") >> finish
        is_report_has_expected_columns >> rail.Label(
            'Yes') >> load_timehseets_csv >> timesheets_data >> query_unique_supervisors \
                >> load_timesheets_data >> supervisors_formatted_timesheets_data >> formatted_timesheets_data_collection \
                >> query_supervisor_email_exist_records >> is_supervisor_email_records_exist
        formatted_timesheets_data_collection >> query_supervisor_email_not_exist_records >> is_supervisor_email_no_records_exist
        is_report_has_expected_columns >> rail.Label('No') >> fail_no_expected_columns

        is_supervisor_email_records_exist >> rail.Label("Yes") >> trigger_send_email_notifications >> wait_for_send_email_notifications >> send_logs
        is_supervisor_email_records_exist >> rail.Label("No") >> send_logs

        is_supervisor_email_no_records_exist >> rail.Label("Yes") >> log_no_supervisor_email >> send_logs
        is_supervisor_email_no_records_exist >> rail.Label("No") >> send_logs

        send_logs >> has_any_entries_in_log
        has_any_entries_in_log >> rail.Label("Yes") >> render_logs_csv >> upload_file_to_s3
        has_any_entries_in_log >> rail.Label("No") >> finish

    return dag

rail.for_each_instance(create_dag)
