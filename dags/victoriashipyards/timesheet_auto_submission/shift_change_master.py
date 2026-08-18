from datetime import timedelta
import rail
from pendulum import datetime
from rail.lib.ecid import get_dagrun_ecid
from victoriashipyards.timesheet_auto_submission.utils import request_payload
from victoriashipyards.timesheet_auto_submission.utils import python_callable
from victoriashipyards.timesheet_auto_submission.utils import custom_methods

# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"victoriashipyards_timesheet_auto_submission_shift_change_master_{config.instance}",
        description=f"victoriashipyards Timesheet Auto Submission Shift Change Master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 12, 7, tz=config.time_zone),
        schedule_interval=config.schedule_interval_shift_change,
        max_active_runs=config.max_active_runs
    ) as dag:

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=custom_methods.logging_details,
            op_args=[config]
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.timesheet_report,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            wait_timeout=config.run_report_wait_timeout,
            retries=0,
            report_params=request_payload.get_report_generate_batch_payload,
            replicon_conn_id=config.replicon_conn_id,
        )

        payload_has_data = rail.IfOperator(
            task_id='payload_has_data',
            test="{{ result('run_report.get_report_result','has_data') }}",
            yes_task='error_not_exists',
            no_task='finish'
        )

        error_not_exists = rail.IfOperator(
            task_id='error_not_exists',
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].error | is_falsy }}",
            yes_task='load_csv_data',
            no_task='fail_error_report'
        )

        fail_error_report = rail.FailOperator(
            task_id="fail_error_report",
            message='{{ result("run_report.get_report_result").reportGenerationResults[0].error }}',
        )

        load_csv_data = rail.LoadCSVFileOperator(
            task_id='load_csv_data',
            document='{{ result("run_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        timesheet_data = rail.CreateCollectionOperator(
            task_id='timesheet_data',
            source="{{ result('load_csv_data') }}",
            columns={
                'Timesheet Period': 'timesheetperiod',
                'Login Name': 'username',
                'Validation Message': 'validationmessages',
                'Approval Status': 'approvalstatus',
                'Timesheet URI': 'timesheeturi',
                'Timesheet Start Date': 'timesheetstartdate',
                'Timesheet End Date': 'timesheetenddate',
            },
            name='timesheet_report'
        )

        not_sumbitted_no_error_timesheets = rail.QueryCollectionOperator(
            task_id='not_sumbitted_no_error_timesheets',
            query="SELECT * FROM timesheet_report WHERE validationmessages='Null' AND approvalstatus='Not Submitted' \
                ORDER BY strftime('%Y-%m-%d', timesheetstartdate) ASC",
            name='not_sumbitted_no_error_timesheets'
        )

        validated_timesheets_size = rail.IfOperator(
            task_id='validated_timesheets_size',
            test='{{ result("not_sumbitted_no_error_timesheets", "length") > 0 }}',
            yes_task='get_reopen_timesheet_details',
            no_task='finish'
        )

        get_reopen_timesheet_details = rail.RepliconReportDetailsOperator(
            task_id='get_reopen_timesheet_details',
            report_name=config.timesheet_report_to_reopen,
        )

        run_reopen_report_entry, run_reopen_report_exit = rail.run_report(
            group_id='run_reopen_timesheet_report',
            wait_timeout=config.run_report_wait_timeout,
            retries=0,
            report_params=request_payload.get_timesheet_audit_reopen_timesheets_payload,
            replicon_conn_id=config.replicon_conn_id,
        )

        error_present = rail.IfOperator(
            task_id='error_present',
            test="{{ result('run_reopen_timesheet_report.get_report_result').reportGenerationResults[0].error | is_falsy }}",
            yes_task='load_audit_report_csv',
            no_task='fail_response_has_error'
        )

        fail_response_has_error = rail.FailOperator(
            task_id="fail_response_has_error",
            message="{{ result('run_reopen_timesheet_report.get_report_result').reportGenerationResults[0].error }}",
        )

        load_audit_report_csv = rail.LoadCSVFileOperator(
            task_id='load_audit_report_csv',
            document="{{ result('run_reopen_timesheet_report.get_report_result').reportGenerationResults[0].payload }}"
        )

        timesheet_reopened_audit_report = rail.CreateCollectionOperator(
            task_id='timesheet_reopened_audit_report',
            source="{{ result('load_audit_report_csv') }}",
            columns={
                "Timesheet Period": "timesheetperiod",
                "User Name": "username",
                "Action": "action",
                "Modification Summary": "modificationsummary",
                "timesheeturi": "timesheeturi"
            },
            name='timesheets_reopened'
        )

        timesheets_to_be_processed = rail.QueryCollectionOperator(
            task_id='timesheets_to_be_processed',
            query='''SELECT * FROM timesheets_reopened WHERE timesheeturi IN (SELECT DISTINCT timesheeturi FROM not_sumbitted_no_error_timesheets)
                    AND modificationsummary LIKE \'"Time off hours were recalculated due to a change in the %\''''
        )

        automatic_timesheet_submission = rail.TriggerDagRunForEachItemOperator(
            task_id='automatic_timesheet_submission',
            retries=0,
            items=lambda: rail.result('timesheets_to_be_processed'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'victoriashipyards_automatic_timesheets_submission_child_dag_{config.instance}',
            conf=lambda item: {
                "username": item['username'],
                "timesheeturi": item['timesheeturi'],
                "timesheetperiod": item['timesheetperiod'],
                "type": 'Shift Update',
                "jobid": get_dagrun_ecid(rail.get_current_context()['dag_run']),
            }
        )

        wait_for_automatic_timesheet_submission = rail.WaitForDagRunsSensor(
            task_id='wait_for_automatic_timesheet_submission',
            dag_runs='{{ result("automatic_timesheet_submission") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=3,
        )

        check_processed_list_size = rail.IfOperator(
            task_id='check_processed_list_size',
            test='{{ result("timesheets_to_be_processed", "length") > 0 }}',
            yes_task='gather_log',
            no_task='finish'
        )

        gather_log = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_log',
            dag_runs='{{ result("automatic_timesheet_submission") }}',
            dagrun_task_id='create_shift_change_log',
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable.load_child_logs
        )

        is_log_exists = rail.IfOperator(
            task_id='is_log_exists',
            test=python_callable.check_logs_size,
            yes_task='compose_format_logs',
            no_task='finish'
        )

        compose_format_logs = rail.CreateCollectionOperator(
            task_id='compose_format_logs',
            source="{{ result('format_logs') | to_json }}",
            name='output_logs'
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('compose_format_logs'),
            header=[
                'User Name',
                'Timesheet Period',
                'Date Time',
                'Status',
                'Remarks',
                'Jobid'],
            row=[
                '{{ item.username }}',
                '{{ item.timesheetperiod }}',
                '{{ item.datetime }}',
                '{{ item.status }}',
                '{{ item.remarks }}',
                '{{ item.jobid }}|{{ item.childjobid }}'],
            lineterminator='\n'
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("render_logs_csv")}}',
            output_file_name='{{ result("get_logging_details").log_filename}}',
            expires_in_seconds=7 * 24 * 60 * 60,
        )

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Error', rail.result('format_logs')))), 'length')
        )

        if_errored_log_present = rail.IfOperator(
            task_id='if_errored_log_present',
            test='{{ result("get_errored_logs", "length") > 0 }}',
            yes_task='send_import_complete_email',
            no_task='finish'
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.alert_email,
            subject='{{ get_company_key() }} + " | Automatic Timesheet Submission completed with errors - \
                {{ result("get_logging_details").dag_run_start_time }}',
            html_content='templates/emails/import_complete.html',
            params={
                'time_zone': config.time_zone
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_logging_details >> get_report_details >> run_report_group_entry
        run_report_group_exit >> payload_has_data

        payload_has_data >> rail.Label("Yes") >> error_not_exists
        payload_has_data >> rail.Label("No") >> finish

        error_not_exists >> rail.Label("Yes") >> load_csv_data
        error_not_exists >> rail.Label("No") >> fail_error_report

        load_csv_data >> timesheet_data >> not_sumbitted_no_error_timesheets >> validated_timesheets_size

        validated_timesheets_size >> rail.Label(
            "Yes") >> get_reopen_timesheet_details
        validated_timesheets_size >> rail.Label("No") >> finish

        get_reopen_timesheet_details >> run_reopen_report_entry
        run_reopen_report_exit >> error_present

        error_present >> rail.Label("Yes") >> load_audit_report_csv >> timesheet_reopened_audit_report \
            >> timesheets_to_be_processed >> automatic_timesheet_submission >> wait_for_automatic_timesheet_submission \
            >> check_processed_list_size
        error_present >> rail.Label("No") >> fail_response_has_error

        check_processed_list_size >> rail.Label("Yes") >> gather_log >> format_logs >> is_log_exists
        is_log_exists >> rail.Label("Yes") >> compose_format_logs >> render_logs_csv >> generate_download_link >> get_errored_logs \
            >> if_errored_log_present
        if_errored_log_present >> rail.Label(
            "Yes") >> send_import_complete_email
        is_log_exists >> rail.Label("No") >> finish
        if_errored_log_present >> rail.Label("No") >> finish
        check_processed_list_size >> rail.Label("No") >> finish

    return dag


rail.for_each_instance(create_dag)
