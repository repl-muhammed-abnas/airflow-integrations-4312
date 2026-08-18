from datetime import timedelta
import rail
from pendulum import datetime
from rail.lib.ecid import get_dagrun_ecid
from victoriashipyards.timesheet_auto_submission_v1.utils import custom_methods
from victoriashipyards.timesheet_auto_submission_v1.utils import request_payload
from victoriashipyards.timesheet_auto_submission_v1.utils import python_callable
from victoriashipyards.timesheet_auto_submission_v1.tasks import recalculated_timesheets_report_batch
from victoriashipyards.timesheet_auto_submission_v1.tasks import timesheets_report_batch

# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.time_punch_master_dagid,
        description=f"victoriashipyards Timesheet Auto Submission Time Punch Master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 12, 7, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs
    ) as dag:

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=custom_methods.logging_details,
            op_args=[config]
        )

        load_timesheets_report, timesheet_data = timesheets_report_batch.report_batch(
            config)

        check_timesheetperiod_exists = rail.IfOperator(
            task_id='check_timesheetperiod_exists',
            test=custom_methods.check_timesheetperiod,
            yes_task='query_validated_timesheets',
            no_task='finish'
        )

        query_validated_timesheets = rail.QueryCollectionOperator(
            task_id='query_validated_timesheets',
            query="SELECT * FROM timesheet_data WHERE validationcheck='Yes'"
        )

        check_validated_timesheets_size = rail.IfOperator(
            task_id='check_validated_timesheets_size',
            test='{{ result("query_validated_timesheets", "length") > 0 }}',
            yes_task='recalculate_timesheet',
            no_task='generate_report'
        )

        recalculate_timesheet = rail.TriggerDagRunForEachItemOperator(
            task_id='recalculate_timesheet',
            retries=0,
            items=lambda: rail.result('query_validated_timesheets'),
            batch_size=50,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.recalculate_timesheet_child_dagid,
        )

        wait_for_recalculate_timesheet = rail.WaitForDagRunsSensor(
            task_id='wait_for_recalculate_timesheet',
            dag_runs='{{ result("recalculate_timesheet") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        generate_report = rail.PythonOperator(
            task_id='generate_report',
            python_callable=custom_methods.empty_method
        )

        load_recalculated_timesheet_report, recalculated_timesheet_data = recalculated_timesheets_report_batch.report_batch(
            config)

        load_timesheet_data_to_csv = rail.WriteCSVFileOperator(
            task_id='load_timesheet_data_to_csv',
            source="{{result('recalculated_timesheet_data')}}",
            row=lambda item: request_payload.get_row_data(item, config)
        )

        validated_timesheet_data = rail.CreateCollectionOperator(
            task_id='validated_timesheet_data',
            source="{{ result('load_timesheet_data_to_csv') }}",
            name='validated_timesheets'
        )

        not_sumbitted_no_error_timesheets = rail.QueryCollectionOperator(
            task_id='not_sumbitted_no_error_timesheets',
            query="SELECT * FROM validated_timesheets WHERE validationmessages='Null' AND approvalstatus='Not Submitted' \
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
            AND modificationsummary=\'"Timesheet automatically reopened to include punches that were added or edited by BSSV User."\''''
        )

        automatic_timesheet_submission = rail.TriggerDagRunForEachItemOperator(
            task_id='automatic_timesheet_submission',
            retries=0,
            items=lambda: rail.result('timesheets_to_be_processed'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.automatic_timesheet_submission_child_dagid,
            conf=lambda item: {
                "username": item['username'],
                "timesheeturi": item['timesheeturi'],
                "timesheetperiod": item['timesheetperiod'],
                "type": 'Time Punch',
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
            dagrun_task_id='create_time_punch_log',
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

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Automatic Timesheet Submission " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " - " + result("get_logging_details").dag_run_start_time }}',
            html_content='templates/emails/import_complete.html',
            params={
                'time_zone': config.time_zone
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_logging_details >> load_timesheets_report, timesheet_data >> check_timesheetperiod_exists
        check_timesheetperiod_exists >> rail.Label(
            'Yes') >> query_validated_timesheets >> check_validated_timesheets_size
        check_validated_timesheets_size >> rail.Label(
            'Yes') >> recalculate_timesheet >> wait_for_recalculate_timesheet >> load_recalculated_timesheet_report, recalculated_timesheet_data
        check_validated_timesheets_size >> rail.Label(
            'No') >> generate_report >> load_recalculated_timesheet_report, recalculated_timesheet_data
        recalculated_timesheet_data >> load_timesheet_data_to_csv >> validated_timesheet_data\
            >> not_sumbitted_no_error_timesheets >> validated_timesheets_size
        validated_timesheets_size >> rail.Label(
            'Yes') >> get_reopen_timesheet_details >> run_reopen_report_entry
        run_reopen_report_exit >> error_present
        error_present >> rail.Label('Yes') >> load_audit_report_csv >> timesheet_reopened_audit_report >> timesheets_to_be_processed \
            >> automatic_timesheet_submission >> wait_for_automatic_timesheet_submission >> check_processed_list_size
        check_processed_list_size >> rail.Label("Yes") >> gather_log >> format_logs >> is_log_exists
        is_log_exists >> rail.Label("Yes") >> compose_format_logs >> render_logs_csv >> generate_download_link \
            >> get_errored_logs >> send_import_complete_email >> finish
        is_log_exists >> rail.Label("No") >> finish
        check_processed_list_size >> rail.Label("No") >> finish
        error_present >> rail.Label('No') >> fail_response_has_error
        validated_timesheets_size >> rail.Label('No') >> finish
        check_timesheetperiod_exists >> rail.Label('No') >> finish

    return dag


rail.for_each_instance(create_dag)
