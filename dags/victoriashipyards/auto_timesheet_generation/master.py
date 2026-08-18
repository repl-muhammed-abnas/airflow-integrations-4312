from datetime import timedelta
import rail
from pendulum import datetime
from rail.lib.ecid import get_dagrun_ecid
from victoriashipyards.auto_timesheet_generation.utils import custom_methods
from victoriashipyards.auto_timesheet_generation.utils import request_payload

# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"victoriashipyards_auto_timesheet_generation_main_dag_{config.instance}",
        description=f"victoriashipyards Auto Timesheet Generation {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 10, 10, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs
    ) as dag:

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=custom_methods.get_log_params,
            op_args=[config.time_zone]
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.timesheet_generation_report,
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id='run_timesheet_generation_report',
            report_params=request_payload.get_report_generate_batch_payload,
            replicon_conn_id=config.replicon_conn_id,
        )

        no_error_exists = rail.IfOperator(
            task_id='no_error_exists',
            test="{{ result('run_timesheet_generation_report.get_report_result').reportGenerationResults[0].error | is_falsy }}",
            yes_task='payload_has_data',
            no_task='fail_error_report'
        )

        payload_has_data = rail.IfOperator(
            task_id='payload_has_data',
            test="{{ result('run_timesheet_generation_report.get_report_result','has_data') }}",
            yes_task='load_csv_data',
            no_task='finish'
        )

        fail_error_report = rail.FailOperator(
            task_id="fail_error_report",
            message='{{ result("run_timesheet_generation_report.get_report_result").reportGenerationResults[0].error }}',
        )

        load_csv_data = rail.LoadCSVFileOperator(
            task_id='load_csv_data',
            document='{{ result("run_timesheet_generation_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        timesheet_report_data = rail.CreateCollectionOperator(
            task_id='timesheet_report_data',
            source="{{ result('load_csv_data') }}",
            columns={
                'Login Name': 'loginname',
                'Uri': 'useruri',
                'Allowed': 'allowed'
            },
            name='timesheet_report_data'
        )

        get_allowed_users = rail.QueryCollectionOperator(
            task_id='get_allowed_users',
            query="SELECT loginname, useruri FROM timesheet_report_data WHERE allowed='Yes'"
        )

        is_alloweduser_present = rail.IfOperator(
            task_id='is_alloweduser_present',
            test='{{result("get_allowed_users", "length") > 0}}',
            yes_task='generate_timesheets',
            no_task='finish'
        )

        generate_timesheets = rail.TriggerDagRunForEachItemOperator(
            task_id='generate_timesheets',
            retries=0,
            batch_size=15,
            items=lambda: rail.result('get_allowed_users'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'victoriashipyards_generate_timesheets_child_dag_{config.instance}',
            conf=lambda item, dag_run: {
                "dag_run_ecid": get_dagrun_ecid(dag_run),
                "user_data": item
            }
        )

        wait_for_generate_timesheets = rail.WaitForDagRunsSensor(
            task_id='wait_for_generate_timesheets',
            dag_runs='{{ result("generate_timesheets") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=3,
        )

        write_timesheet_logs_csv = rail.WriteCSVFileOperator(
            task_id='write_timesheet_logs_csv',
            trigger_rule='all_done',
            source="{{ get_master_log() }}",
            header=[
                'Login Name',
                'Timesheet Date',
                'Status',
                'Details',
                'Jobid'],
            row=[
                '{{ item.properties.loginname }}',
                '{{ item.properties.timesheetdate }}',
                '{{ item.properties.status }}',
                '{{ item.properties.details }}',
                '{{ item.properties.jobid }}|{{ item.properties.childjobid }}'],
            lineterminator='\n'
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            severity='Error',
        )

        get_logged_exceptions = rail.FilterLogEntriesOperator(
            task_id='get_logged_exceptions',
            severity='Exception',
        )

        filename = '{{result("get_logging_details")["log_filename"]}}'

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("write_timesheet_logs_csv")}}',
            output_file_name=filename,
            expires_in_seconds=7 * 24 * 60 * 60,
        )

        send_export_complete_email = rail.EmailOperator(
            task_id='send_export_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Manual timesheet generation " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors | \
                {%- else -%} \
                    {%- if result("get_logged_exceptions", key="length") > 0 -%} \
                        completed with exceptions | \
                    {%- else -%} \
                        completed successfully | \
                    {%- endif -%} \
                {%- endif -%}'
                + ' {{ result("get_logging_details")["dag_run_start_time"] }}',
            html_content="templates/emails/export_complete.html",
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_logging_details >> get_report_details >> run_report_entry
        run_report_exit >> no_error_exists

        no_error_exists >> rail.Label("Yes") >> payload_has_data

        payload_has_data >> rail.Label("Yes") >> load_csv_data >> timesheet_report_data >> get_allowed_users \
            >> is_alloweduser_present

        is_alloweduser_present >> rail.Label("Yes") >> generate_timesheets >> wait_for_generate_timesheets \
            >> write_timesheet_logs_csv >> get_logged_errors >> get_logged_exceptions >> generate_download_link \
            >> send_export_complete_email
        is_alloweduser_present >> rail.Label("No") >> finish

        payload_has_data >> rail.Label("No") >> finish

        no_error_exists >> rail.Label("No") >> fail_error_report

    return dag


rail.for_each_instance(create_dag)
