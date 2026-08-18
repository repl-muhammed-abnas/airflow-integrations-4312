from datetime import timedelta
from pendulum import datetime
import rail
from data_intellect_services.user_sync_v1.utils.python_callable import get_logging_details
from airflow.models import Variable


def create_log_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'data_intellect_user_import_scheduled_log_export_{config.instance}_v1',
        description=f'Data intellect services user sync  - Scheduled log export {config.instance} V1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.log_generation_dag_interval,
        start_date=datetime(2024, 1, 1, tz=config.time_zone),
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_scheduled_logs_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='logging_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='logging_details',
            end_task='dagrun_log_to_sumo',
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=get_logging_details,
            op_args=[config]
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log',
            tenant_wide_name=config.tenant_wide_log_name_for_logs,
            existing_log_mode="append"
        )

        get_all_logs = rail.FilterLogEntriesOperator(
            task_id='get_all_logs',
            log='{{ result("create_log") }}',
            remove_filtered_entries=True
        )

        has_any_entries = rail.IfOperator(
            task_id='has_any_entries',
            test='{{ result("get_all_logs", "length") > 0 }}',
            yes_task='get_logged_success',
            no_task='send_no_users_email',
        )

        # pylint: disable=line-too-long
        subject = '{{ get_company_key() }} | User Sync from HIBOB to Replicon is completed - No changes required - {{ current_time_in_specified_tz("' + config.time_zone +'") }}'

        send_no_users_email = rail.EmailOperator(
            task_id='send_no_users_email',
            to=config.tenant_email,
            subject=subject,
            html_content="/templates/emails/no_new_users.html",
            params={
                'time_zone': config.time_zone
            }
        )

        get_logged_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_success',
            log='{{ result("get_all_logs") }}',
            properties={'status': 'Success'}
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            log='{{ result("get_all_logs") }}',
            properties={'status': 'Error'}
        )

        get_logged_exceptions = rail.FilterLogEntriesOperator(
            task_id='get_logged_exceptions',
            log='{{ result("get_all_logs") }}',
            properties={'status': 'Exception'}
        )

        get_logged_skipped = rail.FilterLogEntriesOperator(
            task_id='get_logged_skipped',
            log='{{ result("get_all_logs") }}',
            properties={'status': 'Skipped'}
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('get_all_logs') }}",
            header=['Username', 'Employee ID', 'Unique ID', 'Action', 'Status', 'Comments', 'ECID'],
            row=['{{ item.properties | attr_or_default("username", "") }}',
                 '{{ item.properties | attr_or_default("employee_id", "") }}',
                 '{{ item.properties | attr_or_default("unique_id", "") }}',
                 '{{ item.properties | attr_or_default("action", "") }}',
                 '{{ item.properties | attr_or_default("status", "") }}',
                 '{{ item.properties | attr_or_default("comments", "") }}',
                 '{{ item.ecid }}']
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name="{{ result('render_logs_csv') }}",
            output_file_name="{{ result('logging_details').log_filename }}",
            expires_in_seconds=7*24*60*60
        )

        subject = '{{ get_company_key() + " | User Sync from HIBOB to Replicon is " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_logged_exceptions", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz("'+ config.time_zone +'") }}'

        send_complete_email = rail.EmailOperator(
            task_id='send_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject=subject,
            html_content="/templates/emails/complete_sync.html",
            params={
                'time_zone': config.time_zone
            }
        )

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

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun")

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label("No") >> logging_details

        logging_details >> create_log >> get_all_logs >> has_any_entries
        has_any_entries >> rail.Label("Yes") >> get_logged_success >> get_logged_errors \
            >> get_logged_exceptions >> get_logged_skipped >> render_logs_csv >> generate_download_link \
                >> send_complete_email >> dagrun_log_to_sumo

        has_any_entries >> rail.Label(
            "No") >> send_no_users_email >> delete_this_dagrun

        delete_this_dagrun >> dagrun_log_to_sumo >> can_fail_dag >> rail.Label(
            "Yes") >> fail_dagrun

        return dag


rail.for_each_instance(create_log_airflow_dag)
