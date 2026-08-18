from datetime import timedelta
from pendulum import datetime
import pendulum
import rail
from capgemini.optional_holidays_auto_population_india.utils.python_callable import get_dagruns_to_process
from capgemini.optional_holidays_auto_population_india.utils.custom_methods import get_logging_details
from capgemini.optional_holidays_auto_population_india.utils.request_payload import load_logs
from airflow.models import Variable


def create_log_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'capgemini_optional_holiday_booking_master_log_scheduled_{config.instance}',
        description=f'Capgemini Auto Population of Optional Holiday Booking - Scheduled {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.log_generation_dag_interval,
        max_active_tasks=config.dag_max_active_tasks,
        start_date=datetime(2023, 7, 1),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            'retries': 0
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
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
            op_args=[config, "schedule_logs"]
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_log_dagruns_to_process = rail.PythonOperator(
            task_id='get_log_dagruns_to_process',
            python_callable=get_dagruns_to_process,
            op_args=[config.time_zone, config.lookup_log_timestamp_var,
                     config.lookup_log_timestamp_hours,
                     f'capgemini_auto_population_of_optional_holidays_india_process_new_users_{config.instance}']
        )

        is_log_dagruns_present = rail.IfOperator(
            task_id='is_log_dagruns_present',
            test="{{ result('get_log_dagruns_to_process') | length > 0 }}",
            yes_task='get_booking_logs',
            no_task='delete_this_dagrun'
        )

        get_booking_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='get_booking_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('get_log_dagruns_to_process') }}",
            dagrun_task_id='render_logs_csv',
            flatten=True
        )

        compose_booking_logs = rail.CreateCollectionOperator(
            task_id='compose_booking_logs',
            source=load_logs
        )

        has_any_data = rail.IfOperator(
            task_id="has_any_data",
            test="{{ result('compose_booking_logs', 'length') > 0 }}",
            yes_task='write_booking_logs',
            no_task='delete_this_dagrun'
        )

        write_booking_logs = rail.WriteLogOperator(
            task_id="write_booking_logs",
            log="{{ result('create_log') }}",
            message="Optional Holiday Booking Logs",
            items="{{ result('compose_booking_logs') }}",
            properties=lambda item: item
        )

        get_logged_success = rail.FilterLogEntriesOperator(
            task_id='get_logged_success',
            log='{{ result("create_log") }}',
            properties={'status': 'Success'}
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            log='{{ result("create_log") }}',
            properties={'status': 'Error'}
        )

        get_logged_skipped = rail.FilterLogEntriesOperator(
            task_id='get_logged_skipped',
            log='{{ result("create_log") }}',
            properties={'status': 'Skipped'}
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('create_log') }}",
            header=['State', 'Username', 'Employee ID',
                    'Booking Date', 'Status', 'Comments', 'ECID'],
            row=['{{ item.properties | attr_or_default("State", "") }}', '{{ item.properties | attr_or_default("Username", "") }}',
                 '{{ item.properties | attr_or_default("Employee_ID", "") }}', '{{ item.properties | attr_or_default("Booking_Date", "") }}',
                 '{{ item.properties | attr_or_default("Status", "") }}', '{{ item.properties | attr_or_default("Comments", "") }}',
                 '{{ item.properties | attr_or_default("ECID", "") }}']
        )

        get_log_filename = rail.PythonOperator(
            task_id='get_log_filename',
            python_callable=lambda: f'{config.log_file_prefix}_Optional_Holiday_Booking_New_Users' + '_Logs_' +
            pendulum.now(config.time_zone).strftime("%Y%m%d_%H%M%S") + '.csv'
        )

        upload_logs_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_logs_to_s3',
            source='{{ result("render_logs_csv") }}',
            key_name=config.s3_log_filepath +
            '/{{ result("get_log_filename") }}',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/{{ result("get_log_filename") }}',
        )

        send_complete_email = rail.EmailOperator(
            task_id='send_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | The Auto population of Optional holiday booking for New Users is " }} \
                {%- if result("get_logged_errors", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " - " + result("logging_details").process_start_time }}',
            html_content="/templates/emails/new_users_complete.html",
            params={
                'log_filepath': config.log_filepath,
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

        logging_details >> create_log >> get_log_dagruns_to_process >> is_log_dagruns_present

        is_log_dagruns_present >> rail.Label(
            'Yes') >> get_booking_logs >> compose_booking_logs >> has_any_data

        has_any_data >> rail.Label(
            "Yes") >> write_booking_logs >> get_logged_success >> get_logged_errors >> get_logged_skipped >> render_logs_csv \
            >> get_log_filename >> upload_logs_to_s3 >> upload_log_to_sftp >> send_complete_email >> dagrun_log_to_sumo

        has_any_data >> rail.Label(
            "No") >> delete_this_dagrun

        is_log_dagruns_present >> rail.Label(
            "No") >> delete_this_dagrun

        delete_this_dagrun >> dagrun_log_to_sumo >> can_fail_dag >> rail.Label(
            "Yes") >> fail_dagrun

        return dag


rail.for_each_instance(create_log_airflow_dag)
