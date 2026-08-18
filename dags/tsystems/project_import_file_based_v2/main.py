from datetime import datetime, timedelta
import rail
from tsystems.project_import_file_based_v2.utils import custom_methods
def create_file_based_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.file_based_master_dag_id,
        description=f'T-Systems Project Import - File-Based Initial Load ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 1, 1),
        schedule_interval=timedelta(minutes=config.schedule_interval_minutes),
        max_active_runs=config.master_max_active_run,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        is_json_file = rail.IfOperator(
            task_id='is_json_file',
            test='{{ result("new_file_sensor") | file_ext | lower == "json" }}',
            yes_task='download_file',
            no_task='send_invalid_format_email',
        )

        send_invalid_format_email = rail.EmailOperator(
            task_id='send_invalid_format_email',
            to=config.tenant_email,
            cc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon Project Import - Invalid Format - {{ current_time_in_specified_tz() }}',
            html_content='templates/emails/invalid_format_email.html'
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath='{{ result("new_file_sensor") }}'
        )

        read_and_parse_json = rail.PythonOperator(
            task_id='read_and_pa' \
            'rse_json',
            python_callable=custom_methods.parse_sftp_json_file_to_project_list
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=f"{config.archive_filepath}/{{{{ dag_run_ecid() | replace(':', '-')}}}}_{{{{ result('new_file_sensor') | file_name }}}}"
        )

        has_projects_in_file = rail.IfOperator(
            task_id='has_projects_in_file',
            test=lambda: len(rail.result('read_and_parse_json')) > 0,
            yes_task='trigger_process_payload',
            no_task='send_no_data_email'
        )

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            cc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon Project Import - No Data - {{ current_time_in_specified_tz() }}',
            html_content='templates/emails/no_data_email.html'
        )

        trigger_process_payload = rail.TriggerDagRunOperator(
            task_id='trigger_process_payload',
            trigger_dag_id=config.process_payload_dag_id,
            conf=lambda: {
                "project_list": rail.result('read_and_parse_json'),
                "operation_type": "initial_load",
                "batch_size": len(rail.result('read_and_parse_json')),
                "source_file": rail.render_template('{{ result("new_file_sensor") | file_name }}')
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_for_processing = rail.WaitForDagRunsSensor(
            task_id='wait_for_processing',
            dag_runs="{{ result('trigger_process_payload') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )
        new_file_sensor >> is_json_file
        is_json_file >> rail.Label("Yes") >> download_file >> read_and_parse_json
        is_json_file >> rail.Label("No") >> send_invalid_format_email
        download_file >> was_new_file_found
        was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        read_and_parse_json >> has_projects_in_file
        has_projects_in_file >> rail.Label("Yes") >> trigger_process_payload >> wait_for_processing
        has_projects_in_file >> rail.Label("No") >> send_no_data_email

    return dag

rail.for_each_instance(create_file_based_main_dag)
