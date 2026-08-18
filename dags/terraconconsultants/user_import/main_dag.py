from datetime import timedelta
import rail
from terraconconsultants.user_import.utils.python_callable_method import get_archivefilename


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/terraconconsultants/user_import/config.py


def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'terraconconsultants_userimport_master_decrypt_and_upload_to_processing_{config.instance}',
        description=f'TerraconConsultants User Import Master Decrypt and upload to processing {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.master_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        is_pgp = rail.IfOperator(
            task_id='is_pgp',
            test='{{ result("new_file_sensor") | file_ext | lower == "pgp" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User Import - Incorrect file format received - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email/email_bad_format_email.html"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            retries=0,
            source="{{ result('download_file') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        upload_processed_file = rail.SFTPUploadFileOperator(
            task_id='upload_processed_file',
            content="{{ result('decrypt_file') }}",
            remote_filepath=config.processing_filepath +
            "/{{ result('new_file_sensor') | file_base }}.csv"
        )

        trigger_userimport_processfile = rail.TriggerDagRunOperator(
            task_id='trigger_userimport_processfile',
            retries=0,
            trigger_dag_id=f'terraconconsultants_userimport_process_each_file_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf={
                "filename": "{{ result('new_file_sensor') | file_base }}",
                "filepath": config.processing_filepath + "/{{ result('new_file_sensor') | file_base }}.csv"
            }
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='get_archive_filename',
            no_task='delete_this_dagrun'
        )

        get_archive_filename = rail.PythonOperator(
            task_id='get_archive_filename',
            python_callable=get_archivefilename,
            op_args=[
                "{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_base }}"]
        )

        if_archive_filename = rail.IfOperator(
            task_id='if_archive_filename',
            test="{{ result('get_archive_filename') | is_truthy }}",
            yes_task='archive_file',
            no_task='delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/{{ result('get_archive_filename') }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        move_unprocessed_file = rail.SFTPMoveFileOperator(
            task_id='move_unprocessed_file',
            trigger_rule='one_failed',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.unprocessed_filepath +
            "/{{ result('new_file_sensor') | file_name }}"
        )

        send_alert_email = rail.EmailOperator(
            task_id='send_alert_email',
            to=config.alert_email,
            subject='{{ get_company_key() }} | User Import - Feed file issue - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email/email_alert.html",
            params={
                'dag_id': f'terraconconsultants_userimport_decrypt_and_upload_to_processing_{config.instance}'
            }
        )

        new_file_sensor >> is_pgp

        is_pgp >> rail.Label(
            'Yes') >> download_file >> decrypt_file >> upload_processed_file >> \
                trigger_userimport_processfile

        trigger_userimport_processfile >> rail.Label(
                'Always') >> was_new_file_found

        trigger_userimport_processfile >> rail.Label(
            'Fail') >> move_unprocessed_file >> send_alert_email

        was_new_file_found >> rail.Label(
            'Yes') >> get_archive_filename >> if_archive_filename

        if_archive_filename >> rail.Label(
            'Yes') >> archive_file

        if_archive_filename >> rail.Label(
            'No') >> delete_this_dagrun

        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun

        is_pgp >> rail.Label(
            'No') >> send_bad_file_format_email

    return dag


rail.for_each_instance(create_master_dag)
