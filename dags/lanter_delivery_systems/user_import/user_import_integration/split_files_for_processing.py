from datetime import timedelta
import rail

# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.split_files_master_dagid,
        description='Lanter Delivery Systems User Import - Split files Processing',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.replicon_sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            sftp_conn_id = config.sftp_conn_id,
            path=config.splitfile_input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='download_file_from_client_sftp',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        download_file_from_client_sftp = rail.SFTPDownloadFileOperator(
            task_id='download_file_from_client_sftp',
            sftp_conn_id = config.sftp_conn_id,
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        is_newtempworkers_file = rail.IfOperator(
            task_id='is_newtempworkers_file',
            test='{{ result("new_file_sensor") | file_base | starts_with("NewTempWorkers")}}',
            yes_task='upload_file_to_process_users_path',
            no_task='is_disabledusers_file'
        )

        upload_file_to_process_users_path = rail.SFTPUploadFileOperator(
            task_id='upload_file_to_process_users_path',
            content="{{ result('download_file_from_client_sftp') }}",
            remote_filepath=config.process_users_input_filepath +
            "/{{ result('new_file_sensor') | file_name }}"
        )

        is_disabledusers_file = rail.IfOperator(
            task_id='is_disabledusers_file',
            test='{{ result("new_file_sensor") | file_base | starts_with("EndedWorkers") }}',
            yes_task='upload_file_to_disable_users_path',
            no_task='upload_file_to_archive'
        )

        upload_file_to_disable_users_path  = rail.SFTPUploadFileOperator(
            task_id='upload_file_to_disable_users_path',
            content="{{ result('download_file_from_client_sftp') }}",
            remote_filepath=config.disable_users_input_filepath +
            "/{{ result('new_file_sensor') | file_name }}"
        )

        upload_file_to_archive  = rail.SFTPUploadFileOperator(
            task_id='upload_file_to_archive',
            content="{{ result('download_file_from_client_sftp') }}",
            remote_filepath=config.archive_filepath +
            "/{{ result('new_file_sensor') | file_name }}"
        )

        send_bad_filename_email = rail.EmailOperator(
            task_id='send_bad_filename_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User Import - Invalid File Name - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/bad_file_name.html"
        )

        delete_file_from_client_sftp = rail.SFTPDeleteFileOperator(
            task_id='delete_file_from_client_sftp',
            sftp_conn_id = config.sftp_conn_id,
            existing_filename='{{ result("new_file_sensor") }}'
        )

        new_file_sensor >> was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        was_new_file_found >> rail.Label('Yes') >> download_file_from_client_sftp
        download_file_from_client_sftp >> is_newtempworkers_file >> rail.Label("No") >> is_disabledusers_file
        is_newtempworkers_file >> rail.Label("Yes") >> upload_file_to_process_users_path >> delete_file_from_client_sftp
        is_disabledusers_file >> rail.Label("Yes") >> upload_file_to_disable_users_path >> delete_file_from_client_sftp
        is_disabledusers_file >> rail.Label("No") >> upload_file_to_archive >> send_bad_filename_email >> delete_file_from_client_sftp

    return dag

rail.for_each_instance(create_main_dag)
