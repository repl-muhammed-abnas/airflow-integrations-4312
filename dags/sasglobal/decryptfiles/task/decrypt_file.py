from datetime import timedelta
import rail

def decrypt_files(config,input_filepath,processing_filepath,archive_filepath):
    with rail.TaskGroup(group_id='decrypt_allfiles', prefix_group_id=False):

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=input_filepath,
            soft_fail_timeout=timedelta(minutes=15),

        )
        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='download_file',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        upload_decryt_data = rail.SFTPUploadFileOperator(
            task_id='upload_decryt_data',
            content="{{ result('decrypt_file') }}",
            remote_filepath=processing_filepath +
            "{{ result('new_file_sensor') | file_name | replace('.pgp', '')}}",
        )

        move_existing_archive = rail.SFTPMoveFileOperator(
            task_id='move_existing_archive',
            new_filename=archive_filepath + '{{ current_time("%H%M%S") }}_' + "{{ result('new_file_sensor') | file_name}}",
            existing_filename=input_filepath + "/{{ result('new_file_sensor') | file_name}}",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        new_file_sensor >> was_new_file_found >> rail.Label(
            "Yes") >> download_file >> decrypt_file >> upload_decryt_data >> move_existing_archive >> log_to_sumo
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

    return new_file_sensor
