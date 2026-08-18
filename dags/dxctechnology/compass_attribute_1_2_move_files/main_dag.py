from datetime import timedelta
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_move_compass_attribute_1_2_files_{config.sub_erp}_{config.instance}",
        description="DXC Attribute 1 and 2 move files",
        company_key=config.company_key,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=1
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
            # We do the timeout with a soft fail here to yield to potential other waiting executions of this DAG
            # Since max_active_runs is set to 1, if this sensor ran indefinitiely then someone manually wanting to
            # retry failed tasks in a past run would also be waiting indefinitely. This way it'll give them a window
            # every 10 minutes to run their tasks.
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='is_attributes_1_2_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath + "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}")

        is_attributes_1_2_file = rail.IfOperator(
            task_id='is_attributes_1_2_file',
            test="{{ result('new_file_sensor') | file_base | matches(['Attributes_1', 'Attributes_2'])}}",
            yes_task='is_file_attribute1',
            no_task='send_bad_file_name_email'
        )

        send_bad_file_name_email = rail.EmailOperator(
            task_id='send_bad_file_name_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            #pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon project field sync - Incorrect File Name - {{ current_time() }}',
            html_content="email_bad_file_name.html",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')


        is_file_attribute1 = rail.IfOperator(
            task_id="is_file_attribute1",
            test="{{ result('new_file_sensor') | file_base | matches(['Attributes_1'])}}",
            yes_task='upload_for_attribute1',
            no_task="is_file_attribute2"
        )

        upload_for_attribute1 = rail.SFTPUploadFileOperator(
            task_id="upload_for_attribute1",
            content="{{result('download_file')}}",
            remote_filepath=config.attribute1_filepath +
            "/{{ result('new_file_sensor') | file_name}}"
        )

        is_file_attribute2 = rail.IfOperator(
            task_id="is_file_attribute2",
            test="{{ result('new_file_sensor') | file_base | matches(['Attributes_2'])}}",
            yes_task='upload_for_attribute2'
        )

        upload_for_attribute2 = rail.SFTPUploadFileOperator(
            task_id="upload_for_attribute2",
            content="{{result('download_file')}}",
            remote_filepath=config.attribute2_filepath +
            "/{{ result('new_file_sensor') | file_name}}"
        )

        new_file_sensor >> download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> is_attributes_1_2_file >> rail.Label("No") \
            >> send_bad_file_name_email >> archive_file
        is_attributes_1_2_file >> rail.Label("Yes") >> is_file_attribute1
        is_file_attribute1 >> rail.Label(
            "Yes") >> upload_for_attribute1 >> archive_file
        is_file_attribute1 >> rail.Label("No") >> is_file_attribute2 >> rail.Label("Yes") \
            >> upload_for_attribute2 >> archive_file

        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
