from datetime import timedelta
import rail


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_wf39_psa_clone_files_{config.instance}",
        description="DXCTechnology Wf39 PSA CLoning files",
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

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='is_C1_file',
            no_task='delete_this_dagrun',
        )

        is_C1_file = rail.IfOperator(
            task_id='is_C1_file',
            test="{{ result('new_file_sensor') | file_base | matches(['C1'])}}",
            yes_task='move_file_to_c1_processing',
            no_task='move_file_to_cp_processing'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        move_file_to_c1_processing = rail.SFTPMoveFileOperator(
            task_id="move_file_to_c1_processing",
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.c1_filepath + "/{{ result('new_file_sensor') | file_name}}"
        )

        move_file_to_cp_processing = rail.SFTPMoveFileOperator(
            task_id="move_file_to_cp_processing",
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.compass_filepath + "/{{ result('new_file_sensor') | file_name}}"
        )

        new_file_sensor >> was_new_file_found >> rail.Label("Yes") >> is_C1_file >> rail.Label("No") >> move_file_to_cp_processing
        is_C1_file >> rail.Label("Yes") >> move_file_to_c1_processing
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
