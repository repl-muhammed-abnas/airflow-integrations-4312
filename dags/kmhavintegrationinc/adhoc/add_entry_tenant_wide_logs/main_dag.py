from datetime import timedelta
import rail

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'kmhavintegrationinc_task_milestone_add_entry_adhoc_master_{config.instance}',
        description=' ADHOC - Add Entry Tenant Wide Logs',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_schedule_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_task_milestone_data = rail.LoadCSVFileOperator(
            task_id='load_task_milestone_data',
            document="{{ result('download_file') }}"
        )

        task_milestone_logger = rail.CreateLogOperator(
            task_id="task_milestone_logger",
            tenant_wide_name="task_milestone_log_table",
            existing_log_mode="append",
        )

        add_new_entry_to_milestone_log_table=rail.WriteLogOperator(
            task_id="add_new_entry_to_milestone_log_table",
            log="{{ result('task_milestone_logger') }}",
            items="{{ result('load_task_milestone_data') }}",
            message="Add_Entry",
            properties=lambda item:{
                'Task Name':  item['Task Name'],
                'Project Name': item['Project Name'],
                'Milestone Range': item['Milestone Range']
            }
        )


        new_file_sensor >> download_file >> rail.Label('Always') >> was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        was_new_file_found >> rail.Label('Yes') >> archive_file

        download_file >> load_task_milestone_data >> task_milestone_logger >> add_new_entry_to_milestone_log_table

    return dag

rail.for_each_instance(create_dag)
