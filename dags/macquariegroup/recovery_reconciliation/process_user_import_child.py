import rail


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"macquarie_recovery_reconciliation_move_newest_file_to_processing_child_{config.instance}",
        description=f"Macquarie user Import Send recovery enabled emails to users {config.instance}",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=10,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_conf")

        list_user_import_input_dir = rail.SFTPListFilesOperator(
            task_id = "list_user_import_input_dir",
            paths=[config.user_import_input_filepath],
            order_direction="descending"
        )

        has_any_files = rail.IfOperator(
            task_id = "has_any_files",
            test = "{{ result('list_user_import_input_dir') | is_truthy }}",
            yes_task="get_latest_file_from_input"
        )

        get_latest_file_from_input = rail.PythonOperator(
            task_id = "get_latest_file_from_input",
            python_callable=lambda : rail.result("list_user_import_input_dir")[config.user_import_input_filepath][0]
        )

        move_latest_file_to_processing = rail.SFTPMoveFileOperator(
            task_id = "move_latest_file_to_processing",
            existing_filename= config.user_import_input_filepath + "/{{result('get_latest_file_from_input').name}}",
            new_filename= config.user_import_processing_filepath + "/{{result('get_latest_file_from_input').name}}"
        )

        archive_older_files = rail.ForEachOperator(
            task_id = "archive_older_files",
            # ignoring the 1st file as that is already moved to the processing folder
            items= lambda: rail.result("list_user_import_input_dir")[config.user_import_input_filepath][1:],
            start_task="move_file_to_archive",
            end_task="for_each_end"
        )

        move_file_to_archive = rail.SFTPMoveFileOperator(
            task_id = "move_file_to_archive",
            existing_filename= config.user_import_input_filepath + "/{{result('archive_older_files').name}}",
            new_filename= config.user_import_archive_filepath + "/skipped_{{result('archive_older_files').name}}"
        )

        for_each_end = rail.EmptyOperator(
            task_id= "for_each_end"
        )

        list_user_import_input_dir >> has_any_files >> rail.Label("Yes") >> get_latest_file_from_input >> move_latest_file_to_processing\
        >> archive_older_files >> move_file_to_archive >> for_each_end
        archive_older_files >> for_each_end

    return dag

rail.for_each_instance(create_child_dag)
