from datetime import timedelta
import rail

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/ppmc_project_and_tasks_import/config.py


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_ppmc_project_task_import_move_file_{config.instance}',
        description=f'PPMC - Move file to processing - New logic to ignore duplicate {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        schedule_interval=timedelta(
            minutes=config.move_file_interval_in_minutes),
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        list_sftp_files = rail.SFTPListFilesOperator(
            task_id='list_sftp_files',
            paths=[config.move_file_input_filepath]
        )

        has_files = rail.IfOperator(
            task_id='has_files',
            test=lambda: bool(rail.result('list_sftp_files').get(
                config.move_file_input_filepath)),
            yes_task='sftp_files'
        )

        sftp_files = rail.CreateCollectionOperator(
            task_id='sftp_files',
            source=lambda: rail.result('list_sftp_files')[
                config.move_file_input_filepath]
        )

        query_empty_files = rail.QueryCollectionOperator(
            task_id='query_empty_files',
            query=f'''SELECT * FROM sftp_files  WHERE type='file' AND  CAST(size as decimal) <= {config.move_file_empty_file_size_in_bytes} '''
        )

        process_empty_files = rail.TriggerDagRunForEachItemOperator(
            task_id='process_empty_files',
            retries=0,
            items=lambda: list(
                map(lambda x: {'sftp_file_name': x['name']}, rail.load_all_records(rail.result('query_empty_files')))),
            trigger_dag_id=f'dxctechnology_ppmc_project_task_import_move_file_archive_child_{config.instance}',
            execution_timeout=timedelta(days=14),
        )

        wait_for_process_empty_files = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_empty_files',
            dag_runs='{{ result("process_empty_files") }}',
            execution_timeout=timedelta(days=14),
        )

        query_input_files = rail.QueryCollectionOperator(
            task_id='query_input_files',
            query=f'''SELECT * FROM sftp_files
                        WHERE type='file' AND  CAST(size as decimal) > {config.move_file_empty_file_size_in_bytes}
                        ORDER BY CAST(modify as DECIMAL), CAST(size as decimal) DESC'''
        )

        def get_process_input_files_source():
            input_files_by_date = rail.load_all_records(
                rail.result('query_input_files'))
            files = []
            index = 0
            #file_date = None
            for file in input_files_by_date:
                index = 0 # index + 1 if file_date == file['modify'] else 0
                files.append({'sftp_file_name': file['name'], 'index': index})
                #file_date = file['modify']
            return files

        process_input_files = rail.TriggerDagRunForEachItemOperator(
            task_id='process_input_files',
            retries=0,
            items=get_process_input_files_source,
            trigger_dag_id=f'dxctechnology_ppmc_project_task_import_move_file_process_child_{config.instance}',
            execution_timeout=timedelta(days=7),
        )

        wait_for_process_input_files = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_input_files',
            dag_runs='{{ result("process_input_files") }}',
            execution_timeout=timedelta(days=7),
        )

        list_sftp_files >> has_files
        has_files >> rail.Label('yes') >> sftp_files >> query_empty_files >> process_empty_files >> wait_for_process_empty_files \
            >> query_input_files >> process_input_files >> wait_for_process_input_files

    return dag


rail.for_each_instance(create_dag)
