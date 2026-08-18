from datetime import timedelta
from airflow.models import Variable
import rail

from galaxyusopcoinc.tiger_assignee_file_merger.utils import custom_methods

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_tiger_assignee_file_merger_child_{config.instance}',
        description='Vialto Partners Tiger Assignee file merger child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='download_file'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='download_file',
            end_task='finish',
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath=config.input_filepath +
            '/' + "{{ dag_run.conf.file_name }}",
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('decrypt_file') }}"
        )

        create_input_collection = rail.CreateCollectionOperator(
            task_id='create_input_collection',
            name='inputdata',
            source="{{ result('load_data') }}",
            columns={
                'Tiger client long name': 'clientlongname',
                'Tiger short name': 'clientshortname',
                'assignee ID': 'assigneeid',
                'First Name': 'firstname',
                'Last Name': 'lastname',
                'Status': 'status'
            }
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test="{{ result('create_input_collection','length') > 0 }}",
            yes_task='get_query_to_merge',
            no_task='move_files_to_archive',
        )

        get_query_to_merge = rail.PythonOperator(
            task_id='get_query_to_merge',
            python_callable=custom_methods.get_query
        )

        query_data = rail.QueryCollectionOperator(
            task_id='query_data',
            query='{{result("get_query_to_merge")}}'
        )

        move_files_to_archive = rail.SFTPMoveFileOperator(
            task_id='move_files_to_archive',
            existing_filename=config.input_filepath +
            '/{{ dag_run.conf.file_name }}',
            new_filename=config.archive_filepath +
            "/{{dag_run.conf.file_index}}_{{current_time('%Y%m%dT%H%M%S')}}_{{dag_run.conf.file_name }}"
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> download_file

        download_file >> decrypt_file >> load_data >> create_input_collection >> has_data
        has_data >> rail.Label("Yes") >> get_query_to_merge >> query_data >> move_files_to_archive >> finish
        has_data >> rail.Label("No") >> move_files_to_archive
        move_files_to_archive >> finish

    return dag

rail.for_each_instance(create_dag)
