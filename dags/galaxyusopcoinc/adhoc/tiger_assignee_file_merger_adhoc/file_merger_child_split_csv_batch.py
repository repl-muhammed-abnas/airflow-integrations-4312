from datetime import timedelta
from airflow.models import Variable
import csv
import rail

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_tiger_assignee_file_merger_split_csv_batch_child_{config.instance}_adhoc',
        description='Vialto Partners Tiger Assignee file merger child Split CSV Batch',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_split_csv_batch,
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
            no_task='log_file_name'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='log_file_name',
            end_task='finish',
        )

        log_file_name = rail.WriteLogOperator(
            task_id = 'log_file_name',
            log="{{ dag_run.conf.batch_log }}",
            message="batch_log_filename",
            properties={
                "actual_file_name": "{{ dag_run.conf.actual_file_name }}",
                "file_name_processed": "{{ dag_run.conf.actual_file_name }}_{{ dag_run.conf.index }}"
            }
        )

        query_data_to_process = rail.QueryCollectionOperator(
            task_id="query_data_to_process",
            name="get_data_to_process_child_{{dag_run.conf.index}}",
            query="""SELECT * from assignee_data_with_index
                        WHERE CAST (ROW_NUM as int) BETWEEN {{dag_run.conf.record_start_index}} AND {{dag_run.conf.record_end_index}}"""
        )

        write_csv_file = rail.WriteCSVFileOperator(
            task_id='write_csv_file',
            source=lambda: rail.result('query_data_to_process'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            header=['Tiger client long name',
                    'Tiger short name',
                    'assignee ID',
                    'First Name',
                    'Last Name',
                    'Status'],
            row=[
                '{{ item.clientlongname }}',
                '{{ item.clientshortname }}',
                '{{ item.assigneeid }}',
                "{{ item.firstname }}",
                "{{ item.lastname }}",
                '{{ item.status }}'],
            quoting=csv.QUOTE_ALL
        )

        upload_file_to_processing = rail.SFTPUploadFileOperator(
            task_id='upload_file_to_processing',
            content="{{ result('write_csv_file') }}",
            remote_filepath=config.processing_filepath +
            '/' +
            "{{ dag_run.conf.actual_file_name }}_{{dag_run.conf.index}}.csv",
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_file_name

        log_file_name >> query_data_to_process >> write_csv_file >> upload_file_to_processing >> finish

    return dag

rail.for_each_instance(create_dag)
