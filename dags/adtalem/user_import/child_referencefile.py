from datetime import timedelta
from airflow.models import Variable
import rail

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_referencefile_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_child_referencefile_{config.instance}',
        description=f'Adtalem Child Reference File {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_referencefile_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_referencefile'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='process_referencefile',
            end_task='finish',
        )

        process_referencefile = rail.EmptyOperator(
            task_id='process_referencefile'
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test="{{ dag_run.conf.reference_file | file_ext | lower == 'csv' }}",
            yes_task='process_file',
            no_task='finish'
        )

        process_file = rail.EmptyOperator(
            task_id='process_file'
        )

        should_archive_file = rail.IfOperator(
            task_id='should_archive_file',
            test="{{ dag_run.conf.action == 'archive' }}",
            yes_task='archive_reference_file',
            no_task='download_reference_file'
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id='archive_reference_file',
            existing_filename='{{ dag_run.conf.reference_file }}',
            # pylint: disable=line-too-long
            new_filename="{{ dag_run.conf.archive_filepath }}/Old_reference_{{ dag_run.conf.filename }}_{{ dag_run.conf.time }}.csv"
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath='{{ dag_run.conf.reference_file }}'
        )

        load_reference_file_data = rail.LoadCSVFileOperator(
            task_id='load_reference_file_data',
            document="{{ result('download_reference_file') }}"
        )

        create_userreference_data = rail.PythonOperator(
            task_id='create_userreference_data',
            python_callable=lambda: rail.load_all_records(
                rail.result('load_reference_file_data')),
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label(
            'No') >> process_referencefile

        process_referencefile >> is_csv

        is_csv >> rail.Label(
            'Yes') >> process_file >> should_archive_file

        is_csv >> rail.Label(
            'No') >> finish

        should_archive_file >> rail.Label(
            'Yes') >> archive_reference_file >> finish

        should_archive_file >> rail.Label(
            'No') >> download_reference_file >> load_reference_file_data >> create_userreference_data >> finish

        return dag


rail.for_each_instance(create_referencefile_child_dag)
