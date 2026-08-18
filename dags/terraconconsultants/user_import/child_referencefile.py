from datetime import timedelta
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/terraconconsultants/user_import/config.py


def create_referencefile_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'terraconconsultants_userimport_child_referencefile_{config.instance}',
        description=f'TerraconConsultants Child Reference File {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_referencefile_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'aws_conn_id': config.aws_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

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
            end_task='dagrun_log_to_sumo',
        )

        process_referencefile = rail.EmptyOperator(
            task_id='process_referencefile'
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test="{{ dag_run.conf.keyname | file_ext | lower == 'csv' }}",
            yes_task='process_file',
            no_task='dagrun_log_to_sumo'
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

        archive_reference_file = rail.S3MoveFileOperator(
            task_id='archive_reference_file',
            source_bucket_name=config.bucket_name,
            existing_key_name='{{ dag_run.conf.keyname }}',
            new_key_name="{{ dag_run.conf.archive_keyname }}",
            aws_conn_id=config.aws_conn_id
        )

        download_reference_file = rail.S3DownloadFileOperator(
            task_id='download_reference_file',
            bucket_name=config.bucket_name,
            key_name="{{ dag_run.conf.keyname }}",
            aws_conn_id=config.aws_conn_id
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

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> process_referencefile

        process_referencefile >> is_csv

        is_csv >> rail.Label(
            'Yes') >> process_file >> should_archive_file

        is_csv >> rail.Label(
            'No') >> dagrun_log_to_sumo

        should_archive_file >> rail.Label(
            'Yes') >> archive_reference_file >> dagrun_log_to_sumo

        should_archive_file >> rail.Label(
            'No') >> download_reference_file >> load_reference_file_data >> create_userreference_data >> \
            dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_referencefile_dag)
