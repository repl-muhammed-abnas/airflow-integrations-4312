"""
Replicon dag to cleanup efs logs
"""
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import airflow
from airflow.models import Variable
import rail

with airflow.DAG(
    dag_id="system_logs_cleanup",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['system'],
    is_paused_upon_creation=True,
    default_args={
        'owner': 'system',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    default_view="graph",
    max_active_runs=1,
    schedule=timedelta(days=1),
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
) as dag:

    foreach_dag = rail.ForEachOperator(
        task_id='foreach_dag',
        items=lambda: list(chunk(get_dag_data(), 50)),
        start_task='clean_logs',
        reset_count=5,
        end_task='foreach_dag_end'
    )

    clean_logs = rail.PythonOperator(
        task_id='clean_logs',
        python_callable=lambda: rail.parallel_run(
            20, clean_up_dag_dir, rail.result('foreach_dag'))
    )

    foreach_dag_end = rail.EmptyOperator(
        task_id='foreach_dag_end'
    )

    def chunk(source, chunk_size):
        next_chunk = []
        for item in source:
            next_chunk.append(item)
            if len(next_chunk) >= chunk_size:
                yield next_chunk
                next_chunk = []
        if len(next_chunk) > 0:
            yield next_chunk

    def get_dag_data():
        DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = 60
        DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS_VAR_NAME = "airflow_db_cleanup__max_db_entry_age_in_days"
        max_db_entry_age_in_days = int(
            Variable.get(
                DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS_VAR_NAME,
                DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS))
        last_date_time = (datetime.utcnow() -
                          timedelta(days=max_db_entry_age_in_days)).isoformat()
        print('last_date_time ', last_date_time)
        data = [(last_date_time, str(dir))
                for dir in filter(lambda dir: not dir.is_file(), Path('logs').iterdir())]
        return data

    def clean_up_dag_dir(last_date_time_str, dir):
        last_date_time = datetime.fromisoformat(last_date_time_str)
        print(f'started cleaning logs for the dir {dir}')
        try:
            clean_dag_run_logs(last_date_time, dir)
        except Exception as ex:
            print(f'error - {repr(ex)}')
        print(f'completed cleaning logs for the dir {dir}')

    def clean_dag_run_logs(last_date_time, dir):
        for dag_dir in filter(lambda dag_dir: not dag_dir.is_file(), Path(f'{dir}').iterdir()):
            last_mod_date = datetime.fromtimestamp(
                dag_dir.lstat().st_mtime)
            if last_mod_date < last_date_time:
                try:
                    shutil.rmtree(dag_dir)
                except Exception as ex:
                    print(f'error - {repr(ex)}')

    foreach_dag >> clean_logs >> foreach_dag_end
    foreach_dag >> foreach_dag_end
