import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
import rail
from test_dags.retry_failed_tasks import config

with DAG(
    dag_id="system_retry_test_failed_tasks",
    catchup=False,
    schedule=None,
    tags=['system_maintenance'],
    start_date=datetime(2022, 1, 1),
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
    default_args={
        'owner': 'system',
        'replicon_conn_id': config.replicon_conn_id
    },
    default_view="graph",
    max_active_runs=1
) as dag:

    rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

    can_run_batch_task = rail.IfOperator(
        task_id='can_run_batch_task',
        test=lambda: Variable.get(
            config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
        yes_task='batch_task',
        no_task='code_loop'
    )

    batch_task = rail.BatchTaskRunOperator(
        task_id='batch_task',
        start_task='code_loop',
        end_task='delete_this_dagrun',
        execution_timeout=timedelta(
            days=14)
    )

    def get_code_loop():
        counter = 0
        while counter <= 10:
            print(f"Counter is at {counter}")
            counter += 1
            time.sleep(60)
        return counter

    code_loop = rail.PythonOperator(
        task_id='code_loop',
        python_callable=get_code_loop
    )

    empty1 = rail.EmptyOperator(
        task_id="empty1"
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        priority_weight=10,
        task_id='delete_this_dagrun')

    can_run_batch_task >> rail.Label(
        "Yes") >> batch_task >> delete_this_dagrun

    can_run_batch_task >> rail.Label(
        "No") >> code_loop >> empty1 >> delete_this_dagrun
