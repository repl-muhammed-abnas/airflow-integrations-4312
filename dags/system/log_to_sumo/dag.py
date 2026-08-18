"""
Replicon dag to push the log to sumo
"""
from datetime import datetime, timedelta
import json
import airflow
from airflow.utils.log import secrets_masker
import rail

with airflow.DAG(
    dag_id="system_log_to_sumo",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=['system'],
    is_paused_upon_creation=False,
    default_args={
        'owner': 'system',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    default_view="graph",
    max_active_runs=5,
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
) as dag:

    batch_task = rail.BatchTaskRunOperator(
        task_id='batch_task',
        start_task='log_to_sumo',
        end_task='finish',
        execution_timeout=timedelta(days=14),
    )

    def do_log_to_sumo():
        conf = rail.get_current_context()['dag_run'].conf
        send = rail.SimpleHttpOperator(
            task_id='log_to_sumo',
            http_conn_id=conf['sumo_conn_id'],
            method='POST',
            headers={"Content-Type": 'application/json; charset=utf-8'},
            data=json.dumps(secrets_masker.redact(conf['data']))
        )
        send.execute(rail.get_current_context())

    log_to_sumo = rail.PythonOperator(
        task_id='log_to_sumo',
        python_callable=do_log_to_sumo
    )

    delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
        task_id='delete_this_dagrun')

    finish = rail.EmptyOperator(
        task_id='finish',
    )

    batch_task >> log_to_sumo >> delete_this_dagrun >> finish
    batch_task >> finish
