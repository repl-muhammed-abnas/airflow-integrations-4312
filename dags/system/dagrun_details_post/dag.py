"""
Replicon dag to post the dagrun details
"""
from datetime import datetime, timedelta
import json
import hashlib
import hmac
import airflow
from airflow.models import Variable
from system.dagrun_details_post import config
import rail

with airflow.DAG(
    dag_id="system_dagrun_details_to_post",
    start_date=datetime(2023, 1, 1),
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
    max_active_runs=config.max_active_runs,
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
) as dag:

    batch_task = rail.BatchTaskRunOperator(
        task_id='batch_task',
        start_task='create_hmac_signature',
        end_task='finish',
        execution_timeout=timedelta(days=config.execution_timeout_days),
    )

    def create_hmac_header():
        conf = rail.get_current_context()['dag_run'].conf
        hmac_secret = bytes(Variable.get(conf['hmac_secret_var']), 'utf-8')
        body = conf['data']
        signature = hmac.new(hmac_secret, bytes(json.dumps(
            body, separators=(",", ":")), 'utf-8'), digestmod=hashlib.sha256)
        return signature.hexdigest()
    create_hmac_signature = rail.PythonOperator(
        task_id='create_hmac_signature',
        python_callable=create_hmac_header
    )

    def post_dagrun_details_to_middleware():
        conf = rail.get_current_context()['dag_run'].conf
        send = rail.SimpleHttpOperator(
            task_id='post_to_middleware',
            http_conn_id=conf['airflow_connector_ui_connid'],
            method='POST',
            endpoint='integration-settings-api/dagrun-history',
            headers={
                "Content-Type": 'application/json; charset=utf-8',
                "x-airflow-connectors-signature": rail.result('create_hmac_signature')
            },
            data=json.dumps(conf['data'])
        )
        send.execute(rail.get_current_context())

    post_to_middleware = rail.PythonOperator(
        task_id='post_to_middleware',
        python_callable=post_dagrun_details_to_middleware
    )

    finish = rail.EmptyOperator(
        task_id='finish'
    )

    batch_task >> create_hmac_signature >> post_to_middleware >> finish
    batch_task >> finish
