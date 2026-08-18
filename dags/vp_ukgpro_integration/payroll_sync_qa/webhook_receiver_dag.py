# pylint: disable=missing-module-docstring,line-too-long,pointless-statement,expression-not-assigned,import-error
from datetime import timedelta
from pendulum import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.api.common.trigger_dag import trigger_dag
import rail


def create_webhook_dag(config):
    dag_id = f'vp_ukgpro_payroll_sync_webhook_{config.instance}'

    # Function to trigger processor DAG with webhook data
    def trigger_processor(**context):
        webhook_data = context['dag_run'].conf

        # Trigger the processor DAG with the webhook data
        trigger_dag(
            dag_id=f'vp_ukgpro_payroll_sync_processor_{config.instance}',
            run_id=f"webhook__{context['dag_run'].run_id.split('__')[1]}",
            conf=webhook_data,
            execution_date=context['execution_date'],
        )

        return webhook_data

    webhook_conf = [{
        'basic_auth_username_var': None,
        'basic_auth_password_var': None,
        'bearer_token_var': None,
        'hmac_secret_var': None,
        'hmac_algorithm': None,
        'query_access_token_var': 'vantagepoint_webhook_token',
        'response_data_task_id': None,
        'trigger_condition': None,
    }]

    default_args = {
        'owner': config.instance,
        'retries': 0,
        'execution_timeout': timedelta(days=config.execution_timeout_days),
        'webhook_conf': webhook_conf,
        'replicon_conn_id': config.vp_conn_id,  # Required by webhook service
        'target_company_key': config.company_key
    }

    dag = DAG(
        dag_id=dag_id,
        description='Receives VantagePoint timesheet webhooks and triggers processing',
        start_date=dt(2025, 1, 1),
        max_active_runs=10,
        tags=['vantagepoint_ukgpro', 'payroll_sync', 'webhook'],
        default_args=default_args,
        catchup=False,
    )

    with dag:
        PythonOperator(
            task_id='trigger_processing_dag',
            python_callable=trigger_processor,
        )

    return dag


rail.for_each_instance(create_webhook_dag)
