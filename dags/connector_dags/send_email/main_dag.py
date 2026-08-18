from datetime import datetime
import rail
from airflow.operators.email import EmailOperator


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"airflow_send_email_dag_{config.region.replace('-', '_')}_{config.instance}",
        description=f'Airflow On email send DAG {config.region} {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=rail.WebhookConf(
            hmac_secret_var=config.webhook_secret,
            response_data_task_id='is_success_send_email'
        ),
        start_date=datetime(2022, 1, 1),
        max_active_runs=config.max_active_runs,
        multi_tenant=True
    ) as dag:
        

        send_success_email = EmailOperator(
            task_id='send_success_email',
            to=config.internal_emails,
            subject='{{dag_run.conf.webhook.data.connector_name}}_Request_raised',
            html_content="template/email.html",
        )

        is_success_send_email= rail.PythonOperator(
            task_id="is_success_send_email",
            trigger_rule="none_failed",
            python_callable= lambda: True
        )

       
        send_success_email >> is_success_send_email 

    return dag

rail.for_each_instance(create_airflow_dag)



