from datetime import timedelta
import rail
from airflow.models import Variable
import json
from requests import post as requests_post
from airflow.exceptions import AirflowException

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag,
        description=f'GroupMPortugal Project Sync V1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,

    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_redirect_to_workato = rail.IfOperator(
            task_id='can_redirect_to_workato',
            test=lambda: Variable.get(
                config.can_redirect_to_workato_var_name, default_var='true').lower() == 'true',
            yes_task='post_to_workato',
            no_task='process_webhook_records_dag_run',
        )

        def post_data_to_workato_callable(dag_run):
            payload_data = dag_run.conf['payload']
            json_data = json.loads(payload_data)
            workato_endpoint = Variable.get(config.workato_api_endpoint)

            response = requests_post(
                workato_endpoint,
                json=json_data,
                headers={'Content-Type': 'application/json; charset=utf-8'}
            )

            if response.status_code != 200:
                raise AirflowException(f"Failed to post data to Workato. Status code: {response.status_code}, Response: {response.text}")

            return {
                "status_code": response.status_code,
                "response": response.json()
            }

        post_to_workato = rail.PythonOperator(
             task_id='post_to_workato',
             python_callable=post_data_to_workato_callable
        )

        process_webhook_records_dag_run = rail.TriggerDagRunOperator(
            task_id='process_webhook_records_dag_run',
            trigger_dag_id=config.process_webhook_records_child_dag,
            execution_timeout=timedelta(hours=config.execution_timeout_days),
            conf=lambda dag_run: {
                "payload":  json.loads(dag_run.conf['payload'])
            }
        )

        wait_for_process_webhook_records_dag_run = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_webhook_records_dag_run',
            dag_runs='{{ result("process_webhook_records_dag_run") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_redirect_to_workato >> rail.Label("Yes") >> post_to_workato >> finish
        can_redirect_to_workato >> rail.Label("No") >> process_webhook_records_dag_run

        process_webhook_records_dag_run >> wait_for_process_webhook_records_dag_run >> finish

        return dag

rail.for_each_instance(create_dag)
