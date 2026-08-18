"""
### System Maintainence DAG to disable out-of-box (OOB) Airflow connectors.

#### Purpose:
- Retrieves OOB connectors workflow states.
- Filters enabled workflows to disable based on the retrieved DAG settings.
- Iterates over the workflows to disable and sends a request to disable each workflow.
- Sends an email notification upon completion.

"""


from datetime import datetime, timedelta

import json
import hashlib
import hmac
import airflow

from airflow.models import Variable

import rail

from system.oob_connector_maintainence import config


with airflow.DAG(
    dag_id='system_disable_airflow_connectors',
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['airflow_connectors_maintenance'],
    is_paused_upon_creation=True,
    doc_md=__doc__,
    max_active_runs=1,
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
    default_args={
        'owner': 'system',
        'depends_on_past': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
) as dag:

    batch_task = rail.BatchTaskRunOperator(
        task_id="batch_task",
        start_task="create_get_connector_workflow_states_request",
        end_task="wait_for_disable_workflow_dag",
        execution_timeout=timedelta(minutes=config.execution_timeout_minutes),
    )

    def get_create_get_connector_workflow_states_request():
        hmac_secret = bytes(Variable.get(config.hmac_secret), 'utf-8')
        dag_run_conf = rail.get_current_context()['dag_run'].conf
        body = {
            'connectorName': dag_run_conf['connector_name'],
            'companyKey': dag_run_conf['company_key'],
            'key': 'dagSettings'
        }
        signature = hmac.new(hmac_secret, bytes(json.dumps(
            body, separators=(",", ":")), 'utf-8'), digestmod=hashlib.sha256)
        return {'signature': signature.hexdigest(), 'query_params': body}

    create_get_connector_workflow_states_request = rail.PythonOperator(
        task_id='create_get_connector_workflow_states_request',
        python_callable=get_create_get_connector_workflow_states_request
    )

    get_connector_workflow_dag_settings = rail.SimpleHttpOperator(
        task_id='get_connector_workflow_dag_settings',
        method='GET',
        http_conn_id=config.airflow_connector_ui_connid,
        endpoint='integration-settings-api/dag-configuration',
        headers={
            'Content-Type': 'application/json',
            'x-airflow-connectors-signature': "{{ result('create_get_connector_workflow_states_request').signature }}"
        },
        data={
            'connectorName': "{{ result('create_get_connector_workflow_states_request') | attr_or_default('query_params.connectorName') }}",
            'companyKey': "{{ result('create_get_connector_workflow_states_request') | attr_or_default('query_params.companyKey') }}",
            'key': "{{ result('create_get_connector_workflow_states_request') | attr_or_default('query_params.key') }}"
        }
    )

    def get_workflows_to_disable():
        dag_run_conf = rail.get_current_context()['dag_run'].conf
        workflow_dag_settings = json.loads(
            rail.result('get_connector_workflow_dag_settings'))
        return list(filter(lambda x: x['current_status'] == 'Yes', map(lambda item: {
            'company_key': dag_run_conf['company_key'],
            'connector_name': dag_run_conf['connector_name'],
            'workflow_id': item['workflowId'],
            'current_status': item['enabled'],
            'install_type': dag_run_conf['install_type'],
            'swimlane': dag_run_conf['swimlane']
        }, workflow_dag_settings)))

    workflows_to_disable = rail.PythonOperator(
        task_id='workflows_to_disable',
        python_callable=get_workflows_to_disable
    )

    is_workflows_to_disable = rail.IfOperator(
        task_id='is_workflows_to_disable',
        test="{{ result('workflows_to_disable') | length > 0 }}",
        yes_task='trigger_disable_workflow_dag'
    )

    trigger_disable_workflow_dag = rail.TriggerDagRunForEachItemOperator(
        task_id='trigger_disable_workflow_dag',
        items=lambda: rail.result('workflows_to_disable'),
        execution_timeout=timedelta(minutes=config.execution_timeout_minutes),
        retries=0,
        trigger_dag_id='system_child_disable_airflow_connector_workflows',
        conf=lambda dag_run, item: {
            **{k: v for k, v in item.items() if k in ('company_key', 'connector_name', 'workflow_id')},
            **{
                'region_environment': dag_run.conf['region_environment'],
                'status': 'No'
            }
        }
    )

    wait_for_disable_workflow_dag = rail.WaitForDagRunsSensor(
        task_id='wait_for_disable_workflow_dag',
        retries=0,
        execution_timeout=timedelta(minutes=config.execution_timeout_minutes),
        dag_runs='{{ result("trigger_disable_workflow_dag") }}'
    )

    batch_task >> rail.Label(
        'Batch Task') >> create_get_connector_workflow_states_request >> get_connector_workflow_dag_settings >> \
        workflows_to_disable >> is_workflows_to_disable

    is_workflows_to_disable >> rail.Label(
        'Yes') >> trigger_disable_workflow_dag >> wait_for_disable_workflow_dag

    batch_task >> rail.Label(
        'Finish') >> wait_for_disable_workflow_dag
