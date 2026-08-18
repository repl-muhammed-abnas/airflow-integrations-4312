"""
### System Maintainence DAG to disable out-of-box (OOB) Airflow Workflows based on connector name. This can be triggered manually with conf.

#### Purpose:
- Disable each workflow based on connector name, company key and workflow ID.

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
    dag_id='system_child_disable_airflow_connector_workflows',
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

    def get_create_workflow_disable_request_body():
        dag_run_conf = rail.get_current_context()['dag_run'].conf
        hmac_secret = bytes(Variable.get(config.hmac_secret), 'utf-8')
        body = {
            'connectorName': dag_run_conf['connector_name'],
            'companyKey': dag_run_conf['company_key'],
            'dagId': dag_run_conf['workflow_id'],
            'status': dag_run_conf['status'],
            'key': 'dagSettings'
        }
        signature = hmac.new(hmac_secret, bytes(json.dumps(
            body, separators=(",", ":")), 'utf-8'), digestmod=hashlib.sha256)
        return {'signature': signature.hexdigest(), 'request_body': json.dumps(body)}
    create_workflow_disable_request_body = rail.PythonOperator(
        task_id='create_workflow_disable_request_body',
        python_callable=get_create_workflow_disable_request_body
    )

    disable_workflow = rail.SimpleHttpOperator(
        task_id='disable_workflow',
        method='POST',
        http_conn_id=config.airflow_connector_ui_connid,
        endpoint='integration-settings-api/dag-configuration',
        headers={
            'Content-Type': 'application/json',
            'x-airflow-connectors-signature': "{{ result('create_workflow_disable_request_body').signature }}"
        },
        data="{{ result('create_workflow_disable_request_body').request_body }}"
    )

    create_workflow_disable_request_body >> disable_workflow
