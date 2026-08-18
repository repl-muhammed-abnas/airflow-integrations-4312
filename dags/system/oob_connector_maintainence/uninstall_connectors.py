"""
### System Maintainence DAG to Uninstall out-of-box (OOB) Airflow connectors. This can be triggered manually with conf.

#### Purpose:
- Sends a request to uninstall each connector based on the DAG Run Conf.

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
    dag_id='system_uninstall_airflow_connectors',
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

    def get_hmac_signature_and_request_body():
        dag_run_conf = rail.get_current_context()['dag_run'].conf
        hmac_secret = bytes(Variable.get(config.hmac_secret), 'utf-8')
        body = {
            'connectorName': dag_run_conf['connector_name'],
            'companyKey': dag_run_conf['company_key']
        }
        signature = hmac.new(hmac_secret, bytes(json.dumps(
            body, separators=(",", ":")), 'utf-8'), digestmod=hashlib.sha256)
        return {'signature': signature.hexdigest(), 'request_body': json.dumps(body)}

    create_hmac_signature_and_get_request_body = rail.PythonOperator(
        task_id='create_hmac_signature_and_get_request_body',
        python_callable=get_hmac_signature_and_request_body
    )

    uninstall_connector = rail.SimpleHttpOperator(
        task_id='uninstall_connector',
        method='POST',
        http_conn_id=config.airflow_connector_ui_connid,
        endpoint='integration-settings-api/uninstall-connector',
        headers={
            'Content-Type': 'application/json',
            'x-airflow-connectors-signature': "{{ result('create_hmac_signature_and_get_request_body').signature }}"
        },
        data="{{ result('create_hmac_signature_and_get_request_body').request_body }}"
    )

    create_hmac_signature_and_get_request_body >> uninstall_connector
