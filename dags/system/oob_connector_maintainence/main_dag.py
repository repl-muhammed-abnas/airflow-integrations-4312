"""
### System Maintainence DAG for disabling and uninstalling out-of-box (OOB) Airflow connectors.

#### Purpose:
- Retrieves OOB connectors by company key.
- Identifies expired or deleted Replicon tenants.
- Triggers workflows to disable connectors for expired tenants.
- Initiates the uninstallation process for deleted tenants.
- Sends an email notification upon completion.

"""

from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import urlparse

import os
import re
import requests
import airflow

from airflow.models import Connection
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.operators.email import EmailOperator

import rail
from rail.hooks.replicon_hook import RepliconHook

from system.oob_connector_maintainence import config

VALID_COMPANY_KEY_PATTERN = re.compile(r'^[a-zA-Z0-9]+$')


with airflow.DAG(
    dag_id='system_disable_uninstall_airflow_connectors',
    schedule='0 0 */2 * *',  # Midnight Every 2 days
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

    @provide_session
    def get_filter_connectors_by_companykey(session=NEW_SESSION):

        mapped_company_keys = defaultdict()
        query_connection_ids = session.query(
            Connection.conn_id, Connection.host).filter(
                Connection.conn_id.like("standard_%")).filter(
                Connection.conn_type == RepliconHook.conn_type).filter(
                Connection.host.in_(config.filter_global_urls)).all()

        if query_connection_ids:
            for replicon_connection, global_url in query_connection_ids:
                parts = replicon_connection.split('_')
                if len(parts) < 4:
                    continue
                install_type = parts[0]
                prefix_connection = parts[1]
                company_key = '_'.join(parts[2:-1])
                if prefix_connection not in config.prefix_name_mapping:
                    continue
                connection_data = {
                    "global_url": global_url,
                    "connection_id": replicon_connection,
                    "connection_type": RepliconHook.conn_type,
                    "install_type": install_type,
                    "prefix_connection": prefix_connection
                }
                if not mapped_company_keys.get(company_key):
                    mapped_company_keys[company_key] = [connection_data]
                    continue
                mapped_company_keys[company_key].append(connection_data)

        rail.set_result(
            key="region-environment", val=f"{os.environ.get('REGION', 'unknown')}-{os.environ.get('AIRFLOW_ENVIRONMENT', 'dev')}")
        return mapped_company_keys

    filter_connectors_by_companykey = rail.PythonOperator(
        task_id="filter_connectors_by_companykey",
        python_callable=get_filter_connectors_by_companykey
    )

    is_filter_connectors_by_companykey = rail.IfOperator(
        task_id='is_filter_connectors_by_companykey',
        test="{{ result('filter_connectors_by_companykey') | sn | is_truthy }}",
        yes_task='disable_uninstall_connectors_list'
    )

    def get_workflows_to_disable_and_uninstall():
        disable_connectors = []
        uninstall_connectors = []
        for company_key, conn_details in rail.result('filter_connectors_by_companykey').items():
            global_url = conn_details[0]['global_url']
            if not VALID_COMPANY_KEY_PATTERN.match(company_key):
                for each_connection in conn_details:
                    uninstall_connectors.append({
                        'company_key': company_key,
                        'install_type': each_connection['install_type'],
                        'connector_name': config.prefix_name_mapping[each_connection['prefix_connection']]
                    })
                continue
            try:
                discovery_url = global_url + '/DiscoveryService1.svc/GetTenantEndpointDetails'
                response = requests.post(discovery_url, json={
                                         'tenant': {'companyKey': company_key}}, timeout=60)
                if response.status_code != 200:
                    raise Exception(response.text)
                deserialised_json = response.json()["d"]
                app_url = deserialised_json["applicationRootUrl"]
                swimlane = urlparse(app_url).hostname.split('.')[0]
                is_enabled = deserialised_json["tenant"]["isEnabled"]
                if not is_enabled:
                    for each_connection in conn_details:
                        disable_connectors.append({
                            'company_key': company_key,
                            'swimlane': swimlane,
                            'install_type': each_connection['install_type'],
                            'connector_name': config.prefix_name_mapping[each_connection['prefix_connection']]
                        })
            except Exception as _ex:  # pylint:disable=broad-except
                # check error correctly for deleted tenants in replicon
                for each_connection in conn_details:
                    uninstall_connectors.append({
                        'company_key': company_key,
                        'install_type': each_connection['install_type'],
                        'connector_name': config.prefix_name_mapping[each_connection['prefix_connection']]
                    })
        rail.set_result(uninstall_connectors, 'uninstall_connectors')
        return disable_connectors

    disable_uninstall_connectors_list = rail.PythonOperator(
        task_id='disable_uninstall_connectors_list',
        python_callable=get_workflows_to_disable_and_uninstall
    )

    is_disable_connectors = rail.IfOperator(
        task_id='is_disable_connectors',
        test="{{ result('disable_uninstall_connectors_list') | length > 0 }}",
        yes_task='trigger_disable_connectors_dag'
    )

    trigger_disable_connectors_dag = rail.TriggerDagRunForEachItemOperator(
        task_id='trigger_disable_connectors_dag',
        items=lambda: rail.result('disable_uninstall_connectors_list'),
        execution_timeout=timedelta(minutes=config.execution_timeout_minutes),
        retries=0,
        trigger_dag_id='system_disable_airflow_connectors',
        conf=lambda item: {
            'region_environment': rail.result('filter_connectors_by_companykey', 'region-environment'),
            **dict(item.items())
        }
    )

    wait_for_workflow_disable_dag = rail.WaitForDagRunsSensor(
        task_id='wait_for_workflow_disable_dag',
        retries=0,
        execution_timeout=timedelta(minutes=config.execution_timeout_minutes),
        dag_runs='{{ result("trigger_disable_connectors_dag") }}'
    )

    gather_workflows_to_disable = rail.GatherResultsFromDagRunsOperator(
        task_id='gather_workflows_to_disable',
        execution_timeout=timedelta(minutes=config.execution_timeout_minutes),
        dag_runs="{{ result('trigger_disable_connectors_dag') }}",
        dagrun_task_id='workflows_to_disable',
        flatten=True
    )

    is_workflows_to_disable = rail.IfOperator(
        task_id='is_workflows_to_disable',
        test="{{ result('gather_workflows_to_disable') | length > 0 }}",
        yes_task='send_disable_email'
    )

    send_disable_email = EmailOperator(
        task_id='send_disable_email',
        to=config.alert_email,
        subject="{{ result('filter_connectors_by_companykey', 'region-environment') }} | Automation System disabled workflows" +
        " at {{ current_time_in_specified_tz('Asia/Calcutta') }}",
        html_content="disable_workflow_email_template.html"
    )

    is_uninstall_workflows = rail.IfOperator(
        task_id='is_uninstall_workflows',
        test="{{ result('disable_uninstall_connectors_list', 'uninstall_connectors') | length > 0 }}",
        yes_task='trigger_uninstall_connector_dag'
    )

    trigger_uninstall_connector_dag = rail.TriggerDagRunForEachItemOperator(
        task_id='trigger_uninstall_connector_dag',
        retries=0,
        trigger_dag_id='system_uninstall_airflow_connectors',
        items=lambda: rail.result(
            'disable_uninstall_connectors_list', 'uninstall_connectors'),
        execution_timeout=timedelta(minutes=config.execution_timeout_minutes),
        conf=lambda item: {
            **dict(item.items()),
            'region_environment': rail.result('filter_connectors_by_companykey', 'region-environment')
        }
    )

    wait_for_uninstall_connector = rail.WaitForDagRunsSensor(
        task_id='wait_for_uninstall_connector',
        retries=0,
        execution_timeout=timedelta(minutes=config.execution_timeout_minutes),
        dag_runs='{{ result("trigger_uninstall_connector_dag") }}'
    )

    send_uninstall_email = EmailOperator(
        task_id='send_uninstall_email',
        to=config.alert_email,
        subject="{{ result('filter_connectors_by_companykey', 'region-environment') }} | Automation System uninstalled OOB Connectors " +
        " at {{ current_time_in_specified_tz('Asia/Calcutta') }}",
        html_content="uninstall_workflow_email_template.html"
    )

    filter_connectors_by_companykey >> is_filter_connectors_by_companykey >> rail.Label(
        'Yes') >> disable_uninstall_connectors_list

    disable_uninstall_connectors_list >> is_disable_connectors >> rail.Label(
        'Yes') >> trigger_disable_connectors_dag >> wait_for_workflow_disable_dag >> gather_workflows_to_disable >> \
        is_workflows_to_disable >> rail.Label(
        'Send Email') >> send_disable_email

    disable_uninstall_connectors_list >> is_uninstall_workflows >> rail.Label(
        'Yes') >> trigger_uninstall_connector_dag >> wait_for_uninstall_connector >> rail.Label(
        'Send Email') >> send_uninstall_email
