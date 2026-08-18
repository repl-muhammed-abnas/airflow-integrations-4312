from datetime import timedelta
import hashlib
import json
import hmac
import os
import pendulum
import rail
from airflow.models import Variable
# pylint: disable=too-many-statements


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/bamboohr/main_dag/config.py


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_bamboohr_{config.region.replace('-', '_')}_main_trigger_{config.instance}",
        description=f'BambooHR {config.region} Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2022, 4, 1, tz=config.timezone_iana),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_hmac_signature_and_get_request_body'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_hmac_signature_and_get_request_body',
            end_task='should_delete_dagrun',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def get_hmac_signature_and_request_body():
            hmac_secret = bytes(Variable.get(config.hmac_secret), 'utf-8')
            body = {
                'connectorName': config.connector_name,
                'environment': os.environ.get('AIRFLOW_ENVIRONMENT', 'dev'),
                'region': os.environ.get('REGION', 'unknown')
            }
            signature = hmac.new(hmac_secret, bytes(json.dumps(
                body, separators=(",", ":")), 'utf-8'), digestmod=hashlib.sha256)
            return {'signature': signature.hexdigest(), 'request_body': json.dumps(body)}
        create_hmac_signature_and_get_request_body = rail.PythonOperator(
            task_id='create_hmac_signature_and_get_request_body',
            python_callable=get_hmac_signature_and_request_body
        )

        get_bamboohr_company_conn_ids = rail.SimpleHttpOperator(
            task_id='get_bamboohr_company_conn_ids',
            method='POST',
            http_conn_id=config.airflow_connector_ui_connid,
            endpoint='integration-settings-api/connector-info',
            headers={
                'Content-Type': 'application/json',
                'x-airflow-connectors-signature': "{{ result('create_hmac_signature_and_get_request_body').signature }}"
            },
            data="{{ result('create_hmac_signature_and_get_request_body').request_body }}"
        )

        def get_bamboohr_clientids_by_integration():
            null = None
            user_import_list = []
            disable_user_list = []
            custom_integrations = []

            # Load bamboohr company connections
            bamboohr_company_connids = json.loads(
                rail.result('get_bamboohr_company_conn_ids'))

            # Define workflow IDs
            user_import = config.user_import
            disable_user = config.disable_user

            # Iterate through bamboohr company connections
            for each_client in bamboohr_company_connids:
                # add default notification email if not present
                if not each_client.get('notification_email'):
                    each_client['notification_email'] = f"rit.internallogs+{each_client['company_key']}@replicon.com"
                # Extract dag settings
                dag_settings = each_client.pop('dag_settings')

                if dag_settings:
                    # Handle user import workflow
                    user_import_workflow = next(
                        iter(filter(lambda x: not (x.get('isCustom')) and x['workflowId'] == user_import and x['enabled'].lower() == 'yes', dag_settings)), null)
                    if user_import_workflow:
                        user_import_list.append(
                            {**each_client, 'customSettings': user_import_workflow['customSettings']})
                    # Handle disable user workflow
                    disable_user_workflow = next(
                        iter(filter(lambda x: not (x.get('isCustom')) and x['workflowId'] == disable_user and x['enabled'].lower() == 'yes',
                                    dag_settings)), null)
                    if disable_user_workflow:
                        disable_user_list.append(
                            {**each_client, 'customSettings': disable_user_workflow['customSettings']})
                    # Handle custom integration workflows
                    custom_integration_workflows = map(lambda y, ec=each_client: {**ec, 'dagId': y['isCustom'],
                                                                                  'customSettings': y['customSettings']}, filter(lambda x: x.get('isCustom') and x['enabled'].lower() == 'yes', dag_settings))
                    if custom_integration_workflows:
                        custom_integrations.extend(
                            custom_integration_workflows)

            return {
                f'{user_import}': user_import_list,
                f'{disable_user}': disable_user_list,
                'custom_integrations': custom_integrations
            }

        parse_bamboohr_clientids = rail.PythonOperator(
            task_id='parse_bamboohr_clientids',
            python_callable=lambda: rail.get_connector_clientids_by_integration(rail.result(
                'get_bamboohr_company_conn_ids'), config.workflows)
        )

        is_user_import = rail.IfOperator(
            task_id='is_user_import',
            test=lambda: len(rail.result('parse_bamboohr_clientids')[
                             config.user_import]) > 0,
            yes_task='trigger_user_import',
            no_task='is_disable_user'
        )

        trigger_user_import = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_user_import',
            trigger_dag_id=config.user_import_dag,
            retries=0,
            items=lambda: rail.result('parse_bamboohr_clientids')[
                config.user_import],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        is_disable_user = rail.IfOperator(
            task_id='is_disable_user',
            test=lambda: len(rail.result('parse_bamboohr_clientids')[
                             config.disable_user]) > 0,
            yes_task='trigger_disable_user',
            no_task='is_custom_integrations_present'
        )

        trigger_disable_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_disable_user',
            trigger_dag_id=config.disable_user_dag,
            retries=0,
            items=lambda: rail.result('parse_bamboohr_clientids')[
                config.disable_user],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        is_custom_integrations_present = rail.IfOperator(
            task_id='is_custom_integrations_present',
            test=lambda: len(rail.result('parse_bamboohr_clientids')[
                'custom_integrations']) > 0,
            yes_task='trigger_custom_integrations',
            no_task='should_delete_dagrun'
        )

        trigger_custom_integrations = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_custom_integrations',
            trigger_dag_id=lambda item: item['dagId'],
            retries=0,
            items=lambda: rail.result('parse_bamboohr_clientids')[
                'custom_integrations'],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        should_delete_dagrun = rail.IfOperator(
            task_id='should_delete_dagrun',
            test="{{ get_task_state('trigger_user_import') == 'skipped' and \
                    get_task_state('trigger_disable_user') == 'skipped' and \
                    get_task_state('trigger_custom_integrations') == 'skipped' }}",
            trigger_rule='all_done',
            yes_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> should_delete_dagrun
        can_run_batch_task >> rail.Label(
            'No') >> create_hmac_signature_and_get_request_body
        create_hmac_signature_and_get_request_body >> get_bamboohr_company_conn_ids >> parse_bamboohr_clientids >> \
            is_user_import
        is_user_import >> rail.Label(
            'Yes') >> trigger_user_import >> is_disable_user
        is_user_import >> rail.Label(
            'No') >> is_disable_user
        is_disable_user >> rail.Label(
            'Yes') >> trigger_disable_user
        trigger_disable_user >> is_custom_integrations_present
        is_disable_user >> rail.Label(
            'No') >> is_custom_integrations_present
        is_custom_integrations_present >> rail.Label(
            'Yes') >> trigger_custom_integrations >> should_delete_dagrun
        is_custom_integrations_present >> rail.Label(
            'No') >> should_delete_dagrun
        should_delete_dagrun >> rail.Label(
            'Yes') >> delete_this_dagrun

        return dag


rail.for_each_instance(create_main_dag)
