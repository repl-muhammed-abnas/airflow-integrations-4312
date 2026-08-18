from datetime import timedelta
import hashlib
import json
import hmac
import os
import pendulum
import rail
from airflow.models import Variable


# pylint: disable = too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_jira_{config.region.replace('-', '_')}_main_trigger_{config.instance}",
        description=f'Jira {config.region} Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2022, 4, 1, tz=config.timezone_iana),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs,
        multi_tenant=True
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: (Variable.get(
                config.can_run_batch_task_var_name, default_var='true') or 'true').lower() == 'true',
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
                'region': os.environ.get('REGION', 'dev')
            }
            signature = hmac.new(hmac_secret, bytes(json.dumps(
                body, separators=(",", ":")), 'utf-8'), digestmod=hashlib.sha256)
            return {'signature': signature.hexdigest(), 'request_body': json.dumps(body)}
        create_hmac_signature_and_get_request_body = rail.PythonOperator(
            task_id='create_hmac_signature_and_get_request_body',
            python_callable=get_hmac_signature_and_request_body
        )

        get_jira_company_conn_ids = rail.SimpleHttpOperator(
            task_id='get_jira_company_conn_ids',
            method='POST',
            http_conn_id=config.airflow_connector_ui_connid,
            endpoint='integration-settings-api/connector-info',
            headers={
                'Content-Type': 'application/json',
                'x-airflow-connectors-signature': "{{ result('create_hmac_signature_and_get_request_body').signature }}"
            },
            data="{{ result('create_hmac_signature_and_get_request_body').request_body }}",
        )

        parse_jira_clientids = rail.PythonOperator(
            task_id='parse_jira_clientids',
            python_callable=lambda: rail.get_connector_clientids_by_integration(rail.result(
                'get_jira_company_conn_ids'), config.workflows)
        )

        is_close_task = rail.IfOperator(
            task_id='is_close_task',
            test=lambda: len(rail.result('parse_jira_clientids')[
                             config.close_task]) > 0,
            yes_task='trigger_close_task',
            no_task='is_create_task'
        )

        trigger_close_task = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_close_task',
            trigger_dag_id=config.close_task_dag,
            retries=0,
            items=lambda: rail.result('parse_jira_clientids')[
                config.close_task],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        is_create_task = rail.IfOperator(
            task_id='is_create_task',
            test=lambda: len(rail.result('parse_jira_clientids')[
                             config.create_task]) > 0,
            yes_task='trigger_create_task',
            no_task='is_create_user'
        )

        trigger_create_task = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_create_task',
            trigger_dag_id=config.create_task_dag,
            retries=0,
            items=lambda: rail.result('parse_jira_clientids')[
                config.create_task],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        is_project_import = rail.IfOperator(
            task_id='is_project_import',
            test=lambda: len(rail.result('parse_jira_clientids')[
                             config.project_import]) > 0,
            yes_task='trigger_project_import',
            no_task='is_custom_integrations_present'
        )

        trigger_project_import = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_project_import',
            trigger_dag_id=config.project_import_dag,
            retries=0,
            items=lambda: rail.result('parse_jira_clientids')[
                config.project_import],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        # Variable holds the ISO date (YYYY-MM-DD, in config.timezone_iana) of the last day
        # create_user fired for this region+instance. The gate ensures it runs at most once per day
        # regardless of when the master is scheduled.
        create_user_last_run_date_var = (
            f"standard_jira_{config.region.replace('-', '_')}_create_user_{config.instance}_{config.company_key}_last_run_date"
        )

        def _should_run_create_user():
            if len(rail.result('parse_jira_clientids')[config.create_user]) == 0:
                return False
            today_iso = pendulum.now(config.timezone_iana).date().isoformat()
            last_run = Variable.get(create_user_last_run_date_var, default_var='')
            return today_iso != last_run

        is_create_user = rail.IfOperator(
            task_id='is_create_user',
            test=_should_run_create_user,
            yes_task='mark_create_user_run_date',
            no_task='is_project_import'
        )

        def _mark_create_user_run_date():
            Variable.set(
                create_user_last_run_date_var,
                pendulum.now(config.timezone_iana).date().isoformat()
            )

        mark_create_user_run_date = rail.PythonOperator(
            task_id='mark_create_user_run_date',
            python_callable=_mark_create_user_run_date
        )

        trigger_create_user = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_create_user',
            trigger_dag_id=config.create_user_dag,
            retries=0,
            items=lambda: rail.result('parse_jira_clientids')[
                config.create_user],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        is_custom_integrations_present = rail.IfOperator(
            task_id='is_custom_integrations_present',
            test=lambda: len(rail.result('parse_jira_clientids')[
                'custom_integrations']) > 0,
            yes_task='trigger_custom_integrations',
            no_task='should_delete_dagrun'
        )

        trigger_custom_integrations = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_custom_integrations',
            trigger_dag_id=lambda item: item['dagId'],
            retries=0,
            items=lambda: rail.result('parse_jira_clientids')[
                'custom_integrations'],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        should_delete_dagrun = rail.IfOperator(
            task_id='should_delete_dagrun',
            test="{{ get_task_state('trigger_close_task') == 'skipped' and \
                    get_task_state('trigger_create_task') == 'skipped' and \
                    get_task_state('trigger_project_import') == 'skipped' and \
                    get_task_state('trigger_create_user') == 'skipped' and \
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
        create_hmac_signature_and_get_request_body >> get_jira_company_conn_ids >> parse_jira_clientids >> is_close_task
        is_close_task >> rail.Label(
            'Yes') >> trigger_close_task >> is_create_task
        is_close_task >> rail.Label(
            'No') >> is_create_task
        is_create_task >> rail.Label(
            'Yes') >> trigger_create_task >> is_create_user
        is_create_task >> rail.Label(
            'No') >> is_create_user
        is_create_user >> rail.Label(
            'Yes') >> mark_create_user_run_date >> trigger_create_user >> is_project_import
        is_create_user >> rail.Label(
            'No') >> is_project_import
        is_project_import >> rail.Label(
            'Yes') >> trigger_project_import >> is_custom_integrations_present
        is_project_import >> rail.Label(
            'No') >> is_custom_integrations_present
        is_custom_integrations_present >> rail.Label(
            'Yes') >> trigger_custom_integrations >> should_delete_dagrun
        is_custom_integrations_present >> rail.Label(
            'No') >> should_delete_dagrun
        should_delete_dagrun >> rail.Label(
            'Yes') >> delete_this_dagrun

        return dag


rail.for_each_instance(create_main_dag)
