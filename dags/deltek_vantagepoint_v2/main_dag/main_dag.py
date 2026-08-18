from datetime import timedelta, datetime
import hashlib
import json
import hmac
import os
import pendulum
import rail
from airflow.models import Variable
from deltek_vantagepoint_v2.main_dag.utils import get_connector_clientids_with_initial_settings
# pylint: disable=too-many-statements


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/deltek_vantagepoint/main_dag/config.py


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_deltek_vantagepoint_{config.region.replace('-', '_')}_main_trigger_{config.instance}",
        description=f'Deltek VantagePoint {config.region} Master {config.instance}',
        company_key=config.company_key,
        start_date=pendulum.datetime(2022, 4, 1, tz=config.timezone_iana),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs,
        multi_tenant=True
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

        get_vantagepoint_company_conn_ids = rail.SimpleHttpOperator(
            task_id='get_vantagepoint_company_conn_ids',
            method='POST',
            http_conn_id=config.airflow_connector_ui_connid,
            endpoint='integration-settings-api/connector-info',
            headers={
                'Content-Type': 'application/json',
                'x-airflow-connectors-signature': "{{ result('create_hmac_signature_and_get_request_body').signature }}"
            },
            data="{{ result('create_hmac_signature_and_get_request_body').request_body }}"
        )

        parse_vantagepoint_clientids = rail.PythonOperator(
            task_id='parse_vantagepoint_clientids',
            python_callable=lambda: get_connector_clientids_with_initial_settings(rail.result(
                'get_vantagepoint_company_conn_ids'), config.workflows)
        )

        def get_overdue_interval_clients(workflow_key, variable_name_prefix, interval_config_name):
            client_ids = rail.result('parse_vantagepoint_clientids').get(workflow_key, [])
            if not client_ids:
                return []

            interval_hours = getattr(config, interval_config_name, 24)
            current_dt = datetime.now()
            overdue = []

            for client in client_ids:
                company_key = client.get('company_key', config.company_key)
                variable_name = f'{variable_name_prefix}_{company_key}'
                last_run_str = Variable.get(variable_name, default_var=None)

                if not last_run_str:
                    overdue.append(client)
                    continue

                try:
                    last_run_dt = datetime.fromisoformat(last_run_str)
                    hours_since_last_run = (current_dt - last_run_dt).total_seconds() / 3600
                    if hours_since_last_run >= interval_hours:
                        overdue.append(client)
                except (ValueError, TypeError):
                    overdue.append(client)

            return overdue

        check_initial_setup_schedule = rail.IfOperator(
            task_id='check_initial_setup_schedule',
            test=lambda: len(get_overdue_interval_clients(
                config.initial_setup, config.initial_setup_last_run_var, 'initial_setup_interval_hours'
            )) > 0,
            yes_task='trigger_initial_setup',
            no_task='check_timecategory_sync_schedule'
        )

        trigger_initial_setup = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_initial_setup',
            trigger_dag_id=config.initial_setup_dag_id,
            retries=0,
            items=lambda: get_overdue_interval_clients(
                config.initial_setup, config.initial_setup_last_run_var, 'initial_setup_interval_hours'
            ),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        check_timecategory_sync_schedule = rail.IfOperator(
            task_id='check_timecategory_sync_schedule',
            test=lambda: len(get_overdue_interval_clients(
                config.initial_setup, config.timecategory_sync_last_run_var, 'timecategory_sync_interval_hours'
            )) > 0,
            yes_task='trigger_user_timecategory_sync',
            no_task='is_user_sync'
        )

        trigger_user_timecategory_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_user_timecategory_sync',
            trigger_dag_id=config.timecategory_sync_dag_id,
            items=lambda: get_overdue_interval_clients(
                config.initial_setup, config.timecategory_sync_last_run_var, 'timecategory_sync_interval_hours'
            ),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        def get_initial_clients_var(workflow_key, var):
            return [
                client for client in rail.result('parse_vantagepoint_clientids').get(workflow_key, [])
                if Variable.get(
                    f'{var}_{client.get("company_key", config.company_key)}',
                    default_var='true'
                ).lower() == 'true'
            ]

        is_user_sync = rail.IfOperator(
            task_id='is_user_sync',
            test=lambda: len(get_initial_clients_var(config.user_sync, config.user_sync_initial_run_var)) > 0,
            yes_task='trigger_user_sync',
            no_task='is_project_sync'
        )


        trigger_user_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_user_sync',
            trigger_dag_id=config.user_sync_dag_id,
            retries=0,
            items=lambda: get_initial_clients_var(config.user_sync, config.user_sync_initial_run_var),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        is_project_sync = rail.IfOperator(
            task_id='is_project_sync',
            test=lambda: len(get_initial_clients_var(config.project_sync, config.is_project_full_sync_var)) > 0,
            yes_task='trigger_project_sync',
            no_task='is_timesheet_sync'
        )

        trigger_project_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_project_sync',
            trigger_dag_id=config.project_sync_main_dag_id,
            retries=0,
            items=lambda: get_initial_clients_var(config.project_sync, config.is_project_full_sync_var),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        is_timesheet_sync = rail.IfOperator(
            task_id='is_timesheet_sync',
            test=lambda: len(rail.result('parse_vantagepoint_clientids')[
                             config.timesheet_sync]) > 0,
            yes_task='trigger_timesheet_sync',
            no_task='is_custom_integrations_present'
        )

        trigger_timesheet_sync = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_timesheet_sync',
            trigger_dag_id=config.timesheet_sync_main_dag_id,
            retries=0,
            items=lambda: rail.result('parse_vantagepoint_clientids')[
                config.timesheet_sync],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        is_custom_integrations_present = rail.IfOperator(
            task_id='is_custom_integrations_present',
            test=lambda: len(rail.result('parse_vantagepoint_clientids')[
                'custom_integrations']) > 0,
            yes_task='trigger_custom_integrations',
            no_task='should_delete_dagrun'
        )

        trigger_custom_integrations = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_custom_integrations',
            trigger_dag_id=lambda item: item['dagId'],
            retries=0,
            items=lambda: rail.result('parse_vantagepoint_clientids')[
                'custom_integrations'],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        should_delete_dagrun = rail.IfOperator(
            task_id='should_delete_dagrun',
            test="{{ get_task_state('trigger_initial_setup') == 'skipped' and \
                    get_task_state('trigger_user_sync') == 'skipped' and \
                    get_task_state('trigger_project_sync') == 'skipped' and \
                    get_task_state('trigger_timesheet_sync') == 'skipped' and \
                    get_task_state('trigger_user_timecategory_sync') == 'skipped' and \
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
        create_hmac_signature_and_get_request_body >> get_vantagepoint_company_conn_ids >> parse_vantagepoint_clientids >> check_initial_setup_schedule
        check_initial_setup_schedule >> rail.Label('Yes') >> trigger_initial_setup >> check_timecategory_sync_schedule
        check_initial_setup_schedule >> rail.Label('No') >> check_timecategory_sync_schedule

        check_timecategory_sync_schedule >> rail.Label('Yes') >> trigger_user_timecategory_sync >> is_user_sync
        check_timecategory_sync_schedule >> rail.Label('No') >> is_user_sync
        is_user_sync >> rail.Label(
            'Yes') >> trigger_user_sync >> is_project_sync
        is_user_sync >> rail.Label(
            'No') >> is_project_sync
        is_project_sync >> rail.Label('Yes') >> trigger_project_sync >> is_timesheet_sync
        is_project_sync >> rail.Label('No') >> is_timesheet_sync
        is_timesheet_sync >> rail.Label(
            'Yes') >> trigger_timesheet_sync >> is_custom_integrations_present
        is_timesheet_sync >> rail.Label(
            'No') >> is_custom_integrations_present
        is_custom_integrations_present >> rail.Label(
            'Yes') >> trigger_custom_integrations >> should_delete_dagrun
        is_custom_integrations_present >> rail.Label(
            'No') >> should_delete_dagrun
        should_delete_dagrun >> rail.Label(
            'Yes') >> delete_this_dagrun

        return dag


rail.for_each_instance(create_main_dag)
