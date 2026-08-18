from datetime import datetime, timedelta
import json
import rail
from airflow.models import Variable


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"airflow_connection_dag_{config.region.replace('-', '_')}_{config.instance}",
        description=f'Airflow Connection DAG {config.region} {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=rail.WebhookConf(
            hmac_secret_var=config.webhook_secret,
            response_data_task_id='log_connection'),
        max_active_runs=config.max_active_runs,
        start_date=datetime(2022, 1, 1),
        multi_tenant=True
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_connector'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='process_connector',
            end_task='log_connection',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        process_connector = rail.EmptyOperator(
            task_id='process_connector'
        )

        is_install_or_uninstall_connector = rail.IfOperator(
            task_id='is_install_or_uninstall_connector',
            test=lambda dag_run: dag_run.conf['webhook']['data'].get(
                'is_delete') == 'true',
            yes_task='should_revoke_client_token',
            no_task='is_create_or_archive_connector'
        )

        def check_if_client_token_to_revoke(dag_run):
            variable_value = json.loads(Variable.get(
                config.revoke_client_token_var, default_var='[]'))
            revoke_client_token_connectors = variable_value if variable_value else config.default_revoke_client_token
            connection_type = dag_run.conf['webhook']['data'].get('conn_type')
            return connection_type in revoke_client_token_connectors
        should_revoke_client_token = rail.IfOperator(
            task_id='should_revoke_client_token',
            test=check_if_client_token_to_revoke,
            yes_task='revoke_client_token',
            no_task='delete_airflow_connection'
        )

        revoke_client_token = rail.RevokeClientToken(
            task_id='revoke_client_token',
            connection_type="{{ dag_run.conf.webhook.data.conn_type }}",
            connection_id="{{ dag_run.conf.webhook.data.connection_Id }}"
        )

        delete_airflow_connection = rail.DeleteAirflowConnection(
            task_id='delete_airflow_connection',
            conn_id="{{ dag_run.conf.webhook.data.connection_Id }}"
        )

        is_create_or_archive_connector = rail.IfOperator(
            task_id='is_create_or_archive_connector',
            test="{{ dag_run.conf.webhook.data | attr_or_default('is_install', 'true') == 'true' }}",
            yes_task='create_airflow_connection',
            no_task='archive_airflow_connection'
        )

        create_airflow_connection = rail.CreateAirflowConnection(
            task_id='create_airflow_connection',
            connection_attributes=lambda dag_run: dag_run.conf[
                'webhook']['data']['connection_attributes']
        )

        archive_airflow_connection = rail.ArchiveAirflowConnection(
            task_id='archive_airflow_connection',
            connection_attributes=lambda dag_run: dag_run.conf[
                'webhook']['data']['connection_attributes']
        )

        log_connection = rail.PythonOperator(
            task_id='log_connection',
            python_callable=lambda: rail.result('create_airflow_connection') or rail.result(
                'archive_airflow_connection') or rail.result('delete_airflow_connection')
        )

        def get_action_info(dag_run):
            webhook_data = dag_run.conf['webhook']['data']
            action = 'uninstalled' if (webhook_data.get(
                'is_delete') == 'true') else 'installed'
            is_token_revoked = (action == 'uninstalled') and (rail.get_current_context()['dag_run'].get_task_instance(
                'revoke_client_token').current_state() == 'success')
            connection_id = webhook_data.get('connection_Id') or webhook_data.get(
                'connection_attributes').get('conn_id')
            company_key = (connection_id.split('_'))[2]
            return {
                'user': webhook_data.get('user_login'),
                'action': action + (" and token revoked" if is_token_revoked else ""),
                'connector': webhook_data.get('connector_type'),
                'companykey': company_key,
                'connection_id': connection_id
            }
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='none_failed',
            sumo_conn_id=config.sumo_conn_id,
            extra_info=get_action_info
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_connection

        can_run_batch_task >> rail.Label(
            'No') >> process_connector >> is_install_or_uninstall_connector

        is_install_or_uninstall_connector >> rail.Label(
            'uninstall') >> should_revoke_client_token

        should_revoke_client_token >> rail.Label(
            'Yes') >> revoke_client_token >> delete_airflow_connection

        should_revoke_client_token >> rail.Label(
            'No') >> delete_airflow_connection >> log_connection

        is_install_or_uninstall_connector >> rail.Label(
            'install') >> is_create_or_archive_connector

        is_create_or_archive_connector >> rail.Label(
            'create') >> create_airflow_connection >> log_connection

        is_create_or_archive_connector >> rail.Label(
            'archive') >> archive_airflow_connection >> log_connection
        
        log_connection >> log_to_sumo

    return dag


rail.for_each_instance(create_airflow_dag)
