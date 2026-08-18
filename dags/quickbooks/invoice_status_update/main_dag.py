from datetime import datetime, timedelta, timezone
import rail
from airflow.models import Variable


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_quickbooks_online_{config.region.replace('-', '_')}_invoice_status_update_{config.instance}",
        description=f'QuickBooks Online {config.region} Invoice Status Update {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_dag_max_active_runs,
        multi_tenant=True
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_lastsync_time_and_current_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_lastsync_time_and_current_time',
            end_task='should_log_history',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_lastsync_time_and_current_time = rail.GetLastSyncTimeOperator(
            task_id='get_lastsync_time_and_current_time',
            workflow_name=config.workflow,
            date_format='%Y-%m-%dT%H:%M:%SZ',
            initial_sync_time=lambda: (datetime.now(
                timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'),
            provider=config.provider
        )

        def get_invoice_date_query():
            last_modified_time = rail.result('get_lastsync_time_and_current_time')[
                'last_synctime']
            return {
                'query': "SELECT * FROM Invoice WHERE MetaData.LastUpdatedTime >= '" + last_modified_time + "'"
            }
        get_query_params = rail.PythonOperator(
            task_id='get_query_params',
            python_callable=get_invoice_date_query
        )

        intuit_invoice_data = rail.InternalQuickbooksAPIOperator(
            task_id='intuit_invoice_data',
            request_method='GET',
            endpoint="/query",
            intuit_conn_id='{{ dag_run.conf.quickbooks_conn_id }}',
            query_params=lambda: rail.result('get_query_params')
        )

        update_lastsync_time = rail.SetLastSyncTimeOperator(
            task_id='update_lastsync_time',
            workflow_name=config.workflow,
            provider=config.provider,
            value_to_set="{{ result('get_lastsync_time_and_current_time').current_time }}"
        )

        def get_sent_intuit_invoice_data():
            invoice_data = rail.result('intuit_invoice_data')[
                'QueryResponse'].get('Invoice') if rail.result('intuit_invoice_data')[
                'QueryResponse'] else []
            return [x for x in invoice_data if x[
                'EmailStatus'] == 'EmailSent' and 'replicon' in x.get('PrivateNote', '')] if invoice_data else []
        parse_invoice_data = rail.PythonOperator(
            task_id='parse_invoice_data',
            python_callable=get_sent_intuit_invoice_data
        )

        get_my_actual_useridentity = rail.RepliconServiceOperator(
            task_id='get_my_actual_useridentity',
            endpoint='/services/UserAccessControlService1.svc/GetMyActualUserIdentity',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        def validate_for_polaris_permissions(response):
            view_psa_v2_permission = 'urn:replicon:psa-action:view-psa-v2'
            return any(filter(lambda item: item['permissionActionUri'] == view_psa_v2_permission, response))

        is_polaris_permissions_present = rail.RepliconServiceOperator(
            task_id='is_polaris_permissions_present',
            endpoint='/services/UserAccessControlService1.svc/GetEffectivePermissions',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                'userUri': "{{ result('get_my_actual_useridentity').uri }}"
            },
            data_handler=validate_for_polaris_permissions
        )

        has_quickbooks_invoice_data = rail.IfOperator(
            task_id='has_quickbooks_invoice_data',
            test="{{ result('parse_invoice_data') | length > 0 }}",
            yes_task='trigger_invoice_child_dag',
            no_task='should_log_history'
        )

        trigger_invoice_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_invoice_child_dag',
            retries=0,
            items=lambda: rail.result('parse_invoice_data'),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f"standard_quickbooks_online_{config.region.replace('-', '_')}_invoice_status_update_child_dag_{config.instance}",
            conf=lambda dag_run, item: {
                **dict(item.items()),
                **{
                    'is_polaris_permissions_present': rail.result('is_polaris_permissions_present'),
                    **{k: v for k, v in dag_run.conf.items() if k not in ('_ancestry', '_ecid', '_replication_position')}
                }
            }
        )

        wait_for_invoice_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_invoice_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('trigger_invoice_child_dag') }}"
        )

        gather_invoice_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_invoice_error',
            dag_runs="{{ result('trigger_invoice_child_dag') }}",
            dagrun_task_id='catch_invoice_error',
            flatten=True
        )

        is_invoice_error = rail.IfOperator(
            task_id='is_invoice_error',
            test="{{ result('gather_invoice_error') | length > 0 }}",
            yes_task='fail_invoice_error',
            no_task='should_log_history'
        )

        fail_invoice_error = rail.FailOperator(
            task_id='fail_invoice_error',
            message="{{ result('gather_invoice_error') | map_to_attr('error') | join('|') }}"
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ not(get_task_state('has_quickbooks_invoice_data') == 'success' and \
                result('has_quickbooks_invoice_data') != 'trigger_invoice_child_dag') }}",
            trigger_rule='all_done',
            yes_task='log_dagrun_details_to_table',
            no_task='delete_this_dagrun'
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_dagrun_details_to_table',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            company_key='{{ dag_run.conf.company_key }}',
            connector_name='quickbooks',
            integration_type='invoice_status_update_import'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> should_log_history
        can_run_batch_task >> rail.Label(
            'No') >> get_lastsync_time_and_current_time
        get_lastsync_time_and_current_time >> get_query_params >> intuit_invoice_data >> \
            update_lastsync_time >> parse_invoice_data >> get_my_actual_useridentity >> \
            is_polaris_permissions_present >> has_quickbooks_invoice_data
        has_quickbooks_invoice_data >> rail.Label(
            'Yes') >> trigger_invoice_child_dag >> wait_for_invoice_child_dag >> \
            gather_invoice_error >> is_invoice_error

        is_invoice_error >> rail.Label(
            'Yes') >> fail_invoice_error >> should_log_history
        is_invoice_error >> rail.Label(
            'No') >> should_log_history

        has_quickbooks_invoice_data >> rail.Label(
            'No') >> should_log_history
        should_log_history >> rail.Label(
            'Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label(
            'No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)