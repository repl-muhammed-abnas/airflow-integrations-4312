from datetime import timedelta, datetime, timezone
import rail
from airflow.models import Variable


null = None


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_xero_connector_{config.region.replace('-', '_')}_invoice_status_update_paid_{config.instance}",
        description=f'Xero Connector {config.region} Invoice Paid Status Update {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_lastsync_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_lastsync_time',
            end_task='should_log_history',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_lastsync_time = rail.GetLastSyncTimeOperator(
            task_id='get_lastsync_time',
            workflow_name=config.workflow,
            date_format='%Y-%m-%dT%H:%M:%S',
            provider=config.provider,
            initial_sync_time=lambda: (datetime.now(
                timezone.utc) - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
        )

        get_my_actual_user_identity = rail.RepliconServiceOperator(
            task_id='get_my_actual_user_identity',
            endpoint='/services/UserAccessControlService1.svc/GetMyActualUserIdentity',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data_handler=lambda response: response['uri']
        )

        def validate_for_polaris_permissions(response):
            view_psa_v2_permission = 'urn:replicon:psa-action:view-psa-v2'
            return any(filter(lambda item: item['permissionActionUri'] == view_psa_v2_permission, response))
        is_polaris_permissions_present = rail.RepliconServiceOperator(
            task_id='is_polaris_permissions_present',
            endpoint='/services/UserAccessControlService1.svc/GetEffectivePermissions',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                'userUri': "{{ result('get_my_actual_user_identity') }}"
            },
            data_handler=validate_for_polaris_permissions
        )

        search_new_updated_invoices = rail.XeroAPIOperator(
            task_id='search_new_updated_invoices',
            xero_conn_id="{{dag_run.conf.xero_conn_id}}",
            endpoint='/api.xro/2.0/Invoices',
            request_method='GET',
            filters='''?where=STATUS=="PAID" AND SentToContact==true''',
            modified_since="{{result('get_lastsync_time').last_synctime}}"
        )

        def parse_invoices():
            invoices = rail.result('search_new_updated_invoices')['Invoices']
            return list(map(lambda invoice: {
                'invoice_number': invoice.get('Reference'),
                'client_name': invoice['Contact'].get('Name')
            }, invoices))

        parse_required_invoices = rail.PythonOperator(
            task_id='parse_required_invoices',
            python_callable=parse_invoices
        )

        has_invoice_data = rail.IfOperator(
            task_id='has_invoice_data',
            test="{{ result('parse_required_invoices') | length > 0 and result('is_polaris_permissions_present') | is_falsy}}",
            yes_task='get_invoice_detail_data',
            no_task='should_log_history'
        )

        def filter_exact_match(response, item):
            response = response['rows']
            matching_invoice = {}
            for row in response:
                if row['cells'][1].get('textValue') == item['invoice_number'] and row['cells'][2].get('textValue') == item['client_name']:
                    matching_invoice = row
                    break
            return {
                'invoice_uri': matching_invoice['cells'][0].get('uri'),
                'invoice_number': matching_invoice['cells'][1].get('textValue'),
                'client_name': matching_invoice['cells'][2].get('textValue'),
                'invoice_status': matching_invoice['cells'][3].get('uri')
            } if matching_invoice else ''

        get_invoice_detail_data = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_invoice_detail_data',
            endpoint='/services/InvoiceListService2.svc/GetData',
            items=lambda: [x for x in rail.result(
                'parse_required_invoices') if x['invoice_number']],
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:invoice2-list-column:invoice",
                    "urn:replicon:invoice2-list-column:invoice-number-text",
                    "urn:replicon:invoice2-list-column:client",
                    "urn:replicon:invoice2-list-column:invoice-status"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:invoice2-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{item.invoice_number}}",
                        },
                    }
                }
            },
            flatten=True,
            replicon_conn_id="{{dag_run.conf.replicon_conn_id}}",
            data_handler=filter_exact_match
        )

        has_matching_invoices = rail.IfOperator(
            task_id='has_matching_invoices',
            test="{{ result('get_invoice_detail_data') | remove_empty | length > 0 }}",
            yes_task='trigger_invoice_status_update_dag',
            no_task='should_log_history'
        )

        trigger_invoice_status_update_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_invoice_status_update_dag',
            retries=0,
            items=lambda: [x for x in rail.result(
                'get_invoice_detail_data') if x],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f"standard_xero_connector_{config.region.replace('-', '_')}_invoice_status_update_paid_child_dag_{config.instance}",
            conf=lambda dag_run, item: {
                **dict(item.items()),
                **{
                    k: v for k, v in dag_run.conf.items() if k not in ('_ancestry', '_ecid', '_replication_position')
                }
            }
        )

        wait_for_invoice_status_update_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_invoice_status_update_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_invoice_status_update_dag") }}'
        )

        gather_status_update_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_status_update_error',
            dag_runs="{{ result('trigger_invoice_status_update_dag') }}",
            dagrun_task_id='catch_status_update_error',
            flatten=True
        )

        is_status_update_error = rail.IfOperator(
            task_id='is_status_update_error',
            test="{{ result('gather_status_update_error') | length > 0 }}",
            yes_task='fail_status_update_error',
            no_task='should_log_history'
        )

        fail_status_update_error = rail.FailOperator(
            task_id='fail_status_update_error',
            message="{{ result('gather_status_update_error') | map_to_attr('error') | join('|') }}"
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ not((get_task_state('has_invoice_data') == 'success' and \
                result('has_invoice_data') != 'get_invoice_detail_data') or \
                (get_task_state('has_matching_invoices') == 'success' and \
                result('has_matching_invoices') != 'trigger_invoice_status_update_dag')) }}",
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
            connector_name='xero',
            integration_type='invoice_status_update_paid'
        )

        update_lastsync_time = rail.SetLastSyncTimeOperator(
            task_id='update_lastsync_time',
            provider=config.provider,
            workflow_name=config.workflow,
            value_to_set="{{result('get_lastsync_time').current_time}}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> should_log_history
        can_run_batch_task >> rail.Label(
            'No') >> get_lastsync_time
        get_lastsync_time >> get_my_actual_user_identity >> is_polaris_permissions_present >> search_new_updated_invoices
        search_new_updated_invoices >> update_lastsync_time >> parse_required_invoices
        parse_required_invoices >> has_invoice_data
        has_invoice_data >> rail.Label(
            'Yes') >> get_invoice_detail_data >> has_matching_invoices
        has_invoice_data >> rail.Label('Yes') >> should_log_history
        has_matching_invoices >> rail.Label(
            'Yes') >> trigger_invoice_status_update_dag
        trigger_invoice_status_update_dag >> wait_for_invoice_status_update_child_dag >> gather_status_update_error
        gather_status_update_error >> is_status_update_error
        is_status_update_error >> rail.Label(
            'Yes') >> fail_status_update_error >> should_log_history
        is_status_update_error >> rail.Label('No') >> should_log_history
        has_matching_invoices >> rail.Label('No') >> should_log_history
        should_log_history >> rail.Label(
            'Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label(
            'No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
