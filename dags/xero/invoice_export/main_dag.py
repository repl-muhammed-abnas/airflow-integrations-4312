from datetime import datetime, timedelta, timezone
import itertools
import rail
from airflow.models import Variable
from xero.invoice_export.request_payload import get_queuedforsync_invoice


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_xero_connector_{config.region.replace('-', '_')}_invoice_export_{config.instance}",
        description=f'Xero Online {config.region} Invoice Export {config.instance}',
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
            provider=config.provider,
            date_format='%Y-%m-%dT%H:%M:%S',
            initial_sync_time=lambda: (datetime.now(
                timezone.utc) - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
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

        get_sync_status_filter_definition = rail.RepliconServiceOperator(
            task_id='get_sync_status_filter_definition',
            endpoint='/services/InvoiceListService2.svc/GetAllFilterDefinitions',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data_handler=lambda response: next(
                iter(filter(lambda x: x['name'] == 'Sync Status', response)), {}).get('uri', '')
        )

        def get_date_string(dateobj):
            return f"{dateobj.get('year')}-{dateobj.get('month')}-{dateobj.get('day')}"

        def get_time_string(timeobj):
            return f"{timeobj.get('hour')}:{timeobj.get('minute')}:{timeobj.get('second')}"

        def page_handler(request, response):
            if len(response['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def get_invoice_detail(row, ignored_data_types):
            try:
                return {
                    'invoice': row['cells'][0]['uri'],
                    'invoice_number': row['cells'][1]['textValue'],
                    'client': {
                        'textValue': row['cells'][2]['textValue'],
                        'uri': row['cells'][2]['uri']
                    },
                    'creation_datetime': get_date_string(row['cells'][3]['dateValue']),
                    'last_modified_datetime': {
                        'date': get_date_string(row['cells'][4]['dateValue']),
                        'time': get_time_string(row['cells'][4]['timeValue'])
                    },
                    'invoice_status': {
                        'textValue': row['cells'][5]['textValue'],
                        'uri': row['cells'][5]['uri']
                    },
                    'payment_due_date': get_date_string(row['cells'][6]['dateValue']),
                    'invoice_date': row['cells'][7]['textValue'],
                    'total_invoice_amount': {k: v for k, v in row['cells'][8].items() if k not in ignored_data_types},
                    'invoice_currency': {k: v for k, v in row['cells'][9].items() if k not in ignored_data_types},
                    'payment_term': {k: v for k, v in row['cells'][10].items() if k not in ignored_data_types},
                    'invoice_amount_in_base_currency': {k: v for k, v in row['cells'][11].items() if k not in ignored_data_types},
                    'description': row['cells'][12].get('textValue', '')
                }
            except KeyError:
                return None

        def filter_data(response):
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            ignored_billing_status = ('Billed', 'Paid')
            ignored_data_types = ('dataType', 'objectType')
            queued_for_sync_invoices =  list(
                map(lambda row: get_invoice_detail(row, ignored_data_types),
                filter(lambda x: x['cells'][5]['textValue'] not in ignored_billing_status, flatten_rows))) if flatten_rows else []
            return [invoice for invoice in queued_for_sync_invoices if invoice is not None]
        get_queued_for_sync_invoice = rail.RepliconServicePageOperator(
            task_id="get_queued_for_sync_invoice",
            endpoint="/services/InvoiceListService2.svc/GetData",
            data=get_queuedforsync_invoice,
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            page_handler=page_handler,
            all_result_data_handler=filter_data
        )

        has_list_data = rail.IfOperator(
            task_id='has_list_data',
            test="{{ result('get_queued_for_sync_invoice') | length > 0 }}",
            yes_task='get_required_invoices',
            no_task='should_log_history'
        )

        def handle_updated_invoices(response, item):
            def compare_datetime_value(datetime_value):
                datetime_value = datetime(
                    year=datetime_value['year'], month=datetime_value['month'], day=datetime_value['day'],
                    hour=datetime_value['hour'], minute=datetime_value['minute'], second=datetime_value['second'])
                last_sync_time = datetime.strptime(rail.result('get_lastsync_time')[
                                                   'last_synctime'], '%Y-%m-%dT%H:%M:%S')
                return datetime_value >= last_sync_time
            last_modified_timestamp = response['lastModifiedTimestamp']['valueInUtc']
            is_valid_invoice = compare_datetime_value(last_modified_timestamp)
            if is_valid_invoice:
                return item
            return ''
        get_required_invoices = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_required_invoices",
            items=lambda: [x for x in rail.result(
                'get_queued_for_sync_invoice') if x['invoice']],
            endpoint="/services/InvoiceService2.svc/GetInvoiceDetails",
            data={
                'invoiceUri': '{{ item.invoice }}'
            },
            flatten=True,
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data_handler=handle_updated_invoices
        )

        has_xero_invoice_data = rail.IfOperator(
            task_id='has_xero_invoice_data',
            test="{{ result('get_required_invoices') | remove_empty | length > 0 }}",
            yes_task='trigger_invoice_child_dag',
            no_task='should_log_history'
        )

        trigger_invoice_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_invoice_child_dag',
            thread_pool_size=4,
            retries=0,
            items=lambda: [x for x in rail.result(
                'get_required_invoices') if x],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f"standard_xero_connector_{config.region.replace('-', '_')}_invoice_export_child_dag_{config.instance}",
            conf=lambda dag_run, item: {
                **dict(item.items()),
                **{
                    k: v for k, v in dag_run.conf.items() if k not in ('_ancestry', '_ecid', '_replication_position')
                }
            }
        )

        wait_for_invoice_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_invoice_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_invoice_child_dag") }}'
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
            test="{{ not((get_task_state('has_list_data') == 'success' and \
                result('has_list_data') != 'get_required_invoices') or \
                (get_task_state('has_xero_invoice_data') == 'success' and \
                result('has_xero_invoice_data') != 'trigger_invoice_child_dag')) }}",
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
            integration_type='invoice_export'
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
            'No') >> get_lastsync_time >> get_my_actual_user_identity >> is_polaris_permissions_present
        is_polaris_permissions_present >> get_sync_status_filter_definition
        get_sync_status_filter_definition >> get_queued_for_sync_invoice >> update_lastsync_time >> has_list_data

        has_list_data >> rail.Label(
            'Yes') >> get_required_invoices >> has_xero_invoice_data
        has_xero_invoice_data >> rail.Label(
            "Yes") >> trigger_invoice_child_dag >> wait_for_invoice_child_dag >> \
            gather_invoice_error >> is_invoice_error

        is_invoice_error >> rail.Label(
            'Yes') >> fail_invoice_error >> should_log_history
        is_invoice_error >> rail.Label(
            'No') >> should_log_history

        has_xero_invoice_data >> rail.Label(
            "No") >> should_log_history
        has_list_data >> rail.Label(
            "No") >> should_log_history
        should_log_history >> rail.Label(
            'Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label(
            'No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
