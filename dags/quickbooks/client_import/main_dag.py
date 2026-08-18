from datetime import timedelta, datetime, timezone
import rail
from airflow.models import Variable


# pylint:disable = too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_quickbooks_online_{config.region.replace('-', '_')}_client_import_{config.instance}",
        description=f'QuickBooks Online {config.region} Client Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_dag_max_active_runs
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
                timezone.utc) - timedelta(days=5)).strftime('%Y-%m-%d %H:%M:%S'),
            provider=config.provider
        )

        def get_customer_data_query():
            last_modified_time = rail.result('get_lastsync_time_and_current_time')[
                'last_synctime']
            return {
                'query': "SELECT * FROM Customer WHERE MetaData.LastUpdatedTime >= '" + last_modified_time + "'"
            }
        get_query_params = rail.PythonOperator(
            task_id='get_query_params',
            python_callable=get_customer_data_query
        )

        intuit_customer_data = rail.InternalQuickbooksAPIOperator(
            task_id='intuit_customer_data',
            request_method='GET',
            endpoint="/query",
            intuit_conn_id='{{ dag_run.conf.quickbooks_conn_id }}',
            query_params=lambda: rail.result('get_query_params')
        )

        update_lastsync_time = rail.SetLastSyncTimeOperator(
            task_id='update_lastsync_time',
            workflow_name=config.workflow,
            provider=config.provider,
            value_to_set='{{result("get_lastsync_time_and_current_time").current_time}}'
        )

        has_quickbooks_customer_data = rail.IfOperator(
            task_id='has_quickbooks_customer_data',
            test=lambda: rail.result('intuit_customer_data')['QueryResponse'] and rail.result(
                'intuit_customer_data')['QueryResponse'].get('Customer'),
            yes_task='get_all_clients',
            no_task='should_log_history'
        )

        get_all_clients = rail.RepliconServiceOperator(
            task_id='get_all_clients',
            endpoint='/services/ClientService1.svc/GetAllClients',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        get_all_countries = rail.RepliconServiceOperator(
            task_id='get_all_countries',
            endpoint='/services/InternationalizationService1.svc/GetAllCountries',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        def parse_quickbooks_customer():
            country_mapper = {
                'US': 'United States',
                'USA': 'United States',
                'America': 'United States',
                'United States of America': 'United States',
                'HK': 'Hong Kong',
                'UK': 'United Kingdom',
                'UAE': 'United Arab Emirates'
            }
            incoming_customer = []
            intuit_raw_data = rail.result('intuit_customer_data')
            customer_raw_data = intuit_raw_data['QueryResponse']['Customer']
            existing_client_in_replicon = rail.result('get_all_clients')
            countries_from_replicon = rail.result('get_all_countries')
            # pylint: disable=cell-var-from-loop
            for customer in customer_raw_data:
                existing_client_info = list(filter(
                    lambda data: customer['DisplayName'] and
                    data['name'].lower() == customer['DisplayName'].lower(), existing_client_in_replicon))
                shipping_country_to_assign = country_mapper.get(
                    customer.get('ShipAddr', {}).get('Country'), customer.get('ShipAddr', {}).get('Country'))
                billing_country_to_assign = country_mapper.get(
                    customer.get('BillAddr', {}).get('Country'), customer.get('BillAddr', {}).get('Country'))
                shipping_country_info = list(filter(
                    lambda data: shipping_country_to_assign and
                    data['name'].lower() == shipping_country_to_assign.lower(), countries_from_replicon))
                billing_country_info = list(filter(
                    lambda data: billing_country_to_assign and
                    data['name'].lower() == billing_country_to_assign.lower(), countries_from_replicon))

                incoming_customer.append({
                    'client_name': customer['DisplayName'],
                    'is_new_client': not existing_client_info,
                    'client_contact': f"{customer['GivenName']} {customer['FamilyName']}" if customer.get('FamilyName') else '',
                    'client_address': customer['ShipAddr'].get('Line1') if customer.get('ShipAddr') else '',
                    'client_city': customer['ShipAddr'].get('City') if customer.get('ShipAddr') else '',
                    'client_state': customer['ShipAddr'].get('CountrySubDivisionCode') if customer.get('ShipAddr') else '',
                    'client_country': shipping_country_info[0]['uri'] if shipping_country_info else None,
                    'client_zip': customer['ShipAddr'].get('PostalCode') if customer.get('ShipAddr') else '',
                    'billing_address': customer['BillAddr'].get('Line1') if customer.get('BillAddr') else '',
                    'billing_city': customer['BillAddr'].get('City') if customer.get('BillAddr') else '',
                    'billing_state': customer['BillAddr'].get('CountrySubDivisionCode') if customer.get('BillAddr') else '',
                    'billing_country': billing_country_info[0]['uri'] if billing_country_info else None,
                    'billing_zip': customer['BillAddr'].get('PostalCode') if customer.get('BillAddr') else '',
                    'client_phone_number': customer['PrimaryPhone'].get('FreeFormNumber') if customer.get('PrimaryPhone') else '',
                    'client_email': customer['PrimaryEmailAddr']['Address'] if customer.get('PrimaryEmailAddr') else '',
                    'client_fax': customer['Fax'].get('FreeFormNumber') if customer.get('Fax') else '',
                    'client_website': customer['WebAddr'].get('URI') if customer.get('WebAddr') else ''
                })
            return incoming_customer
        parse_quickbooks_data = rail.PythonOperator(
            task_id='parse_quickbooks_data',
            python_callable=parse_quickbooks_customer
        )

        trigger_client_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_client_child_dag',
            retries=0,
            items=lambda: rail.result('parse_quickbooks_data'),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f"standard_quickbooks_online_{config.region.replace('-', '_')}_client_import_child_dag_{config.instance}",
            conf=lambda dag_run, item: {
                **dict(item.items()),
                **{
                    k: v for k, v in dag_run.conf.items() if k not in ('_ancestry', '_ecid', '_replication_position')
                }
            }
        )

        wait_for_client_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_client_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_client_child_dag") }}'
        )

        gather_client_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_client_error',
            dag_runs="{{ result('trigger_client_child_dag') }}",
            dagrun_task_id='catch_client_error',
            flatten=True
        )

        is_client_error = rail.IfOperator(
            task_id='is_client_error',
            test="{{ result('gather_client_error') | length > 0 }}",
            yes_task='fail_client_error',
            no_task='should_log_history'
        )

        fail_client_error = rail.FailOperator(
            task_id='fail_client_error',
            message="{{ result('gather_client_error') | map_to_attr('error') | join('|') }}"
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ not(get_task_state('has_quickbooks_customer_data') == 'success' and \
                result('has_quickbooks_customer_data') != 'get_all_clients') }}",
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
            integration_type='client_import'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> should_log_history
        can_run_batch_task >> rail.Label(
            'No') >> get_lastsync_time_and_current_time
        get_lastsync_time_and_current_time >> get_query_params >> intuit_customer_data >> \
            update_lastsync_time >> has_quickbooks_customer_data
        has_quickbooks_customer_data >> rail.Label(
            'Yes') >> get_all_clients >> get_all_countries >> parse_quickbooks_data >> \
            trigger_client_child_dag >> wait_for_client_child_dag >> gather_client_error >> \
            is_client_error
        is_client_error >> rail.Label(
            'Yes') >> fail_client_error >> should_log_history
        is_client_error >> rail.Label(
            'No') >> should_log_history
        has_quickbooks_customer_data >> rail.Label(
            'No') >> should_log_history
        should_log_history >> rail.Label(
            'Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label(
            'No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
