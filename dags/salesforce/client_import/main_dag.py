from datetime import datetime, timedelta, timezone
import rail
import pycountry
from airflow.models import Variable


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_salesforce_{config.region.replace('-', '_')}_client_import_{config.instance}",
        description=f'Salesforce {config.region} Client Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
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
            date_format='%Y-%m-%dT%H:%M:%Sz',
            initial_sync_time=lambda: (datetime.now(
                timezone.utc) - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S'),
            provider=config.provider
        )

        def get_new_updated_account_query(dag_run):
            where_clause = f"LastModifiedDate >= {rail.result('get_lastsync_time_and_current_time')['last_synctime']}"
            base_select_query = 'SELECT Id, Name, Type, OwnerId, AccountNumber, Description, \
                ShippingStreet, ShippingCity, ShippingState, ShippingCountry, ShippingPostalCode, \
                    BillingStreet, BillingCity, BillingState, BillingCountry, BillingPostalCode, \
                        Phone, Fax, Website FROM Account'
            account_types_to_sync = dag_run.conf['customSettings']['accountTypesToSync']
            if account_types_to_sync and account_types_to_sync != 'ALL' and not dag_run.conf['customSettings']['syncAccountsWithNoTypes']:
                account_types_to_sync = "('" + "','".join(
                    list(map(str.strip, account_types_to_sync.split(','))))+"')"
                where_clause = f'{where_clause} AND Type IN {account_types_to_sync}'
            return f"{base_select_query} WHERE {where_clause}"
        new_updated_account = rail.SalesforceQueryOperator2(
            task_id="new_updated_account",
            salesforce_conn_id="{{ dag_run.conf.salesforce_conn_id }}",
            query=get_new_updated_account_query
        )

        update_lastsync_time = rail.SetLastSyncTimeOperator(
            task_id='update_lastsync_time',
            workflow_name=config.workflow,
            provider=config.provider,
            value_to_set='{{result("get_lastsync_time_and_current_time").current_time}}'
        )

        is_new_updated_account_found = rail.IfOperator(
            task_id="is_new_updated_account_found",
            test="{{ result('new_updated_account') | is_truthy and \
                 result('new_updated_account', 'length') > 0 }}",
            yes_task='get_all_countries',
            no_task='should_log_history'
        )

        get_all_countries = rail.RepliconServiceOperator(
            task_id='get_all_countries',
            endpoint='/services/InternationalizationService1.svc/GetAllCountries',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        def get_client_child_dag_conf(dag_run, item):
            def get_country_uri(alpha2_or_alpha3_name):
                country = pycountry.countries.get(
                    alpha_2=alpha2_or_alpha3_name) or pycountry.countries.get(
                    alpha_3=alpha2_or_alpha3_name) or pycountry.countries.get(name=alpha2_or_alpha3_name)
                return rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_countries'), 'name', country.name, 'uri', '') if country else ''
            shipping_country_uri = get_country_uri(
                item['ShippingCountry']) if item['ShippingCountry'] else ''
            billing_country_uri = get_country_uri(
                item['BillingCountry']) if item['BillingCountry'] else ''
            return {
                **{
                    'account_type': item['Type'],
                    'account_name': item['Name'],
                    'owner_id': item['OwnerId'],
                    'account_id': item['Id'],
                    'account_number': item['AccountNumber'],
                    'account_description': item['Description'],
                    'shipping_street': item['ShippingStreet'],
                    'shipping_city': item['ShippingCity'],
                    'shipping_state_province': item['ShippingState'],
                    'shipping_country_uri': shipping_country_uri,
                    'shipping_zip_postal_code': item['ShippingPostalCode'],
                    'billing_street': item['BillingStreet'],
                    'billing_city': item['BillingCity'],
                    'billing_state_province': item['BillingState'],
                    'billing_country_uri': billing_country_uri,
                    'billing_zip_postal_code': item['BillingPostalCode'],
                    'account_phone': item['Phone'],
                    'account_fax': item['Fax'],
                    'website': item['Website']
                },
                **{
                    k: v for k, v in dag_run.conf.items() if k not in ('_ancestry', '_ecid', '_replication_position')
                }
            }
        trigger_client_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_client_child_dag',
            retries=0,
            items=lambda: rail.result('new_updated_account')['records'],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f"standard_salesforce_{config.region.replace('-', '_')}_client_import_child_dag_{config.instance}",
            conf=get_client_child_dag_conf
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
            test="{{ not(get_task_state('is_new_updated_account_found') == 'success' and \
                result('is_new_updated_account_found') != 'get_all_countries')}}",
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
            connector_name='salesforce',
            integration_type='client_import'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> should_log_history
        can_run_batch_task >> rail.Label(
            'No') >> get_lastsync_time_and_current_time >> new_updated_account >> update_lastsync_time >> is_new_updated_account_found
        is_new_updated_account_found >> rail.Label(
            'Yes') >> get_all_countries >> trigger_client_child_dag >> wait_for_client_child_dag >> \
            gather_client_error >> is_client_error

        is_client_error >> rail.Label(
            'Yes') >> fail_client_error >> should_log_history
        is_client_error >> rail.Label(
            'No') >> should_log_history

        is_new_updated_account_found >> rail.Label(
            'No') >> should_log_history
        should_log_history >> rail.Label(
            'Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label(
            'No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
