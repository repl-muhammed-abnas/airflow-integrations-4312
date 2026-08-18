from datetime import timedelta, datetime
import pycountry
import rail
from airflow.models import Variable


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_xero_connector_{config.region.replace('-', '_')}_client_import_{config.instance}",
        description=f'Xero Connector {config.region} Client Import {config.instance}',
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
            initial_sync_time=lambda: (
                datetime(year=1970, month=1, day=1)).strftime('%Y-%m-%d %H:%M:%S'),
            provider=config.provider
        )

        get_new_or_updated_contacts_in_xero = rail.XeroAPIOperator(
            task_id='get_new_or_updated_contacts_in_xero',
            xero_conn_id='{{ dag_run.conf.xero_conn_id }}',
            endpoint='/api.xro/2.0/Contacts',
            request_method='GET',
            modified_since="{{result('get_lastsync_time').last_synctime}}"
        )

        has_xero_contacts_data = rail.IfOperator(
            task_id='has_xero_contacts_data',
            test=lambda: rail.result('get_new_or_updated_contacts_in_xero') and rail.result(
                'get_new_or_updated_contacts_in_xero')['Contacts'],
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

        def parse_xero_contacts():
            def get_country_name_from_iso(iso_code):
                if iso_code is None or (isinstance(iso_code, str) and len(iso_code) > 3):
                    return iso_code
                country = pycountry.countries.get(
                    alpha_2=iso_code) or pycountry.countries.get(alpha_3=iso_code)
                return country.name if country else ''
            country_mapper = {
                'US': 'United States',
                'USA': 'United States',
                'America': 'United States',
                'United States of America': 'United States',
                'HK': 'Hong Kong',
                'SG': 'Singapore',
                'UK': 'United Kingdom',
                'UAE': 'United Arab Emirates'
            }
            incoming_contact = []
            contact_raw_data = rail.result(
                'get_new_or_updated_contacts_in_xero')['Contacts']
            existing_client_in_replicon = rail.result('get_all_clients')
            countries_from_replicon = rail.result('get_all_countries')
            # pylint: disable=cell-var-from-loop
            for contact in contact_raw_data:
                existing_client_info = list(filter(
                    lambda data: contact['Name'] and
                    data['name'].lower() == contact['Name'].lower(), existing_client_in_replicon))
                street_address = rail.find_first_by_attr_and_get_attr(
                    contact.get('Addresses'), 'AddressType', 'STREET')
                postal_address = rail.find_first_by_attr_and_get_attr(
                    contact.get('Addresses'), 'AddressType', 'POBOX')
                client_country_to_assign = country_mapper.get(street_address.get(
                    'Country'), get_country_name_from_iso(street_address.get('Country')))
                billing_country_to_assign = country_mapper.get(postal_address.get(
                    'Country'), get_country_name_from_iso(postal_address.get('Country')))
                client_country_info = list(filter(
                    lambda data: client_country_to_assign and
                    data['name'].lower() == client_country_to_assign.lower(), countries_from_replicon))
                billing_country_info = list(filter(
                    lambda data: billing_country_to_assign and
                    data['name'].lower() == billing_country_to_assign.lower(), countries_from_replicon))
                default_phone = rail.find_first_by_attr_and_get_attr(
                    contact.get('Phones'), 'PhoneType', 'DEFAULT')
                fax_phone = rail.find_first_by_attr_and_get_attr(
                    contact.get('Phones'), 'PhoneType', 'FAX')

                incoming_contact.append({
                    'client_name': contact['Name'],
                    'is_new_client': not existing_client_info,
                    'client_contact': f"{contact.get('FirstName','')} {contact.get('LastName','')}",
                    'client_address': (street_address.get('AddressLine1') if (not street_address.get(
                        'AddressLine2') or street_address.get('AddressLine2').strip() in street_address.get('AddressLine1')) else ', '.join(
                            [street_address.get('AddressLine1'), street_address.get('AddressLine2')])) if (street_address and street_address.get(
                            'AddressLine1')) else '',
                    'client_city': street_address.get('City') if street_address.get('City') else '',
                    'client_region': street_address.get('Region') if street_address.get('Region') else '',
                    'client_country': client_country_info[0]['uri'] if client_country_info else None,
                    'client_zip': street_address.get('PostalCode') if street_address.get('PostalCode') else '',
                    'billing_address': (postal_address.get('AddressLine1') if (not postal_address.get(
                        'AddressLine2') or postal_address.get('AddressLine2').strip() in postal_address.get('AddressLine1')) else ', '.join(
                            [postal_address.get('AddressLine1'), postal_address.get('AddressLine2')])) if (postal_address and postal_address.get(
                            'AddressLine1')) else '',
                    'billing_city': postal_address.get('City') if postal_address.get('City') else '',
                    'billing_region': postal_address.get('Region') if postal_address.get('Region') else '',
                    'billing_country': billing_country_info[0]['uri'] if billing_country_info else None,
                    'billing_zip': postal_address.get('PostalCode') if postal_address.get('PostalCode') else '',
                    'client_phone_number': default_phone.get('PhoneNumber') if default_phone.get('PhoneNumber') else '',
                    'client_email': contact['EmailAddress'] if contact.get('EmailAddress') else '',
                    'client_fax_number': fax_phone.get('PhoneNumber') if fax_phone.get('PhoneNumber') else ''
                })
            return incoming_contact

        parse_xero_data = rail.PythonOperator(
            task_id='parse_xero_data',
            python_callable=parse_xero_contacts
        )

        trigger_client_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_client_child_dag',
            thread_pool_size=4,
            retries=0,
            items=lambda: rail.result('parse_xero_data'),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f"standard_xero_connector_{config.region.replace('-', '_')}_client_import_child_dag_{config.instance}",
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
            test="{{ not(get_task_state('has_xero_contacts_data') == 'success' and \
                result('has_xero_contacts_data') != 'get_all_clients') }}",
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
            integration_type='client_import'
        )

        update_lastsync_time = rail.SetLastSyncTimeOperator(
            task_id='update_lastsync_time',
            workflow_name=config.workflow,
            provider=config.provider,
            value_to_set='{{result("get_lastsync_time").current_time}}'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> should_log_history
        can_run_batch_task >> rail.Label(
            'No') >> get_lastsync_time
        get_lastsync_time >> get_new_or_updated_contacts_in_xero >> update_lastsync_time >> has_xero_contacts_data
        has_xero_contacts_data >> rail.Label(
            'Yes') >> get_all_clients >> get_all_countries >> parse_xero_data >> \
            trigger_client_child_dag >> wait_for_client_child_dag >> gather_client_error >> \
            is_client_error
        is_client_error >> rail.Label(
            'Yes') >> fail_client_error >> should_log_history
        is_client_error >> rail.Label(
            'No') >> should_log_history
        has_xero_contacts_data >> rail.Label(
            'No') >> should_log_history
        should_log_history >> rail.Label(
            'Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label(
            'No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
