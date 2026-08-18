
from datetime import timedelta
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_xero_connector_{config.region.replace('-', '_')}_client_export_child_dag_{config.instance}",
        description=f'Xero Connector {config.region} Client Import Child Dag {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.max_active_runs_client_export_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_contact_in_xero_by_name'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_contact_in_xero_by_name',
            end_task='catch_client_export_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        search_contact_in_xero_by_name = rail.XeroAPIOperator(
            task_id='search_contact_in_xero_by_name',
            xero_conn_id='{{ dag_run.conf.xero_conn_id }}',
            endpoint='/api.xro/2.0/Contacts',
            request_method='GET',
            filters='''?where=Name="{{dag_run.conf.client_name}}"'''
        )

        if_contact_present_in_xero = rail.IfOperator(
            task_id='if_contact_present_in_xero',
            test=lambda dag_run: rail.result('search_contact_in_xero_by_name') and rail.result(
                'search_contact_in_xero_by_name')['Contacts'] and (rail.result(
                    'search_contact_in_xero_by_name')['Contacts'][0]['Name'] == dag_run.conf['client_name']),
            yes_task="upsert_contact",
            no_task="if_contact_not_present_in_xero",
        )

        def get_request_body_for_updating_or_creating(dag_run):
            is_contact_present = bool(rail.result(
                'search_contact_in_xero_by_name')['Contacts'])
            pobox_address = json.loads(dag_run.conf['pobox_address'])
            physical_address = json.loads(dag_run.conf['physical_address'])
            request_body = {
                "ContactStatus": 'ACTIVE',
                "FirstName": " ".join((dag_run.conf['billing_contact']).split()[:-1]) if dag_run.conf['billing_contact'] else " ",
                "LastName": (((dag_run.conf['billing_contact']).split())[-1]) if dag_run.conf['billing_contact'] else " ",
                "EmailAddress": dag_run.conf['email'],
                "Addresses": [
                    {
                        'AddressType': 'POBOX',
                        'AddressLine1': pobox_address['addressline1'],
                        'City': pobox_address['city'],
                        'Region': pobox_address['region'],
                        'PostalCode': pobox_address['postalcode'],
                        'Country': pobox_address['country']
                    },
                    {
                        'AddressType': 'STREET',
                        'AddressLine1': physical_address['addressline1'],
                        'City': physical_address['city'],
                        'Region': physical_address['region'],
                        'PostalCode': physical_address['postalcode'],
                        'Country': physical_address['country']
                    }
                ],
                "Phones": [
                    {
                        "PhoneType": "DEFAULT",
                        "PhoneNumber": dag_run.conf['phone_number'],
                        "PhoneAreaCode": "",
                        "PhoneCountryCode": ""
                    },
                    {
                        "PhoneType": "FAX",
                        "PhoneNumber": dag_run.conf['fax_number'],
                        "PhoneAreaCode": "",
                        "PhoneCountryCode": ""
                    }
                ]
            }
            if is_contact_present:
                request_body["ContactID"] = rail.result('search_contact_in_xero_by_name')[
                    'Contacts'][0]['ContactID']
            else:
                request_body['Name'] = dag_run.conf['client_name']
                request_body['Phones'].append({
                    "PhoneType": "MOBILE",
                    "PhoneNumber": dag_run.conf['phone_number'],
                    "PhoneAreaCode": "",
                    "PhoneCountryCode": ""
                })
            return request_body

        upsert_contact = rail.XeroAPIOperator(
            task_id='upsert_contact',
            xero_conn_id='{{ dag_run.conf.xero_conn_id }}',
            endpoint='/api.xro/2.0/Contacts',
            request_method='POST',
            request_body=get_request_body_for_updating_or_creating
        )

        if_contact_not_present_in_xero = rail.IfOperator(
            task_id='if_contact_not_present_in_xero',
            test=lambda dag_run: not (rail.result('search_contact_in_xero_by_name') and rail.result(
                'search_contact_in_xero_by_name')['Contacts'] and (rail.result(
                    'search_contact_in_xero_by_name')['Contacts'][0]['Name'] == dag_run.conf['client_name'])),
            yes_task="create_contact",
            no_task="catch_client_export_error",
        )

        create_contact = rail.XeroAPIOperator(
            task_id='create_contact',
            xero_conn_id='{{ dag_run.conf.xero_conn_id }}',
            endpoint='/api.xro/2.0/Contacts',
            request_method='POST',
            request_body=get_request_body_for_updating_or_creating
        )

        def get_downstreamtasks_error(client_name, error_message):
            return {
                'error': f'Error with {client_name} - {error_message}'
            }
        catch_client_export_error = rail.PythonOperator(
            task_id='catch_client_export_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.client_name }}',
                     '{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_client_export_error
        can_run_batch_task >> rail.Label(
            'No') >> search_contact_in_xero_by_name >> if_contact_present_in_xero
        if_contact_present_in_xero >> rail.Label(
            'Yes') >> upsert_contact >> if_contact_not_present_in_xero
        if_contact_present_in_xero >> rail.Label(
            'No') >> if_contact_not_present_in_xero
        if_contact_not_present_in_xero >> rail.Label(
            'Yes') >> create_contact >> catch_client_export_error
        if_contact_not_present_in_xero >> rail.Label(
            'No') >> catch_client_export_error

    return dag


rail.for_each_instance(create_dag)
