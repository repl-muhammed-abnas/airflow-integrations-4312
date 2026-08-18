from datetime import timedelta
import itertools
import uuid
import rail
from airflow.models import Variable


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_salesforce_{config.region.replace('-', '_')}_client_import_child_dag_{config.instance}",
        description=f'Salesforce {config.region} Client Import Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='should_process_client'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='should_process_client',
            end_task='catch_client_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def is_process_client(dag_run):
            account_types_to_sync = dag_run.conf['customSettings']['accountTypesToSync']
            account_type = dag_run.conf['account_type']
            is_sync_account_with_no_types = dag_run.conf['customSettings']['syncAccountsWithNoTypes']
            if account_types_to_sync != 'ALL' and account_type and (
                    account_type not in account_types_to_sync):
                return False
            if not account_type:
                if not is_sync_account_with_no_types:
                    return False
            return True
        should_process_client = rail.IfOperator(
            task_id='should_process_client',
            test=is_process_client,
            yes_task='search_client_in_replicon',
            no_task='catch_client_error'
        )

        def page_handler(request, response):
            if len(response['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def get_clienturi(response, dag_run):
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            return rail.smartjoin_by_delim(
                [x['cells'][0]['uri']
                    for x in flatten_rows if x['cells'][1]['textValue'] == dag_run.conf['account_name']]
            ) if flatten_rows else ''
        search_client_in_replicon = rail.RepliconServicePageOperator(
            task_id='search_client_in_replicon',
            endpoint='/services/ClientListService1.svc/GetData',
            data=lambda dag_run: {
                'page': 1,
                'pagesize': 10000,
                'columnUris': [
                    'urn:replicon:client-list-column:client',
                    'urn:replicon:client-list-column:name'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:client-list-filter:name'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['account_name']
                        }
                    }
                }
            },
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            page_handler=page_handler,
            all_result_data_handler=get_clienturi
        )

        is_ownerid_present = rail.IfOperator(
            task_id='is_ownerid_present',
            test='{{ dag_run.conf.owner_id | is_truthy }}',
            yes_task='search_user_in_salesforce',
            no_task='search_contact_in_salesforce'
        )

        def get_username(response):
            records = response.get('records', [])
            return records[0].get('Username', '') if records else ''
        search_user_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='search_user_in_salesforce',
            salesforce_conn_id='{{ dag_run.conf.salesforce_conn_id }}',
            query="SELECT Username FROM User WHERE Id = '{{ dag_run.conf.owner_id }}' LIMIT 150",
            data_handler=get_username
        )

        def get_contact(response):
            records = response.get('records', [])
            return records[0] if records else {}
        search_contact_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='search_contact_in_salesforce',
            salesforce_conn_id='{{ dag_run.conf.salesforce_conn_id }}',
            query="SELECT FirstName, LastName, Email FROM Contact WHERE AccountId = '{{ dag_run.conf.account_id }}' LIMIT 150",
            data_handler=get_contact
        )

        def get_replicon_user(response):
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            return rail.smartjoin_by_delim(
                [x['cells'][0]['uri']
                    for x in flatten_rows if x['cells'][1]['textValue'] == rail.result('search_user_in_salesforce')]
            ) if flatten_rows else ''
        search_user_in_replicon = rail.RepliconServicePageOperator(
            task_id='search_user_in_replicon',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:login-name'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('search_user_in_salesforce')
                        }
                    }
                }
            },
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            page_handler=page_handler,
            all_result_data_handler=get_replicon_user
        )

        is_update_client_and_update_disabled = rail.IfOperator(
            task_id='is_update_client_and_update_disabled',
            test="{{ result('search_client_in_replicon') | is_truthy and \
                dag_run.conf.customSettings.toUpdate | sn | is_falsy }}",
            yes_task='catch_client_error',
            no_task='create_update_client'
        )

        def get_create_update_client(dag_run):
            null = None
            first_name = rail.result('search_contact_in_salesforce')[
                'FirstName'] if rail.result('search_contact_in_salesforce') else null
            last_name = rail.result('search_contact_in_salesforce')[
                'LastName'] if rail.result('search_contact_in_salesforce') else null
            client_contact = f"{first_name}{last_name}" if first_name and last_name else null
            email = rail.result('search_contact_in_salesforce')['Email'] if rail.result(
                'search_contact_in_salesforce') else null
            return {
                'target': {'uri': rail.result('search_client_in_replicon')} if rail.result('search_client_in_replicon') else null,
                'modifications': {
                    'nameToApply': {
                        'value': dag_run.conf['account_name']
                    },
                    'codeToApply': {
                        'value': dag_run.conf['account_number']
                    },
                    'descriptionToApply': {
                        'value': dag_run.conf['account_description']
                    } if dag_run.conf['account_description'] else null,
                    'statusToApply': True,
                    'clientContactToApply': {
                        'value': client_contact
                    } if client_contact else null,
                    'clientAddressToApply': {
                        'address': {
                            'value': dag_run.conf['shipping_street']
                        } if dag_run.conf['shipping_street'] else null,
                        'city': {'value': dag_run.conf['shipping_city']} if dag_run.conf['shipping_city'] else null,
                        'stateProvince': {'value': dag_run.conf['shipping_state_province']} if dag_run.conf['shipping_state_province'] else null,
                        'country': {'value': {'uri': dag_run.conf['shipping_country_uri']}} if dag_run.conf['shipping_country_uri'] else null,
                        'zipPostalCode': {'value': dag_run.conf['shipping_zip_postal_code']} if dag_run.conf['shipping_zip_postal_code'] else null,
                        'phoneNumber': {'value': dag_run.conf['account_phone']} if dag_run.conf['account_phone'] else null,
                        'faxNumber': {'value': dag_run.conf['account_fax']} if dag_run.conf['account_fax'] else null,
                        'email': {'value': email} if email else null,
                        'website': {'value': dag_run.conf['website']} if dag_run.conf['website'] else null,
                    },
                    'billingAddressToApply': {
                        'address': {
                            'value': dag_run.conf['billing_street']
                        } if dag_run.conf['billing_street'] else null,
                        'city': {'value': dag_run.conf['billing_city']} if dag_run.conf['billing_city'] else null,
                        'stateProvince': {'value': dag_run.conf['billing_state_province']} if dag_run.conf['billing_state_province'] else null,
                        'country': {'value': {'uri': dag_run.conf['billing_country_uri']}} if dag_run.conf['billing_country_uri'] else null,
                        'zipPostalCode': {'value': dag_run.conf['billing_zip_postal_code']} if dag_run.conf['billing_zip_postal_code'] else null
                    },
                    'clientManagerToApply': {
                        'user': {
                            'uri': rail.result('search_user_in_replicon')
                        }
                    } if rail.result('search_user_in_replicon') else null
                },
                'clientModificationOptionUri': 'urn:replicon:client-modification-option:save',
                'unitOfWorkId': str(uuid.uuid4())
            }
        create_update_client = rail.RepliconServiceOperator(
            task_id='create_update_client',
            endpoint='/services/ClientService1.svc/CreateClientOrApplyModifications',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=get_create_update_client
        )

        def get_downstreamtasks_error(account_name, error_message):
            return {
                'error': f'Error with {account_name} - {error_message}'
            }
        catch_client_error = rail.PythonOperator(
            task_id='catch_client_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.account_name }}',
                     '{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> rail.Label(
                'on Error') >> catch_client_error

        can_run_batch_task >> rail.Label(
            'No') >> should_process_client

        should_process_client >> rail.Label(
            'Yes') >> search_client_in_replicon >> is_ownerid_present

        is_ownerid_present >> rail.Label(
            'Yes') >> search_user_in_salesforce >> search_user_in_replicon >> search_contact_in_salesforce

        is_ownerid_present >> rail.Label(
            'No') >> search_contact_in_salesforce

        search_contact_in_salesforce >> is_update_client_and_update_disabled

        is_update_client_and_update_disabled >> rail.Label(
            'Yes') >> catch_client_error
        is_update_client_and_update_disabled >> rail.Label(
            'No') >> create_update_client >> rail.Label(
                'On Error') >> catch_client_error

        should_process_client >> rail.Label(
            'No') >> catch_client_error

    return dag


rail.for_each_instance(create_child_dag)
