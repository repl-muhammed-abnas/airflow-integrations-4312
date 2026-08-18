from datetime import timedelta
import itertools
import hashlib
import json
import rail
from airflow.models import Variable


null = None


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_xero_connector_{config.region.replace('-', '_')}_client_export_{config.instance}",
        description=f'Xero Online {config.region} Client Export {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_bulk_details_of_all_clients'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_bulk_details_of_all_clients',
            end_task='should_log_history',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def page_handler(request, response):
            if len(response['rows']) > 0:
                (request['page']) += 1
                return request
            return None

        def filter_data(response):
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            return list(map(lambda row: {
                'client_name': row['cells'][0].get('textValue'),
                'client_uri': row['cells'][0].get('uri'),
                'details_md5': hashlib.md5((json.dumps(row)).encode('utf-8')).hexdigest(),
                'billing_contact': row['cells'][1].get('textValue', ''),
                'pobox_address': {
                    'addressline1': row['cells'][2].get('textValue'),
                    'city': row['cells'][3].get('textValue'),
                    'region': row['cells'][4].get('textValue'),
                    'postalcode': row['cells'][5].get('textValue'),
                    'country': row['cells'][6].get('textValue')
                },
                'physical_address': {
                    'addressline1': row['cells'][7].get('textValue'),
                    'city': row['cells'][8].get('textValue'),
                    'region': row['cells'][9].get('textValue'),
                    'postalcode': row['cells'][10].get('textValue'),
                    'country': row['cells'][11].get('textValue')
                },
                'phone_number': row['cells'][12].get('textValue'),
                'fax_number': row['cells'][13].get('textValue'),
                'email': row['cells'][14].get('textValue'),
            }, flatten_rows)) if flatten_rows else []

        get_bulk_details_of_all_clients = rail.RepliconServicePageOperator(
            task_id='get_bulk_details_of_all_clients',
            endpoint='/services/ClientListService1.svc/GetData',
            data={
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:client-list-column:client",
                    "urn:replicon:client-list-column:billing-contact",
                    "urn:replicon:client-list-column:billing-contact-address",
                    "urn:replicon:client-list-column:billing-contact-city",
                    "urn:replicon:client-list-column:billing-contact-state-province",
                    "urn:replicon:client-list-column:billing-contact-zip-postal-code",
                    "urn:replicon:client-list-column:billing-contact-country",
                    "urn:replicon:client-list-column:client-contact-address",
                    "urn:replicon:client-list-column:client-contact-city",
                    "urn:replicon:client-list-column:client-contact-state-province",
                    "urn:replicon:client-list-column:client-contact-zip-postal-code",
                    "urn:replicon:client-list-column:client-contact-country",
                    "urn:replicon:client-list-column:client-contact-phone",
                    "urn:replicon:client-list-column:client-contact-fax",
                    "urn:replicon:client-list-column:client-contact-email"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:client-list-filter:active"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "bool": "true"
                        },
                    },
                }
            },
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            page_handler=page_handler,
            all_result_data_handler=filter_data
        )

        create_collection_of_clients = rail.CreateCollectionOperator(
            task_id='create_collection_of_clients',
            source=lambda: rail.result('get_bulk_details_of_all_clients'),
            name='all_clients'
        )

        get_tenant_wide_table_for_client_reference = rail.CreateLogOperator(
            task_id='get_tenant_wide_table_for_client_reference',
            existing_log_mode='append',
            tenant_wide_name=lambda dag_run: 'standard_xero_' +
            dag_run.conf['company_key'] + '_clients'
        )

        get_clients_reference = rail.FilterLogEntriesOperator(
            task_id='get_clients_reference',
            log="{{result('get_tenant_wide_table_for_client_reference')}}",
            remove_filtered_entries=True
        )

        update_client_reference = rail.WriteLogOperator(
            task_id='update_client_reference',
            message='na',
            log="{{result('get_tenant_wide_table_for_client_reference')}}",
            items="{{result('create_collection_of_clients')}}",
            properties={
                'client_name': "{{item.client_name}}",
                'client_uri': "{{item.client_uri}}",
                'details_md5': "{{item.details_md5}}"
            }
        )

        is_migration_client_first_export = rail.IfOperator(
            task_id='is_migration_client_first_export',
            test=lambda dag_run: Variable.get(
                f"standard_xero_{dag_run.conf['company_key']}_client_export_is_migrated_customer",
                default_var='false').lower() == 'true' and rail.result('get_clients_reference','length') == 0,
            yes_task='should_log_history',
            no_task='compose_csv_reference'
        )

        compose_csv_reference = rail.WriteCSVFileOperator(
            task_id='compose_csv_reference',
            source="{{result('get_clients_reference')}}",
            header=['client_name', 'client_uri', 'details_md5'],
            row=lambda item: [
                item['properties']['client_name'],
                item['properties']['client_uri'],
                item['properties']['details_md5']
            ]
        )

        create_reference_file_collection = rail.CreateCollectionOperator(
            task_id='create_reference_file_collection',
            source="{{result('compose_csv_reference')}}",
            name='clients_reference'
        )

        query_delta_clients = rail.QueryCollectionOperator(
            task_id='query_delta_clients',
            query='SELECT * FROM all_clients WHERE all_clients.details_md5 NOT IN ( SELECT details_md5 from clients_reference)'
        )

        has_updated_clients = rail.IfOperator(
            task_id='has_updated_clients',
            test="{{ result('query_delta_clients','length') > 0 }}",
            yes_task='trigger_client_export_child',
            no_task='should_log_history'
        )

        trigger_client_export_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_client_export_child',
            thread_pool_size=4,
            retries=0,
            items="{{result('query_delta_clients')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f"standard_xero_connector_{config.region.replace('-', '_')}_client_export_child_dag_{config.instance}",
            conf=lambda dag_run, item: {
                **dict(item.items()),
                **{
                    k: v for k, v in dag_run.conf.items() if k not in ('_ancestry', '_ecid', '_replication_position')
                }
            }
        )

        wait_for_client_export_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_client_export_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_client_export_child") }}'
        )

        gather_client_export_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_client_export_error',
            dag_runs="{{ result('trigger_client_export_child') }}",
            dagrun_task_id='catch_client_export_error',
            flatten=True
        )

        is_client_export_error = rail.IfOperator(
            task_id='is_client_export_error',
            test="{{ result('gather_client_export_error') | length > 0 }}",
            yes_task='fail_client_export_error',
            no_task='should_log_history'
        )

        fail_client_export_error = rail.FailOperator(
            task_id='fail_client_export_error',
            message="{{ result('gather_client_export_error') | map_to_attr('error') | join('|') }}"
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ not(get_task_state('has_updated_clients') == 'success' and \
                result('has_updated_clients') != 'trigger_client_export_child') }}",
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
            integration_type='client_export'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> should_log_history
        can_run_batch_task >> rail.Label(
            'No') >> get_bulk_details_of_all_clients >> create_collection_of_clients >> get_tenant_wide_table_for_client_reference
        get_tenant_wide_table_for_client_reference >> get_clients_reference >> update_client_reference >> is_migration_client_first_export
        is_migration_client_first_export >> rail.Label('Yes') >> should_log_history
        is_migration_client_first_export >> rail.Label('No') >> compose_csv_reference
        compose_csv_reference >> create_reference_file_collection >> query_delta_clients >> has_updated_clients
        has_updated_clients >> rail.Label(
            'Yes') >> trigger_client_export_child >> wait_for_client_export_child_dag
        wait_for_client_export_child_dag >> gather_client_export_error >> is_client_export_error
        is_client_export_error >> rail.Label(
            'Yes') >> fail_client_export_error >> should_log_history
        is_client_export_error >> rail.Label('No') >> should_log_history
        has_updated_clients >> rail.Label('No') >> should_log_history
        should_log_history >> rail.Label('Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label('No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
