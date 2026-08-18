from datetime import timedelta
import itertools
import uuid
import rail
from airflow.models import Variable
from salesforce.project_import.utils.request_payload import get_create_project_payload, get_link_attachments_to_objects, get_uri_from_response, page_handler
from salesforce.project_import.utils.python_callable_method import get_and_apply_customfield_values, get_polaris_payload
from salesforce.project_import.utils.util import get_project_details, get_project_status


# pylint:disable = too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_salesforce_{config.region.replace('-', '_')}_project_import_child_dag_{config.instance}",
        description=f'Salesforce {config.region} Project Import Child DAG {config.instance}',
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
            no_task='is_accountid_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_accountid_present',
            end_task='catch_project_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        is_accountid_present = rail.IfOperator(
            task_id='is_accountid_present',
            test='{{ dag_run.conf.account_id | sn | is_truthy }}',
            yes_task='search_accountname_in_salesforce',
            no_task='search_user_in_salesforce'
        )

        def get_accountname(response):
            records = response.get('records', [])
            return records[0].get('Name', '') if records else ''

        search_accountname_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='search_accountname_in_salesforce',
            salesforce_conn_id='{{ dag_run.conf.salesforce_conn_id }}',
            query="SELECT Name FROM Account WHERE Id = '{{ dag_run.conf.account_id }}' LIMIT 150",
            data_handler=get_accountname
        )

        search_client_in_replicon = rail.RepliconServicePageOperator(
            task_id='search_client_in_replicon',
            endpoint='/services/ClientListService1.svc/GetData',
            data=lambda: {
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
                            'text': rail.result('search_accountname_in_salesforce')
                        }
                    }
                }
            },
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            page_handler=page_handler,
            all_result_data_handler=lambda response: get_uri_from_response(
                response, rail.result('search_accountname_in_salesforce'))
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
            all_result_data_handler=lambda response: get_uri_from_response(
                response, rail.result('search_user_in_salesforce'))
        )

        is_update_project = rail.IfOperator(
            task_id='is_update_project',
            test=lambda dag_run: bool(
                    get_project_details(
                        dag_run.conf['replicon_projects'],
                        dag_run.conf['opportunity_name']
                    )
            ),
            yes_task='is_update_project_disabled',
            no_task='create_project_in_replicon'
        )

        is_update_project_disabled = rail.IfOperator(
            task_id='is_update_project_disabled',
            test='{{ dag_run.conf.customSettings.toUpdate | sn | is_falsy }}',
            yes_task='catch_project_error',
            no_task='update_project_in_replicon'
        )

        update_project_in_replicon = rail.RepliconServiceOperator(
            task_id='update_project_in_replicon',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda dag_run: {
                'target': {
                    'uri': get_project_details(
                        dag_run.conf['replicon_projects'],
                        dag_run.conf['opportunity_name']
                    )['uri']
                },
                'modifications': {
                    'descriptionToApply': {
                        'value': dag_run.conf['description']
                    } if dag_run.conf['description'] else None
                },
                'projectModificationOptionUri': 'urn:replicon:project-modification-option:save',
                'unitOfWorkId': str(uuid.uuid4())
            }
        )

        is_client_present_in_replicon = rail.IfOperator(
            task_id='is_client_present_in_replicon',
            test="{{ get_task_state('search_client_in_replicon') == 'success' and \
                result('search_client_in_replicon') | is_truthy }}",
            yes_task='update_project_client',
            no_task='should_update_project_custom_fields'
        )

        update_project_client = rail.RepliconServiceOperator(
            task_id='update_project_client',
            endpoint='/services/ProjectService1.svc/ApplyNewClient2',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda dag_run: {
                'projectUri': get_project_details(
                    dag_run.conf['replicon_projects'],
                    dag_run.conf['opportunity_name']
                )['uri'],
                'clientUri': rail.result('search_client_in_replicon'),
                'optionUri': 'urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes'
            }
        )

        create_project_in_replicon = rail.RepliconServiceOperator(
            task_id='create_project_in_replicon',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=get_create_project_payload
        )

        should_update_project_type = rail.IfOperator(
            task_id='should_update_project_type',
            test="{{ dag_run.conf.is_polaris_permissions_present | is_truthy \
                and get_task_state('create_project_in_replicon') == 'success' }}",
            yes_task='update_project_type',
            no_task='should_update_project_custom_fields'
        )

        update_project_type = rail.RepliconServiceOperator(
            task_id='update_project_type',
            endpoint='/services/ProjectService1.svc/PutKeyValueForProject',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                'projectUri': "{{ result('create_project_in_replicon').uri }}",
                'keyValue': {
                    'keyUri': 'urn:replicon:project-key-value-key:project-management-type',
                    'value': {
                        'uri': 'urn:replicon:project-management-type:managed'
                    }
                }
            }
        )

        def get_status_update_payload(dag_run):
            project_status = 'Initiate'
            if dag_run.conf['customSettings'].get('toSyncStatus'):
                status = get_project_status(dag_run.conf)
                if status:
                    project_status = status

            project_uri = rail.result('create_project_in_replicon')['uri']
            return get_polaris_payload(project_uri, project_status.upper())

        update_polaris_project_status = rail.RepliconServiceOperator(
            task_id='update_polaris_project_status',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=get_status_update_payload,
            endpoint='/graphql',
            app='polaris'
        )

        should_update_project_custom_fields = rail.IfOperator(
            task_id='should_update_project_custom_fields',
            test="{{ dag_run.conf.oef_list | sn | is_truthy }}",
            yes_task='update_project_custom_fields',
            no_task='validate_skip_attachment_process'
        )

        update_project_custom_fields = rail.RepliconServiceOperator(
            task_id='update_project_custom_fields',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=get_and_apply_customfield_values
        )

        def get_should_skip_attachment_process(dag_run):
            if dag_run.conf['is_polaris_permissions_present'] and dag_run.conf['customSettings']['syncAttachmentsFromOpportunity']:
                is_skip_attachment_sync = dag_run.conf.get(
                    'is_skip_attachment', False)
                if is_skip_attachment_sync:
                    if rail.render_template("{{ get_task_state('create_project_in_replicon') }}") == 'success':
                        is_skip_attachment_sync = False
                        rail.set_result(True, 'process_attachment_query')
                return is_skip_attachment_sync
            return True

        validate_skip_attachment_process = rail.PythonOperator(
            task_id='validate_skip_attachment_process',
            python_callable=get_should_skip_attachment_process
        )

        should_sync_attachments = rail.IfOperator(
            task_id='should_sync_attachments',
            test="{{ result('validate_skip_attachment_process') | is_falsy }}",
            yes_task='process_attachment_sync',
            no_task='is_user_present_in_replicon'
        )

        process_attachment_sync = rail.EmptyOperator(
            task_id='process_attachment_sync'
        )

        should_trigger_attachment_query = rail.IfOperator(
            task_id='should_trigger_attachment_query',
            test="{{ result('validate_skip_attachment_process', 'process_attachment_query') | sn | is_truthy }}",
            yes_task='search_documentlinks_in_salesforce',
            no_task='trigger_attachment_child_dag'
        )

        def get_attachments(response):
            records = response.get('records', [])
            return list(map(lambda x: {
                'id': x['Id'],
                'content_document_id': x['ContentDocumentId'],
                'opportunity_id': x['LinkedEntityId']
            }, records)) if records else []
        search_documentlinks_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='search_documentlinks_in_salesforce',
            salesforce_conn_id='{{ dag_run.conf.salesforce_conn_id }}',
            query="SELECT ContentDocumentId, Id, LinkedEntityId FROM ContentDocumentLink \
                WHERE IsDeleted = false AND LinkedEntityId = '{{ dag_run.conf.opportunity_id }}'",
            data_handler=get_attachments
        )

        is_documentlinks_present = rail.IfOperator(
            task_id='is_documentlinks_present',
            test="{{ result('search_documentlinks_in_salesforce', 'length') > 0 }}",
            yes_task='trigger_attachment_child_dag',
            no_task='is_user_present_in_replicon'
        )

        trigger_attachment_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_attachment_child_dag',
            retries=0,
            items=lambda dag_run: rail.result('search_documentlinks_in_salesforce') if rail.result(
                'search_documentlinks_in_salesforce') else dag_run.conf['content_document_ids'].split(','),
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f"standard_salesforce_{config.region.replace('-', '_')}_project_import_attachment_child_dag_{config.instance}",
            conf=lambda dag_run, item: {
                    'content_document_id': item['content_document_id'] if rail.result(
                        'search_documentlinks_in_salesforce') else item,
                    'linked_entity_id': item['opportunity_id'] if rail.result(
                        'search_documentlinks_in_salesforce') else dag_run.conf['opportunity_id'],
                **{
                    k: v for k, v in dag_run.conf.items() if k in ('replicon_conn_id', 'salesforce_conn_id')
                        }
            }
        )

        wait_for_attachment_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_attachment_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('trigger_attachment_child_dag') }}"
        )

        gather_s3_attachments = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_s3_attachments',
            dag_runs="{{ result('trigger_attachment_child_dag') }}",
            dagrun_task_id='put_binary_object',
            flatten=True
        )

        gather_invalid_attachments = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_invalid_attachments',
            dag_runs="{{ result('trigger_attachment_child_dag') }}",
            dagrun_task_id='invalid_contentversions_in_salesforce',
            flatten=True
        )

        def get_attachment_exception():
            invalid_attachments = [x for x in rail.result(
                'gather_invalid_attachments') if x]
            return 'attachment_exception' if len(invalid_attachments) > 0 else ''
        gather_attachment_exception = rail.PythonOperator(
            task_id='gather_attachment_exception',
            python_callable=get_attachment_exception
        )

        link_attachments_to_objects = rail.RepliconServiceOperator(
            task_id='link_attachments_to_objects',
            endpoint='/graphql',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            app='polaris',
            data=get_link_attachments_to_objects
        )

        is_user_present_in_replicon = rail.IfOperator(
            task_id='is_user_present_in_replicon',
            test="{{ get_task_state('search_user_in_replicon') == 'success' and \
                result('search_user_in_replicon') | is_truthy }}",
            yes_task='assign_co_manager_to_project',
            no_task='catch_project_error'
        )

        def assign_co_manager_params(dag_run):
            project = get_project_details(
                dag_run.conf['replicon_projects'], dag_run.conf['opportunity_name'])
            project_uri = project['uri'] if project else None

            return {
                'projectUri': project_uri if project_uri else rail.result(
                    'create_project_in_replicon').get('uri'),
                'sharedUris': [rail.result('search_user_in_replicon')]
            }
        assign_co_manager_to_project = rail.RepliconServiceOperator(
            task_id='assign_co_manager_to_project',
            endpoint='/services/ProjectService1.svc/PutExplicitSharingAssignments',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=assign_co_manager_params
        )

        def get_downstreamtasks_error(opportunity_name, error_message):
            return {
                'error': f'Error with {opportunity_name} - {error_message}'
            }
        catch_project_error = rail.PythonOperator(
            task_id='catch_project_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.opportunity_name }}',
                     '{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> rail.Label(
                'on Error') >> catch_project_error

        can_run_batch_task >> rail.Label(
            'No') >> is_accountid_present

        is_accountid_present >> rail.Label(
            'Yes') >> search_accountname_in_salesforce >> search_client_in_replicon >> search_user_in_salesforce
        is_accountid_present >> rail.Label(
            'No') >> search_user_in_salesforce

        search_user_in_salesforce >> search_user_in_replicon >> is_update_project

        is_update_project >> rail.Label(
            'Yes') >> is_update_project_disabled

        is_update_project_disabled >> rail.Label(
            'Yes') >> catch_project_error
        is_update_project_disabled >> rail.Label(
            'No') >> update_project_in_replicon >> is_client_present_in_replicon

        is_client_present_in_replicon >> rail.Label(
            'Yes') >> update_project_client >> should_update_project_custom_fields
        is_client_present_in_replicon >> rail.Label(
            'No') >> should_update_project_custom_fields

        is_update_project >> rail.Label(
            'No') >> create_project_in_replicon >> should_update_project_type

        should_update_project_type >> rail.Label(
            'Yes') >> update_project_type >> update_polaris_project_status >> should_update_project_custom_fields

        should_update_project_type >> rail.Label(
            'No') >> should_update_project_custom_fields

        should_update_project_custom_fields >> rail.Label(
            'Yes') >> update_project_custom_fields >> validate_skip_attachment_process
        should_update_project_custom_fields >> rail.Label(
            'No') >> validate_skip_attachment_process
        validate_skip_attachment_process >> should_sync_attachments
        should_sync_attachments >> rail.Label(
            'Yes') >> process_attachment_sync >> should_trigger_attachment_query

        should_trigger_attachment_query >> rail.Label(
            'Yes') >> search_documentlinks_in_salesforce >> is_documentlinks_present

        is_documentlinks_present >> rail.Label(
            'Yes') >> trigger_attachment_child_dag

        is_documentlinks_present >> rail.Label(
            'No') >> is_user_present_in_replicon

        should_trigger_attachment_query >> rail.Label(
            'No') >> trigger_attachment_child_dag

        trigger_attachment_child_dag >> wait_for_attachment_child_dag >> \
            gather_s3_attachments >> gather_invalid_attachments >> gather_attachment_exception >> \
            link_attachments_to_objects >> is_user_present_in_replicon

        should_sync_attachments >> rail.Label(
            'No') >> is_user_present_in_replicon

        is_user_present_in_replicon >> rail.Label(
            'Yes') >> assign_co_manager_to_project >> catch_project_error
        is_user_present_in_replicon >> rail.Label(
            'No') >> catch_project_error

    return dag


rail.for_each_instance(create_child_dag)
