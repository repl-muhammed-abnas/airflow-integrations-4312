from datetime import datetime, timedelta, timezone
import rail
from airflow.models import Variable
from salesforce.project_import.utils.python_callable_method import get_new_updated_attachment_query, get_new_updated_opportunity_query
from salesforce.project_import.utils.request_payload import get_filtered_opportunities, get_opportunity_from_opportunity_ids, get_project_child_dag_item, get_project_status_sync_items, map_response


UNSUPPORTED_ATTACHMENT_EXCEPTION_MESSAGE = "Some attachment(s) were not synced due to an invalid file format or exceeding the maximum file size. Please refer to the help section for supported file formats"


# pylint:disable = too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_salesforce_{config.region.replace('-', '_')}_project_import_{config.instance}",
        description=f'Salesforce {config.region} Project Import {config.instance}',
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

        is_customfields_specified = rail.IfOperator(
            task_id='is_customfields_specified',
            test="{{ dag_run.conf.customSettings | attr_or_default('basedOnOpportunityCustomFields') | sn | is_truthy or \
                dag_run.conf.customSettings | attr_or_default('customFields') | sn | is_truthy }}",
            yes_task='trigger_customfield_dag',
            no_task='new_updated_opportunity'
        )

        trigger_customfield_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_customfield_dag',
            trigger_dag_id=f"standard_salesforce_{config.region.replace('-', '_')}_custom_field_{config.instance}",
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                **{
                    'sfobject': 'Opportunity',
                    'sfobject_fields': ['name', 'label', 'type', 'picklistValues'],
                    'bindingUri': 'urn:replicon:object-type:project',
                    'should_add_customfield_query': bool(dag_run.conf['customSettings']['basedOnOpportunityCustomFields']),
                    'should_process_oef_creation': bool(dag_run.conf['customSettings'].get('customFields'))
                },
                **{
                    k: v for k, v in dag_run.conf.items() if k not in (
                        '_ancestry', '_ecid', '_replication_position')
                }
            }
        )

        wait_for_customfield_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_customfield_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_customfield_dag") }}'
        )

        gather_customfield_dag_result = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_customfield_dag_result',
            dag_runs='{{ result("trigger_customfield_dag") }}',
            dagrun_task_id='final_result',
            flatten=True
        )

        new_updated_opportunity = rail.SalesforceQueryOperator2(
            task_id="new_updated_opportunity",
            salesforce_conn_id="{{ dag_run.conf.salesforce_conn_id }}",
            query=get_new_updated_opportunity_query
        )

        filtered_opportunities = rail.PythonOperator(
            task_id='filtered_opportunities',
            python_callable=get_filtered_opportunities
        )

        should_process_attachments = rail.IfOperator(
            task_id='should_process_attachments',
            test="{{ dag_run.conf.customSettings | attr_or_default('syncAttachmentsFromOpportunity') | sn | is_truthy }}",
            yes_task='new_updated_attachment',
            no_task='update_lastsync_time'
        )

        new_updated_attachment = rail.SalesforceQueryOperator2(
            task_id="new_updated_attachment",
            salesforce_conn_id="{{ dag_run.conf.salesforce_conn_id }}",
            query=get_new_updated_attachment_query
        )

        update_lastsync_time = rail.SetLastSyncTimeOperator(
            task_id='update_lastsync_time',
            workflow_name=config.workflow,
            provider=config.provider,
            value_to_set='{{result("get_lastsync_time_and_current_time").current_time}}'
        )

        is_new_updated_opportunity_attachment_found = rail.IfOperator(
            task_id="is_new_updated_opportunity_attachment_found",
            test="{{ result('filtered_opportunities', 'filtered') | length > 0 or \
                    (get_task_state('new_updated_attachment') == 'success' \
                    and result('new_updated_attachment', 'length') > 0) }}",
            yes_task="validate_salesforce_data",
            no_task="should_log_history"
        )

        def get_validated_salesforce_data():
            opportunity_length = len(rail.result(
                'filtered_opportunities', 'filtered'))
            attachment_length = rail.result('new_updated_attachment', 'length') if rail.render_template(
                "{{ get_task_state('new_updated_attachment') }}") == 'success' else 0

            if attachment_length > 0:
                if opportunity_length == 0:
                    return 'trigger_opportunity'
                return 'merge_collection'
            return 'skip_attachment'

        validate_salesforce_data = rail.PythonOperator(
            task_id='validate_salesforce_data',
            python_callable=get_validated_salesforce_data
        )

        is_skip_attachment = rail.IfOperator(
            task_id='is_skip_attachment',
            test="{{ result('validate_salesforce_data') == 'skip_attachment' }}",
            yes_task='get_my_actual_useridentity',
            no_task='is_collection_to_create'
        )

        is_collection_to_create = rail.IfOperator(
            task_id="is_collection_to_create",
            test="{{ result('validate_salesforce_data') == 'trigger_opportunity' or \
                result('validate_salesforce_data') == 'merge_collection' }}",
            yes_task="create_attachment_collection",
            no_task="get_my_actual_useridentity"
        )

        create_attachment_collection = rail.CreateCollectionOperator(
            task_id='create_attachment_collection',
            name='attachment',
            source=lambda: rail.result('new_updated_attachment')['records'],
            columns={
                'ContentDocumentId': 'content_document_id',
                'Id': 'id',
                'LinkedEntityId': 'linked_entity_id'
            }
        )

        query_distinct_opportunities = rail.QueryCollectionOperator(
            task_id='query_distinct_opportunities',
            name='distinct_opportunities',
            query="SELECT linked_entity_id, (SELECT GROUP_CONCAT(content_document_id) FROM \
                    (SELECT content_document_id FROM attachment WHERE linked_entity_id = t.linked_entity_id \
                        AND NULLIF(content_document_id, '') IS NOT NULL) AS subquery) \
                        AS content_document_ids FROM attachment AS t GROUP BY linked_entity_id"
        )

        is_trigger_opportunity = rail.IfOperator(
            task_id="is_trigger_opportunity",
            test="{{ result('validate_salesforce_data') == 'trigger_opportunity' }}",
            yes_task="get_opportunities",
            no_task="create_opportunity_collection"
        )

        get_opportunities = rail.SalesforceQueryOperator2(
            task_id="get_opportunities",
            salesforce_conn_id="{{ dag_run.conf.salesforce_conn_id }}",
            query=get_opportunity_from_opportunity_ids,
            data_handler=map_response
        )

        create_opportunity_collection = rail.CreateCollectionOperator(
            task_id='create_opportunity_collection',
            name='opportunity',
            source=lambda: rail.result('filtered_opportunities', 'filtered')
        )

        def opportunity_attachment_query():
            select_part = 'SELECT opportunity.Id, opportunity.Name, opportunity.Type, \
                opportunity.StageName, opportunity.Probability, opportunity.OwnerId, opportunity.AccountId, opportunity.Description, \
                    opportunity.CloseDate'
            result = rail.result('gather_customfield_dag_result')
            if result and result[0] and result[0].get('oef_list'):
                custom_fields = [
                    f"opportunity.{oef['code']}" for oef in result[0]['oef_list']]
                select_part = f"{select_part}, {', '.join(custom_fields)}"

            return f"{select_part}, distinct_opportunities.content_document_ids FROM opportunity \
                        LEFT JOIN distinct_opportunities ON opportunity.Id = distinct_opportunities.linked_entity_id \
                            WHERE NULLIF(distinct_opportunities.content_document_ids, '') IS NOT NULL"
        make_opportunity_attachment_updated_query = rail.PythonOperator(
            task_id='make_opportunity_attachment_updated_query',
            python_callable=opportunity_attachment_query
        )

        get_opportunity_attachment_updated = rail.QueryCollectionOperator(
            task_id='get_opportunity_attachment_updated',
            name='opportunity_with_attachments',
            query="{{ result('make_opportunity_attachment_updated_query') }}"
        )

        get_opportunity_updated_no_attachment = rail.QueryCollectionOperator(
            task_id='get_opportunity_updated_no_attachment',
            name='opportunity_with_no_attachments',
            query="SELECT * FROM opportunity WHERE opportunity.id NOT IN (SELECT linked_entity_id FROM distinct_opportunities)"
        )

        get_no_opportunity_attachment_updated = rail.QueryCollectionOperator(
            task_id='get_no_opportunity_attachment_updated',
            name='no_opportunity_with_attachments',
            query="SELECT * FROM distinct_opportunities WHERE linked_entity_id NOT IN (SELECT opportunity.id FROM opportunity)"
        )

        is_no_opportunity_attachment_updated = rail.IfOperator(
            task_id='is_no_opportunity_attachment_updated',
            test="{{ result('get_no_opportunity_attachment_updated', 'length') > 0 }}",
            yes_task='get_opportunities',
            no_task='get_my_actual_useridentity'
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

        def get_sf_projects():
            updated_opportunity = rail.result(
                'filtered_opportunities', 'filtered_for_status_sync')
            updated_attachments = [
                {'Name': x['opportunity_name']} for x in rail.result(get_opportunities.task_id)
            ] if rail.result(get_opportunities.task_id) else []
            return [*updated_opportunity, *updated_attachments] if updated_attachments else updated_opportunity

        def filter_projects_details(response):
            projects = [
                {
                    'uri': x['projectDetails']['uri'],
                    'name': x['projectDetails']['name'],
                    'status': x['projectDetails']['status']['name']
                } for x in response if x['projectDetails']
            ]
            rail.set_result(projects, 'replicon_projects')

        search_projects_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='search_projects_in_replicon',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails2',
            items=get_sf_projects,
            flatten=True,
            batch_size=config.bulk_get_projects_batch_size,
            data=lambda items: {
                'projects': list(map(lambda item: {'name': item['Name']}, items))
            },
            all_result_data_handler=filter_projects_details
        )

        should_update_project_status = rail.IfOperator(
            task_id='should_update_project_status',
            test=lambda dag_run: dag_run.conf['customSettings'].get(
                'toSyncStatus')
            and len(get_sf_projects()),
            yes_task='trigger_project_status_sync_dag',
            no_task='trigger_project_child_dag'
        )

        trigger_project_status_sync_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_project_status_sync_dag',
            retries=0,
            items=get_project_status_sync_items,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f"standard_salesforce_{config.region.replace('-', '_')}_project_import_status_sync_dag_{config.instance}",
            conf=lambda dag_run, item: {
                **dict(item.items()),
                **{
                    k: v for k, v in dag_run.conf.items() if k not in (
                        '_ancestry', '_ecid', '_replication_position')
                },
                **{
                    'is_polaris_project': rail.result('is_polaris_permissions_present'),
                    'replicon_projects': rail.result('search_projects_in_replicon', 'replicon_projects')
                }
            }
        )

        trigger_project_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_project_child_dag',
            retries=0,
            items=get_project_child_dag_item,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f"standard_salesforce_{config.region.replace('-', '_')}_project_import_child_dag_{config.instance}",
            conf=lambda dag_run, item: {
                **dict(item.items()),
                **{
                    'oef_list': rail.result('gather_customfield_dag_result')[0]['oef_list']
                    if rail.result('gather_customfield_dag_result') and
                    rail.result('gather_customfield_dag_result')[0] and
                    rail.result('gather_customfield_dag_result')[0].get('oef_list') else [],
                    'is_polaris_permissions_present': rail.result('is_polaris_permissions_present'),
                    'replicon_projects': rail.result(
                        'search_projects_in_replicon',
                        'replicon_projects'
                    )
                },
                **{
                    k: v for k, v in dag_run.conf.items() if k not in (
                        '_ancestry', '_ecid', '_replication_position')
                }
            }
        )

        wait_for_project_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_project_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_project_child_dag") }}'
        )

        gather_all_attachment_exceptions = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_all_attachment_exceptions',
            dag_runs="{{ result('trigger_project_child_dag') }}",
            dagrun_task_id='gather_attachment_exception',
            flatten=True
        )

        gather_project_error = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_project_error',
            dag_runs="{{ result('trigger_project_child_dag') }}",
            dagrun_task_id='catch_client_error',
            flatten=True
        )

        is_project_error = rail.IfOperator(
            task_id='is_project_error',
            test="{{ result('gather_project_error') | length > 0 }}",
            yes_task='fail_project_error',
            no_task='should_log_history'
        )

        fail_project_error = rail.FailOperator(
            task_id='fail_project_error',
            message="{{ result('gather_project_error') | map_to_attr('error') | join('|') }}"
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ not(get_task_state('is_new_updated_opportunity_attachment_found') == 'success' and \
                result('is_new_updated_opportunity_attachment_found') != 'validate_salesforce_data') }}",
            trigger_rule='all_done',
            yes_task='log_dagrun_details_to_table',
            no_task='delete_this_dagrun'
        )

        def get_exception_message():
            exceptions = list(filter(bool, rail.result(
                'gather_all_attachment_exceptions') or []))
            return UNSUPPORTED_ATTACHMENT_EXCEPTION_MESSAGE if exceptions else None
        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_dagrun_details_to_table',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            company_key='{{ dag_run.conf.company_key }}',
            connector_name='salesforce',
            integration_type='project_import',
            message=get_exception_message
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> should_log_history
        can_run_batch_task >> rail.Label(
            'No') >> get_lastsync_time_and_current_time >> is_customfields_specified

        is_customfields_specified >> rail.Label(
            'Yes') >> trigger_customfield_dag >> wait_for_customfield_child_dag >> gather_customfield_dag_result >> new_updated_opportunity

        is_customfields_specified >> rail.Label(
            'No') >> new_updated_opportunity

        new_updated_opportunity >> filtered_opportunities >> should_process_attachments

        should_process_attachments >> rail.Label(
            'Yes') >> new_updated_attachment >> update_lastsync_time
        should_process_attachments >> rail.Label(
            'No') >> update_lastsync_time >> is_new_updated_opportunity_attachment_found

        is_new_updated_opportunity_attachment_found >> rail.Label(
            'Yes') >> validate_salesforce_data >> is_skip_attachment

        is_skip_attachment >> rail.Label(
            'Yes') >> get_my_actual_useridentity
        is_skip_attachment >> rail.Label(
            'No') >> is_collection_to_create

        is_collection_to_create >> rail.Label(
            'Yes') >> create_attachment_collection >> query_distinct_opportunities >> \
            is_trigger_opportunity
        is_collection_to_create >> rail.Label(
            'No') >> get_my_actual_useridentity

        is_trigger_opportunity >> rail.Label(
            'Yes') >> get_opportunities

        is_trigger_opportunity >> rail.Label(
            'No') >> create_opportunity_collection >> make_opportunity_attachment_updated_query >> \
            get_opportunity_attachment_updated >> get_opportunity_updated_no_attachment >> \
            get_no_opportunity_attachment_updated >> is_no_opportunity_attachment_updated

        is_no_opportunity_attachment_updated >> rail.Label(
            'Yes') >> get_opportunities
        is_no_opportunity_attachment_updated >> rail.Label(
            'No') >> get_my_actual_useridentity

        get_opportunities >> get_my_actual_useridentity >> is_polaris_permissions_present

        is_polaris_permissions_present >> search_projects_in_replicon >> should_update_project_status
        should_update_project_status >> rail.Label(
            'Yes') >> trigger_project_status_sync_dag >> trigger_project_child_dag
        should_update_project_status >> rail.Label(
            'No') >> trigger_project_child_dag

        trigger_project_child_dag >> \
            wait_for_project_child_dag >> gather_all_attachment_exceptions >> \
            gather_project_error >> is_project_error

        is_project_error >> rail.Label(
            'Yes') >> fail_project_error >> should_log_history
        is_project_error >> rail.Label(
            'No') >> should_log_history

        is_new_updated_opportunity_attachment_found >> rail.Label(
            'No') >> should_log_history
        should_log_history >> rail.Label(
            'Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label(
            'No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
