# Child DAG for processing individual project records
# Handles creation and updates of Replicon Projects from Salesforce Opportunities

from datetime import timedelta
from airflow.models import Variable
import rail
from ipipeline.project_client_import.utils import request_payload, custom_methods

null = None  # For JSON null representation
true = True


def create_process_project_child_dag(config):
    """
    Create child DAG for processing individual projects
    """
    with rail.create_airflow_dag(
        dag_id=config.process_project_child_dag_id,
        description=f'iPipeline Project Client Import Process Project child {config.dag_id_suffix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        # View incoming configuration
        view_dagrun_conf = rail.ViewDagRunConfOperator(
            task_id='view_dagrun_conf'
        )

        # Check if batch task can run based on Airflow Variable
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_project_log'
        )

        # Batch task to optimize processing in batches
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='create_project_log',
            end_task='catch_and_log_error',
        )

        # Create log for project processing
        create_project_log = rail.CreateLogOperator(
            task_id='create_project_log'
        )

        # Workflow for Client representative
        is_client_present_in_replicon_and_ownerid_in_payload = rail.IfOperator(
            task_id='is_client_present_in_replicon_and_ownerid_in_payload',
            test=lambda dag_run: dag_run.conf.get(
                'client_uri') and dag_run.conf.get('OwnerId'),
            yes_task='get_existing_client_representatives_list_for_client',
            no_task='is_co_manager_in_input'
        )

        get_existing_client_representatives_list_for_client = rail.RepliconServiceOperator(
            task_id="get_existing_client_representatives_list_for_client",
            endpoint="/services/ClientService1.svc/GetClientDetails",
            data=lambda dag_run: {
                "clientUri": dag_run.conf.get('client_uri')
            },
            data_handler=lambda response: [
                client_rep['uri'] for client_rep in response['clientRepresentatives']] if response['clientRepresentatives'] else []
        )

        get_client_representative_email_from_salesforce = rail.SalesforceQueryOperator2(
            task_id="get_client_representative_email_from_salesforce",
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda dag_run: request_payload.soql_query_for_user_lookup(
                dag_run.conf['OwnerId']),
            data_handler=lambda res: request_payload.get_user_email_from_payload(
                res)
        )

        get_client_representative_details_in_replicon = rail.RepliconServiceOperator(
            task_id="get_client_representative_details_in_replicon",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda: {
                "users": [
                    {
                        "loginName": rail.result('get_client_representative_email_from_salesforce')
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: {
                'uri': response[0]['userDetails']['uri'],
                'cr_permission': rail.find_first_by_attr_and_get_attr(
                    response[0]['permissionSets'], 'displayText', 'Client Representative', 'name', '') if response[0]['permissionSets'] else '',
                'status': response[0]['userDetails']['isEnabled'],
            }if response else {}
        )

        is_client_rep_present_in_replicon_and_not_assigned_to_client = rail.IfOperator(
            task_id='is_client_rep_present_in_replicon_and_not_assigned_to_client',
            test=lambda: rail.result('get_client_representative_details_in_replicon').get('uri') and bool(rail.result(
                'get_client_representative_details_in_replicon').get('status')) and (rail.result(
                    'get_client_representative_details_in_replicon').get('uri') not in rail.result(
                        'get_existing_client_representatives_list_for_client')),
            yes_task='is_client_representative_permission_not_assigned',
            no_task='check_client_representative_assignment_exception',
        )

        is_client_representative_permission_not_assigned = rail.IfOperator(
            task_id='is_client_representative_permission_not_assigned',
            test=lambda: not rail.result(
                'get_client_representative_details_in_replicon').get('cr_permission'),
            yes_task='assign_client_representative_permission',
            no_task='get_list_of_client_representatives_to_put',
        )

        assign_client_representative_permission = rail.RepliconServiceOperator(
            task_id='assign_client_representative_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=lambda dag_run: {
                'userUri': rail.result('get_client_representative_details_in_replicon').get('uri'),
                'permissionSetUri': dag_run.conf['client_rep_permission_set_uri']
            }
        )

        get_list_of_client_representatives_to_put = rail.PythonOperator(
            task_id='get_list_of_client_representatives_to_put',
            python_callable=custom_methods.client_representatives_list_to_assign
        )

        assign_client_representative_at_client_level = rail.RepliconServiceOperator(
            task_id="assign_client_representative_at_client_level",
            endpoint="/services/ClientService1.svc/PutClientRepresentativeAssignments",
            data=lambda dag_run: {
                "clientUri": dag_run.conf.get('client_uri'),
                "clientRepresentativeUris": rail.result('get_list_of_client_representatives_to_put')
            }
        )

        check_client_representative_assignment_exception = rail.PythonOperator(
            task_id='check_client_representative_assignment_exception',
            python_callable=custom_methods.get_cr_assignment_exception
        )

        is_co_manager_in_input = rail.IfOperator(
            task_id='is_co_manager_in_input',
            test=lambda dag_run: bool(dag_run.conf['Engagement_Manager__c']),
            yes_task='get_engagement_manager_email_from_salesforce',
            no_task='is_project_manager_in_input'
        )

        get_engagement_manager_email_from_salesforce = rail.SalesforceQueryOperator2(
            task_id="get_engagement_manager_email_from_salesforce",
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda dag_run: request_payload.soql_query_for_user_lookup(
                dag_run.conf['Engagement_Manager__c']),
            data_handler=lambda res: request_payload.get_user_email_from_payload(
                res)
        )

        # Failsafe check
        check_engagement_manager_present_in_sf = rail.IfOperator(
            task_id='check_engagement_manager_present_in_sf',
            test=lambda: rail.result(
                'get_engagement_manager_email_from_salesforce'),
            yes_task='get_engagement_manager_details',
            no_task='is_project_manager_in_input'
        )

        get_engagement_manager_details = rail.RepliconServiceOperator(
            task_id="get_engagement_manager_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda: {
                "users": [
                    {
                        "loginName": rail.result('get_engagement_manager_email_from_salesforce')
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: {
                'em_uri': response[0]['userDetails']['uri'],
                'project_manager_permission': rail.find_first_by_attr_and_get_attr(
                    response[0]['permissionSets'], 'displayText', 'Project Manager', 'name', '') if response[0]['permissionSets'] else '',
                'project_manager_admin_permission': rail.find_first_by_attr_and_get_attr(
                    response[0]['permissionSets'], 'displayText', 'Project Manager Admin', 'name', '') if response[0]['permissionSets'] else '',
                'status': response[0]['userDetails']['isEnabled'],
            }if response else {}
        )

        if_em_not_found_or_pm_permission_not_available_or_em_disabled = rail.IfOperator(
            task_id='if_em_not_found_or_pm_permission_not_available_or_em_disabled',
            test=lambda: not (rail.result('get_engagement_manager_details')) or not (rail.result(
                'get_engagement_manager_details')['project_manager_permission'] or rail.result(
                    'get_engagement_manager_details')['project_manager_admin_permission']) or str(rail.result(
                        'get_engagement_manager_details')['status']).lower() == 'false',
            yes_task='log_em_assignment_exceptions',
            no_task='is_project_manager_in_input'
        )

        log_em_assignment_exceptions = rail.PythonOperator(
            task_id='log_em_assignment_exceptions',
            python_callable=lambda: ("Project manager permission not assigned to Engagement manager in replicon" if not (rail.result(
                'get_engagement_manager_details')['project_manager_permission']) else "Engagement manager is disabled in replicon") if rail.result(
                    'get_engagement_manager_details') else "Engagement manager not found in replicon"
        )

        # Workflow for project manager
        is_project_manager_in_input = rail.IfOperator(
            task_id='is_project_manager_in_input',
            test=lambda dag_run: bool(dag_run.conf['Project_Manager__c']),
            yes_task='get_project_manager_email_from_salesforce',
            no_task='check_action_to_be_done'
        )

        get_project_manager_email_from_salesforce = rail.SalesforceQueryOperator2(
            task_id="get_project_manager_email_from_salesforce",
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda dag_run: request_payload.soql_query_for_user_lookup(
                dag_run.conf['Project_Manager__c']),
            data_handler=lambda res: request_payload.get_user_email_from_payload(
                res)
        )

        # Failsafe check
        check_project_manager_present_in_sf = rail.IfOperator(
            task_id='check_project_manager_present_in_sf',
            test=lambda: rail.result(
                'get_project_manager_email_from_salesforce'),
            yes_task='get_project_manager_details',
            no_task='check_action_to_be_done'
        )

        get_project_manager_details = rail.RepliconServiceOperator(
            task_id="get_project_manager_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda: {
                "users": [
                    {
                        "loginName": rail.result('get_project_manager_email_from_salesforce')
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: {
                'pm_uri': response[0]['userDetails']['uri'],
                'project_manager_permission': rail.find_first_by_attr_and_get_attr(
                    response[0]['permissionSets'], 'displayText', 'Project Manager', 'name', '') if response[0]['permissionSets'] else '',
                'project_manager_admin_permission': rail.find_first_by_attr_and_get_attr(
                    response[0]['permissionSets'], 'displayText', 'Project Manager Admin', 'name', '') if response[0]['permissionSets'] else '',
                'status': response[0]['userDetails']['isEnabled'],
            }if response else {}
        )

        if_pm_not_found_or_pm_permission_not_available_or_pm_disabled = rail.IfOperator(
            task_id='if_pm_not_found_or_pm_permission_not_available_or_pm_disabled',
            test=lambda: not (rail.result('get_project_manager_details')) or not (rail.result(
                'get_project_manager_details')['project_manager_permission'] or (rail.result(
                    'get_project_manager_details')['project_manager_admin_permission'])) or str(rail.result(
                        'get_project_manager_details')['status']).lower() == 'false',
            yes_task='log_pm_not_found_or_pm_permission_not_found_or_disabled',
            no_task='check_action_to_be_done'
        )

        log_pm_not_found_or_pm_permission_not_found_or_disabled = rail.PythonOperator(
            task_id='log_pm_not_found_or_pm_permission_not_found_or_disabled',
            python_callable=lambda: ("Project manager permission not assigned to project manager in replicon" if not (rail.result(
                'get_project_manager_details')['project_manager_permission']) else "Project manager is disabled in replicon") if rail.result(
                    'get_project_manager_details') else "Project manager not found in replicon"
        )

        # Determine if this is an update or create operation based on project_uri presence
        check_action_to_be_done = rail.IfOperator(
            task_id='check_action_to_be_done',
            test=lambda dag_run: bool(dag_run.conf.get('project_uri')),
            yes_task='get_project_details',
            no_task='validate_fields_for_project_creation'
        )

        # Get existing project details for update
        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=lambda dag_run: {
                "projects": [
                    {
                        "uri": dag_run.conf.get('project_uri')
                    }
                ]
            },
            data_handler=lambda resp: resp[0]['projectDetails'] if resp and resp[0].get(
                'projectDetails') else None
        )

        update_project = rail.RepliconServiceOperator(
            task_id='update_project',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=lambda dag_run: request_payload.get_update_project_payload(
                dag_run, rail.result('get_project_details'), config)
        )

        is_tcv_updated = rail.IfOperator(
            task_id='is_tcv_updated',
            test=lambda: bool(rail.result('update_project', 'is_tcv_updated')) and config.instance in ['trial','uat'],
            yes_task='get_revenue_contract_details_for_project',
            no_task='if_comanager_in_input_and_validated_in_replicon'
        )

        get_revenue_contract_details_for_project = rail.RepliconServiceOperator(
            task_id='get_revenue_contract_details_for_project',
            endpoint='/services/RevenueContractProjectService1.svc/GetContractDetailsForProject',
            data=lambda dag_run: {
                "project": {
                    "uri": dag_run.conf.get('project_uri')
                }
            },
            data_handler=lambda resp: custom_methods.get_project_contract_details(
                resp)
        )

        is_revenue_contract_script_eligible_for_update = rail.IfOperator(
            task_id='is_revenue_contract_script_eligible_for_update',
            test=lambda: (rail.result('get_revenue_contract_details_for_project').get(
                'script_name') in config.REVENUE_CONTRACT_POLICIES_ALLOWED_FOR_UPDATE),
            yes_task='put_updated_revenue_contract_details_for_project',
            no_task='if_comanager_in_input_and_validated_in_replicon'
        )

        put_updated_revenue_contract_details_for_project = rail.RepliconServiceOperator(
            task_id='put_updated_revenue_contract_details_for_project',
            endpoint='/services/RevenueContractProjectService1.svc/PutContractToProject',
            data=lambda dag_run: custom_methods.get_revenue_contract_payload(
                dag_run)
        )

        if_comanager_in_input_and_validated_in_replicon = rail.IfOperator(
            task_id='if_comanager_in_input_and_validated_in_replicon',
            test=lambda dag_run: bool(dag_run.conf['Engagement_Manager__c']) and not (
                rail.result('log_em_assignment_exceptions')),
            yes_task='assign_comanager_to_project',
            no_task='log_project_process_completion'
        )

        assign_comanager_to_project = rail.RepliconServiceOperator(
            task_id="assign_comanager_to_project",
            endpoint="/services/ProjectService1.svc/PutExplicitSharingAssignments",
            data=lambda dag_run: {
                "projectUri": dag_run.conf.get('project_uri'),
                "sharedUris": [(rail.result('get_engagement_manager_details')['em_uri'])]
            }
        )

        # Validate fields for project creation
        validate_fields_for_project_creation = rail.PythonOperator(
            task_id='validate_fields_for_project_creation',
            python_callable=lambda dag_run: request_payload.validate_mandatory_fields_for_creation(
                dag_run, config)
        )

        # Check if mandatory field validation failed
        if_invalid_fields = rail.IfOperator(
            task_id='if_invalid_fields',
            test=lambda: rail.result('validate_fields_for_project_creation'),
            yes_task='log_exception_for_invalid_fields',
            no_task='create_project_copy_batch'
        )

        # Log validation exceptions for missing mandatory fields
        log_exception_for_invalid_fields = rail.WriteLogOperator(
            task_id='log_exception_for_invalid_fields',
            log='{{ result("create_project_log") }}',
            severity='Exception',
            message='Validation failed',
            properties=lambda dag_run: {
                'name': dag_run.conf.get('Name'),
                'id': dag_run.conf.get('Id'),
                'type': 'opportunity',
                'action': 'Add',
                'status': 'Exception',
                'details': 'Project not created ; ' + ' ; '.join(rail.result('validate_fields_for_project_creation'))
            }
        )

        create_project_copy_batch = rail.RepliconServiceOperator(
            task_id='create_project_copy_batch',
            endpoint='/services/ProjectService1.svc/CreateProjectCopyBatch2',
            data=request_payload.get_project_copy_batch_param
        )

        execute_projects_batch, wait_for_batch_completion = rail.batch_execution(
            'execute_projects_batch', create_project_copy_batch.task_id,
        )

        get_projectcopy_uri = rail.RepliconServiceOperator(
            task_id='get_projectcopy_uri',
            endpoint='/services/ProjectService1.svc/GetProjectCopyBatchResults',
            data=lambda: {"projectCopyBatchUri": rail.result(
                'create_project_copy_batch')},
            data_handler=lambda res: res.get('project', {}).get(
                'uri', '') if res and not (res.get('error')) else ''
        )

        # Apply modifications to project created using duplication
        apply_modifications_to_created_project = rail.RepliconServiceOperator(
            task_id='apply_modifications_to_created_project',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=lambda dag_run: request_payload.get_create_project_payload(
                dag_run, config)
        )

        if_comanager_in_input_and_validated_for_new_project = rail.IfOperator(
            task_id='if_comanager_in_input_and_validated_for_new_project',
            test=lambda dag_run: bool(dag_run.conf['Engagement_Manager__c']) and not (
                rail.result('log_em_assignment_exceptions')),
            yes_task='assign_comanager_to_new_project',
            no_task='log_project_process_completion'
        )

        assign_comanager_to_new_project = rail.RepliconServiceOperator(
            task_id="assign_comanager_to_new_project",
            endpoint="/services/ProjectService1.svc/PutExplicitSharingAssignments",
            data=lambda: {
                "projectUri": rail.result("apply_modifications_to_created_project")['uri'],
                "sharedUris": [rail.result('get_engagement_manager_details')['em_uri']]
            }
        )

        # Log successful project processing
        log_project_process_completion = rail.WriteLogOperator(
            task_id='log_project_process_completion',
            log='{{ result("create_project_log") }}',
            severity=lambda dag_run: custom_methods.get_processing_details(
                'Project', 'status', dag_run.conf.get('project_uri')),
            message="Project processing Completed",
            properties=lambda dag_run: {
                'name': dag_run.conf.get('Name'),
                'id': dag_run.conf.get('Id'),
                'type': 'opportunity',
                'action': 'Update' if dag_run.conf.get('project_uri') else 'Add',
                'status': custom_methods.get_processing_details('Project', 'status', dag_run.conf.get('project_uri')),
                'details': custom_methods.get_processing_details('Project', 'details', dag_run.conf.get('project_uri')),
            }
        )

        # Catch and log any errors
        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log='{{ result("create_project_log") }}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'name': dag_run.conf.get('Name'),
                'id': dag_run.conf.get('Id'),
                'type': 'opportunity',
                'action': 'Update' if dag_run.conf.get('project_uri') else 'Add',
                'status': 'Error',
                'details': "{{ get_error_message() }}"
            }
        )

        # Batch task flow
        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            "No") >> create_project_log

        # Main processing flow
        create_project_log >> is_client_present_in_replicon_and_ownerid_in_payload

        is_client_present_in_replicon_and_ownerid_in_payload >> rail.Label(
            "No") >> is_co_manager_in_input
        is_client_present_in_replicon_and_ownerid_in_payload >> rail.Label(
            "Yes") >> get_existing_client_representatives_list_for_client >> get_client_representative_email_from_salesforce \
            >> get_client_representative_details_in_replicon >> is_client_rep_present_in_replicon_and_not_assigned_to_client

        is_client_rep_present_in_replicon_and_not_assigned_to_client >> rail.Label(
            "No") >> check_client_representative_assignment_exception >> is_co_manager_in_input
        is_client_rep_present_in_replicon_and_not_assigned_to_client >> rail.Label(
            "Yes") >> is_client_representative_permission_not_assigned

        is_client_representative_permission_not_assigned >> rail.Label(
            "No") >> get_list_of_client_representatives_to_put
        is_client_representative_permission_not_assigned >> rail.Label(
            "Yes") >> assign_client_representative_permission >> get_list_of_client_representatives_to_put

        get_list_of_client_representatives_to_put >> assign_client_representative_at_client_level >> is_co_manager_in_input

        is_co_manager_in_input >> rail.Label(
            "No") >> is_project_manager_in_input
        is_co_manager_in_input >> rail.Label(
            "Yes") >> get_engagement_manager_email_from_salesforce >> check_engagement_manager_present_in_sf

        check_engagement_manager_present_in_sf >> rail.Label(
            "No") >> is_project_manager_in_input
        check_engagement_manager_present_in_sf >> rail.Label(
            "Yes") >> get_engagement_manager_details >> if_em_not_found_or_pm_permission_not_available_or_em_disabled

        if_em_not_found_or_pm_permission_not_available_or_em_disabled >> rail.Label(
            "No") >> is_project_manager_in_input
        if_em_not_found_or_pm_permission_not_available_or_em_disabled >> rail.Label(
            "Yes") >> log_em_assignment_exceptions >> is_project_manager_in_input

        is_project_manager_in_input >> rail.Label(
            "Yes") >> get_project_manager_email_from_salesforce >> check_project_manager_present_in_sf

        check_project_manager_present_in_sf >> rail.Label(
            "Yes") >> get_project_manager_details
        check_project_manager_present_in_sf >> rail.Label(
            "No") >> check_action_to_be_done

        get_project_manager_details >> if_pm_not_found_or_pm_permission_not_available_or_pm_disabled

        if_pm_not_found_or_pm_permission_not_available_or_pm_disabled >> rail.Label(
            'Yes') >> log_pm_not_found_or_pm_permission_not_found_or_disabled >> check_action_to_be_done
        if_pm_not_found_or_pm_permission_not_available_or_pm_disabled >> rail.Label(
            'No') >> check_action_to_be_done

        is_project_manager_in_input >> rail.Label(
            "No") >> check_action_to_be_done

        # Update path
        check_action_to_be_done >> rail.Label(
            "Update") >> get_project_details >> update_project >> is_tcv_updated

        is_tcv_updated >> rail.Label(
            "No") >> if_comanager_in_input_and_validated_in_replicon
        is_tcv_updated >> rail.Label(
            "Yes") >> get_revenue_contract_details_for_project >> is_revenue_contract_script_eligible_for_update

        is_revenue_contract_script_eligible_for_update >> rail.Label(
            "No") >> if_comanager_in_input_and_validated_in_replicon
        is_revenue_contract_script_eligible_for_update >> rail.Label(
            "Yes") >> put_updated_revenue_contract_details_for_project >> if_comanager_in_input_and_validated_in_replicon

        if_comanager_in_input_and_validated_in_replicon >> rail.Label(
            "No") >> log_project_process_completion
        if_comanager_in_input_and_validated_in_replicon >> rail.Label(
            "Yes") >> assign_comanager_to_project >> log_project_process_completion

        # Create path
        check_action_to_be_done >> rail.Label(
            "Add") >> validate_fields_for_project_creation >> if_invalid_fields

        if_invalid_fields >> rail.Label(
            "Yes") >> log_exception_for_invalid_fields >> catch_and_log_error
        if_invalid_fields >> rail.Label(
            "No") >> create_project_copy_batch >> execute_projects_batch

        execute_projects_batch >> wait_for_batch_completion >> get_projectcopy_uri >> apply_modifications_to_created_project \
            >> if_comanager_in_input_and_validated_for_new_project

        if_comanager_in_input_and_validated_for_new_project >> rail.Label(
            "No") >> log_project_process_completion
        if_comanager_in_input_and_validated_for_new_project >> rail.Label(
            "Yes") >> assign_comanager_to_new_project >> log_project_process_completion

        log_project_process_completion

        log_project_process_completion >> catch_and_log_error

    return dag


rail.for_each_instance(create_process_project_child_dag)
