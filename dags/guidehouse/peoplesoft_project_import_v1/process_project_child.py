from datetime import timedelta
import rail
from airflow.models import Variable
from guidehouse.peoplesoft_project_import_v1.utils import request_payload, response_filter, custom_method

def create_project_processing_dags(config):
    """Create project processing child DAGs with batch execution support"""

    add_dags = []

    for idx in range(0, config.PROJECT_BATCH_COUNT):
        get_postfix = "" if idx == 0 else f'_batch_{idx}'

        with rail.create_airflow_dag(
            dag_id=f"{config.process_project_dag_id}{get_postfix}",
            description='Guidehouse Process Each Project Child - Phase 1',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_child,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='create_project_log'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(days=config.execution_timeout_days),
                start_task='create_project_log',
                end_task='catch_and_log_errors',
            )

            create_project_log= rail.CreateLogOperator(
                task_id="create_project_log",
            )

            get_project_data_from_query = rail.QueryCollectionOperator(
                task_id='get_project_data_from_query',
                query="""SELECT * FROM validdata WHERE project_id == :project_id""",
                query_params={
                    'project_id': '{{ dag_run.conf.project_id }}'
                }
            )

            load_project_data_from_query = rail.PythonOperator(
                task_id="load_project_data_from_query",
                python_callable=lambda: rail.load_all_records(rail.result("get_project_data_from_query"))[0]
            )

            validate_optional_fields = rail.PythonOperator(
                task_id='validate_optional_fields',
                python_callable=lambda: custom_method.validate_optional_fields(
                    rail.result("load_project_data_from_query")
                )
            )

            validate_dates = rail.PythonOperator(
                task_id="validate_dates",
                python_callable=lambda: custom_method.validate_project_dates_only(rail.result(
                    "load_project_data_from_query"), config.DATE_FORMAT_INPUT, config.MAX_FIELD_LENGTH)
            )

            are_project_dates_valid = rail.IfOperator(
                task_id='are_project_dates_valid',
                test=lambda: rail.result("validate_dates")['is_valid'],
                yes_task='get_project_details',
                no_task='log_project_date_validation_error'
            )

            log_project_date_validation_error = rail.WriteLogOperator(
                task_id="log_project_date_validation_error",
                log='{{ result("create_project_log") }}',
                severity="Exception",
                message=lambda: "; ".join(rail.result("validate_dates")['errors']),
                properties=lambda: custom_method.get_guidehouse_task_log_properties(
                    rail.result("load_project_data_from_query"),
                    {'activity': '', 'activity_descr': ''},
                    "Validation",
                    "Exception",
                    "; ".join(rail.result("validate_dates")['errors'])
                )
            )

            get_project_details = rail.RepliconServiceOperator(
                task_id="get_project_details",
                endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
                data=lambda dag_run: {
                    "projects": custom_method.get_projects_for_lookup(dag_run)
                },
                data_handler=lambda response: custom_method.process_project_lookup_response(response)
            )

            get_source_system_dropdown = rail.RepliconServiceOperator(
                task_id='get_source_system_dropdown',
                endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
                data=lambda dag_run: {
                    "customFieldUri": dag_run.conf["sourcesystem_custom_field_uri"]
                },
                data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'PeopleSoft', 'uri', '')
            )

            get_enforce_dropdown = rail.RepliconServiceOperator(
                task_id='get_enforce_dropdown',
                endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
                data=lambda dag_run: {
                    "customFieldUri": dag_run.conf["enforce_custom_field_uri"]
                },
                data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                    response, 'displayText',
                    'Yes' if rail.result("load_project_data_from_query")['enforce'].upper() == 'YES' else 'No',
                    'uri', ''
                )
            )

            has_project_type = rail.IfOperator(
                task_id='has_project_type',
                test=lambda: bool(rail.result("load_project_data_from_query")['project_type']),
                yes_task='get_project_type_dropdown',
                no_task='has_task_type'
            )

            get_project_type_dropdown = rail.RepliconServiceOperator(
                task_id='get_project_type_dropdown',
                endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
                data=lambda dag_run: {
                    "customFieldUri": dag_run.conf["projecttype_custom_field_uri"]
                },
                data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                    response, 'displayText',
                    rail.result("load_project_data_from_query")['project_type'], 'uri', ''
                )
            )

            has_task_type = rail.IfOperator(
                task_id='has_task_type',
                test=lambda: bool(rail.result("load_project_data_from_query")['activity_type']),
                yes_task='get_task_type_dropdown',
                no_task='is_project_available'
            )

            get_task_type_dropdown = rail.RepliconServiceOperator(
                task_id='get_task_type_dropdown',
                endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
                data=lambda dag_run: {
                    "customFieldUri": dag_run.conf["task_custom_field_uri"]
                },
                data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                    response, 'displayText',
                    rail.result("load_project_data_from_query")['activity_type'], 'uri', ''
                )
            )

            is_project_available = rail.IfOperator(
                task_id = 'is_project_available',
                test=lambda: bool(rail.result("get_project_details").get('current_project')),
                yes_task= 'update_project',
                no_task= 'create_project_in_replicon'
            )

            create_project_in_replicon = rail.RepliconServiceOperator(
                task_id="create_project_in_replicon",
                endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
                data=request_payload.create_project_rest_api,
                data_handler=response_filter.extract_project_uri_from_rest_api
            )

            remove_all_users_timesheet_access = rail.RepliconServiceOperator(
                task_id="remove_all_users_timesheet_access",
                endpoint="/graphql",
                app="polaris",
                data=request_payload.remove_all_users_timesheet_access
            )

            update_project = rail.RepliconServiceOperator(
                task_id="update_project",
                endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
                data=request_payload.create_project_with_tasks_payload
            )

            is_project_manager_is_present = rail.IfOperator(
                task_id = 'is_project_manager_is_present',
                test=lambda: bool(rail.result("load_project_data_from_query").get('project_manager')),
                yes_task= 'get_project_manager_in_replicon',
                no_task= 'has_cp_project'
            )

            get_project_manager_in_replicon = rail.RepliconServiceOperator(
                task_id= 'get_project_manager_in_replicon',
                endpoint="/services/ImportService1.svc/BulkGetUsers3",
                data=lambda: {
                    "users": [
                        {
                            "employeeId": rail.result("load_project_data_from_query").get('project_manager', ''),
                        }
                    ],
                    "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
                },
                data_handler=lambda response: response_filter.validate_project_manager_enabled(
                    response[0].get('userDetails') if response and response[0].get('userDetails') else None
                )
            )

            is_project_manager_enabled = rail.IfOperator(
                task_id='is_project_manager_enabled',
                test=lambda: rail.result("get_project_manager_in_replicon")["is_enabled"],
                yes_task='get_assigned_pm_permissions',
                no_task='has_cp_project'
            )

            get_assigned_pm_permissions = rail.RepliconServiceOperator(
                task_id='get_assigned_pm_permissions',
                endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
                data=lambda: {
                    "userUri": rail.result("get_project_manager_in_replicon")["user_details"]["uri"]
                }
            )

            determine_missing_permissions = rail.PythonOperator(
                task_id='determine_missing_permissions',
                python_callable=lambda dag_run: custom_method.get_missing_permission_sets(
                    rail.result("get_assigned_pm_permissions"),
                    dag_run.conf.get('project_management_permission_set_uri', '')
                )
            )

            has_missing_permissions = rail.IfOperator(
                task_id='has_missing_permissions',
                test=lambda: len(rail.result("determine_missing_permissions")) > 0,
                yes_task='assign_missing_permissions',
                no_task='assign_project_manager_to_project'
            )

            assign_missing_permissions = rail.RepliconServiceOperator(
                task_id="assign_missing_permissions",
                endpoint="/services/ImportService1.svc/ApplyUserModifications3",
                data=request_payload.assign_pm_permissions_payload
            )

            assign_project_manager_to_project = rail.RepliconServiceOperator(
                task_id='assign_project_manager_to_project',
                endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
                data=lambda: {
                    "projectUri": request_payload.get_project_uri(),
                    "userUri": rail.result("get_project_manager_in_replicon")['user_details']['uri']
                }
            )

            # ========== IWO (Inter company worker order) Project Linking Tasks ==========

            has_cp_project = rail.IfOperator(
                task_id='has_cp_project',
                test=lambda: bool(
                    rail.result("get_project_details").get('parent_project') or
                    rail.result("get_project_details").get('parent_project_error')
                ),
                yes_task='validate_parent_project',
                no_task='combine_parent_and_csv_co_managers'
            )

            validate_parent_project = rail.PythonOperator(
                task_id='validate_parent_project',
                python_callable=lambda: custom_method.validate_parent_project_for_linking()
            )

            check_existing_project_links = rail.RepliconServiceOperator(
                task_id="check_existing_project_links",
                endpoint="/services/ProjectService1.svc/GetLinksForProject",
                data=lambda: {"project": {"uri": rail.result("get_project_details")['parent_project']['uri']}}
            )

            should_create_project_link = rail.IfOperator(
                task_id='should_create_project_link',
                test=lambda: custom_method.should_create_iwo_project_link(),
                yes_task='create_iwo_project_link',
                no_task='get_parent_project_leader'
            )

            create_iwo_project_link = rail.RepliconServiceOperator(
                task_id="create_iwo_project_link",
                endpoint="/services/ProjectService1.svc/CreateProjectLink",
                data=lambda: {
                    "baseProject": {"code": rail.result("load_project_data_from_query").get('cp_project', '')},
                    "targetProject": {"uri": request_payload.get_project_uri()},
                    "projectLinkTypeUri": "urn:replicon:project-link-type:relates-to"
                }
            )

            get_parent_project_leader = rail.PythonOperator(
                task_id='get_parent_project_leader',
                python_callable=lambda: custom_method.extract_parent_project_leader()
            )

            get_parent_project_managers = rail.RepliconServiceOperator(
                task_id="get_parent_project_managers",
                endpoint="/services/ProjectService1.svc/GetExplicitSharingAssignments",
                data=lambda: {"projectUri": rail.result("get_project_details")['parent_project']['uri']},
                data_handler=lambda response: custom_method.extract_parent_project_manager_uris(response)
            )

            combine_parent_and_csv_co_managers = rail.PythonOperator(
                task_id='combine_parent_and_csv_co_managers',
                python_callable=lambda: custom_method.combine_parent_and_csv_co_managers()
            )

            check_validation_result = rail.IfOperator(
                task_id='check_validation_result',
                test=lambda: rail.result('validate_parent_project', {}).get('should_continue', True),
                yes_task='check_existing_project_links',
                no_task='combine_parent_and_csv_co_managers'
            )

            process_co_managers = rail.PythonOperator(
                task_id='process_co_managers',
                python_callable=lambda: custom_method.parse_co_managers(
                    rail.result("load_project_data_from_query").get('co_manager', '')
                )
            )

            has_co_managers = rail.IfOperator(
                task_id='has_co_managers',
                test=lambda: custom_method.has_any_co_managers_combined(),
                yes_task='get_co_managers_in_replicon',
                no_task='log_project_success'
            )

            get_co_managers_in_replicon = rail.RepliconServiceOperator(
                task_id='get_co_managers_in_replicon',
                endpoint="/services/ImportService1.svc/BulkGetUsers3",
                data=lambda: {
                    "users": [{"employeeId": emp_id} for emp_id in rail.result("process_co_managers")],
                    "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
                },
                data_handler=response_filter.get_co_manager_users_from_response
            )

            are_co_managers_available = rail.IfOperator(
                task_id='are_co_managers_available',
                test=lambda: custom_method.are_any_co_managers_available(),
                yes_task='get_assigned_co_manager_permissions',
                no_task='log_project_success'
            )

            get_assigned_co_manager_permissions = rail.RepliconServiceOperator(
                task_id='get_assigned_co_manager_permissions',
                endpoint="/services/PermissionSetService1.svc/BulkGetAssignedPermissionSetsForUsers",
                data=lambda: custom_method.get_all_co_manager_uris_for_permission_check()
            )

            determine_co_manager_missing_permissions = rail.PythonOperator(
                task_id='determine_co_manager_missing_permissions',
                python_callable=lambda dag_run: custom_method.get_co_manager_missing_permission_sets(
                    rail.result("get_assigned_co_manager_permissions"),
                    dag_run.conf.get('project_management_permission_set_uri', '')
                )
            )

            has_co_manager_missing_permissions = rail.IfOperator(
                task_id='has_co_manager_missing_permissions',
                test=lambda: len(rail.result("determine_co_manager_missing_permissions")) > 0,
                yes_task='assign_co_manager_missing_permissions',
                no_task='get_existing_sharing_assignments'
            )

            assign_co_manager_missing_permissions = rail.RepliconServiceCallForEachItemOperator(
                task_id="assign_co_manager_missing_permissions",
                items='{{ result("determine_co_manager_missing_permissions") | to_json }}',
                endpoint="/services/ImportService1.svc/ApplyUserModifications3",
                data=request_payload.assign_co_manager_permissions_payload
            )

            get_existing_sharing_assignments = rail.RepliconServiceOperator(
                task_id='get_existing_sharing_assignments',
                endpoint="/services/ProjectService1.svc/GetExplicitSharingAssignments",
                data=lambda: {
                    "projectUri": request_payload.get_project_uri()
                }
            )

            should_assign_co_managers = rail.IfOperator(
                task_id='should_assign_co_managers',
                test=lambda: custom_method.should_update_co_manager_assignments(),
                yes_task='assign_co_managers_to_project',
                no_task='log_project_success'
            )

            assign_co_managers_to_project = rail.RepliconServiceOperator(
                task_id='assign_co_managers_to_project',
                endpoint="/services/ProjectService1.svc/PutExplicitSharingAssignments",
                data=lambda: request_payload.get_co_manager_sharing_payload()
            )

            def _get_project_log_properties():
                log_result = request_payload.get_log_message()  # SINGLE CALL
                return custom_method.get_guidehouse_task_log_properties(
                    rail.result("load_project_data_from_query"),
                    {'activity': '', 'activity_descr': ''},
                    "Update" if request_payload.does_wbs_exist() else "Add",
                    log_result['status'],
                    log_result['message']
                )

            log_project_success = rail.WriteLogOperator(
                task_id="log_project_success",
                log='{{ result("create_project_log") }}',
                message="Project synced successfully",
                properties=lambda: _get_project_log_properties()
            )

            is_new_project = rail.IfOperator(
                task_id='is_new_project',
                test=lambda: not request_payload.does_wbs_exist(),
                yes_task='get_all_task_to_add_update',
                no_task='get_all_tasks_for_project'
            )

            get_all_tasks_for_project = rail.RepliconServiceOperator(
                task_id="get_all_tasks_for_project",
                endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
                data=lambda: {
                    "parentUri": request_payload.get_project_uri()
                },
                data_handler=response_filter.format_existing_tasks
            )

            get_all_task_to_add_update = rail.PythonOperator(
                task_id="get_all_task_to_add_update",
                python_callable=lambda: custom_method.get_task_to_add_update_skip(config.DATE_FORMAT_INPUT)
            )

            create_add_task_batches = rail.PythonOperator(
                task_id='create_add_task_batches',
                python_callable=lambda: custom_method.get_batched_tasks('add', 500)
            )

            has_tasks_to_add = rail.IfOperator(
                task_id='has_tasks_to_add',
                test='{{ result("create_add_task_batches") | is_truthy }}',
                yes_task='add_task_batches',
                no_task='process_enforce_logic'
            )

            add_task_batches = rail.RepliconServiceCallForEachItemOperator(
                task_id="add_task_batches",
                items='{{ result("create_add_task_batches") | to_json }}',
                endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
                data=request_payload.get_batched_add_task_payload
            )

            get_all_project_team_members = rail.RepliconServiceOperator(
                task_id='get_all_project_team_members',
                endpoint='/services/ProjectService1.svc/BulkGetAllProjectTeamMembers2',
                data=lambda: {
                    'projectUris': [
                        request_payload.get_project_uri()
                    ]
                },
                data_handler=lambda response: custom_method.create_resource_assignment_batches_from_response(
                    response,
                    custom_method.extract_newly_added_task_uris_from_batches(),
                    config.RESOURCE_ASSIGNMENT_BATCH_SIZE
                )
            )

            has_resource_batches_to_assign = rail.IfOperator(
                task_id='has_resource_batches_to_assign',
                test=lambda: len(rail.result("get_all_project_team_members", [])) > 0,
                yes_task='trigger_add_resource_child_dags',
                no_task='process_enforce_logic'
            )

            trigger_add_resource_child_dags = rail.TriggerDagRunForEachItemOperator(
                task_id='trigger_add_resource_child_dags',
                items='{{ result("get_all_project_team_members") | to_json }}',
                trigger_dag_id=config.process_add_resource_dag_id,
                conf=lambda item: {
                    "task_uri": item['task_uri'],
                    "resource_uris": item['resource_uris']
                },
                execution_timeout=timedelta(days=config.execution_timeout_days)
            )

            wait_for_resource_assignment = rail.WaitForDagRunsSensor(
                task_id="wait_for_resource_assignment",
                dag_runs="{{result('trigger_add_resource_child_dags')}}",
                execution_timeout=timedelta(days=config.execution_timeout_days)
            )

            log_task_added = rail.WriteLogOperator(
                task_id="log_task_added",
                log='{{ result("create_project_log") }}',
                message="{{ item.details }}",
                items=lambda: custom_method.map_task_success_error("add_task_batches", "add"),
                properties=lambda item: custom_method.get_guidehouse_task_log_properties(
                    rail.result("load_project_data_from_query"),
                    item,
                    "Add",
                    item.get('status', 'Success'),
                    item.get('details', 'Task added successfully')
                )
            )

            process_enforce_logic = rail.IfOperator(
                task_id='process_enforce_logic',
                test=lambda: rail.result("load_project_data_from_query").get('enforce', '').upper() == 'NO',
                yes_task='get_all_project_tasks_for_enforce',
                no_task='log_task_added'
            )

            get_all_project_tasks_for_enforce = rail.PythonOperator(
                task_id='get_all_project_tasks_for_enforce',
                python_callable=lambda: custom_method.get_all_project_task_uris_for_enforce()
            )

            assign_groups_to_project_team = rail.RepliconServiceOperator(
                task_id='assign_groups_to_project_team',
                endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
                data=lambda dag_run: {
                    "projectUri": request_payload.get_project_uri(),
                    "resourceUri": [
                        dag_run.conf["peoplesoft_service_center_uri"],
                        dag_run.conf["india_service_center_uri"]
                    ],
                    "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
                }
            )

            trigger_service_center_task_assignment = rail.TriggerDagRunForEachItemOperator(
                task_id='trigger_service_center_task_assignment',
                items='{{ result("get_all_project_tasks_for_enforce") | to_json }}',
                trigger_dag_id=config.process_add_resource_dag_id,
                conf=lambda item, dag_run: {
                    "task_uri": item['uri'],
                    "resource_uris": [
                        dag_run.conf["peoplesoft_service_center_uri"],
                        dag_run.conf["india_service_center_uri"]
                    ]
                },
                execution_timeout=timedelta(days=config.execution_timeout_days)
            )

            wait_for_service_center_assignment = rail.WaitForDagRunsSensor(
                task_id="wait_for_service_center_assignment",
                dag_runs="{{ result('trigger_service_center_task_assignment') }}",
                execution_timeout=timedelta(days=config.execution_timeout_days)
            )

            create_update_task_batches = rail.PythonOperator(
                task_id='create_update_task_batches',
                python_callable=lambda: custom_method.get_batched_tasks('update', 500)
            )

            has_tasks_to_update = rail.IfOperator(
                task_id='has_tasks_to_update',
                test='{{ result("create_update_task_batches") | is_truthy }}',
                yes_task='update_task_batches',
                no_task='has_tasks_to_skip'
            )

            update_task_batches = rail.RepliconServiceCallForEachItemOperator(
                task_id="update_task_batches",
                items='{{ result("create_update_task_batches") | to_json }}',
                endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
                data=request_payload.get_batched_update_task_payload
            )

            log_task_updated = rail.WriteLogOperator(
                task_id="log_task_updated",
                log='{{ result("create_project_log") }}',
                message="{{ item.details }}",
                items=lambda: custom_method.map_task_success_error("update_task_batches", "update"),
                properties=lambda item: custom_method.get_guidehouse_task_log_properties(
                    rail.result("load_project_data_from_query"),
                    item,
                    "Update",
                    item.get('status', 'Success'),
                    item.get('details', 'Task updated successfully')
                )
            )

            has_tasks_to_skip = rail.IfOperator(
                task_id='has_tasks_to_skip',
                test='{{ result("get_all_task_to_add_update").skip | length > 0 }}',
                yes_task='log_task_skipped',
                no_task='catch_and_log_errors'
            )

            log_task_skipped = rail.WriteLogOperator(
                task_id="log_task_skipped",
                log='{{ result("create_project_log") }}',
                severity="Exception",
                message="{{ item.message }}",
                items=lambda: rail.result("get_all_task_to_add_update", {}).get('skip', []),
                properties=lambda item: custom_method.get_guidehouse_task_log_properties(
                    rail.result("load_project_data_from_query"),
                    {
                        'activity': item.get('taskcode', ''),
                        'activity_descr': item.get('taskname', '')
                    },
                    item.get('action', 'Skip'),
                    item.get('status', 'Skipped'),
                    item.get('message', 'Task skipped due to validation errors')
                )
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                trigger_rule='one_failed',
                log='{{ result("create_project_log") }}',
                message='{{ get_error_message() }}',
                severity= 'Error',
                properties=lambda: custom_method.get_guidehouse_task_log_properties(
                    rail.result("load_project_data_from_query"),
                    {'activity': '', 'activity_descr': ''},
                    "Add",
                    "Error",
                    '{{ get_error_message() }}'
                )
            )

            log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id='log_to_sumo',
                trigger_rule='all_done',
                sumo_conn_id='sumologic-dagrunlogger',
            )

            can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> create_project_log

            create_project_log >> get_project_data_from_query >> load_project_data_from_query >> validate_optional_fields >> validate_dates >> are_project_dates_valid

            are_project_dates_valid >> rail.Label('Yes') >> get_project_details >> get_source_system_dropdown >> get_enforce_dropdown >> has_project_type

            has_project_type >> rail.Label('Yes') >> get_project_type_dropdown >> has_task_type
            has_project_type >> rail.Label('No') >> has_task_type

            has_task_type >> rail.Label('Yes') >> get_task_type_dropdown >> is_project_available
            has_task_type >> rail.Label('No') >> is_project_available

            are_project_dates_valid >> rail.Label('No') >> log_project_date_validation_error >> catch_and_log_errors

            is_project_available >> rail.Label('Yes') >> update_project >> is_project_manager_is_present

            is_project_available >> rail.Label('No') >> create_project_in_replicon >> remove_all_users_timesheet_access >> is_project_manager_is_present

            is_project_manager_is_present >> rail.Label('Yes') >> get_project_manager_in_replicon
            is_project_manager_is_present >> rail.Label('No') >> has_cp_project

            get_project_manager_in_replicon >> is_project_manager_enabled
            is_project_manager_enabled >> rail.Label('Yes') >> get_assigned_pm_permissions >> determine_missing_permissions >> has_missing_permissions
            is_project_manager_enabled >> rail.Label('No') >> has_cp_project

            has_missing_permissions >> rail.Label('Yes') >> assign_missing_permissions >> assign_project_manager_to_project >> has_cp_project
            has_missing_permissions >> rail.Label('No') >> assign_project_manager_to_project >> has_cp_project

            # ========== IWO (Inter company worker order) Project Linking Workflow ==========
            has_cp_project >> rail.Label('Yes') >> validate_parent_project
            validate_parent_project >> check_validation_result
            check_validation_result >> rail.Label('Yes') >> check_existing_project_links >> should_create_project_link
            should_create_project_link >> rail.Label('Yes') >> create_iwo_project_link >> get_parent_project_leader
            should_create_project_link >> rail.Label('No') >> get_parent_project_leader
            get_parent_project_leader >> get_parent_project_managers >> combine_parent_and_csv_co_managers >> process_co_managers
            # Handle validation failures
            check_validation_result >> rail.Label('No') >> combine_parent_and_csv_co_managers
            # Skip IWO linking if no CP_PROJECT
            has_cp_project >> rail.Label('No') >> combine_parent_and_csv_co_managers >> process_co_managers

            process_co_managers >> has_co_managers
            has_co_managers >> rail.Label('Yes') >> get_co_managers_in_replicon >> are_co_managers_available
            has_co_managers >> rail.Label('No') >> log_project_success

            # Co-manager permission checking flow
            are_co_managers_available >> rail.Label('Yes') >> get_assigned_co_manager_permissions
            get_assigned_co_manager_permissions >> determine_co_manager_missing_permissions >> has_co_manager_missing_permissions
            has_co_manager_missing_permissions >> rail.Label('Yes') >> assign_co_manager_missing_permissions >> get_existing_sharing_assignments
            has_co_manager_missing_permissions >> rail.Label('No') >> get_existing_sharing_assignments

            # Co-manager sharing assignment flow
            get_existing_sharing_assignments >> should_assign_co_managers
            should_assign_co_managers >> rail.Label('Yes') >> assign_co_managers_to_project >> log_project_success
            should_assign_co_managers >> rail.Label('No') >> log_project_success
            are_co_managers_available >> rail.Label('No') >> log_project_success

            log_project_success >> is_new_project

            is_new_project >> rail.Label("Yes") >> get_all_task_to_add_update >> create_add_task_batches

            is_new_project >> rail.Label("No") >> get_all_tasks_for_project >> get_all_task_to_add_update >> create_add_task_batches

            create_add_task_batches >> has_tasks_to_add
            has_tasks_to_add >> rail.Label("Yes") >> add_task_batches >> get_all_project_team_members >> has_resource_batches_to_assign

            has_resource_batches_to_assign >> rail.Label("Yes") >> trigger_add_resource_child_dags >> wait_for_resource_assignment >> process_enforce_logic

            has_resource_batches_to_assign >> rail.Label("No") >> process_enforce_logic

            has_tasks_to_add >> rail.Label("No") >> process_enforce_logic

            process_enforce_logic >> rail.Label('Yes') >> get_all_project_tasks_for_enforce >> assign_groups_to_project_team >> trigger_service_center_task_assignment >> wait_for_service_center_assignment >> log_task_added
            process_enforce_logic >> rail.Label('No') >> log_task_added >> create_update_task_batches

            create_update_task_batches >> has_tasks_to_update

            has_tasks_to_update >> rail.Label("Yes") >> update_task_batches >> log_task_updated >> has_tasks_to_skip
            has_tasks_to_update >> rail.Label("No") >> has_tasks_to_skip

            has_tasks_to_skip >> rail.Label("Yes") >> log_task_skipped >> catch_and_log_errors
            has_tasks_to_skip >> rail.Label("No") >> catch_and_log_errors

            catch_and_log_errors >> log_to_sumo

        add_dags.append(dag)

    return add_dags

rail.for_each_instance(create_project_processing_dags)
