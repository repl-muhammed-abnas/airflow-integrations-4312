from datetime import timedelta
import rail
from mercury_systems_inc.project_import_v1.utils import request_payload, custom_method, response_filter
from airflow.models import Variable


def create_child_dag_wbs(config):

    add_dags = []

    for idx in range(0, config.PROJECT_BATCH_COUNT):
        get_postfix = "" if idx == 0 else f'_batch_{idx}'

        with rail.create_airflow_dag(
            dag_id=f"{config.process_project_dag_id}{get_postfix}",
            description='Mercury Process Each Project Child',
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
                no_task='get_project_data_from_query'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='get_project_data_from_query',
                end_task='catch_and_log_errors',
            )

            get_project_data_from_query = rail.QueryCollectionOperator(
                task_id='get_project_data_from_query',
                query="""SELECT * from validwbsdata WHERE project_code == :projectcode""",
                query_params={
                    'projectcode': '{{ dag_run.conf.project_code }}'
                }
            )

            get_project_details = rail.RepliconServiceOperator(
                task_id="get_project_details",
                endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
                data={
                    "projects": [
                        {
                            "code": "{{ dag_run.conf.project_code }}",
                        }
                    ]
                },
                data_handler=lambda response: response[0].get('projectDetails')
            )

            validate_owning_org = rail.PythonOperator(
                task_id='validate_owning_org',
                python_callable=lambda dag_run: {
                    'owning_org': request_payload.get_project_data().get('owning_org'),
                    'owning_org_uri': request_payload.find_department_by_code(
                        rail.load_json_artifact(dag_run.conf['depaprtment_details']),
                        request_payload.get_project_data().get('owning_org')
                    ) if request_payload.get_project_data().get('owning_org') else None,
                    'is_valid': True if not request_payload.get_project_data().get('owning_org')
                                    or request_payload.find_department_by_code(
                                        rail.load_json_artifact(dag_run.conf['depaprtment_details']),
                                        request_payload.get_project_data().get('owning_org')
                                    ) else False
                }
            )

            is_owning_org_valid = rail.IfOperator(
                task_id='is_owning_org_valid',
                test="{{ not result('validate_owning_org').owning_org or result('validate_owning_org').is_valid }}",
                yes_task='create_or_update_project',
                no_task='log_owning_org_not_found'
            )

            log_owning_org_not_found = rail.WriteLogOperator(
                task_id="log_owning_org_not_found",
                log="{{ dag_run.conf.log }}",
                message="owningOrg department not found",
                severity="Error",
                properties=lambda: {
                    "projectcode": request_payload.get_project_data()['project_code'],
                    "projectname": request_payload.get_project_data()['project_name'],
                    "program": request_payload.get_project_data()['program'],
                    "taskcode": '',
                    "taskname": '',
                    "action": "Validation",
                    "Status": "Exception",
                    "details": f"owningOrg is '{request_payload.get_project_data().get('owning_org', '')}' not found in replicon department hierarchy"
                }
            )

            is_assign_team_valid = rail.IfOperator(
                task_id='is_assign_team_valid',
                test=lambda: bool(request_payload.get_project_data().get('assign_team')),
                yes_task='assign_or_unassign_team_to_project',
                no_task='is_project_manager_is_present'
            )

            assign_or_unassign_team_to_project = rail.RepliconServiceOperator(
                task_id='assign_or_unassign_team_to_project',
                endpoint='/services/ProjectService1.svc/PutKeyValueForProject',
                data=request_payload.get_keyvalue_for_project
            )

            create_or_update_project = rail.RepliconServiceOperator(
                task_id="create_or_update_project",
                endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
                data=lambda dag_run: request_payload.create_projectorapply_modifications(
                    dag_run, config.program_mapper)
            )

            is_project_manager_is_present = rail.IfOperator(
                task_id='is_project_manager_is_present',
                test=lambda: bool(request_payload.get_project_data()[
                                  'project_manager']),
                yes_task='get_project_manager_in_replicon',
                no_task='is_new_project'
            )

            get_project_manager_in_replicon = rail.RepliconServiceOperator(
                task_id='get_project_manager_in_replicon',
                endpoint="/services/ImportService1.svc/BulkGetUsers3",
                data=lambda: {
                    "users": [
                        {
                            "employeeId": request_payload.get_project_data()['project_manager'],
                        }
                    ],
                    "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
                },
                data_handler=lambda response: {
                    'uri': response[0].get('userDetails', []).get('uri') if response else None,
                    'permission_sets': [item['displayText'] for item in response[0].get('permissionSets', [])] if response else []
                }
            )

            is_project_manager_available = rail.IfOperator(
                task_id='is_project_manager_available',
                test=lambda: bool(rail.result(
                    "get_project_manager_in_replicon")['uri']),
                yes_task='is_permission_set_assigned_to_user',
                no_task='is_new_project'
            )

            is_permission_set_assigned_to_user = rail.IfOperator(
                task_id='is_permission_set_assigned_to_user',
                test='{{ "Project Manager" in result("get_project_manager_in_replicon").permission_sets }}',
                yes_task='assign_project_manager_to_project',
                no_task='assign_permission_set'
            )

            assign_permission_set = rail.RepliconServiceOperator(
                task_id="assign_permission_set",
                endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
                data=lambda dag_run: {
                    "userUri": rail.result("get_project_manager_in_replicon")['uri'],
                    "permissionSetUri": dag_run.conf['project_manager_permission_set_uri']
                }
            )

            assign_project_manager_to_project = rail.RepliconServiceOperator(
                task_id='assign_project_manager_to_project',
                endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
                data=lambda: {
                    "projectUri": rail.result('create_or_update_project')['uri'],
                    "userUri": rail.result("get_project_manager_in_replicon")['uri']
                }
            )

            is_new_project = rail.IfOperator(
                task_id='is_new_project',
                test=lambda: not request_payload.does_wbs_exist(),
                yes_task='get_all_task_to_add_update',
                no_task='get_all_tasks_for_project'
            )

            get_all_tasks_for_project = rail.RepliconServiceOperator(
                task_id="get_all_tasks_for_project",
                endpoint="/services/ProjectService1.svc/BulkGetTaskDetails2",
                data={
                    "pageIndex": 1,
                    "pageSize": 1000,
                    "projectUris": ["{{ result('create_or_update_project').uri }}"]
                },
                data_handler=response_filter.map_existing_project_tasks
            )

            get_all_task_to_add_update = rail.PythonOperator(
                task_id="get_all_task_to_add_update",
                python_callable=custom_method.get_task_to_add_update_skip
            )

            has_tasks_to_add = rail.IfOperator(
                task_id='has_tasks_to_add',
                test='{{ result("get_all_task_to_add_update").tasks_to_add | is_truthy }}',
                yes_task='add_task',
                no_task='has_tasks_to_update'
            )

            add_task = rail.RepliconServiceOperator(
                task_id="add_task",
                endpoint="services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
                data=request_payload.get_add_task_payload
            )

            log_task_added_success_error = rail.WriteLogOperator(
                task_id="log_task_added_success_error",
                log="{{ dag_run.conf.log }}",
                message="{{ item.details }}",
                items=lambda: custom_method.map_task_success_error(
                    "add_task", "add", "tasks_to_add"),
                properties=lambda item: {
                    "projectcode": item['project_code'],
                    "projectname": item['project_name'],
                    "program": item['program'],
                    "taskcode": item['task_code'],
                    "taskname": item['task_name'],
                    'action': 'Add',
                    "details": item['details'],
                    "Status": item['status']
                }
            )

            has_tasks_to_update = rail.IfOperator(
                task_id='has_tasks_to_update',
                test='{{ result("get_all_task_to_add_update").tasks_to_update | is_truthy }}',
                yes_task='update_task',
                no_task='is_received_program_valid'
            )

            update_task = rail.RepliconServiceOperator(
                task_id="update_task",
                endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
                data=request_payload.get_update_task_payload
            )

            log_task_updated_success_error = rail.WriteLogOperator(
                task_id="log_task_updated_success_error",
                log="{{ dag_run.conf.log }}",
                message="{{ item.details }}",
                items=lambda: custom_method.map_task_success_error(
                    "update_task", "update", "tasks_to_update"),
                properties=lambda item: {
                    "projectcode": item['project_code'],
                    "projectname": item['project_name'],
                    "program": item['program'],
                    "taskcode": item['task_code'],
                    "taskname": item['task_name'],
                    'action': 'Update',
                    "details": item['details'],
                    "Status": item['status']
                }
            )

            is_received_program_valid = rail.IfOperator(
                task_id='is_received_program_valid',
                test=lambda: request_payload.get_project_data(
                )['program'] in config.program_mapper,
                yes_task='get_all_required_department_uris',
                no_task='log_project_success'
            )

            get_all_required_department_uris = rail.PythonOperator(
                task_id='get_all_required_department_uris',
                python_callable=lambda dag_run: custom_method.get_all_required_department_uris(
                    dag_run, config.dept_mapper)
            )

            is_any_department_present = rail.IfOperator(
                task_id='is_any_department_present',
                test=lambda: bool(rail.result(
                    'get_all_required_department_uris')),
                yes_task='bulk_update_project_team_members',
                no_task='log_project_success'
            )

            bulk_update_project_team_members = rail.RepliconServiceOperator(
                task_id='bulk_update_project_team_members',
                endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
                data=lambda: {
                    'projectUri': rail.result('create_or_update_project')['uri'],
                    'resourceUri': [item['uri'] for item in rail.result("get_all_required_department_uris")],
                    'projectTeamMemberAssignmentOptionUri': 'urn:replicon:project-team-member-assignment-option:assign'
                }
            )

            log_project_success = rail.WriteLogOperator(
                task_id="log_project_success",
                log="{{ dag_run.conf.log }}",
                message="Project created successfully",
                properties=lambda: {
                    "projectcode": request_payload.get_project_data()['project_code'],
                    "projectname": request_payload.get_project_data()['project_name'],
                    "program": request_payload.get_project_data()['program'],
                    "taskcode": '',
                    "taskname": '',
                    "action": "Update" if request_payload.does_wbs_exist() else "Add",
                    "Status": "Success",
                    "details": custom_method.get_exception_message(config.program_mapper),
                }
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                trigger_rule='one_failed',
                log="{{ dag_run.conf.log }}",
                message='{{ get_error_message() }}',
                severity='Error',
                properties=lambda: {
                    "projectcode": request_payload.get_project_data()['project_code'],
                    "projectname": request_payload.get_project_data()['project_name'],
                    "program": request_payload.get_project_data()['program'],
                    "taskcode": '',
                    "taskname": '',
                    "action": "Add",
                    "Status": "Error",
                    'details': rail.render_template('{{ get_error_message() }}')
                }
            )

            log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id='log_to_sumo',
                sumo_conn_id='sumologic-dagrunlogger',
                trigger_rule='all_done',
                extra_info=lambda dag_run: {
                    "projectcode": request_payload.get_project_data()['project_code'],
                    "projectname": request_payload.get_project_data()['project_name'],
                    "program": request_payload.get_project_data()['program'],
                    'details': 'Project and Tasks are synced successfully.'
                }
            )

            can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> catch_and_log_errors

            can_run_batch_task >> rail.Label(
                'No') >> get_project_data_from_query >> get_project_details >> validate_owning_org >> is_owning_org_valid

            is_owning_org_valid >> rail.Label(
                'Yes') >> create_or_update_project >> is_assign_team_valid

            is_assign_team_valid >> rail.Label(
                'Yes') >> assign_or_unassign_team_to_project >> is_project_manager_is_present

            is_assign_team_valid >> rail.Label(
                'No') >> is_project_manager_is_present

            is_owning_org_valid >> rail.Label(
                'No') >> log_owning_org_not_found >> catch_and_log_errors

            is_project_manager_is_present >> rail.Label(
                "Yes") >> get_project_manager_in_replicon >> is_project_manager_available

            is_project_manager_is_present >> rail.Label(
                "No") >> is_new_project

            is_project_manager_available >> rail.Label(
                "Yes") >> is_permission_set_assigned_to_user

            is_project_manager_available >> rail.Label(
                "No") >> is_new_project

            is_permission_set_assigned_to_user >> rail.Label(
                "Yes") >> assign_project_manager_to_project >> is_new_project

            is_permission_set_assigned_to_user >> rail.Label(
                "No") >> assign_permission_set >> assign_project_manager_to_project            

            is_new_project >> rail.Label(
                "Yes") >> get_all_task_to_add_update

            is_new_project >> rail.Label(
                "No") >> get_all_tasks_for_project >> get_all_task_to_add_update >> has_tasks_to_add

            has_tasks_to_add >> rail.Label(
                "Yes") >> add_task >> log_task_added_success_error >> has_tasks_to_update

            has_tasks_to_add >> rail.Label(
                "No") >> has_tasks_to_update

            has_tasks_to_update >> rail.Label(
                "Yes") >> update_task >> log_task_updated_success_error >> is_received_program_valid

            has_tasks_to_update >> rail.Label(
                "No") >> is_received_program_valid
            
            is_received_program_valid >> rail.Label(
                "No") >> log_project_success

            is_received_program_valid >> rail.Label(
                "Yes") >> get_all_required_department_uris >> is_any_department_present

            is_any_department_present >> rail.Label(
                "Yes") >> bulk_update_project_team_members >> log_project_success
            
            is_any_department_present >> rail.Label(
                "No") >> log_project_success >> catch_and_log_errors >> log_to_sumo

        add_dags.append(dag)

    return add_dags


rail.for_each_instance(create_child_dag_wbs)
