from datetime import timedelta
import rail
from alvarezandmarsalholdings.enterprise_project_import_v1.utils import request_payload,response_filter,python_callable
from alvarezandmarsalholdings.enterprise_project_import_v1.task.process_project_manager import process_project_manager_task_group
from alvarezandmarsalholdings.enterprise_project_import_v1.task.add_update_tasks import get_task_added_or_updated
from alvarezandmarsalholdings.enterprise_project_import_v1.task.assign_unassign_resource import assign_unassign_resource
from airflow.models import Variable

def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_projects,
        description=f'{config.company_key} Enterprise Project Import - Process Each Project Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_projects,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_project_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_project_log',
            end_task='catch_and_log_errors',
        )

        create_project_log = rail.CreateLogOperator(
            task_id="create_project_log"
        )

        has_mandatory_fields = rail.IfOperator(
            task_id='has_mandatory_fields',
            test=request_payload.get_all_mandatory_check_projects,
            yes_task="get_project_details",
            no_task="log_mandatory_project_fields_not_present"
        )

        log_mandatory_project_fields_not_present = rail.WriteLogOperator(
            task_id='log_mandatory_project_fields_not_present',
            log='{{ result("create_project_log") }}',
            message="Missing mandatory fields",
            severity='Exception',
            properties=lambda dag_run: {
                'projectcode': dag_run.conf['Project'],
                'projectname': dag_run.conf['ProjectDescription'],
                'taskcode': '',
                'taskname': '',
                'action': 'Validation',
                'status': 'Exception',
                "details": request_payload.get_exception_message(dag_run, request_payload.MANDATORY_FIELDS['project_fields']),
            }
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data={
                "projects": [
                    {
                        "code": "{{ dag_run.conf.Project }}",
                    }
                ]
            },
            data_handler=lambda response: response[0].get('projectDetails')
        )

        process_project_manager = rail.EmptyOperator(
            task_id='process_project_manager'
        )

        process_project_manager_entry,  process_project_manager_exit= process_project_manager_task_group(config)

        is_project_not_available_in_replicon = rail.IfOperator(
            task_id = 'is_project_not_available_in_replicon',
            test=lambda: not request_payload.does_project_code_exist(),
            yes_task= 'create_project',
            no_task= 'update_project'
        )

        create_project = rail.RepliconServiceOperator(
            task_id="create_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: request_payload.create_or_update_project_payload(
                dag_run, config
            )
        )

        update_project = rail.RepliconServiceOperator(
            task_id="update_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: request_payload.create_or_update_project_payload(
                dag_run, config
            )
        )

        log_project_success = rail.WriteLogOperator(
            task_id="log_project_success",
            log="{{result('create_project_log')}}",
            message="Project created/updated successfully",
            properties=lambda dag_run: {
                'projectcode': dag_run.conf['Project'],
                'projectname': dag_run.conf['ProjectDescription'],
                'taskcode': '',
                'taskname': '',
                "action": "Update" if request_payload.does_project_code_exist() else "Add",
                "status": request_payload.get_project_log_details(dag_run)['status'],
                "details": request_payload.get_project_log_details(dag_run)['message'],
            }
        )
        
        def get_project_uri():
            return rail.result('update_project')['uri'] if request_payload.does_project_code_exist() else \
                    rail.result('create_project')['uri']

        get_all_tasks_for_project = rail.RepliconServiceOperator(
            task_id=f"get_all_tasks_for_project",
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data=lambda: {
                "parentUri": get_project_uri()
            },
            data_handler=response_filter.format_project_task_details
        )

        format_payload_tasks = rail.PythonOperator(
            task_id="format_payload_tasks",
            python_callable=python_callable.get_formatted_payload_tasks
        )

        log_orphan_tasks = rail.WriteLogOperator(
            task_id='log_orphan_tasks',
            log='{{ result("create_project_log") }}',
            items= "{{ result('format_payload_tasks').orphan_tasks | to_json }}",
            message="Parent Task is neither present in payload nor in replicon",
            severity='Exception',
            properties=lambda item, dag_run: {
                'projectcode': dag_run.conf['Project'],
                'projectname': dag_run.conf['ProjectDescription'],
                'taskcode': item['taskcode'],
                'taskname': item['taskname'],
                'action': 'Validation',
                'status': 'Exception',
                "details": "Parent Task is neither present in payload nor in replicon",
            }
        )
        
        has_billing_resp_users = rail.IfOperator(
            task_id='has_billing_resp_users',
            test=lambda: bool(rail.result('format_payload_tasks')['billing_responsibles']),
            yes_task="get_billing_responsible_users_data",
            no_task="has_task_mandatory_fields"
        )

        get_billing_responsible_users_data = rail.RepliconServiceOperator(
            task_id='get_billing_responsible_users_data',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_billing_responsible_users_payload,
            data_handler=response_filter.get_user_data_from_list
        )
        
        has_task_mandatory_fields = rail.IfOperator(
            task_id='has_task_mandatory_fields',
            test=lambda: not bool(rail.result('format_payload_tasks')['missing_mandatory_fields']),
            yes_task="process_task_level1",
            no_task="log_mandatory_task_fields_not_present"
        )

        log_mandatory_task_fields_not_present = rail.WriteLogOperator(
            task_id='log_mandatory_task_fields_not_present',
            log='{{ result("create_project_log") }}',
            items= "{{ result('format_payload_tasks').missing_mandatory_fields | to_json }}",
            message="Missing Mandatory Task Fields",
            severity='Exception',
            properties=lambda item, dag_run: {
                'projectcode': dag_run.conf['Project'],
                'projectname': dag_run.conf['ProjectDescription'],
                'taskcode': item['taskcode'],
                'taskname': item['taskname'],
                'action': 'Validation',
                'status': 'Exception',
                "details": item['message'],
            }
        )

        process_task_level1 = rail.EmptyOperator(
            task_id='process_task_level1'
        )

        process_task_level1_entry,  process_task_level1_exit= get_task_added_or_updated(
            'group_task_level1', 1, 
        )

        process_tasks_with_parents_in_replicon = rail.EmptyOperator(
            task_id='process_tasks_with_parents_in_replicon'
        )

        process_tasks_with_parents_entry,  process_tasks_with_parents_exit= get_task_added_or_updated(
            'group_task_with_parents', 1, 'parent_in_system'
        )

        process_task_level2 = rail.EmptyOperator(
            task_id='process_task_level2'
        )

        process_task_level2_entry,  process_task_level2_exit= get_task_added_or_updated(
            'group_task_level2', 2, 
        )

        process_task_level3 = rail.EmptyOperator(
            task_id='process_task_level3'
        )

        process_task_level3_entry,  process_task_level3_exit= get_task_added_or_updated(
            'group_task_level3', 3, 
        )

        project_team_member_uris = rail.PythonOperator(
            task_id="project_team_member_uris",
            python_callable=lambda dag_run:python_callable.get_project_team_members_uris(dag_run, config.instance)
        )

        update_project_team_members = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_project_team_members',
            endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
            items=lambda: rail.result('project_team_member_uris')['resource_uris'],
            data=lambda item: {
                'projectUri': rail.result('update_project')['uri'] if request_payload.does_project_code_exist() else \
                    rail.result('create_project')['uri'],
                'resourceUri': item,
                'projectTeamMemberAssignmentOptionUri': 'urn:replicon:project-team-member-assignment-option:assign'
            }
        )
        
        has_billing_responsible_users = rail.IfOperator(
            task_id='has_billing_responsible_users',
            test=lambda: bool(rail.result('format_payload_tasks')['billing_responsibles']),
            yes_task="get_permissions_to_assign",
            no_task="has_invalid_team_member_uris"
        )

        get_permissions_to_assign = rail.PythonOperator(
            task_id="get_permissions_to_assign",
            python_callable=lambda dag_run: python_callable.get_permissions_to_assign(dag_run, config)
        )

        add_missing_permissions = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_missing_permissions',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda: rail.result('get_permissions_to_assign')['permissions_to_add'],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=lambda item: item
        )

        has_invalid_records_for_permission = rail.IfOperator(
            task_id='has_invalid_records_for_permission',
            test=lambda: bool(rail.result('get_permissions_to_assign')['log_details']),
            yes_task="log_invalid_records_for_permission",
            no_task="has_invalid_team_member_uris"
        )

        log_invalid_records_for_permission = rail.WriteLogOperator(
            task_id='log_invalid_records_for_permission',
            log='{{ result("create_project_log") }}',
            items=lambda: rail.result('get_permissions_to_assign')['log_details'],
            message="Missing billing responsible users",
            severity='Exception',
            properties=lambda dag_run, item: {
                'projectcode': dag_run.conf['Project'],
                'projectname': dag_run.conf['ProjectDescription'],
                'taskcode': item['task_code'],
                'taskname': item['task_name'],
                'action': 'Validation',
                'status': 'Exception',
                "details": item['message'],
            }
        )

        has_invalid_team_member_uris = rail.IfOperator(
            task_id='has_invalid_team_member_uris',
            test=lambda: bool(rail.result('project_team_member_uris')['log_messages']),
            yes_task="log_invalid_resources",
            no_task="process_tasks_resource"
        )

        log_invalid_resources = rail.WriteLogOperator(
            task_id='log_invalid_resources',
            log='{{ result("create_project_log") }}',
            items=lambda: rail.result('project_team_member_uris')['log_messages'],
            message="Missing Mandatory Task Fields",
            severity='Exception',
            properties=lambda dag_run, item: {
                'projectcode': dag_run.conf['Project'],
                'projectname': dag_run.conf['ProjectDescription'],
                'taskcode': item['task_code'],
                'taskname': item['task_name'],
                'action': 'Validation',
                'status': 'Exception',
                "details": item['message'],
            }
        )

        process_tasks_resource = rail.EmptyOperator(
            task_id='process_tasks_resource'
        )

        process_tasks_resource_entry,  process_tasks_resource_exit= assign_unassign_resource(config.instance)

        process_resource_assign_unassign = rail.EmptyOperator(
            task_id='process_resource_assign_unassign'
        )

        process_add_resource = rail.TriggerDagRunForEachItemOperator(
            task_id='process_add_resource',
            items= '{{ result("get_resources_add_remove").resource_to_add | to_json }}',
            trigger_dag_id=config.process_add_resource,
            conf=lambda item: {
                "task_uri": item['task_uri'],
                "uris": item['uris'],
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_process_add_resource = rail.WaitForDagRunsSensor(
            task_id="wait_process_add_resource",
            dag_runs="{{result('process_add_resource')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        process_remove_resource = rail.TriggerDagRunForEachItemOperator(
            task_id='process_remove_resource',
            items= '{{ result("get_resources_add_remove").resource_to_remove | to_json }}',
            trigger_dag_id=config.process_remove_resource,
            conf=lambda item: {
                "task_uri": item['task_uri'],
                "uris": item['uris'],
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_process_remove_resource = rail.WaitForDagRunsSensor(
            task_id="wait_process_remove_resource",
            dag_runs="{{result('process_remove_resource')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_final_descendant_task_details = rail.RepliconServiceOperator(
            task_id=f"get_final_descendant_task_details",
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data=lambda: {
                "parentUri": get_project_uri()
            },
            data_handler=response_filter.format_project_task_details
        )

        get_valid_hierarchy_tasks = rail.PythonOperator(
            task_id=f"get_valid_hierarchy_tasks",
            python_callable=python_callable.get_tasks_for_hierarchy_update
        )

        update_task_hierarchy = rail.RepliconServiceCallForEachItemOperator(
            task_id=f"update_task_hierarchy",
            endpoint="/services/TaskService1.svc/MoveTask",
            items=lambda: rail.result(f"get_valid_hierarchy_tasks")['update'],
            data=lambda item: {
                "taskUri": item['taskuri'],
                "targetUri": item['targeturi'],
                "moveTaskMethodUri": "urn:replicon:move-task-method:child-of-target"
            }
        )

        log_task_updated_task_hierarchy = rail.WriteLogOperator(
            task_id=f"log_task_updated_task_hierarchy",
            log="{{result('create_project_log')}}",
            message="{{ item.details }}",
            items=lambda: rail.result(f"get_valid_hierarchy_tasks")['msg'],
            properties=lambda dag_run, item: {
                'projectcode': dag_run.conf['Project'],
                'projectname': dag_run.conf['ProjectDescription'],
                'taskcode': item['taskcode'],
                'taskname': item['taskname'],
                "action": "Update",
                "status": "Success",
                "details": item['details'],
            }
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{result('create_project_log')}}",
            message='{{ get_error_message() }}',
            severity= 'Error',
            properties=lambda dag_run:{
                'projectcode': dag_run.conf['Project'],
                'projectname': dag_run.conf['ProjectDescription'],
                'taskcode': '',
                'taskname': '',
                "action": "Update" if request_payload.does_project_code_exist() else "Add",
                "status": "Error",
                'details': '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_project_log

        create_project_log >> \
        has_mandatory_fields >> rail.Label(
            "Yes") >> get_project_details >> process_project_manager
        has_mandatory_fields >> rail.Label(
            "No") >> log_mandatory_project_fields_not_present >> finish
        process_project_manager >> process_project_manager_entry
        process_project_manager_exit >> is_project_not_available_in_replicon
        is_project_not_available_in_replicon >> rail.Label(
            'Yes') >> create_project >> log_project_success
        is_project_not_available_in_replicon >> rail.Label('No') >> update_project >> log_project_success
        log_project_success >> get_all_tasks_for_project >> format_payload_tasks >> log_orphan_tasks >> has_billing_resp_users
        has_billing_resp_users >> rail.Label(
            "Yes") >> get_billing_responsible_users_data >> has_task_mandatory_fields
        has_billing_resp_users >> rail.Label(
            "No") >> has_task_mandatory_fields
        has_task_mandatory_fields >> rail.Label(
            "Yes") >> process_task_level1
        has_task_mandatory_fields >> rail.Label("No") >> log_mandatory_task_fields_not_present >> process_task_level1
        process_task_level1 >> process_task_level1_entry
        process_task_level1_exit >> process_tasks_with_parents_in_replicon >> process_tasks_with_parents_entry
        process_tasks_with_parents_exit >> process_task_level2 >> process_task_level2_entry
        process_task_level2_exit >> process_task_level3 >> process_task_level3_entry
        process_task_level3_exit >> project_team_member_uris
        project_team_member_uris >> update_project_team_members >> has_billing_responsible_users >> rail.Label(
            "Yes") >> get_permissions_to_assign >> add_missing_permissions >> \
            has_invalid_records_for_permission >> rail.Label("Yes") >> log_invalid_records_for_permission >> has_invalid_team_member_uris
        has_invalid_records_for_permission >> rail.Label("No") >> has_invalid_team_member_uris
        has_billing_responsible_users >> rail.Label(
            "No") >> has_invalid_team_member_uris
        has_invalid_team_member_uris >> rail.Label(
            "Yes") >> log_invalid_resources >> process_tasks_resource
        has_invalid_team_member_uris >> rail.Label("No") >> process_tasks_resource >> process_tasks_resource_entry
        process_tasks_resource_exit >> process_resource_assign_unassign >> process_add_resource >> wait_process_add_resource >> process_remove_resource
        process_remove_resource >> wait_process_remove_resource >> get_final_descendant_task_details >> get_valid_hierarchy_tasks >> \
        update_task_hierarchy >> log_task_updated_task_hierarchy >> finish >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
