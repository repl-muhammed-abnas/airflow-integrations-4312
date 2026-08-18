from datetime import timedelta
import rail
from tsystems.project_import_file_based_v2.utils import request_payload, custom_methods
from airflow.models import Variable

def create_process_each_record_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_record_dag_id,
        description='T-Systems Process Each Record Child DAG',
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
            no_task='get_existing_project'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='get_existing_project',
            end_task='catch_and_log_errors',
        )

        get_existing_project = rail.RepliconServiceOperator(
            task_id='get_existing_project',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run: {
                "projects": [
                    {
                        "code": dag_run.conf.get('project_code', ''),
                    }
                ]
            },
            data_handler=lambda response: response[0]['projectDetails'] if response else None,
        )

        validate_project_dates = rail.PythonOperator(
            task_id='validate_project_dates',
            python_callable=lambda dag_run: custom_methods.validate_all_project_dates(
                dag_run.conf, rail.result('get_existing_project')
            )
        )

        should_continue_processing = rail.IfOperator(
            task_id='should_continue_processing',
            test=lambda: rail.result('validate_project_dates')['is_valid'],
            yes_task='is_project_manager_present',
            no_task='log_date_validation_error'
        )

        log_date_validation_error = rail.WriteLogOperator(
            task_id="log_date_validation_error",
            log='{{ dag_run.conf.main_log }}',
            severity="Warning",
            message="Date validation failed - skipping project processing",
            properties=lambda dag_run: {
                'projectid': dag_run.conf['project_code'],
                'projectname': dag_run.conf['project_name'],
                'clientcode': dag_run.conf.get('client_code', ''),
                'action': 'Validation',
                'details': custom_methods.get_date_validation_error_details(),
                'status': 'Exception'
            }
        )

        is_project_manager_present = rail.IfOperator(
            task_id = 'is_project_manager_present',
            test=lambda dag_run: bool(dag_run.conf['project_manager_id']),
            yes_task= 'get_project_manager_details',
            no_task= 'create_or_update_project'
        )

        get_project_manager_details = rail.RepliconServiceOperator(
            task_id='get_project_manager_details',
            endpoint="/services/UserService1.svc/BulkGetUsers2",
            data=lambda dag_run:{
                "users": [
                    {
                        "employeeId": dag_run.conf['project_manager_id']
                    }
                ]
            },
            data_handler=lambda response: [] if response == [None] else response
        )

        is_project_manager_valid = rail.IfOperator(
            task_id='is_project_manager_valid',
            test=lambda: bool(rail.result('get_project_manager_details')),
            yes_task='assign_manager_permission_set',
            no_task='create_or_update_project'
        )

        assign_manager_permission_set = rail.RepliconServiceOperator(
            task_id= "assign_manager_permission_set",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run: {
                "userUri": rail.result('get_project_manager_details')[0]['uri'],
                "permissionSetUri": dag_run.conf['project_manager_permission_set']
            }
        )

        create_or_update_project = rail.RepliconServiceOperator(
            task_id='create_or_update_project',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.get_create_or_update_project_payload
        )

        should_create_task = rail.IfOperator(
            task_id='should_create_task',
            test=lambda: not request_payload.does_wbs_exist(),
            yes_task='create_general_task',
            no_task='has_restrictions'
        )

        create_general_task = rail.RepliconServiceOperator(
            task_id='create_general_task',
            endpoint='/services/TaskService1.svc/CreateTaskOrApplyModifications',
            data=request_payload.get_create_task_payload
        )

        assign_resources_to_task = rail.RepliconServiceOperator(
            task_id='assign_resources_to_task',
            endpoint='/services/TaskService1.svc/AssignProjectTeamMembersToTaskResourceAssignments',
            data=lambda: {
                "taskUri": rail.result('create_general_task')['uri'],
            }
        )

        has_restrictions = rail.IfOperator(
            task_id='has_restrictions',
            test=lambda dag_run: bool(
                dag_run.conf.get('team_departments', {}).get('assign_from_department_uris', []) or
                dag_run.conf.get('team_departments', {}).get('assign_from_employee_type_uris', [])
            ),
            yes_task='put_eligible_project_team_member',
            no_task='has_team_assignment'
        )

        put_eligible_project_team_member = rail.RepliconServiceOperator(
            task_id='put_eligible_project_team_member',
            endpoint='/services/ProjectService1.svc/PutEligibleProjectTeamMemberDataAccessScopesForProject',
            data=request_payload.put_eligible_project_team_member
        )

        has_team_assignment = rail.IfOperator(
            task_id='has_team_assignment',
            test=lambda dag_run: bool(dag_run.conf.get('team_departments', {}).get('should_assign_team', False)),
            yes_task='assign_team_members_to_project',
            no_task='log_project_success'
        )

        assign_team_members_to_project = rail.RepliconServiceOperator(
            task_id= 'assign_team_members_to_project',
            endpoint='/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
            data=lambda dag_run: {
                "projectUri": rail.result("create_or_update_project")['uri'],
                "resourceUri": dag_run.conf['team_departments']['uris'],
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        get_all_tasks_for_project = rail.RepliconServiceOperator(
            task_id="get_all_tasks_for_project",
            endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
            data={
                "parentUri": "{{result('create_or_update_project').uri}}"
            },
            data_handler=lambda resp: list(map(lambda task: {
                "uri": task['uri']
            }, resp))
        )

        assign_team_members_to_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id="assign_team_members_to_tasks",
            items=lambda: rail.result("get_all_tasks_for_project"),
            endpoint= "/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda item, dag_run: {
                "taskUri": item['uri'],
                "resourceUris": dag_run.conf['team_departments']['uris'],
                "isAssigned": True
            }
        )

        log_project_success = rail.WriteLogOperator(
            task_id="log_project_success",
            log='{{ dag_run.conf.main_log }}',
            severity="Success",
            message="Project processed successfully",
            properties= custom_methods.get_success_or_exception_logs
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.main_log }}',
            message='{{ get_error_message() }}',
            severity='Error',
            properties=lambda dag_run: {
                'projectid': dag_run.conf['project_code'],
                'projectname': dag_run.conf['project_name'],
                'clientcode': dag_run.conf.get('client_code', ''),
                'action': 'Create' if not request_payload.does_wbs_exist() else 'Update',
                'details': rail.render_template('{{ get_error_message() }}'),
                'status': 'Error'
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_existing_project

        get_existing_project >> validate_project_dates >> should_continue_processing

        should_continue_processing >> rail.Label("Yes") >> is_project_manager_present
        should_continue_processing >> rail.Label("No") >> log_date_validation_error >> catch_and_log_errors

        is_project_manager_present >> rail.Label("Yes") >> get_project_manager_details >> is_project_manager_valid
        is_project_manager_present >> rail.Label("No") >> create_or_update_project

        is_project_manager_valid >> rail.Label("Yes") >> assign_manager_permission_set >> create_or_update_project
        is_project_manager_valid >> rail.Label("No") >> create_or_update_project >> should_create_task

        should_create_task >> rail.Label("Yes") >> create_general_task >> assign_resources_to_task >> has_restrictions
        should_create_task >> rail.Label("No") >> has_restrictions

        has_restrictions >> rail.Label("Yes") >> put_eligible_project_team_member >> has_team_assignment
        has_restrictions >> rail.Label("No") >> has_team_assignment

        has_team_assignment >> rail.Label("Yes") >> assign_team_members_to_project >> get_all_tasks_for_project >> \
            assign_team_members_to_tasks >> log_project_success
        has_team_assignment >> rail.Label("No") >> log_project_success

        log_project_success >> catch_and_log_errors

    return dag

rail.for_each_instance(create_process_each_record_dag)