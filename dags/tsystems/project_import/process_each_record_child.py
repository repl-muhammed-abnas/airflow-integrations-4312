from datetime import timedelta
import rail
from tsystems.project_import.utils import request_payload, custom_methods
from airflow.models import Variable

def create_process_each_record_dag(config):
    """
    Child DAG to process each project record
    Handles project creation/update, task creation, and team assignments
    """
    with rail.create_airflow_dag(
        dag_id=config.process_each_record_dag_id,
        description='T-Systems Process Each Record Child DAG',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:
        
        # Display DAG run configuration for debugging
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Check if batch processing is enabled via Airflow variable
        # Allows runtime control over task execution mode
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_existing_project'
        )

        # Batch task wrapper for improved error handling and rollback capabilities
        # Groups all project processing tasks for atomic execution
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='get_existing_project',
            end_task='catch_and_log_errors',
        )

        # Check if project already exists in Replicon by project code
        # Returns project details for update operations or None for new projects
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

        # Comprehensive date validation for project timeline
        # Validates format, start < end, and compatibility with existing project dates
        validate_project_dates = rail.PythonOperator(
            task_id='validate_project_dates',
            python_callable=lambda dag_run: custom_methods.validate_all_project_dates(
                dag_run.conf, rail.result('get_existing_project')
            )
        )

        # Decision point: Continue processing only if date validation passes
        # Prevents invalid date scenarios from corrupting project data
        should_continue_processing = rail.IfOperator(
            task_id='should_continue_processing',
            test=lambda: rail.result('validate_project_dates')['is_valid'],
            yes_task='is_project_manager_present',
            no_task='log_date_validation_error'
        )

        # Log date validation failures with detailed error information
        # Records specific validation errors (format, order, existing project conflicts)
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

        # Check if project manager ID is provided in payload
        # Project manager assignment is optional - projects can exist without one
        is_project_manager_present = rail.IfOperator(
            task_id = 'is_project_manager_present',
            test=lambda dag_run: bool(dag_run.conf['project_manager_id']),
            yes_task= 'get_project_manager_details',
            no_task= 'create_or_update_project'
        )

        # Fetch project manager user details from Replicon
        # Validates that the employee ID exists before attempting assignment
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

        # Verify project manager exists in Replicon before assignment
        # Continues without PM assignment if employee ID not found
        is_project_manager_valid = rail.IfOperator(
            task_id='is_project_manager_valid',
            test=lambda: bool(rail.result('get_project_manager_details')),
            yes_task='assign_manager_permission_set',
            no_task='create_or_update_project'
        )

        # Assign "Project Manager" permission set to the designated user
        # Grants necessary permissions for project management functions
        assign_manager_permission_set = rail.RepliconServiceOperator(
            task_id= "assign_manager_permission_set",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run: {
                "userUri": rail.result('get_project_manager_details')[0]['uri'],
                "permissionSetUri": dag_run.conf['project_manager_permission_set']
            }
        )

        # Core project creation or update operation
        # Handles both new project creation and existing project modifications
        create_or_update_project = rail.RepliconServiceOperator(
            task_id='create_or_update_project',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.get_create_or_update_project_payload
        )

        # Determine if general task creation is needed
        # Only create tasks for new projects, not updates
        should_create_task = rail.IfOperator(
            task_id='should_create_task',
            test=lambda: not request_payload.does_wbs_exist(),
            yes_task='create_general_task',
            no_task='has_team_assignment'
        )

        # Create "General" task for new projects
        # Task configuration depends on billing type (Fixed Bid, T&M, Non-Billable)
        create_general_task = rail.RepliconServiceOperator(
            task_id='create_general_task',
            endpoint='/services/TaskService1.svc/CreateTaskOrApplyModifications',
            data=request_payload.get_create_task_payload
        )

        # Assign project team members to the newly created task
        # Links task with available project resources
        assign_resources_to_task = rail.RepliconServiceOperator(
            task_id='assign_resources_to_task',
            endpoint='/services/TaskService1.svc/AssignProjectTeamMembersToTaskResourceAssignments',
            data=lambda: {
                "taskUri": rail.result('create_general_task')['uri'],
            }
        )

        # Check if team assignment is required based on department mapping
        # Team assignment depends on cost center and accounting area combinations
        has_team_assignment = rail.IfOperator(
            task_id='has_team_assignment',
            test=lambda dag_run: bool(dag_run.conf.get('team_departments', {}).get('should_assign_team', False)),
            yes_task='put_eligible_project_team_member',
            no_task='log_project_success'
        )

        # Assign eligible team members to project based on service center mapping
        # Grants project access to users in specific departments/service centers
        put_eligible_project_team_member = rail.RepliconServiceOperator(
            task_id='put_eligible_project_team_member',
            endpoint='/services/ProjectService1.svc/PutEligibleProjectTeamMemberDataAccessScopesForProject',
            data=request_payload.put_eligible_project_team_member
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

        # Log successful project processing with action details
        # Records whether project was created or updated, including any reference exceptions
        log_project_success = rail.WriteLogOperator(
            task_id="log_project_success",
            log='{{ dag_run.conf.main_log }}',
            severity="Success",
            message="Project processed successfully",
            properties= custom_methods.get_success_or_exception_logs
        )

        # Comprehensive error handling for any task failures
        # Captures detailed error information for troubleshooting and monitoring
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',  # Executes when any upstream task fails
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
        
        # Task dependency definitions and workflow control
        
        # Batch processing branch - wraps entire workflow for atomic execution
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_existing_project

        # Main processing workflow - validation, processing, completion
        get_existing_project >> validate_project_dates >> should_continue_processing

        # Date validation branch - continue or log validation errors
        should_continue_processing >> rail.Label("Yes") >> is_project_manager_present
        should_continue_processing >> rail.Label("No") >> log_date_validation_error >> catch_and_log_errors

        # Project manager processing branch
        is_project_manager_present >> rail.Label("Yes") >> get_project_manager_details >> is_project_manager_valid
        is_project_manager_present >> rail.Label("No") >> create_or_update_project

        # Permission assignment branch - assign PM permissions if user exists
        is_project_manager_valid >> rail.Label("Yes") >> assign_manager_permission_set >> create_or_update_project
        is_project_manager_valid >> rail.Label("No") >> create_or_update_project >> should_create_task

        # Task creation branch - only for new projects
        should_create_task >> rail.Label("Yes") >> create_general_task >> assign_resources_to_task >> has_team_assignment
        should_create_task >> rail.Label("No") >> has_team_assignment

        # Team assignment branch - assign team members if mapping exists
        has_team_assignment >> rail.Label("Yes") >> put_eligible_project_team_member >> assign_team_members_to_project >> log_project_success
        has_team_assignment >> rail.Label("No") >> log_project_success

        # Final logging - success flows into error handler for consistent logging
        log_project_success >> catch_and_log_errors

    return dag

rail.for_each_instance(create_process_each_record_dag)