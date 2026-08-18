"""T-Systems Time Import Child DAG for processing individual employee records."""

from datetime import timedelta
import rail
from airflow.models import Variable
from tsystems.jira_time_import.utils import custom_methods, request_payload, response_filters

null = None

def create_child_dag(config):
    """
    Creates the Child DAG for processing individual time entry records.
    
    This DAG handles the processing of a single time entry including project
    validation, task verification, field validation, and time entry creation.

    Args:
        config: Configuration module containing instance-specific settings,
                validation rules, and processing parameters
    
    Returns:
        Airflow DAG: The configured child DAG for processing individual time entries
    """
    with rail.create_airflow_dag(
        dag_id=config.process_each_entry_child,
        description=f'T-Systems Jira Time Import Child - Process Each Entry {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Task: Check if batch processing mode is enabled
        # Controls execution flow for debugging vs production processing
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_project_details'
        )

        # Task: Execute entry processing pipeline in batch mode
        # Wraps individual entry processing for monitoring and error handling
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_all_project_details',
            end_task='catch_and_log_errors',
        )

        # Task: Retrieve project details from Replicon by project code
        # Validates that the specified project exists and is accessible
        get_all_project_details = rail.RepliconServiceOperator(
            task_id="get_all_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run: {
                "projects": [
                    {
                        "code": dag_run.conf['input_data']['project_id']
                    }
                ]
            },
            data_handler=lambda res: res[0] if (res and res[0].get('projectDetails')) else null
        )

        # Task: Verify that project lookup returned valid results
        # Determines whether to proceed with task validation or log error
        check_project_exists = rail.IfOperator(
            task_id="check_project_exists",
            test=lambda: rail.result('get_all_project_details'),
            yes_task="get_required_tasks_for_project",
            no_task="log_project_not_found"
        )

        # Task: Log error when specified project is not found in Replicon
        # Records project validation failure for reporting and troubleshooting
        log_project_not_found = rail.WriteLogOperator(
            task_id='log_project_not_found',
            log='{{ dag_run.conf.user_log }}',
            severity='Exception',
            message='Project is not available in Replicon',
            properties={
                'unique_id': '{{ dag_run.conf.input_data.unique_id }}',
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'entry_date': '{{ dag_run.conf.input_data.entry_date }}',
                'hours': '{{ dag_run.conf.input_data.hours }}',
                'project_id': '{{ dag_run.conf.input_data.project_id }}',
                'task_name': '{{ dag_run.conf.input_data.task_name }}',
                'status': 'Exception',
                'action': 'Validation',
                'details': 'Project is not available in Replicon'
            },
        )

        # Task: Retrieve all tasks associated with the validated project
        # Gets task hierarchy and details for task name validation
        get_required_tasks_for_project = rail.RepliconServiceOperator(
            task_id="get_required_tasks_for_project",
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data=lambda: {
                "parentUri": rail.result('get_all_project_details')['projectDetails']['uri']
            },
            data_handler=lambda response, dag_run: response_filters.format_project_task_details(response, dag_run)
        )

        # Task: Verify that specified task name exists within the project
        # Validates task existence before proceeding with time entry creation
        check_task_exists = rail.IfOperator(
            task_id="check_task_exists",
            test=lambda: rail.result("get_required_tasks_for_project"),
            yes_task="get_time_entry_details",
            no_task="log_task_not_found"
        )

        # Task: Log error when specified task is not found within the project
        # Records task validation failure with detailed context information
        log_task_not_found = rail.WriteLogOperator(
            task_id='log_task_not_found',
            log='{{ dag_run.conf.user_log }}',
            severity='Exception',
            message='Task is not available for project in Replicon',
            properties={
                'unique_id': '{{ dag_run.conf.input_data.unique_id }}',
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'entry_date': '{{ dag_run.conf.input_data.entry_date }}',
                'hours': '{{ dag_run.conf.input_data.hours }}',
                'project_id': '{{ dag_run.conf.input_data.project_id }}',
                'task_name': '{{ dag_run.conf.input_data.task_name }}',
                'status': 'Exception',
                'action': 'Validation',
                'details': 'Task is not available for project in Replicon'
            },
        )

        # Task: Retrieve existing time entries for each date to be processed
        # Gets current time entries that may need to be deleted before adding new ones
        get_time_entry_details = rail.RepliconServiceOperator(
            task_id="get_time_entry_details",
            endpoint="/services/TimeEntryRevisionGroupService1.svc/GetTimeEntryRevisionGroupsForUserAndDateRange",
            data=lambda dag_run: request_payload.get_time_entries_for_user_date_range(dag_run, config.ENTRY_DATE_FORMAT),
            data_handler=lambda response, dag_run: response_filters.filter_time_entries(response, dag_run)
        )

        if_time_entry_exists = rail.IfOperator(
            task_id='if_time_entry_exists',
            test=lambda: rail.result('get_time_entry_details'),
            yes_task='update_time_entry',
            no_task='add_time_entry'
        )

        # Task: Update time entry in Replicon timesheet
        # Submits time allocation with project, task, activity, and time data
        update_time_entry = rail.RepliconServiceOperator(
            task_id="update_time_entry",
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=lambda dag_run: request_payload.put_time_entry_payload(dag_run, config.ENTRY_DATE_FORMAT)
        )

        # Task: Log successful time entry creation
        # Records successful processing for reporting and audit purposes
        log_update_time_entry_success = rail.WriteLogOperator(
            task_id="log_update_time_entry_success",
            log='{{ dag_run.conf.user_log }}',
            severity="Success",
            message="Time entry updated successfully",
            properties={
                'unique_id': '{{ dag_run.conf.input_data.unique_id }}',
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'entry_date': '{{ dag_run.conf.input_data.entry_date }}',
                'hours': '{{ dag_run.conf.input_data.hours }}',
                'project_id': '{{ dag_run.conf.input_data.project_id }}',
                'task_name': '{{ dag_run.conf.input_data.task_name }}',
                'status': 'Success',
                'action': 'Update',
                'details': 'Time entry updated successfully'
            }
        )

        # Task: Create new time entry in Replicon timesheet
        # Submits time allocation with project, task, activity, and time data
        add_time_entry = rail.RepliconServiceOperator(
            task_id="add_time_entry",
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=lambda dag_run: request_payload.put_time_entry_payload(dag_run, config.ENTRY_DATE_FORMAT)
        )

        # Task: Log successful time entry creation
        # Records successful processing for reporting and audit purposes
        log_add_time_entry_success = rail.WriteLogOperator(
            task_id="log_add_time_entry_success",
            log='{{ dag_run.conf.user_log }}',
            severity="Success",
            message="Time entry added successfully",
            properties={
                'unique_id': '{{ dag_run.conf.input_data.unique_id }}',
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'entry_date': '{{ dag_run.conf.input_data.entry_date }}',
                'hours': '{{ dag_run.conf.input_data.hours }}',
                'project_id': '{{ dag_run.conf.input_data.project_id }}',
                'task_name': '{{ dag_run.conf.input_data.task_name }}',
                'status': 'Success',
                'action': 'Add',
                'details': 'Time entry added successfully'
            }
        )

        # Task: Capture and log any processing errors for this entry
        # Central error handler for troubleshooting and failure reporting
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{dag_run.conf.user_log}}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'unique_id': '{{ dag_run.conf.input_data.unique_id }}',
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'entry_date': '{{ dag_run.conf.input_data.entry_date }}',
                'hours': '{{ dag_run.conf.input_data.hours }}',
                'project_id': '{{ dag_run.conf.input_data.project_id }}',
                'task_name': '{{ dag_run.conf.input_data.task_name }}',
                'status': 'Error',
                'action': 'Add',
                'details': '{{ get_error_message() }}'
            },
        )

        # DAG dependencies
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_all_project_details

        get_all_project_details >> check_project_exists
        check_project_exists >> rail.Label('Yes') >> get_required_tasks_for_project
        check_project_exists >> rail.Label('No') >> log_project_not_found >> catch_and_log_errors

        get_required_tasks_for_project >> check_task_exists
        check_task_exists >> rail.Label('Yes') >> get_time_entry_details

        get_time_entry_details >> if_time_entry_exists
        
        if_time_entry_exists >> rail.Label('Yes') >> update_time_entry >> log_update_time_entry_success
        if_time_entry_exists >> rail.Label('No') >> add_time_entry >> log_add_time_entry_success

        check_task_exists >> rail.Label('No') >> log_task_not_found >> catch_and_log_errors

        log_add_time_entry_success >> catch_and_log_errors
        log_update_time_entry_success >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
