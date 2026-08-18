"""T-Systems Time Import Child DAG for processing individual employee records."""

from datetime import datetime, timedelta
import rail
from airflow.models import Variable
from tsystems.time_import.utils import custom_methods, request_payload, response_filters

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
        description=f'T-Systems Time Import Child - Process Each Entry {config.instance}',
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
            no_task='if_project_task_present'
        )

        # Task: Execute entry processing pipeline in batch mode
        # Wraps individual entry processing for monitoring and error handling
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='if_project_task_present',
            end_task='catch_and_log_errors',
        )

        # Task: Check if project ID and task name are provided in entry data
        # Routes processing based on whether project-specific validation is needed
        if_project_task_present = rail.IfOperator(
            task_id="if_project_task_present",
            test=lambda dag_run: bool(dag_run.conf['entry_data']['project_id'] and dag_run.conf['entry_data']['task_name']),
            yes_task="get_all_project_details",
            no_task="process_time_entry_details"
        )

        # Task: Retrieve project details from Replicon by project code
        # Validates that the specified project exists and is accessible
        get_all_project_details = rail.RepliconServiceOperator(
            task_id="get_all_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run: {
                "projects": [
                    {
                        "code": dag_run.conf['entry_data']['project_id']
                    }
                ]
            },
            data_handler=lambda res: res[0] if (res and res[0].get('projectDetails')) else None
        )

        # Task: Verify that project lookup returned valid results
        # Determines whether to proceed with task validation or log error
        check_project_exists = rail.IfOperator(
            task_id="check_project_exists",
            test=lambda: bool(rail.result('get_all_project_details')),
            yes_task="get_all_tasks_for_project",
            no_task="log_project_not_found"
        )

        # Task: Log error when specified project is not found in Replicon
        # Records project validation failure for reporting and troubleshooting
        log_project_not_found = rail.WriteLogOperator(
            task_id='log_project_not_found',
            log='{{ dag_run.conf.user_log }}',
            severity='Exception',
            message='Project not found in Replicon for project_id: {{ dag_run.conf.entry_data.project_id }}',
            properties={
                'employee_id': '{{ dag_run.conf.entry_data.employee_id }}',
                'entry_date': '{{ dag_run.conf.entry_data.entry_date }}',
                'project_id': '{{ dag_run.conf.entry_data.project_id }}',
                'task_name': '{{ dag_run.conf.entry_data.task_name }}',
                'activity': '{{ dag_run.conf.entry_data.activity }}',
                'status': 'Exception',
                'action': 'Add',
                'details': 'Project not found in Replicon for project_id: {{ dag_run.conf.entry_data.project_id }}'
            },
        )

        # Task: Retrieve all tasks associated with the validated project
        # Gets task hierarchy and details for task name validation
        get_all_tasks_for_project = rail.RepliconServiceOperator(
            task_id="get_all_tasks_for_project",
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data=lambda: {
                "parentUri": rail.result('get_all_project_details')['projectDetails']['uri']
            },
            data_handler=response_filters.format_project_task_details
        )

        # Task: Verify that specified task name exists within the project
        # Validates task existence before proceeding with time entry creation
        check_task_exists = rail.IfOperator(
            task_id="check_task_exists",
            test=lambda dag_run: bool(rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_tasks_for_project'), 'task_name', dag_run.conf['entry_data']['task_name'], 'uri', False
            )),
            yes_task="process_time_entry_details",
            no_task="log_task_not_found"
        )

        # Task: Log error when specified task is not found within the project
        # Records task validation failure with detailed context information
        log_task_not_found = rail.WriteLogOperator(
            task_id='log_task_not_found',
            log='{{ dag_run.conf.user_log }}',
            severity='Exception',
            message='Task not found in Replicon for project_id: {{ dag_run.conf.entry_data.project_id }}',
            properties={
                'employee_id': '{{ dag_run.conf.entry_data.employee_id }}',
                'entry_date': '{{ dag_run.conf.entry_data.entry_date }}',
                'project_id': '{{ dag_run.conf.entry_data.project_id }}',
                'task_name': '{{ dag_run.conf.entry_data.task_name }}',
                'activity': '{{ dag_run.conf.entry_data.activity }}',
                'status': 'Exception',
                'action': 'Add',
                'details': 'Task not found in Replicon for project_id: {{ dag_run.conf.entry_data.project_id }}'
            },
        )

        # Task: Synchronization point for time entry processing flow
        # Consolidates different validation paths before field validation
        process_time_entry_details = rail.EmptyOperator(
            task_id="process_time_entry_details"
        )

        # Task: Validate mandatory fields based on user's timesheet template type
        # Checks different field requirements for duration vs time-based templates
        has_required_ts_based_fields = rail.PythonOperator(
            task_id="has_required_ts_based_fields",
            python_callable=lambda dag_run: custom_methods.validate_ts_based_mandatory_fields(dag_run.conf['entry_data'], dag_run.conf['user_ts_type']),
        )

        # Task: Determine if mandatory field validation found any errors
        # Routes to error logging or continues with OEF configuration
        check_missing_mandatory_fields = rail.IfOperator(
            task_id="check_missing_mandatory_fields",
            test=lambda ti: bool(rail.result('has_required_ts_based_fields')),
            yes_task="log_missing_ts_based_fields",
            no_task="if_inout_or_hour_present"
        )

        # Task: Log errors for missing mandatory fields specific to timesheet type
        # Records detailed validation failures for timesheet template requirements
        log_missing_ts_based_fields = rail.WriteLogOperator(
            task_id='log_missing_ts_based_fields',
            log='{{ dag_run.conf.user_log }}',
            severity='Error',
            message='Missing required distribution fields: {{ result("has_required_ts_based_fields") }}',
            properties={
                'employee_id': '{{ dag_run.conf.entry_data.employee_id }}',
                'entry_date': '{{ dag_run.conf.entry_data.entry_date }}',
                'project_id': '{{ dag_run.conf.entry_data.project_id }}',
                'task_name': '{{ dag_run.conf.entry_data.task_name }}',
                'activity': '{{ dag_run.conf.entry_data.activity }}',
                'status': 'Exception',
                'action': 'Add',
                'details': 'Missing required distribution fields: {{ result("has_required_ts_based_fields") }}'
            },
        )

        # Task: Check if time data is provided (hours or in/out times)
        # Validates that at least one form of time allocation is present
        if_inout_or_hour_present = rail.IfOperator(
            task_id="if_inout_or_hour_present",
            test=lambda dag_run: bool(dag_run.conf['entry_data']['hours'] or \
                            (dag_run.conf['entry_data']['in_time'] and dag_run.conf['entry_data']['out_time'])),
            yes_task="add_time_entry",
            no_task="catch_and_log_errors"
        )

        # Task: Create new time entry in Replicon timesheet
        # Submits time allocation with project, task, activity, and time data
        add_time_entry = rail.RepliconServiceOperator(
            task_id="add_time_entry",
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=lambda dag_run: request_payload.put_time_entry_payload(
                dag_run,
                rail.find_first_by_attr_and_get_attr(rail.result('get_all_tasks_for_project'), 'task_name', dag_run.conf['entry_data']['task_name'], 'uri', ''),
            )
        )

        # Task: Log successful time entry creation
        # Records successful processing for reporting and audit purposes
        log_success = rail.WriteLogOperator(
            task_id="log_success",
            log='{{ dag_run.conf.user_log }}',
            severity="Success",
            message="Time entry Added successfully",
            properties=lambda item: {
                'employee_id': '{{ dag_run.conf.entry_data.employee_id }}',
                'entry_date': '{{ dag_run.conf.entry_data.entry_date }}',
                'project_id': '{{ dag_run.conf.entry_data.project_id }}',
                'task_name': '{{ dag_run.conf.entry_data.task_name }}',
                'activity': '{{ dag_run.conf.entry_data.activity }}',
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
                'employee_id': '{{ dag_run.conf.entry_data.employee_id }}',
                'entry_date': '{{ dag_run.conf.entry_data.entry_date }}',
                'project_id': '{{ dag_run.conf.entry_data.project_id }}',
                'task_name': '{{ dag_run.conf.entry_data.task_name }}',
                'activity': '{{ dag_run.conf.entry_data.activity }}',
                'status': 'Error',
                'action': 'Add',
                'details': '{{ get_error_message() }}'
            },
        )

        # DAG dependencies
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> if_project_task_present

        if_project_task_present >> rail.Label('Yes') >> get_all_project_details
        if_project_task_present >> rail.Label('No') >> process_time_entry_details


        get_all_project_details >> check_project_exists
        check_project_exists >> rail.Label('Yes') >> get_all_tasks_for_project
        check_project_exists >> rail.Label('No') >> log_project_not_found >> process_time_entry_details

        get_all_tasks_for_project >> check_task_exists
        check_task_exists >> rail.Label('Yes') >> process_time_entry_details
        check_task_exists >> rail.Label('No') >> log_task_not_found >> process_time_entry_details

        process_time_entry_details >> has_required_ts_based_fields

        # Distribution only path
        has_required_ts_based_fields >> check_missing_mandatory_fields
        check_missing_mandatory_fields >> rail.Label('Yes') >> log_missing_ts_based_fields >> catch_and_log_errors
        check_missing_mandatory_fields >> rail.Label('No') >> if_inout_or_hour_present

        if_inout_or_hour_present >> rail.Label('Yes') >> add_time_entry
        if_inout_or_hour_present >> rail.Label('No') >> catch_and_log_errors

        add_time_entry >> log_success
        log_success >> catch_and_log_errors


    return dag

rail.for_each_instance(create_child_dag)
