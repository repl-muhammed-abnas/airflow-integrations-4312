"""T-Systems Time Import Child DAG for processing individual employee records."""

from datetime import timedelta
from uuid import uuid4
from tsystems.jira_time_import_v2.utils import request_payload, custom_methods, response_filters
from airflow.models import Variable
import rail

null = None

def create_child_dag(config):
    """
    Creates the Child DAG for processing individual T-Systems employee records.
    
    This DAG handles user-specific processing including user validation, timesheet
    management, time entry deletion, and triggering individual entry processing.

    Args:
        config: Configuration module containing instance-specific settings,
                connection IDs, timeouts, and processing parameters
    
    Returns:
        Airflow DAG: The configured child DAG for processing unique employee records
    """
    with rail.create_airflow_dag(
        dag_id=config.process_unique_users_child,
        description=f'T-Systems Jira Time Import Child - Process Unique Users {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Task: Check if batch processing is enabled via Airflow Variable
        # Controls whether tasks run in batch mode or individually for debugging
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_process_user_log'
        )

        # Task: Execute all user processing tasks in batch mode
        # Wraps the entire processing pipeline for error handling and monitoring
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_process_user_log',
            end_task='catch_and_log_errors',
        )

        # Task: Initialize logging for this employee's processing
        # Creates dedicated log stream for tracking all operations for this user
        create_process_user_log = rail.CreateLogOperator(
            task_id="create_process_user_log"
        )

        # Task: Retrieve all valid time entries for the current employee
        # Filters the validated records collection by employee ID
        get_all_records_for_user = rail.QueryCollectionOperator(
            task_id="get_all_records_for_user",
            query="""SELECT * FROM valid_entries fd WHERE fd.employee_id =:EMP_ID""",
            query_params={
                "EMP_ID": "{{ dag_run.conf.employee_id }}"
            },
            name="all_user_records"
        )

        # Task: Validate date range and 24-hour limit
        validate_date_and_hours = rail.PythonOperator(
            task_id='validate_date_and_hours',
            python_callable=custom_methods.validate_user_records_date_and_hours
        )
        
        # Task: Check if there are any invalid records from date/hours validation
        check_invalid_date_hours_validation = rail.IfOperator(
            task_id='check_invalid_date_hours_validation',
            test=lambda: rail.result('validate_date_and_hours')['invalid_count'] > 0,
            yes_task='log_date_hours_invalid_records',
            no_task='check_valid_date_hours_validation'
        )
        
        # Task: Log invalid records from date/hours validation
        log_date_hours_invalid_records = rail.WriteLogOperator(
            task_id='log_date_hours_invalid_records',
            log='{{ result("create_process_user_log") }}',
            items='{{ result("validate_date_and_hours").invalid_records | load_json_artifact | to_json }}',
            severity='Exception',
            message=lambda item: item['validation_error'],
            properties=lambda item: {
                'unique_id': item['unique_id'],
                'employee_id': item['employee_id'],
                'employee_email': item['user_email'],
                'entry_date': item['entry_date'],
                'hours': item['hours'],
                'project_id': item['project_id'],
                'project_manager_email': item['project_manager_email'],
                'task_name': item['task_name'],
                'status': 'Exception',
                'action': 'Validation',
                'details': item['validation_error']
            }
        )

        # Task: Check if there are any invalid records from date/hours validation
        check_valid_date_hours_validation = rail.IfOperator(
            task_id='check_valid_date_hours_validation',
            test=lambda: rail.result('validate_date_and_hours')['valid_count'] > 0,
            yes_task='get_user_details',
            no_task='catch_and_log_errors'
        )

        # Task: Fetch user details and timesheet template from Replicon
        # Retrieves user profile, assigned activities, and timesheet configuration
        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_user_data_payload,
            data_handler=lambda res: res[0] if (res and res[0]['userDetails']['isEnabled']) else null
        )

        # Task: Verify user exists and is enabled in Replicon system
        # Routes workflow based on successful user lookup results
        if_user_uri_present = rail.IfOperator(
            task_id='if_user_uri_present',
            test=lambda: rail.result('get_user_details'),
            yes_task="get_user_current_group_assignments",
            no_task="log_user_missing_in_replicon"
        )
        
        log_user_missing_in_replicon = rail.WriteLogOperator(
            task_id='log_user_missing_in_replicon',
            log='{{ result("create_process_user_log") }}',
            items='{{ result("validate_date_and_hours").valid_records | load_json_artifact | to_json }}',
            severity='Exception',
            message='User is not present or disabled in Replicon',
            properties=lambda item: {
                'unique_id': item['unique_id'],
                'employee_id': item['employee_id'],
                'employee_email': item['user_email'],
                'entry_date': item['entry_date'],
                'hours': item['hours'],
                'project_id': item['project_id'],
                'project_manager_email': item['project_manager_email'],
                'task_name': item['task_name'],
                'status': 'Exception',
                'action': 'Validation',
                'details': 'User is not present or disabled in Replicon'
            }
        )

        # Task: Fetch user's current assigned groups (department, cost center, location, employee type)
        # These groups will be used to validate if user can log time against project resources
        get_user_current_group_assignments = rail.RepliconServiceOperator(
            task_id="get_user_current_group_assignments",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data=lambda: {
                "userUri": rail.result('get_user_details')['userDetails']['uri']
            },
            data_handler=response_filters.filter_user_group_assignments
        )

        if_activity_assigned_to_user = rail.IfOperator(
            task_id ='if_activity_assigned_to_user',
            test=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('get_user_details')['assignedActivities'],
                'uri', dag_run.conf["activity_uri"], 'uri'),
            yes_task="if_user_has_timesheet_template",
            no_task="log_activity_not_assigned_to_user"
        )

        log_activity_not_assigned_to_user = rail.WriteLogOperator(
            task_id='log_activity_not_assigned_to_user',
            log='{{ result("create_process_user_log") }}',
            items='{{ result("validate_date_and_hours").valid_records | load_json_artifact | to_json }}',
            severity='Exception',
            message=f'Activity {config.activity_name} is not assigned to user',
            properties=lambda item: {
                'unique_id': item['unique_id'],
                'employee_id': item['employee_id'],
                'employee_email': item['user_email'],
                'entry_date': item['entry_date'],
                'hours': item['hours'],
                'project_id': item['project_id'],
                'project_manager_email': item['project_manager_email'],
                'task_name': item['task_name'],
                'status': 'Exception',
                'action': 'Validation',
                'details': f'Activity {config.activity_name} is not assigned to user'
            }
        )

        if_user_has_timesheet_template = rail.IfOperator(
            task_id='if_user_has_timesheet_template',
            test=lambda: rail.result("get_user_details")["timesheetTemplate"]["uri"] if rail.result("get_user_details")["timesheetTemplate"] else False,
            yes_task='get_timesheet_details',
            no_task='log_user_has_no_timesheet_template_in_replicon'
        )

        log_user_has_no_timesheet_template_in_replicon = rail.WriteLogOperator(
            task_id='log_user_has_no_timesheet_template_in_replicon',
            log='{{ result("create_process_user_log") }}',
            items='{{ result("validate_date_and_hours").valid_records | load_json_artifact | to_json }}',
            severity='Exception',
            message='Timesheet template is not assigned to the user in Replicon',
            properties=lambda item: {
                'unique_id': item['unique_id'],
                'employee_id': item['employee_id'],
                'employee_email': item['user_email'],
                'entry_date': item['entry_date'],
                'hours': item['hours'],
                'project_id': item['project_id'],
                'project_manager_email': item['project_manager_email'],
                'task_name': item['task_name'],
                'status': 'Exception',
                'action': 'Validation',
                'details': 'Timesheet template is not assigned to the user in Replicon'
            }
        )

        # Task: Get timesheet details for each entry date for the user
        # Retrieves or creates timesheets for all dates in the user's records
        get_timesheet_details = rail.RepliconServiceCallForEachItemOperator(
            task_id = "get_timesheet_details",
            items="{{result('get_all_records_for_user')}}",
            endpoint= "/services/TimesheetService1.svc/GetTimesheetDetailsForDate",
            data=lambda item: {
                "userUri": rail.result('get_user_details')['userDetails']['uri'],
                "date": rail.parse_date(
                    item['entry_date'], config.ENTRY_DATE_FORMAT),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            },
            all_result_data_handler=response_filters.get_timesheet_details
        )

        # Task: Extract URIs of submitted timesheets that need reopening
        # Identifies timesheets in submitted/approved status for modification
        get_submitted_ts_uris = rail.PythonOperator(
            task_id='get_submitted_ts_uris',
            python_callable=custom_methods.get_submitted_timesheet_uris
        )

        # Task: Reopen submitted timesheets to allow modifications
        # Changes timesheet status from submitted/approved back to open for editing
        reopen_timesheets = rail.RepliconServiceCallForEachItemOperator(
            task_id="reopen_timesheets",
            items="{{result('get_submitted_ts_uris') | to_json}}",
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data=lambda item: {
                "timesheetUri": item['ts_uri'],
                "unitOfWorkId": str(uuid4()),
                "comments": "Timesheet is reopened by the Integration (Jira Time Data Import)"
            }
        )

        # # Task: Delete existing time entries for clean data replacement
        # # Removes current time entries to prevent duplicates when adding new data
        # delete_time_entry = rail.RepliconServiceCallForEachItemOperator(
        #     task_id='delete_time_entry',
        #     items=lambda: rail.result('get_time_entry_details') if rail.result('get_time_entry_details') else [],
        #     endpoint='/services/TimeEntryRevisionGroupService1.svc/DeleteTimeEntryRevisionGroup',
        #     data=lambda item: {
        #         "timeEntryRevisionGroupUri": item
        #     }
        # )

        # Task: Extract unique entry dates for processing in/out time entries
        # Gets distinct dates to process attendance data separately from project time
        get_unique_entry_date_for_user = rail.QueryCollectionOperator(
            task_id="get_unique_entry_date_for_user",
            query="""SELECT DISTINCT entry_date FROM valid_entries fd WHERE fd.employee_id =:EMP_ID""",
            query_params={
                "EMP_ID": "{{dag_run.conf.employee_id}}"
            },
            name="unique_entry_date"
        )

        # Task: Launch child DAGs for processing project-based time entries
        # Triggers distribution time processing for each individual record with project context
        trigger_process_each_entry = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_process_each_entry',
            items="{{ result('validate_date_and_hours').valid_records | load_json_artifact | to_json }}",
            trigger_dag_id=config.process_each_entry_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                'input_data': {
                    **item,
                },
                **dag_run.conf,
                'user_uri': rail.result('get_user_details')['userDetails']['uri'],
                'user_email': rail.result("get_user_details")["userDetails"]["emailAddress"],
                'user_log': rail.result('create_process_user_log'),
                'user_groups': rail.result('get_user_current_group_assignments'),
            }
        )

        # Task: Wait for all individual entry processing DAGs to complete
        # Synchronization point ensuring all entries are processed before proceeding
        wait_for_process_each_entry = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_entry',
            dag_runs='{{ result("trigger_process_each_entry") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Task: Re-submit timesheets after processing is complete
        # Returns previously submitted timesheets back to submitted status
        submit_reopen_timesheets = rail.RepliconServiceCallForEachItemOperator(
            task_id='submit_reopen_timesheets',
            items="{{result('get_submitted_ts_uris') | to_json }}",
            endpoint="/services/TimesheetApprovalService1.svc/Submit2",
            data=lambda item: {
                "timesheetUri": item['ts_uri'],
                "unitOfWorkId": str(uuid4()),
                "comments": "Timesheet is submitted by the Integration (Jira Time Data Import)"
            }
        )

        # Task: Capture and log any errors that occur during processing
        # Error handler that logs failures for troubleshooting and reporting
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{result("create_process_user_log")}}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'unique_id': '',
                'employee_id': dag_run.conf["employee_id"],
                'employee_email': rail.result("get_user_details")["userDetails"]["emailAddress"] if rail.result("get_user_details") else '',
                'entry_date': '',
                'hours': '',
                'project_id': '',
                'project_manager_email': '',
                'task_name': '',
                'status': "Error",
                'action': "Validation",
                'details': rail.render_template('{{ get_error_message() }}')
            },
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_process_user_log

        create_process_user_log >> get_all_records_for_user >> validate_date_and_hours >> check_invalid_date_hours_validation
        check_invalid_date_hours_validation >> rail.Label("Yes") >> log_date_hours_invalid_records \
            >> check_valid_date_hours_validation
        check_invalid_date_hours_validation >> rail.Label("No") >> check_valid_date_hours_validation
        check_valid_date_hours_validation >> rail.Label("Yes") >> get_user_details
        check_valid_date_hours_validation >> rail.Label("No") >> catch_and_log_errors
        get_user_details >> if_user_uri_present
        if_user_uri_present >> rail.Label("Yes") >> get_user_current_group_assignments >> if_activity_assigned_to_user
        if_activity_assigned_to_user >> rail.Label("No") >> log_activity_not_assigned_to_user >> catch_and_log_errors
        if_activity_assigned_to_user >> rail.Label("Yes") >> if_user_has_timesheet_template
        if_user_has_timesheet_template >> rail.Label("No") >> log_user_has_no_timesheet_template_in_replicon >> catch_and_log_errors
        if_user_has_timesheet_template >> rail.Label("Yes") >> get_timesheet_details >> get_submitted_ts_uris
        get_submitted_ts_uris >> reopen_timesheets >> get_unique_entry_date_for_user >> trigger_process_each_entry >> \
            wait_for_process_each_entry >> submit_reopen_timesheets >> catch_and_log_errors
        if_user_uri_present >> rail.Label("No") >> log_user_missing_in_replicon >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
