"""T-Systems Time Import Child DAG for processing individual employee records."""

import rail
from datetime import timedelta
from uuid import uuid4
from tsystems.time_import_v1.utils import request_payload, custom_methods, response_filters
from airflow.models import Variable

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
        description=f'T-Systems Time Import Child - Process Unique Users {config.instance}',
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
                "EMP_ID": "{{dag_run.conf.employee_id}}"
            },
            name="all_user_records"
        )

        # Task: Fetch user details and timesheet template from Replicon
        # Retrieves user profile, assigned activities, and timesheet configuration
        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: request_payload.get_user_data_payload(dag_run.conf['employee_id']),
            data_handler=lambda res: res[0] if len(
                res) > 0 and res[0]["userDetails"]["uri"] else None
        )

        # Task: Verify user exists and is enabled in Replicon system
        # Routes workflow based on successful user lookup results
        if_user_uri_present = rail.IfOperator(
            task_id ='if_user_uri_present',
            test = lambda: bool(rail.result('get_user_details')),
            yes_task="get_user_substitute_users_details",
            no_task="log_user_missing_in_replicon"
        )
        
        log_user_missing_in_replicon = rail.WriteLogOperator(
            task_id='log_user_missing_in_replicon',
            items='{{ result("get_all_records_for_user") }}',
            log='{{ result("create_process_user_log") }}',
            severity='Exception',
            message='User is not present or disabled in replicon for EmployeeId: {{ dag_run.conf.employee_id }}',
            properties={
                'row_number': '{{ item.row_number }}',
                'employee_id': '{{ item.employee_id }}',
                'entry_date': '{{ item.entry_date }}',
                'project_id': '{{ item.project_id }}',
                'task_name': '{{ item.task_name }}',
                'activity': '{{ item.activity }}',
                'status': 'Exception',
                'action': 'Validation',
                'details': 'User is not present or disabled in replicon for EmployeeId: {{ dag_run.conf.employee_id }}'
            },
        )

        # Task: Fetch user's substitute users details
        get_user_substitute_users_details = rail.RepliconServiceOperator(
            task_id="get_user_substitute_users_details",
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('get_user_details')['userDetails']['uri']
            }
        )

        # Task: Verify reported by user is part of user's substitute users in Replicon system
        # Routes workflow based on successful user lookup results
        if_reported_user_is_substitute_user = rail.IfOperator(
            task_id ='if_reported_user_is_substitute_user',
            test=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result("get_user_substitute_users_details"),
                "user.uri", dag_run.conf["reported_by_user_uri"], "user.uri", None) is not None,
            yes_task="validate_records",
            no_task="log_reported_by_user_is_not_substitute_in_replicon"
        )

        log_reported_by_user_is_not_substitute_in_replicon = rail.WriteLogOperator(
            task_id='log_reported_by_user_is_not_substitute_in_replicon',
            items='{{ result("get_all_records_for_user") }}',
            log='{{ result("create_process_user_log") }}',
            severity='Exception',
            message='Reported by user is not the substitute user of the end user',
            properties={
                'row_number': '{{ item.row_number }}',
                'employee_id': '{{ item.employee_id }}',
                'entry_date': '{{ item.entry_date }}',
                'project_id': '{{ item.project_id }}',
                'task_name': '{{ item.task_name }}',
                'activity': '{{ item.activity }}',
                'status': 'Exception',
                'action': 'Validation',
                'details': 'Reported by user is not the substitute user of the end user'
            },
        )

        validate_records = rail.PythonOperator(
            task_id='validate_records',
            python_callable=custom_methods.validate_records_format
        )
        
        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items='{{ result("validate_records").invalid_records | to_json }}',
            log='{{ result("create_process_user_log") }}',
            severity='Exception',
            message='{{ item.validation_error }}',
            properties={
                'row_number': '{{ item.row_number }}',
                'employee_id': '{{ item.employee_id }}',
                'entry_date': '{{ item.entry_date }}',
                'project_id': '{{ item.project_id }}',
                'task_name': '{{ item.task_name }}',
                'activity': '{{ item.activity }}',
                'status': 'Exception',
                'action': 'Validation',
                'details': '{{ item.validation_error }}'
            },
        )

        if_valid_records_exists = rail.IfOperator(
            task_id='if_valid_records_exists',
            test=lambda: len(rail.result("validate_records")["valid_records"]) > 0,
            yes_task="get_timesheet_details",
            no_task="catch_and_log_errors"
        )

        # Task: Get timesheet details for each entry date for the user
        # Retrieves or creates timesheets for all dates in the user's records
        get_timesheet_details = rail.RepliconServiceCallForEachItemOperator(
            task_id = "get_timesheet_details",
            items="{{ result('validate_records').valid_records | to_json }}",
            endpoint= "/services/TimesheetService1.svc/GetTimesheetDetailsForDate",
            data=lambda item: {
                "userUri": rail.result('get_user_details')['userDetails']['uri'],
                "date": rail.parse_date(
                    item['entry_date'], config.entry_dateformat),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            },
            all_result_data_handler=response_filters.get_timesheet_details
        )

        # Task: Extract URIs of submitted timesheets that need reopening
        # Identifies timesheets in submitted/approved status for modification
        get_submitted_ts_uris = rail.PythonOperator(
            task_id='get_submitted_ts_uris',
            python_callable=lambda: custom_methods.get_submitted_timesheet_uris(rail.result('get_timesheet_details'))
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
                "comments": "Timesheet is reopened by Integration (Time Data Import)"
            }
        )

        # Task: Retrieve existing time entries for each date to be processed
        # Gets current time entries that may need to be deleted before adding new ones
        get_time_entry_details = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_time_entry_details",
            items="{{ result('validate_records').valid_records | to_json }}",
            endpoint="/services/TimeEntryRevisionGroupService1.svc/GetTimeEntryRevisionGroupsForUserAndDateRange",
            data=lambda item: request_payload.get_time_entries_for_user_date_range(
                user_uri=rail.result('get_user_details')['userDetails']['uri'],
                entry_date=rail.parse_date(item['entry_date'], config.entry_dateformat)
            ),
            all_result_data_handler=response_filters.filter_time_entries
        )

        # Task: Delete existing time entries for clean data replacement
        # Removes current time entries to prevent duplicates when adding new data
        delete_time_entry = rail.RepliconServiceCallForEachItemOperator(
            task_id='delete_time_entry',
            items=lambda: rail.result('get_time_entry_details') if rail.result('get_time_entry_details') else [],
            endpoint='/services/TimeEntryRevisionGroupService1.svc/DeleteTimeEntryRevisionGroup',
            data=lambda item: {
                "timeEntryRevisionGroupUri": item
            }
        )

        # Task: Extract unique entry dates for processing in/out time entries
        # Gets distinct dates to process attendance data separately from project time
        get_unique_entry_date_for_user = rail.QueryCollectionOperator(
            task_id="get_unique_entry_date_for_user",
            query="""SELECT DISTINCT entry_date FROM all_user_records fd WHERE fd.employee_id =:EMP_ID""",
            query_params={
                "EMP_ID": "{{dag_run.conf.employee_id}}"
            },
            name="unique_entry_date"
        )

        # Task: Launch child DAGs for processing in/out time entries by date
        # Triggers attendance time processing for each unique date with OEF configuration
        trigger_process_each_inout_entry = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_process_each_inout_entry',
            items="{{ result('validate_records').unique_entry_dates | to_json }}",
            trigger_dag_id=config.process_each_inout_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                **item,
                **dag_run.conf,
                'user_ts_template': custom_methods.get_effective_policy_set(item)['displayText'],
                'user_ts_uri': custom_methods.get_effective_policy_set(item)['uri'],
                'user_uri': rail.result('get_user_details')['userDetails']['uri'],
                'user_ts_type': config.TIMESHEET_TEMPLATES.get(custom_methods.get_effective_policy_set(item)['displayText'], ''),
                'user_log': rail.result('create_process_user_log'),
            }
        )

        # Task: Wait for all in/out time entry processing DAGs to complete
        # Synchronization point ensuring attendance time processing finishes first
        wait_for_process_each_inout_entry = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_inout_entry',
            dag_runs='{{ result("trigger_process_each_inout_entry") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Task: Launch child DAGs for processing project-based time entries
        # Triggers distribution time processing for each individual record with project context
        trigger_process_each_entry = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_process_each_entry',
            items="{{ result('validate_records').valid_records | to_json }}",
            trigger_dag_id=config.process_each_entry_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                'entry_data': {
                    **item,
                },
                **dag_run.conf,
                'activity_uri': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_user_details')['assignedActivities'], 'displayText', item['activity'], 'uri', ''
                ),
                'billing_rate_uri': rail.find_first_by_attr_and_get_attr(
                    dag_run.conf['billing_rates'], 'displayText', item['billing_rate_name'], 'uri', ''
                ),
                'user_ts_template': custom_methods.get_effective_policy_set(item)['displayText'],
                'user_ts_uri': custom_methods.get_effective_policy_set(item)['uri'],
                'user_uri': rail.result('get_user_details')['userDetails']['uri'],
                'user_ts_type': config.TIMESHEET_TEMPLATES.get(custom_methods.get_effective_policy_set(item)['displayText'], ''),
                'user_log': rail.result('create_process_user_log'),
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
            items="{{result('get_submitted_ts_uris') | to_json}}",
            endpoint="/services/TimesheetApprovalService1.svc/Submit2",
            data=lambda item: {
                "timesheetUri": item['ts_uri'],
                "unitOfWorkId": str(uuid4()),
                "comments": "Timesheet is submitted by Integration (Time Data Import)"
            }
        )

        # Task: Capture and log any errors that occur during processing
        # Error handler that logs failures for troubleshooting and reporting
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{result("create_process_user_log")}}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'row_number': '00',
                'employee_id': '{{ dag_run.conf.employee_id }}',
                'entry_date': '',
                'project_id': '',
                'task_name': '',
                'activity': '',
                'status': "Error",
                'action': "Validation",
                'details': '{{ get_error_message() }}'
            },
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_process_user_log

        create_process_user_log >> get_all_records_for_user >> get_user_details >> if_user_uri_present
        
        if_user_uri_present >> rail.Label("Yes") >> get_user_substitute_users_details >> if_reported_user_is_substitute_user
        if_user_uri_present >> rail.Label("No") >> log_user_missing_in_replicon >> catch_and_log_errors

        if_reported_user_is_substitute_user >> rail.Label("Yes") >> validate_records >> log_invalid_records >> if_valid_records_exists
        if_reported_user_is_substitute_user >> rail.Label("No") >> log_reported_by_user_is_not_substitute_in_replicon \
            >> catch_and_log_errors
        
        if_valid_records_exists >> rail.Label("No") >> catch_and_log_errors
        if_valid_records_exists >> rail.Label("Yes") >> get_timesheet_details

        get_timesheet_details >> get_submitted_ts_uris >> \
        reopen_timesheets >> get_time_entry_details >> delete_time_entry >> \
        get_unique_entry_date_for_user >> trigger_process_each_inout_entry >> wait_for_process_each_inout_entry >>\
        trigger_process_each_entry >> \
        wait_for_process_each_entry >> submit_reopen_timesheets >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
