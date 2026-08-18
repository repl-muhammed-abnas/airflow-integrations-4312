"""Eisner Amper TimeOff Import Child DAG for processing individual employee time off records."""

from datetime import timedelta
from uuid import uuid4
from eisner_amper.timeoff_import_workday.utils import request_payload, custom_methods, response_filters
from airflow.models import Variable
import rail

null = None

def create_child_dag(config):
    """
    Creates the Child DAG for processing individual Eisner Amper employee time off records.
    
    This DAG handles user-specific processing including user validation, timesheet
    management, time off entry deletion, and triggering individual entry processing.

    Args:
        config: Configuration module containing instance-specific settings,
                connection IDs, timeouts, and processing parameters
    
    Returns:
        Airflow DAG: The configured child DAG for processing unique employee records
    """
    with rail.create_airflow_dag(
        dag_id=config.process_unique_users_child,
        description=f'Eisner Amper Workday TimeOff Import Child - Process Unique Users {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_users_child,
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
        # Filters the validated records for the specific employee
        get_all_records_for_user = rail.PythonOperator(
            task_id="get_all_records_for_user",
            python_callable=custom_methods.get_records_for_employee
        )

        # Task: Fetch user details and timesheet template from Replicon
        # Retrieves user profile, assigned activities, and timesheet configuration
        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_user_data_payload,
            data_handler=lambda res: res[0] if len(
                res) > 0 and res[0]["userDetails"]["isEnabled"] else null
        )

        # Task: Verify user exists and is enabled in Replicon system
        # Routes workflow based on successful user lookup results
        if_user_uri_present = rail.IfOperator(
            task_id='if_user_uri_present',
            test=lambda: rail.result('get_user_details'),
            yes_task="if_user_has_timesheet_template",
            no_task="log_user_missing_in_replicon"
        )
        
        log_user_missing_in_replicon = rail.WriteLogOperator(
            task_id='log_user_missing_in_replicon',
            log='{{ result("create_process_user_log") }}',
            items='{{ result("get_all_records_for_user") | to_json }}',
            severity='Exception',
            message='User is not present or disabled in Replicon',
            properties=lambda item: {
                'booking_reference_id': item['booking_reference_id'],
                'employee_id': item['employee_id'],
                'start_date': item['start_date'],
                'hours': item['hours'],
                'project_code': item['project_code'],
                'status': 'Exception',
                'action': 'Validation',
                'details': 'User is not present or disabled in Replicon'
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
            items='{{ result("get_all_records_for_user") | to_json }}',
            severity='Exception',
            message='Timesheet template is not assigned to the user in Replicon',
            properties=lambda item: {
                'booking_reference_id': item['booking_reference_id'],
                'employee_id': item['employee_id'],
                'start_date': item['start_date'],
                'hours': item['hours'],
                'project_code': item['project_code'],
                'status': 'Exception',
                'action': 'Validation',
                'details': 'Timesheet template is not assigned to the user in Replicon'
            }
        )

        # Task: Get timesheet details for each entry date for the user
        # Retrieves or creates timesheets for all dates in the user's records
        get_timesheet_details = rail.RepliconServiceCallForEachItemOperator(
            task_id = "get_timesheet_details",
            items="{{ result('get_all_records_for_user') | to_json }}",
            endpoint= "/services/TimesheetService1.svc/GetTimesheetDetailsForDate",
            data=lambda item: {
                "userUri": rail.result('get_user_details')['userDetails']['uri'],
                "date": rail.parse_date(
                    item['start_date'], config.ENTRY_DATE_FORMAT),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            },
            data_handler=lambda response, item: response_filters.get_timesheet_details(response, item),
            all_result_data_handler=lambda data_handler: list(filter(lambda item: item, data_handler))
        )
        
        # Task: Get all time entries for the user to check existing entries
        # Fetches time entries for all dates to enable update/delete decisions
        get_time_entries_for_user = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_time_entries_for_user",
            items='{{ result("get_all_records_for_user") | to_json }}',
            endpoint="/services/TimeEntryRevisionGroupService1.svc/GetTimeEntryRevisionGroupsForUserAndDateRange",
            data=lambda item: {
                "user": {
                    "uri": rail.result('get_user_details')['userDetails']['uri'],
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                },
                "dateRange": {
                    "startDate": rail.parse_date(item['start_date'], config.ENTRY_DATE_FORMAT),
                    "endDate": rail.parse_date(item['start_date'], config.ENTRY_DATE_FORMAT),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
            data_handler=lambda response, item: {
                'date': item['start_date'],
                'entries': response_filters.format_time_entries_for_enrichment(response)
            },
            all_result_data_handler=lambda results: rail.write_json_artifact(results)
        )
        
        # Task: Enrich user records with existing time entry details
        # Appends entry information to records for smart processing decisions
        enrich_user_records_with_entry_details = rail.PythonOperator(
            task_id='enrich_user_records_with_entry_details',
            python_callable=lambda: custom_methods.enrich_records_with_entry_details(
                rail.result('get_all_records_for_user'),
                rail.load_json_artifact(rail.result('get_time_entries_for_user'))
            )
        )

        # Task: Categorize timesheets and records by approval status
        # Identifies which records can be processed and which timesheets need actions
        categorize_user_records_on_timesheets = rail.PythonOperator(
            task_id='categorize_user_records_on_timesheets',
            python_callable=custom_methods.categorize_user_records_and_timesheets
        )
        
        # Task: Check if there are records that cannot be processed
        check_blocked_records = rail.IfOperator(
            task_id='check_blocked_records',
            test=lambda: rail.result('categorize_user_records_on_timesheets')['has_blocked_records'],
            yes_task='log_blocked_records',
            no_task='reopen_timesheets'
        )
        
        # Task: Log records that cannot be processed with specific reasons
        log_blocked_records = rail.WriteLogOperator(
            task_id='log_blocked_records',
            log='{{ result("create_process_user_log") }}',
            items='{{ result("categorize_user_records_on_timesheets").blocked_records | to_json }}',
            severity='Exception',
            message='Cannot process entry',
            properties=lambda item: {
                'booking_reference_id': item['booking_reference_id'],
                'employee_id': item['employee_id'],
                'start_date': item['start_date'],
                'hours': item['hours'],
                'project_code': item['project_code'],
                'status': 'Exception',
                'action': 'Delete' if float(item.get('hours', 0)) == 0 else ('Update' if item.get('existing_entry_uri') else 'Add'),
                'details': item.get('block_reason', 'Cannot process entry')
            }
        )

        # Task: Reopen submitted timesheets to allow modifications
        # Changes timesheet status from submitted/approved back to open for editing
        reopen_timesheets = rail.RepliconServiceCallForEachItemOperator(
            task_id="reopen_timesheets",
            items="{{ result('categorize_user_records_on_timesheets').timesheets_to_reopen | to_json }}",
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data=lambda item: {
                "timesheetUri": item['ts_uri'],
                "unitOfWorkId": str(uuid4()),
                "comments": "Timesheet is reopened by the Replicon Integration (Workday TimeOff Data Import)"
            }
        )

        # Task: Launch child DAGs for processing project-based time entries
        # Only processes records that are not under approved timesheets
        trigger_process_each_entry = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_process_each_entry',
            items="{{ result('categorize_user_records_on_timesheets').processable_records | to_json }}",
            trigger_dag_id=config.process_each_entry_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                'input_data': {
                    **item,
                },
                **dag_run.conf,
                'user_uri': rail.result('get_user_details')['userDetails']['uri'],
                'user_work_location': custom_methods.get_effective_user_location(),
                'timesheet_work_location_uri': rail.find_first_by_attr_and_get_attr(custom_methods.get_work_location_oef_values(dag_run), "name", custom_methods.get_effective_user_location()["displayText"], "uri") if custom_methods.get_effective_user_location() else null,
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

        # Task: Submit all timesheets after processing is complete
        # This includes reopened timesheets AND timesheets that were Not Submitted/Rejected
        submit_all_timesheets = rail.RepliconServiceCallForEachItemOperator(
            task_id='submit_all_timesheets',
            items="{{ result('categorize_user_records_on_timesheets').timesheets_to_submit | to_json }}",
            endpoint="/services/TimesheetApprovalService1.svc/Submit2",
            data=lambda item: {
                "timesheetUri": item['ts_uri'],
                "unitOfWorkId": str(uuid4()),
                "comments": "Timesheet is submitted by the Replicon Integration (Workday TimeOff Data Import)"
            }
        )

        # Task: Capture and log any errors that occur during processing
        # Error handler that logs failures for troubleshooting and reporting
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{ result("create_process_user_log") }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'booking_reference_id': '',
                'employee_id': '{{ dag_run.conf.employee_id }}',
                'start_date': '',
                'hours': '',
                'project_code': '',
                'status': "Error",
                'action': "Validation",
                'details': '{{ get_error_message() }}'
            },
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_process_user_log

        create_process_user_log >> get_all_records_for_user >> get_user_details >> if_user_uri_present
        if_user_uri_present >> rail.Label("Yes") >> if_user_has_timesheet_template
        if_user_has_timesheet_template >> rail.Label("No") >> log_user_has_no_timesheet_template_in_replicon >> catch_and_log_errors
        if_user_has_timesheet_template >> rail.Label("Yes") >> get_timesheet_details >> get_time_entries_for_user \
            >> enrich_user_records_with_entry_details >> categorize_user_records_on_timesheets >> check_blocked_records
        
        # Handle blocked records logging
        check_blocked_records >> rail.Label("Yes") >> log_blocked_records >> reopen_timesheets
        check_blocked_records >> rail.Label("No") >> reopen_timesheets
        
        # Main processing flow
        reopen_timesheets >> trigger_process_each_entry >> wait_for_process_each_entry >> submit_all_timesheets >> catch_and_log_errors
        if_user_uri_present >> rail.Label("No") >> log_user_missing_in_replicon >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
