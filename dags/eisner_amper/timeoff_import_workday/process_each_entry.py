"""Eisner Amper TimeOff Import Child DAG for processing individual employee time off records."""

from datetime import timedelta
from uuid import uuid4
import rail
from airflow.models import Variable
from eisner_amper.timeoff_import_workday.utils import custom_methods, request_payload, response_filters
from eisner_amper.timeoff_import_workday.utils.custom_methods import (
    ENTRY_STATUS_APPROVED, ENTRY_STATUS_WAITING_APPROVAL
)

null = None

def create_child_dag(config):
    """
    Creates the Child DAG for processing individual time off entry records.
    
    This DAG handles the processing of a single time off entry including project
    validation, task verification, field validation, and time off entry creation.

    Args:
        config: Configuration module containing instance-specific settings,
                validation rules, and processing parameters
    
    Returns:
        Airflow DAG: The configured child DAG for processing individual time off entries
    """
    with rail.create_airflow_dag(
        dag_id=config.process_each_entry_child,
        description=f'Eisner Amper Workday TimeOff Import Child - Process Each Entry {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_time_entries_child,
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
                        "code": dag_run.conf['input_data']['project_code']
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
                'booking_reference_id': '{{ dag_run.conf.input_data.booking_reference_id }}',
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'start_date': '{{ dag_run.conf.input_data.start_date }}',
                'hours': '{{ dag_run.conf.input_data.hours }}',
                'project_code': '{{ dag_run.conf.input_data.project_code }}',
                'status': 'Exception',
                'action': 'Validation',
                'details': 'Project is not available in Replicon'
            },
        )

        # Task: Retrieve all tasks associated with the validated project
        # For Eisner Amper time off entries, we always look for "Default Task"
        get_required_tasks_for_project = rail.RepliconServiceOperator(
            task_id="get_required_tasks_for_project",
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data=lambda: {
                "parentUri": rail.result('get_all_project_details')['projectDetails']['uri']
            },
            data_handler=lambda response: response_filters.format_project_task_details(response, config.DEFAULT_TASK_NAME)
        )

        # Task: Verify that specified task name exists within the project
        # Validates task existence before proceeding with time entry creation
        check_task_exists = rail.IfOperator(
            task_id="check_task_exists",
            test=lambda: rail.result("get_required_tasks_for_project"),
            yes_task="if_time_entry_exists",
            no_task="log_task_not_found"
        )

        # Task: Log error when specified task is not found within the project
        # Records task validation failure with detailed context information
        log_task_not_found = rail.WriteLogOperator(
            task_id='log_task_not_found',
            log='{{ dag_run.conf.user_log }}',
            severity='Exception',
            message='Default Task is not available for project in Replicon',
            properties={
                'booking_reference_id': '{{ dag_run.conf.input_data.booking_reference_id }}',
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'start_date': '{{ dag_run.conf.input_data.start_date }}',
                'hours': '{{ dag_run.conf.input_data.hours }}',
                'project_code': '{{ dag_run.conf.input_data.project_code }}',
                'status': 'Exception',
                'action': 'Validation',
                'details': 'Default Task is not available for project in Replicon'
            },
        )

        # Task: Check if time entry exists using enriched data from parent DAG
        # Determines whether to create new or update/delete existing
        if_time_entry_exists = rail.IfOperator(
            task_id='if_time_entry_exists',
            test=lambda dag_run: dag_run.conf['input_data']['existing_entry_uri'],
            yes_task='get_time_entry_details',
            no_task='check_if_delete_for_new'
        )
        
        # Task: Get fresh time entry details from API for latest status
        # Ensures we have current approval status before making modifications
        get_time_entry_details = rail.RepliconServiceOperator(
            task_id='get_time_entry_details',
            endpoint='/services/TimeEntryRevisionGroupService1.svc/GetTimeEntryRevisionGroupsForUserAndDateRange',
            data=lambda dag_run: request_payload.get_time_entries_for_user_date_range(dag_run, config.ENTRY_DATE_FORMAT),
            data_handler=lambda response, dag_run: response_filters.filter_time_entries(response, dag_run)
        )
        
        # Check if existing entry is approved (cannot be modified)
        check_if_approved = rail.IfOperator(
            task_id='check_if_approved',
            test=lambda: rail.result('get_time_entry_details') and rail.result('get_time_entry_details')['approval_status'] == ENTRY_STATUS_APPROVED,
            yes_task='check_if_delete_for_approved',
            no_task='check_if_needs_reopen'
        )
        
        # Check if entry needs reopening (only Waiting For Approval status needs reopening)
        # Rejected status entries are already editable
        check_if_needs_reopen = rail.IfOperator(
            task_id='check_if_needs_reopen',
            test=lambda: rail.result('get_time_entry_details') and rail.result('get_time_entry_details')['approval_status'] == ENTRY_STATUS_WAITING_APPROVAL,
            yes_task='reopen_time_entry',
            no_task='check_if_delete_for_existing'
        )
        
        # Task: Reopen time entry if it's in Waiting For Approval status
        reopen_time_entry = rail.RepliconServiceOperator(
            task_id='reopen_time_entry',
            endpoint='/services/TimeEntryRevisionGroupApprovalService1.svc/Reopen',
            data=lambda: {
                'timeEntryRevisionGroupUri': rail.result('get_time_entry_details')['entry_uri'],
                'unitOfWorkId': str(uuid4()),
                'comments': 'Time Entry is reopened by the Replicon Integration (Workday TimeOff Data Import)'
            }
        )
        
        # For approved entries, check if it's a DELETE operation
        check_if_delete_for_approved = rail.IfOperator(
            task_id='check_if_delete_for_approved',
            test=lambda dag_run: float(dag_run.conf['input_data']['hours']) == 0,
            yes_task='log_cannot_delete_approved',
            no_task='log_cannot_update_approved'
        )
        
        # Log error when trying to update approved entry
        log_cannot_update_approved = rail.WriteLogOperator(
            task_id='log_cannot_update_approved',
            log='{{ dag_run.conf.user_log }}',
            severity='Exception',
            message='Cannot update approved time entry',
            properties={
                'booking_reference_id': '{{ dag_run.conf.input_data.booking_reference_id }}',
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'start_date': '{{ dag_run.conf.input_data.start_date }}',
                'hours': '{{ dag_run.conf.input_data.hours }}',
                'project_code': '{{ dag_run.conf.input_data.project_code }}',
                'status': 'Exception',
                'action': 'Update',
                'details': 'Cannot update approved time entry'
            }
        )
        
        # Log error when trying to delete approved entry
        log_cannot_delete_approved = rail.WriteLogOperator(
            task_id='log_cannot_delete_approved',
            log='{{ dag_run.conf.user_log }}',
            severity='Exception',
            message='Cannot delete approved time entry',
            properties={
                'booking_reference_id': '{{ dag_run.conf.input_data.booking_reference_id }}',
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'start_date': '{{ dag_run.conf.input_data.start_date }}',
                'hours': '{{ dag_run.conf.input_data.hours }}',
                'project_code': '{{ dag_run.conf.input_data.project_code }}',
                'status': 'Exception',
                'action': 'Delete',
                'details': 'Cannot delete approved time entry'
            }
        )
        
        # For existing entries, check if it's a DELETE operation
        check_if_delete_for_existing = rail.IfOperator(
            task_id='check_if_delete_for_existing',
            test=lambda dag_run: float(dag_run.conf['input_data']['hours']) == 0,
            yes_task='delete_time_entry',
            no_task='update_time_entry'
        )
        
        # For non-existing entries, check if it's a DELETE operation
        check_if_delete_for_new = rail.IfOperator(
            task_id='check_if_delete_for_new',
            test=lambda dag_run: float(dag_run.conf['input_data']['hours']) == 0,
            yes_task='log_delete_entry_not_found',
            no_task='add_time_entry'
        )

        # Task: Log when entry to delete is not found
        log_delete_entry_not_found = rail.WriteLogOperator(
            task_id='log_delete_entry_not_found',
            log='{{ dag_run.conf.user_log }}',
            severity='Exception',
            message='Time entry not found for deletion',
            properties={
                'booking_reference_id': '{{ dag_run.conf.input_data.booking_reference_id }}',
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'start_date': '{{ dag_run.conf.input_data.start_date }}',
                'hours': '{{ dag_run.conf.input_data.hours }}',
                'project_code': '{{ dag_run.conf.input_data.project_code }}',
                'status': 'Exception',
                'action': 'Delete',
                'details': 'Time entry not found for deletion'
            }
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
                'booking_reference_id': '{{ dag_run.conf.input_data.booking_reference_id }}',
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'start_date': '{{ dag_run.conf.input_data.start_date }}',
                'hours': '{{ dag_run.conf.input_data.hours }}',
                'project_code': '{{ dag_run.conf.input_data.project_code }}',
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
                'booking_reference_id': '{{ dag_run.conf.input_data.booking_reference_id }}',
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'start_date': '{{ dag_run.conf.input_data.start_date }}',
                'hours': '{{ dag_run.conf.input_data.hours }}',
                'project_code': '{{ dag_run.conf.input_data.project_code }}',
                'status': 'Success',
                'action': 'Add',
                'details': 'Time entry added successfully'
            }
        )

        # Task: Delete time entry when hours = 0
        # Removes time entry from Replicon
        delete_time_entry = rail.RepliconServiceOperator(
            task_id='delete_time_entry',
            endpoint='/services/TimeEntryRevisionGroupService1.svc/DeleteTimeEntryRevisionGroup',
            data=lambda: {
                'timeEntryRevisionGroupUri': rail.result('get_time_entry_details')['entry_uri']
            }
        )
        
        # Task: Log successful deletion
        log_delete_success = rail.WriteLogOperator(
            task_id='log_delete_success',
            log='{{ dag_run.conf.user_log }}',
            severity='Success',
            message='Time entry deleted successfully',
            properties={
                'booking_reference_id': '{{ dag_run.conf.input_data.booking_reference_id }}',
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'start_date': '{{ dag_run.conf.input_data.start_date }}',
                'hours': '{{ dag_run.conf.input_data.hours }}',
                'project_code': '{{ dag_run.conf.input_data.project_code }}',
                'status': 'Success',
                'action': 'Delete',
                'details': 'Time entry deleted successfully'
            }
        )
        
        # Task: Submit time entry after update
        submit_time_entry_after_update = rail.RepliconServiceOperator(
            task_id='submit_time_entry_after_update',
            endpoint='/services/TimeEntryRevisionGroupApprovalService1.svc/Submit',
            data=lambda: {
                'timeEntryRevisionGroupUri': rail.result('update_time_entry')['uri'],
                'unitOfWorkId': str(uuid4()),
                'comments': 'Time Entry is submitted by the Replicon Integration (Workday TimeOff Data Import)'
            }
        )
        
        # Task: Submit time entry after add
        submit_time_entry_after_add = rail.RepliconServiceOperator(
            task_id='submit_time_entry_after_add',
            endpoint='/services/TimeEntryRevisionGroupApprovalService1.svc/Submit',
            data=lambda: {
                'timeEntryRevisionGroupUri': rail.result('add_time_entry')['uri'],
                'unitOfWorkId': str(uuid4()),
                'comments': 'Time Entry is submitted by the Replicon Integration (Workday TimeOff Data Import)'
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
                'booking_reference_id': '{{ dag_run.conf.input_data.booking_reference_id }}',
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'start_date': '{{ dag_run.conf.input_data.start_date }}',
                'hours': '{{ dag_run.conf.input_data.hours }}',
                'project_code': '{{ dag_run.conf.input_data.project_code }}',
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
        check_task_exists >> rail.Label('Yes') >> if_time_entry_exists
        
        # If entry exists, get fresh details then check if it's approved
        if_time_entry_exists >> rail.Label('Yes') >> get_time_entry_details >> check_if_approved
        
        # If approved, determine if it's update or delete and log error
        check_if_approved >> rail.Label('Yes') >> check_if_delete_for_approved
        check_if_delete_for_approved >> rail.Label('Yes') >> log_cannot_delete_approved >> catch_and_log_errors
        check_if_delete_for_approved >> rail.Label('No') >> log_cannot_update_approved >> catch_and_log_errors
        
        # If not approved, check if needs reopening
        check_if_approved >> rail.Label('No') >> check_if_needs_reopen
        
        # If needs reopening (Waiting For Approval or Rejected), reopen first
        check_if_needs_reopen >> rail.Label('Yes') >> reopen_time_entry >> check_if_delete_for_existing
        
        # If doesn't need reopening (Not Submitted), proceed directly
        check_if_needs_reopen >> rail.Label('No') >> check_if_delete_for_existing
        
        # Delete flow for existing entries (not approved)
        check_if_delete_for_existing >> rail.Label('Yes') >> delete_time_entry >> log_delete_success
        
        # Update flow for existing entries (not approved)
        check_if_delete_for_existing >> rail.Label('No') >> update_time_entry >> submit_time_entry_after_update \
            >> log_update_time_entry_success >> catch_and_log_errors
        
        # If entry was deleted, no submission needed
        log_delete_success >> catch_and_log_errors
        
        # If entry doesn't exist, check what operation to perform
        if_time_entry_exists >> rail.Label('No') >> check_if_delete_for_new
        check_if_delete_for_new >> rail.Label('Yes') >> log_delete_entry_not_found >> catch_and_log_errors
        check_if_delete_for_new >> rail.Label('No') >> add_time_entry >> submit_time_entry_after_add >> log_add_time_entry_success

        check_task_exists >> rail.Label('No') >> log_task_not_found >> catch_and_log_errors

        log_add_time_entry_success >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
