"""
Creates or updates individual time entries in Replicon
Validates task, and user assignments before creating entries
"""

from datetime import timedelta
from pendulum import datetime
from airflow.models import Variable
from ipipeline.time_import.utils import request_payload, response_filters, custom_methods
import rail

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_time_entry_child_dag_id,
        description=f"iPipeline JIRA Time Import Process Each Time Entry Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_each_time_entry_max_active_runs
    ) as dag:

        # View incoming configuration (standalone task for debugging)
        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_conf'
        )

       # Task: Check if batch processing mode is enabled
        # Controls execution flow for debugging vs production processing
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_timesheet_details_for_user_and_date'
        )

        # Task: Execute entry processing pipeline in batch mode
        # Wraps individual entry processing for monitoring and error handling
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_timesheet_details_for_user_and_date',
            end_task='catch_and_log_errors',
        )

        get_timesheet_details_for_user_and_date = rail.RepliconServiceOperator(
            task_id="get_timesheet_details_for_user_and_date",
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetailsForDate",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "date": rail.parse_date(
                    dag_run.conf['time_entry_date'], config.ENTRY_DATE_FORMAT),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            },
            data_handler=lambda response: response_filters.get_timesheet_details_for_user_and_date(
                response)
        )

        if_timesheet_not_in_open_status = rail.IfOperator(
            task_id="if_timesheet_not_in_open_status",
            test=lambda: rail.result('get_timesheet_details_for_user_and_date')[
                'timesheet_status'] != 'open',
            yes_task="log_timesheet_not_in_open_status",
            no_task="get_all_project_details"
        )

        # Task: Log exception when specified timesheet is not in Open status
        # Records project validation failure for reporting and troubleshooting
        log_timesheet_not_in_open_status = rail.WriteLogOperator(
            task_id='log_timesheet_not_in_open_status',
            log='{{ dag_run.conf.log }}',
            severity='Exception',
            message='Timesheet is not in open status',
            properties=lambda dag_run: {
                'task_issue_id': dag_run.conf['task_issue_id'],
                'task_type': dag_run.conf['task_type'],
                'time_entry_date': dag_run.conf['time_entry_date'],
                'hours': dag_run.conf['hours'],
                'replicon_id': dag_run.conf['replicon_id'],
                'action': 'Validation',
                'status': 'Exception',
                "details": "Timesheet is not in OPEN status",
            }
        )

        # Step 1: Validate project exists in Replicon
        get_all_project_details = rail.RepliconServiceOperator(
            task_id='get_all_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=lambda dag_run: {
                "projects": [{
                    "code": dag_run.conf['replicon_id']
                }]
            },
            data_handler=lambda res: res[0] if (
                res and res[0].get('projectDetails')) else null
        )

        # Task: Verify that project lookup returned valid results
        # Determines whether to proceed with task validation or log exception
        check_if_project_is_ready_for_time_entry = rail.IfOperator(
            task_id="check_if_project_is_ready_for_time_entry",
            test=lambda: custom_methods.get_project_validaiton_check_and_details(
                'check'),
            yes_task="get_required_tasks_for_project",
            no_task="log_project_validation_exception"
        )

        # Task: Log exception when specified project is not valid for time entry in Replicon
        # Records project validation failure for reporting and troubleshooting
        log_project_validation_exception = rail.WriteLogOperator(
            task_id='log_project_validation_exception',
            log='{{ dag_run.conf.log }}',
            severity='Exception',
            message='Project is not valid for time entry',
            properties=lambda dag_run: {
                'task_issue_id': dag_run.conf['task_issue_id'],
                'task_type': dag_run.conf['task_type'],
                'time_entry_date': dag_run.conf['time_entry_date'],
                'hours': dag_run.conf['hours'],
                'replicon_id': dag_run.conf['replicon_id'],
                'action': 'Validation',
                'status': 'Exception',
                "details": "Time entry not processed - " + custom_methods.get_project_validaiton_check_and_details('details'),
            }
        )

        # Task: Retrieve all tasks associated with the validated project
        # Gets task hierarchy and details for task name validation
        get_required_tasks_for_project = rail.RepliconServiceOperator(
            task_id="get_required_tasks_for_project",
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data=lambda: {
                "parentUri": rail.result('get_all_project_details')['projectDetails']['uri']
            },
            data_handler=lambda response, dag_run: response_filters.format_project_task_details(
                response, dag_run)
        )

        # Task: Verify that specified task name exists within the project
        # Validates task existence before proceeding with time entry creation
        check_task_exists = rail.IfOperator(
            task_id="check_task_exists",
            test=lambda: rail.result("get_required_tasks_for_project"),
            yes_task="get_resources_assigned_to_task",
            no_task="log_task_not_found"
        )

        # Task: Log exception when specified task is not found within the project
        # Records task validation failure with detailed context information
        log_task_not_found = rail.WriteLogOperator(
            task_id='log_task_not_found',
            log='{{ dag_run.conf.log }}',
            severity='Exception',
            message='Task is not available for project in Replicon',
            properties=lambda dag_run: {
                'task_issue_id': dag_run.conf['task_issue_id'],
                'task_type': dag_run.conf['task_type'],
                'time_entry_date': dag_run.conf['time_entry_date'],
                'hours': dag_run.conf['hours'],
                'replicon_id': dag_run.conf['replicon_id'],
                'action': 'Validation',
                'status': 'Exception',
                "details": 'Task is not available for project in Replicon',
            }
        )

        get_resources_assigned_to_task = rail.RepliconServiceOperator(
            task_id='get_resources_assigned_to_task',
            endpoint='/services/TaskService1.svc/BulkGetResourceAssignments',
            data=lambda dag_run: {
                "taskUris": [
                    rail.find_first_by_attr_and_get_attr(rail.result(
                        'get_required_tasks_for_project'), 'full_task_name', dag_run.conf['task_type'], 'uri', '')
                ],
                "asOfDate": rail.parse_date(dag_run.conf['time_entry_date'], config.ENTRY_DATE_FORMAT)
            },
            data_handler=lambda response: list(map(lambda x: {
                'user_uri': x['resource']['user']['uri'] if x['resource']['user'] else '',
                'user_loginname': x['resource']['user']['loginName'] if x['resource']['user'] else ''
            }, response[0]['assignments'])) if (response and response[0]) else []
        )

        check_user_resource_at_task_level = rail.IfOperator(
            task_id="check_user_resource_at_task_level",
            test=lambda dag_run: bool(rail.find_first_by_attr_and_get_attr(
                rail.result("get_resources_assigned_to_task"), 'user_uri', dag_run.conf['user_uri'], '')),
            yes_task="add_time_entry",
            no_task="log_resource_not_assigned_to_task"
        )

        # Task: Log exception when specified task is not found within the project
        # Records task validation failure with detailed context information
        log_resource_not_assigned_to_task = rail.WriteLogOperator(
            task_id='log_resource_not_assigned_to_task',
            log='{{ dag_run.conf.log }}',
            severity='Exception',
            message='Resource is not assigned to task',
            properties=lambda dag_run: {
                'task_issue_id': dag_run.conf['task_issue_id'],
                'task_type': dag_run.conf['task_type'],
                'time_entry_date': dag_run.conf['time_entry_date'],
                'hours': dag_run.conf['hours'],
                'replicon_id': dag_run.conf['replicon_id'],
                'action': 'Validation',
                'status': 'Exception',
                "details": 'Resource is not assigned to task',
            }
        )

        # Task: Create new time entry in Replicon timesheet
        # Submits time allocation with project, task and time data
        add_time_entry = rail.RepliconServiceOperator(
            task_id="add_time_entry",
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=lambda dag_run: request_payload.put_time_entry_payload(
                dag_run, config.ENTRY_DATE_FORMAT, config.OEF_MAPPER)
        )

        # Records successful processing for reporting and audit purposes
        log_process_time_entry_success = rail.WriteLogOperator(
            task_id='log_process_time_entry_success',
            log='{{ dag_run.conf.log }}',
            severity='Success',
            message=lambda: "Time entry " +
                ("updated" if rail.result('get_time_entry_details')
                 else "added") + " successfully.",
            properties=lambda dag_run: {
                'task_issue_id': dag_run.conf['task_issue_id'],
                'task_type': dag_run.conf['task_type'],
                'time_entry_date': dag_run.conf['time_entry_date'],
                'hours': dag_run.conf['hours'],
                'replicon_id': dag_run.conf['replicon_id'],
                'action': "Update" if rail.result('get_time_entry_details') else "Add",
                'status': 'Success',
                "details": "Time entry " + ("updated" if rail.result('get_time_entry_details') else "added") + " successfully.",
                }
        )

        # Task: Capture and log any processing errors for this entry
        # Central error handler for troubleshooting and failure reporting
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.log }}',
            severity='Exception',
            message='Time entry not processed due to error',
            properties=lambda dag_run: {
                'task_issue_id': dag_run.conf['task_issue_id'],
                'task_type': dag_run.conf['task_type'],
                'time_entry_date': dag_run.conf['time_entry_date'],
                'hours': dag_run.conf['hours'],
                'replicon_id': dag_run.conf['replicon_id'],
                'action': 'Process Time entry',
                'status': 'Error',
                "details": '{{get_error_message()}}',
            }
        )

       # DAG dependencies
        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> get_timesheet_details_for_user_and_date

        get_timesheet_details_for_user_and_date >> if_timesheet_not_in_open_status

        if_timesheet_not_in_open_status >> rail.Label(
            'No') >> get_all_project_details
        if_timesheet_not_in_open_status >> rail.Label(
            'Yes') >> log_timesheet_not_in_open_status >> catch_and_log_errors

        get_all_project_details >> check_if_project_is_ready_for_time_entry
        check_if_project_is_ready_for_time_entry >> rail.Label(
            'Yes') >> get_required_tasks_for_project
        check_if_project_is_ready_for_time_entry >> rail.Label(
            'No') >> log_project_validation_exception >> catch_and_log_errors

        get_required_tasks_for_project >> check_task_exists

        check_task_exists >> rail.Label(
            'No') >> log_task_not_found >> catch_and_log_errors
        check_task_exists >> rail.Label(
            'Yes') >> get_resources_assigned_to_task

        get_resources_assigned_to_task >> check_user_resource_at_task_level

        check_user_resource_at_task_level >> rail.Label(
            'No') >> log_resource_not_assigned_to_task >> catch_and_log_errors
        check_user_resource_at_task_level >> rail.Label(
            'Yes') >> add_time_entry

        add_time_entry >> log_process_time_entry_success

        log_process_time_entry_success >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
