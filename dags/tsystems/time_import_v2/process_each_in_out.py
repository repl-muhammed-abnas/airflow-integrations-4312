"""T-Systems Time Import Child DAG for processing individual employee records."""

from datetime import timedelta
import rail
from airflow.models import Variable
from tsystems.time_import_v2.utils import custom_methods, request_payload

def create_child_dag(config):
    """
    Creates the Child DAG for processing attendance-based in/out time entries.
    
    This DAG handles attendance time allocation by processing in-time and out-time
    data for specific dates. It creates time entries with attendance allocation type
    and applies appropriate Object Extension Field values for worktype classification.

    Args:
        config: Configuration module containing instance-specific settings,
                DAG IDs, and timesheet template configurations
    
    Returns:
        Airflow DAG: The configured child DAG for processing attendance time entries
    """
    with rail.create_airflow_dag(
        dag_id=config.process_each_inout_child,
        description=f'T-Systems Time Import Child - Process Each IN/OUT {config.instance}',
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
            no_task='if_timesheet_template_present_in_mapper'
        )

        # Task: Execute entry processing pipeline in batch mode
        # Wraps individual entry processing for monitoring and error handling
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='if_timesheet_template_present_in_mapper',
            end_task='catch_and_log_errors',
        )

        if_timesheet_template_present_in_mapper = rail.IfOperator(
            task_id='if_timesheet_template_present_in_mapper',
            test='{{ dag_run.conf.user_ts_type | is_truthy }}',
            yes_task='check_timesheet_type_not_distribution_only',
            no_task='log_timesheet_type_not_supported'
        )

        log_timesheet_type_not_supported = rail.WriteLogOperator(
            task_id='log_timesheet_type_not_supported',
            log='{{ dag_run.conf.user_log }}',
            severity='Exception',
            message='Timesheet type not supported for time import',
            properties=lambda dag_run: {
                'row_number': dag_run.conf.get('row_number', ''),
                'employee_id': dag_run.conf.get('employee_id', ''),
                'user_name': dag_run.conf.get('user_name') or '',
                'entry_date': dag_run.conf.get('entry_date', ''),
                'project_id': dag_run.conf.get('project_id', ''),
                'task_name': dag_run.conf.get('task_name', ''),
                'activity': dag_run.conf.get('activity', ''),
                'status': 'Exception',
                'action': 'Add',
                'details': 'Timesheet type not supported for time import'
            },
        )

        # Task: Check if timesheet type is not 'Distribution Only'
        # Distribution Only timesheets should not process time entries
        check_timesheet_type_not_distribution_only = rail.IfOperator(
            task_id='check_timesheet_type_not_distribution_only',
            test=custom_methods.is_timesheet_type_not_distribution_only,
            yes_task='check_time_format_error',
            no_task='catch_and_log_errors'
        )

        # Task: Check if time format validation failed
        # Time format errors should block all processing
        check_time_format_error = rail.IfOperator(
            task_id='check_time_format_error',
            test=lambda dag_run: bool(custom_methods.check_time_format_error(dag_run)),
            yes_task='log_time_validation_error',
            no_task='check_worktype_error'
        )
        
        # Task: Check if worktype validation failed
        # Worktype errors should be logged but processing continues
        check_worktype_error = rail.IfOperator(
            task_id='check_worktype_error',
            test=lambda dag_run: bool(custom_methods.check_worktype_error(dag_run)),
            yes_task='log_worktype_validation_error',
            no_task='get_oef_and_tags'
        )
        
        # Task: Log time format validation errors (blocking)
        log_time_validation_error = rail.WriteLogOperator(
            task_id='log_time_validation_error',
            log='{{ dag_run.conf.user_log }}',
            severity='Exception',
            message=lambda dag_run: custom_methods.check_time_format_error(dag_run),
            properties=lambda dag_run: {
                'row_number': dag_run.conf.get('row_number', ''),
                'employee_id': dag_run.conf.get('employee_id', ''),
                'user_name': dag_run.conf.get('user_name') or '',
                'entry_date': dag_run.conf.get('entry_date', ''),
                'project_id': dag_run.conf.get('project_id', ''),
                'task_name': dag_run.conf.get('task_name', ''),
                'activity': dag_run.conf.get('activity', ''),
                'status': 'Exception',
                'action': 'Add',
                'details': custom_methods.check_time_format_error(dag_run)
            },
        )
        
        # Task: Log worktype validation errors (non-blocking)
        log_worktype_validation_error = rail.WriteLogOperator(
            task_id='log_worktype_validation_error',
            log='{{ dag_run.conf.user_log }}',
            severity='Exception',
            message=lambda dag_run: custom_methods.check_worktype_error(dag_run),
            properties=lambda dag_run: {
                'row_number': dag_run.conf.get('row_number', ''),
                'employee_id': dag_run.conf.get('employee_id', ''),
                'user_name': dag_run.conf.get('user_name') or '',
                'entry_date': dag_run.conf.get('entry_date', ''),
                'project_id': dag_run.conf.get('project_id', ''),
                'task_name': dag_run.conf.get('task_name', ''),
                'activity': dag_run.conf.get('activity', ''),
                'status': 'Exception',
                'action': 'Add',
                'details': custom_methods.check_worktype_error(dag_run)
            },
        )
        
        # Task: Determine appropriate OEF (Object Extension Field) configuration
        # Selects correct worktype OEF and tag URIs based on entry data
        get_oef_and_tags = rail.PythonOperator(
            task_id="get_oef_and_tags",
            python_callable=custom_methods.get_oef_and_tags_details,
        )

        # Task: Check if both in/out times are present before creating entry
        # Skip silently if both are missing (no attendance data to import)
        if_inout_times_present = rail.IfOperator(
            task_id='if_inout_times_present',
            test=lambda dag_run: bool(dag_run.conf.get('in_time') and dag_run.conf.get('out_time')),
            yes_task='add_inout_entry',
            no_task='catch_and_log_errors'
        )

        # Task: Create attendance time entry with in/out time pair
        # Submits time allocation using attendance type with timePair and OEF values
        add_inout_entry = rail.RepliconServiceOperator(
            task_id="add_inout_entry",
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=request_payload.put_inout_entry_payload
        )

        # Task: Log successful attendance time entry creation
        # Records successful in/out time processing for reporting and audit purposes
        log_success = rail.WriteLogOperator(
            task_id="log_success",
            log='{{ dag_run.conf.user_log }}',
            severity="Success",
            message="Time In/Out Added successfully",
            properties={
                'row_number': '{{ dag_run.conf.row_number }}',
                'employee_id': '{{ dag_run.conf.employee_id }}',
                'user_name': '{{ dag_run.conf.user_name or "" }}',
                'entry_date': '{{ dag_run.conf.entry_date }}',
                'project_id': '{{ dag_run.conf.project_id }}',
                'task_name': '{{ dag_run.conf.task_name }}',
                'activity': '{{ dag_run.conf.activity }}',
                'status': 'Success',
                'action': 'Add',
                'details': 'Time In/Out added successfully'
            }
        )

        # Task: Capture and log any attendance processing errors
        # Central error handler for in/out time entry troubleshooting and reporting
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{dag_run.conf.user_log}}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'row_number': '{{ dag_run.conf.row_number }}',
                'employee_id': '{{ dag_run.conf.employee_id }}',
                'user_name': '{{ dag_run.conf.user_name or "" }}',
                'entry_date': '{{ dag_run.conf.entry_date }}',
                'project_id': '{{ dag_run.conf.project_id }}',
                'task_name': '{{ dag_run.conf.task_name }}',
                'activity': '{{ dag_run.conf.activity }}',
                'status': 'Error',
                'action': 'Add',
                'details': '{{ get_error_message() }}'
            },
        )

        # DAG dependencies
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> if_timesheet_template_present_in_mapper

        if_timesheet_template_present_in_mapper >> rail.Label('Yes') >> check_timesheet_type_not_distribution_only
        if_timesheet_template_present_in_mapper >> rail.Label('No') >> log_timesheet_type_not_supported >> catch_and_log_errors

        check_timesheet_type_not_distribution_only >> rail.Label('Yes') >> check_time_format_error
        check_timesheet_type_not_distribution_only >> rail.Label('No') >> catch_and_log_errors
        
        # Time format error path (blocking)
        check_time_format_error >> rail.Label('Yes') >> log_time_validation_error >> catch_and_log_errors
        # No time format error, check worktype
        check_time_format_error >> rail.Label('No') >> check_worktype_error
        
        # Worktype error path (non-blocking)
        check_worktype_error >> rail.Label('Yes') >> log_worktype_validation_error >> get_oef_and_tags
        # No errors path
        check_worktype_error >> rail.Label('No') >> get_oef_and_tags

        # Check if in/out times present before creating entry
        get_oef_and_tags >> if_inout_times_present
        if_inout_times_present >> rail.Label('Yes') >> add_inout_entry >> log_success >> catch_and_log_errors
        if_inout_times_present >> rail.Label('No') >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
