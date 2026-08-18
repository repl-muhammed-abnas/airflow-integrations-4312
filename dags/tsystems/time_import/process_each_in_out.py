"""T-Systems Time Import Child DAG for processing individual employee records."""

from datetime import datetime, timedelta
import rail
from airflow.models import Variable
from tsystems.time_import.utils import custom_methods, request_payload, response_filters

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
            no_task='get_oef_and_tags'
        )

        # Task: Execute entry processing pipeline in batch mode
        # Wraps individual entry processing for monitoring and error handling
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_oef_and_tags',
            end_task='catch_and_log_errors',
        )

        # Task: Determine appropriate OEF (Object Extension Field) configuration
        # Selects correct worktype OEF and tag URIs based on entry data
        get_oef_and_tags = rail.PythonOperator(
            task_id="get_oef_and_tags",
            python_callable=custom_methods.get_oef_and_tags_details,
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
            properties=lambda item: {
                'employee_id': '{{ dag_run.conf.employee_id }}',
                'entry_date': '{{ dag_run.conf.entry_date }}',
                'project_id': '',
                'task_name': '',
                'activity': '',
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
                'employee_id': '{{ dag_run.conf.employee_id }}',
                'entry_date': '{{ dag_run.conf.entry_date }}',
                'project_id': '',
                'task_name': '',
                'activity': '',
                'status': 'Error',
                'action': 'Add',
                'details': '{{ get_error_message() }}'
            },
        )

        # DAG dependencies
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_oef_and_tags

        get_oef_and_tags >> add_inout_entry >> log_success >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
