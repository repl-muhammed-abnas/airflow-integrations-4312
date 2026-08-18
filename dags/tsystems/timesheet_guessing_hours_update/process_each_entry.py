from datetime import timedelta
from uuid import uuid4
import rail
from tsystems.timesheet_guessing_hours_update.utils import response_filters, request_payload
from airflow.models import Variable

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_entry,
        description=f'T-Systems Guessing Hours Update - Process Single Entry {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true'
            ).lower() == 'true',
            yes_task='batch_task',
            no_task='get_time_entry_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=getattr(config, 'execution_timeout_days', 1)),
            start_task='get_time_entry_details',
            end_task='catch_and_log_errors',
        )

        get_time_entry_details = rail.RepliconServiceOperator(
            task_id="get_time_entry_details",
            endpoint="/services/TimeEntryRevisionGroupService1.svc/GetTimeEntryRevisionGroupsDetails",
            data=request_payload.get_time_entry_details_payload,
            data_handler=lambda response, dag_run: response_filters.filter_time_entries(response, dag_run)
        )

        if_time_entry_exists = rail.IfOperator(
            task_id='if_time_entry_exists',
            test=lambda: rail.result('get_time_entry_details'),
            yes_task='put_time_entry_revision_group',
            no_task='catch_and_log_errors'
        )
        
        put_time_entry_revision_group = rail.RepliconServiceOperator(
            task_id='put_time_entry_revision_group',
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=lambda dag_run: request_payload.put_time_entry_payload(dag_run, config.ENTRY_DATE_FORMAT),  
        )

        log_success = rail.WriteLogOperator(
            task_id="log_update_time_entry_success",
            log='{{ dag_run.conf.user_log }}',
            severity="Success",
            message="Guessing hours updated to 0",
            properties={
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'user_name': '{{ dag_run.conf.input_data.user_name }}',
                'entry_date': '{{ dag_run.conf.input_data.entry_date }}',
                'original_hours': '{{ dag_run.conf.input_data.hours }}',
                'task_name': '{{ dag_run.conf.input_data.task_name }}',
                'project_name': '{{ dag_run.conf.input_data.project_name }}',
                'org_structure_code': '{{ dag_run.conf.input_data.org_structure_code }}',
                'status': 'Success',
                'action': 'Update',
                'details': 'Guessing hours successfully updated to 0'
            }
        )
        

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'user_name': '{{ dag_run.conf.input_data.user_name }}',
                'entry_date': '{{ dag_run.conf.input_data.entry_date }}',
                'original_hours': '{{ dag_run.conf.input_data.hours }}',
                'task_name': '{{ dag_run.conf.input_data.task_name }}',
                'status': 'Error',
                'action': 'Update',
                'details': '{{ get_error_message() }}'
            },
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_time_entry_details

        get_time_entry_details >> if_time_entry_exists
        if_time_entry_exists >> rail.Label('Yes') >> put_time_entry_revision_group >> log_success >> catch_and_log_errors
        if_time_entry_exists >> rail.Label('No') >> catch_and_log_errors

    
    return dag

rail.for_each_instance(create_child_dag)