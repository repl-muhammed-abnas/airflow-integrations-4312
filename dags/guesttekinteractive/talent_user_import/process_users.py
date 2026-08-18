"""
Process Users - GuestTek Talent User Import Child DAG

Orchestrates individual user processing by routing to new user or update user workflows.
This child DAG is triggered for each user record and determines whether to create a new user
or update an existing one based on Replicon lookup.
"""
from datetime import timedelta
from airflow.models import Variable
import rail
from guesttekinteractive.talent_user_import.utils import request_payload, response_filters, custom_method

null = None


def create_child_dag(config):
    """Create child DAG for orchestrating individual user processing."""
    with rail.create_airflow_dag(
        dag_id=config.process_each_user,
        description='GuestTek Talent User Import - Process Users',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_users,
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        
        create_process_user_log = rail.CreateLogOperator(task_id='create_process_user_log')
        
        # Search for user in Replicon by employee ID
        get_user_by_empl_id = rail.RepliconServiceOperator(
            task_id="get_user_by_empl_id",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_user_data_payload,
            data_handler=response_filters.get_filtered_user_data
        )
        
        # Check if user exists in Replicon
        user_exists_in_replicon = rail.IfOperator(
            task_id='user_exists_in_replicon',
            test="{{ result('get_user_by_empl_id') | is_truthy }}",
            yes_task='check_if_user_disabled',
            no_task='check_if_deactivated_skip'
        )

        # If user not found in Replicon, check if deactivated - skip instead of creating
        check_if_deactivated_skip = rail.IfOperator(
            task_id='check_if_deactivated_skip',
            test=lambda dag_run: dag_run.conf.get('user_deactivated', 0) == 1,
            yes_task='log_deactivated_not_in_replicon',
            no_task='trigger_add_new_user'
        )

        log_deactivated_not_in_replicon = rail.WriteLogOperator(
            task_id='log_deactivated_not_in_replicon',
            log='{{ result("create_process_user_log") }}',
            severity='Skipped',
            message='Deactivated user not found in Replicon - skipping',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf.get('employee_id', ''),
                "action": "Disable User",
                "status": "Skipped",
                "details": "Deactivated user not found in Replicon - skipping"
            }
        )

        # Check if user is disabled in Talent (deactivated)
        check_if_user_disabled = rail.IfOperator(
            task_id='check_if_user_disabled',
            test=lambda dag_run: dag_run.conf.get('user_deactivated', 0) == 1,
            yes_task='trigger_disable_user',
            no_task='trigger_update_user'
        )
        
        # Trigger add new user child DAG
        trigger_add_new_user = rail.TriggerDagRunOperator(
            task_id='trigger_add_new_user',
            trigger_dag_id=config.process_new_users,
            conf=lambda dag_run: {
                **dag_run.conf,
                'user_log': rail.result('create_process_user_log'),
            },
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Trigger update user child DAG
        trigger_update_user = rail.TriggerDagRunOperator(
            task_id='trigger_update_user',
            trigger_dag_id=config.process_update_users,
            conf=lambda dag_run: {
                **dag_run.conf,
                'useruri': rail.result('get_user_by_empl_id')[0]['userDetails']['uri'],
                'user_log': rail.result('create_process_user_log'),
            },
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )
        
        # Disable user in Replicon
        trigger_disable_user = rail.RepliconServiceOperator(
            task_id='trigger_disable_user',
            endpoint='/services/SecurityService1.svc/DisableLogin',
            data=lambda: {'userUri': rail.result('get_user_by_empl_id')[0]['userDetails']['uri']}
        )
        
        log_user_disabled = rail.WriteLogOperator(
            task_id='log_user_disabled',
            log='{{ result("create_process_user_log") }}',
            severity='Success',
            message='User disabled',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf.get('employee_id', ''),
                "action": "Disable User",
                "status": "Success",
                "details": "User disabled"
            }
        )
        
        finish = rail.EmptyOperator(task_id='finish')
        
        # Define flow
        create_process_user_log >> get_user_by_empl_id
        get_user_by_empl_id >> user_exists_in_replicon
        user_exists_in_replicon >> [check_if_user_disabled, check_if_deactivated_skip]
        check_if_deactivated_skip >> [log_deactivated_not_in_replicon, trigger_add_new_user]
        check_if_user_disabled >> [trigger_disable_user, trigger_update_user]
        trigger_disable_user >> log_user_disabled >> finish
        [trigger_add_new_user, trigger_update_user, log_deactivated_not_in_replicon] >> finish
    
    return dag


rail.for_each_instance(create_child_dag)
