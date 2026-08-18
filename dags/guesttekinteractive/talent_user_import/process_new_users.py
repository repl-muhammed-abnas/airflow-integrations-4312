"""
Process New Users - GuestTek Talent User Import Child DAG

All assignments (license, timesheet template, time off types, holiday calendar,
schedule, payrule, groups, timezone) are included in the single
CreateUserOrApplyModifications call via the modifications payload.
"""
from datetime import timedelta
import rail
from guesttekinteractive.talent_user_import.utils import request_payload, response_filters, custom_method
from guesttekinteractive.talent_user_import.task.process_supervisor import (
    process_supervisor_assignment_task_group,
    SUPERVISOR_OUTCOME_SUCCESS, SUPERVISOR_OUTCOME_NOT_FOUND, SUPERVISOR_OUTCOME_DISABLED
)
from guesttekinteractive.talent_user_import.task.get_talent_users import fetch_additional_user_info

null = None

_SUPERVISOR_EXCEPTION_DETAILS = {
    SUPERVISOR_OUTCOME_NOT_FOUND: "User created successfully. Supervisor not found in Replicon",
    SUPERVISOR_OUTCOME_DISABLED: "User created successfully. Supervisor is disabled in Replicon",
}


def _get_supervisor_outcome():
    """Safely retrieve supervisor outcome, returning None if the flow was not entered."""
    try:
        return rail.result('supervisor_assignment_end')
    except Exception:
        return None


def _get_log_properties(dag_run):
    """Build log properties with supervisor outcome included."""
    base = {
        "employee_id": dag_run.conf.get('employee_id', ''),
        "login_name": dag_run.conf.get('login_name', ''),
        "first_name": dag_run.conf.get('first_name', ''),
        "last_name": dag_run.conf.get('last_name', ''),
        "action": "Add User",
    }

    supervisor_outcome = _get_supervisor_outcome()
    if supervisor_outcome and supervisor_outcome in _SUPERVISOR_EXCEPTION_DETAILS:
        base["status"] = "Exception"
        base["details"] = _SUPERVISOR_EXCEPTION_DETAILS[supervisor_outcome]
    else:
        base["status"] = "Success"
        base["details"] = "User created successfully"

    return base


def _get_log_severity():
    """Determine log severity based on supervisor outcome."""
    supervisor_outcome = _get_supervisor_outcome()
    if supervisor_outcome and supervisor_outcome in _SUPERVISOR_EXCEPTION_DETAILS:
        return 'Exception'
    return 'Success'


def create_child_dag(config):
    """Create child DAG for processing new user creation."""
    with rail.create_airflow_dag(
        dag_id=config.process_new_users,
        description='GuestTek Talent User Import - Process New Users',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_new_users,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        fetch_additional_info = rail.PythonOperator(
            task_id='fetch_additional_info',
            python_callable=lambda dag_run: fetch_additional_user_info(config, dag_run.conf['talent_user_id'])
        )

        # Single API call that creates user AND assigns all attributes
        add_new_user = rail.RepliconServiceOperator(
            task_id="add_new_user",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.get_create_update_user_payload(config, dag_run, "add_user")
        )

        if_supervisor_id_present = rail.IfOperator(
            task_id='if_supervisor_id_present',
            test=lambda dag_run: bool(dag_run.conf.get('supervisor_employee_id')),
            yes_task='if_user_is_supervisor',
            no_task='log_user_completion'
        )

        if_user_is_supervisor = rail.IfOperator(
            task_id='if_user_is_supervisor',
            test=lambda dag_run: dag_run.conf.get('supervisor_employee_id') == dag_run.conf.get('employee_id'),
            yes_task='log_user_supervisor_same',
            no_task='search_supervisor_in_replicon'
        )

        log_user_supervisor_same = rail.EmptyOperator(task_id='log_user_supervisor_same')

        process_supervisor_entry, process_supervisor_exit = process_supervisor_assignment_task_group(
            'add_new_user', 'new_user', config
        )

        log_user_completion = rail.WriteLogOperator(
            task_id='log_user_completion',
            log='{{ dag_run.conf.user_log }}',
            severity=_get_log_severity,
            message='User processing completed',
            properties=_get_log_properties
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "employee_id": "{{ dag_run.conf.employee_id }}",
                "action": "Add User",
                "status": "Error",
                "details": "{{ get_error_message() }}"
            }
        )

        # Flow: create user (with all assignments) >> supervisor >> log
        fetch_additional_info >> add_new_user >> if_supervisor_id_present
        if_supervisor_id_present >> [if_user_is_supervisor, log_user_completion]
        if_user_is_supervisor >> [log_user_supervisor_same, process_supervisor_entry]
        process_supervisor_exit >> log_user_completion
        log_user_supervisor_same >> log_user_completion
        log_user_completion >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
