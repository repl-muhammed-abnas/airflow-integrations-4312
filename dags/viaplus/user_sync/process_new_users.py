"""
ViaPlus User Sync - Process New Users Child DAG

This child DAG handles creation of new users in Replicon.
It performs the following:
1. Create user profile with SSO authentication
2. Assign licenses (Polaris PSA, Time-Off Ent)
3. Assign permission set (Project Resource with Reports)
4. Assign timesheet template and approval path
5. Assign time-off template
6. Set schedule (timezone, holiday calendar, work week)
7. Assign groups (location, department, legal entity)
8. Process supervisor assignment (queued if supervisor not processed yet)

Matches CRL user_import_ireland_v1 pattern.
"""
from datetime import timedelta
from airflow.models import Variable
import rail

from viaplus.user_sync.utils import request_payload
from viaplus.user_sync.tasks.process_supervisor import process_supervisor_assignment_task_group

null = None
# Template variable helpers for Jinja2
OPEN_BRACKETS = "{{"
CLOSE_BRACKETS = "}}"


# pylint: disable=too-many-statements
def create_child_dag(config):
    """Create the process_new_users child DAGs (one per batch)."""
    add_dags = []

    for idx in range(0, config.BATCH_COUNT):
        with rail.create_airflow_dag(
            dag_id=f"{config.process_new_users_dagid}_batch_{idx + 1}",
            description='ViaPlus User Sync - Process New Users',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_new_users,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            # ================================================================
            # Batch Task Control
            # ================================================================
            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name,
                    default_var='true'
                ).lower() == 'true',
                yes_task='batch_task',
                no_task='is_enddate_available'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(days=config.execution_timeout_days),
                start_task='is_enddate_available',
                end_task='catch_and_log_errors',
            )

            # ================================================================
            # Validation - Check if end date available (new user shouldn't have end date)
            # ================================================================
            is_enddate_available = rail.IfOperator(
                task_id='is_enddate_available',
                test=lambda dag_run: bool(dag_run.conf.get('end_date')),
                yes_task="log_enddate_exception",
                no_task="add_new_user"
            )

            log_enddate_exception = rail.WriteLogOperator(
                task_id='log_enddate_exception',
                log='{{ dag_run.conf.user_log }}',
                message="User not Created, as End Date present while User Creation",
                severity='Exception',
                properties={
                    'employee_id': '{{ dag_run.conf.emp_id }}',
                    'first_name': '{{ dag_run.conf.first_name }}',
                    'last_name': '{{ dag_run.conf.last_name }}',
                    "action": "Validation",
                    "status": "Exception",
                    'details': "User not Created, as End Date present while User Creation"
                }
            )

            # ================================================================
            # Create New User via PutUser3
            # ================================================================
            add_new_user = rail.RepliconServiceOperator(
                task_id="add_new_user",
                endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
                data=lambda dag_run: request_payload.get_put_user_payload(dag_run, config)
            )

            # ================================================================
            # Check Supervisor Assignment
            # ================================================================
            is_supervisor_in_feed = rail.IfOperator(
                task_id='is_supervisor_in_feed',
                test=lambda dag_run: bool(dag_run.conf.get('sup_emp_id')),
                yes_task='search_supervisor_in_replicon',
                no_task='log_user_completion'
            )

            # ================================================================
            # Process Supervisor Assignment TaskGroup
            # ================================================================
            process_supervisor_entry, process_supervisor_exit = process_supervisor_assignment_task_group(
                'add_new_user', 'new_user')

            # ================================================================
            # Log User Completion
            # ================================================================
            log_user_completion = rail.WriteLogOperator(
                task_id='log_user_completion',
                log='{{ dag_run.conf.user_log }}',
                message=request_payload.get_add_user_message,
                severity=request_payload.get_add_user_severity,
                properties=lambda dag_run: {
                    "employee_id": dag_run.conf.get('emp_id', ''),
                    "last_name": dag_run.conf.get('last_name', ''),
                    "first_name": dag_run.conf.get('first_name', ''),
                    "action": "Add",
                    "status": request_payload.get_add_user_severity(dag_run),
                    'details': request_payload.get_add_user_message(dag_run)
                }
            )

            # ================================================================
            # Error Handling
            # ================================================================
            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log='{{ dag_run.conf.user_log }}',
                trigger_rule='one_failed',
                severity='Error',
                message="\
                    {%- if get_task_state('add_new_user') == 'success' -%} \
                        User Added Partially; {{ get_error_message() }}\
                    {%- else -%}\
                        User not created; {{ get_error_message() }}\
                    {%- endif -%}",
                properties={
                    'employee_id': '{{ dag_run.conf.emp_id }}',
                    "last_name": "{{ dag_run.conf.last_name }}",
                    "first_name": "{{ dag_run.conf.first_name }}",
                    "action": "Add",
                    'status': 'Error',
                    'details': "\
                    {%- if get_task_state('add_new_user') == 'success' -%} \
                        User Added Partially; {{ get_error_message() }}\
                    {%- else -%}\
                        User not created; {{ get_error_message() }}\
                    {%- endif -%}"
                }
            )

            # ================================================================
            # Task Dependencies (matching CRL pattern)
            # ================================================================
            can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> is_enddate_available

            is_enddate_available >> rail.Label('Yes') >> log_enddate_exception >> catch_and_log_errors
            is_enddate_available >> rail.Label('No') >> add_new_user

            add_new_user >> is_supervisor_in_feed

            is_supervisor_in_feed >> rail.Label('No') >> log_user_completion
            is_supervisor_in_feed >> rail.Label('Yes') >> process_supervisor_entry

            process_supervisor_exit >> log_user_completion >> catch_and_log_errors

        add_dags.append(dag)

    return add_dags


rail.for_each_instance(create_child_dag)
