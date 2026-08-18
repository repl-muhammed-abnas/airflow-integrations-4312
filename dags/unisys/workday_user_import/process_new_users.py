"""
Process New Users - Unisys Workday User Import Child DAG

Creates new user accounts in Replicon from Workday data.
This child DAG handles the complete workflow for adding new users including
user creation, supervisor assignment, and error handling.

Key features:
    - Creates new users via ImportService2.svc/CreateUserOrApplyModifications
    - Validates supervisor assignments
    - Handles supervisor-as-user edge cases
    - Processes supervisor relationships
    - Logs success/error/exception statuses
    - Supports batch task execution

Functions:
    create_child_dag(config): Creates the process new users child DAG
"""
from datetime import timedelta
from airflow.models import Variable
import rail
from unisys.workday_user_import.utils.custom_method import get_task_state
from unisys.workday_user_import.utils import request_payload, response_filters, custom_method
from unisys.workday_user_import.task.process_supervisor import process_supervisor_assignment_task_group

def create_child_dag(config):
    """
    Create child DAG for processing new user creation.

    This DAG creates new users in Replicon with all required attributes,
    handles supervisor assignment, and logs outcomes. Uses batch execution
    for improved error handling.

    Args:
        config: Configuration object containing DAG settings including:
            - process_new_users: DAG ID for this child DAG
            - company_key: Replicon company identifier
            - replicon_conn_id: Replicon connection ID
            - max_active_runs_process_new_users: Max parallel DAG runs
            - can_run_batch_task: Variable name controlling batch execution
            - execution_timeout_days: Task execution timeout

    Returns:
        DAG: Configured Airflow DAG object for new user processing

    DAG Configuration:
        dag_run.conf should contain:
            - employee_id: Employee ID from Workday
            - first_name: User's first name
            - last_name: User's last name
            - email: User's email address
            - login_name: Login name for Replicon
            - supervisor_id: Supervisor's employee ID
            - user_log: Log artifact for tracking operations
            - supervisor_log: Log artifact for supervisor operations
            - All other user attributes required for creation
    """
    # pylint: disable=too-many-statements, line-too-long, cell-var-from-loop
    append_dags = []
    for idx in range(0, config.PROCESS_USER_BATCH_COUNT):
        with rail.create_airflow_dag(
            dag_id=f'{config.process_new_users}_batch_{idx+1}',
            description='Unisys Workday User Import - Process New Users',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_new_users,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='add_new_user'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='add_new_user',
                end_task='catch_and_log_errors',
            )

            add_new_user = rail.RepliconServiceOperator(
                task_id="add_new_user",
                endpoint="/services/importservice2.svc/CreateUserOrApplyModifications",
                data=lambda dag_run: request_payload.get_create_update_user_payload(config, dag_run, "add_user")
            )
            
            update_license_for_user = rail.RepliconServiceOperator(
                task_id='update_license_for_user',
                endpoint='/services/ImportService1.svc/ApplyUserModifications2',
                data=lambda: {
                    "user": {
                        "uri": rail.result('add_new_user')['user']['uri'],
                    },
                    "modifications": {
                        "productAssignmentsToApply": {
                            "productUrisToUnassign": [
                                "urn:replicon-saas:product:time-bill-plus",
                                "urn:replicon-saas:product:time-intelligence",
                            ]
                        }
                    },
                    "userModificationOptionUri": "urn:replicon:user-modification-option:save"
                }
            )

            get_project_details = rail.PythonOperator(
                task_id='get_project_details',
                python_callable=lambda dag_run: custom_method.get_admin_project_codes(dag_run.conf['companycode_costcenter'], dag_run.conf['user_type'])
            )

            if_project_available = rail.IfOperator(
                task_id='if_project_available',
                test=lambda dag_run: bool(rail.result('get_project_details')),
                yes_task='log_project_resource',
                no_task='if_supervisor_id_present'
            )

            log_project_resource = rail.WriteLogOperator(
                task_id='log_project_resource',
                log='{{ dag_run.conf.project_user_log }}',
                items='{{ result("get_project_details") | to_json }}',
                message="NA",
                severity="Skipped",
                properties=lambda item: {
                    "projectcode": item["code"],
                    "useruri": rail.result('add_new_user')['user']['uri']
                }
            )

            if_supervisor_id_present = rail.IfOperator(
                task_id='if_supervisor_id_present',
                test=lambda dag_run: bool(dag_run.conf['supervisor_id']),
                yes_task='if_user_is_supervisor',
                no_task='log_user_completion'
            )

            if_user_is_supervisor = rail.IfOperator(
                task_id='if_user_is_supervisor',
                test=lambda dag_run: dag_run.conf['supervisor_id'] == dag_run.conf['employee_id'],
                yes_task='log_user_supervisor_same',
                no_task='search_supervisor_in_replicon'
            )

            log_user_supervisor_same = rail.EmptyOperator(
                task_id='log_user_supervisor_same',
            )

            process_supervisor_entry,  process_supervisor_exit= process_supervisor_assignment_task_group(
                'add_new_user', 'new_user', config)

            log_user_completion = rail.WriteLogOperator(
                task_id='log_user_completion',
                log = '{{ dag_run.conf.user_log }}',
                message=custom_method.get_add_user_message,
                severity=custom_method.get_add_user_severity,
                properties=lambda dag_run: {
                    'lastname': dag_run.conf['last_name'],
                    'firstname': dag_run.conf['first_name'],
                    'loginname':  dag_run.conf['login_name'],
                    'employeeid': dag_run.conf['employee_id'],
                    'manager': dag_run.conf['supervisor_id'],
                    "userstatus": dag_run.conf['user_status'],
                    "co_costcenter": dag_run.conf['cost_center_description'],
                    "location": dag_run.conf['location_description'],
                    'action': 'Add',
                    'status': custom_method.get_add_user_severity(),
                    'details': custom_method.get_add_user_message(),
                }
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log = '{{ dag_run.conf.user_log }}',
                trigger_rule='one_failed',
                severity='Error',
                message="\
                    {%- if get_task_state('add_new_user') == 'success' -%} \
                        User Added Partially; {{ get_error_message() }}\
                    {%- else -%}\
                        User not created; {{ get_error_message() }}\
                    {%- endif -%}",
                properties={
                    'lastname': '{{dag_run.conf.last_name}}',
                    'firstname': '{{dag_run.conf.first_name}}',
                    'loginname': '{{dag_run.conf.login_name}}',
                    'employeeid': '{{dag_run.conf.employee_id}}',
                    'manager': "{{dag_run.conf.supervisor_id}}",
                    "userstatus": "{{ dag_run.conf.user_status }}",
                    "co_costcenter": "{{ dag_run.conf.cost_center_description }}",
                    "location": "{{ dag_run.conf.location_description }}",
                    'action': 'Add',
                    'status': "{{'Exception' if get_task_state('add_new_user') == 'success' else 'Error' }}",
                    'details': "\
                    {%- if get_task_state('add_new_user') == 'success' -%} \
                        User Added Partially; {{ get_error_message() }}\
                    {%- else -%}\
                        User not created; {{ get_error_message() }}\
                    {%- endif -%}"
                }
            )

            can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> add_new_user

            add_new_user >> update_license_for_user >> get_project_details >> if_project_available >> rail.Label(
                "Yes") >> log_project_resource >> if_supervisor_id_present
            if_project_available >> rail.Label(
                "No") >> if_supervisor_id_present >> rail.Label("Yes") >> if_user_is_supervisor
            if_supervisor_id_present >> rail.Label("No") >> log_user_completion

            if_user_is_supervisor >> rail.Label("Yes") >> log_user_supervisor_same >> log_user_completion
            if_user_is_supervisor >> rail.Label("No") >> process_supervisor_entry
            process_supervisor_exit >> log_user_completion
            log_user_completion >> catch_and_log_errors

        append_dags.append(dag)
    return append_dags

rail.for_each_instance(create_child_dag)
