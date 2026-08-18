"""
ViaPlus User Sync - Process Users Child DAG

This child DAG routes each employee to the appropriate processing DAG:
- New users → process_new_users
- Existing users → process_update_users (or process_disable_users if exit date)

It also handles the re-hire scenario where same email exists but different employee number.

Matches CRL user_import_ireland_v1 pattern.
"""
from datetime import timedelta
from airflow.models import Variable
import rail

from viaplus.user_sync.utils import request_payload

null = None
DATE_FORMAT = "%d/%m/%Y"
# Template variable helpers for Jinja2
OPEN_BRACKETS = "{{"
CLOSE_BRACKETS = "}}"


def create_child_dag(config):
    """Create the process_users child DAG."""

    with rail.create_airflow_dag(
        dag_id=config.process_users_dagid,
        description='ViaPlus User Sync - Process Users Router',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_users,
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
            no_task='create_user_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='create_user_log',
            end_task='catch_and_log_errors',
        )

        # ================================================================
        # Create User Log
        # ================================================================
        create_user_log = rail.CreateLogOperator(
            task_id="create_user_log"
        )

        # ================================================================
        # Get User Data by Employee ID
        # ================================================================
        get_user_data = rail.RepliconServiceOperator(
            task_id="get_user_data",
            endpoint="/services/UserService1.svc/BulkGetUsers2",
            data={
                "users": [{
                    "uri": null,
                    "loginName": null,
                    "employeeId": "{{dag_run.conf.emp_id}}",
                    "parameterCorrelationId": null
                }]
            },
            data_handler=lambda response: [] if response == [None] else response
        )

        # ================================================================
        # Validate Required Fields
        # ================================================================
        has_valid_data = rail.IfOperator(
            task_id='has_valid_data',
            test=request_payload.test_valid_fields,
            yes_task="is_supervisor_id_available",
            no_task="log_invalid_data"
        )

        log_invalid_data = rail.WriteLogOperator(
            task_id='log_invalid_data',
            log='{{ result("create_user_log") }}',
            message=request_payload.get_invalid_fields_message,
            severity='Exception',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf.get('emp_id', ''),
                "first_name": dag_run.conf.get('first_name', ''),
                "last_name": dag_run.conf.get('last_name', ''),
                "action": "Validation",
                "status": "Exception",
                'details': request_payload.get_invalid_fields_message(dag_run),
            }
        )

        is_supervisor_id_available = rail.IfOperator(
            task_id='is_supervisor_id_available',
            test=lambda dag_run: bool(dag_run.conf['sup_emp_id']),
            yes_task='get_supervisor_details_from_keka',
            no_task='is_user_available'
        )

        get_supervisor_details_from_keka = rail.SimpleHttpOperator(
            task_id="get_supervisor_details_from_keka",
            method='GET',
            http_conn_id=config.keka_api_conn_id,
            endpoint='api/v1/hris/employees/{{dag_run.conf.sup_emp_id}}',  # Include /api/v1 prefix
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "Mozilla",
                "Authorization": f"Bearer {OPEN_BRACKETS} dag_run.conf.token {CLOSE_BRACKETS}"
            },
            execution_timeout=timedelta(minutes=30),
            log_response=True
        )

        # ================================================================
        # Check if User Already Exists
        # ================================================================
        is_user_available = rail.IfOperator(
            task_id='is_user_available',
            test=lambda: bool(rail.result('get_user_data')),
            yes_task='process_update_user',
            no_task='get_user_data_based_on_login_name'
        )

        # ================================================================
        # Check by Login Name (for re-hire scenario)
        # ================================================================
        get_user_data_based_on_login_name = rail.RepliconServiceOperator(
            task_id="get_user_data_based_on_login_name",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [{
                    "uri": null,
                    "loginName": "{{dag_run.conf.login_name}}",
                    "employeeId": null,
                    "parameterCorrelationId": null
                }],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: [] if response == [None] else response
        )

        is_same_login_name_already_available = rail.IfOperator(
            task_id='is_same_login_name_already_available',
            test=lambda: bool(rail.result('get_user_data_based_on_login_name')),
            yes_task='is_end_date_present_in_old_profile',
            no_task='process_new_user'
        )

        # ================================================================
        # Re-hire Handling
        # ================================================================
        is_end_date_present_in_old_profile = rail.IfOperator(
            task_id='is_end_date_present_in_old_profile',
            test=request_payload.validate_enddate_for_old_profile,
            yes_task='update_old_profile_login_name',
            no_task='log_end_date_not_present_in_old_profile'
        )

        log_end_date_not_present_in_old_profile = rail.WriteLogOperator(
            task_id='log_end_date_not_present_in_old_profile',
            log='{{ result("create_user_log") }}',
            message=lambda dag_run: f"User with employee id {dag_run.conf.get('emp_id')} not created because another profile with same login name was not disabled",
            severity='Exception',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf.get('emp_id', ''),
                "first_name": dag_run.conf.get('first_name', ''),
                "last_name": dag_run.conf.get('last_name', ''),
                "action": "Validation",
                "status": "Exception",
                'details': f"User with employee id {dag_run.conf.get('emp_id')} not created because another profile with same login name was not disabled/end date updated",
            }
        )

        update_old_profile_login_name = rail.RepliconServiceOperator(
            task_id='update_old_profile_login_name',
            endpoint='/services/SecurityService1.svc/SetSSOAuthenticationForUser',
            data=request_payload.update_old_profile_login_name
        )

        # ================================================================
        # Trigger Child DAGs (Batched)
        # ================================================================
        def get_add_update_trigger_id(dag_run, action):
            """Get trigger dag_id based on action and modulo."""
            if action == "add":
                return f"{config.process_new_users_dagid}_batch_{dag_run.conf.get('modulo', 0) + 1}"
            return f"{config.process_update_users_dagid}_batch_{dag_run.conf.get('modulo', 0) + 1}"

        process_new_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_user',
            items=[0],
            trigger_dag_id=lambda dag_run: get_add_update_trigger_id(dag_run, "add"),
            conf=request_payload.get_process_new_users_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_new_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_new_user',
            dag_runs='{{ result("process_new_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_update_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_update_user',
            items=[0],
            trigger_dag_id=lambda dag_run: get_add_update_trigger_id(dag_run, "update"),
            conf=request_payload.get_process_update_users_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_user',
            dag_runs='{{ result("process_update_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # ================================================================
        # Error Handling
        # ================================================================
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{result("create_user_log")}}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "employee_id": "{{dag_run.conf.emp_id}}",
                "last_name": "{{dag_run.conf.last_name}}",
                "first_name": "{{dag_run.conf.first_name}}",
                "action": "Sync",
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            },
        )

        # ================================================================
        # Task Dependencies (matching CRL pattern)
        # ================================================================
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_user_log

        create_user_log >> get_user_data >> has_valid_data
        has_valid_data >> rail.Label('No') >> log_invalid_data >> catch_and_log_errors
        has_valid_data >> rail.Label('Yes') >> is_supervisor_id_available

        is_supervisor_id_available >> rail.Label('Yes') >> get_supervisor_details_from_keka >> is_user_available
        is_supervisor_id_available >> rail.Label('No') >> is_user_available

        is_user_available >> rail.Label('No') >> get_user_data_based_on_login_name >> is_same_login_name_already_available

        is_same_login_name_already_available >> rail.Label('Yes') >> is_end_date_present_in_old_profile
        is_end_date_present_in_old_profile >> rail.Label('Yes') >> update_old_profile_login_name
        is_end_date_present_in_old_profile >> rail.Label('No') >> log_end_date_not_present_in_old_profile >> catch_and_log_errors
        update_old_profile_login_name >> process_new_user
        is_same_login_name_already_available >> rail.Label('No') >> process_new_user

        process_new_user >> wait_for_process_new_user >> catch_and_log_errors
        is_user_available >> rail.Label('Yes') >> process_update_user >> wait_for_process_update_user >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
