"""
Process Update Users - Unisys Workday User Import Child DAG

Updates existing user accounts in Replicon with changed Workday data.
This child DAG handles the complete workflow for updating existing users including
employment date changes, user enablement/disablement, profile updates, and supervisor changes.

Key features:
    - Updates user employment date ranges
    - Disables users with past end dates
    - Enables disabled users when appropriate
    - Updates user custom field values
    - Syncs user group memberships
    - Updates project role assignments
    - Modifies user profiles via ImportService2.svc/CreateUserOrApplyModifications
    - Processes supervisor relationship changes
    - Validates end date constraints
    - Logs success/error/exception statuses
    - Supports batch task execution

Functions:
    create_child_dag(config): Creates the process update users child DAG
"""
from datetime import timedelta
import json
from airflow.models import Variable
import rail

from unisys.workday_user_import_v1.utils.custom_method import get_task_state
from unisys.workday_user_import_v1.utils import request_payload, response_filters, custom_method
from unisys.workday_user_import_v1.task.process_supervisor import process_supervisor_assignment_task_group

null= None

def create_child_dag(config):
    """
    Create child DAG for processing user updates.

    This DAG updates existing users in Replicon with changed attributes from Workday,
    handles employment date modifications, user status changes, and supervisor updates.
    Includes validation to prevent invalid date ranges.

    Args:
        config: Configuration object containing DAG settings including:
            - process_update_users: DAG ID for this child DAG
            - company_key: Replicon company identifier
            - replicon_conn_id: Replicon connection ID
            - max_active_runs_process_update_users: Max parallel DAG runs
            - can_run_batch_task: Variable name controlling batch execution
            - execution_timeout_days: Task execution timeout

    Returns:
        DAG: Configured Airflow DAG object for user updates

    DAG Configuration:
        dag_run.conf should contain:
            - useruri: Replicon user URI
            - employee_id: Employee ID from Workday
            - first_name: User's first name
            - last_name: User's last name
            - email: User's email address
            - start_date: Employment start date (MM/DD/YYYY)
            - end_date: Employment end date (MM/DD/YYYY) or None
            - supervisor_id: Supervisor's employee ID
            - user_data: Artifact containing current user data
            - user_log: Log artifact for tracking operations
            - supervisor_log: Log artifact for supervisor operations
            - All other user attributes for update
    """
    # pylint: disable=too-many-statements, line-too-long, cell-var-from-loop
    append_dags = []
    for idx in range(0, config.PROCESS_USER_BATCH_COUNT):
        with rail.create_airflow_dag(
            dag_id=f'{config.process_update_users}_batch_{idx+1}',
            description='Unisys Workday User Import - Process Update Users',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_update_users,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='get_user_data'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='get_user_data',
                end_task='catch_and_log_errors',
            )

            get_user_data = rail.PythonOperator(
                task_id='get_user_data',
                python_callable=lambda dag_run: rail.load_all_records(dag_run.conf['user_data'])
            )

            if_end_date_present = rail.IfOperator(
                task_id="if_end_date_present",
                test=lambda dag_run: dag_run.conf['end_date'],
                yes_task="is_enddate_greater_than_start_date",
                no_task="empty_is_user_disabled"
            )

            is_enddate_greater_than_start_date = rail.IfOperator(
                task_id ='is_enddate_greater_than_start_date',
                test = custom_method.validate_enddate,
                yes_task="update_employee_endate",
                no_task="log_endate_exception"
            )

            update_employee_endate = rail.RepliconServiceOperator(
                task_id='update_employee_endate',
                endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
                data=lambda dag_run: {
                    "userUri": dag_run.conf['useruri'],
                    "dateRange": {
                        "startDate": request_payload.get_replicon_date(dag_run.conf['start_date']),
                        "endDate": request_payload.get_replicon_date(dag_run.conf['end_date'])
                    }
                }
            )

            if_can_be_disabled = rail.IfOperator(
                task_id ='if_can_be_disabled',
                test=custom_method.if_end_date_in_past,
                yes_task="disable_login",
                no_task="empty_is_user_disabled"
            )

            disable_login = rail.RepliconServiceOperator(
                task_id='disable_login',
                endpoint='/services/securityservice1.svc/DisableLogin',
                data={
                    "userUri": '{{ dag_run.conf.useruri }}'
                }
            )

            log_endate_exception = rail.WriteLogOperator(
                task_id = 'log_endate_exception',
                log = '{{ dag_run.conf.user_log }}',
                message = "User end date is prior to user start date, user not updated",
                severity='Exception',
                properties ={
                    "lastname": "{{dag_run.conf.last_name}}",
                    "firstname": "{{dag_run.conf.first_name}}",
                    "loginname": "{{dag_run.conf.login_name}}",
                    "employeeid": "{{dag_run.conf.employee_id}}",
                    'manager': "{{ dag_run.conf.supervisor_id }}",
                    "userstatus": "{{ dag_run.conf.user_status }}",
                    "co_costcenter": "{{ dag_run.conf.cost_center_description }}",
                    "location": "{{ dag_run.conf.location_description }}",
                    'action': 'Validation',
                    'status': "Exception",
                    'details': "User end date is prior to user start date, user not updated"
                }
            )

            log_disabled_success = rail.WriteLogOperator(
                task_id = 'log_disabled_success',
                log = '{{ dag_run.conf.user_log }}',
                message = "User Disabled Successfully",
                severity='Success',
                properties = {
                    "lastname": "{{dag_run.conf.last_name}}",
                    "firstname": "{{dag_run.conf.first_name}}",
                    "loginname": "{{dag_run.conf.login_name}}",
                    "employeeid": "{{dag_run.conf.employee_id}}",
                    'manager': "{{ dag_run.conf.supervisor_id }}",
                    "userstatus": "{{ dag_run.conf.user_status }}",
                    "co_costcenter": "{{ dag_run.conf.cost_center_description }}",
                    "location": "{{ dag_run.conf.location_description }}",
                    'action': 'Disable',
                    'status': "Success",
                    'details': "User Disabled Successfully"
                }
            )

            empty_is_user_disabled = rail.EmptyOperator(
                task_id='empty_is_user_disabled'
            )

            get_direct_reports_for_user = rail.RepliconServiceOperator(
                task_id = "get_direct_reports_for_user",
                endpoint = "/services/UserService1.svc/GetDirectReportsForUser",
                data = {
                    "userUri": "{{dag_run.conf.useruri}}",
                    "asOfDate": None,
                    "userStatusOptionUri": "urn:replicon:user-status-option:include-all-users"
                }
            )

            is_user_disabled =  rail.IfOperator(
                task_id="is_user_disabled",
                test=custom_method.can_user_profile_enable,
                yes_task="enable_login",
                no_task="get_current_udf_values"
            )

            enable_login = rail.RepliconServiceOperator(
                task_id='enable_login',
                endpoint='/services/securityservice1.svc/EnableLogin',
                data={
                    "userUri": '{{ dag_run.conf.useruri }}'
                }
            )

            get_current_udf_values = rail.PythonOperator(
                task_id='get_current_udf_values',
                python_callable=lambda: rail.result('get_user_data')[0][
                    'userDetails']['customFieldValues']
            )

            get_effective_user_groupmembership = rail.RepliconServiceOperator(
                task_id='get_effective_user_groupmembership',
                endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
                data={
                    "userUri": "{{dag_run.conf.useruri}}",
                    "dateRange": null
                },
                data_handler=response_filters.get_effective_user_groupmembership_filter
            )

            update_existing_user = rail.RepliconServiceOperator(
                task_id='update_existing_user',
                endpoint='/services/importservice2.svc/CreateUserOrApplyModifications',
                data=lambda dag_run: request_payload.get_create_update_user_payload(config, dag_run, "update_user"),
            )

            get_effective_co_code_usertype = rail.PythonOperator(
                task_id='get_effective_co_code_usertype',
                python_callable=lambda: {
                    "division": custom_method.get_effective_division_or_user(rail.result('get_user_data')[0]['divisionSchedule'], 'division'),
                    "usertype": custom_method.get_effective_division_or_user(rail.result('get_user_data')[0]['employeeTypeGroupSchedule'], 'employeeTypeGroup'),
                }
            )

            get_project_code_details = rail.PythonOperator(
                task_id='get_project_code_details',
                python_callable=lambda dag_run: custom_method.has_company_code_changed(dag_run)
            )

            if_company_code_changed = rail.IfOperator(
                task_id="if_company_code_changed",
                test=lambda : bool(rail.result('get_project_code_details')['project_codes']),
                yes_task='get_old_admin_project_uris',
                no_task='if_supervisor_id_present'
            )

            get_old_admin_project_uris = rail.PythonOperator(
                task_id='get_old_admin_project_uris',
                python_callable=lambda dag_run: custom_method.get_old_admin_project_uris(dag_run, rail.result('get_project_code_details')['old_projects'])
            )

            log_project_resource = rail.WriteLogOperator(
                task_id='log_project_resource',
                log='{{ dag_run.conf.project_user_log }}',
                items='{{ result("get_project_code_details").new_projects | to_json }}',
                message="NA",
                severity="Skipped",
                properties=lambda item, dag_run: {
                    "projectcode": item["code"],
                    "useruri": dag_run.conf['useruri']
                }
            )

            has_old_project = rail.IfOperator(
                task_id="has_old_project",
                test=lambda: bool(rail.result('get_project_code_details')['old_projects']),
                yes_task='reset_old_project_resource',
                no_task='if_supervisor_id_present'
            )

            reset_old_project_resource = rail.RepliconServiceOperator(
                task_id='reset_old_project_resource',
                endpoint='/graphql',
                data=lambda dag_run: request_payload._old_project_assign_resource(
                    dag_run.conf['useruri']
                ),
                app='polaris',
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
                task_id='log_user_supervisor_same'
            )

            process_supervisor_entry,  process_supervisor_exit= process_supervisor_assignment_task_group(
                'useruri', 'update_user', config)

            log_user_completion = rail.WriteLogOperator(
                task_id='log_user_completion',
                log = '{{ dag_run.conf.user_log }}',
                message=custom_method.get_update_user_message,
                severity=custom_method.get_update_user_severity,
                properties=lambda dag_run: {
                    'lastname': dag_run.conf['last_name'],
                    'firstname': dag_run.conf['first_name'],
                    'loginname':  dag_run.conf['login_name'],
                    'employeeid': dag_run.conf['employee_id'],
                    'manager': dag_run.conf['supervisor_id'],
                    "userstatus": dag_run.conf['user_status'],
                    "co_costcenter": dag_run.conf['cost_center_description'],
                    "location": dag_run.conf['location_description'],
                    'action': 'Update',
                    'status': custom_method.get_update_user_severity(),
                    'details': custom_method.get_update_user_message(),
                }
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log = '{{ dag_run.conf.user_log }}',
                trigger_rule='one_failed',
                severity='Error',
                message='{{ get_error_message() }}',
                properties={
                    "lastname": "{{dag_run.conf.last_name}}",
                    "firstname": "{{dag_run.conf.first_name}}",
                    "loginname": "{{dag_run.conf.login_name}}",
                    "employeeid": "{{dag_run.conf.employee_id}}",
                    'manager': '{{dag_run.conf.supervisor_id}}',
                    "userstatus": "{{ dag_run.conf.user_status }}",
                    "co_costcenter": "{{ dag_run.conf.cost_center_description }}",
                    "location": "{{ dag_run.conf.location_description }}",
                    'action': 'Update',
                    'status': 'Error',
                    'details': '{{ get_error_message() }}',
                }
            )

            can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> get_user_data

            get_user_data >> if_end_date_present >> rail.Label("No") >> empty_is_user_disabled
            if_end_date_present >> rail.Label("Yes") >> is_enddate_greater_than_start_date >> rail.Label(
                "No") >> log_endate_exception >> catch_and_log_errors
            is_enddate_greater_than_start_date >> rail.Label("Yes") >> update_employee_endate >> if_can_be_disabled

            if_can_be_disabled >> rail.Label('Yes') >> disable_login >> log_disabled_success >> get_current_udf_values
            if_can_be_disabled >> rail.Label('No') >> empty_is_user_disabled

            empty_is_user_disabled >> get_direct_reports_for_user >> is_user_disabled >> rail.Label('Yes') >> enable_login >> get_current_udf_values
            is_user_disabled >> rail.Label('No') >> get_current_udf_values

            get_current_udf_values >> get_effective_user_groupmembership
            get_effective_user_groupmembership >> update_existing_user >> get_effective_co_code_usertype >> \
            get_project_code_details >> if_company_code_changed >> rail.Label(
                "Yes") >> get_old_admin_project_uris >> log_project_resource >> has_old_project >> rail.Label(
                "Yes") >> reset_old_project_resource >> if_supervisor_id_present
            has_old_project >> rail.Label(
                "No") >> if_supervisor_id_present
            if_company_code_changed >> rail.Label(
                "No") >> if_supervisor_id_present
            
            if_supervisor_id_present >> rail.Label('No') >> log_user_completion
            if_supervisor_id_present >> rail.Label('Yes') >> if_user_is_supervisor

            if_user_is_supervisor >> rail.Label('No') >> process_supervisor_entry
            if_user_is_supervisor >> rail.Label('Yes') >> log_user_supervisor_same >> log_user_completion
            process_supervisor_exit >> log_user_completion >> catch_and_log_errors

        append_dags.append(dag)
    return append_dags

rail.for_each_instance(create_child_dag)
