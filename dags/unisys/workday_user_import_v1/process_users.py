"""
Process Users - Unisys Workday User Import Child DAG

Orchestrates individual user processing by routing to new user or update user workflows.
This child DAG is triggered for each user record and determines whether to create a new user
or update an existing one, handles employee conversions, and validates location scope.

Key features:
    - Queries user data from validation collection
    - Searches for existing users in Replicon
    - Handles employee type conversions (e.g., Employee to Contingent Worker)
    - Validates location scope for contingent workers
    - Routes to appropriate child DAG (new or update)
    - Logs user processing results
    - Supports batch task execution

Functions:
    create_child_dag(config): Creates the process users child DAG
"""
from datetime import timedelta
from airflow.models import Variable
import rail
from unisys.workday_user_import_v1.utils import request_payload, response_filters, custom_method

def create_child_dag(config):
    """
    Create child DAG for orchestrating individual user processing.

    This DAG determines whether a user needs to be created or updated in Replicon,
    handles employee conversion scenarios, validates location scope, and triggers
    the appropriate processing child DAG.

    Args:
        config: Configuration object containing DAG settings including:
            - process_each_user: DAG ID for this child DAG
            - company_key: Replicon company identifier
            - replicon_conn_id: Replicon connection ID
            - max_active_runs_process_users: Max parallel DAG runs
            - process_new_users: DAG ID for new user processing
            - process_update_users: DAG ID for user updates
            - can_run_batch_task: Variable name controlling batch execution
            - execution_timeout_days: Task execution timeout
            - OUT_OF_SCOPE_LOCATIONS: List of restricted locations

    Returns:
        DAG: Configured Airflow DAG object for user processing

    DAG Configuration:
        dag_run.conf should contain:
            - employee_id: Employee ID to process
            - replicon_location_details: Artifact with location data
            - replicon_usertypes_details: Artifact with user type data
            - replicon_division_details: Artifact with division data
            - replicon_user_udfs: User-defined field definitions
            - replicon_permission_sets: Permission set data
            - replicon_policy_sets: Policy set data
            - replicon_ts_approval_paths: Timesheet approval paths
            - replicon_all_timezones: Timezone data
            - replicon_office_schedule: Office schedule data
            - replicon_user_status_dropdown: User status options
            - replicon_ts_period_list: Timesheet period data
            - replicon_holiday_calendars: Holiday calendar data
            - replicon_purchase_order_ids: Purchase order data
            - replicon_activity_uris: Activity URIs
            - supervisor_log: Log artifact for supervisor operations
    """
    # pylint: disable=too-many-statements, line-too-long, cell-var-from-loop
    append_dags = []
    for idx in range(0, config.PROCESS_USER_BATCH_COUNT):
        with rail.create_airflow_dag(
            dag_id=f'{config.process_each_user}_batch_{idx+1}',
            description='Unisys Workday User Import - Process Each Users',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_users,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='process_user_log'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='process_user_log',
                end_task='catch_and_log_errors',
            )

            process_user_log = rail.CreateLogOperator(
                task_id="process_user_log"
            )

            process_project_log = rail.CreateLogOperator(
                task_id="process_project_log"
            )
            
            query_user_data = rail.QueryCollectionOperator(
                task_id="query_user_data",
                query="""SELECT * FROM valid_data WHERE employee_id=:empl_id""",
                query_params={
                    'empl_id': '{{ dag_run.conf.employee_id }}'
                }
            )

            get_user_payload_data = rail.PythonOperator(
                task_id='get_user_payload_data',
                python_callable=custom_method.get_payload_user_data
            )

            get_user_by_empl_id = rail.RepliconServiceOperator(
                task_id="get_user_by_empl_id",
                endpoint="/services/ImportService1.svc/BulkGetUsers3",
                data=request_payload.get_user_data_payload,
                data_handler=response_filters.get_filtered_user_data
            )

            has_conversion_detail = rail.IfOperator(
                task_id='has_conversion_detail',
                test="{{ result('get_user_payload_data').old_data | is_truthy and result('get_user_by_empl_id') | is_truthy }}",
                yes_task='update_existing_user_profile',
                no_task='is_existing_user'
            )

            update_existing_user_profile = rail.RepliconServiceOperator(
                task_id='update_existing_user_profile',
                endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
                data=request_payload.get_employee_conversion_payload
            )

            disable_user = rail.RepliconServiceOperator(
                task_id='disable_user',
                endpoint='services/SecurityService1.svc/DisableLogin',
                data={
                    'userUri': "{{ result('get_user_by_empl_id')[0].userDetails.uri }}",
                }
            )

            log_old_profile_update = rail.WriteLogOperator(
                task_id='log_old_profile_update',
                log = '{{result("process_user_log")}}',
                severity='Success',
                message='Old Profile Update.',
                properties=custom_method.get_old_profile_update_log,
            )

            is_existing_user = rail.IfOperator(
                task_id='is_existing_user',
                test="{{ result('get_user_by_empl_id') | is_truthy }}",
                yes_task='get_effective_user_groups',
                no_task='if_location_out_of_scope'
            )

            get_effective_user_groups = rail.RepliconServiceOperator(
                task_id='get_effective_user_groups',
                endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
                data={
                    "userUri": "{{ result('get_user_by_empl_id')[0].userDetails.uri }}",
                    "dateRange": None
                },
                data_handler=response_filters.get_effective_user_groupmembership_filter
            )

            is_user_type_change = rail.IfOperator(
                task_id='is_user_type_change',
                test=custom_method.has_user_type_changed,
                yes_task='update_old_profile_usertype',
                no_task='if_location_out_of_scope'
            )

            update_old_profile_usertype = rail.RepliconServiceOperator(
                task_id='update_old_profile_usertype',
                endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
                data=request_payload.get_old_user_type_payload
            )

            if_location_out_of_scope = rail.IfOperator(
                task_id='if_location_out_of_scope',
                test=lambda: custom_method.get_out_of_scope_location(rail.result('get_user_payload_data')['user_data'], config.OUT_OF_SCOPE_LOCATIONS),
                yes_task="log_out_of_scope_users",
                no_task="if_user_present"
            )

            log_out_of_scope_users = rail.WriteLogOperator(
                task_id='log_out_of_scope_users',
                log = '{{result("process_user_log")}}',
                severity='Exception',
                message='Users location is out of scope',
                properties=lambda dag_run: {
                    "lastname": rail.result('get_user_payload_data')['user_data']['last_name'],
                    "firstname": rail.result('get_user_payload_data')['user_data']['first_name'],
                    "loginname": rail.result('get_user_payload_data')['user_data']['login_name'],
                    "employeeid": dag_run.conf['employee_id'],
                    "manager": rail.result('get_user_payload_data')['user_data']['supervisor_id'],
                    "userstatus": rail.result('get_user_payload_data')['user_data']['user_status'],
                    "co_costcenter": rail.result('get_user_payload_data')['user_data']['cost_center_description'],
                    "location": rail.result('get_user_payload_data')['user_data']['location_description'],
                    "action": "Add",
                    'status': 'Exception',
                    'details': f"User location { rail.result('get_user_payload_data')['user_data']['location'].split('|')[0]} is out of scope for Contingent Worker.",
                },
            )

            if_user_present = rail.IfOperator(
                task_id ='if_user_present',
                test = lambda dag_run: bool(rail.result('get_user_by_empl_id') and (
                    not custom_method.has_user_type_changed(dag_run) and not rail.result('get_user_payload_data')['old_data'])),
                yes_task="process_update_user",
                no_task="process_new_user"
            )

            def get_process_users_batch_dag_id(dag_id, modulo):
                return f'{dag_id}_batch_{modulo+1}'

            process_new_user = rail.TriggerDagRunForEachItemOperator(
                task_id='process_new_user',
                items=[0],
                trigger_dag_id=lambda dag_run: get_process_users_batch_dag_id(config.process_new_users, int(dag_run.conf['modulo'])),
                conf=custom_method.get_process_new_users_conf,
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
                trigger_dag_id=lambda dag_run: get_process_users_batch_dag_id(config.process_update_users, int(dag_run.conf['modulo'])),
                conf=custom_method.get_process_update_users_conf,
                execution_timeout=timedelta(days=config.execution_timeout_days),
                retries=0,
            )

            wait_for_process_update_user = rail.WaitForDagRunsSensor(
                task_id='wait_for_process_update_user',
                dag_runs='{{ result("process_update_user") }}',
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log = '{{result("process_user_log")}}',
                trigger_rule='one_failed',
                severity='Error',
                message='{{ get_error_message() }}',
                properties={
                    "lastname": "{{ result('get_user_payload_data')['user_data'].last_name }}",
                    "firstname": "{{ result('get_user_payload_data')['user_data'].first_name }}",
                    "loginname": "{{ result('get_user_payload_data')['user_data'].login_name }}",
                    "employeeid": "{{ dag_run.conf.employee_id }}",
                    "manager": "{{ result('get_user_payload_data')['user_data'].supervisor_id }}",
                    "userstatus": "{{ result('get_user_payload_data')['user_data'].user_status }}",
                    "co_costcenter": "{{ result('get_user_payload_data')['user_data'].cost_center_description }}",
                    "location": "{{ result('get_user_payload_data')['user_data'].location_description }}",
                    "action": "Sync",
                    'status': 'Error',
                    'details': '{{ get_error_message() }}',
                },
            )

            can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> process_user_log

            process_user_log >> process_project_log >> query_user_data >> get_user_payload_data >> get_user_by_empl_id
            get_user_by_empl_id >> has_conversion_detail >> rail.Label(
                "Yes") >> update_existing_user_profile >> disable_user >> log_old_profile_update >> if_location_out_of_scope >> rail.Label("No") >> if_user_present
            has_conversion_detail >> rail.Label(
                "No") >> is_existing_user >> rail.Label("Yes") >> get_effective_user_groups >> is_user_type_change
            is_user_type_change >> rail.Label("Yes") >> update_old_profile_usertype >> if_location_out_of_scope
            is_user_type_change >> rail.Label("No") >> if_location_out_of_scope
            is_existing_user >> rail.Label("No") >> if_location_out_of_scope
            if_location_out_of_scope >> rail.Label("Yes") >> log_out_of_scope_users >> catch_and_log_errors
            if_user_present >> rail.Label('Yes') >> process_update_user >> wait_for_process_update_user >> catch_and_log_errors
            if_user_present >> rail.Label('No') >> process_new_user >> wait_for_process_new_user >> catch_and_log_errors

        append_dags.append(dag)
    return append_dags

rail.for_each_instance(create_child_dag)
