"""
Process Supervisor Check - Unisys Workday User Import Child DAG

Validates and processes supervisor assignments for users.
This child DAG handles supervisor validation, permission assignment, login enablement,
and supervisor schedule updates for both new and existing users.

Key features:
    - Searches for supervisor in Replicon by employee ID
    - Validates supervisor status (enabled/disabled)
    - Checks supervisor end date validity
    - Enables disabled supervisor logins when appropriate
    - Assigns supervisor permissions if missing
    - Updates supervisor assignment schedules
    - Handles supervisor change detection
    - Logs exceptions for invalid supervisors
    - Supports batch task execution

Functions:
    create_child_dag_wbs(config): Creates the supervisor check child DAG
"""
from datetime import timedelta
from airflow.models import Variable
import rail
from unisys.workday_user_import_v1.utils import request_payload, response_filters, custom_method


def create_child_dag_wbs(config):
    """
    Create child DAG for processing and validating supervisor assignments.

    This DAG performs comprehensive supervisor checks including existence validation,
    status checks, permission verification, and schedule updates. It handles both
    new supervisor assignments and updates to existing supervisor relationships.

    Args:
        config: Configuration object containing DAG settings including:
            - processs_supervisor: DAG ID for this child DAG
            - company_key: Replicon company identifier
            - replicon_conn_id: Replicon connection ID
            - max_active_runs_process_supervisor: Max parallel DAG runs
            - can_run_batch_task: Variable name controlling batch execution
            - execution_timeout_days: Task execution timeout

    Returns:
        DAG: Configured Airflow DAG object for supervisor processing

    DAG Configuration:
        dag_run.conf should contain:
            - employeeid: Employee ID to process
            - useruri: User URI in Replicon
            - supervisor_log: Log artifact for tracking operations
            - supervisor_log: User log artifact for filtering
            - action: 'Add' or 'Update' action type
            - change_effective_date: Date when change takes effect
            - supervisor_permission_uri: URI of supervisor permission set
    """
    with rail.create_airflow_dag(
        dag_id=config.processs_supervisor,
        description='Unisys Workday User Import - Process Supervisor',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_supervisor,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='filter_user_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='filter_user_logs',
            end_task='on_error',
        )

        filter_user_logs = rail.FilterLogEntriesOperator(
            task_id='filter_user_logs',
            log='{{ dag_run.conf.user_log }}',
            properties={
                'employeeid': '{{ dag_run.conf.employeeid }}'
            },
            remove_filtered_entries=True
        )

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id='search_supervisor_in_replicon',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=custom_method.get_supervisor_data_with_manager_id,
            data_handler=response_filters.map_supervisor_list_data
        )

        is_supervisor_exists = rail.IfOperator(
            task_id='is_supervisor_exists',
            test=lambda: bool(rail.result('search_supervisor_in_replicon')),
            yes_task='is_supervisor_disabled',
            no_task='log_supervisor_not_present'
        )

        log_supervisor_not_present = rail.EmptyOperator(
            task_id="log_supervisor_not_present",
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id='is_supervisor_disabled',
            test=lambda: rail.result('search_supervisor_in_replicon')['status'],
            yes_task='is_enddate_in_past',
            no_task='get_missing_supervisor_permission'
        )

        is_enddate_in_past = rail.IfOperator(
            task_id='is_enddate_in_past',
            test=lambda: rail.result('search_supervisor_in_replicon')['is_enddate_in_past'],
            yes_task='log_supervisor_disabled_with_past_enddate',
            no_task='enable_supervisor_login'
        )

        log_supervisor_disabled_with_past_enddate = rail.WriteLogOperator(
            task_id='log_supervisor_disabled_with_past_enddate',
            log='{{ dag_run.conf.user_log }}',
            message="Supervisor disabled with past enddate",
            items="{{ result('filter_user_logs') }}",
            severity='Exception',
            properties= lambda item, dag_run:{
                'lastname': item['properties']['lastname'],
                'firstname': item['properties']['firstname'],
                'loginname':  item['properties']['loginname'],
                'employeeid': item['properties']['employeeid'],
                'manager': item['properties']['manager'],
                "userstatus": item['properties']['userstatus'],
                "co_costcenter": item['properties']['co_costcenter'],
                "location": item['properties']['location'],
                'action': item['properties']['action'],
                'status': 'Exception',
                'details':'Supervisor disabled with past enddate'
            }
        )

        enable_supervisor_login = rail.RepliconServiceOperator(
            task_id='enable_supervisor_login',
            endpoint='/services/securityservice1.svc/EnableLogin',
            data={
                "userUri": "{{ result('search_supervisor_in_replicon').uri }}"
            }
        )

        get_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data=lambda: {
                'userUri': rail.result('search_supervisor_in_replicon')['uri']
            },
            data_handler=response_filters.is_assign_supervisorpermission
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_missing_supervisor_permission') | is_truthy }}",
            yes_task='add_missing_supervisor_permission',
            no_task='is_new_user_supervisor_assignment'
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=lambda dag_run: {
                'userUri': rail.result('search_supervisor_in_replicon')['uri'],
                'permissionSetUri': dag_run.conf['supervisor_permission_uri']
            }
        )

        is_new_user_supervisor_assignment = rail.IfOperator(
            task_id='is_new_user_supervisor_assignment',
            test=lambda dag_run: dag_run.conf['action'] == 'Add',
            yes_task='update_supervisor_schedule_for_user',
            no_task='get_effective_supervisor_of_user'
        )

        get_effective_supervisor_of_user  = rail.RepliconServiceOperator(
            task_id="get_effective_supervisor_of_user",
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data={
                "userUri": "{{ dag_run.conf.useruri}}",
                "asOfDate": request_payload.get_today_date()
            }
        )

        is_supervisor_changed = rail.IfOperator(
            task_id='is_supervisor_changed',
            test=custom_method.validate_supervisor_changed,
            yes_task='update_supervisor_schedule_for_user',
            no_task='same_supervisor_already_assigned'
        )

        same_supervisor_already_assigned = rail.EmptyOperator(
            task_id="same_supervisor_already_assigned",
        )

        update_supervisor_schedule_for_user = rail.RepliconServiceOperator(
            task_id="update_supervisor_schedule_for_user",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_supervisor_in_replicon')['uri'],
                "dateRange": None if dag_run.conf['action'] == 'Add' else {
                    "startDate": request_payload.get_replicon_date(dag_run.conf['effectivedate'])
                }
            }
        )

        dummy_filter_user_logs = rail.EmptyOperator(
            task_id="dummy_filter_user_logs",
        )

        is_filtered_userlogs = rail.IfOperator(
            task_id='is_filtered_userlogs',
            test="{{ result('filter_user_logs', 'length') > 0 }}",
            yes_task='update_userlog_entries',
            no_task='on_error'
        )

        update_userlog_entries = rail.WriteLogOperator(
            task_id='update_userlog_entries',
            message='update supervisor entries',
            log='{{ dag_run.conf.user_log }}',
            items="{{ result('filter_user_logs') }}",
            properties=lambda item, dag_run: {
                'lastname': item['properties']['lastname'],
                'firstname': item['properties']['firstname'],
                'loginname':  item['properties']['loginname'],
                'employeeid': item['properties']['employeeid'],
                'manager': item['properties']['manager'],
                "userstatus": item['properties']['userstatus'],
                "co_costcenter": item['properties']['co_costcenter'],
                "location": item['properties']['location'],
                'action': item['properties']['action'],
                'status': custom_method.get_supervisor_severity(),
                'details':custom_method.get_supervisor_message(item['properties']['action'], dag_run),
            }
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        is_entries_present_error = rail.IfOperator(
            task_id='is_entries_present_error',
            test="{{ result('filter_user_logs', 'length') > 0 }}",
            yes_task='update_userlog_entries_error',
            no_task='log_to_sumo'
        )

        update_userlog_entries_error = rail.WriteLogOperator(
            task_id='update_userlog_entries_error',
            message='update supervisor entries',
            log='{{ dag_run.conf.user_log }}',
            severity='Error',
            items="{{ result('filter_user_logs') }}",
            properties={
                'lastname': '{{ item.properties.lastname }}',
                'firstname': '{{ item.properties.firstname }}',
                'loginname': '{{ item.properties.loginname }}',
                'employeeid': '{{ item.properties.employeeid }}',
                'manager': '{{ item.properties.manager }}',
                "userstatus": "{{ item.properties.userstatus }}",
                "co_costcenter": "{{ item.properties.co_costcenter }}",
                "location": "{{ item.properties.location }}",
                'action': '{{ item.properties.action }}',
                'status': 'Error',
                'details': "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> on_error
        can_run_batch_task >> rail.Label('No') >> filter_user_logs

        filter_user_logs >> search_supervisor_in_replicon >> is_supervisor_exists >> rail.Label('No') >> log_supervisor_not_present
        log_supervisor_not_present >> dummy_filter_user_logs

        is_supervisor_exists >> rail.Label('Yes') >> is_supervisor_disabled >> rail.Label('Yes') >> is_enddate_in_past
        is_enddate_in_past >> rail.Label('Yes') >> log_supervisor_disabled_with_past_enddate >> dummy_filter_user_logs
        is_enddate_in_past >> rail.Label('No') >> enable_supervisor_login >> get_missing_supervisor_permission
        is_supervisor_disabled >> rail.Label('No') >> get_missing_supervisor_permission >> should_add_missing_permissions >> rail.Label(
            "Yes") >> add_missing_supervisor_permission >> is_new_user_supervisor_assignment
        should_add_missing_permissions >> rail.Label(
            "No") >> is_new_user_supervisor_assignment

        is_new_user_supervisor_assignment >> rail.Label('Yes') >> update_supervisor_schedule_for_user
        is_new_user_supervisor_assignment >> rail.Label('No') >> get_effective_supervisor_of_user >> is_supervisor_changed
        is_supervisor_changed >> rail.Label(
            'Yes') >> update_supervisor_schedule_for_user >> dummy_filter_user_logs
        is_supervisor_changed >> rail.Label(
            'No') >> same_supervisor_already_assigned >> dummy_filter_user_logs
        dummy_filter_user_logs >> is_filtered_userlogs >> rail.Label('Yes') >> update_userlog_entries >> on_error
        is_filtered_userlogs >> rail.Label('No') >> on_error >> is_entries_present_error  >> rail.Label('Yes') >> update_userlog_entries_error >> log_to_sumo
        is_entries_present_error  >> rail.Label('No') >> log_to_sumo



    return dag


rail.for_each_instance(create_child_dag_wbs)
