from datetime import timedelta
import rail
from macquariegroup.user_import.utils.request_payload import get_search_supervisor_payload, get_update_supervisor_payload, get_today_date
from macquariegroup.user_import.utils.custom_methods import map_supervisor_list
from airflow.models import Variable

def create_process_supervisor_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'macquarie_user_import_process_supervisors_child_{config.instance}',
        description=f'Macquarie User Import process supervisors child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_conf")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "can_assign_supervisor"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='can_assign_supervisor',
            end_task="catch_and_log_error",
        )

        can_assign_supervisor = rail.IfOperator(
            task_id="can_assign_supervisor",
            test="{{dag_run.conf.supervisor != dag_run.conf.user_employee_id}}",
            yes_task="is_supervisor_uri_present",
            no_task="log_user_emp_id_supervisor_id_same"
        )

        log_user_emp_id_supervisor_id_same = rail.WriteLogOperator(
            task_id="log_user_emp_id_supervisor_id_same",
            severity='Exception',
            message='User Employee id is same as supervisor id',
            properties={
                'userloginname': '{{ dag_run.conf.login_name }}',
                'user_name': "{{dag_run.conf.user_name}}",
                'employee_id': "{{dag_run.conf.user_employee_id}}",
                'action': '{{dag_run.conf.action}}',
                'status': 'Exception',
                'details': 'User Employee id is same as supervisor id'
            }
        )

        is_supervisor_uri_present = rail.IfOperator(
            task_id="is_supervisor_uri_present",
            test="{{dag_run.conf.supervisor_uri | is_truthy}}",
            yes_task="dummy_get_assigned_permissions_for_supervisor",
            no_task="search_supervisor_in_replicon"
        )

        def get_assigned_permissions_for_supervisor_response_filter(response):
            response = response.json()['d']
            if not response:
                return []
            return rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.uri')

        dummy_get_assigned_permissions_for_supervisor = rail.EmptyOperator(
            task_id="dummy_get_assigned_permissions_for_supervisor"
        )

        get_assigned_permissions_for_supervisor = rail.RepliconServiceOperator(
            task_id="get_assigned_permissions_for_supervisor",
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{dag_run.conf.supervisor_uri if dag_run.conf.supervisor_uri else result('search_supervisor_in_replicon')[0].useruri}}"
            },
            response_filter=get_assigned_permissions_for_supervisor_response_filter
        )

        get_users_current_timesheet_end_date = rail.RepliconServiceOperator(
            task_id="get_users_current_timesheet_end_date",
            endpoint="services/TimesheetService1.svc/GetNextTimesheetDueDate",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}",
                "asOfDate": get_today_date()
            }
        )

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id="search_supervisor_in_replicon",
            endpoint="/services/UserListService1.svc/GetData",
            data=get_search_supervisor_payload,
            response_filter=map_supervisor_list
        )

        # pylint: disable=line-too-long
        supervisor_uri = "{{dag_run.conf.supervisor_uri if dag_run.conf.supervisor_uri else result('search_supervisor_in_replicon')[0].useruri}}"

        has_any_supervisor_found = rail.IfOperator(
            task_id="has_any_supervisor_found",
            test=lambda: len(rail.result(
                "search_supervisor_in_replicon")) > 0,
            yes_task="dummy_has_many_users",
            no_task="is_action_add"
        )

        is_action_add = rail.IfOperator(
            task_id = "is_action_add",
            test = "{{ dag_run.conf.action | lower == 'add'}}",
            yes_task="assign_default_supervisor",
            no_task="log_no_user_found_with_employee_id"
        )

        assign_default_supervisor = rail.RepliconServiceOperator(
            task_id="assign_default_supervisor",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "supervisorUri": dag_run.conf['default_supervisor_uri'],
                "dateRange":  None
            }
        )

        log_assigned_default_supervisor = rail.WriteLogOperator(
            task_id="log_assigned_default_supervisor",
            severity='Success',
            message='No User found with supervisor id. Assigned Default supervisor',
            properties={
                'userloginname': '{{ dag_run.conf.login_name }}',
                'user_name': "{{dag_run.conf.user_name}}",
                'employee_id': "{{dag_run.conf.user_employee_id}}",
                'action': '{{dag_run.conf.action}}',
                'status': 'Success',
                'details': 'No User found with supervisor id {{dag_run.conf.supervisor}} in Replicon.  Assigned Default supervisor'
            }
        )

        log_no_user_found_with_employee_id = rail.WriteLogOperator(
            task_id="log_no_user_found_with_employee_id",
            severity='Exception',
            message='No User found with supervisor id',
            properties={
                'userloginname': '{{ dag_run.conf.login_name }}',
                'user_name': "{{dag_run.conf.user_name}}",
                'employee_id': "{{dag_run.conf.user_employee_id}}",
                'action': '{{dag_run.conf.action}}',
                'status': 'Exception',
                'details': 'No User found with supervisor id {{dag_run.conf.supervisor}}'
            }
        )

        dummy_has_many_users = rail.EmptyOperator(
            task_id="dummy_has_many_users"
        )

        has_many_users = rail.IfOperator(
            task_id="has_many_users",
            test=lambda: len(rail.result(
                "search_supervisor_in_replicon")) > 1,
            yes_task="log_multiple_employees_with_same_employee_id",
            no_task="get_assigned_permissions_for_supervisor"
        )

        log_multiple_employees_with_same_employee_id = rail.WriteLogOperator(
            task_id="log_multiple_employees_with_same_employee_id",
            severity='Exception',
            message='Multiple User found with same supervisor id',
            properties={
                'userloginname': '{{ dag_run.conf.login_name }}',
                'user_name': "{{dag_run.conf.user_name}}",
                'employee_id': "{{dag_run.conf.user_employee_id}}",
                'action': '{{dag_run.conf.action}}',
                'status': 'Exception',
                'details': 'Multiple User found with same supervisor id {{dag_run.conf.supervisor}}'
            }
        )

        has_supervisor_permission = rail.IfOperator(
            task_id="has_supervisor_permission",
            test="{{result('get_assigned_permissions_for_supervisor') | is_truthy}}",
            yes_task="update_supervisor_schedule_for_user",
            no_task="get_supervisor_permission_set"
        )

        get_supervisor_permission_set = rail.RepliconServiceOperator(
            task_id="get_supervisor_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'displayText', 'Gen3 Supervisor', 'uri')
        )

        assign_supervisor_permissions = rail.RepliconServiceOperator(
            task_id="assign_supervisor_permissions",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": supervisor_uri,
                "permissionSetUri": "{{result('get_supervisor_permission_set')}}"
            }
        )

        update_supervisor_schedule_for_user = rail.RepliconServiceOperator(
            task_id="update_supervisor_schedule_for_user",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=get_update_supervisor_payload
        )

        def get_supervisor_log_message(dag_run):
            if dag_run.conf['can_assign_default'] not in ["No", False, 'False']:
                return "Supervisor disabled in Replicon. Default Supervisor assigned"
            if rail.result('search_supervisor_in_replicon') and \
                rail.result('search_supervisor_in_replicon')[0]['enabled'] in [False, 'False']:
                return "Supervisor disabled in Replicon. Default Supervisor assigned"
            return "Supervisor assigned successfully"

        log_success = rail.WriteLogOperator(
            task_id="log_success",
            severity='Success',
            message='User Supervisor assigned successfully',
            properties=lambda dag_run: {
                'userloginname': dag_run.conf['login_name'],
                'user_name': dag_run.conf['user_name'],
                'employee_id': dag_run.conf['user_employee_id'],
                'action': dag_run.conf['action'],
                'status': 'Success',
                'details': get_supervisor_log_message(dag_run)
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'userloginname': '{{ dag_run.conf.login_name }}',
                'user_name': "{{dag_run.conf.user_name}}",
                'employee_id': "{{dag_run.conf.user_employee_id}}",
                'action': '{{dag_run.conf.action}}',
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> rail.Label("On Error") >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> can_assign_supervisor
        can_assign_supervisor >> rail.Label("Yes") >> is_supervisor_uri_present >> rail.Label(
            "Yes") >> dummy_get_assigned_permissions_for_supervisor >> get_assigned_permissions_for_supervisor
        can_assign_supervisor >> log_user_emp_id_supervisor_id_same >> rail.Label(
            "On Error") >> catch_and_log_error

        is_supervisor_uri_present >> rail.Label("No") >> search_supervisor_in_replicon >> has_any_supervisor_found >> rail.Label("Yes")\
            >> dummy_has_many_users >> has_many_users >> rail.Label("Yes") >> log_multiple_employees_with_same_employee_id >> rail.Label("On Error") >> catch_and_log_error
        has_any_supervisor_found >> rail.Label(
            "No") >> is_action_add >> rail.Label("No") >> log_no_user_found_with_employee_id >> rail.Label("On Error") >> catch_and_log_error
        is_action_add >> rail.Label("Yes") >> assign_default_supervisor >> log_assigned_default_supervisor >> rail.Label("On Error") >> catch_and_log_error
        has_many_users >> rail.Label("No") >> get_assigned_permissions_for_supervisor >> get_users_current_timesheet_end_date\
            >> has_supervisor_permission >> rail.Label("Yes") >> update_supervisor_schedule_for_user
        has_supervisor_permission >> rail.Label(
            "No") >> get_supervisor_permission_set >> assign_supervisor_permissions >> update_supervisor_schedule_for_user >> log_success
        log_success >> rail.Label(
            "On Error") >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_process_supervisor_dag)
