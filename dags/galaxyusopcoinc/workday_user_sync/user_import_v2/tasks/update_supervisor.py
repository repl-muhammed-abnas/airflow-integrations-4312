import ast
from json import loads
import rail
from galaxyusopcoinc.workday_user_sync.user_import_v2.utils import request_payload, response_filter
from airflow.exceptions import AirflowException

def get_update_supervisor(caller="NA"):
    null = None
    with rail.TaskGroup(group_id='update_supervisor', prefix_group_id=False) as update_supervisor:

        has_supervisor = rail.IfOperator(
            task_id='has_supervisor',
            test=lambda:  bool(request_payload.get_conf()['managerid']),
            yes_task='has_valid_supervisor',
            no_task='update_supervisor_complete',
        )

        has_valid_supervisor = rail.IfOperator(
            task_id='has_valid_supervisor',
            test=lambda: request_payload.get_conf()['managerid'] !=
            request_payload.get_conf()['employeeid'],
            yes_task='search_supervisor_by_employeeid',
            no_task='log_invalid_supervisor'
        )

        log_invalid_supervisor = rail.WriteLogOperator(
            task_id='log_invalid_supervisor',
            log="{{dag_run.conf.create_user_log}}",
            message='Supervisor not assigned since ManagerID and user EmployeeID are the same',
            severity='Exception',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}' if caller != "supervisor" else "{{dag_run.conf.username}}",
                'loginname': '{{dag_run.conf.workemail}}' if caller != "supervisor" else "{{dag_run.conf.loginname}}",
                'status': 'Exception',
                'action': 'Pre-Check',
                'message': 'Supervisor not assigned since ManagerID and user EmployeeID are the same',
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": "{{dag_run.conf.managerid}}",
                "is_add_and_errored": "False"
            }
        )

        search_supervisor_by_employeeid = rail.RepliconServiceOperator(
            task_id='search_supervisor_by_employeeid',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_search_supervisor_param,
            response_filter=response_filter.map_supervisor_list
        )

        is_user_found = rail.IfOperator(
            task_id="is_supervisor_found",
            test=lambda: bool(rail.result("search_supervisor_by_employeeid")),
            yes_task="has_many_supervisor",
            no_task="update_supervisor_complete"
        )

        has_many_supervisor = rail.IfOperator(
            task_id='has_many_supervisor',
            test=lambda: len(rail.result(
                search_supervisor_by_employeeid.task_id)) > 1,
            yes_task='add_mutiplese_supervisor_log',
            no_task='map_supervisor_details'
        )

        add_mutiplese_supervisor_log = rail.WriteLogOperator(
            task_id='add_mutiplese_supervisor_log',
            log="{{dag_run.conf.create_user_log}}",
            message='Supervisor not assigned since there multiple users available with ID : {{ dag_run.conf.managerid }}',
            severity='Exception',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}' if caller != "supervisor" else "{{dag_run.conf.username}}",
                'loginname': '{{dag_run.conf.workemail}}' if caller != "supervisor" else "{{dag_run.conf.loginname}}",
                'status': 'Exception',
                'action': ('Update' if caller == "update" else "Add") if caller != "supervisor" else '{{dag_run.conf.action}}',
                'message': 'Supervisor not assigned since there multiple users available with ID  : {{ dag_run.conf.managerid }}',
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": "{{dag_run.conf.managerid}}",
                "is_add_and_errored": "False"
            }
        )

        def do_map_supervisor_details():
            if rail.result(search_supervisor_by_employeeid.task_id) and \
                    len(rail.result(search_supervisor_by_employeeid.task_id)) == 1:
                return rail.result(search_supervisor_by_employeeid.task_id)[0]
            return None

        map_supervisor_details = rail.PythonOperator(
            task_id='map_supervisor_details',
            python_callable=do_map_supervisor_details
        )

        can_assign_supervisor = rail.IfOperator(
            task_id='can_assign_supervisor',
            test=lambda: rail.result(map_supervisor_details.task_id) and
            rail.result(map_supervisor_details.task_id)['useruri'],
            yes_task='has_supervisor_changed',
            no_task='update_supervisor_complete'
        )

        has_supervisor_changed = rail.IfOperator(
            task_id='has_supervisor_changed',
            test=lambda: not rail.result('bulk_get_user3') or not request_payload.get_current_schedule(
                rail.result('bulk_get_user3')['supervisorAssignmentSchedule'])
            or request_payload.get_current_schedule(rail.result('bulk_get_user3')['supervisorAssignmentSchedule'])['supervisor']['uri'] !=
            rail.result(map_supervisor_details.task_id)['useruri'],
            yes_task='is_supervisor_enabled',
            no_task='update_supervisor_complete'

        )

        is_supervisor_enabled = rail.IfOperator(
            task_id='is_supervisor_enabled',
            test="{{ result('map_supervisor_details').enabled == True }}",
            yes_task='get_assigned_supervisor_permissonsets',
            no_task='enable_supervisor'
        )

        enable_supervisor = rail.RepliconServiceOperator(
            task_id='enable_supervisor',
            endpoint='/services/securityService1.svc/EnableLogin',
            data={
                    "userUri": "{{ result('map_supervisor_details').useruri }}"
            },

        )

        add_supervisor_disabled_log = rail.WriteLogOperator(
            task_id='add_supervisor_disabled_log',
            log="{{dag_run.conf.create_user_log}}",
            message='Supervisor with ID : {{ dag_run.conf.managerid }} was disabled. Enabled the user and assigned to user',
            severity='Exception',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}' if caller != "supervisor" else "{{dag_run.conf.username}}",
                'loginname': '{{dag_run.conf.workemail}}' if caller != "supervisor" else "{{dag_run.conf.loginname}}",
                'status': 'Exception',
                'action': ('Update' if caller == "update" else "Add") if caller != "supervisor" else '{{dag_run.conf.action}}',
                'message': 'Supervisor with ID : {{ dag_run.conf.managerid }} was disabled. Enabled the user and assigned to user',
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": "{{dag_run.conf.managerid}}",
                "is_add_and_errored": "False"
            }
        )

        get_assigned_supervisor_permissonsets = rail.RepliconServiceOperator(
            task_id='get_assigned_supervisor_permissonsets',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                "userUri": "{{ result('map_supervisor_details').useruri }}"
            }
        )

        has_manager_permission = rail.IfOperator(
            task_id='has_manager_permission',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_assigned_supervisor_permissonsets'), 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.name')) and
            bool(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_assigned_supervisor_permissonsets'), 'policyUri', 'urn:replicon:policy:user', 'permissionSet.name')),
            yes_task='update_supervisor_assignmentschedule_overdaterange',
            no_task='assign_manager_permission_to_supervisor'
        )

        assign_manager_permission_to_supervisor = rail.RepliconServiceOperator(
            task_id='assign_manager_permission_to_supervisor',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                "userUri": "{{ result('map_supervisor_details').useruri }}",
                "permissionSetUri": "{{ dag_run.conf.permissionsets | find_first_by_attr_and_get_attr('displayText', 'Supervisor','uri') }}"
            }
        )

        assign_user_permission_to_supervisor = rail.RepliconServiceOperator(
            task_id='assign_user_permission_to_supervisor',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                "userUri": "{{ result('map_supervisor_details').useruri }}",
                "permissionSetUri": "{{ dag_run.conf.permissionsets | find_first_by_attr_and_get_attr('displayText', 'Project Resource with Reports','uri') }}"
            }
        )

        def get_effective_date_to_use(dag_run):
            if caller.lower() == "update":
                return request_payload.get_new_effective_date()
            if caller.lower() == "supervisor":
                if dag_run.conf['action'].lower() == "add":
                    return null
                if isinstance(dag_run.conf['user_effective_date'], str):
                    return ast.literal_eval(dag_run.conf['user_effective_date'])
                return dag_run.conf['user_effective_date']
            raise AirflowException(f"Unknown caller for supervisor update caller {caller}")

        update_supervisor_assignmentschedule_overdaterange = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignmentschedule_overdaterange',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda dag_run: {
                "userUri": (request_payload.get_user_uri() or request_payload.get_created_user_uri())
                    if dag_run.conf.get("rehire", '') != "yes" else request_payload.get_created_user_uri(),
                "supervisorUri": rail.result('map_supervisor_details')['useruri'],
                "dateRange": {
                    "startDate": get_effective_date_to_use(dag_run) if caller.lower() != "add" else null,
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        update_supervisor_complete = rail.EmptyOperator(
            task_id='update_supervisor_complete'
        )

        has_supervisor >> rail.Label(
            'Yes') >> has_valid_supervisor
        has_supervisor >> rail.Label(
            'No') >> update_supervisor_complete

        has_valid_supervisor >> rail.Label(
            'Yes') >> search_supervisor_by_employeeid
        has_valid_supervisor >> rail.Label(
            'No') >> log_invalid_supervisor >> update_supervisor_complete

        search_supervisor_by_employeeid >> is_user_found >> rail.Label(
            'Yes') >> has_many_supervisor
        is_user_found >> rail.Label('No') >> update_supervisor_complete

        has_many_supervisor >> rail.Label(
            'Yes') >> add_mutiplese_supervisor_log >> update_supervisor_complete
        has_many_supervisor >> rail.Label(
            'No') >> map_supervisor_details >> can_assign_supervisor

        can_assign_supervisor >> rail.Label('Yes') >> has_supervisor_changed
        can_assign_supervisor >> rail.Label(
            'No') >> update_supervisor_complete

        has_supervisor_changed >> rail.Label(
            'Yes') >> is_supervisor_enabled
        has_supervisor_changed >> rail.Label(
            'No') >> update_supervisor_complete

        is_supervisor_enabled >> rail.Label(
            'Yes') >> get_assigned_supervisor_permissonsets >> has_manager_permission
        is_supervisor_enabled >> rail.Label(
            'No') >> enable_supervisor >> add_supervisor_disabled_log >> get_assigned_supervisor_permissonsets >> has_manager_permission

        has_manager_permission >> rail.Label(
            'Yes') >> update_supervisor_assignmentschedule_overdaterange >> update_supervisor_complete
        has_manager_permission >> rail.Label(
            'No') >> assign_manager_permission_to_supervisor >> assign_user_permission_to_supervisor >>\
            update_supervisor_assignmentschedule_overdaterange >> update_supervisor_complete

    return update_supervisor, update_supervisor_assignmentschedule_overdaterange
