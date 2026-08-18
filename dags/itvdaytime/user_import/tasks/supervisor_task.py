import rail
from itvdaytime.user_import.utils import data_handler, request_payload
from itvdaytime.user_import.utils.custom_methods import get_today_date
execution_timeout_days = 10


def get_supervisor_task(user_uri, is_update_user=True, caller=None):
    with rail.TaskGroup(group_id="add_update_supervisor", prefix_group_id=False):

        is_line_manager_parent = rail.IfOperator(
            task_id="is_line_manager_parent",
            test="{{dag_run.conf.line_manager | is_truthy}}",
            yes_task="is_manager_employee_id_same",
            no_task="assign_supervisor_end"
        )

        is_manager_employee_id_same = rail.IfOperator(
            task_id="is_manager_employee_id_same",
            test="{{ dag_run.conf.employee_number == dag_run.conf.line_manager }}",
            yes_task="log_employee_and_line_manager_same",
            no_task="search_supervisor"
        )

        log_employee_and_line_manager_same = rail.WriteLogOperator(
            task_id="log_employee_and_line_manager_same",
            severity="Exception",
            message="Employee id {{dag_run.conf.employee_number}} and manager id {{dag_run.conf.line_manager}} are same",
            properties={
                "employee_number": "{{dag_run.conf.employee_number}}",
                "loginname": "{{dag_run.conf.first_name}}" + '.' + "{{dag_run.conf.last_name}}" if not caller else "{{dag_run.conf.loginname}}",
                "status": "Exception",
                "action": "Update" if is_update_user else "Add",
                "details": "Employee id {{dag_run.conf.employee_number}} and manager id {{dag_run.conf.line_manager}} are same",
                "line_manager": "{{dag_run.conf.line_manager}}",
                "user_uri": "{{dag_run.conf.user_uri}}",
                "allowed_for_supervisor_processing": "No"
            }
        )

        search_supervisor = rail.RepliconServiceOperator(
            task_id="search_supervisor",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: request_payload.get_search_user_payload(
                dag_run, is_supervisor=True),
            data_handler=lambda response, dag_run: data_handler.get_search_user_data_handler(
                response, dag_run, "supervisor")
        )

        is_supervisor_found = rail.IfOperator(
            task_id="is_supervisor_found",
            test=lambda: len(rail.result('search_supervisor')) > 0,
            yes_task="has_multiple_supervisor_found",
            no_task="log_for_supervisor_processing"
        )

        log_for_supervisor_processing = rail.WriteLogOperator(
            task_id="log_for_supervisor_processing",
            severity="pending",
            log="{{dag_run.conf.log}}" if not caller else None,
            message="allowed for supervisor processing",
            properties={
                "employee_number": "{{dag_run.conf.employee_number}}",
                "loginname": "{{dag_run.conf.first_name}}" + '.' + "{{dag_run.conf.last_name}}" if not caller else "{{dag_run.conf.loginname}}",
                "status": "Exception",
                "action": "Update" if is_update_user else "Add",
                "details": "Supervisor not found {{dag_run.conf.line_manager}}",
                "line_manager": "{{dag_run.conf.line_manager}}",
                "user_uri": "{{dag_run.conf.user_uri}}" if is_update_user else user_uri,
                "allowed_for_supervisor_processing": "Yes" if not caller else "No"
            }
        )

        has_multiple_supervisor_found = rail.IfOperator(
            task_id="has_multiple_supervisor_found",
            test=lambda: len(rail.result('search_supervisor')) != 1,
            yes_task="log_multiple_supervisor_found",
            no_task="is_supervisor_disabled"
        )

        log_multiple_supervisor_found = rail.WriteLogOperator(
            task_id="log_multiple_supervisor_found",
            severity="process",
            message="Multiple supervisor found for same id: {{dag_run.conf.line_manager}}",
            properties={
                "employee_number": "{{dag_run.conf.employee_number}}",
                "loginname": "{{dag_run.conf.first_name}}" + '.' + "{{dag_run.conf.last_name}}" if not caller else "{{dag_run.conf.loginname}}",
                "status": "Exception",
                "action": "Update" if is_update_user else "Add",
                "details": "Multiple supervisor found for same id: {{dag_run.conf.line_manager}}",
                "line_manager": "{{dag_run.conf.line_manager}}",
                "user_uri": "{{dag_run.conf.user_uri}}" if is_update_user else user_uri,
                "allowed_for_supervisor_processing": "No"
            }
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id="is_supervisor_disabled",
            test=lambda: rail.result('search_supervisor')[
                0]['status'].lower() != 'true',
            yes_task="log_supervisor_disabled",
            no_task="get_missing_supervisor_permissions"
        )

        log_supervisor_disabled = rail.WriteLogOperator(
            task_id="log_supervisor_disabled",
            severity="process",
            message="Supervisor is in disabled state",
            properties={
                "employee_number": "{{dag_run.conf.employee_number}}",
                "loginname": "{{dag_run.conf.first_name}}" + '.' + "{{dag_run.conf.last_name}}" if not caller else "{{dag_run.conf.loginname}}",
                "status": "Exception",
                "action": "Update" if is_update_user else "Add",
                "details": "Supervisor is in disabled state: {{dag_run.conf.line_manager}}",
                "line_manager": "{{dag_run.conf.line_manager}}",
                "user_uri": "{{dag_run.conf.user_uri}}" if is_update_user else user_uri,
                "allowed_for_supervisor_processing": "No"
            }
        )

        def get_assigned_permissions_for_supervisor_response_filter(response):
            response = response.json()['d']
            if not response:
                return []
            return rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.uri')

        get_missing_supervisor_permissions = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                'userUri': "{{ result('search_supervisor')[0].user_uri }}"
            },
            response_filter=get_assigned_permissions_for_supervisor_response_filter
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_missing_supervisor_permissions') | is_falsy }}",
            yes_task='add_missing_supervisor_permissions',
            no_task='update_supervisor_schedule_for_user'
        )

        add_missing_supervisor_permissions = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=lambda dag_run: {
                'userUri': rail.result('search_supervisor')[0]['user_uri'],
                'permissionSetUri': rail.find_first_by_attr_and_get_attr(dag_run.conf['permission_sets'], 'name', 'Supervisor', 'uri')
            }
        )

        update_supervisor_schedule_for_user = rail.RepliconServiceOperator(
            task_id="update_supervisor_schedule_for_user",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": user_uri if not caller else '{{dag_run.conf.user_uri}}',
                "supervisorUri": "{{ result('search_supervisor')[0].user_uri }}",
                "dateRange": {
                    'startDate': get_today_date()
                } if is_update_user else None
            }
        )

        log_supervisor_assigned = rail.WriteLogOperator(
            task_id="log_supervisor_assigned",
            severity="process",
            message="Supervisor updated successfully",
            properties={
                "employee_number": "{{dag_run.conf.employee_number}}",
                "loginname": "{{dag_run.conf.first_name}}" + '.' + "{{dag_run.conf.last_name}}" if not caller else "{{dag_run.conf.loginname}}",
                "status": "",
                "action": "Update" if is_update_user else "Add",
                "details": "Supervisor assigned",
                "line_manager": "{{dag_run.conf.line_manager}}",
                "user_uri": "{{dag_run.conf.user_uri}}" if is_update_user else user_uri,
                "allowed_for_supervisor_processing": "No"
            }
        )

        assign_supervisor_end = rail.EmptyOperator(
            task_id="assign_supervisor_end"
        )

        is_line_manager_parent >> is_manager_employee_id_same >> rail.Label("NO") >> search_supervisor >> is_supervisor_found\
            >> rail.Label("Yes") >> has_multiple_supervisor_found >> rail.Label("No") >> is_supervisor_disabled >> rail.Label("No")\
            >> get_missing_supervisor_permissions
        get_missing_supervisor_permissions >> should_add_missing_permissions\
            >> rail.Label("Yes") >> add_missing_supervisor_permissions >> update_supervisor_schedule_for_user\
            >> log_supervisor_assigned >> assign_supervisor_end

        is_supervisor_disabled >> rail.Label(
            "Yes") >> log_supervisor_disabled >> assign_supervisor_end
        is_line_manager_parent >> rail.Label("No") >> assign_supervisor_end
        is_manager_employee_id_same >> rail.Label(
            "Yes") >> log_employee_and_line_manager_same >> assign_supervisor_end
        is_supervisor_found >> rail.Label(
            "No") >> log_for_supervisor_processing >> assign_supervisor_end
        has_multiple_supervisor_found >> rail.Label(
            "Yes") >> log_multiple_supervisor_found >> assign_supervisor_end
        should_add_missing_permissions >> rail.Label(
            "No") >> update_supervisor_schedule_for_user

        return is_line_manager_parent, assign_supervisor_end
