import rail
from pwcglobal.user_import_australia import request_payload, custom_methods


def create_assign_supervisor_task(user_uri, caller):
    with rail.TaskGroup(group_id="assign_supervisor_task", prefix_group_id=False):
        add_supervisor_end = rail.EmptyOperator(
            task_id="add_supervisor_end"
        )
        empty_check_manager_present = rail.EmptyOperator(
            task_id="empty_check_manager_present",
        )
        empty_add_supervisor_end = rail.EmptyOperator(
            task_id="empty_add_supervisor_end",
        )

        def map_supervisor_list(response, dag_run):
            data = response.json()['d']['rows']
            return list(
                filter(lambda x: x['employeeid'] == dag_run.conf['manager_id'],
                       map(lambda item:
                           {
                               'useruri': item['cells'][0]['uri'],
                               'employeeid': item['cells'][1].get('textValue', None),
                               'enabled': item['cells'][2].get('boolValue', None),
                           }, data))
            )

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id="search_supervisor_in_replicon",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_search_supervisor_payload,
            response_filter=map_supervisor_list
        )
        supervisor_uri = "{{result('search_supervisor_in_replicon')[0].useruri}}"
        check_manager_present = rail.IfOperator(
            task_id="check_manager_present",
            test="{{dag_run.conf.manager_id | is_truthy and \
                dag_run.conf.manager_id != dag_run.conf.employee_id and dag_run.conf.manager_id != dag_run.conf.guid}}",
            yes_task=search_supervisor_in_replicon.task_id,
            no_task=add_supervisor_end.task_id
        )

        def get_assigned_permissions_for_supervisor_response_filter(response):
            response = response.json()['d']
            if not response:
                return []
            return rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.uri')

        get_assigned_permissions_for_supervisor = rail.RepliconServiceOperator(
            task_id="get_assigned_permissions_for_supervisor",
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": supervisor_uri
            },
            response_filter=get_assigned_permissions_for_supervisor_response_filter
        )
        is_supervisor_present_and_enabled = rail.IfOperator(
            task_id="is_supervisor_present_and_enabled",
            test=supervisor_uri and "{{result('search_supervisor_in_replicon')[0].enabled | is_truthy}}",
            yes_task=get_assigned_permissions_for_supervisor.task_id,
            no_task=add_supervisor_end.task_id
        )

        add_record_for_supervisor_processing = rail.WriteLogOperator(
            task_id="add_record_for_supervisor_processing" if caller != "supervisor" else "log_supervisor_not_found",
            log="{{dag_run.conf.supervisor_log}}" if caller != "supervisor" else "{{dag_run.conf.log}}",
            severity="allowed_supervisor_processing",
            message="allowed for supervisor processing",
            properties={
                "guid": '{{dag_run.conf.guid}}',
                "employee_id": "{{dag_run.conf.employee_id}}",
                "firstname": "{{dag_run.conf.firstname}}",
                "lastname": "{{dag_run.conf.lastname}}",
                "action": caller,
                "status": "reprocess",
                "details": "",
                "manager_id": "{{dag_run.conf.manager_id}}",
                "processed": "yes",
                "user_uri": user_uri
            }
        )

        has_any_supervisor_found = rail.IfOperator(
            task_id="has_any_supervisor_found",
            test=lambda: len(rail.result(
                search_supervisor_in_replicon.task_id)) > 0,
            yes_task=is_supervisor_present_and_enabled.task_id,
            no_task=add_record_for_supervisor_processing.task_id
        )
        has_many_users = rail.IfOperator(
            task_id="has_many_users",
            test=lambda: len(rail.result(
                search_supervisor_in_replicon.task_id)) > 1,
            yes_task=add_supervisor_end.task_id,
            no_task=has_any_supervisor_found.task_id
        )

        is_supervisor_already_assigned = rail.IfOperator(
            task_id="is_supervisor_already_assigned",
            test=lambda: custom_methods.get_is_supervisor_already_assigned(
                caller),
            yes_task="get_manager_details" if caller != 'add' else [],
            no_task="empty_check_manager_present"
        )
        get_manager_details = rail.RepliconServiceOperator(
            task_id="get_manager_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_manager_details_payload
        )
        is_manager_changed = rail.IfOperator(
            task_id="is_manager_changed",
            test="{{dag_run.conf.manager_id != result('get_manager_details')[0].userDetails.employeeId}}",
            yes_task="empty_check_manager_present",
            no_task="empty_add_supervisor_end"
        )

        get_supervisor_permission_set = rail.RepliconServiceOperator(
            task_id="get_supervisor_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'displayText', 'Supervisor', 'uri')
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
            data={
                "userUri": user_uri,
                "supervisorUri": supervisor_uri,
                "dateRange": None
            }
        )
        has_supervisor_permission = rail.IfOperator(
            task_id="has_supervisor_permission",
            test="{{result('get_assigned_permissions_for_supervisor') | is_truthy}}",
            yes_task=update_supervisor_schedule_for_user.task_id,
            no_task=get_supervisor_permission_set.task_id
        )

        is_supervisor_already_assigned >> rail.Label(
            "No") >> empty_check_manager_present
        is_supervisor_already_assigned >> rail.Label(
            "Yes") >> get_manager_details >> is_manager_changed >> rail.Label("Yes") >> empty_check_manager_present
        is_manager_changed >> rail.Label(
            "No") >> empty_add_supervisor_end >> add_supervisor_end
        empty_check_manager_present >> check_manager_present >> rail.Label("Yes") >> search_supervisor_in_replicon >> has_many_users >> rail.Label("No")\
            >> has_any_supervisor_found >> rail.Label("Yes") >> is_supervisor_present_and_enabled >> rail.Label("Yes")\
            >> get_assigned_permissions_for_supervisor >> has_supervisor_permission >> rail.Label("Yes") >> update_supervisor_schedule_for_user
        has_supervisor_permission >> rail.Label(
            "No") >> get_supervisor_permission_set >> assign_supervisor_permissions >> update_supervisor_schedule_for_user >> add_supervisor_end
        has_many_users >> rail.Label("Yes") >> add_supervisor_end

        has_any_supervisor_found >> rail.Label(
            "No") >> add_record_for_supervisor_processing >> add_supervisor_end
        [check_manager_present,
            is_supervisor_present_and_enabled] >> rail.Label("No") >> add_supervisor_end

        return is_supervisor_already_assigned, add_supervisor_end
