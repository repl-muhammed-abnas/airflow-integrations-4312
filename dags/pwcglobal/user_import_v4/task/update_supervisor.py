import rail
from pwcglobal.user_import_v4.utils import request_payload, custom_method
from pwcglobal.user_import_v4.task.put_table_view_setting import get_put_table_view_setting


def get_update_supervisor(user_uri, can_queue_assignment=True):
    null = None
    with rail.TaskGroup(group_id='update_supervisor', prefix_group_id=False) as update_supervisor:

        has_supervisor = rail.IfOperator(
            task_id='has_supervisor',
            test=lambda:  bool(request_payload.get_conf()['supervisor']),
            yes_task='has_valid_supervisor',
            no_task='update_supervisor_complete'
        )

        has_valid_supervisor = rail.IfOperator(
            task_id='has_valid_supervisor',
            test=lambda: '||' in request_payload.get_conf()['supervisor'] and
            request_payload.get_conf()['supervisor'].split(
                '||')[0] != request_payload.get_conf()['employeeid'],
            yes_task='search_supervisor_by_partyid',
            no_task='log_invalid_supervisor'
        )

        log_invalid_supervisor = rail.WriteLogOperator(
            task_id='log_invalid_supervisor',
            log="{{ dag_run.conf.log }}",
            message='Supervisor not assigned since Supervisor Party ID and user party ID are the same',
            severity='Exception',
            properties={
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'message': 'Supervisor not assigned since Supervisor Party ID and user party ID are the same',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'action': 'Update',
                'status': 'Exception',
            }
        )

        search_supervisor_by_partyid = rail.RepliconServiceOperator(
            task_id='search_supervisor_by_partyid',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_search_user_by_partyid_param,
            response_filter=custom_method.map_supervisor_list
        )

        has_many_supervisor = rail.IfOperator(
            task_id='has_many_supervisor',
            test=lambda: len(rail.result(
                search_supervisor_by_partyid.task_id)) > 1,
            yes_task='has_supervisor_legalentity_uri',
            no_task='map_supervisor_details'
        )

        has_supervisor_legalentity_uri = rail.IfOperator(
            task_id='has_supervisor_legalentity_uri',
            test=lambda: bool(request_payload.get_conf()[
                'supervisorlegalentityuri']),
            yes_task='search_supervisor_by_legalentity',
            no_task='add_supervisor_notfound_log'
        )

        add_supervisor_notfound_log = rail.WriteLogOperator(
            task_id='add_supervisor_notfound_log',
            log="{{ dag_run.conf.log }}",
            message='Supervisor not assigned since the Supervisor not available with ID : {{ dag_run.conf.supervisor }}',
            severity='Exception',
            properties={
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'message': 'Supervisor not assigned since the Supervisor not available with ID : {{ dag_run.conf.supervisor }}',
                'status': 'Exception',
                'action': 'Update',
            }
        )

        search_supervisor_by_legalentity = rail.RepliconServiceOperator(
            task_id='search_supervisor_by_legalentity',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_search_user_by_legalentity_param,
            response_filter=custom_method.map_supervisor_by_legalentity
        )

        if_multiple_users_found = rail.IfOperator(
            task_id='if_multiple_users_found',
            test=lambda: len(rail.result('search_supervisor_by_legalentity')) > 1,
            yes_task='log_multiple_users_exception',
            no_task='map_supervisor_details'
        )

        log_multiple_users_exception = rail.WriteLogOperator(
            task_id='log_multiple_users_exception',
            log='{{ dag_run.conf.log }}',
            severity='Exception',
            message="Multiple Users found",
            properties={                
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'message': "Multiple Users found in replicon for supervisor with the same 'User Party ID' and 'Legal Entity Party ID'",
                'status': 'Exception',
                'action': 'Validation'
            }
        )

        def do_map_supervisor_details():
            if rail.result(search_supervisor_by_legalentity.task_id) and \
                    len(rail.result(search_supervisor_by_legalentity.task_id)) == 1:
                return rail.result(search_supervisor_by_legalentity.task_id)[0]
            if rail.result(search_supervisor_by_partyid.task_id) and \
                    len(rail.result(search_supervisor_by_partyid.task_id)) == 1:
                return rail.result(search_supervisor_by_partyid.task_id)[0]
            return None

        map_supervisor_details = rail.PythonOperator(
            task_id='map_supervisor_details',
            python_callable=do_map_supervisor_details
        )

        can_assign_supervisor = rail.IfOperator(
            task_id='can_assign_supervisor',
            test=lambda: rail.result(map_supervisor_details.task_id) and
            rail.result(map_supervisor_details.task_id)['useruri'] and
            rail.result(map_supervisor_details.task_id)['enabled'],
            yes_task='has_supervisor_changed',
            no_task='can_queue_supervisor_assignment'
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
            no_task='add_supervisor_disabled_log'

        )

        add_supervisor_disabled_log = rail.WriteLogOperator(
            task_id='add_supervisor_disabled_log',
            log="{{ dag_run.conf.log }}",
            message='Supervisor not assigned since the Supervisor with ID : {{ dag_run.conf.supervisor }} is disabled',
            severity='Exception',
            properties={
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'message': 'Supervisor not assigned since the Supervisor with ID : {{ dag_run.conf.supervisor }} is disabled',
                'status': 'Exception',
                'action': 'Update',
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
                'get_assigned_supervisor_permissonsets'), 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.name')),
            yes_task='update_supervisor_assignmentschedule_overdaterange',
            no_task='assign_manager_permission_to_supervisor'
        )

        assign_manager_permission_to_supervisor = rail.RepliconServiceOperator(
            task_id='assign_manager_permission_to_supervisor',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                "userUri": "{{ result('map_supervisor_details').useruri }}",
                "permissionSetUri": "{{ dag_run.conf.managerpermissionuri }}"
            }
        )

        put_tableview_settings = get_put_table_view_setting(user_uri)

        update_supervisor_assignmentschedule_overdaterange = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignmentschedule_overdaterange',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda: {
                "userUri": request_payload.get_user_uri() or request_payload.get_created_user_uri(),
                "supervisorUri": rail.result('map_supervisor_details')['useruri'],
                "dateRange": {
                    "startDate": request_payload.get_today_date() if rail.result('bulk_get_user3') else null,
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        can_queue_supervisor_assignment = rail.IfOperator(
            task_id='can_queue_supervisor_assignment',
            test=lambda: can_queue_assignment,
            yes_task='queue_supervisor_assignment',
            no_task='add_supervisor_notfound_log'

        )

        queue_supervisor_assignment = rail.PythonOperator(
            task_id='queue_supervisor_assignment',
            python_callable=lambda: {
                "useruri": request_payload.get_user_uri() or request_payload.get_created_user_uri(),
                "employeeid": request_payload.get_conf()['employeeid'],
                'firstname': request_payload.get_conf()['firstname'],
                'lastname': request_payload.get_conf()['lastname'],
                'legalentity': request_payload.get_conf()['legalentity'],
                'supervisor': request_payload.get_conf()['supervisor'],
                'supervisorlegalentityuri': request_payload.get_conf()['supervisorlegalentityuri'],
                'managerpermissionuri': request_payload.get_conf()['managerpermissionuri'],
                'log': request_payload.get_conf()['log'],
            }
        )

        update_supervisor_complete = rail.EmptyOperator(
            task_id='update_supervisor_complete'
        )

        has_supervisor >> rail.Label('Yes') >> has_valid_supervisor
        has_valid_supervisor >> rail.Label(
            'Yes') >> search_supervisor_by_partyid
        has_valid_supervisor >> rail.Label(
            'No') >> log_invalid_supervisor >> update_supervisor_complete
        search_supervisor_by_partyid >> has_many_supervisor
        has_many_supervisor >> rail.Label(
            'Yes') >> has_supervisor_legalentity_uri
        has_many_supervisor >> rail.Label(
            'No') >> map_supervisor_details >> can_assign_supervisor

        has_supervisor_legalentity_uri >> rail.Label('Yes') >> search_supervisor_by_legalentity >> \
            if_multiple_users_found >> rail.Label("Yes") >> log_multiple_users_exception >> update_supervisor_complete

        if_multiple_users_found >> rail.Label("No") >> map_supervisor_details >> can_assign_supervisor
        has_supervisor_legalentity_uri >> rail.Label(
            'No') >> add_supervisor_notfound_log >> update_supervisor_complete

        can_assign_supervisor >> rail.Label('Yes') >> has_supervisor_changed
        can_assign_supervisor >> rail.Label(
            'No') >> can_queue_supervisor_assignment
        can_queue_supervisor_assignment >> rail.Label(
            'yes') >> queue_supervisor_assignment >> update_supervisor_complete
        can_queue_supervisor_assignment >> rail.Label(
            'no') >> add_supervisor_notfound_log >> update_supervisor_complete

        has_supervisor_changed >> rail.Label(
            'Yes') >> is_supervisor_enabled
        has_supervisor_changed >> rail.Label(
            'No') >> update_supervisor_complete

        is_supervisor_enabled >> rail.Label(
            'Yes') >> get_assigned_supervisor_permissonsets >> has_manager_permission
        is_supervisor_enabled >> rail.Label(
            'No') >> add_supervisor_disabled_log >> update_supervisor_complete

        has_manager_permission >> rail.Label(
            'Yes') >> update_supervisor_assignmentschedule_overdaterange >> update_supervisor_complete
        has_manager_permission >> rail.Label('No') >> assign_manager_permission_to_supervisor >> \
            put_tableview_settings >> update_supervisor_assignmentschedule_overdaterange >> update_supervisor_complete

        has_supervisor >> rail.Label('No') >> update_supervisor_complete

    return update_supervisor, update_supervisor_assignmentschedule_overdaterange
