import rail
from deltek_northstar.user_sync_polaris_philippines_v1.utils.request_payload import get_supervisor_data_payload, get_today_date
from deltek_northstar.user_sync_polaris_philippines_v1.utils.response_filter import map_supervisor_list_data, is_assign_supervisorpermission

def process_supervisor_assignment_task_group(user_uri, status, config):
    with rail.TaskGroup(group_id='process_supervisor_assignment_task', prefix_group_id=False):

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id='search_supervisor_in_replicon',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=get_supervisor_data_payload,
            data_handler=map_supervisor_list_data
        )

        is_supervisor_exists = rail.IfOperator(
            task_id='is_supervisor_exists',
            test=lambda: bool(rail.result('search_supervisor_in_replicon')),
            yes_task='is_supervisor_disabled',
            no_task='log_supervisor_not_present'
        )

        log_supervisor_not_present = rail.WriteLogOperator(
            task_id='log_supervisor_not_present',
            log='{{ dag_run.conf.supervisor_log }}',
            message="Supervisor not found in Replicon",
            severity='Pending',
            properties= lambda dag_run:{
                'lastname': dag_run.conf['last_name'],
                'firstname': dag_run.conf['first_name'],
                'loginname':  dag_run.conf['email_id'],
                'employeeid': dag_run.conf['empl_id'],
                'effectivedate': dag_run.conf['effect_date'],
                'useruri': dag_run.conf[user_uri] if status != 'new_user' else rail.result(user_uri)['user']['uri'],
                'manager': dag_run.conf['mgr_empl_id'],
                'action':'Add' if status == "new_user" else 'Update',
                'status':'Pending',
                'details':'Supervisor not found in Replicon'
            }
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id='is_supervisor_disabled',
            test=lambda: not rail.result('search_supervisor_in_replicon')['status'],
            yes_task='log_supervisor_disabled_in_replicon',
            no_task='get_missing_supervisor_permission'
        )

        log_supervisor_disabled_in_replicon = rail.WriteLogOperator(
            task_id='log_supervisor_disabled_in_replicon',
            log='{{ dag_run.conf.user_log }}',
            message="Supervisor present but disabled",
            severity='Exception',
            properties= lambda dag_run:{
                'lastname': dag_run.conf['last_name'],
                'firstname': dag_run.conf['first_name'],
                'loginname':  dag_run.conf['email_id'],
                'employeeid': dag_run.conf['empl_id'],
                'effectivedate': dag_run.conf['effect_date'],
                'useruri': dag_run.conf[user_uri] if status != 'new_user' else rail.result(user_uri)['user']['uri'],
                'manager': dag_run.conf['mgr_empl_id'],
                'action': 'Add',
                'status': 'Exception',
                'details':'Supervisor present but disabled'
            }
        )

        get_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data=lambda: {
                'userUri': rail.result('search_supervisor_in_replicon')['uri']
            },
            data_handler=is_assign_supervisorpermission
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
            test=status == 'new_user',
            yes_task='update_supervisor_for_user',
            no_task='get_effective_supervisor_of_user'
        )

        get_effective_supervisor_of_user  = rail.RepliconServiceOperator(
            task_id="get_effective_supervisor_of_user",
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data=lambda dag_run: {
                "userUri": dag_run.conf[user_uri] if status != 'new_user' else rail.result(user_uri)['user']['uri'],
                "asOfDate": get_today_date(config)
            }
        )

        is_supervisor_changed = rail.IfOperator(
            task_id='is_supervisor_changed',
            test=lambda: rail.result('search_supervisor_in_replicon')['loginname'] != rail.result(
                'get_effective_supervisor_of_user')['supervisor']['user']['loginName']
            if rail.result('get_effective_supervisor_of_user') and rail.result('search_supervisor_in_replicon') else True,
            yes_task='update_supervisor_for_user',
            no_task='same_supervisor_already_assigned'
        )

        same_supervisor_already_assigned = rail.EmptyOperator(
            task_id="same_supervisor_already_assigned",
        )

        update_supervisor_for_user = rail.RepliconServiceOperator(
            task_id="update_supervisor_for_user",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf[user_uri] if status != 'new_user' else rail.result(user_uri)['user']['uri'],
                "supervisorUri": rail.result('search_supervisor_in_replicon')['uri'],
                "dateRange": None if status == 'new_user' else {
                    "startDate": get_today_date(config)
                }
            }
        )

        finish_process_supervisor = rail.EmptyOperator(
            task_id="finish_process_supervisor",
        )

        search_supervisor_in_replicon >> is_supervisor_exists >> rail.Label(
            'No') >> log_supervisor_not_present >> finish_process_supervisor
        is_supervisor_exists >> rail.Label('Yes') >>  is_supervisor_disabled >> rail.Label('No') >> get_missing_supervisor_permission

        get_missing_supervisor_permission >> should_add_missing_permissions >> rail.Label('Yes') >> add_missing_supervisor_permission
        should_add_missing_permissions >> rail.Label('No') >> is_new_user_supervisor_assignment

        add_missing_supervisor_permission >> is_new_user_supervisor_assignment >> rail.Label('Yes') >> update_supervisor_for_user
        is_new_user_supervisor_assignment >> rail.Label('No') >> get_effective_supervisor_of_user >> is_supervisor_changed

        is_supervisor_changed >> rail.Label(
            'Yes') >> update_supervisor_for_user >> finish_process_supervisor
        is_supervisor_changed >> rail.Label(
            'No') >> same_supervisor_already_assigned >> finish_process_supervisor
        is_supervisor_disabled >> rail.Label(
            'Yes') >> log_supervisor_disabled_in_replicon >> finish_process_supervisor
        # search_supervisor_in_replicon >> finish_process_supervisor

    return search_supervisor_in_replicon, finish_process_supervisor
