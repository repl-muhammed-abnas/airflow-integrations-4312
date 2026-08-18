from datetime import datetime, timedelta
import rail
from matlensilver.user_sync_integration.user_sync.utils import request_payload
from matlensilver.user_sync_integration.user_sync.utils import response_filter


def process_supervisor_assignment_task_group(user_uri, status):
    with rail.TaskGroup(group_id='process_supervisor_assignment_task', prefix_group_id=False):

        is_supervisor_same_as_user = rail.IfOperator(
            task_id='is_supervisor_same_as_user',
            test=lambda dag_run: dag_run.conf['supervisorcode'] == dag_run.conf['employeeid'],
            yes_task='log_supervisor_same_as_user',
            no_task='search_supervisor_in_replicon'
        )

        log_supervisor_same_as_user = rail.WriteLogOperator(
            task_id='log_supervisor_same_as_user',
            log="{{result('add_user_exception_log')}}" if status == 'new_user' else "{{result('update_user_exception_log')}}",
            message="Supervisor not updated  - Supervisor login name is same as User login name",
            severity='Exception',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'firstname': '{{dag_run.conf.firstname}}',
                'lastname': '{{dag_run.conf.lastname}}',
                'status': 'Exception',
            },
        )

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id='search_supervisor_in_replicon',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_data_for_supervisor_payload,
            response_filter=response_filter.map_supervisor_list_data
        )

        is_supervisor_exists = rail.IfOperator(
            task_id='is_supervisor_exists',
            test=lambda: rail.result('search_supervisor_in_replicon') != [],
            yes_task='is_supervisor_disabled',
            no_task='log_supervisor_not_present'
        )

        log_supervisor_not_present = rail.WriteLogOperator(
            task_id='log_supervisor_not_present',
            message="Supervisor not found in Replicon",
            severity='Pending',
            properties= lambda dag_run:{
                'employeeid':dag_run.conf['employeeid'],
                'firstname': '{{dag_run.conf.firstname}}',
                'lastname': '{{dag_run.conf.lastname}}',
                'useruri': dag_run.conf[user_uri] if status != 'new_user' else rail.result(user_uri)['uri'],
                'supervisorcode': dag_run.conf['supervisorcode'],
                'getuserinfo': rail.result('get_user_info')['supervisorAssignmentSchedule'] if status != 'new_user' else 'Not_required',
                'action':'Add' if status == 'new_user' else 'Update',
                'todays_date': dag_run.conf['todays_date']
            },
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id='is_supervisor_disabled',
            test=lambda: rail.result('search_supervisor_in_replicon')[
                0]['status'] == 'False',
            yes_task='enable_supervisor',
            no_task='get_missing_supervisor_permissions'
        )

        enable_supervisor = rail.RepliconServiceOperator(
            task_id='enable_supervisor',
            endpoint='/services/SecurityService1.svc/EnableLogin',
            data={
                'userUri': "{{ result('search_supervisor_in_replicon').0.uri }}"
            }
        )

        get_missing_supervisor_permissions = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                'userUri': "{{ result('search_supervisor_in_replicon').0.uri }}"
            },
            log_response=True,
            data_handler=response_filter.get_missing_permissions
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_missing_supervisor_permissions') | length > 0 }}",
            yes_task='add_missing_supervisor_permissions',
            no_task='is_supervisor_end_date_in_past'
        )

        add_missing_supervisor_permissions = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_missing_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda: rail.result('get_missing_supervisor_permissions'),
            execution_timeout=timedelta(days=14),
            data={
                'userUri': "{{ result('search_supervisor_in_replicon').0.uri }}",
                'permissionSetUri': '{{ item }}'
            }
        )

        is_supervisor_end_date_in_past = rail.IfOperator(
            task_id='is_supervisor_end_date_in_past',
            test=lambda: datetime.strptime(str((datetime.now()).strftime("%m-%d-%Y")), "%m-%d-%Y") > datetime.strptime(
                rail.result('search_supervisor_in_replicon')[0]['enddate'], "%m-%d-%Y")
            if rail.result('search_supervisor_in_replicon')[0]['enddate'] else False,
            yes_task='log_supervisor_end_date_in_past',
            no_task='is_new_user_supervisor_assignment'
        )

        is_new_user_supervisor_assignment = rail.IfOperator(
            task_id='is_new_user_supervisor_assignment',
            test=status == 'new_user',
            yes_task='update_supervisor_schedule_for_user',
            no_task='is_supervisor_changed'
        )

        log_supervisor_end_date_in_past = rail.WriteLogOperator(
            task_id='log_supervisor_end_date_in_past',
            log="{{result('add_user_exception_log')}}" if status == 'new_user' else "{{result('update_user_exception_log')}}",
            message="Supervisor end date in past",
            severity='Exception',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'firstname': '{{dag_run.conf.firstname}}',
                'lastname': '{{dag_run.conf.lastname}}',
                'status': 'Exception',
            },
        )

        is_supervisor_changed = rail.IfOperator(
            task_id='is_supervisor_changed',
            test=lambda: rail.result('search_supervisor_in_replicon')[0]['loginname'] != rail.result(
                'get_user_info')['supervisorAssignmentSchedule'][-1]['supervisor']['user']['loginName']
            if rail.result('get_user_info')['supervisorAssignmentSchedule'] else True,
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
                "userUri": dag_run.conf[user_uri] if status != 'new_user' else rail.result(user_uri)['uri'],
                "supervisorUri": rail.result('search_supervisor_in_replicon')[0]['uri'],
                "dateRange": None if status == 'new_user' else {
                    "startDate": dag_run.conf['todays_date']
                }
            }
        )

        finish_process_supervisor = rail.EmptyOperator(
            task_id="finish_process_supervisor",
        )

        is_supervisor_same_as_user >> rail.Label(
            'Yes') >> log_supervisor_same_as_user >> finish_process_supervisor
        is_supervisor_same_as_user >> rail.Label('No') >> search_supervisor_in_replicon >> is_supervisor_exists >> rail.Label(
            'No') >> log_supervisor_not_present >> finish_process_supervisor
        is_supervisor_exists >> rail.Label('Yes') >> is_supervisor_disabled >> rail.Label(
            'No') >> get_missing_supervisor_permissions
        get_missing_supervisor_permissions >> should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permissions >> is_supervisor_end_date_in_past >> rail.Label(
            'Yes') >> log_supervisor_end_date_in_past >> finish_process_supervisor
        is_supervisor_end_date_in_past >> rail.Label(
            'No') >> is_new_user_supervisor_assignment >> rail.Label(
            'Yes') >> update_supervisor_schedule_for_user >> finish_process_supervisor
        should_add_missing_permissions >> rail.Label('No') >> is_supervisor_end_date_in_past
        is_new_user_supervisor_assignment >> rail.Label('No') >> is_supervisor_changed
        is_supervisor_changed >> rail.Label(
            'Yes') >> update_supervisor_schedule_for_user
        is_supervisor_changed >> rail.Label(
            'No') >> same_supervisor_already_assigned >> finish_process_supervisor
        is_supervisor_disabled >> rail.Label(
            'Yes') >> enable_supervisor >> get_missing_supervisor_permissions

    return is_supervisor_same_as_user, finish_process_supervisor
