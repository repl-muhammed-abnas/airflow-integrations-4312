from datetime import datetime, timedelta
import rail

from moodys.user_sync.costa_rica_v1.utils import request_payload, response_filter


def process_supervisor_assignment_task_group(user_uri, status, config):
    with rail.TaskGroup(group_id='process_supervisor_assignment_task', prefix_group_id=False):

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id='search_supervisor_in_replicon',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_data_for_supervisor_payload,
            response_filter=response_filter.map_supervisor_list_data
        )

        is_supervisor_exists = rail.IfOperator(
            task_id='is_supervisor_exists',
            test=lambda: rail.result('search_supervisor_in_replicon') != [],
            yes_task='is_supervisor_end_date_in_past',
            no_task='log_supervisor_not_present'
        )

        log_supervisor_not_present = rail.WriteLogOperator(
            task_id='log_supervisor_not_present',
            log='{{ dag_run.conf.supervisor_log }}',
            message="Supervisor not found in Replicon",
            severity='Pending',
            properties=lambda dag_run: {
                "countryid": dag_run.conf['countryid'],
                "lastname": dag_run.conf['lastname'],
                "firstname": dag_run.conf['firstname'],
                'loginname':  dag_run.conf['loginname'],
                'useruri': dag_run.conf[user_uri] if status != 'new_user' else rail.result(user_uri)['uri'],
                'supervisorid': dag_run.conf['supervisorid'],
                'supervisorfirstname': dag_run.conf['supervisorfirstname'],
                'supervisorlastname': dag_run.conf['supervisorlastname'],
                'supervisoremailid': dag_run.conf['supervisoremailid'],
                'action': 'Add' if status == 'new_user' else 'Update',
                'effectivedate': dag_run.conf['effectivedate'],
                'user_log': dag_run.conf['user_log'],
                'timezone': dag_run.conf['timezoneuri']
            },
        )

        is_supervisor_end_date_in_past = rail.IfOperator(
            task_id='is_supervisor_end_date_in_past',
            test=lambda: datetime.now() > datetime.strptime(
                rail.result('search_supervisor_in_replicon')[0]['enddate'], "%m-%d-%Y")
            if rail.result('search_supervisor_in_replicon') and rail.result('search_supervisor_in_replicon')[0]['enddate'] else False,
            yes_task='log_supervisor_end_date_in_past',
            no_task='is_supervisor_disabled'
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id='is_supervisor_disabled',
            test=lambda: rail.result('search_supervisor_in_replicon')[
                0]['status'] == 'False',
            yes_task='log_supervisor_disabled_in_replicon',
            no_task='get_missing_supervisor_permissions'
        )

        log_supervisor_disabled_in_replicon = rail.EmptyOperator(
            task_id="log_supervisor_disabled_in_replicon"
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
            no_task='is_new_user_supervisor_assignment'
        )

        add_missing_supervisor_permissions = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_missing_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda: rail.result('get_missing_supervisor_permissions'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data={
                'userUri': "{{ result('search_supervisor_in_replicon').0.uri }}",
                'permissionSetUri': '{{ item }}'
            }
        )

        is_new_user_supervisor_assignment = rail.IfOperator(
            task_id='is_new_user_supervisor_assignment',
            test=status == 'new_user',
            yes_task='update_supervisor_schedule_for_user',
            no_task='get_effective_supervisor_of_user'
        )

        log_supervisor_end_date_in_past = rail.EmptyOperator(
            task_id="log_supervisor_end_date_in_past"
        )

        get_effective_supervisor_of_user = rail.RepliconServiceOperator(
            task_id="get_effective_supervisor_of_user",
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data={
                "userUri": "{{ dag_run.conf.useruri}}",
                "asOfDate": request_payload.get_today_date()
            }
        )

        is_supervisor_changed = rail.IfOperator(
            task_id='is_supervisor_changed',
            test=lambda: rail.result('search_supervisor_in_replicon')[0]['loginname'] != rail.result(
                'get_effective_supervisor_of_user')['supervisor']['user']['loginName']
            if rail.result('get_effective_supervisor_of_user') and rail.result('search_supervisor_in_replicon') else True,
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
                    "startDate": request_payload.get_replicon_date(dag_run.conf['effectivedate'])
                }
            }
        )

        finish_process_supervisor = rail.EmptyOperator(
            task_id="finish_process_supervisor",
        )

        search_supervisor_in_replicon >> is_supervisor_exists >> rail.Label(
            'No') >> log_supervisor_not_present >> finish_process_supervisor
        is_supervisor_exists >> rail.Label('Yes') >> is_supervisor_end_date_in_past >> rail.Label(
            'Yes') >> log_supervisor_end_date_in_past >> finish_process_supervisor
        is_supervisor_end_date_in_past >> rail.Label(
            'No') >> is_supervisor_disabled >> rail.Label('No') >> get_missing_supervisor_permissions

        get_missing_supervisor_permissions >> should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permissions >> is_new_user_supervisor_assignment >> rail.Label(
            'Yes') >> update_supervisor_schedule_for_user >> finish_process_supervisor

        should_add_missing_permissions >> rail.Label(
            'No') >> is_new_user_supervisor_assignment
        is_new_user_supervisor_assignment >> rail.Label(
            'No') >> get_effective_supervisor_of_user >> is_supervisor_changed
        is_supervisor_changed >> rail.Label(
            'Yes') >> update_supervisor_schedule_for_user
        is_supervisor_changed >> rail.Label(
            'No') >> same_supervisor_already_assigned >> finish_process_supervisor
        is_supervisor_disabled >> rail.Label(
            'Yes') >> log_supervisor_disabled_in_replicon >> finish_process_supervisor

    return search_supervisor_in_replicon, finish_process_supervisor
