import rail

from lanter_delivery_systems.user_import.user_import_integration.utils.request_payload import get_data_for_supervisor_payload, get_today_date, get_replicon_date
from lanter_delivery_systems.user_import.user_import_integration.utils.response_filter import map_supervisor_list_data

def process_supervisor_assignment_task_group(user_uri, status):
    with rail.TaskGroup(group_id='process_supervisor_assignment_task', prefix_group_id=False):

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id='search_supervisor_in_replicon',
            endpoint='/services/UserListService1.svc/GetData',
            data= get_data_for_supervisor_payload,
            data_handler= map_supervisor_list_data
        )

        is_supervisor_exists = rail.IfOperator(
            task_id='is_supervisor_exists',
            test=lambda: rail.result('search_supervisor_in_replicon') != [],
            yes_task='is_supervisor_disabled',
            no_task='log_supervisor_not_present'
        )

        log_supervisor_not_present = rail.WriteLogOperator(
            task_id='log_supervisor_not_present',
            log='{{ dag_run.conf.supervisor_log }}',
            message="Supervisor not found in Replicon",
            severity='Pending',
            properties= lambda dag_run:{
                "lastname": dag_run.conf['lastname'],
                "firstname": dag_run.conf['firstname'],
                'loginname':  dag_run.conf['loginname'],
                'useruri': dag_run.conf[user_uri] if status != 'new_user' else rail.result(user_uri)['uri'],
                'supervisorusername': dag_run.conf['supervisorusername'],
                'action':'Add' if status == 'new_user' else 'Update',
                'effectivedate': dag_run.conf['todaysdate'] if status != 'new_user' else None,
                'user_log': dag_run.conf['user_log'],
                'exception_logs': rail.result('add_new_user', 'exception_logs')
                    if status == 'new_user' else rail.result('apply_user_modifications', 'exception_logs')
            },
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id='is_supervisor_disabled',
            test=lambda: rail.result('search_supervisor_in_replicon')[
                0]['status'] == 'False',
            yes_task='log_supervisor_disabled_in_replicon',
            no_task='is_new_user_supervisor_assignment'
        )

        log_supervisor_disabled_in_replicon = rail.EmptyOperator(
            task_id="log_supervisor_disabled_in_replicon"
        )

        is_new_user_supervisor_assignment = rail.IfOperator(
            task_id='is_new_user_supervisor_assignment',
            test=status == 'new_user',
            yes_task='update_supervisor_schedule_for_user',
            no_task='get_effective_supervisor_of_user'
        )

        get_effective_supervisor_of_user  = rail.RepliconServiceOperator(
            task_id="get_effective_supervisor_of_user",
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data={
                "userUri": "{{ dag_run.conf.useruri}}",
                "asOfDate": get_today_date()
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
                    "startDate": get_replicon_date(dag_run.conf['todaysdate'])
                }
            }
        )

        finish_process_supervisor = rail.EmptyOperator(
            task_id="finish_process_supervisor",
        )

        search_supervisor_in_replicon >> is_supervisor_exists >> rail.Label(
            'No') >> log_supervisor_not_present >> finish_process_supervisor
        is_supervisor_exists >> rail.Label('Yes') >>  is_supervisor_disabled >> rail.Label('No') >> is_new_user_supervisor_assignment

        is_new_user_supervisor_assignment >> rail.Label('Yes') >> update_supervisor_schedule_for_user >> finish_process_supervisor
        is_new_user_supervisor_assignment >> rail.Label('No') >> get_effective_supervisor_of_user >> is_supervisor_changed

        is_supervisor_changed >> rail.Label(
            'Yes') >> update_supervisor_schedule_for_user
        is_supervisor_changed >> rail.Label(
            'No') >> same_supervisor_already_assigned >> finish_process_supervisor
        is_supervisor_disabled >> rail.Label(
            'Yes') >> log_supervisor_disabled_in_replicon >> finish_process_supervisor

    return search_supervisor_in_replicon, finish_process_supervisor
