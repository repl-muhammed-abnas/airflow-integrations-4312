from datetime import timedelta, datetime
from dateutil.parser import parse as date_parser
import rail
from crl.user_import_canada_v5.utils.request_payload import get_today_date, get_replicon_date, get_date_from_replicon_date
from crl.user_import_canada_v5.utils.response_filter import get_missing_permissions

null = None
DATE_FORMAT = "%m/%d/%Y"

def process_supervisor_assignment_task_group(user_uri, status):
    with rail.TaskGroup(group_id='process_supervisor_assignment_task', prefix_group_id=False):

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id='search_supervisor_in_replicon',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": "{{ dag_run.conf.sup_emp_id }}",
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda res: {
                'uri': res[0]['userDetails']['uri'],
                'employee_id': res[0]['userDetails']['employeeId'],
                'status': res[0]['userDetails']['isEnabled'],
                'end_date': get_date_from_replicon_date(res[0]['userDetails']['employmentDateRange']['endDate']).strftime(DATE_FORMAT)
                    if res[0]['userDetails']['employmentDateRange']['endDate'] else null
            } if res else []
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
                "last_name": dag_run.conf['last_name'],
                "first_name": dag_run.conf['first_name'],
                'employee_id':  dag_run.conf['emp_id'],
                'useruri': dag_run.conf[user_uri] if status != 'new_user' else rail.result(user_uri)['uri'],
                'sup_emp_id': dag_run.conf['sup_emp_id'],
                'action': 'Add' if status == 'new_user' else ('Update' if status != 'rehire_user' else 'Rehire'),
                'effectivedate': dag_run.conf['change_effective_date'] if status != 'new_user' else None,
                'exception_logs': rail.result('apply_user_modifications', 'exception_logs') if status != 'new_user' else None,
                'user_log': dag_run.conf['user_log']
            },
        )

        is_supervisor_end_date_in_past = rail.IfOperator(
            task_id='is_supervisor_end_date_in_past',
            test=lambda: datetime.now().date() > (date_parser(rail.result('search_supervisor_in_replicon')['end_date'])).date()
            if rail.result('search_supervisor_in_replicon')['end_date'] else False,
            yes_task='log_supervisor_end_date_in_past',
            no_task='is_supervisor_disabled'
        )

        log_supervisor_end_date_in_past =rail.EmptyOperator(
            task_id="log_supervisor_end_date_in_past"
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id='is_supervisor_disabled',
            test=lambda: not rail.result('search_supervisor_in_replicon')['status'],
            yes_task='enable_supervisor',
            no_task='get_missing_supervisor_permissions'
        )

        enable_supervisor = rail.RepliconServiceOperator(
            task_id='enable_supervisor',
            endpoint='/services/SecurityService1.svc/EnableLogin',
            data={
                'userUri': "{{ result('search_supervisor_in_replicon').uri }}"
            }
        )

        get_missing_supervisor_permissions = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                'userUri': "{{ result('search_supervisor_in_replicon').uri }}"
            },
            log_response=True,
            data_handler=get_missing_permissions
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
            execution_timeout=timedelta(days=14),
            data={
                'userUri': "{{ result('search_supervisor_in_replicon').uri }}",
                'permissionSetUri': '{{ item }}'
            }
        )

        is_new_user_supervisor_assignment = rail.IfOperator(
            task_id='is_new_user_supervisor_assignment',
            test=status == 'new_user',
            yes_task='update_supervisor_schedule_for_user',
            no_task='get_effective_supervisor_of_user'
        )

        get_effective_supervisor_of_user = rail.RepliconServiceOperator(
            task_id="get_effective_supervisor_of_user",
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data={
                "userUri": "{{ dag_run.conf.useruri}}",
                "asOfDate": get_today_date()
            }
        )

        is_supervisor_changed = rail.IfOperator(
            task_id='is_supervisor_changed',
            test=lambda: rail.result('search_supervisor_in_replicon')['uri'] != rail.result(
                'get_effective_supervisor_of_user')['supervisor']['user']['uri']
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
                "supervisorUri": rail.result('search_supervisor_in_replicon')['uri'],
                "dateRange": None if status == 'new_user' else {
                    "startDate": get_replicon_date(dag_run.conf['change_effective_date'])
                        if rail.result('get_effective_supervisor_of_user') else null
                }
            }
        )

        finish_process_supervisor = rail.EmptyOperator(
            task_id="finish_process_supervisor",
        )

        search_supervisor_in_replicon >> is_supervisor_exists >> rail.Label(
            'No') >> log_supervisor_not_present >> finish_process_supervisor
        is_supervisor_exists >> rail.Label('Yes') >> is_supervisor_end_date_in_past >> rail.Label('No') >> is_supervisor_disabled

        is_supervisor_end_date_in_past >> rail.Label('Yes') >> log_supervisor_end_date_in_past >> finish_process_supervisor

        is_supervisor_disabled >> rail.Label('No') >> get_missing_supervisor_permissions
        is_supervisor_disabled >> rail.Label('Yes') >> enable_supervisor >> get_missing_supervisor_permissions

        get_missing_supervisor_permissions >> should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permissions >> is_new_user_supervisor_assignment

        should_add_missing_permissions >> rail.Label(
            'No') >> is_new_user_supervisor_assignment

        is_new_user_supervisor_assignment >> rail.Label(
            'Yes') >> update_supervisor_schedule_for_user >> finish_process_supervisor
        is_new_user_supervisor_assignment >> rail.Label(
            'No') >> get_effective_supervisor_of_user >> is_supervisor_changed

        is_supervisor_changed >> rail.Label(
            'Yes') >> update_supervisor_schedule_for_user
        is_supervisor_changed >> rail.Label(
            'No') >> same_supervisor_already_assigned >> finish_process_supervisor


    return search_supervisor_in_replicon, finish_process_supervisor
