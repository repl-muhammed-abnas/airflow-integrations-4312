from datetime import timedelta
import time
import rail
from dxctechnology.psa_user_profile_gsap.user_profile_sync.utils.python_callable_method import get_activities_to_update, \
    get_user_exception_log_message, log_supervisor, compose_supervisor_details
from dxctechnology.psa_user_profile_gsap.user_profile_sync.utils.request_payload import get_data_for_supervisor_payload, get_today_date, \
    validate_enddate_with_startdate, update_supervisor_enddate
from dxctechnology.psa_user_profile_gsap.user_profile_sync.utils.response_filter import map_supervisor_list_data, get_missing_permissions


def process_supervisor_assignment_task_group(execution_timeout_days, is_update_user=False):
    with rail.TaskGroup(group_id='process_supervisor_assignment_task', prefix_group_id=False):

        get_data_for_supervisor = rail.RepliconServiceOperator(
            task_id='get_data_for_supervisor',
            endpoint='/services/UserListService1.svc/GetData',
            data=get_data_for_supervisor_payload,
            data_handler=map_supervisor_list_data
        )

        get_matching_supervisor = rail.PythonOperator(
            task_id='get_matching_supervisor',
            python_callable=compose_supervisor_details,
            op_args=['{{ dag_run.conf.managerid }}']
        )

        is_supervisor_exists = rail.IfOperator(
            task_id='is_supervisor_exists',
            test="{{ result('get_matching_supervisor').uri | is_truthy }}",
            yes_task='get_supervisor_assignment_details' if is_update_user else 'is_supervisor_disabled',
            no_task='log_supervisor_check'
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id='is_supervisor_disabled',
            test="{{ result('get_matching_supervisor') | attr_or_default('status') == 'False' }}",
            yes_task='check_supervisor_enddate',
            no_task='supervisor_empty_operator2'
        )

        supervisor_empty_operator2 = rail.EmptyOperator(
            task_id='supervisor_empty_operator2'
        )

        check_supervisor_enddate = rail.IfOperator(
            task_id='check_supervisor_enddate',
            test=lambda: validate_enddate_with_startdate(
                rail.result('get_matching_supervisor')['enddate'], time.strftime('%Y-%m-%d'), '%Y-%m-%d') or rail.result(
                    'get_matching_supervisor')['enddate'] is None,
            yes_task='enable_supervisor',
            no_task='get_update_activities_payload' if is_update_user else 'get_exception_logs'
        )

        enable_supervisor = rail.RepliconServiceOperator(
            task_id='enable_supervisor',
            endpoint='/services/SecurityService1.svc/EnableLogin',
            data={
                'userUri': "{{ result('get_matching_supervisor').uri }}"
            }
        )

        update_enddate = rail.RepliconServiceOperator(
            task_id='update_enddate',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=lambda: update_supervisor_enddate(
                rail.result('get_matching_supervisor')['uri'], rail.result('get_matching_supervisor')['startdate'])
        )

        get_missing_supervisor_permissions = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                'userUri': "{{ result('get_matching_supervisor').uri }}"
            },
            data_handler=get_missing_permissions
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_missing_supervisor_permissions') | length > 0 }}",
            yes_task='add_missing_supervisor_permissions',
            no_task='update_supervisor_over_date_range' if is_update_user else 'assign_initial_supervisor'
        )

        add_missing_supervisor_permissions = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_missing_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda: rail.result('get_missing_supervisor_permissions'),
            execution_timeout=timedelta(days=execution_timeout_days),
            data={
                'userUri': "{{ result('get_matching_supervisor').uri }}",
                'permissionSetUri': '{{ item }}'
            }
        )

        assign_supervisor = rail.RepliconServiceOperator(
            task_id='update_supervisor_over_date_range' if is_update_user else 'assign_initial_supervisor',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda dag_run: {
                'userUri': dag_run.conf['user_uri'] if is_update_user else rail.result('create_user_in_replicon')['uri'],
                'supervisorUri': rail.result('get_matching_supervisor')['uri'],
                'dateRange': {
                    'startDate': get_today_date()
                } if is_update_user else None
            }
        )

        # only this log goes to the master table
        log_supervisor_check = rail.WriteLogOperator(
            task_id='log_supervisor_check',
            message='Pending Supervisor Check',
            severity='Pending',
            properties=lambda dag_run: log_supervisor(
                dag_run.conf['user_uri'], 'Update') if is_update_user else log_supervisor(
                    rail.result('create_user_in_replicon')['uri'], 'Add')
        )

        get_data_for_supervisor >> get_matching_supervisor >> is_supervisor_exists

        is_supervisor_same = None
        if is_update_user:

            get_supervisor_assignment_details = rail.RepliconServiceOperator(
                task_id='get_supervisor_assignment_details',
                endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
                data=lambda dag_run: {
                    'userUri': dag_run.conf['user_uri'],
                    'asOfDate': get_today_date()
                }
            )

            is_supervisor_assignment_available = rail.IfOperator(
                task_id='is_supervisor_assignment_available',
                test='{{ result("get_supervisor_assignment_details") | is_truthy }}',
                yes_task='get_supervisor_details',
                no_task='is_supervisor_disabled'
            )

            get_supervisor_details = rail.RepliconServiceOperator(
                task_id='get_supervisor_details',
                endpoint='/services/UserService1.svc/GetUserDetails',
                data=lambda dag_run: {"userUri": rail.result(
                        'get_supervisor_assignment_details')['supervisor']['user']['uri'] if (rail.result(
                            'get_supervisor_assignment_details').get('supervisor', {}).get('user', {}).get('loginName') if rail.result(
                                'get_supervisor_assignment_details') else None) else None,
                }
            )

            is_supervisor_same = rail.IfOperator(
                task_id='is_supervisor_same',
                test=lambda dag_run: rail.result(
                    'get_supervisor_details')['employeeId'].lower() == dag_run.conf[
                        'managerid'] if rail.result('get_supervisor_details') and rail.result(
                            'get_supervisor_details').get('employeeId') else False,
                yes_task='get_update_activities_payload',
                no_task='is_supervisor_disabled'
            )

            get_update_activities_payload = rail.PythonOperator(
                task_id='get_update_activities_payload',
                python_callable=get_activities_to_update
            )

            is_supervisor_exists >> rail.Label(
                'Yes') >> get_supervisor_assignment_details >> is_supervisor_assignment_available

            is_supervisor_assignment_available >> rail.Label(
                "Yes") >> get_supervisor_details >> is_supervisor_same

            is_supervisor_assignment_available >> rail.Label(
                "No") >> is_supervisor_disabled

            is_supervisor_same >> rail.Label(
                'No') >> is_supervisor_disabled
            
            check_supervisor_enddate >> rail.Label(
                'No') >> get_update_activities_payload

        else:

            get_exception_logs = rail.PythonOperator(
                task_id='get_exception_logs',
                python_callable=get_user_exception_log_message,
                op_args=['should_update_supervisor', 'check_supervisor_enddate']
            )

            is_supervisor_exists >> rail.Label(
                'Yes') >> is_supervisor_disabled
            
            check_supervisor_enddate >> rail.Label(
                'No') >> get_exception_logs

        is_supervisor_disabled >> rail.Label(
            'Yes') >> check_supervisor_enddate

        check_supervisor_enddate >> rail.Label(
            "Yes") >> enable_supervisor >> update_enddate >> get_missing_supervisor_permissions

        is_supervisor_disabled >> rail.Label(
            'No') >> supervisor_empty_operator2 >> get_missing_supervisor_permissions

        get_missing_supervisor_permissions >> should_add_missing_permissions

        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permissions >> assign_supervisor

        should_add_missing_permissions >> rail.Label(
            'No') >> assign_supervisor

        is_supervisor_exists >> rail.Label(
            'No') >> log_supervisor_check

    # pylint: disable=line-too-long
    return (get_data_for_supervisor, assign_supervisor, log_supervisor_check, is_supervisor_same, get_update_activities_payload) if is_update_user else (
        get_data_for_supervisor, assign_supervisor, log_supervisor_check, get_exception_logs)
