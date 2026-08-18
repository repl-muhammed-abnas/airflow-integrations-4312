from datetime import timedelta
import rail
from dxctechnology.cwf_user_profile.user_profile_sync.utils.python_callable_method import get_activities_to_update, \
    log_supervisor, compose_supervisor_details
from dxctechnology.cwf_user_profile.user_profile_sync.utils.request_payload import get_data_for_supervisor_payload, get_today_date
from dxctechnology.cwf_user_profile.user_profile_sync.utils.response_filter import map_supervisor_list_data, get_missing_permissions


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
            op_args=['{{ dag_run.conf.manageremail }}',
                     '{{ dag_run.conf.managerid }}']
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
            yes_task='enable_supervisor',
            no_task='get_missing_supervisor_permissions'
        )

        enable_supervisor = rail.RepliconServiceOperator(
            task_id='enable_supervisor',
            endpoint='/services/SecurityService1.svc/EnableLogin',
            data={
                'userUri': "{{ result('get_matching_supervisor').uri }}"
            }
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

            is_supervisor_same = rail.IfOperator(
                task_id='is_supervisor_same',
                test=lambda dag_run: rail.result(
                    'get_supervisor_assignment_details')['supervisor']['user']['loginName'].lower() == dag_run.conf[
                        'manageremail'] if rail.result('get_supervisor_assignment_details') and rail.result(
                            'get_supervisor_assignment_details').get('supervisor', {}).get('user', {}).get('loginName') else False,
                yes_task='get_update_activities_payload',
                no_task='is_supervisor_disabled'
            )

            get_update_activities_payload = rail.PythonOperator(
                task_id='get_update_activities_payload',
                python_callable=get_activities_to_update
            )

            is_supervisor_exists >> rail.Label(
                'Yes') >> get_supervisor_assignment_details >> is_supervisor_same
            
            is_supervisor_same >> rail.Label(
                'Yes') >> get_update_activities_payload

            is_supervisor_same >> rail.Label(
                'No') >> is_supervisor_disabled

        else:

            is_supervisor_exists >> rail.Label(
                'Yes') >> is_supervisor_disabled

        is_supervisor_disabled >> rail.Label(
            'Yes') >> enable_supervisor >> get_missing_supervisor_permissions

        is_supervisor_disabled >> rail.Label(
            'No') >> get_missing_supervisor_permissions

        get_missing_supervisor_permissions >> should_add_missing_permissions

        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permissions >> assign_supervisor

        should_add_missing_permissions >> rail.Label(
            'No') >> assign_supervisor

        is_supervisor_exists >> rail.Label(
            'No') >> log_supervisor_check

    return (get_data_for_supervisor, assign_supervisor, log_supervisor_check, get_update_activities_payload) if is_update_user else (
        get_data_for_supervisor, assign_supervisor, log_supervisor_check)
