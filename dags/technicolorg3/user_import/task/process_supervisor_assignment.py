import rail
from technicolorg3.user_import.utils.python_callable_method import compose_supervisor_details
from technicolorg3.user_import.utils.request_payload import get_listdata_for_supervisor, get_today_date
from technicolorg3.user_import.utils.response_filter import is_assign_supervisorpermission, map_supervisor_listdata


def process_supervisor_assignment_task_group(is_update_user=False):
    with rail.TaskGroup(group_id='process_supervisor_assignment_task', prefix_group_id=False):

        should_update_supervisor = rail.IfOperator(
            task_id='should_update_supervisor',
            test=lambda dag_run: dag_run.conf['managerid'] != dag_run.conf['globalid'],
            yes_task='get_data_for_supervisor',
            no_task='finish_supervisor_assignment'
        )

        get_data_for_supervisor = rail.RepliconServiceOperator(
            task_id='get_data_for_supervisor',
            endpoint='/services/UserListService1.svc/GetData',
            data=get_listdata_for_supervisor,
            data_handler=map_supervisor_listdata
        )

        get_matching_supervisor = rail.PythonOperator(
            task_id='get_matching_supervisor',
            python_callable=compose_supervisor_details,
            op_args=['{{ dag_run.conf.managerid }}',
                     is_update_user, '{{ dag_run.conf.useruri }}']
        )

        is_supervisor_exists = rail.IfOperator(
            task_id='is_supervisor_exists',
            test="{{ result('get_matching_supervisor').uri | is_truthy }}",
            yes_task='get_supervisor_assignment_details' if is_update_user else 'process_multiple_supervisorcheck',
            no_task='log_supervisor_pending'
        )

        process_multiple_supervisorcheck = rail.EmptyOperator(
            task_id='process_multiple_supervisorcheck'
        )

        is_single_supervisor = rail.IfOperator(
            task_id='is_single_supervisor',
            test="{{ result('get_data_for_supervisor') | filter_by_attr('employeeid', 'equals', dag_run.conf.managerid) | length == 1 }}",
            yes_task='process_disable_supervisorcheck',
            no_task='finish_supervisor_assignment'
        )

        process_disable_supervisorcheck = rail.EmptyOperator(
            task_id='process_disable_supervisorcheck'
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id='is_supervisor_disabled',
            test=lambda: rail.result('get_matching_supervisor')[
                'status'] != 'true',
            yes_task='log_supervisor_pending_disabled',
            no_task='get_missing_supervisor_permission'
        )

        get_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                'userUri': "{{ result('get_matching_supervisor').uri }}"
            },
            data_handler=is_assign_supervisorpermission
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_missing_supervisor_permission') | is_truthy }}",
            yes_task='add_missing_supervisor_permission',
            no_task='update_supervisor_over_date_range' if is_update_user else 'assign_initial_supervisor'
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('get_matching_supervisor').uri }}",
                'permissionSetUri': '{{ dag_run.conf.supervisor_permission }}'
            }
        )

        assign_supervisor = rail.RepliconServiceOperator(
            task_id='update_supervisor_over_date_range' if is_update_user else 'assign_initial_supervisor',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda: {
                'userUri': rail.result('get_matching_supervisor')['userdetails_uri'],
                'supervisorUri': rail.result('get_matching_supervisor')['uri'],
                'dateRange': {
                    'startDate': get_today_date()
                } if is_update_user else None
            }
        )

        log_supervisor_pending = rail.WriteLogOperator(
            task_id='log_supervisor_pending',
            log='{{ dag_run.conf.supervisor_log }}',
            message='Pending Supervisor Assignment',
            severity='Queued',
            properties={
                'username': '{{ dag_run.conf.globalid }}',
                'useruri':  "{{ result('get_matching_supervisor').userdetails_uri }}",
                'supervisorloginname': '{{ dag_run.conf.managerid }}',
                'action': 'Update' if is_update_user else 'Add',
                'status': 'Queued',
                'user_log': "{{ result('create_user_log') }}"
            }
        )

        log_supervisor_pending_disabled = rail.WriteLogOperator(
            task_id='log_supervisor_pending_disabled',
            log='{{ dag_run.conf.supervisor_log }}',
            message='Pending Supervisor Assignment',
            severity='Queued',
            properties={
                'username': '{{ dag_run.conf.globalid }}',
                'useruri':  "{{ result('get_matching_supervisor').userdetails_uri }}",
                'supervisorloginname': '{{ dag_run.conf.managerid }}',
                'action': 'Update' if is_update_user else 'Add',
                'status': 'Queued',
                'user_log': "{{ result('create_user_log') }}"
            }
        )

        finish_supervisor_assignment = rail.EmptyOperator(
            task_id='finish_supervisor_assignment'
        )

        should_update_supervisor >> rail.Label(
            'Yes') >> get_data_for_supervisor >> get_matching_supervisor >> is_supervisor_exists

        if is_update_user:

            get_supervisor_assignment_details = rail.RepliconServiceOperator(
                task_id='get_supervisor_assignment_details',
                endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
                data=lambda dag_run: {
                    'userUri': dag_run.conf['useruri'],
                    'asOfDate': get_today_date()
                }
            )

            is_supervisor_same = rail.IfOperator(
                task_id='is_supervisor_same',
                test=lambda: rail.result(
                    'get_supervisor_assignment_details')['supervisor']['user']['loginName'].lower() == rail.result(
                        'get_matching_supervisor')['loginname'] if rail.result('get_supervisor_assignment_details') and rail.result(
                            'get_supervisor_assignment_details').get('supervisor', {}).get('user', {}).get('loginName') else False,
                yes_task='finish_supervisor_assignment',
                no_task='process_multiple_supervisorcheck'
            )

            is_supervisor_exists >> rail.Label(
                'Yes') >> get_supervisor_assignment_details >> is_supervisor_same

            is_supervisor_same >> rail.Label(
                'Yes') >> finish_supervisor_assignment

            is_supervisor_same >> rail.Label(
                'No') >> process_multiple_supervisorcheck
        else:
            is_supervisor_exists >> rail.Label(
                'Yes') >> process_multiple_supervisorcheck

        process_multiple_supervisorcheck >> is_single_supervisor

        is_single_supervisor >> rail.Label(
            'Yes') >> process_disable_supervisorcheck >> is_supervisor_disabled

        is_supervisor_disabled >> rail.Label(
            'Yes') >> log_supervisor_pending_disabled >> finish_supervisor_assignment

        is_supervisor_disabled >> rail.Label(
            'No') >> get_missing_supervisor_permission >> should_add_missing_permissions

        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permission >> assign_supervisor

        should_add_missing_permissions >> rail.Label(
            'No') >> assign_supervisor >> finish_supervisor_assignment

        is_single_supervisor >> rail.Label(
            'No') >> finish_supervisor_assignment

        is_supervisor_exists >> rail.Label(
            'No') >> log_supervisor_pending >> finish_supervisor_assignment

        should_update_supervisor >> rail.Label(
            'No') >> finish_supervisor_assignment

    return (should_update_supervisor, finish_supervisor_assignment)
