import rail
from terraconconsultants.user_import.utils.request_payload import get_today_date
from terraconconsultants.user_import.utils.response_filter import get_supervisor_uri_status, is_assign_supervisorpermission


def process_supervisor_assignment_task_group(is_update_user=False):
    with rail.TaskGroup(group_id='process_supervisor_assignment_task', prefix_group_id=False):

        should_update_supervisor = rail.IfOperator(
            task_id='should_update_supervisor',
            test=lambda dag_run: dag_run.conf['supervisoremployeeid'] and 'Y' in dag_run.conf[
                'supervisor_required'],
            yes_task='get_supervisor_useruri_status',
            no_task='finish_supervisor_assignment'
        )

        get_supervisor_useruri_status = rail.RepliconServiceOperator(
            task_id='get_supervisor_useruri_status',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: {
                'page': '1',
                'pagesize': '100',
                'columnUris': [
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:login-name'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['supervisoremployeeid']
                        }
                    }
                }
            },
            data_handler=get_supervisor_uri_status
        )

        if is_update_user:
            is_supervisor_different = rail.IfOperator(
                task_id='is_supervisor_different',
                test="{{ result('get_supervisor_useruri_status').uri != result('parse_csv_user_data')['supervisoruri'] }}",
                yes_task='process_supervisor_exists',
                no_task='finish_supervisor_assignment'
            )

            process_supervisor_exists = rail.EmptyOperator(
                task_id='process_supervisor_exists'
            )

        is_supervisor_exists = rail.IfOperator(
            task_id='is_supervisor_exists',
            test="{{ result('get_supervisor_useruri_status').uri | is_truthy }}",
            yes_task='process_disablesupervisor_exists',
            no_task='log_supervisor_pending'
        )

        process_disablesupervisor_exists = rail.EmptyOperator(
            task_id='process_disablesupervisor_exists'
        )

        is_supervisor_enabled = rail.IfOperator(
            task_id='is_supervisor_enabled',
            test="{{ result('get_supervisor_useruri_status').status == 'true' }}",
            yes_task='get_missing_supervisor_permission',
            no_task='finish_supervisor_assignment'
        )

        get_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                'userUri': "{{ result('get_supervisor_useruri_status').uri }}"
            },
            data_handler=is_assign_supervisorpermission
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_missing_supervisor_permission') | is_truthy }}",
            yes_task='add_missing_supervisor_permission',
            no_task='assign_supervisor'
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('get_supervisor_useruri_status').uri }}",
                'permissionSetUri': '{{ dag_run.conf.supervisor_permissionuri }}'
            }
        )

        assign_supervisor = rail.RepliconServiceOperator(
            task_id='assign_supervisor',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'] if is_update_user else rail.result(
                    'create_user')['uri'],
                'supervisorUri': rail.result('get_supervisor_useruri_status')['uri'],
                'dateRange': {
                    'startDate': get_today_date()
                } if is_update_user else None
            }
        )

        log_supervisor_pending = rail.WriteLogOperator(
            task_id='log_supervisor_pending',
            log='{{ dag_run.conf.supervisor_log }}',
            message='Pending Supervisor Assignment',
            severity='Pending',
            properties={
                'enduseruri':  "{{ dag_run.conf.useruri }}" if is_update_user else "{{ result('create_user').uri }}",
                'supervisorid': '{{ dag_run.conf.supervisoremployeeid }}',
                'status': 'pending',
                'type': 'update' if is_update_user else 'add',
                'loginname': '{{ dag_run.conf.employeenumber }}',
                'user_log': '{{ dag_run.conf.log }}'
            }
        )

        finish_supervisor_assignment = rail.EmptyOperator(
            task_id='finish_supervisor_assignment'
        )

        should_update_supervisor >> rail.Label(
            'Yes') >> get_supervisor_useruri_status

        if is_update_user:
            get_supervisor_useruri_status >> is_supervisor_different

            is_supervisor_different >> rail.Label(
                'Yes') >> process_supervisor_exists >> is_supervisor_exists

            is_supervisor_different >> rail.Label(
                'No') >> finish_supervisor_assignment
        else:
            get_supervisor_useruri_status >> is_supervisor_exists

        is_supervisor_exists >> rail.Label(
            'Yes') >> process_disablesupervisor_exists >> is_supervisor_enabled

        is_supervisor_enabled >> rail.Label(
            'Yes') >> get_missing_supervisor_permission >> should_add_missing_permissions

        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permission >> assign_supervisor

        should_add_missing_permissions >> rail.Label(
            'No') >> assign_supervisor

        assign_supervisor >> finish_supervisor_assignment

        is_supervisor_enabled >> rail.Label(
            'No') >> finish_supervisor_assignment

        is_supervisor_exists >> rail.Label(
            'No') >> log_supervisor_pending >> finish_supervisor_assignment

        should_update_supervisor >> rail.Label(
            'No') >> finish_supervisor_assignment

    return (should_update_supervisor, finish_supervisor_assignment)
