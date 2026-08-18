from datetime import datetime, timedelta
import rail

from avenu.user_import.utils import request_payload
from avenu.user_import.utils import response_filter
from avenu.user_import.utils import python_callable_method


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'avenu_user_sync_process_supervisor_check_{config.instance}_child',
        description='Avenu User Sync Process Supervisor',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_supervisor_check,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id='search_supervisor_in_replicon',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_data_for_supervisor_payload,
            response_filter=response_filter.map_supervisor_list_data
        )

        def do_filter_log(log, dag_run):
            return log['properties']['employeeid'] == dag_run.conf['employeeid']

        filter_master_logs = rail.FilterLogEntriesOperator(
            task_id='filter_master_logs',
            log='{{ get_master_log()}}',
            severity='Pending_User',
            filter_callable=do_filter_log
        )

        is_supervisor_exists = rail.IfOperator(
            task_id='is_supervisor_exists',
            test=lambda: rail.result('search_supervisor_in_replicon') != [],
            yes_task='is_supervisor_disabled',
            no_task='log_supervisor_not_present'
        )

        log_supervisor_not_present = rail.EmptyOperator(
            task_id="log_supervisor_not_present",
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id='is_supervisor_disabled',
            test=lambda: rail.result('search_supervisor_in_replicon')[
                0]['status'] == 'False',
            yes_task='log_exception_to_master_log',
            no_task='is_supervisor_end_date_in_past'
        )

        log_exception_to_master_log = rail.WriteLogOperator(
            task_id='log_exception_to_master_log',
            severity='Exception',
            message='Supervisor is disabled',
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'firstname': dag_run.conf['firstname'],
                'lastname': dag_run.conf['lastname'],
                'status': 'Exception'
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
            no_task='is_new_user_supervisor_assignment'
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
            no_task='get_missing_supervisor_permissions'
        )

        is_new_user_supervisor_assignment = rail.IfOperator(
            task_id='is_new_user_supervisor_assignment',
            test=lambda dag_run: dag_run.conf['action'] == 'Add',
            yes_task='update_supervisor_schedule_for_user',
            no_task='is_supervisor_changed'
        )

        log_supervisor_end_date_in_past = rail.EmptyOperator(
            task_id="log_supervisor_end_date_in_past",
        )

        is_supervisor_changed = rail.IfOperator(
            task_id='is_supervisor_changed',
            test=lambda dag_run: rail.result('search_supervisor_in_replicon')[
                0]['loginname'] != dag_run.conf['getuserinfo']
            [-1]['supervisor']['user']['loginName']
            if dag_run.conf['getuserinfo'] else True,
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
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_supervisor_in_replicon')[0]['uri'],
                "dateRange": {
                    "startDate": request_payload.get_replicon_date(dag_run.conf['hiredate']) if dag_run.conf['action'] == 'Add' else dag_run.conf['todays_date']
                }
            }
        )

        finish_process_supervisor = rail.EmptyOperator(
            task_id="finish_process_supervisor",
        )

        has_master_log_entry = rail.IfOperator(
            task_id='has_master_log_entry',
            test="{{ result('filter_master_logs', 'length') > 0 }}",
            yes_task='get_message_from_log'
        )

        get_message_from_log = rail.PythonOperator(
            task_id='get_message_from_log',
            python_callable=python_callable_method.get_message_from_log
        )

        log_to_master_log = rail.WriteLogOperator(
            task_id='log_to_master_log',
            severity=request_payload.get_supervisor_check_severity,
            message=request_payload.get_supervisor_message_master_log,
            properties=request_payload.get_supervisor_properties
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        log_error_to_master_log = rail.WriteLogOperator(
            task_id='log_error_to_master_log',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'firstname': dag_run.conf['firstname'],
                'lastname': dag_run.conf['lastname'],
                'status': 'Error'
            }
        )

        filter_master_logs >> search_supervisor_in_replicon >> is_supervisor_exists >> rail.Label(
            'No') >> log_supervisor_not_present
        log_supervisor_not_present >> finish_process_supervisor
        is_supervisor_exists >> rail.Label('Yes') >> is_supervisor_disabled >> rail.Label(
            'No') >> is_supervisor_end_date_in_past
        get_missing_supervisor_permissions >> should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permissions >> is_new_user_supervisor_assignment
        is_supervisor_end_date_in_past >> rail.Label(
            'Yes') >> log_supervisor_end_date_in_past >> finish_process_supervisor
        is_supervisor_end_date_in_past >> rail.Label(
            'No') >> get_missing_supervisor_permissions
        is_new_user_supervisor_assignment >> rail.Label(
            'Yes') >> update_supervisor_schedule_for_user
        should_add_missing_permissions >> rail.Label(
            'No') >> is_new_user_supervisor_assignment
        is_new_user_supervisor_assignment >> rail.Label(
            'No') >> is_supervisor_changed
        is_supervisor_changed >> rail.Label(
            'Yes') >> update_supervisor_schedule_for_user >> finish_process_supervisor
        is_supervisor_changed >> rail.Label(
            'No') >> same_supervisor_already_assigned >> finish_process_supervisor
        is_supervisor_disabled >> rail.Label(
            'Yes') >> log_exception_to_master_log >> finish_process_supervisor
        finish_process_supervisor >> has_master_log_entry >> rail.Label(
            'Yes') >> get_message_from_log >> log_to_master_log >> on_error >> log_error_to_master_log

    return dag


rail.for_each_instance(create_child_dag_wbs)
