from datetime import timedelta

from airflow.models import Variable
import rail

from valleychildrens.user_import.utils import request_payload, response_filter

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_supervisor_assignment_dagid,
        description='ValleyChildrens User Import - Process Supervisor Assignment',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_supervisor_assignment,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id='view_dagrun_conf')
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_supervisor',
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_supervisor',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_supervisor = rail.RepliconServiceOperator(
            task_id='get_supervisor',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda dag_run: {
                'users': [{
                    'employeeId': dag_run.conf.get('supid'),
                    'loginName': null,
                    'uri': null,
                    'parameterCorrelationId': null,
                }],
                'dataLoadOptionUri': 'urn:replicon:data-load-option:omit-data-if-insufficient-access-permission',
            },
            data_handler=response_filter.first_or_none,
        )

        has_supervisor = rail.IfOperator(
            task_id='has_supervisor',
            test=lambda: bool(rail.result('get_supervisor')),
            yes_task='get_existing_supervisor_assignment',
            no_task='log_supervisor_not_found',
        )

        log_supervisor_not_found = rail.WriteLogOperator(
            task_id='log_supervisor_not_found',
            log='{{ dag_run.conf["supervisor_log_id"] }}',
            severity='Exception',
            message=lambda dag_run: f"Supervisor {dag_run.conf.get('supid')} not found for employee {dag_run.conf['employeeid']}",
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['employeeid'],
                'sup_id': dag_run.conf.get('supid'),
                'action': 'SupervisorAssignment',
                'status': 'Exception',
                'details': f"Supervisor {dag_run.conf.get('supid')} not found in Replicon",
            },
        )

        get_existing_supervisor_assignment = rail.RepliconServiceOperator(
            task_id='get_existing_supervisor_assignment',
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data=lambda dag_run: {
                'user': {'uri': dag_run.conf['useruri']},
            },
        )

        is_first_assignment = rail.IfOperator(
            task_id='is_first_assignment',
            test=lambda: not bool(rail.result('get_existing_supervisor_assignment')),
            yes_task='assign_manager_permission_to_supervisor',
            no_task='update_existing_supervisor_assignment',
        )

        assign_manager_permission_to_supervisor = rail.RepliconServiceOperator(
            task_id='assign_manager_permission_to_supervisor',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=lambda dag_run: {
                'userUri': rail.result('get_supervisor')['userDetails']['uri'],
                'permissionSetUri': dag_run.conf['supervisorpermissionuri'],
            },
        )

        assign_user_permission = rail.RepliconServiceOperator(
            task_id='assign_user_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'permissionSetUri': dag_run.conf.get('userpermissionuri'),
            },
        )

        put_initial_supervisor = rail.RepliconServiceOperator(
            task_id='put_initial_supervisor',
            endpoint='/services/UserService1.svc/PutSupervisorAssignmentSchedule',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'supervisorUri': rail.result('get_supervisor')['userDetails']['uri'],
                'dateRange': null,
            },
        )

        update_existing_supervisor_assignment = rail.RepliconServiceOperator(
            task_id='update_existing_supervisor_assignment',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'supervisorUri': rail.result('get_supervisor')['userDetails']['uri'],
                'dateRange': {'startDate': request_payload.today_date_struct(config.pacific_timezone)},
            },
        )

        log_success = rail.WriteLogOperator(
            task_id='log_success',
            log='{{ dag_run.conf["supervisor_log_id"] }}',
            severity='Info',
            message=lambda dag_run: f"Supervisor {dag_run.conf.get('supid')} assigned to employee {dag_run.conf['employeeid']}",
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['employeeid'],
                'sup_id': dag_run.conf.get('supid'),
                'action': 'SupervisorAssignment',
                'status': 'Success',
                'details': 'Supervisor assigned',
            },
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log='{{ dag_run.conf["supervisor_log_id"] }}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['employeeid'],
                'sup_id': dag_run.conf.get('supid'),
                'action': 'SupervisorAssignment',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
            },
        )
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_supervisor
        get_supervisor >> has_supervisor
        has_supervisor >> rail.Label('Yes') >> get_existing_supervisor_assignment >> is_first_assignment
        has_supervisor >> rail.Label('No') >> log_supervisor_not_found
        is_first_assignment >> rail.Label('Yes') >> assign_manager_permission_to_supervisor \
            >> assign_user_permission >> put_initial_supervisor >> log_success
        is_first_assignment >> rail.Label('No') >> update_existing_supervisor_assignment >> log_success
        log_success >> catch_and_log_error
    return dag

rail.for_each_instance(create_child_dag)
