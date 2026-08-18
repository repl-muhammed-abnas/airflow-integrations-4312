from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils.request_payload import get_today_date
from adtalem.user_import.utils.response_filter import is_assign_supervisorpermission, map_supervisor_listdata


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_supervisor_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_user_import_supervisor_child_{config.instance}',
        description=f'Adtalem User Import Supervisor Assignment {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_data_for_supervisor'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_data_for_supervisor',
            end_task='dagrun_log_to_sumo',
        )

        get_data_for_supervisor = rail.RepliconServiceOperator(
            task_id='get_data_for_supervisor',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: {
                'page': '1',
                'pagesize': '100',
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:login-name'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['supervisorloginname'] or 'central.queue'
                        }
                    }
                }
            },
            data_handler=map_supervisor_listdata
        )

        def compose_supervisor_details(supervisor_loginname, user_uri):
            supervisor = list(filter(lambda x: x['loginname'] == supervisor_loginname or 'central.queue', rail.result(
                'get_data_for_supervisor'))) if rail.result('get_data_for_supervisor') else []
            return {
                'name': supervisor[0]['name'] if supervisor else '',
                'uri': supervisor[0]['uri'] if supervisor else '',
                'status': supervisor[0]['status'].lower() if supervisor else '',
                'userdetails_uri': user_uri
            }
        get_matching_supervisor = rail.PythonOperator(
            task_id='get_matching_supervisor',
            python_callable=compose_supervisor_details,
            op_args=['{{ dag_run.conf.supervisorloginname }}',
                     '{{ dag_run.conf.enduseruri }}']
        )

        should_update_supervisor = rail.IfOperator(
            task_id='should_update_supervisor',
            test=lambda: bool(rail.result('get_matching_supervisor')['uri']) and (
                rail.result('get_matching_supervisor')['status'] == 'true'),
            yes_task='get_missing_supervisor_permission',
            no_task='dagrun_log_to_sumo'
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
            no_task='update_or_add_supervisor'
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('get_matching_supervisor').uri }}",
                'permissionSetUri': '{{ dag_run.conf.supervisorpermissionuri }}'
            }
        )

        update_or_add_supervisor = rail.RepliconServiceOperator(
            task_id='update_or_add_supervisor',
            endpoint="\
                {%- if dag_run.conf.type == 'update' -%} \
                    /services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange \
                {%- else -%} \
                    /services/UserService1.svc/PutSupervisorAssignmentSchedule \
                {%- endif -%}",
            data=lambda dag_run: {
                'userUri': dag_run.conf['enduseruri'],
                'supervisorUri': rail.result('get_matching_supervisor')['uri'],
                'dateRange': {
                    'startDate': get_today_date()
                }
            } if dag_run.conf['type'] == 'update' else {
                'userUri': dag_run.conf['enduseruri'],
                'initialSupervisorUri': rail.result('get_matching_supervisor')['uri']
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> get_data_for_supervisor

        get_data_for_supervisor >> get_matching_supervisor >> should_update_supervisor

        should_update_supervisor >> rail.Label(
            'Yes') >> get_missing_supervisor_permission >> should_add_missing_permissions

        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permission >> update_or_add_supervisor

        should_add_missing_permissions >> rail.Label(
            'No') >> update_or_add_supervisor

        update_or_add_supervisor >> dagrun_log_to_sumo

        should_update_supervisor >> rail.Label(
            'No') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_supervisor_assignment_dag)
