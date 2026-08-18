from datetime import timedelta
from airflow.models import Variable
import rail
from terraconconsultants.user_import.utils.request_payload import get_today_date
from terraconconsultants.user_import.utils.response_filter import is_assign_supervisorpermission


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/terraconconsultants/user_import/config.py


def create_supervisor_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'terraconconsultants_userimport_child_supervisor_assignment_{config.instance}',
        description=f'TerraconConsultants Child Supervisor Assignment {config.instance}',
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
            no_task='get_supervisor_useruri_status'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_supervisor_useruri_status',
            end_task='on_error',
        )

        def get_supervisor_uri_status(response, dag_run):
            user_uri = ''
            user_status = ''
            if response['rows']:
                user_uri = rail.smartjoin_by_delim(
                    [x['cells'][0]['uri'] for x in response['rows'] if x['cells'][0]['textValue'] == dag_run.conf['supervisorid']])
                user_status = rail.smartjoin_by_delim(
                    [x['cells'][1]['textValue'].lower() for x in response['rows'] if x['cells'][0]['textValue'] == dag_run.conf['supervisorid']])
            return {
                'uri': user_uri,
                'status': user_status
            }
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
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['supervisorid']
                        }
                    }
                }
            },
            data_handler=get_supervisor_uri_status
        )

        should_update_supervisor = rail.IfOperator(
            task_id='should_update_supervisor',
            test=lambda: bool(rail.result('get_supervisor_useruri_status')['uri']) and (
                rail.result('get_supervisor_useruri_status')['status'] == 'true'),
            yes_task='get_missing_supervisor_permission',
            no_task='filter_user_logs'
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
            no_task='update_or_add_supervisor'
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('get_supervisor_useruri_status').uri }}",
                'permissionSetUri': '{{ dag_run.conf.supervisor_permissionuri }}'
            }
        )

        update_or_add_supervisor = rail.RepliconServiceOperator(
            task_id='update_or_add_supervisor',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                'userUri': dag_run.conf['enduseruri'],
                'supervisorUri': rail.result('get_supervisor_useruri_status')['uri'],
                'dateRange': {
                    'startDate': get_today_date()
                }
            } if dag_run.conf['type'] == 'Update' else {
                'userUri': dag_run.conf['enduseruri'],
                'supervisorUri': rail.result('get_supervisor_useruri_status')['uri']
            }
        )

        filter_user_logs = rail.FilterLogEntriesOperator(
            task_id='filter_user_logs',
            log='{{ dag_run.conf.user_log }}',
            properties={
                'loginname': '{{ dag_run.conf.loginname }}'
            },
            remove_filtered_entries=True
        )

        is_filtered_userlogs = rail.IfOperator(
            task_id='is_filtered_userlogs',
            test="{{ result('filter_user_logs', 'length') > 0 }}",
            yes_task='update_userlog_entries',
            no_task='on_error'
        )

        update_userlog_entries = rail.WriteLogOperator(
            task_id='update_userlog_entries',
            message='update supervisor entries',
            log='{{ dag_run.conf.user_log }}',
            items="{{ result('filter_user_logs') }}",
            properties=lambda item: {
                'loginname': item['properties']['loginname'],
                'uri': item['properties']['uri'],
                'action': item['properties']['action'],
                'status': 'Exception',
                'reason': f"{item['properties']['reason'].replace('NA', '')} Supervisor not assigned, since Supervisor is not present in Replicon"
            }
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        is_entries_present_error = rail.IfOperator(
            task_id='is_entries_present_error',
            test="{{ result('filter_user_logs', 'length') > 0 }}",
            yes_task='update_userlog_entries_error',
            no_task='dagrun_log_to_sumo'
        )

        update_userlog_entries_error = rail.WriteLogOperator(
            task_id='update_userlog_entries_error',
            message='update supervisor entries',
            log='{{ dag_run.conf.user_log }}',
            severity='Error',
            items="{{ result('filter_user_logs') }}",
            properties={
                'loginname': '{{ item.properties.loginname }}',
                'uri': '{{ item.properties.uri }}',
                'action': '{{ item.properties.action }}',
                'status': 'Error',
                'reason': "{{ item.properties.reason | replace('NA', '') }} Supervisor not assigned"
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> on_error

        can_run_batch_task >> rail.Label(
            'No') >> get_supervisor_useruri_status

        get_supervisor_useruri_status >> should_update_supervisor

        should_update_supervisor >> rail.Label(
            'Yes') >> get_missing_supervisor_permission >> should_add_missing_permissions

        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permission >> update_or_add_supervisor

        should_add_missing_permissions >> rail.Label(
            'No') >> update_or_add_supervisor

        update_or_add_supervisor >> filter_user_logs

        should_update_supervisor >> rail.Label(
            'No') >> filter_user_logs

        filter_user_logs >> is_filtered_userlogs

        is_filtered_userlogs >> rail.Label(
            'Yes') >> update_userlog_entries >> on_error

        is_filtered_userlogs >> rail.Label(
            'No') >> on_error

        on_error >> is_entries_present_error

        is_entries_present_error >> rail.Label(
            'Yes') >> update_userlog_entries_error >> dagrun_log_to_sumo

        is_entries_present_error >> rail.Label(
            'No') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_supervisor_assignment_dag)
