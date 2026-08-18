from datetime import timedelta
import rail
from airflow.models import Variable
from technicolorg3.user_import.utils.python_callable_method import get_downstreamtasks_error, get_supervisor_exception_message
from technicolorg3.user_import.utils.request_payload import get_today_date
from technicolorg3.user_import.utils.response_filter import is_assign_supervisorpermission, map_supervisor_listdata


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/technicolorg3/user_import/config.py


# pylint:disable=too-many-statements
def create_supervisor_assignment_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_user_import_supervisor_child_{config.instance}',
        description=f'Technicolor_Child_Add Supervisor {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_supervisor_assignment,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        supervisor_logtable_entries = (
            'username', 'useruri', 'supervisorloginname', 'action')

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
            end_task='on_error',
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
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['supervisorloginname']
                        }
                    }
                }
            },
            data_handler=map_supervisor_listdata
        )

        def compose_supervisor_details(supervisorloginname, user_uri):
            supervisor = list(filter(lambda x: x['employeeid'] == supervisorloginname, rail.result(
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
                     '{{ dag_run.conf.useruri }}']
        )

        should_update_supervisor = rail.IfOperator(
            task_id='should_update_supervisor',
            test=lambda dag_run: dag_run.conf['supervisorloginname'] != dag_run.conf['username'],
            yes_task='process_supervisor',
            no_task='get_supervisor_exceptions_error'
        )

        process_supervisor = rail.EmptyOperator(
            task_id='process_supervisor'
        )

        is_supervisor_present = rail.IfOperator(
            task_id='is_supervisor_present',
            test="{{ result('get_matching_supervisor').uri | is_truthy }}",
            yes_task='process_multiple_supervisorcheck',
            no_task='get_supervisor_exceptions_error'
        )

        process_multiple_supervisorcheck = rail.EmptyOperator(
            task_id='process_multiple_supervisorcheck'
        )

        is_single_supervisor = rail.IfOperator(
            task_id='is_single_supervisor',
            test="{{ result('get_data_for_supervisor') | filter_by_attr('employeeid', 'equals', dag_run.conf.supervisorloginname) | length == 1 }}",
            yes_task='process_disable_supervisorcheck',
            no_task='get_supervisor_exceptions_error'
        )

        process_disable_supervisorcheck = rail.EmptyOperator(
            task_id='process_disable_supervisorcheck'
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id='is_supervisor_disabled',
            test=lambda: rail.result('get_matching_supervisor')[
                'status'] != 'true',
            yes_task='get_supervisor_exceptions_error',
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
            no_task='update_or_add_supervisor'
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('get_matching_supervisor').uri }}",
                'permissionSetUri': '{{ dag_run.conf.supervisor_permission_uri }}'
            }
        )

        update_or_add_supervisor = rail.RepliconServiceOperator(
            task_id='update_or_add_supervisor',
            endpoint="\
                {%- if dag_run.conf.action == 'Update' -%} \
                    /services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange \
                {%- else -%} \
                    /services/UserService1.svc/PutSupervisorAssignmentSchedule \
                {%- endif -%}",
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'supervisorUri': rail.result('get_matching_supervisor')['uri'],
                'dateRange': {
                    'startDate': get_today_date()
                }
            } if dag_run.conf['action'] == 'Update' else {
                'userUri': dag_run.conf['useruri'],
                'initialSupervisorUri': rail.result('get_matching_supervisor')['uri']
            }
        )

        is_error = rail.IfOperator(
            task_id='is_error',
            trigger_rule='all_done',
            test='{{ get_failed_upstream_task_ids() | length > 0 }}',
            yes_task='catch_and_log_error',
            no_task='get_supervisor_exceptions_error'
        )

        catch_and_log_error = rail.PythonOperator(
            task_id='catch_and_log_error',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ get_error_message() }}']
        )

        get_supervisor_exceptions_error = rail.PythonOperator(
            task_id='get_supervisor_exceptions_error',
            python_callable=get_supervisor_exception_message,
            op_args=['should_update_supervisor', 'is_supervisor_present',
                     'is_single_supervisor', 'is_supervisor_disabled', '{{ dag_run.conf.supervisorloginname }}']
        )

        filter_user_logs = rail.FilterLogEntriesOperator(
            task_id='filter_user_logs',
            log='{{ dag_run.conf.user_log }}',
            properties={
                'globalid': '{{ dag_run.conf.username }}'
            },
            remove_filtered_entries=True
        )

        is_filtered_userlogs = rail.IfOperator(
            task_id='is_filtered_userlogs',
            test="{{ result('filter_user_logs', 'length') > 0 }}",
            yes_task='update_userlog_entries',
            no_task='mark_supervisorlog_table_complete'
        )

        def userlog_entry_details(details, action):
            prefix = 'added' if action.lower() == 'add' else 'updated'
            return f"Partially {prefix} - {rail.result('get_supervisor_exceptions_error')['exception_message']}" if rail.result(
                'get_supervisor_exceptions_error')['exception_message'] else details
        update_userlog_entries = rail.WriteLogOperator(
            task_id='update_userlog_entries',
            message='update supervisor entries',
            log='{{ dag_run.conf.user_log }}',
            items="{{ result('filter_user_logs') }}",
            properties=lambda item: {
                'globalid': item['properties']['globalid'],
                'action': item['properties']['action'],
                'status': 'Error' if rail.result('catch_and_log_error') else (
                    'Exception' if rail.result('get_supervisor_exceptions_error')['exception_message'] else item['properties']['status']),
                'details': userlog_entry_details(item['properties']['details'], item['properties']['action']),
                'username': item['properties']['username'],
                'new_location': item['properties']['new_location'],
                'location': item['properties']['location']
            }
        )

        mark_supervisorlog_table_complete = rail.WriteLogOperator(
            task_id='mark_supervisorlog_table_complete',
            message='Completed Supervisor Check',
            log='{{ dag_run.conf.supervisor_log }}',
            severity='Completed',
            properties=lambda dag_run: {
                **{k: v for k, v in dag_run.conf.items() if k in supervisor_logtable_entries},
                ** {'status': 'completed'}
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
            no_task='mark_supervisorlog_table_complete_error'
        )

        update_userlog_entries_error = rail.WriteLogOperator(
            task_id='update_userlog_entries_error',
            message='update supervisor entries',
            log='{{ dag_run.conf.user_log }}',
            severity='Error',
            items="{{ result('filter_user_logs') }}",
            properties={
                'globalid': '{{ item.properties.globalid }}',
                'action': '{{ item.properties.action }}',
                'status': 'Error',
                'details': "{{ item.properties.details }}, {{ get_error_message() }}",
                'username': '{{ item.properties.username }}',
                'new_location': '{{ item.properties.new_location }}',
                'location': '{{ item.properties.location }}'
            }
        )

        mark_supervisorlog_table_complete_error = rail.WriteLogOperator(
            task_id='mark_supervisorlog_table_complete_error',
            message='Completed Supervisor Check',
            log='{{ dag_run.conf.supervisor_log }}',
            severity='Completed',
            properties=lambda dag_run: {
                **{k: v for k, v in dag_run.conf.items() if k in supervisor_logtable_entries},
                ** {'status': 'completed'}
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> on_error

        can_run_batch_task >> rail.Label(
            'No') >> get_data_for_supervisor

        get_data_for_supervisor >> get_matching_supervisor >> should_update_supervisor

        should_update_supervisor >> rail.Label(
            'Yes') >> process_supervisor >> is_supervisor_present

        is_supervisor_present >> rail.Label(
            'Yes') >> process_multiple_supervisorcheck >> is_single_supervisor

        is_single_supervisor >> rail.Label(
            'Yes') >> process_disable_supervisorcheck >> is_supervisor_disabled

        is_supervisor_disabled >> rail.Label(
            'No') >> get_missing_supervisor_permission >> should_add_missing_permissions

        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permission >> update_or_add_supervisor

        should_add_missing_permissions >> rail.Label(
            'No') >> update_or_add_supervisor

        update_or_add_supervisor >> is_error

        is_error >> rail.Label(
            'Yes') >> catch_and_log_error >> get_supervisor_exceptions_error

        is_error >> rail.Label(
            'No') >> get_supervisor_exceptions_error

        is_supervisor_disabled >> rail.Label(
            'Yes') >> get_supervisor_exceptions_error

        is_single_supervisor >> rail.Label(
            'No') >> get_supervisor_exceptions_error

        is_supervisor_present >> rail.Label(
            'No') >> get_supervisor_exceptions_error

        should_update_supervisor >> rail.Label(
            'No') >> get_supervisor_exceptions_error

        get_supervisor_exceptions_error >> filter_user_logs >> is_filtered_userlogs

        is_filtered_userlogs >> rail.Label(
            'Yes') >> update_userlog_entries >> mark_supervisorlog_table_complete

        is_filtered_userlogs >> rail.Label(
            'No') >> mark_supervisorlog_table_complete

        mark_supervisorlog_table_complete >> on_error

        on_error >> is_entries_present_error

        is_entries_present_error >> rail.Label(
            'Yes') >> update_userlog_entries_error >> mark_supervisorlog_table_complete_error

        is_entries_present_error >> rail.Label(
            'No') >> mark_supervisorlog_table_complete_error

        mark_supervisorlog_table_complete_error >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_supervisor_assignment_child_dag)
