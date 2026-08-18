from datetime import timedelta, datetime
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/mccarthy/user_import/config.py


def create_supervisorassignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'mccarthy_user_import_supervisor_assignment_child_{config.instance}',
        description=f'LIVE | Mccarthy User Sync_child_Supervisor Assignment {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
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
            start_task='get_data_for_supervisor',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def map_supervisor_listdata(response, dag_run):
            filtered_supervisor = [
                x for x in response['rows'] if x['cells'][1]['textValue'] == dag_run.conf['supervisorloginname']] if response['rows'] else []
            return {
                'name': filtered_supervisor[0]['cells'][0]['textValue'] if filtered_supervisor else '',
                'uri': filtered_supervisor[0]['cells'][0]['uri'] if filtered_supervisor else '',
                'status': filtered_supervisor[0]['cells'][2]['textValue'].lower() if filtered_supervisor else ''
            }
        get_data_for_supervisor = rail.RepliconServiceOperator(
            task_id='get_data_for_supervisor',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: {
                'page': '1',
                'pagesize': '100',
                'columnUris': [
                    'urn:replicon:user-list-column:user',
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
                            'text': dag_run.conf['supervisorloginname']
                        }
                    }
                }
            },
            data_handler=map_supervisor_listdata
        )

        is_supervisoruri_not_present = rail.IfOperator(
            task_id='is_supervisoruri_not_present',
            test="{{ result('get_data_for_supervisor').uri | is_falsy }}",
            yes_task="filter_user_logs",
            no_task="is_status_equals_false"
        )

        filter_user_logs = rail.FilterLogEntriesOperator(
            task_id='filter_user_logs',
            log='{{ dag_run.conf.user_log }}',
            properties={
                'loginname': '{{ dag_run.conf.userloginname }}'
            },
            remove_filtered_entries=True
        )

        is_filtered_userlogs = rail.IfOperator(
            task_id='is_filtered_userlogs',
            test="{{ result('filter_user_logs', 'length') > 0 }}",
            yes_task='update_userlog_entries',
            no_task='dagrun_log_to_sumo'
        )

        update_userlog_entries = rail.WriteLogOperator(
            task_id='update_userlog_entries',
            message='update supervisor entries',
            log='{{ dag_run.conf.user_log }}',
            items="{{ result('filter_user_logs') }}",
            properties=lambda item: {
                'loginname': item['properties']['loginname'],
                'email': item['properties']['email'],
                'action': 'Exception',
                'status': item['properties']['status'],
                'details': f"{item['properties']['details']}| Supervisor Assignment couldn't be done as no supervisor found"
            }
        )

        is_status_equals_false = rail.IfOperator(
            task_id='is_status_equals_false',
            test="{{ result('get_data_for_supervisor').status == 'false' }}",
            yes_task="dagrun_log_to_sumo",
            no_task="get_missing_supervisor_permission"
        )

        def is_assign_supervisorpermission(response, dag_run):
            supervisor_permission = False
            if response:
                if not rail.find_first_by_attr_and_get_attr(response, 'permissionSet.name', dag_run.conf['supervisorpermissionname'], 'permissionSet', ''):
                    supervisor_permission = True
            return supervisor_permission
        get_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                'userUri': "{{ result('get_data_for_supervisor').uri }}"
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
                'userUri': "{{ result('get_data_for_supervisor').uri }}",
                'permissionSetUri': '{{ dag_run.conf.supervisorpermissionuri }}'
            }
        )

        def get_replicon_date(date_str, fmt='%m/%d/%Y'):
            datetime_obj = datetime.strptime(date_str, fmt)
            return {
                'year': datetime_obj.year,
                'month': datetime_obj.month,
                'day': datetime_obj.day
            }
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
                'supervisorUri': rail.result('get_data_for_supervisor')['uri'],
                'dateRange': {
                    'startDate': get_replicon_date(
                        dag_run.conf['effective_date']) if dag_run.conf['effective_date'] else {
                            'year': None,
                            'month': None,
                            'day': None
                    }
                }
            } if dag_run.conf['action'] == 'Update' else {
                'userUri': dag_run.conf['useruri'],
                'initialSupervisorUri': rail.result('get_data_for_supervisor')['uri']
            }
        )

        update_locationgroup = rail.RepliconServiceOperator(
            task_id='update_locationgroup',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('get_data_for_supervisor').uri }}",
                "scheduleEntries": [
                    {
                        "location": {
                            "uri": "{{ dag_run.conf.locationuri }}"
                        }
                    }
                ]
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
        get_data_for_supervisor >> is_supervisoruri_not_present
        is_supervisoruri_not_present >> rail.Label(
            'Yes') >> filter_user_logs >> is_filtered_userlogs
        is_filtered_userlogs >> rail.Label(
            'Yes') >> update_userlog_entries >> dagrun_log_to_sumo
        is_filtered_userlogs >> rail.Label(
            'No') >> dagrun_log_to_sumo
        is_supervisoruri_not_present >> rail.Label(
            'No') >> is_status_equals_false
        is_status_equals_false >> rail.Label(
            'Yes') >> dagrun_log_to_sumo
        is_status_equals_false >> rail.Label(
            'No') >> get_missing_supervisor_permission >> should_add_missing_permissions
        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permission >> update_or_add_supervisor
        should_add_missing_permissions >> rail.Label(
            'No') >> update_or_add_supervisor
        update_or_add_supervisor >> update_locationgroup >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_supervisorassignment_dag)
