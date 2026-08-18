from datetime import timedelta
from airflow.models import Variable
import rail
from strayeruniversity.user_sync_v4.utils import python_callable
from strayeruniversity.user_sync_v4.utils.python_callable import get_exceptions


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_update_supervisor_dag_id,
        description=f'strayeruniversity_usersync_update_supervisor_child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.assign_supervisor_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_for_user_with_managername'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_for_user_with_managername',
            end_task='on_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        search_for_user_with_managername = rail.RepliconServiceOperator(
            task_id='search_for_user_with_managername',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:login-name"
                ]
            },
            data_handler=python_callable.get_userdata_list_for_managername
        )

        if_supervisor_uri_present_and_enabled = rail.IfOperator(
            task_id='if_supervisor_uri_present_and_enabled',
            test='''{{ result('search_for_user_with_managername') | is_truthy and \
                result('search_for_user_with_managername')[0].uri | is_truthy and \
                result('search_for_user_with_managername')[0].enabled.lower() == 'true' }}''',
            yes_task="assign_supervisor",
            no_task="log_supervisor_assign_skipped",
        )

        assign_supervisor = rail.RepliconServiceOperator(
            task_id="assign_supervisor",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('search_for_user_with_managername')[0].uri }}"
            }
        )

        log_supervisor_assign_skipped = rail.PythonOperator(
            task_id='log_supervisor_assign_skipped',
            python_callable=lambda: "Supervisor assignment skipped as" + (" supervisor is disabled in instance" if rail.result(
                'search_for_user_with_managername') and rail.result(
                'search_for_user_with_managername')[0]['enabled'].lower() == 'false' else " supervisor not found in instance")
        )

        get_filtered_user_log = rail.FilterLogEntriesOperator(
            task_id='get_filtered_user_log',
            log="{{dag_run.conf.user_log}}",
            properties={
                'username': "{{ dag_run.conf.username }}" + "|" + "{{ dag_run.conf.emplid }}"
            },
            remove_filtered_entries=True
        )

        if_existing_user_log_found = rail.IfOperator(
            task_id='if_existing_user_log_found',
            test="{{ result('get_filtered_user_log', 'length') > 0 }}",
            yes_task='add_updated_log',
            no_task='on_error'
        )

        add_updated_log = rail.WriteLogOperator(
            task_id='add_updated_log',
            log="{{dag_run.conf.user_log}}",
            items="{{ result('get_filtered_user_log') }}",
            message='na',
            severity=lambda item: 'Error' if 'Error' in item['properties']['status'] else (
                'Exception' if get_exceptions() else item['properties']['status']),
            properties=lambda item, dag_run: {
                "username": item['properties']['username'],
                "action": item['properties']['action'],
                "status": 'Error' if 'Error' in item['properties']['status'] else (
                    'Exception' if get_exceptions() else item['properties']['status']),
                "details": item['properties']['details'] + ' ; ' + (get_exceptions() if get_exceptions() else 'Supervisor updated successfully')
            }
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        if_filter_entry_present_and_supervisor_error = rail.IfOperator(
            task_id='if_filter_entry_present_and_supervisor_error',
            test="{{ result('get_filtered_user_log', 'length') > 0 }}",
            yes_task='update_userlog_entries_error',
        )

        update_userlog_entries_error = rail.WriteLogOperator(
            task_id="update_userlog_entries_error",
            log="{{dag_run.conf.user_log}}",
            items="{{ result('get_filtered_user_log') }}",
            severity="Error",
            message='update supervisor entries',
            properties=lambda item: {
                "username": item['properties']['username'],
                "action": item['properties']['action'],
                "status": "Error",
                "details": item['properties']['details'] + " ; " + "Error in supervisor assignment-" + "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> on_error
        can_run_batch_task >> rail.Label(
            'No') >> search_for_user_with_managername

        search_for_user_with_managername >> if_supervisor_uri_present_and_enabled

        if_supervisor_uri_present_and_enabled >> rail.Label(
            'Yes') >> assign_supervisor >> get_filtered_user_log
        if_supervisor_uri_present_and_enabled >> rail.Label(
            'No') >> log_supervisor_assign_skipped >> get_filtered_user_log >> if_existing_user_log_found

        if_existing_user_log_found >> rail.Label(
            'Yes') >> add_updated_log >> on_error
        if_existing_user_log_found >> rail.Label(
            'No') >> on_error

        on_error >> if_filter_entry_present_and_supervisor_error

        if_filter_entry_present_and_supervisor_error >> rail.Label(
            'Yes') >> update_userlog_entries_error

    return dag


rail.for_each_instance(create_dag)
