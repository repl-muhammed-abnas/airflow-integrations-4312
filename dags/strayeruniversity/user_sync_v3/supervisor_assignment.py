from datetime import timedelta
from airflow.models import Variable
import rail
from strayeruniversity.user_sync_v3.utils import python_callable
from strayeruniversity.user_sync_v3.utils.python_callable import get_exceptions


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'strayeruniversity_usersync_update_supervisor_child_v3_{config.instance}',
        description=f'strayeruniversity_usersync_update_supervisor_child_v3_{config.instance}',
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
            end_task='catch_and_log_error',
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
            python_callable=lambda: "-supervisorassignment skipped as supervisor not found or is disabled"
        )

        search_userimport_logs_for_user_and_delete_to_update = rail.FilterLogEntriesOperator(
            task_id='search_userimport_logs_for_user_and_delete_to_update',
            log="{{ dag_run.conf.logger }}",
            properties={
                "username": "{{ dag_run.conf.username }}" + "|" + "{{ dag_run.conf.empid }}"
            },
            remove_filtered_entries=True
        )

        load_found_logs_entry = rail.PythonOperator(
            task_id='load_found_logs_entry',
            python_callable=lambda: rail.load_all_records(rail.result(
                'search_userimport_logs_for_user_and_delete_to_update'))
        )

        if_entry_is_present = rail.IfOperator(
            task_id='if_entry_is_present',
            test='''{{ result('search_userimport_logs_for_user_and_delete_to_update','length') > 0 | is_truthy }}''',
            yes_task="add_updated_log",
            no_task="catch_and_log_error",
        )

        add_updated_log = rail.WriteLogOperator(
            task_id='add_updated_log',
            log="{{dag_run.conf.logger}}",
            message='na',
            severity=lambda: 'Error' if 'Error' in rail.result('load_found_logs_entry')[0]['properties']['status'] else (
                    'Exception' if get_exceptions() else rail.result('load_found_logs_entry')[0]['properties']['status']),
            properties=lambda: {
                "username": rail.result('load_found_logs_entry')[0]['properties']['username'],
                "action": rail.result('load_found_logs_entry')[0]['properties']['action'],
                "status": 'Error' if 'Error' in rail.result('load_found_logs_entry')[0]['properties']['status'] else (
                    'Exception' if get_exceptions() else rail.result('load_found_logs_entry')[0]['properties']['status']),
                "details": rail.result('load_found_logs_entry')[0]['properties']['details'] + ',' + get_exceptions()
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log='{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "username": "{{ dag_run.conf.username }}" + "|" + "{{ dag_run.conf.empid }}",
                "action": "Supervisor Assignment",
                "status": "Error",
                "details": "{{ dag_run_ecid() }}" + "-" + "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> search_for_user_with_managername

        search_for_user_with_managername >> if_supervisor_uri_present_and_enabled

        if_supervisor_uri_present_and_enabled >> rail.Label(
            'Yes') >> assign_supervisor >> search_userimport_logs_for_user_and_delete_to_update
        if_supervisor_uri_present_and_enabled >> rail.Label(
            'No') >> log_supervisor_assign_skipped >> search_userimport_logs_for_user_and_delete_to_update

        search_userimport_logs_for_user_and_delete_to_update >> load_found_logs_entry >> if_entry_is_present

        if_entry_is_present >> rail.Label(
            'Yes') >> add_updated_log >> catch_and_log_error
        if_entry_is_present >> rail.Label('No') >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
