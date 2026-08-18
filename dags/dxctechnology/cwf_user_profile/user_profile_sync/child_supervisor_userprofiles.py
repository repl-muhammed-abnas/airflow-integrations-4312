from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.cwf_user_profile.user_profile_sync.utils.request_payload import get_data_for_supervisor_payload, get_today_date
from dxctechnology.cwf_user_profile.user_profile_sync.utils.response_filter import get_missing_permissions, map_supervisor_list_data
from dxctechnology.cwf_user_profile.user_profile_sync.utils.python_callable_method import compose_supervisor_details, add_supervisorcheck_user_log


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/cwf_user_profile/user_profile_sync/config.py


def create_supervisor_userprofile_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_cwf_userprofiles_supervisor_child_{config.instance}',
        description=f'DXC_Fieldglass CWFUserProfiles_Child_Supervisor Assignment {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_user_profile_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        supervisor_logtable_entries = (
            'user_loginname', 'user_uri', 'user_name', 'supervisor_loginname', 'action', 'emp_id')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='filter_child_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='filter_child_logs',
            end_task='on_error',
        )

        def do_filter_log(log):
            action_to_check = ('Add', 'Update')
            return log['properties']['action'] in action_to_check
        filter_child_logs = rail.FilterLogEntriesOperator(
            task_id='filter_child_logs',
            log='{{ dag_run.conf.child_log }}',
            filter_callable=do_filter_log,
            remove_filtered_entries=True
        )

        get_data_for_supervisor = rail.RepliconServiceOperator(
            task_id='get_data_for_supervisor',
            endpoint='/services/UserListService1.svc/GetData',
            data=get_data_for_supervisor_payload,
            data_handler=map_supervisor_list_data
        )

        get_matching_supervisor = rail.PythonOperator(
            task_id='get_matching_supervisor',
            python_callable=compose_supervisor_details,
            op_args=['{{ dag_run.conf.manageremail }}',
                     '{{ dag_run.conf.managerid }}']
        )

        is_supervisor_present = rail.IfOperator(
            task_id='is_supervisor_present',
            test="{{ result('get_matching_supervisor').uri | is_truthy }}",
            yes_task='is_supervisor_disabled',
            no_task='is_child_log_entries_present'
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id='is_supervisor_disabled',
            test="{{ result('get_matching_supervisor').status | is_falsy }}",
            yes_task='enable_supervisor',
            no_task='get_missing_supervisor_permissions'
        )

        enable_supervisor = rail.RepliconServiceOperator(
            task_id='enable_supervisor',
            endpoint='/services/SecurityService1.svc/EnableLogin',
            data={
                'userUri': "{{ result('get_matching_supervisor').uri }}"
            }
        )

        get_missing_supervisor_permissions = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                'userUri': "{{ result('get_matching_supervisor').uri }}"
            },
            data_handler=get_missing_permissions
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_missing_supervisor_permissions') | length > 0 }}",
            yes_task='add_missing_supervisor_permissions',
            no_task='update_or_add_supervisor'
        )

        add_missing_supervisor_permissions = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_missing_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda: rail.result('get_missing_supervisor_permissions'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data={
                'userUri': "{{ result('get_matching_supervisor').uri }}",
                'permissionSetUri': '{{ item }}'
            }
        )

        update_or_add_supervisor = rail.RepliconServiceOperator(
            task_id='update_or_add_supervisor',
            # pylint: disable=line-too-long
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange if {{ dag_run.conf.action }} == 'Update' else /services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data=lambda dag_run: {
                'userUri': dag_run.conf['user_uri'],
                'supervisorUri': rail.result('get_matching_supervisor')['uri'],
                'dateRange': {
                    'startDate': get_today_date()
                }
            } if dag_run.conf['action'] == 'Update' else {
                'userUri': dag_run.conf['user_uri'],
                'initialSupervisorUri': rail.result('get_matching_supervisor')['uri']
            }
        )

        is_child_log_entries_present = rail.IfOperator(
            task_id='is_child_log_entries_present',
            test="{{ result('filter_child_logs', 'length') > 0 }}",
            yes_task='log_to_user_child_log',
            no_task='write_supervisor_entry_complete'
        )

        log_to_user_child_log = rail.WriteLogOperator(
            task_id='log_to_user_child_log',
            log='{{ dag_run.conf.child_log }}',
            message='log supervisor check user log',
            items="{{ result('filter_child_logs') }}",
            properties=add_supervisorcheck_user_log
        )

        write_supervisor_entry_complete = rail.WriteLogOperator(
            task_id='write_supervisor_entry_complete',
            message='Completed Supervisor Check',
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
            test="{{ result('filter_child_logs', 'length') > 0 }}",
            yes_task='log_to_user_child_log_error',
            no_task='write_supervisor_entry_complete_error'
        )

        log_to_user_child_log_error = rail.WriteLogOperator(
            task_id='log_to_user_child_log_error',
            log='{{ dag_run.conf.child_log }}',
            message='log supervisor check user log',
            items="{{ result('filter_child_logs') }}",
            properties={
                'userid': '{{ item.properties.userid }}',
                'email': '{{ item.properties.email }}',
                'action': '{{ item.properties.status }}',
                'status': 'Error',
                'details': "{{ item.properties.details }};{{ get_error_message() }}"
            }
        )

        write_supervisor_entry_complete_error = rail.WriteLogOperator(
            task_id='write_supervisor_entry_complete_error',
            message='Completed Supervisor Check',
            severity='Completed',
            properties=lambda dag_run: {
                **{k: v for k, v in dag_run.conf.items() if k in supervisor_logtable_entries},
                ** {'status': 'completed'}
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> on_error

        can_run_batch_task >> rail.Label(
            'No') >> filter_child_logs

        filter_child_logs >> get_data_for_supervisor >> get_matching_supervisor >> is_supervisor_present

        is_supervisor_present >> rail.Label(
            'Yes') >> is_supervisor_disabled

        is_supervisor_disabled >> rail.Label(
            'Yes') >> enable_supervisor >> get_missing_supervisor_permissions

        is_supervisor_disabled >> rail.Label(
            'No') >> get_missing_supervisor_permissions

        get_missing_supervisor_permissions >> should_add_missing_permissions

        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permissions >> update_or_add_supervisor >> on_error

        should_add_missing_permissions >> rail.Label(
            'No') >> update_or_add_supervisor

        is_supervisor_present >> rail.Label(
            'No') >> is_child_log_entries_present

        is_child_log_entries_present >> rail.Label(
            'Yes') >> log_to_user_child_log >> write_supervisor_entry_complete

        is_child_log_entries_present >> rail.Label(
            'No') >> write_supervisor_entry_complete

        write_supervisor_entry_complete >> on_error >> is_entries_present_error

        is_entries_present_error >> rail.Label(
            'Yes') >> log_to_user_child_log_error >> write_supervisor_entry_complete_error

        is_entries_present_error >> rail.Label(
            'No') >> write_supervisor_entry_complete_error

        return dag


rail.for_each_instance(create_supervisor_userprofile_child_dag)
