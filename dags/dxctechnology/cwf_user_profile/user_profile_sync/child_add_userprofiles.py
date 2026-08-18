from datetime import timedelta
import rail
from airflow.models import Variable
from dxctechnology.cwf_user_profile.user_profile_sync.task.process_supervisor_assignment import process_supervisor_assignment_task_group
from dxctechnology.cwf_user_profile.user_profile_sync.utils.python_callable_method import get_contract_dates
from dxctechnology.cwf_user_profile.user_profile_sync.utils.request_payload import get_put_user_payload


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/cwf_user_profile/user_profile_sync/config.py


def create_add_userprofile_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_cwf_userprofiles_add_child_{config.instance}',
        description=f'DXC_FieldglassCWF_Child_Add User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_user_profile_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_contract_startdate_enddate'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_contract_startdate_enddate',
            end_task='catch_and_log_errors',
        )

        get_contract_startdate_enddate = rail.PythonOperator(
            task_id='get_contract_startdate_enddate',
            python_callable=get_contract_dates
        )

        create_user_in_replicon = rail.RepliconServiceOperator(
            task_id='create_user_in_replicon',
            endpoint=config.put_user_service,
            data=lambda dag_run: get_put_user_payload(
                dag_run, config.should_add_emailaddress)
        )

        remove_timeoff_assignment = rail.RepliconServiceOperator(
            task_id='remove_timeoff_assignment',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data={
                'userUri': "{{ result('create_user_in_replicon').uri }}",
                'timeOffTypeUris': []
            }
        )

        put_product_assignments = rail.RepliconServiceOperator(
            task_id='put_product_assignments',
            endpoint='/services/AccountManagementService1.svc/PutProductAssignmentsForUser',
            data=lambda dag_run: {
                'userUri': rail.result('create_user_in_replicon')['uri'],
                'productUris': dag_run.conf['product_uri']
            }
        )

        should_update_timeentry_approval_path = rail.IfOperator(
            task_id='should_update_timeentry_approval_path',
            test='{{ dag_run.conf.timeentry_approval_path | sn | is_truthy }}',
            yes_task='update_timeentry_approval_path',
            no_task='should_update_supervisor'
        )

        update_timeentry_approval_path = rail.RepliconServiceOperator(
            task_id='update_timeentry_approval_path',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data={
                'user': {
                    'uri': "{{ result('create_user_in_replicon').uri }}"
                },
                'modifications': {
                    'timeEntryRevisionGroupApprovalPathToApply': {
                        'name': '{{ dag_run.conf.timeentry_approval_path }}'
                    }
                },
                'userModificationOptionUri': 'urn:replicon:user-modification-option:save'
            }
        )

        should_update_supervisor = rail.IfOperator(
            task_id='should_update_supervisor',
            test=lambda dag_run: not (dag_run.conf['managerid'] == dag_run.conf['hpid'] or dag_run.conf[
                    'manageremail'] == dag_run.conf['emailaddress']),
            yes_task='get_data_for_supervisor',
            no_task='log_add_user'
        )

        (get_data_for_supervisor, assign_initial_supervisor, log_supervisor_check) = process_supervisor_assignment_task_group(
            config.execution_timeout_days)

        log_add_user = rail.WriteLogOperator(
            task_id='log_add_user',
            log='{{ dag_run.conf.log }}',
            message="{{ 'User created partially - Supervisor not updated - Supervisor is same as User' if \
                    result('should_update_supervisor') == 'log_add_user' else 'User created successfully' }}",
            properties={
                'userid': '{{ dag_run.conf.hpid }}',
                'email': '{{ dag_run.conf.emailaddress }}',
                'action': 'Add',
                'status': "{{ 'Exception' if result('should_update_supervisor') == 'log_add_user' else 'Success' }}",
                'details': "{{ 'User created partially - Supervisor not updated - Supervisor is same as User' if \
                    result('should_update_supervisor') == 'log_add_user' else 'User created successfully' }}"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log }}',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            properties={
                'userid': '{{ dag_run.conf.hpid }}',
                'email': '{{ dag_run.conf.emailaddress }}',
                'action': 'Add',
                'status': 'Error',
                'details': "{{ get_error_message() }}"
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            'No') >> get_contract_startdate_enddate

        get_contract_startdate_enddate >> create_user_in_replicon >> remove_timeoff_assignment >> \
            put_product_assignments >> should_update_timeentry_approval_path

        should_update_timeentry_approval_path >> rail.Label(
            'Yes') >> update_timeentry_approval_path >> should_update_supervisor

        should_update_timeentry_approval_path >> rail.Label(
            'No') >> should_update_supervisor

        assign_initial_supervisor >> log_add_user

        log_supervisor_check >> log_add_user

        should_update_supervisor >> rail.Label(
            'Yes') >> get_data_for_supervisor

        should_update_supervisor >> rail.Label(
            'No') >> log_add_user

        log_add_user >> catch_and_log_errors >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_add_userprofile_child_dag)
