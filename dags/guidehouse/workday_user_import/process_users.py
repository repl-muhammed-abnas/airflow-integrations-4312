from datetime import timedelta
from airflow.models import Variable
import rail
from guidehouse.workday_user_import.utils import request_payload, response_filters, custom_method


def create_child_dag(config):
    """
    Create child DAG for orchestrating individual user processing.

    Args:
        config: Configuration object containing DAG settings.

    Returns:
        list[DAG]: List of configured Airflow DAG objects (one per batch)
    """
    # pylint: disable=too-many-statements, line-too-long, cell-var-from-loop
    append_dags = []
    for idx in range(0, config.PROCESS_USER_BATCH_COUNT):
        with rail.create_airflow_dag(
            dag_id=f'{config.process_each_user}_batch_{idx+1}',
            description='Guidehouse Workday User Import - Process Each Users',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_users,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='process_user_log'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='process_user_log',
                end_task='catch_and_log_errors',
            )

            process_user_log = rail.CreateLogOperator(
                task_id="process_user_log"
            )

            query_user_data = rail.QueryCollectionOperator(
                task_id="query_user_data",
                query="""SELECT * FROM valid_data WHERE employee_id=:empl_id""",
                query_params={
                    'empl_id': '{{ dag_run.conf.employee_id }}'
                }
            )

            get_user_payload_data = rail.PythonOperator(
                task_id='get_user_payload_data',
                python_callable=custom_method.get_payload_user_data
            )

            get_user_by_empl_id = rail.RepliconServiceOperator(
                task_id="get_user_by_empl_id",
                endpoint="/services/ImportService1.svc/BulkGetUsers3",
                data=request_payload.get_user_data_payload,
                data_handler=response_filters.get_filtered_user_data
            )

            get_create_user_data = rail.PythonOperator(
                task_id='get_create_user_data',
                python_callable=lambda dag_run: custom_method.get_process_new_users_conf(config, dag_run)
            )

            is_existing_user = rail.IfOperator(
                task_id='is_existing_user',
                test="{{ result('get_user_by_empl_id') | is_truthy }}",
                yes_task='get_effective_user_groups',
                no_task='process_new_user'
            )

            get_effective_user_groups = rail.RepliconServiceOperator(
                task_id='get_effective_user_groups',
                endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
                data={
                    "userUri": "{{ result('get_user_by_empl_id')[0].userDetails.uri }}",
                    "dateRange": None
                },
                data_handler=response_filters.get_effective_user_groupmembership_filter
            )

            def get_process_users_batch_dag_id(dag_id, modulo):
                return f'{dag_id}_batch_{modulo+1}'

            process_new_user = rail.TriggerDagRunForEachItemOperator(
                task_id='process_new_user',
                items=[0],
                trigger_dag_id=lambda dag_run: get_process_users_batch_dag_id(config.process_new_users, int(dag_run.conf['modulo'])),
                conf=lambda dag_run: custom_method.get_process_new_users_conf(config, dag_run),
                execution_timeout=timedelta(days=config.execution_timeout_days),
                retries=0,
            )

            wait_for_process_new_user = rail.WaitForDagRunsSensor(
                task_id='wait_for_process_new_user',
                dag_runs='{{ result("process_new_user") }}',
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            process_update_user = rail.TriggerDagRunForEachItemOperator(
                task_id='process_update_user',
                items=[0],
                trigger_dag_id=lambda dag_run: get_process_users_batch_dag_id(config.process_update_users, int(dag_run.conf['modulo'])),
                conf=lambda dag_run: custom_method.get_process_update_users_conf(config, dag_run),
                execution_timeout=timedelta(days=config.execution_timeout_days),
                retries=0,
            )

            wait_for_process_update_user = rail.WaitForDagRunsSensor(
                task_id='wait_for_process_update_user',
                dag_runs='{{ result("process_update_user") }}',
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log='{{result("process_user_log")}}',
                trigger_rule='one_failed',
                severity='Error',
                message='{{ get_error_message() }}',
                properties={
                    "lastname": "{{ result('get_user_payload_data').last_name }}",
                    "firstname": "{{ result('get_user_payload_data').first_name }}",
                    "loginname": "{{ result('get_user_payload_data').login_name }}",
                    "employeeid": "{{ dag_run.conf.employee_id }}",
                    "manager": "{{ result('get_user_payload_data').supervisor_id }}",
                    "userstatus": "{{ result('get_user_payload_data').user_status }}",
                    "co_costcenter": "{{ result('get_user_payload_data').company_description }}",
                    "location": "{{ result('get_user_payload_data').location }}",
                    "action": "Sync",
                    'status': 'Error',
                    'details': '{{ get_error_message() }}',
                },
            )

            can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> process_user_log

            process_user_log >> query_user_data >> get_user_payload_data >> get_user_by_empl_id
            get_user_by_empl_id >> get_create_user_data >> is_existing_user >> rail.Label("Yes") >> get_effective_user_groups >> process_update_user
            is_existing_user >> rail.Label("No") >> process_new_user >> wait_for_process_new_user >> catch_and_log_errors
            process_update_user >> wait_for_process_update_user >> catch_and_log_errors

        append_dags.append(dag)
    return append_dags


rail.for_each_instance(create_child_dag)
