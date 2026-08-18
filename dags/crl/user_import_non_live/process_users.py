from datetime import timedelta
from airflow.models import Variable
import rail

from crl.user_import_non_live.utils import request_payload

null = None
DATE_FORMAT = "%m/%d/%Y"

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_users_dagid,
        description='CRL User Import - User Import Process Users',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_users,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_user_log',
            end_task='catch_and_log_errors',
        )

        create_user_log = rail.CreateLogOperator(
            task_id="create_user_log"
        )

        get_user_data = rail.RepliconServiceOperator(
            task_id="get_user_data",
            endpoint="/services/UserService1.svc/BulkGetUsers2",
            data={
                "users": [
                    {
                    "uri": null,
                    "loginName": null,
                    "employeeId": "{{dag_run.conf.emp_id}}",
                    "parameterCorrelationId": null
                    }
                ]
            },
            data_handler=lambda response: [] if response == [None] else response
        )

        is_user_available = rail.IfOperator(
            task_id='is_user_available',
            test=lambda: bool(rail.result('get_user_data')),
            yes_task='process_update_user',
            no_task='process_new_user'
        )

        process_new_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_user',
            items = [0],
            trigger_dag_id=config.process_new_users_dagid,
            conf=request_payload.get_process_new_users_conf,
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
            items = [0],
            trigger_dag_id=config.process_update_users_dagid,
            conf=request_payload.get_process_update_users_conf,
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
            log='{{result("create_user_log")}}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "employee_id": "{{dag_run.conf.emp_id}}",
                "last_name": "{{dag_run.conf.last_name}}",
                "first_name": "{{dag_run.conf.first_name}}",
                "action": "Sync",
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_user_log

        create_user_log >> get_user_data >> is_user_available
        is_user_available >> rail.Label(
            'No') >> process_new_user

        process_new_user >> wait_for_process_new_user >> catch_and_log_errors
        is_user_available >> rail.Label(
            'Yes') >> process_update_user >> wait_for_process_update_user >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
