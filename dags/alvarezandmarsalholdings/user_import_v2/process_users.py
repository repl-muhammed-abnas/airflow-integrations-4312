from datetime import timedelta
from airflow.models import Variable
import rail

from alvarezandmarsalholdings.user_import_v2.utils import request_payload

null = None
DATE_FORMAT = "%m/%d/%Y"


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_users_dagid,
        description='Alvarezandmarsalholdings User Import - User Import Process Users',
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
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": "{{dag_run.conf.employee_id}}",
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: [] if response == [None] else response
        )

        has_valid_data = rail.IfOperator(
            task_id='has_valid_data',
            test=request_payload.test_valid_fields,
            yes_task="is_user_available",
            no_task="log_invalid_data"
        )

        log_invalid_data = rail.WriteLogOperator(
            task_id='log_invalid_data',
            log='{{ result("create_user_log") }}',
            message=request_payload.get_invalid_fields_message,
            severity='Exception',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf['employee_id'],
                "action": "Validation",
                "status": "Exception",
                'details':  request_payload.get_invalid_fields_message(dag_run),
            }
        )

        is_user_available = rail.IfOperator(
            task_id='is_user_available',
            test=lambda: bool(rail.result('get_user_data')),
            yes_task='process_update_user',
            no_task='get_user_data_based_on_login_name'
        )

        get_user_data_based_on_login_name = rail.RepliconServiceOperator(
            task_id="get_user_data_based_on_login_name",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": "{{dag_run.conf.workday_user_name}}",
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: [] if response == [None] else response
        )

        is_same_login_name_already_available = rail.IfOperator(
            task_id='is_same_login_name_already_available',
            test=lambda: bool(rail.result(
                'get_user_data_based_on_login_name')),
            yes_task='log_exception_same_loginname_exists',
            no_task='process_new_user'
        )

        log_exception_same_loginname_exists = rail.WriteLogOperator(
            task_id='log_exception_same_loginname_exists',
            log='{{ result("create_user_log") }}',
            message="User with same login name already exists",
            severity='Exception',
            properties=lambda dag_run: {
                "employee_id": dag_run.conf['employee_id'],
                "action": "Validation",
                "status": "Exception",
                'details': "User with same login name already exists"
            }
        )

        def get_add_update_trigger_id(dag_run, action):
            if action == "add":
                if dag_run.conf['modulo'] == 0:
                    return config.process_new_users_dagid
                return f"{config.process_new_users_dagid}_batch_{dag_run.conf['modulo']}"
            if dag_run.conf['modulo'] == 0:
                return config.process_update_users_dagid
            return f"{config.process_update_users_dagid}_batch_{dag_run.conf['modulo']}"

        process_new_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_user',
            items=[0],
            trigger_dag_id=lambda dag_run: get_add_update_trigger_id(
                dag_run, "add"),
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
            items=[0],
            trigger_dag_id=lambda dag_run: get_add_update_trigger_id(
                dag_run, "update"),
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
                "employee_id": "{{dag_run.conf.employee_id}}",
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

        create_user_log >> get_user_data >> has_valid_data >> rail.Label(
            'No') >> log_invalid_data >> catch_and_log_errors
        has_valid_data >> rail.Label('Yes') >> is_user_available
        is_user_available >> rail.Label(
            'No') >> get_user_data_based_on_login_name >> is_same_login_name_already_available

        is_same_login_name_already_available >> rail.Label(
            'Yes') >> log_exception_same_loginname_exists >> catch_and_log_errors
        is_same_login_name_already_available >> rail.Label(
            'No') >> process_new_user

        process_new_user >> wait_for_process_new_user >> catch_and_log_errors
        is_user_available >> rail.Label(
            'Yes') >> process_update_user >> wait_for_process_update_user >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
