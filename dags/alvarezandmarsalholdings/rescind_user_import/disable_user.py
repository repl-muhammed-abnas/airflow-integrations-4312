from datetime import timedelta
import rail
from airflow.models import Variable
from alvarezandmarsalholdings.rescind_user_import.utils import request_payload


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_disable_users_dag_id,
        description=f'{config.company_key} Rescind User Import Process Each User Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_disable_users,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_disable_user_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_disable_user_log',
            end_task='catch_and_log_errors',
        )

        create_disable_user_log = rail.CreateLogOperator(
            task_id="create_disable_user_log"
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": None,
                        "loginName": None,
                        "employeeId": dag_run.conf["employee_id"],
                        "parameterCorrelationId": None
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
        )

        if_user_availablein_replicon = rail.IfOperator(
            task_id='if_user_availablein_replicon',
            test=lambda: bool(rail.result('get_user_details')),
            yes_task='update_user',
            no_task='log_exception_user_not_available'
        )

        log_exception_user_not_available = rail.WriteLogOperator(
            task_id='log_exception_user_not_available',
            log='{{ result("create_disable_user_log") }}',
            message="User not available in Replicon",
            severity='Exception',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['employee_id'],
                'action': 'Validation',
                'status': 'Exception',
                "details": "User not available in Replicon"
            }
        )

        update_user = rail.RepliconServiceOperator(
            task_id="update_user",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.get_update_user_payload(
                dag_run)
        )

        logs_success_or_exception = rail.WriteLogOperator(
            task_id='logs_success_or_exception',
            log="{{ result('create_disable_user_log') }}",
            message="na",
            severity=lambda dag_run: "Success" if dag_run.conf["event_identifier"] == "RESCIND" else "Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf['employee_id'],
                "action": "Disable",
                "status": "Success" if dag_run.conf["event_identifier"] == "RESCIND" else "Exception",
                "details": "User disabled successfully" + (", Exeption - event identifier is not RESCIND" if dag_run.conf["event_identifier"] != "RESCIND" else ""),
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_disable_user_log') }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf['employee_id'],
                "action": "Disable",
                "status": "Error",
                "details": "{{get_error_message()}}",
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> create_disable_user_log >> get_user_details >> if_user_availablein_replicon
        if_user_availablein_replicon >> rail.Label(
            'Yes') >> update_user >> logs_success_or_exception >> catch_and_log_errors
        if_user_availablein_replicon >> rail.Label(
            'No') >> log_exception_user_not_available >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
