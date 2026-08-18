from datetime import timedelta
from airflow.models import Variable
import rail
from darkmattertechnologiesllc.user_sync_v1.utils import request_payload, python_callable

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.process_each_user_child_dagid,
        description=config.process_each_user_child_dagid,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_eachuser_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_user_log',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_user_log = rail.CreateLogOperator(
            task_id = "create_user_log"
        )

        has_valid_dateformat = rail.IfOperator(
            task_id="has_valid_dateformat",
            test=python_callable.validate_date_fields,
            yes_task="search_user",
            no_task="log_invalid_dateformat"
        )

        log_invalid_dateformat = rail.WriteLogOperator(
            task_id="log_invalid_dateformat",
            log = '{{ dag_run.conf.logger}}',
            message='Skipped',
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "action": "Validation",
                "status": "Skipped",
                "details": "Invalid date format"
            }
        )

        search_user = rail.RepliconServiceOperator(
            task_id='search_user',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data = {
                "users": [
                    {
                        "employeeId": "{{ dag_run.conf.employeeid }}"
                    }
                ]
            }
        )

        check_user_in_replicon_present = rail.IfOperator(
            task_id='check_user_in_replicon_present',
            test='''{{ result('search_user') | is_truthy }}''',
            yes_task="get_my_actual_user_identity",
            no_task="is_user_active_for_add",
        )

        get_my_actual_user_identity = rail.RepliconServiceOperator(
            task_id='get_my_actual_user_identity',
            endpoint="/services/UserAccessControlService1.svc/GetMyActualUserIdentity",
        )

        if_user_loginname_equal_actual_user_identity = rail.IfOperator(
            task_id='if_user_loginname_equal_actual_user_identity',
            test=lambda dag_run: dag_run.conf['loginname'] != rail.result('get_my_actual_user_identity')['loginName'],
            yes_task="process_update_user",
            no_task="log_integration_user",
        )

        process_update_user = rail.TriggerDagRunOperator(
            task_id='process_update_user',
            trigger_dag_id=config.update_user_child_dagid,
            conf=request_payload.process_update_user_payload,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_user',
            dag_runs='{{ result("process_update_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        is_user_active_for_add = rail.IfOperator(
            task_id='is_user_active_for_add',
            test='''{{ dag_run.conf.employeestatus | lower == 'active' }}''',
            yes_task="process_add_user",
            no_task="log_skip_user",
        )

        process_add_user = rail.TriggerDagRunOperator(
            task_id='process_add_user',
            trigger_dag_id=config.add_user_child_dagid,
            conf=request_payload.process_add_user_payload,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_add_user',
            dag_runs='{{ result("process_add_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        log_skip_user = rail.WriteLogOperator(
            task_id="log_skip_user",
            log = '{{ dag_run.conf.logger}}',
            message='Skipped',
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "action": "Validation",
                "status": "Skipped",
                "details": "User Creation skipped as User status is {{ dag_run.conf.employeestatus }}."
            }
        )

        log_integration_user = rail.WriteLogOperator(
            task_id="log_integration_user",
            log = '{{ dag_run.conf.logger}}',
            message='Skipped',
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "action": "Validation",
                "status": "Skipped",
                "details": "User processing skipped as user is Used for Integration."
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "action": "Process User",
                "status": "Error",
                "details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> create_user_log

        create_user_log >> has_valid_dateformat

        has_valid_dateformat >> rail.Label('No') >> log_invalid_dateformat >> catch_and_log_error
        has_valid_dateformat >> rail.Label('Yes') >> search_user

        search_user >> check_user_in_replicon_present

        check_user_in_replicon_present >> rail.Label('Yes') >> get_my_actual_user_identity >> if_user_loginname_equal_actual_user_identity
        check_user_in_replicon_present >> rail.Label('No') >> is_user_active_for_add

        if_user_loginname_equal_actual_user_identity >> rail.Label('No') >> process_update_user >> wait_for_process_update_user >> catch_and_log_error
        if_user_loginname_equal_actual_user_identity >> rail.Label('Yes') >> log_integration_user >> catch_and_log_error

        is_user_active_for_add >> rail.Label('Yes') >> process_add_user >> wait_for_process_add_user >> catch_and_log_error
        is_user_active_for_add >> rail.Label('No') >> log_skip_user >> catch_and_log_error

        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
