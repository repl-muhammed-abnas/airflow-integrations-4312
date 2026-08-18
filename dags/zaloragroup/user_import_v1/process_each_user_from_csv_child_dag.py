from datetime import timedelta
from airflow.models import Variable
import rail
from zaloragroup.user_import_v1.utils import python_callable_method
from zaloragroup.user_import_v1.utils import request_payload

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'zaloragroup_user_import_process_each_user_from_csv_child_{config.instance}_v1',
        description=f'zaloragroup_user_import_process_each_user_from_csv_child_{config.instance}_v1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_each_user_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config_child", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='check_login_name_provided'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_login_name_provided',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        check_login_name_provided = rail.IfOperator(
            task_id = "check_login_name_provided",
            test = "{{ dag_run.conf.loginname | is_truthy}}",
            yes_task = "check_user_present",
            no_task = "log_no_loginname"
        )

        log_no_loginname = rail.WriteLogOperator(
            task_id='log_no_loginname',
            log = "{{ dag_run.conf.logger }}",
            message="na",
            severity="Exception",
            properties={
                "login_name": "",
                "status": "Exception",
                "failure_reason": "Loginname not found for Employee ID : {{ dag_run.conf.employeeid }}, in the feed file."
            }
        )

        check_user_present = rail.RepliconServiceOperator(
            task_id='check_user_present',
            endpoint="/services/UserListService1.svc/GetData",
            data = request_payload.get_userdetails,
            data_handler = python_callable_method.get_user_uri_by_loginname
        )

        is_useruri_present = rail.IfOperator(
            task_id='is_useruri_present',
            test="{{ result('check_user_present') | is_truthy }}",
            yes_task='update_user_from_import',
            no_task='add_user_from_import'
        )

        update_user_from_import = rail.TriggerDagRunOperator(
            task_id='update_user_from_import',
            trigger_dag_id=f'zaloragroup_user_import_update_user_child_{config.instance}_v1',
            conf=request_payload.process_update_user_data,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_user',
            dag_runs='{{ result("update_user_from_import") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        add_user_from_import = rail.TriggerDagRunOperator(
            task_id='add_user_from_import',
            trigger_dag_id=f'zaloragroup_user_import_add_user_child_{config.instance}_v1',
            conf=request_payload.process_add_user_data,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_user',
            dag_runs='{{ result("add_user_from_import") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )


        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> check_login_name_provided

        check_login_name_provided >> rail.Label('No') >> log_no_loginname >> catch_and_log_error
        check_login_name_provided >> rail.Label('Yes') >> check_user_present >> is_useruri_present
        is_useruri_present >> rail.Label('Yes') >> update_user_from_import >> wait_for_update_user >> catch_and_log_error
        is_useruri_present >> rail.Label('No') >> add_user_from_import >> wait_for_add_user >> catch_and_log_error

        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
