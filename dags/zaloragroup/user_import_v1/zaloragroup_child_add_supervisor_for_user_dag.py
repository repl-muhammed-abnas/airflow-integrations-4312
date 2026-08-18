
from datetime import timedelta
from airflow.models import Variable
import rail
from zaloragroup.user_import_v1.utils import python_callable_method
from zaloragroup.user_import_v1.utils import request_payload

# pylint: disable=too-many-statements, line-too-long

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'zaloragroup_user_import_update_supervisor_from_logs_child_{config.instance}_v1',
        description=f'zaloragroup_user_import_update_supervisor_from_logs_child_{config.instance}_v1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.update_supervisor_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_details',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/UserListService1.svc/GetData",
            data = request_payload.get_userdetails,
            data_handler = python_callable_method.get_user_uri_by_loginname
        )

        if_request_supervisor_present = rail.IfOperator(
            task_id='if_request_supervisor_present',
            test="{{ dag_run.conf.initialsupervisorloginname | is_truthy }}",
            yes_task="if_request_loginname_not_equals_request_initialsupervisorloginname",
            no_task="catch_and_log_error",
        )

        if_request_loginname_not_equals_request_initialsupervisorloginname = rail.IfOperator(
            task_id='if_request_loginname_not_equals_request_initialsupervisorloginname',
            test="{{ dag_run.conf.loginname != dag_run.conf.initialsupervisorloginname }}",
            yes_task="check_if_supervisor_available",
            no_task="catch_and_log_error",
        )

        check_if_supervisor_available = rail.RepliconServiceOperator(
            task_id='check_if_supervisor_available',
            endpoint="/services/UserListService1.svc/GetData",
            data = request_payload.get_supervisordetails,
            data_handler = python_callable_method.get_supervisor_uri_by_loginname
        )

        is_supervisor_uri_present = rail.IfOperator(
            task_id='is_supervisor_uri_present',
            test="{{ result('check_if_supervisor_available') | is_truthy }}",
            yes_task='update_supervisor_for_user',
            no_task='no_supervisor_log_failure'
        )

        update_supervisor_for_user = rail.RepliconServiceOperator(
            task_id='update_supervisor_for_user',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=request_payload.update_supervisor_from_mapper
        )

        no_supervisor_log_failure = rail.WriteLogOperator(
            task_id='no_supervisor_log_failure',
            log = "{{ dag_run.conf.logger }}",
            message="na",
            severity="Error",
            properties={
                "login_name": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                "failure_reason": "Supervisor not updated for user \"{{ dag_run.conf.username }}\" as supervisor with \
                    login name \"{{ dag_run.conf.supervisorid }}\" not available in Replicon"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = "{{ dag_run.conf.logger }}",
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "login_name": "{{dag_run.conf.loginname}}",
                "status": "Error",
                "failure_reason": 'User profile not updated successfully. Error: {{ get_error_message() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_user_details

        get_user_details >> if_request_supervisor_present
        if_request_supervisor_present >> rail.Label(
            'Yes') >> if_request_loginname_not_equals_request_initialsupervisorloginname
        if_request_supervisor_present >> rail.Label(
            'No') >> catch_and_log_error
        if_request_loginname_not_equals_request_initialsupervisorloginname >> rail.Label(
            'Yes') >> check_if_supervisor_available >> is_supervisor_uri_present
        if_request_loginname_not_equals_request_initialsupervisorloginname >> rail.Label(
            'No') >> catch_and_log_error
        is_supervisor_uri_present >> rail.Label(
            'Yes') >> update_supervisor_for_user >> catch_and_log_error
        is_supervisor_uri_present >> rail.Label(
            'No') >> no_supervisor_log_failure >> catch_and_log_error
        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
