from datetime import timedelta
from pendulum import datetime
import rail
from mammoet.user_import_v1.utils import request_payload
from airflow.models import Variable


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.user_import_indirect_employee_add_users_child_dag_id,
        description="Mammoet User Import Process Add User",
        start_date=datetime(2023, 9, 1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_add_user_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_user',
            end_task='catch_and_log_error',
        )

        create_user = rail.RepliconServiceOperator(
            task_id="create_user",
            endpoint="/services/importService1.svc/PutUser3",
            data=request_payload.get_indirect_add_user_payload
        )

        log_user_created = rail.WriteLogOperator(
            task_id="log_user_created",
            severity="Success",
            message="User created successfully",
            log="{{dag_run.conf.log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "status": "Success",
                "action": "Add",
                "details": "User created successfully"
            }
        )

        remove_timeoff_assignments = rail.RepliconServiceOperator(
            task_id="remove_timeoff_assignments",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "timeOffTypeUris": []
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            severity="Error",
            message="{{get_error_message()}}",
            trigger_rule='one_failed',
            log="{{dag_run.conf.log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "status": "Error",
                "action": "Add",
                "details": "{{get_error_message()}}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule="all_done"
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> create_user

        create_user >> log_user_created >> remove_timeoff_assignments\
            >> rail.Label("On Error") >> catch_and_log_error >> log_to_sumo
    return dag


rail.for_each_instance(create_main_dag)
