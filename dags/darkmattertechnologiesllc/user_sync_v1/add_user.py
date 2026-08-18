from datetime import timedelta
from airflow.models import Variable
import rail
from darkmattertechnologiesllc.user_sync_v1.utils import request_payload
from darkmattertechnologiesllc.user_sync_v1.task.supervisor_assignment import supervisor_assignment

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.add_user_child_dagid,
        description=config.add_user_child_dagid,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.add_user_child_dag_active_runs,
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
            no_task='create_user'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_user',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_user = rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=request_payload.get_createuser_payload
        )

        assign_timeoff = rail.TriggerDagRunOperator(
            task_id='assign_timeoff',
            trigger_dag_id=config.assign_timeoff_newuser_child_dagid,
            conf={
                'useruri' : "{{ result('create_user').user.uri }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        supervisor_assignement_task = supervisor_assignment("add")

        log_adduser_success = rail.WriteLogOperator(
            task_id="log_adduser_success",
            log = '{{ dag_run.conf.logger}}',
            message='Success',
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "action": "Add",
                "status": "Success",
                "details": "User added successfully."
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='Error',
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "action": "Add",
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
        can_run_batch_task >> rail.Label('No') >> create_user

        create_user >> assign_timeoff >> supervisor_assignement_task >> log_adduser_success >> catch_and_log_error

        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
