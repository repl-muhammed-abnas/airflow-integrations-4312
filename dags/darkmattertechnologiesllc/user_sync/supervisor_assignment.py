from datetime import timedelta
from airflow.models import Variable
import rail
from darkmattertechnologiesllc.user_sync.task.supervisor_assignment import supervisor_assignment

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'darkmattertechnologiesllc_usersync_update_supervisor_child_{config.instance}',
        description=f'darkmattertechnologiesllc_usersync_update_supervisor_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.assign_supervisor_child_dag_active_runs,
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
            no_task='supervisor_assignment_start'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='supervisor_assignment_start',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        supervisor_assignment_start = rail.EmptyOperator(
            task_id = "supervisor_assignment_start"
        )

        supervisor_assignement_task = supervisor_assignment("{{ dag_run.conf.caller }}", can_queue_assignment=False)

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "action": "{{ dag_run.conf.caller }}",
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
        can_run_batch_task >> rail.Label('No') >> supervisor_assignment_start

        supervisor_assignment_start >> supervisor_assignement_task >> catch_and_log_error

        catch_and_log_error >> log_to_sumo


    return dag

rail.for_each_instance(create_dag)
