from datetime import timedelta
import rail
from airflow.models import Variable
from eisner_amper.project_import_internal_v2.utils import request_payload

null = None

# pylint: disable=too-many-statements
def create_child_dag_wbs(config):
    append_dags = []

    for idx in range(0, config.TASK_BATCH_COUNT):
        with rail.create_airflow_dag(
            dag_id=f"{config.process_each_task}_batch_{idx+1}",
            description='Eisner Amper Project Data Import - Internal Records Process Each Project',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_tasks,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='has_mandatory_task_fields'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='has_mandatory_task_fields',
                end_task='catch_and_log_errors',
            )

            has_mandatory_task_fields = rail.IfOperator(
                task_id='has_mandatory_task_fields',
                test=request_payload.get_all_mandatory_check_tasks,
                yes_task="is_task_present_in_replicon",
                no_task="log_mandatory_task_fields_not_present"
            )

            is_task_present_in_replicon = rail.IfOperator(
                task_id='is_task_present_in_replicon',
                test=lambda dag_run: bool(dag_run.conf['task_details']),
                yes_task="update_task",
                no_task="create_task"
            )

            create_task = rail.RepliconServiceOperator(
                task_id='create_task',
                endpoint='/services/ProjectService1.svc/PutTask',
                data=request_payload.get_put_task_data
            )

            update_task= rail.RepliconServiceOperator(
                task_id="update_task",
                endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
                data=request_payload.get_update_task_payload
            )

            log_task_successfull = rail.WriteLogOperator(
                task_id='log_task_successfull',
                log='{{ dag_run.conf.project_log }}',
                message=lambda dag_run: 'Task Added Succesfully' if not bool(dag_run.conf['task_details']) else 'Task Updated Succesfully',
                severity='Success',
                properties=lambda dag_run: {
                    'clientcode': dag_run.conf['client_code'],
                    'projectcode': dag_run.conf['project_code'],
                    'taskname': dag_run.conf["task_name"],
                    'taskcode': dag_run.conf["task_code"],
                    'action': 'Add' if not bool(dag_run.conf['task_details']) else 'Update',
                    'status': 'Success',
                }
            )

            log_mandatory_task_fields_not_present = rail.WriteLogOperator(
                task_id='log_mandatory_task_fields_not_present',
                log='{{ dag_run.conf.project_log }}',
                message=request_payload.get_exception_message_tasks,
                severity='Exception',
                properties=lambda dag_run: {
                    'clientcode': dag_run.conf['client_code'],
                    'projectcode': dag_run.conf['project_code'],
                    'taskname': dag_run.conf["task_name"],
                    'taskcode': dag_run.conf["task_code"],
                    'action': 'Validation',
                    'status': 'Exception',
                }
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log='{{ dag_run.conf.project_log }}',
                trigger_rule='one_failed',
                severity='Error',
                message='{{ get_error_message() }}',
                properties=lambda dag_run: {
                    'clientcode': dag_run.conf['client_code'],
                    'projectcode': dag_run.conf['project_code'],
                    'taskname': dag_run.conf["task_name"],
                    'taskcode': dag_run.conf["task_code"],
                    'action': 'Sync',
                    'status': 'Error',
                }
            )

            log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id='log_to_sumo',
                sumo_conn_id='sumologic-dagrunlogger',
                trigger_rule='all_done',
            )


            can_run_batch_task >> rail.Label(
                "Yes") >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label("No") >> has_mandatory_task_fields

            has_mandatory_task_fields >> rail.Label('No') >> log_mandatory_task_fields_not_present >> catch_and_log_errors
            has_mandatory_task_fields >> rail.Label('No') >> is_task_present_in_replicon

            is_task_present_in_replicon >> rail.Label('Yes') >> update_task >> log_task_successfull
            is_task_present_in_replicon >> rail.Label('No') >> create_task >> log_task_successfull
            log_task_successfull >> catch_and_log_errors >> log_to_sumo

        append_dags.append(dag)

    return append_dags

rail.for_each_instance(create_child_dag_wbs)
