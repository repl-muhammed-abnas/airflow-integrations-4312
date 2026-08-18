from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_wbs_import_v2.utils import request_payload
from dxctechnology.gsap_wbs_import_v2.utils import response_filter


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_wbs_import_child_process_create_task_{config.instance}_v2',
        description='DXC_GSAP_WBS_Automation Process Create Task',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_create_task,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_parent_wbs_task_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_parent_wbs_task_details',
            end_task='finish',
        )

        get_parent_wbs_task_details = rail.RepliconServiceOperator(
            task_id="get_parent_wbs_task_details",
            endpoint="/services/TaskService1.svc/BulkGetTaskDetails",
            data={
                "taskUris": [
                    "{{dag_run.conf.parent_wbs_task_uri}}"
                ]
            }
        )

        is_level_one = rail.IfOperator(
            task_id="is_level_one",
            test="{{dag_run.conf.level == '1'}}",
            yes_task="create_task",
            no_task="get_parent_task_details"
        )

        get_parent_task_details = rail.RepliconServiceOperator(
            task_id='get_parent_task_details',
            endpoint='/services/TaskListService1.svc/GetData',
            data=lambda dag_run: request_payload.get_all_project_tasks_payload(
                dag_run.conf['processing_wbs_uri']),
            data_handler=response_filter.get_specific_task_details
        )

        create_task = rail.RepliconServiceOperator(
            task_id="create_task",
            endpoint="services/ProjectService1.svc/PutTask",
            data=request_payload.get_put_task_payload
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_fail_dag = rail.IfOperator(
            task_id = "can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task= "fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id = "fail_dagrun",
            message='{{ get_error_message() }}'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_parent_wbs_task_details

        get_parent_wbs_task_details >> is_level_one >> rail.Label(
            'Yes') >> create_task
        is_level_one >> rail.Label(
            'Yes') >> get_parent_task_details >> create_task >> finish >> log_to_sumo >> can_fail_dag

        can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_child_dag_wbs)
