import rail
from dxctechnology.compass_iwo_details_v1.utils import request_payload
from dxctechnology.compass_iwo_details_v1.utils.custom_methods import get_specific_task_details


def create_iwo_details_process_each_task_per_level_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_iwo_create_task_{config.dag_id_postfix}',
        description=f'DXC COMPASS IWO WBS create task {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

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
            response_filter=get_specific_task_details
        )

        create_task = rail.RepliconServiceOperator(
            task_id="create_task",
            endpoint="services/ProjectService1.svc/PutTask",
            data=request_payload.get_put_task_payload
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        get_parent_wbs_task_details >> is_level_one >> rail.Label(
            "Yes") >> create_task
        is_level_one >> rail.Label(
            "No") >> get_parent_task_details >> create_task >> log_to_sumo
    return dag


rail.for_each_instance(
    create_iwo_details_process_each_task_per_level_child_dag)
