from datetime import timedelta
import rail


def create_iwo_details_process_each_task_per_level_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_iwo_process_tasks_by_level_child_{config.dag_id_postfix}',
        description=f'DXC COMPASS IWO WBS process task by level {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_all_tasks_for_level = rail.QueryCollectionOperator(
            task_id="get_all_tasks_for_level",
            query="SELECT * FROM query_tasks_not_present_in_child_project WHERE levels = :levels",
            query_params={
                "levels": "{{dag_run.conf.level}}"
            }
        )

        create_tasks = rail.TriggerDagRunForEachItemOperator(
            task_id="create_tasks",
            trigger_dag_id=f'dxctechnology_compass_iwo_create_task_{config.dag_id_postfix}',
            items="{{result('get_all_tasks_for_level')}}",
            conf=lambda item, dag_run: {
                "level": item['levels'],
                "taskname": item['taskname'],
                "task_full_path": item['task_fullpath'],
                "parent": item['parent_task_name'] if item['parent_present'] else '',
                "code": item['code'],
                "start_date": item['start_date'],
                "end_date": item['end_date'],
                "resources": dag_run.conf['resource_list'],
                "parent_wbs_task_uri": item['uri'],
                "parent_wbs": dag_run.conf["parent_wbs"],
                "processing_wbs_uri": dag_run.conf['processing_wbs_uri'],
                "processing_wbs": dag_run.conf['processing_wbs'],
                "task_type": dag_run.conf['task_type']
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_create_tasks = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_tasks_by_level",
            dag_runs="{{result('create_tasks')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        get_all_tasks_for_level >> create_tasks >> wait_for_create_tasks >> log_to_sumo

    return dag


rail.for_each_instance(
    create_iwo_details_process_each_task_per_level_child_dag)
