from datetime import datetime, timedelta
from airflow.utils.edgemodifier import Label

import rail
from dxctechnology.ppmc_project_and_tasks_import import request_payload

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/ppmc_project_and_tasks_import/config.py


# pylint: disable=too-many-statements
def create_child_task_process_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_ppmc_project_task_import_child_task_process{dag_id_postfix}',
        description=f'DXC PPMC Tasks_Process each PPMC project {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        start_date=datetime(2022, 1, 1)
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_tasklistperproject_collection = rail.CreateCollectionOperator(
            task_id="create_tasklistperproject_collection",
            name="tasklistperproject",
            source=lambda: request_payload.get_dag_run_conf()['task']
        )

        query_distinct_task = rail.QueryCollectionOperator(
            task_id="query_distinct_task",
            name="query_distinct_task",
            query="""SELECT * FROM
                        tasklistperproject
                     WHERE
                        task2enddate != '' AND task2enddate IS NOT NULL AND
                        task2startdate != '' AND task2startdate IS NOT NULL AND 
                        task2code != '' AND task2code IS NOT NULL
                     GROUP BY
                        task2code
                    """
        )

        query_invalid_record = rail.QueryCollectionOperator(
            task_id="query_invalid_record",
            name="query_invalid_record",
            query="""SELECT * FROM
                            tasklistperproject
                        WHERE
                            task2enddate == '' OR task2enddate IS  NULL OR
                            task2startdate == '' OR task2startdate IS  NULL OR
                            task2code == '' AND task2code IS  NULL
                        GROUP BY
                            task2name
                    """
        )

        has_invalid_record = rail.IfOperator(
            task_id="has_invalid_record",
            test="{{ result('query_invalid_record','length')  > 0 }}",
            yes_task="log_validation_error",
            no_task="has_attr_list",
        )

        log_validation_error = rail.WriteLogOperator(
            task_id='log_validation_error',
            message='PPMC project Start Date or End Date or project name is not present',
            items='{{ result("query_invalid_record") }}',
            properties={
                'wbs': '{{ dag_run.conf.wbsname }}',
                'task': '{{ dag_run.conf.systemid }} - {{ item.task1code }} - {{ item.task2code }}',
                'message': 'PPMC project Start Date or End Date or name is not present',
                'status': 'Exception',
            }
        )

        has_attr_list = rail.IfOperator(
            task_id="has_attr_list",
            test="{{ dag_run.conf.attrlist | length  > 0 }}",
            yes_task="get_children_tasks",
            no_task="process_task",
        )

        process_task = rail.TriggerDagRunForEachItemOperator(
            task_id='process_task',
            retries=0,
            items="{{ result('query_distinct_task') }}",
            trigger_dag_id=lambda item: request_payload.get_process_task_dag_id(
                dag_id_postfix, item),
            execution_timeout=timedelta(days=14),
            conf=request_payload.get_process_task_conf
        )

        wait_for_process_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_task',
            dag_runs='{{ result("process_task") }}',
            execution_timeout=timedelta(days=14),
        )

        get_children_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_children_tasks',
            endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
            data={'parentUri': '{{ item.TaskUri}}'},
            execution_timeout=timedelta(days=14),
            items=lambda: request_payload.get_dag_run_conf()['attrlist']
        )

        map_attr_task = rail.PythonOperator(
            task_id='map_attr_task',
            python_callable=request_payload.map_attr_task,
        )

        def get_process_update_child_task_conf(item):
            conf = request_payload.get_dag_run_conf()
            return {
                'name': f'{conf["systemid"]}-{item["task1code"]}-{item["task2code"]}',
                'description': item['task2name'],
                'startdate': item['task2startdate'],
                'enddate': item['task2enddate'],
                'projecturi': conf['projecturi'],
                'taskuri': item['taskuri'],  # for add / update
                'attrlist': conf['attrlist'],
                'wbsparenttaskuri': item['wbsparenttaskuri'],
                'attributesparenttaskuri': None,
                'tasktypeuri': conf['tasktypeuri'],
                'tasktypeoption_ppmcproject': conf['tasktypeoption_ppmcproject'],
                'tasktypeoption_ppmctask': conf['tasktypeoption_ppmctask'],
                'wbsname': conf['wbsname'],
                'taskdata': None,
                'aid': item['aid'],
                'aid_udfuri': item['aid_udfuri'],
                'task2estimatedhours': item['task2estimatedhours'],
                'resourceuris': request_payload.get_resource_uris(item)
            }

        process_update_child_task = rail.TriggerDagRunForEachItemOperator(
            task_id='process_update_child_task',
            retries=0,
            items=lambda: rail.result('map_attr_task'),
            execution_timeout=timedelta(days=14),
            trigger_dag_id=lambda item: f'dxctechnology_ppmc_project_task_import_child_task_update{dag_id_postfix}' if item['taskuri']
            else f'dxctechnology_ppmc_project_task_import_child_task_create{dag_id_postfix}',
            conf=get_process_update_child_task_conf
        )

        wait_for_process_update_child_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_child_task',
            dag_runs='{{ result("process_update_child_task") }}',
            execution_timeout=timedelta(days=7),
        )

        create_tasklistperproject_collection >> query_distinct_task >> query_invalid_record >> has_invalid_record

        has_invalid_record >> Label(
            "Yes") >> log_validation_error >> has_attr_list
        has_invalid_record >> Label("No") >> has_attr_list

        has_attr_list >> Label(
            "Yes") >> get_children_tasks >> map_attr_task >> process_update_child_task >> wait_for_process_update_child_task
        has_attr_list >> Label("No") >> process_task >> wait_for_process_task

    return dag


rail.for_each_instance(create_child_task_process_dag)
