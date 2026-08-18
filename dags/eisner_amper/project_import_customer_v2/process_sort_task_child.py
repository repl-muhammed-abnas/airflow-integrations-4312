import rail

null = None

def create_child_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.sort_task_child_dagid,
        description='Eisner Amper Project Data Import - Customer Records Process SORT Tasks',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_sort_tasks_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_task_sort_batch = rail.RepliconServiceOperator(
            task_id='create_task_sort_batch',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchySortBatch',
            data={
                "taskHierarchySortBatchParameter": {
                    "project": {
                        "uri": "{{dag_run.conf.project_uri}}",
                        "name": null,
                        "code": null,
                        "parameterCorrelationId": null
                        },
                    "sortOptionUri": "urn:replicon:task-sort-option:sort-by-name",
                    "isAscending": True
                }
            },
        )

        execute_batch, _ = rail.batch_execution(
            group_id='batch_execution',
            creation_task_id=create_task_sort_batch.task_id
        )

        create_task_sort_batch >> execute_batch

    return dag

rail.for_each_instance(create_child_wbs)
