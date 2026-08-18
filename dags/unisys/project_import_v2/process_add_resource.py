import rail

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_add_resource_dag_id,
        description=f'{config.company_key} Project Import - Process Add Resource Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.RepliconServiceOperator(
            task_id='add_task_resource',
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda dag_run: {
                "taskUri": dag_run.conf['task_uri'],
                "resourceUris": dag_run.conf['resource_uris'],
                "isAssigned": "true"
            }
        )

    return dag

rail.for_each_instance(create_child_dag)
