from datetime import timedelta
import rail

def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_remove_resource,
        description=f'{config.company_key} Enterprise Project Import - Process Remove Resource Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_resource,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.RepliconServiceOperator(
            task_id="remove_task_resource",
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda dag_run: {
                "taskUri": dag_run.conf['task_uri'],
                "resourceUris": dag_run.conf['uris'],
                "isAssigned": "false"
            }
        )

    return dag

rail.for_each_instance(create_child_dag)
