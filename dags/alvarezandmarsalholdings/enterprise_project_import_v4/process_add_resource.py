from functools import lru_cache
import rail

@lru_cache(maxsize=None)
def load_uris(uris_artifact):
    return rail.load_all_records(uris_artifact)

def get_uri_batches(uris_artifact, batch_size):
    uris = [item['uri'] for item in load_uris(uris_artifact)]
    return [uris[i:i + batch_size] for i in range(0, len(uris), batch_size)]

def create_child_dag(config):
    add_dags = []

    for idx in range(0, config.ADD_RESOURCE_BATCH_COUNT):
        get_postfix = "" if idx == 0 else f'_batch_{idx}'

        with rail.create_airflow_dag(
            dag_id=f'{config.process_add_resource}{get_postfix}',
            description=f'{config.company_key} Enterprise Project Import - Process Add Resource Child',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_resource,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            rail.RepliconServiceCallForEachItemOperator(
                task_id='add_task_resource',
                endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
                items=lambda dag_run: get_uri_batches(dag_run.conf['uris_artifact'], config.resource_batch_size),
                data=lambda item, dag_run: {
                    "taskUri": dag_run.conf['task_uri'],
                    "resourceUris": item,
                    "isAssigned": "true"
                }
            )

        add_dags.append(dag)
    return add_dags

rail.for_each_instance(create_child_dag)
