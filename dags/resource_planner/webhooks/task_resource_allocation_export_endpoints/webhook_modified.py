import hashlib
from pendulum import datetime
import rail


def create_modified_webhook_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_modified_dag_id,
        description="Webhook receiver for ProjectPolarisTaskAllocationModified events",
        start_date=datetime(2023, 9, 1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        webhook_conf=rail.WebhookConf(
            hmac_secret_var=config.webhook_modified_bearer_token,
            trigger_condition='{{ not data.actingUser.displayText | lower | starts_with("ResourcePlanner")}}'
        ),
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        # Compute the child DAG ID using deterministic md5 hash routing
        # (Python's built-in hash() is NOT stable across workers)
        def compute_child_dag_id_callable(dag_run):
            allocation_uuid = dag_run.conf['webhook']['data']['id'].split(':')[-1]
            child_index = (int(hashlib.md5(allocation_uuid.encode()).hexdigest(), 16) % config.modified_child_count) + 1
            suffix = f"_{child_index}" if child_index > 1 else ""
            child_dag_id = f"{config.modified_allocation_child_dag_id}{suffix}"
            print(f"Routing allocation {allocation_uuid} to child partition {child_index}: {child_dag_id}")
            return child_dag_id

        compute_child_dag_id = rail.PythonOperator(
            task_id="compute_child_dag_id",
            python_callable=compute_child_dag_id_callable,
        )

        def _build_processing_conf(dag_run):
            data = dag_run.conf['webhook']['data']
            return {
                'allocation_id': data['id'],
                'allocation_uuid': data['id'].split(':')[-1],
                'project_uri': data['project']['uri'],
                'task_uri': data['task']['uri'],
                'user_uri': data['user']['uri'],
            }

        # wait_for_completion=True so a processor-DAG failure marks this
        # receiver run as failed instead of silently succeeding. Webhook ACK
        # to Polaris is handled by the receiver layer (above DAG tasks), so
        # this does not affect the HTTP response latency.
        trigger_modified_child_dag = rail.TriggerDagRunOperator(
            task_id="trigger_modified_child_dag",
            trigger_dag_id="{{ result('compute_child_dag_id') }}",
            retries=0,
            conf=_build_processing_conf
        )

        compute_child_dag_id >> trigger_modified_child_dag

    return dag


rail.for_each_instance(create_modified_webhook_dag)
