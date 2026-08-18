from pendulum import datetime
import rail


def create_deleted_webhook_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.webhook_deleted_dag_id,
        description="Webhook receiver for ProjectPolarisTaskAllocationDeleted events",
        start_date=datetime(2023, 9, 1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        webhook_conf=rail.WebhookConf(
            hmac_secret_var=config.webhook_deleted_bearer_token,
        ),
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

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
        trigger_deleted_allocation_dag = rail.TriggerDagRunOperator(
            task_id="trigger_deleted_allocation_dag",
            trigger_dag_id=config.deleted_allocation_dag_id,
            retries=0,
            conf=_build_processing_conf
        )

        trigger_deleted_allocation_dag

    return dag


rail.for_each_instance(create_deleted_webhook_dag)
