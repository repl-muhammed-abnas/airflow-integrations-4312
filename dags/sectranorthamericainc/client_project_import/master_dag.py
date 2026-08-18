from datetime import timedelta
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Sectranorthamerica_client_project_import_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
        webhook_conf=[rail.WebhookConf(
            bearer_token_var=config.sectranorthamerica_webhook_bearer_token_var)],
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.TriggerDagRunOperator(
            task_id="trigger_sectranorthamerica_client_project_import_child",
            trigger_dag_id=config.child_dag_id,
            conf=lambda dag_run: {
                **dag_run.config
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

    return dag


rail.for_each_instance(create_dag)
