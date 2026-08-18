"""D365 Enquiry webhook — triggers enquiry sync child DAG."""
import rail


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.enquiry_webhook_dag_id,
        description='D365 PIM Enquiry webhook endpoint',
        max_active_runs=config.max_active_runs,
        integration_type='generic',
        company_key=config.company_key,
        replicon_conn_id=None,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var,
        ),
    ) as dag:

        view_conf = rail.ViewDagRunConfOperator(task_id='view_dag_run_conf')

        def _get_child_conf(dag_run):
            data = dag_run.conf.get('webhook', {}).get('data', {})
            return {
                'entity_guid': data.get(
                    'PrimaryEntityId', data.get('entity_guid', '')
                ),
            }

        trigger_child = rail.TriggerDagRunOperator(
            task_id='trigger_enquiry_sync',
            trigger_dag_id=config.enquiry_sync_child_dag_id,
            conf=_get_child_conf,
        )

        view_conf >> trigger_child

        return dag


rail.for_each_instance(create_dag)
