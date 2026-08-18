from datetime import timedelta
from uuid import UUID
from pendulum import datetime
import rail


def _create_single_webhook_master_dag(config, postfix=""):

    with rail.create_airflow_dag(
        dag_id=f"{config.master_dag_id}{postfix}",
        description="Galaxy US Opco Inc Time Entry Sync - Webhook Receiver",
        start_date=datetime(2026, 2, 1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        webhook_conf=[
            rail.WebhookConf(bearer_token_var=f"{config.galaxyusopcoinc_time_import_bearer_token_var}{postfix}")
        ]
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        def _get_batch_index(dag_run):
            payload = dag_run.conf.get('webhook', {}).get('data', {})
            oef = payload.get('oef', [])
            if oef and oef[0].get('name') == 'Processing Counter' and oef[0].get('value'):
                value = str(oef[0]['value'])
                try:
                    if value.isnumeric():
                        return int(value) % config.TOTAL_BATCHES
                    return UUID(value).int % config.TOTAL_BATCHES
                except (ValueError, TypeError, AttributeError):
                    return 0
            payload_id = payload.get('payloadid', '')
            try:
                return UUID(payload_id).int % config.TOTAL_BATCHES
            except (ValueError, AttributeError):
                return 0

        def get_batch_child_dag_id(dag_run):
            batch_index = _get_batch_index(dag_run)
            if batch_index == 0:
                return config.child_dag_id
            return f"{config.child_dag_id}_batch_{batch_index}"

        def get_time_entry_data(dag_run):
            payload = dag_run.conf.get('webhook', {}).get('data', {})
            oef = payload.get('oef', [])
            if oef and oef[0].get('name') == 'Processing Counter':
                return {**payload, 'oef': oef[1:]}
            return payload

        rail.TriggerDagRunForEachItemOperator(
            task_id='process_timedata',
            items=[0],
            trigger_dag_id=get_batch_child_dag_id,
            conf=lambda dag_run: {
                "time_entry_data": get_time_entry_data(dag_run),
                "master_ecid": dag_run.conf['_ecid'],
                "webhook_headers": dag_run.conf.get('webhook', {}).get('headers', {})
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

    return dag


def create_main_dag(config):
    add_dags = []
    for batch_idx in range(0, config.TOTAL_WEBHOOK_ENDPOINTS):
        postfix = "" if batch_idx == 0 else f'_{batch_idx}'
        add_dags.append(_create_single_webhook_master_dag(config, postfix))
    return add_dags


rail.for_each_instance(create_main_dag)
