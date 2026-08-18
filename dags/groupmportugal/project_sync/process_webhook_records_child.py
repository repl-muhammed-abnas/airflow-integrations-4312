from datetime import timedelta
import rail
from groupmportugal.project_sync.utils import python_callable, request_payload

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_webhook_records_child_dag,
        description=f"Process Webhook Records child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        get_all_clients = rail.RepliconServiceOperator(
            task_id='get_all_clients',
            endpoint="/services/ClientService1.svc/GetActiveClients"
        )

        get_unique_clients = rail.PythonOperator(
            task_id='get_unique_clients',
            python_callable=python_callable.get_unique_clients
        )

        process_each_unique_clients_records = rail.trigger_parallel_dagrun(
            task_id='process_each_unique_clients_records',
            items="{{ result('get_unique_clients') | to_json }}",
            trigger_dag_id=config.add_client,
            parallel_count=config.parallel_count_clients,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, index: {
                "clienturi": item['clienturi'],
                "clientname": item['advertiser']
            }
        )

        get_all_task_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_task_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                'objectUri': "urn:replicon:object-type:task"
            },
        )

        process_each_payload_records = rail.trigger_parallel_dagrun(
            task_id='process_each_payload_records',
            items=lambda dag_run: dag_run.conf['payload']['array'],
            parallel_count=config.parallel_count,
            trigger_dag_id=config.create_update_projects,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_conf_payload
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_all_clients >> get_unique_clients >> process_each_unique_clients_records >> \
        get_all_task_custom_fields >> process_each_payload_records >> finish

    return dag

rail.for_each_instance(create_child_dag)
