from datetime import timedelta
import rail
from operationalsustainability.client_sync.utils import python_callable
from operationalsustainability.client_sync.utils import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.child_dag_id,
        description= 'Sync new client in Replicon to QuickBooks| add client Child Dag',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        get_client_details = rail.RepliconServiceOperator(
            task_id = 'get_client_details',
            endpoint="/services/ClientListService1.svc/BulkGetClientDetails",
            data= {
                "clientUri": "{{ dag_run.conf.client_uri }}"
            }
        )

        create_customer_qbo = rail.InternalQuickbooksAPIOperator(
            task_id='create_customer_qbo',
            request_method='POST',
            endpoint="/customer",
            intuit_conn_id= config.qbo_conn_id,
            request_body=request_payload.create_customer_qbo_request,
            retries=3,
            retry_delay=timedelta(seconds=30)
        )

        get_client_details >> create_customer_qbo


rail.for_each_instance(create_child_dag)