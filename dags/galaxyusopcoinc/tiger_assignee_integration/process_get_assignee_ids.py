import rail
from galaxyusopcoinc.tiger_assignee_integration.utils import request_payload
from galaxyusopcoinc.tiger_assignee_integration.utils import response_filter

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_tiger_assignee_integration_child_process_get_assignee_details_{config.instance}',
        description='Vialto Partners Tiger Assignee Integration Process GET Assignee Details',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_get_assignee_details,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.RepliconServiceOperator(
            task_id = "get_assignee_details",
            endpoint='/services/ObjectExtensionTagListService1.svc/GetData',
            data=request_payload.get_assignee_details_data,
            data_handler=response_filter.get_filtered_assignee_details
        )

    return dag


rail.for_each_instance(create_child_dag_wbs)
