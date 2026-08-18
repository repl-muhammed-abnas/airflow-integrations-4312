from pendulum import datetime
import rail
from unisys.resource_assignment_export_v1.utils import custom_methods, request_payload

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.allocation_details_child_dag_id,
        description=f'Unisys Resource Assignment Export Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs= config.max_active_runs_child
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_webhook_payload")

        # Check if event is deleted
        is_deleted = rail.IfOperator(
            task_id="is_deleted",
            test="{{ dag_run.conf.event_type == 'Deleted' }}",
            yes_task="prepare_allocation_row",
            no_task="get_allocation_details_graphql"
        )

        # For non-deleted events, get allocation details via GraphQL
        get_allocation_details_graphql = rail.RepliconServiceOperator(
            task_id="get_allocation_details_graphql",
            endpoint="graphql",
            app='polaris',
            data=request_payload.get_allocation_details_graphql_query,
            data_handler=custom_methods.extract_resource_allocations_from_graphql
        )

        # Prepare CSV row for active (Created/Modified) allocations
        prepare_allocation_row = rail.PythonOperator(
            task_id="prepare_allocation_row",
            python_callable=custom_methods.prepare_csv_row_active
        )

        # Active allocation path
        is_deleted >> rail.Label("No") >> get_allocation_details_graphql >> prepare_allocation_row

        # Deleted allocation path
        is_deleted >> rail.Label("Yes") >> prepare_allocation_row

    return dag

rail.for_each_instance(create_dag)
