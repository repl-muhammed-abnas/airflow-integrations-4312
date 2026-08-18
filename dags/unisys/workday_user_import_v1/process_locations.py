"""
Process Locations - Unisys Workday User Import Child DAG

Creates location hierarchies in Replicon based on Workday location data.
This child DAG is triggered for each location hierarchy that needs to be created or modified.

Key features:
    - Creates hierarchical location structures
    - Applies modifications to existing locations
    - Processes locations from Workday export

Functions:
    create_child_dag(config): Creates the process locations child DAG
"""
import uuid
import rail
from unisys.workday_user_import_v1.utils import request_payload

null = None

def create_child_dag(config):
    """
    Create child DAG for processing location hierarchies.

    This DAG creates or modifies location hierarchies in Replicon using the
    LocationService1.svc/CreateLocationHierarchyOrApplyModifications endpoint.
    Expected to receive location data via dag_run.conf.

    Args:
        config: Configuration object containing DAG settings including:
            - process_new_locations: DAG ID for this child DAG
            - company_key: Replicon company identifier
            - replicon_conn_id: Replicon connection ID
            - max_active_runs_process_locations: Max parallel DAG runs

    Returns:
        DAG: Configured Airflow DAG object for location processing

    DAG Configuration:
        dag_run.conf should contain:
            - full_path: Pipe-separated location hierarchy (e.g., "Country|State|City")
            - location_description: Location code/description
    """
    with rail.create_airflow_dag(
        dag_id=config.process_new_locations,
        description='Unisys Workday User Import - Process Locations',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_locations,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_location_hierarchy = rail.RepliconServiceOperator(
            task_id="create_location_hierarchy",
            endpoint="/services/LocationService1.svc/CreateLocationHierarchyOrApplyModifications",
            data=request_payload.get_locations_hierarchy_payload
        )

    return dag

rail.for_each_instance(create_child_dag)
