import rail

from tpg.user_import.utils.response_filter import groups_filter
from tpg.user_import.utils.request_payload import get_add_location_payload

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_locations,
        description='TPG User Import - Process Locations',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_locations,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_parent_location_details = rail.RepliconServiceOperator(
            task_id="get_parent_location_details",
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:location-list-column:name",
                    "urn:replicon:location-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response, dag_run: list(filter(lambda item: item['full_path'] == dag_run.conf['parent_location_full_path'],
                groups_filter(response)))
        )

        create_new_location = rail.RepliconServiceOperator(
            task_id='create_locationorapply_modification',
            endpoint='/services/LocationService1.svc/CreateLocationOrApplyModification',
            data=get_add_location_payload
        )

        get_parent_location_details >> create_new_location

    return dag

rail.for_each_instance(create_child_dag)
