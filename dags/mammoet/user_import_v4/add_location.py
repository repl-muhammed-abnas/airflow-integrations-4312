from uuid import uuid4
import rail
from mammoet.user_import_v4.utils.response_filter import get_groups_data_handler
from mammoet.user_import_v4.utils.custom_methods import LOCATION_DELIMITER
null = None


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.user_import_add_location_child_dag_id,
        description="Mammoet User Import Process add location",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_groups_max_active_runs

    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        get_parent_location_uri = rail.RepliconServiceOperator(
            task_id="get_parent_location_uri",
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:effectively-enabled",
                    "urn:replicon:location-list-column:full-path",
                    "urn:replicon:location-list-column:code"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=lambda response, dag_run: list(filter(lambda item: item['full_path'] == dag_run.conf['parent_location_full_path'],
                                                               get_groups_data_handler(response)))
        )
        add_location_to_replicon = rail.RepliconServiceOperator(
            task_id="add_location_to_replicon",
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data=lambda dag_run: {
                "location": {
                    "name": null,
                    "uri": null,
                    "parent": {
                        "name": null,
                        "uri": rail.result('get_parent_location_uri')[0]['uri'],
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "parameterCorrelationId": null
                } if dag_run.conf['length'] != "1" else null,
                "modifications": {
                    "name": dag_run.conf['location_fullpath'].split(LOCATION_DELIMITER)[-1],
                    "codeToApply": {
                        "value": dag_run.conf['location_code']
                    },
                    "descriptionToApply": {
                        "value": dag_run.conf['parent_location_code']
                    },
                    "isEnabled": "1"
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        get_parent_location_uri >> add_location_to_replicon

    return dag


rail.for_each_instance(create_main_dag)
