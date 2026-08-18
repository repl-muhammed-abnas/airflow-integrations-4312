from uuid import uuid4
import rail
from darkmattertechnologiesllc.user_sync.utils.python_callable import get_all_group_data_from_replicon_filter
from darkmattertechnologiesllc.user_sync.utils import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'darkmattertechnologiesllc_usersync_add_location_child_{config.instance}',
        description=f'darkmattertechnologiesllc_usersync_add_location_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_groups_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_conf")

        get_parent_location_details = rail.RepliconServiceOperator(
            task_id="get_parent_location_details",
            endpoint="/services/LocationListService1.svc/GetData",
            data=request_payload.get_location_payload,
            data_handler=lambda response, dag_run: list(filter(lambda item: item['full_path'] == dag_run.conf['parent_location_full_path'],
                                                               get_all_group_data_from_replicon_filter(response)))
        )

        create_new_location = rail.RepliconServiceOperator(
            task_id="create_new_location",
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data=lambda dag_run: {
                "location": {
                    "parent": {
                        "uri": rail.result("get_parent_location_details")[0]['uri']
                    }
                } if dag_run.conf['length'] != '1' else None,
                "modifications": {
                    "name": dag_run.conf['name'],
                    "isEnabled": 1
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        get_parent_location_details >> create_new_location

    return dag

rail.for_each_instance(create_child_dag)
