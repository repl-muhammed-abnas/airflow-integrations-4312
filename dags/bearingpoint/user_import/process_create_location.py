from uuid import uuid4
from bearingpoint.user_import.utils import custom_methods
from airflow.models import Variable
import rail

null = None
true = True


def create_ariflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.create_locations_child_dag_id,
        description=f"BearingPoint User Import Create Locations Child {config.instance}",
        company_key=config.company_key,
        max_active_runs=config.create_locations_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_location_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="query_distinct_locations"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="query_distinct_locations",
            end_task="catch_and_log_errors"
        )

        query_distinct_locations = rail.QueryCollectionOperator(
            task_id="query_distinct_locations",
            query="""SELECT DISTINCT location_name, location_code
                FROM valid_users_data
                WHERE NULLIF(location_name, '') IS NOT NULL""",
            name="locations"
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id="get_all_locations",
            endpoint="/services/LocationService1.svc/GetAllLocations",
            data_handler=custom_methods.get_all_locations,
             target="artifact"
        )

        create_existing_locations_collections = rail.CreateCollectionOperator(
            task_id="create_existing_locations_collections",
            source='{{ result("get_all_locations") | load_all_records | to_json}}',
            name="existing_locations"
        )

        query_distinct_existing_locations = rail.QueryCollectionOperator(
            task_id="query_distinct_existing_locations",
            query="""SELECT DISTINCT location_name from existing_locations""",
            name="distinct_existing_locations"
        )

        query_new_locations = rail.QueryCollectionOperator(
            task_id="query_new_locations",
            query="""SELECT location_name, location_code
                FROM locations
                WHERE location_name NOT IN (
                    SELECT location_name
                    FROM distinct_existing_locations
            )"""
        )

        if_new_locations = rail.IfOperator(
            task_id="if_new_locations",
            test='{{result("query_new_locations", "length")>0}}',
            yes_task="create_location_in_replicon",
            no_task="location_end"
        )

        create_location_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id="create_location_in_replicon",
            items='{{result("query_new_locations")}}',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data=lambda item: {
                "location": null,
                "modifications": {
                    "name": item["location_name"],
                    "codeToApply": {
                        "value": item["location_code"]
                    },
                    "descriptionToApply": null,
                    "isEnabled": true
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        location_end = rail.EmptyOperator(task_id="location_end")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.groups_log_artifact }}',
            message="Location create failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda: {
                "employeeid": "",
                "action": "",
                "status": "Error",
                "details": "Location create failed" + custom_methods.get_error_message(),
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >>\
            query_distinct_locations >> get_all_locations >> create_existing_locations_collections >>\
            query_distinct_existing_locations >> query_new_locations >>\
            if_new_locations >> rail.Label("Yes") >> create_location_in_replicon >>\
            location_end
        if_new_locations >> rail.Label(
            "No") >> location_end >> catch_and_log_errors

        return dag


rail.for_each_instance(create_ariflow_child)
