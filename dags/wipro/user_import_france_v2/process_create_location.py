import json
from uuid import uuid4
from airflow.models import Variable
from wipro.user_import_france_v2.utils import custom_methods
import rail

null = None
true = True


def create_ariflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.create_location_dag_id,
        description="wipro User import process record",
        company_key=config.company_key,
        max_active_runs=config.master_max_active_run,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_location_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_process_batch_task, default_var="true").lower() == "true",
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
            query="""SELECT DISTINCT location from validuserrecords
                        WHERE NULLIF(location,"") IS NOT NULL""",
            name="locations"
        )

        create_existing_locations_collections = rail.CreateCollectionOperator(
            task_id="create_existing_locations_collections",
            source='{{dag_run.conf.location_details | to_json}}',
            name="existing_locations"
        )

        query_distinct_existing_locations = rail.QueryCollectionOperator(
            task_id="query_distinct_existing_locations",
            query="""SELECT DISTINCT location from existing_locations""",
            name="distinct_existing_locations"
        )

        query_new_locations = rail.QueryCollectionOperator(
            task_id="query_new_locations",
            query="""SELECT location FROM locations
            EXCEPT SELECT location FROM distinct_existing_locations"""
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
            data=lambda dag_run, item: {
                "location": {
                    "name": null,
                    "uri": null,
                    "parent": {
                        "name": null,
                        "uri": dag_run.conf["locationcountryuri"],
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": item["location"],
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": true
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        location_end = rail.EmptyOperator(task_id="location_end")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{dag_run.conf.lookuptable}}',
            message="Location create failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "employee_id": "",
                "employee_first_name": "",
                "employee_last_name": "",
                "country": dag_run.conf["country"],
                "company_code": "",
                "status": "Failed",
                "details": "Location create failed" + custom_methods.get_error_message(),
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >>\
            query_distinct_locations >> create_existing_locations_collections >>\
            query_distinct_existing_locations >> query_new_locations >>\
            if_new_locations >> rail.Label("Yes") >> create_location_in_replicon >>\
            location_end
        if_new_locations >> rail.Label(
            "No") >> location_end >> catch_and_log_errors

        return dag


rail.for_each_instance(create_ariflow_child)
