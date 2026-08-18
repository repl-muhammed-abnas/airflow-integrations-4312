from uuid import uuid4
from bearingpoint.user_import.utils import custom_methods
from airflow.models import Variable
import rail

null = None
true = True


def create_ariflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.create_costcenters_child_dag_id,
        description=f"BearingPoint User Import Create Cost Centers Child {config.instance}",
        company_key=config.company_key,
        max_active_runs=config.create_costcenters_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_costcenter_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="query_distinct_costcenters"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="query_distinct_costcenters",
            end_task="catch_and_log_errors"
        )

        query_distinct_costcenters = rail.QueryCollectionOperator(
            task_id="query_distinct_costcenters",
            query="""SELECT DISTINCT costcenter_name, costcenter_code
                FROM valid_users_data WHERE NULLIF(costcenter_name, '') IS NOT NULL""",
            name="costcenters"
        )

        get_all_costcenters = rail.RepliconServiceOperator(
            task_id="get_all_costcenters",
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data_handler=custom_methods.get_all_costcenters,
            target="artifact"
        )

        create_existing_costcenters_collections = rail.CreateCollectionOperator(
            task_id="create_existing_costcenters_collections",
            source='{{ result("get_all_costcenters") | load_all_records | to_json}}',
            name="existing_costcenters"
        )

        query_distinct_existing_costcenters = rail.QueryCollectionOperator(
            task_id="query_distinct_existing_costcenters",
            query="""SELECT DISTINCT costcenter_name from existing_costcenters""",
            name="distinct_existing_costcenters"
        )

        query_new_costcenters = rail.QueryCollectionOperator(
            task_id="query_new_costcenters",
            query="""SELECT costcenter_name, costcenter_code
                FROM costcenters
                WHERE costcenter_name NOT IN (
                    SELECT costcenter_name
                    FROM distinct_existing_costcenters
            )"""
        )

        if_new_costcenters = rail.IfOperator(
            task_id="if_new_costcenters",
            test='{{result("query_new_costcenters", "length")>0}}',
            yes_task="create_costcenter_in_replicon",
            no_task="costcenter_end"
        )

        create_costcenter_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id="create_costcenter_in_replicon",
            items='{{result("query_new_costcenters")}}',
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            data=lambda item: {
                "costCenter": null,
                "modifications": {
                    "name": item["costcenter_name"],
                    "codeToApply": {
                        "value": item["costcenter_code"]
                    },
                    "descriptionToApply": null,
                    "isEnabled": true
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        costcenter_end = rail.EmptyOperator(task_id="costcenter_end")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.groups_log_artifact }}',
            message="Cost Center create failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda: {
                "employeeid": "",
                "action": "",
                "status": "Error",
                "details": "Cost Center create failed - " + custom_methods.get_error_message(),
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >>\
            query_distinct_costcenters >> get_all_costcenters >> create_existing_costcenters_collections >>\
            query_distinct_existing_costcenters >> query_new_costcenters >>\
            if_new_costcenters >> rail.Label("Yes") >> create_costcenter_in_replicon >>\
            costcenter_end
        if_new_costcenters >> rail.Label(
            "No") >> costcenter_end >> catch_and_log_errors

        return dag


rail.for_each_instance(create_ariflow_child)
