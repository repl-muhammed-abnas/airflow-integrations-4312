from datetime import timedelta
from uuid import uuid4
from airflow.models import Variable
import rail
null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sigroup_costcenters_dag_id,
       description="sigroup user import costcenters child",
        max_active_runs=config.master_max_active_runs,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(config.sigroup_batch_task_flag, "true").lower() == "true",
            yes_task="batch_task",
            no_task="get_enabled_costcenters"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task="get_enabled_costcenters",
            end_task="batch_end"
        )

        get_enabled_costcenters = rail.RepliconServiceOperator(
            task_id="get_enabled_costcenters",
            endpoint="/services/CostCenterService1.svc/GetEnabledCostCenters"
        )

        create_existing_costcenters_collection = rail.CreateCollectionOperator(
            task_id="create_existing_costcenters_collection",
            source='{{result("get_enabled_costcenters")|to_json}}',
            name="existing_costcenters"
        )

        query_new_costcenters = rail.QueryCollectionOperator(
            task_id="query_new_costcenters",
            query="""SELECT * FROM query_costcenters_from_feed_file WHERE financecostcenter NOT IN
            (SELECT DISTINCT displayText from existing_costcenters )"""
        )

        if_new_costcenters = rail.IfOperator(
            task_id="if_new_costcenters",
            test='{{result("query_new_costcenters", "length") > 0}}',
            yes_task="create_new_costcenters_in_replicon",
            no_task="batch_end"
        )

        create_new_costcenters_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id="create_new_costcenters_in_replicon",
            items='{{result("query_new_costcenters")}}',
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            data={
                    "division": null,
                    "modifications": {
                        "name": '{{item.financecostcenter}}',
                        "codeToApply": {
                            "value": '{{item.financecostcentercode}}'
                        },
                        "descriptionToApply": null,
                        "isEnabled": "true"
                    },
                "unitOfWorkId": str(uuid4())
            }
        )

        batch_end = rail.EmptyOperator(task_id="batch_end")

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> batch_end
        can_run_batch_task >> rail.Label("No") >>\
        get_enabled_costcenters >>\
            create_existing_costcenters_collection >>\
            query_new_costcenters >>\
            if_new_costcenters >> rail.Label("Yes") >>\
            create_new_costcenters_in_replicon >> batch_end
        if_new_costcenters >> rail.Label("No") >> batch_end

        return dag


rail.for_each_instance(create_airflow_dag)
