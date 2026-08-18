from datetime import timedelta
from uuid import uuid4
from airflow.models import Variable
import rail
null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sigroup_legal_employers_dag_id,
       description="sigroup user import legal_employers child",
        max_active_runs=config.master_max_active_runs,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(config.sigroup_batch_task_flag, "true").lower() == "true",
            yes_task="batch_task",
            no_task="get_enabled_legal_employers"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task="get_enabled_legal_employers",
            end_task="batch_end"
        )

        get_enabled_legal_employers = rail.RepliconServiceOperator(
            task_id="get_enabled_legal_employers",
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters"
        )

        create_existing_legal_employers_collection = rail.CreateCollectionOperator(
            task_id="create_existing_legal_employers_collection",
            source='{{result("get_enabled_legal_employers")|to_json}}',
            name="existing_legal_employers"
        )

        query_new_legal_employers = rail.QueryCollectionOperator(
            task_id="query_new_legal_employers",
            query="""SELECT * FROM query_legal_employers_from_feed_file WHERE legalemployer NOT IN
            (SELECT DISTINCT displayText from existing_legal_employers )"""
        )

        if_new_legal_employers = rail.IfOperator(
            task_id="if_new_legal_employers",
            test='{{result("query_new_legal_employers", "length") > 0}}',
            yes_task="create_new_legal_employers_in_replicon",
            no_task="batch_end"
        )

        create_new_legal_employers_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id="create_new_legal_employers_in_replicon",
            items='{{result("query_new_legal_employers")}}',
            endpoint="/services/ServiceCenterService1.svc/CreateServiceCenterOrApplyModification",
            data={
                    "division": null,
                    "modifications": {
                        "name": '{{item.legalemployer}}',
                        "codeToApply": {
                            "value": '{{item.legalemployercode}}'
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
        get_enabled_legal_employers >>\
            create_existing_legal_employers_collection >>\
            query_new_legal_employers >>\
            if_new_legal_employers >> rail.Label("Yes") >>\
            create_new_legal_employers_in_replicon >> batch_end
        if_new_legal_employers >> rail.Label("No") >> batch_end

        return dag


rail.for_each_instance(create_airflow_dag)
