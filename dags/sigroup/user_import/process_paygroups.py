from datetime import timedelta
from uuid import uuid4
from airflow.models import Variable
import rail
null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sigroup_paygroups_dag_id,
       description="sigroup user import paygroups child",
        max_active_runs=config.master_max_active_runs,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(config.sigroup_batch_task_flag, "true").lower() == "true",
            yes_task="batch_task",
            no_task="get_enabled_paygroups"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task="get_enabled_paygroups",
            end_task="batch_end"
        )

        get_enabled_paygroups = rail.RepliconServiceOperator(
            task_id="get_enabled_paygroups",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups"
        )

        create_existing_paygroups_collection = rail.CreateCollectionOperator(
            task_id="create_existing_paygroups_collection",
            source='{{result("get_enabled_paygroups")|to_json}}',
            name="existing_paygroups"
        )

        query_new_paygroups = rail.QueryCollectionOperator(
            task_id="query_new_paygroups",
            query="""SELECT * FROM query_paygroups_from_feed_file WHERE paygroup NOT IN
            (SELECT DISTINCT displayText from existing_paygroups )"""
        )

        if_new_paygroups = rail.IfOperator(
            task_id="if_new_paygroups",
            test='{{result("query_new_paygroups", "length") > 0}}',
            yes_task="create_new_paygroups_in_replicon",
            no_task="batch_end"
        )

        create_new_paygroups_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id="create_new_paygroups_in_replicon",
            items='{{result("query_new_paygroups")}}',
            endpoint="/services/EmployeeTypeGroupService1.svc/CreateEmployeeTypeGroupOrApplyModification",
            data={
                    "division": null,
                    "modifications": {
                        "name": '{{item.paygroup}}',
                        "codeToApply": {
                            "value": '{{item.paygroupcode}}'
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
        get_enabled_paygroups >>\
            create_existing_paygroups_collection >>\
            query_new_paygroups >>\
            if_new_paygroups >> rail.Label("Yes") >>\
            create_new_paygroups_in_replicon >> batch_end
        if_new_paygroups >> rail.Label("No") >> batch_end

        return dag


rail.for_each_instance(create_airflow_dag)
