from uuid import uuid4
from bearingpoint.user_import_v1.utils import custom_methods
from airflow.models import Variable
import rail

null = None
true = True


def create_ariflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.create_servicecenters_child_dag_id,
        description=f"BearingPoint User Import Create Service Centers Child {config.instance}",
        company_key=config.company_key,
        max_active_runs=config.create_servicecenters_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_servicecenter_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="query_distinct_servicecenters"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="query_distinct_servicecenters",
            end_task="catch_and_log_errors"
        )

        query_distinct_servicecenters = rail.QueryCollectionOperator(
            task_id="query_distinct_servicecenters",
            query="""SELECT DISTINCT company_code_name, company_code
                FROM valid_users_data
                WHERE NULLIF(company_code_name, '') IS NOT NULL""",
            name="servicecenters"
        )

        get_all_servicecenters = rail.RepliconServiceOperator(
            task_id="get_all_servicecenters",
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters",
            data_handler=custom_methods.get_all_servicecenters,
            target="artifact"
        )

        create_existing_servicecenters_collections = rail.CreateCollectionOperator(
            task_id="create_existing_servicecenters_collections",
            source='{{ result("get_all_servicecenters") | load_all_records | to_json}}',
            name="existing_servicecenters"
        )

        query_distinct_existing_servicecenters = rail.QueryCollectionOperator(
            task_id="query_distinct_existing_servicecenters",
            query="""SELECT DISTINCT company_code_name from existing_servicecenters""",
            name="distinct_existing_servicecenters"
        )

        query_new_servicecenters = rail.QueryCollectionOperator(
            task_id="query_new_servicecenters",
            query="""SELECT company_code_name, company_code
                FROM servicecenters
                WHERE company_code_name NOT IN (
                    SELECT company_code_name
                    FROM distinct_existing_servicecenters
            )"""
        )

        if_new_servicecenters = rail.IfOperator(
            task_id="if_new_servicecenters",
            test='{{result("query_new_servicecenters", "length")>0}}',
            yes_task="create_servicecenter_in_replicon",
            no_task="servicecenter_end"
        )

        create_servicecenter_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id="create_servicecenter_in_replicon",
            items='{{result("query_new_servicecenters")}}',
            endpoint="/services/ServiceCenterService1.svc/CreateServiceCenterOrApplyModification",
            data=lambda item: {
                "serviceCenter": null,
                "modifications": {
                    "name": item["company_code_name"],
                    "codeToApply": {
                        "value": item["company_code"]
                    },
                    "descriptionToApply": null,
                    "isEnabled": true
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        servicecenter_end = rail.EmptyOperator(task_id="servicecenter_end")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.groups_log_artifact }}',
            message="Service Center create failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda: {
                "employeeid": "",
                "action": "",
                "status": "Error",
                "details": "Service Center create failed" + custom_methods.get_error_message(),
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >>\
            query_distinct_servicecenters >> get_all_servicecenters >> create_existing_servicecenters_collections >>\
            query_distinct_existing_servicecenters >> query_new_servicecenters >>\
            if_new_servicecenters >> rail.Label("Yes") >> create_servicecenter_in_replicon >>\
            servicecenter_end
        if_new_servicecenters >> rail.Label(
            "No") >> servicecenter_end >> catch_and_log_errors

        return dag


rail.for_each_instance(create_ariflow_child)
