from datetime import timedelta
from uuid import uuid4
from airflow.models import Variable
import rail
null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sigroup_departments_dag_id,
       description="sigroup user import departments child",
        max_active_runs=config.master_max_active_runs,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(config.sigroup_batch_task_flag, "true").lower() == "true",
            yes_task="batch_task",
            no_task="get_enabled_departments"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task="get_enabled_departments",
            end_task="batch_end"
        )

        get_enabled_departments = rail.RepliconServiceOperator(
            task_id="get_enabled_departments",
            endpoint="/services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups"
        )

        get_parent_department_uri = rail.PythonOperator(
            task_id="get_parent_department_uri",
            python_callable=lambda :rail.find_first_by_attr_and_get_attr(
                rail.result("get_enabled_departments"),
                "displayText",
                "SI Group",
                "uri"
            )
        )

        create_existing_departments_collection = rail.CreateCollectionOperator(
            task_id="create_existing_departments_collection",
            source='{{result("get_enabled_departments")|to_json}}',
            name="existing_departments"
        )

        query_new_departments = rail.QueryCollectionOperator(
            task_id="query_new_departments",
            query="""SELECT * FROM query_departments_from_feed_file WHERE department NOT IN
            (SELECT DISTINCT displayText from existing_departments )"""
        )

        if_new_departments = rail.IfOperator(
            task_id="if_new_departments",
            test='{{result("query_new_departments", "length") > 0}}',
            yes_task="create_new_departments_in_replicon",
            no_task="batch_end"
        )

        create_new_departments_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id="create_new_departments_in_replicon",
            items='{{result("query_new_departments")}}',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda item:{
                    "departmentGroup": {
                        "uri": null,
                        "parent": {
                        "uri": rail.result("get_parent_department_uri"),
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                        },
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "modifications": {
                        "name": item["department"],
                        "codeToApply": {
                        "value": item["departmentcode"]
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
        get_enabled_departments >>\
        get_parent_department_uri>>\
        create_existing_departments_collection >>\
        query_new_departments >>\
        if_new_departments >> rail.Label("Yes") >>\
        create_new_departments_in_replicon >> batch_end
        if_new_departments >> rail.Label("No") >> batch_end

        return dag


rail.for_each_instance(create_airflow_dag)
