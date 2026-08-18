from uuid import uuid4
from bearingpoint.user_import.utils import custom_methods
from airflow.models import Variable
import rail

null = None
true = True


def create_ariflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.create_employeetypes_child_dag_id,
        description=f"BearingPoint User Import Create Employee Types Child {config.instance}",
        company_key=config.company_key,
        max_active_runs=config.create_emptypes_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_employeetype_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="query_distinct_employeetypes"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="query_distinct_employeetypes",
            end_task="catch_and_log_errors"
        )

        query_distinct_employeetypes = rail.QueryCollectionOperator(
            task_id="query_distinct_employeetypes",
            query="""SELECT DISTINCT employee_type_name, employee_type_code
                FROM valid_users_data
                WHERE NULLIF(employee_type_name, '') IS NOT NULL""",
            name="employeetypes"
        )

        get_all_employeetypes = rail.RepliconServiceOperator(
            task_id="get_all_employeetypes",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
            data_handler=custom_methods.get_all_employeetypes,
            target="artifact"
        )

        create_existing_employeetypes_collections = rail.CreateCollectionOperator(
            task_id="create_existing_employeetypes_collections",
            source='{{ result("get_all_employeetypes") | load_all_records | to_json}}',
            name="existing_employeetypes"
        )

        query_distinct_existing_employeetypes = rail.QueryCollectionOperator(
            task_id="query_distinct_existing_employeetypes",
            query="""SELECT DISTINCT employee_type_name from existing_employeetypes""",
            name="distinct_existing_employeetypes"
        )

        query_new_employeetypes = rail.QueryCollectionOperator(
            task_id="query_new_employeetypes",
            query="""SELECT employee_type_name, employee_type_code
                FROM employeetypes
                WHERE employee_type_name NOT IN (
                    SELECT employee_type_name
                    FROM distinct_existing_employeetypes
            )"""
        )

        if_new_employeetypes = rail.IfOperator(
            task_id="if_new_employeetypes",
            test='{{result("query_new_employeetypes", "length")>0}}',
            yes_task="create_employeetype_in_replicon",
            no_task="employeetype_end"
        )

        create_employeetype_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id="create_employeetype_in_replicon",
            items='{{result("query_new_employeetypes")}}',
            endpoint="/services/EmployeeTypeGroupService1.svc/CreateEmployeeTypeGroupOrApplyModification",
            data=lambda item: {
                "employeeTypeGroup": null,
                "modifications": {
                    "name": item["employee_type_name"],
                    "codeToApply": {
                        "value": item["employee_type_code"]
                    },
                    "descriptionToApply": null,
                    "isEnabled": true
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        employeetype_end = rail.EmptyOperator(task_id="employeetype_end")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.groups_log_artifact }}',
            message="Employee Type create failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda: {
                "employeeid": "",
                "action": "",
                "status": "Error",
                "details": "Employee Type create failed" + custom_methods.get_error_message(),
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >>\
            query_distinct_employeetypes >> get_all_employeetypes >> create_existing_employeetypes_collections >>\
            query_distinct_existing_employeetypes >> query_new_employeetypes >>\
            if_new_employeetypes >> rail.Label("Yes") >> create_employeetype_in_replicon >>\
            employeetype_end
        if_new_employeetypes >> rail.Label(
            "No") >> employeetype_end >> catch_and_log_errors

        return dag


rail.for_each_instance(create_ariflow_child)
