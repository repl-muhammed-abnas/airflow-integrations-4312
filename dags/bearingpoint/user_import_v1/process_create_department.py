from uuid import uuid4
from bearingpoint.user_import_v1.utils import custom_methods
from airflow.models import Variable
import rail

null = None
true = True


def create_ariflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.create_departments_child_dag_id,
        description=f"BearingPoint User Import Create Departments Child {config.instance}",
        company_key=config.company_key,
        max_active_runs=config.create_departments_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_department_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="query_distinct_departments"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="query_distinct_departments",
            end_task="catch_and_log_errors"
        )

        query_distinct_departments = rail.QueryCollectionOperator(
            task_id="query_distinct_departments",
            query="""SELECT DISTINCT department_name, department_code, department_name || '-' || department_code
                AS department_unique_name
                FROM valid_users_data
                WHERE NULLIF(department_name, '') IS NOT NULL""",
            name="departments"
        )

        get_all_departments = rail.RepliconServiceOperator(
            task_id="get_all_departments",
            endpoint="/services/DepartmentGroupService1.svc/GetAllDepartmentGroups",
            data_handler=custom_methods.get_all_departments,
            target="artifact"
        )

        create_existing_departments_collections = rail.CreateCollectionOperator(
            task_id="create_existing_departments_collections",
            source='{{ result("get_all_departments") | load_all_records | to_json}}',
            name="existing_departments"
        )

        query_distinct_existing_departments = rail.QueryCollectionOperator(
            task_id="query_distinct_existing_departments",
            query="""SELECT DISTINCT department_name from existing_departments""",
            name="distinct_existing_departments"
        )

        query_new_departments = rail.QueryCollectionOperator(
            task_id="query_new_departments",
            query="""SELECT department_name, department_code, department_unique_name
                FROM departments
                WHERE department_unique_name NOT IN (
                    SELECT department_name 
                    FROM distinct_existing_departments
            )"""
        )

        if_new_departments = rail.IfOperator(
            task_id="if_new_departments",
            test='{{result("query_new_departments", "length")>0}}',
            yes_task="create_department_in_replicon",
            no_task="department_end"
        )

        create_department_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id="create_department_in_replicon",
            items='{{result("query_new_departments")}}',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda item: {
                "departmentGroup": {
                    "parent": {
                        "uri": null,
                        "name": "BearingPoint"
                    },
                },
                "modifications": {
                    "name": item["department_unique_name"],
                    "codeToApply": {
                        "value": item["department_code"]
                    },
                    "descriptionToApply": null,
                    "isEnabled": true
                },
                "unitOfWorkId": str(uuid4())
            }
        )

        department_end = rail.EmptyOperator(task_id="department_end")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.groups_log_artifact }}',
            message="Department create failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda: {
                "employeeid": "",
                "action": "",
                "status": "Error",
                "details": "Department create failed" + custom_methods.get_error_message(),
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >>\
            query_distinct_departments >> get_all_departments >> create_existing_departments_collections >>\
            query_distinct_existing_departments >> query_new_departments >>\
            if_new_departments >> rail.Label("Yes") >> create_department_in_replicon >>\
            department_end
        if_new_departments >> rail.Label(
            "No") >> department_end >> catch_and_log_errors

        return dag


rail.for_each_instance(create_ariflow_child)
