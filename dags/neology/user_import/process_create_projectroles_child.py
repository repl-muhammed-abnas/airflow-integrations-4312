from neology.user_import.utils import response_filters
from neology.user_import.utils.custom_methods import get_error_message
from airflow.models import Variable
import rail

null = None
true = True


def create_airflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.create_project_roles_child_dag_id,
        description=f"Neology User Import Create Project Roles Child {config.instance}",
        company_key=config.company_key,
        max_active_runs=config.create_groups_child_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_create_project_roles_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="query_distinct_project_roles"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="query_distinct_project_roles",
            end_task="catch_and_log_errors"
        )

        query_distinct_project_roles = rail.QueryCollectionOperator(
            task_id="query_distinct_project_roles",
            query="""SELECT DISTINCT ratecode_oef FROM bamboohr_valid_users_data WHERE NULLIF(ratecode_oef, '') IS NOT NULL""",
            name="distinctprojectroles"
        )

        get_all_project_roles = rail.RepliconServiceOperator(
            task_id="get_all_project_roles",
            endpoint="/services/ProjectRoleService1.svc/GetAllRoles",
            data_handler=response_filters.get_all_project_roles,
            target="artifact"
        )

        create_existing_project_roles_collections = rail.CreateCollectionOperator(
            task_id="create_existing_project_roles_collections",
            source='{{ result("get_all_project_roles") | load_all_records | to_json}}',
            name="existing_project_roles"
        )

        query_distinct_existing_project_roles = rail.QueryCollectionOperator(
            task_id="query_distinct_existing_project_roles",
            query="""SELECT DISTINCT project_role_name from existing_project_roles""",
            name="distinct_existing_project_roles"
        )

        query_new_project_roles = rail.QueryCollectionOperator(
            task_id="query_new_project_roles",
            query="""SELECT ratecode_oef FROM distinctprojectroles
                WHERE ratecode_oef NOT IN (SELECT project_role_name FROM distinct_existing_project_roles)"""
        )

        if_new_project_roles = rail.IfOperator(
            task_id="if_new_project_roles",
            test='{{ result("query_new_project_roles", "length") > 0 }}',
            yes_task="create_project_roles_in_replicon",
            no_task="create_project_roles_end"
        )

        create_project_roles_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id="create_project_roles_in_replicon",
            items='{{ result("query_new_project_roles") }}',
            endpoint="/services/ProjectRoleService1.svc/PutProjectRole",
            data=lambda item: {
                "projectRoleUri": {
                    "target": {
                        "uri": null,
                        "name": item["ratecode_oef"]
                    },
                    "name": item["ratecode_oef"],
                    "description": item["ratecode_oef"],
                    "isArchived": "false",
                    "isBillable": "false",
                    "rateSchedule": null
                }
            }
        )

        create_project_roles_end = rail.EmptyOperator(task_id="create_project_roles_end")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.groups_log_artifact }}',
            message="Project roles creation failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda: {
                "employeeid": "",
                "action": "Add",
                "status": "Error",
                "details": "Project roles creation failed - " + get_error_message(),
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> query_distinct_project_roles >> get_all_project_roles \
            >> create_existing_project_roles_collections >> query_distinct_existing_project_roles \
                >> query_new_project_roles >> if_new_project_roles
        if_new_project_roles >> rail.Label("Yes") >> create_project_roles_in_replicon >> create_project_roles_end
        if_new_project_roles >> rail.Label("No") >> create_project_roles_end >> catch_and_log_errors

        return dag


rail.for_each_instance(create_airflow_child)
