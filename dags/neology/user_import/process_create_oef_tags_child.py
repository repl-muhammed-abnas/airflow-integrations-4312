from neology.user_import.utils import response_filters, custom_methods, request_payload
from airflow.models import Variable
import rail

null = None

def create_add_oef_tags_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.create_oef_tags_child_dag_id,
        description="Neology Add OEF Tags Child DAG - Creates new OEF tags for List OEFs",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.create_oef_tags_child_max_active_runs
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_create_oef_tags_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="check_oef_exists"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="check_oef_exists",
            end_task="catch_and_log_errors"
        )

        # Generic OEF Tag Processing
        check_oef_exists = rail.IfOperator(
            task_id="check_oef_exists",
            test=lambda dag_run: dag_run.conf.get("oef_uri") is not None,
            yes_task="query_distinct_values",
            no_task="oef_tags_end"
        )

        query_distinct_values = rail.QueryCollectionOperator(
            task_id="query_distinct_values",
            query="""SELECT DISTINCT {{ dag_run.conf.bamboohr_field }}
                    FROM bamboohr_valid_users_data 
                    WHERE NULLIF ({{ dag_run.conf.bamboohr_field }}, '') IS NOT NULL""",
            name="users_oef_values"
        )

        get_all_oef_tags = rail.RepliconServiceOperator(
            task_id="get_all_oef_tags",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda dag_run: {
                "objectExtensionTagDefinitionUri": dag_run.conf["oef_uri"],
            },
            data_handler=response_filters.get_all_oef_tags
        )

        create_existing_oef_collections = rail.CreateCollectionOperator(
            task_id="create_existing_oef_collections",
            source='{{ result("get_all_oef_tags") | to_json}}',
            columns=["oef_tag", "code", "description", "is_enabled", "uri"],
            name="existing_oef_tags"
        )

        query_distinct_existing_values = rail.QueryCollectionOperator(
            task_id="query_distinct_existing_values",
            query="""SELECT DISTINCT oef_tag as existing_value from existing_oef_tags""",
            name="distinct_existing_values"
        )

        query_new_values = rail.QueryCollectionOperator(
            task_id="query_new_values",
            query="""SELECT {{ dag_run.conf.bamboohr_field }} FROM users_oef_values
                WHERE {{ dag_run.conf.bamboohr_field }} NOT IN (
                    SELECT existing_value FROM distinct_existing_values
            )"""
        )

        if_new_values = rail.IfOperator(
            task_id="if_new_values",
            test='{{ result("query_new_values", "length") > 0 }}',
            yes_task="put_oef_tags",
            no_task="oef_tags_end"
        )

        put_oef_tags = rail.RepliconServiceOperator(
            task_id='put_oef_tags',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data=lambda dag_run: request_payload.get_put_oef_tags_payload(
                dag_run.conf["oef_uri"],
                rail.result("get_all_oef_tags"),
                dag_run.conf["bamboohr_field"],
                rail.result("query_new_values")
            )
        )

        oef_tags_end = rail.EmptyOperator(task_id="oef_tags_end")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.groups_log_artifact }}',
            message="OEF Tags creation failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "employeeid": "",
                "action": "Add",
                "status": "Error",
                "details": "OEF Tags creation failed - " + custom_methods.get_error_message(),
            }
        )

        # DAG Flow
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        
        can_run_batch_task >> rail.Label("No") >> check_oef_exists
        
        # OEF processing
        check_oef_exists >> rail.Label("Yes") >> query_distinct_values >> get_all_oef_tags \
            >> create_existing_oef_collections >> query_distinct_existing_values >> query_new_values >> if_new_values
        
        if_new_values >> rail.Label("Yes") >> put_oef_tags >> oef_tags_end
        if_new_values >> rail.Label("No") >> oef_tags_end
        
        check_oef_exists >> rail.Label("No") >> oef_tags_end
        
        oef_tags_end >> catch_and_log_errors

        return dag

# Create child DAG for each instance
rail.for_each_instance(create_add_oef_tags_child_dag)