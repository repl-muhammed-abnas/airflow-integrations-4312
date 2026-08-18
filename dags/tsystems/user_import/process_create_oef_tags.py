from tsystems.user_import.utils import response_filters, custom_methods, request_payload
from airflow.models import Variable
import rail

null = None

def create_add_oef_tags_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.create_oef_tags_child_dag_id,
        description="T-Systems Add OEF Tags Child DAG - Creates new OEF tags for Country and City of Employment",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.create_oef_tags_child_max_active_runs
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="true").lower() == "true",
            yes_task="batch_task",
            no_task="get_specific_user_oefs"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_specific_user_oefs",
            end_task="catch_and_log_errors"
        )

        get_specific_user_oefs = rail.RepliconServiceOperator(
            task_id="get_specific_user_oefs",
            endpoint="services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data=lambda: {
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                "country_of_emp_oef": rail.find_first_by_attr_and_get_attr(response, 'name', "Country of Employment", 'uri'),
                "city_of_emp_oef": rail.find_first_by_attr_and_get_attr(response, 'name', "City of Employment", 'uri')
            }
        )

        # Process Country of Employment OEF Tags
        query_distinct_countries = rail.QueryCollectionOperator(
            task_id="query_distinct_countries",
            query="""SELECT DISTINCT country_of_employment
                    FROM valid_users_payload_data 
                    WHERE NULLIF(country_of_employment, '') IS NOT NULL""",
            name="users_countries"
        )

        get_all_country_oef_tags = rail.RepliconServiceOperator(
            task_id="get_all_country_oef_tags",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result("get_specific_user_oefs")["country_of_emp_oef"],
            },
            data_handler=response_filters.get_all_oef_tags,
            target="artifact"
        )

        create_existing_country_oef_collections = rail.CreateCollectionOperator(
            task_id="create_existing_country_oef_collections",
            source='{{ result("get_all_country_oef_tags") | load_all_records | to_json}}',
            name="existing_country_oef_tags"
        )

        query_distinct_existing_countries = rail.QueryCollectionOperator(
            task_id="query_distinct_existing_countries",
            query="""SELECT DISTINCT oef_tag as country_name from existing_country_oef_tags""",
            name="distinct_existing_countries"
        )

        query_new_countries = rail.QueryCollectionOperator(
            task_id="query_new_countries",
            query="""SELECT country_of_employment
                FROM users_countries
                WHERE country_of_employment NOT IN (
                    SELECT country_name
                    FROM distinct_existing_countries
            )"""
        )

        if_new_countries = rail.IfOperator(
            task_id="if_new_countries",
            test='{{ result("query_new_countries", "length") > 0 }}',
            yes_task="put_country_oef_tags",
            no_task="process_city_oef_tags"
        )

        put_country_oef_tags = rail.RepliconServiceOperator(
            task_id='put_country_oef_tags',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data=lambda: request_payload.get_put_oef_tags_payload(
                rail.result("get_specific_user_oefs")["country_of_emp_oef"],
                rail.load_all_records(rail.result("get_all_country_oef_tags")),
                "country_of_employment",
                rail.load_all_records(rail.result("query_new_countries"))
            )
        )

        process_city_oef_tags = rail.EmptyOperator(task_id="process_city_oef_tags")

        # Process City of Employment OEF Tags
        query_distinct_cities = rail.QueryCollectionOperator(
            task_id="query_distinct_cities",
            query="""SELECT DISTINCT city_of_employment
                    FROM valid_users_payload_data 
                    WHERE NULLIF(city_of_employment, '') IS NOT NULL""",
            name="users_cities"
        )

        get_all_city_oef_tags = rail.RepliconServiceOperator(
            task_id="get_all_city_oef_tags",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result("get_specific_user_oefs")["city_of_emp_oef"],
            },
            data_handler=response_filters.get_all_oef_tags,
            target="artifact"
        )

        create_existing_city_oef_collections = rail.CreateCollectionOperator(
            task_id="create_existing_city_oef_collections",
            source='{{ result("get_all_city_oef_tags") | load_all_records | to_json}}',
            name="existing_city_oef_tags"
        )

        query_distinct_existing_cities = rail.QueryCollectionOperator(
            task_id="query_distinct_existing_cities",
            query="""SELECT DISTINCT oef_tag as city_name from existing_city_oef_tags""",
            name="distinct_existing_cities"
        )

        query_new_cities = rail.QueryCollectionOperator(
            task_id="query_new_cities",
            query="""SELECT city_of_employment
                FROM users_cities
                WHERE city_of_employment NOT IN (
                    SELECT city_name
                    FROM distinct_existing_cities
            )"""
        )

        if_new_cities = rail.IfOperator(
            task_id="if_new_cities",
            test='{{ result("query_new_cities", "length") > 0 }}',
            yes_task="put_city_oef_tags",
            no_task="oef_tags_end"
        )

        put_city_oef_tags = rail.RepliconServiceOperator(
            task_id='put_city_oef_tags',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data=lambda: request_payload.get_put_oef_tags_payload(
                rail.result("get_specific_user_oefs")["city_of_emp_oef"],
                rail.load_all_records(rail.result("get_all_city_oef_tags")),
                "city_of_employment",
                rail.load_all_records(rail.result("query_new_cities"))
            )
        )

        oef_tags_end = rail.EmptyOperator(task_id="oef_tags_end")

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{ dag_run.conf.groups_log_artifact }}',
            message="OEF Tags create failed",
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda: {
                "employeeid": "",
                "action": "",
                "status": "Error",
                "details": "OEF Tags create failed - " + custom_methods.get_error_message(),
            }
        )

        # DAG Flow
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        
        can_run_batch_task >> rail.Label("No") >> get_specific_user_oefs >> query_distinct_countries >> get_all_country_oef_tags >> \
            create_existing_country_oef_collections >> query_distinct_existing_countries >> \
            query_new_countries >> if_new_countries
        
        if_new_countries >> rail.Label("Yes") >> put_country_oef_tags >> process_city_oef_tags
        if_new_countries >> rail.Label("No") >> process_city_oef_tags
        
        process_city_oef_tags >> query_distinct_cities >> get_all_city_oef_tags >> \
            create_existing_city_oef_collections >> query_distinct_existing_cities >> \
            query_new_cities >> if_new_cities
        
        if_new_cities >> rail.Label("Yes") >> put_city_oef_tags >> oef_tags_end
        if_new_cities >> rail.Label("No") >> oef_tags_end
        
        oef_tags_end >> catch_and_log_errors

        return dag

# Create child DAG for each instance
rail.for_each_instance(create_add_oef_tags_child_dag)