from datetime import timedelta
from airflow.models import Variable
from sigroup.user_import.utils import custom_methods
import rail
null = None


def create_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.sigroup_cba_appendix_levels_dag_id,
       description="sigroup user import cba_appendix child",
        max_active_runs=config.master_max_active_runs,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(config.sigroup_batch_task_flag, "true").lower() == "true",
            yes_task="batch_task",
            no_task="get_cba_appendix_custom_field_dropdown"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task="get_cba_appendix_custom_field_dropdown",
            end_task="batch_end"
        )

        get_cba_appendix_custom_field_dropdown = rail.RepliconServiceOperator(
            task_id="get_cba_appendix_custom_field_dropdown",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": '{{dag_run.conf.cba_appendix_uri}}'
            }
        )

        create_cba_appendix_existing_values = rail.CreateCollectionOperator(
            task_id="create_cba_appendix_existing_values",
            source='{{result("get_cba_appendix_custom_field_dropdown")|to_json}}',
            name="cba_appendix_in_replicon"
        )

        query_new_cba_appendix_drop_down = rail.QueryCollectionOperator(
            task_id="query_new_cba_appendix",
            query="""SELECT * FROM query_cba_appendix_from_feed_file WHERE cbaappendix NOT IN
            (SELECT DISTINCT displayText from cba_appendix_in_replicon )"""
        )

        query_new_cba_appendix_lower_drop_down = rail.QueryCollectionOperator(
            task_id="query_new_cba_appendix_lower_drop_down",
            query="""SELECT * FROM query_cba_appendix_from_feed_file WHERE lower(cbaappendix) NOT IN
            (SELECT DISTINCT lower(displayText) from cba_appendix_in_replicon )"""
        )

        if_new_cba_appendix = rail.IfOperator(
            task_id="if_new_cba_appendix",
            test='{{result("query_new_cba_appendix_lower_drop_down", "length") > 0}}',
            yes_task="create_drop_down_requests",
            no_task="batch_end"
        )

        create_drop_down_requests = rail.PythonOperator(
            task_id="create_drop_down_requests",
            python_callable=lambda:custom_methods.get_custom_field_drop_down_request(
                rail.result("create_cba_appendix_existing_values"),
                rail.result("query_new_cba_appendix_lower_drop_down"),
                "cbaappendix")
        )

        create_new_cba_appendix_in_replicon = rail.RepliconServiceOperator(
            task_id="create_new_cba_appendix_in_replicon",
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda dag_run:{
                    "customFieldUri": dag_run.conf["cba_appendix_uri"],
                    "customFieldDropDownOptionUris": rail.result("create_drop_down_requests")
            }
        )

        batch_end = rail.EmptyOperator(task_id="batch_end")

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> batch_end
        can_run_batch_task >> rail.Label("No") >>\
        get_cba_appendix_custom_field_dropdown >>\
        create_cba_appendix_existing_values >> query_new_cba_appendix_drop_down >>\
        query_new_cba_appendix_lower_drop_down >>\
        if_new_cba_appendix >> rail.Label("Yes") >>\
        create_drop_down_requests >>\
        create_new_cba_appendix_in_replicon >> batch_end
        if_new_cba_appendix >> rail.Label("No") >> batch_end

        return dag


rail.for_each_instance(create_airflow_dag)
