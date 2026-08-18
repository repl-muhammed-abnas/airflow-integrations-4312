import rail
from airflow.models import Variable

from dataaxle.user_import.utils import custom_methods, request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_create_custom_fields_dag_id,
        description=f"Dataaxle User Import - Create custom fields child DAG {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_create_custom_fields_child
    ) as dag:

        view_dagrun_config = rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_use_batch = rail.IfOperator(
            task_id="can_use_batch",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var="false"
            ).lower() == "true",
            yes_task="batch_wrapper",
            no_task="start_process",
        )

        batch_wrapper = rail.BatchTaskRunOperator(
            task_id="batch_wrapper",
            start_task="start_process",
            end_task="finish",
        )

        start_process = rail.EmptyOperator(task_id="start_process")

        get_all_custom_fields_drop_down_options = rail.RepliconServiceOperator(
            task_id="get_all_custom_fields_drop_down_options",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda dag_run: request_payload.get_all_custom_fields_drop_down_options(dag_run.conf["custom_field_uri"])
        )

        get_new_custom_fields = rail.PythonOperator(
            task_id="get_new_custom_fields",
            python_callable=lambda dag_run: custom_methods.get_new_custom_fields(dag_run.conf["input_file_custom_field_values"], dag_run.conf["column_name"])
        )

        if_new_custom_fields_found = rail.IfOperator(
            task_id="if_new_custom_fields_found",
            test=lambda: len(rail.result("get_new_custom_fields")) > 0,
            yes_task="create_custom_fields",
            no_task="finish"
        )

        create_custom_fields = rail.RepliconServiceOperator(
            task_id="create_custom_fields",
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda dag_run: request_payload.create_custom_fields_payload(dag_run)
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        can_use_batch >> rail.Label("Yes") >> batch_wrapper >> finish
        can_use_batch >> rail.Label("No") >> start_process >> \
            get_all_custom_fields_drop_down_options >> get_new_custom_fields >> if_new_custom_fields_found
        if_new_custom_fields_found >> rail.Label("No") >> finish
        if_new_custom_fields_found >> rail.Label("Yes") >> create_custom_fields >> finish



    return dag

rail.for_each_instance(create_child_dag)