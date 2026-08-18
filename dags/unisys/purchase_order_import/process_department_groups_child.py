import rail
from datetime import timedelta
from airflow.models import Variable
from unisys.purchase_order_import.utils import request_payload

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_department_groups_child_dag_id,
        description=f"Unisys Purchase Order IDs Import - Process Purchase Order ID Child DAG {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_process_department_groups,
        default_args={
            "execution_timeout": timedelta(hours=1),
        },
    ) as dag:
        
        view_dagrun_conf = rail.ViewDagRunConfOperator(
            task_id="view_dagrun_conf")
        
        can_use_batch = rail.IfOperator(
            task_id="can_use_batch",
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var="false"
            ).lower()
            == "true",
            yes_task="batch_wrapper",
            no_task="start_process",
        )

        batch_wrapper = rail.BatchTaskRunOperator(
            task_id="batch_wrapper",
            start_task="start_process",
            end_task="catch_and_log_error",
        )

        start_process = rail.EmptyOperator(
            task_id="start_process"
        )

        add_department_group = rail.RepliconServiceOperator(
            task_id="add_department_group",
            endpoint=config.create_or_apply_modification_department_endpoint,
            data=lambda dag_run: request_payload.create_department_group_payload(
                purchase_order_id=dag_run.conf["purchase_order_id"],
                parent_department=dag_run.conf["parent_department"],
            ),
        )

        log_add_success = rail.WriteLogOperator(
            task_id="log_add_success",
            log="{{ dag_run.conf.processing_log }}",
            severity="success",
            message="Purchase Order ID added successfully",
            properties=lambda dag_run: {
                "purchase_order_id": dag_run.conf["purchase_order_id"],
                "status": "Success",
                "details": "Purchase Order ID added successfully",
            },
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            trigger_rule="one_failed",
            log="{{ dag_run.conf.processing_log }}",
            severity="Error",
            message="Unexpected error processing purchase order ID",
            properties=lambda dag_run: {
                "purchase_order_id": dag_run.conf.get("purchase_order_id", "Unknown"),
                "status": "Error",
                "details": rail.render_template("{{ get_error_message() }}"),
            },
        )
        
        view_dagrun_conf
        can_use_batch >> rail.Label(
            "Yes") >> batch_wrapper >> catch_and_log_error
        can_use_batch >> rail.Label("No") >> start_process >>\
        add_department_group >> log_add_success >> catch_and_log_error


    return dag

rail.for_each_instance(create_child_dag)

