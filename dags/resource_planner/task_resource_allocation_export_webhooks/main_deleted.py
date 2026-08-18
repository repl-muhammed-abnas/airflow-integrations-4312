import json
from rail import (for_each_instance, create_airflow_dag, Label, PythonOperator,
                  IfOperator, BatchTaskRunOperator, SimpleHttpOperator,
                  EmptyOperator, ViewDagRunConfOperator)
from airflow.models import Variable
from resource_planner.task_resource_allocation_export_webhooks.utils import API_HEADERS, build_api_payload


def create_deleted_allocation_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    with create_airflow_dag(
        dag_id=config.deleted_allocation_dag_id,
        description="Processes ProjectPolarisTaskAllocationDeleted webhook events",
        start_date=config.start_date,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.deleted_max_active_runs,
    ) as dag:

        ViewDagRunConfOperator(task_id="view_dag_run_conf")

        can_run_batch_task = IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                "resource_planner_task_alloc_webhook_deleted_enable_batch_task", "true"
            ).lower() == "true",
            yes_task="batch_task",
            no_task="prepare_mark_deleted_request"
        )

        batch_task = BatchTaskRunOperator(
            task_id="batch_task",
            start_task="prepare_mark_deleted_request",
            end_task="end_task"
        )

        def prepare_mark_deleted_payload(**context):
            dag_run = context["dag_run"]
            return json.dumps(build_api_payload(
                config.rp_api_target_table,
                markDeleted=[{
                    "sourceBookingIdPrefix": dag_run.conf['allocation_uuid'],
                    "sourceSystem": "Polaris",
                }],
            ))

        prepare_mark_deleted_request = PythonOperator(
            task_id="prepare_mark_deleted_request",
            python_callable=prepare_mark_deleted_payload,
        )

        mark_allocation_deleted = SimpleHttpOperator(
            task_id="mark_allocation_deleted",
            method="PATCH",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/sourceAllocations",
            headers=_api_headers,
            data="{{ result('prepare_mark_deleted_request') }}",
            response_check=lambda response: response.status_code == 200,
            log_response=True,
            extra_options={"verify": False},
        )

        end_task = EmptyOperator(task_id="end_task")

        can_run_batch_task >> Label("Yes") >> batch_task >> end_task
        can_run_batch_task >> Label("No") >> prepare_mark_deleted_request >> mark_allocation_deleted >> end_task

    return dag


for_each_instance(create_deleted_allocation_dag)
