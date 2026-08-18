"""
### Source Opportunities Project Sync — Close Project Op-DAG

One run per qualifying opportunity (stageName in {Closed Lost, Closed/No Decision,
Sales Rejected} AND probability == 0), triggered by the page-child DAG
(``child_dag.py``). Transitions the Polaris project to the "Closed" workflow state.

#### Project not found — not an error

Unlike the update-project path, a missing project is **not** a failure here.
An opportunity can be lost or rejected before it ever reached Closing stage, so
no Polaris project would have been created. In that case ``log_not_found_in_polaris``
logs a structured warning record and the op-DAG run succeeds (action=
"skipped_project_not_found"). Only opportunities whose project was previously
created (i.e. they reached Closing at some point before being rejected) will
proceed to the workflow transition.

#### Input (dag_run.conf — set by the page-child's trigger_close_project)

```json
{
  "opportunityId":    "...",
  "opportunityName":  "...",
  "clientName":       "...",
  "stageName":        "Closed Lost",
  "probability":      0,
  "startDate":        "2026-05-01",
  "servicesRevenue":  123456.0,
  "loadedAt":         "...",
  "masterRunId":      "...",
  "pageNumber":       5,
  "targetTable":      "..." (optional)
}
```
"""
from datetime import timedelta

from rail import (
    for_each_instance, create_airflow_dag, result,
    PythonOperator, IfOperator, EmptyOperator, Label, BatchTaskRunOperator,
    ViewDagRunConfOperator, RepliconServiceOperator,
)
from airflow.models import Variable
from resource_planner.source_opportunities_project_sync.utils import request_payload, custom_methods


def _common_dag_args(config):
    return {
        "start_date":              config.start_date,
        "company_key":             config.company_key,
        "replicon_conn_id":        config.replicon_conn_id,
        "max_active_runs":         config.max_active_runs_op,
        "schedule_interval":       None,
        "is_paused_upon_creation": True,
        "default_args": {
            "owner": "resource_planner",
            "retries": 2,
            "retry_delay": timedelta(minutes=1),
        },
    }


def create_source_opportunities_project_sync_close_project_dag(config):
    """Op-DAG: transition a Polaris project to "Closed" for a lost/rejected opportunity.

    Triggered by the page-child when stageName is in CLOSE_OUT_STAGES and
    probability == 0. If no project is found (the opportunity was rejected before
    ever reaching Closing), logs a structured warning and succeeds — not a failure.
    """
    with create_airflow_dag(
        dag_id=f"resource_planner_source_opportunities_project_sync_close_project_{config.instance}",
        description="Op-DAG: transition Polaris project to Closed for a lost/rejected opportunity",
        **_common_dag_args(config),
    ) as dag:

        ViewDagRunConfOperator(task_id="view_dag_run_conf")

        can_run_batch_task = IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.resource_planner_source_opportunities_project_sync_enable_batch_task, "true"
            ).lower() == "true",
            yes_task="batch_task",
            no_task="capture_conf",
        )

        batch_task = BatchTaskRunOperator(
            task_id="batch_task",
            start_task="capture_conf",
            end_task="end_task",
        )

        capture_conf = PythonOperator(
            task_id="capture_conf",
            python_callable=custom_methods.capture_conf,
        )

        validate_opportunity = PythonOperator(
            task_id="validate_opportunity",
            python_callable=lambda: custom_methods.validate_opportunity(
                result("capture_conf"), required_fields=("opportunityName",)
            ),
        )

        # ------------------------------------------------------------
        # 1. Find the project — branch on existence, do NOT raise if missing.
        #    A rejected opportunity may never have had a Polaris project.
        # ------------------------------------------------------------

        find_existing_project = RepliconServiceOperator(
            task_id="find_existing_project",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails2",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_search_existing_project_payload(
                result("capture_conf")["opportunityName"]
            ),
            data_handler=lambda response: custom_methods.get_projects_list(response),
        )

        project_found = IfOperator(
            task_id="project_found",
            test=lambda: bool(result("find_existing_project") or []),
            yes_task="update_project_workflow_state",
            no_task="log_not_found_in_polaris",
        )

        # ------------------------------------------------------------
        # 2a. Project exists — transition workflow state to "Closed".
        # ------------------------------------------------------------

        update_project_workflow_state = RepliconServiceOperator(
            task_id="update_project_workflow_state",
            endpoint="/graphql",
            app="polaris",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_workflow_state_mutation_payload(
                (result("find_existing_project") or [{}])[0].get("uri"),
                config.POLARIS_CLOSEOUT_STATE_ID,
            ),
        )

        processing_result = PythonOperator(
            task_id="processing_result",
            python_callable=lambda: custom_methods.processing_result_close_out(
                result("capture_conf")
            ),
        )

        # ------------------------------------------------------------
        # 2b. Project does not exist — log warning, succeed.
        # ------------------------------------------------------------

        log_not_found_in_polaris = PythonOperator(
            task_id="log_not_found_in_polaris",
            python_callable=lambda: custom_methods.log_not_found_in_polaris(
                result("capture_conf")
            ),
        )

        # ------------------------------------------------------------
        # 3. Join both branches, then shared failure logging + end.
        # ------------------------------------------------------------

        join_close_project_result = EmptyOperator(
            task_id="join_close_project_result",
            trigger_rule="none_failed_min_one_success",
        )

        log_failure = PythonOperator(
            task_id="log_failure",
            python_callable=custom_methods.log_failure,
            trigger_rule="all_done",
        )

        end_task = EmptyOperator(task_id="end_task", trigger_rule="all_done")

        # ------------------------------------------------------------
        # Dependencies
        # ------------------------------------------------------------

        can_run_batch_task >> Label("Yes") >> batch_task >> end_task
        can_run_batch_task >> Label("No") >> capture_conf

        (capture_conf
            >> validate_opportunity
            >> find_existing_project
            >> project_found)

        # Yes branch — project found, transition to Closed
        (project_found
            >> Label("Yes")
            >> update_project_workflow_state
            >> processing_result
            >> join_close_project_result)

        # No branch — project not found, log warning and succeed
        (project_found
            >> Label("No")
            >> log_not_found_in_polaris
            >> join_close_project_result)

        join_close_project_result >> log_failure >> end_task

    return dag


for_each_instance(create_source_opportunities_project_sync_close_project_dag)
