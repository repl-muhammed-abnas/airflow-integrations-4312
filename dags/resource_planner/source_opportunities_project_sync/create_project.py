"""
### Source Opportunities Project Sync — Create Project Op-DAG

One run per qualifying opportunity (stage='Closing', probability >= 70),
triggered by the page-child DAG (``child_dag.py``). Each run performs the
full Polaris workflow needed to turn one opportunity into a new project:

    resolve client (search or create)
      -> check_project_exists (idempotency guard)
      -> resolve template by engagementContractType
      -> duplicate project from template
      -> poll batch completion
      -> apply opportunity data to the new project
      -> attach client
      -> transition workflow state to "Initiate"

#### Why one DAG run per opportunity

Template selection depends on each opportunity's ``engagementContractType``,
and project creation is a multi-step Polaris workflow. Isolating it to one
DAG run per opportunity means a single opportunity's failure never blocks
its siblings on the same page, and it can be replayed from the Airflow UI
without re-running the rest of the page.

#### Idempotency — check_project_exists

There is no per-row mark-synced call in this integration, so the master's
cursor is the only commit point; on any page/op failure, the next scheduled
run re-issues ``/batches`` with the same ``lastModifiedAfter`` and retries
the whole window. That makes it possible to be asked to create the same
opportunity's project twice. ``check_project_exists`` looks up
``BulkGetProjectDetails2`` by ``opportunityName`` before duplicating
anything; if a project with that name already exists, this run short-circuits
straight to ``processing_result`` with ``action="skipped_already_exists"``
instead of duplicating a second copy.

``wait_for_duplicate_project_batch`` is a ``RepliconBatchExecutionSensor``
used WITHOUT ``tasks_to_retry`` — ``CreateProjectCopyBatch2`` has no
idempotency key (unlike the later modification/workflow calls, which carry
``unitOfWorkId``), so auto-retrying it on batch failure would create a
second duplicate project. A batch failure surfaces as a normal task failure
instead, relying on Airflow-level ``retries`` plus ``check_project_exists``
on any manual re-trigger.

#### Input (dag_run.conf — set by the page-child's trigger_create_project)

```json
{
  "opportunityId":          "...",
  "opportunityNumber":      "...",
  "opportunityName":        "...",
  "clientId":               "...",
  "clientName":             "...",
  "productSubmodule":       "...",
  "engagementContractType": "Statement of Work (SOW)",
  "type":                   "...",
  "stageName":              "Closing",
  "probability":            75,
  "startDate":              "2026-05-01",
  "servicesRevenue":        123456.0,
  "loadedAt":               "...",
  "masterRunId":            "...",
  "pageNumber":             5,
  "targetTable":            "..." (optional)
}
```
"""
from datetime import timedelta

from rail import (
    for_each_instance, create_airflow_dag, result, Label,
    PythonOperator, IfOperator, EmptyOperator, BatchTaskRunOperator,
    ViewDagRunConfOperator, RepliconServiceOperator, RepliconBatchExecutionSensor,
    TriggerDagRunOperator,
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


def _safe_result(task_id):
    """Best-effort XCom pull — returns None if the task was skipped (e.g.
    by the client_exists branch) or has no result, instead of raising.
    """
    try:
        return result(task_id)
    except Exception as e:
        print(f"_safe_result({task_id!r}): task was skipped or XCom unavailable: {e!r}")
        return None


def create_source_opportunities_project_sync_create_project_dag(config):
    with create_airflow_dag(
        dag_id=f"resource_planner_source_opportunities_project_sync_create_project_{config.instance}",
        description="Op-DAG: create one Polaris project from one qualifying opportunity",
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

        # ------------------------------------------------------------
        # 1. Validate required fields. Isolated to this one run — a bad
        #    row fails only its own op-DAG, not the whole page.
        # ------------------------------------------------------------

        validate_opportunity = PythonOperator(
            task_id="validate_opportunity",
            python_callable=lambda: custom_methods.validate_opportunity(
                result("capture_conf")
            ),
        )

        # ------------------------------------------------------------
        # 2. Resolve the project template name from engagementContractType
        # ------------------------------------------------------------

        resolve_project_template_name = PythonOperator(
            task_id="resolve_project_template_name",
            python_callable=lambda: request_payload.resolve_project_template_name(
                result("capture_conf")
            ),
        )

        # ------------------------------------------------------------
        # 3. Resolve the client — search, then create if not found.
        # ------------------------------------------------------------

        search_client_in_polaris = RepliconServiceOperator(
            task_id="search_client_in_polaris",
            endpoint="/services/ClientListService1.svc/GetData",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_search_client_payload(
                result("capture_conf")["clientName"]
            ),
        )

        client_exists = IfOperator(
            task_id="client_exists",
            test=lambda: custom_methods.collect_client_uri(
                _safe_result("create_client_in_polaris"),
                result("capture_conf")["clientName"],
                result("search_client_in_polaris"),
                test=True
            ),
            yes_task="collect_client_uri",
            no_task="create_client_in_polaris",
        )

        create_client_in_polaris = RepliconServiceOperator(
            task_id="create_client_in_polaris",
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_create_client_payload(
                result("capture_conf")["clientName"]
            ),
        )

        collect_client_uri = PythonOperator(
            task_id="collect_client_uri",
            python_callable=lambda: custom_methods.collect_client_uri(
                _safe_result("create_client_in_polaris"),
                result("capture_conf")["clientName"],
                result("search_client_in_polaris"),
            ),
        )

        # ------------------------------------------------------------
        # 4. Idempotency guard — skip straight to processing_result if a
        #    project with this opportunity's name already exists.
        # ------------------------------------------------------------

        check_project_exists = RepliconServiceOperator(
            task_id="check_project_exists",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails2",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_search_existing_project_payload(
                result("capture_conf")["opportunityName"]
            ),
            data_handler=lambda response: custom_methods.get_projects_list(response),
        )

        project_already_exists = IfOperator(
            task_id="project_already_exists",
            test=lambda: len(result("check_project_exists") or []) > 0,
            yes_task="join_before_processing_result",
            no_task="get_project_template",
        )

        # ------------------------------------------------------------
        # 5. Resolve template -> duplicate -> poll batch completion
        # ------------------------------------------------------------

        get_project_template = RepliconServiceOperator(
            task_id="get_project_template",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails2",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_get_project_template_payload(
                result("resolve_project_template_name")
            ),
            data_handler=lambda response: custom_methods.get_projects_list(response),
        )

        guard_template_found = PythonOperator(
            task_id="guard_template_found",
            python_callable=lambda: custom_methods.guard_template_found(
                result("get_project_template"), result("resolve_project_template_name")
            ),
        )

        create_duplicate_project = RepliconServiceOperator(
            task_id="create_duplicate_project",
            endpoint="/services/ProjectService1.svc/CreateProjectCopyBatch2",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_create_duplicate_project_payload(
                result("capture_conf"), result("guard_template_found")
            ),
        )

        processing_batch_in_background = RepliconServiceOperator(
            task_id="processing_batch_in_background",
            endpoint="/services/BatchManagementService1.svc/ExecuteInBackground",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_processing_batch_in_background_payload(
                result("create_duplicate_project")
            ),
        )

        # No tasks_to_retry: CreateProjectCopyBatch2 has no idempotency key,
        # so an auto-retry here would create a second duplicate project.
        wait_for_duplicate_project_batch = RepliconBatchExecutionSensor(
            task_id="wait_for_duplicate_project_batch",
            batch_uri='{{ result("create_duplicate_project") }}',
            replicon_conn_id=config.replicon_conn_id,
            execution_timeout=timedelta(minutes=30),
        )

        # ------------------------------------------------------------
        # 6. Apply opportunity data, attach client, transition workflow
        # ------------------------------------------------------------

        modify_duplicate_project = RepliconServiceOperator(
            task_id="modify_duplicate_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_modify_project_payload(
                result("capture_conf")
            ),
        )

        update_client = RepliconServiceOperator(
            task_id="update_client",
            endpoint="/services/ProjectService1.svc/UpdateClients",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_update_client_payload(
                result("modify_duplicate_project")["uri"], result("collect_client_uri")
            ),
        )

        update_project_workflow_state = RepliconServiceOperator(
            task_id="update_project_workflow_state",
            endpoint="/graphql",
            app="polaris",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_workflow_state_mutation_payload(
                result("modify_duplicate_project")["uri"],
                config.POLARIS_INITIATE_STATE_ID,
            ),
        )

        # ------------------------------------------------------------
        # 7. JIT-populate rp_source_time_codes (fire-and-forget)
        # ------------------------------------------------------------
        # Project is brand new here — checkPresence will always return
        # projectPresent=False, so ensure_project_tasks will do a full
        # fetch-and-upsert. No need to pass task_ids.

        trigger_ensure_project_tasks = TriggerDagRunOperator(
            task_id="trigger_ensure_project_tasks",
            trigger_dag_id=f"resource_planner_ensure_project_tasks_{config.instance}",
            conf=lambda dag_run, dag: {
                "project_id":       (result("modify_duplicate_project") or {}).get("uri", "").split(":")[-1],
                "task_ids":         [],
                "sourceSystem":     "Polaris",
                "masterRunId":      dag_run.run_id if dag_run else "",
                "triggeredByDagId": dag.dag_id if dag else "",
            },
            wait_for_completion=False,
        )

        # ------------------------------------------------------------
        # 8. Result + failure logging
        # ------------------------------------------------------------

        join_before_processing_result = EmptyOperator(
            task_id="join_before_processing_result",
            trigger_rule="none_failed_min_one_success",
        )

        processing_result = PythonOperator(
            task_id="processing_result",
            python_callable=lambda: custom_methods.processing_result(
                result("capture_conf"), result("check_project_exists")
            ),
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

        capture_conf >> validate_opportunity >> resolve_project_template_name >> search_client_in_polaris >> client_exists
        client_exists >> Label("No") >> create_client_in_polaris >> collect_client_uri
        client_exists >> Label("Yes") >> collect_client_uri

        collect_client_uri >> check_project_exists >> project_already_exists

        project_already_exists >> Label("Yes") >> join_before_processing_result

        (project_already_exists >> Label("No")
            >> get_project_template
            >> guard_template_found
            >> create_duplicate_project
            >> processing_batch_in_background
            >> wait_for_duplicate_project_batch
            >> modify_duplicate_project
            >> update_client
            >> update_project_workflow_state
            >> trigger_ensure_project_tasks
            >> join_before_processing_result)

        join_before_processing_result >> processing_result >> log_failure >> end_task

    return dag


for_each_instance(create_source_opportunities_project_sync_create_project_dag)
