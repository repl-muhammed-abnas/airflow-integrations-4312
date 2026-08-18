"""
### Source Opportunities Project Sync — Update Project Op-DAG

One run per qualifying opportunity (stage='Closed Won', probability=100),
triggered by the page-child DAG (``child_dag.py``). Normally the project
already exists in Polaris — created earlier by ``create_project.py`` when the
opportunity was at stage='Closing'. But that earlier step can be skipped or
fail independently (page-level failure, replay gap, ETL backfill straight to
Closed Won, etc.), so ``project_found`` branches instead of raising:

    find_existing_project
      -> project_found (IfOperator)
           Yes -> update startDate + servicesRevenue -> transition to "Execution"
           No  -> run the full create flow (client resolve, template resolve,
                  duplicate project, poll, apply opportunity data, attach
                  client) -> transition straight to "Execution" (no
                  intermediate "Initiate" stop — the opportunity is already
                  Closed Won by the time this runs)

This guarantees the Polaris project always ends up existing and at the
correct workflow stage, instead of failing the whole run when the create
step never happened.

#### Input (dag_run.conf — set by the page-child's trigger_update_project)

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
  "stageName":              "Closed Won",
  "probability":            100,
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


def create_source_opportunities_project_sync_update_project_dag(config):
    """Op-DAG: get one Polaris project into the Execution workflow stage for a
    Closed Won opportunity — updating it if it already exists, or creating it
    from scratch (mirroring create_project.py's flow) if it doesn't.

    ``project_found`` branches on ``find_existing_project`` instead of raising
    a hard guard — a missing project here is a recoverable gap (the earlier
    'Closing' -> Initiate step was skipped/failed/never ran for this
    opportunity), not a fatal error, so this run creates the project itself
    rather than requiring a manual replay of the create-project run first.
    """
    with create_airflow_dag(
        dag_id=f"resource_planner_source_opportunities_project_sync_update_project_{config.instance}",
        description="Op-DAG: get an existing/newly-created Polaris project to Execution stage for a Closed Won opportunity",
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
                result("capture_conf")
            ),
        )

        # ------------------------------------------------------------
        # 1. Find the project — branch, do NOT raise if missing. A missing
        #    project here means the earlier create step never ran/succeeded
        #    for this opportunity; the No branch below creates it instead of
        #    failing this run.
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
            yes_task="modify_existing_project",
            no_task="resolve_project_template_name",
        )

        # ------------------------------------------------------------
        # 2a. Project exists — update fields, transition to Execution.
        # ------------------------------------------------------------

        modify_existing_project = RepliconServiceOperator(
            task_id="modify_existing_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_modify_project_payload(
                result("capture_conf")
            ),
        )

        update_project_workflow_state = RepliconServiceOperator(
            task_id="update_project_workflow_state",
            endpoint="/graphql",
            app="polaris",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_workflow_state_mutation_payload(
                result("modify_existing_project")["uri"],
                config.POLARIS_EXECUTION_STATE_ID,
            ),
        )

        processing_result = PythonOperator(
            task_id="processing_result",
            python_callable=lambda: custom_methods.processing_result_update_execution(
                result("capture_conf")
            ),
        )

        # ------------------------------------------------------------
        # 2b. Project does not exist — run the create flow (mirrors
        #     create_project.py steps 2-6, minus its own idempotency check —
        #     find_existing_project above already confirmed no project exists),
        #     then transition straight to Execution (no Initiate stop).
        # ------------------------------------------------------------

        resolve_project_template_name = PythonOperator(
            task_id="resolve_project_template_name",
            python_callable=lambda: request_payload.resolve_project_template_name(
                result("capture_conf")
            ),
        )

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

        update_project_workflow_state_after_create = RepliconServiceOperator(
            task_id="update_project_workflow_state_after_create",
            endpoint="/graphql",
            app="polaris",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: request_payload.build_workflow_state_mutation_payload(
                result("modify_duplicate_project")["uri"],
                config.POLARIS_EXECUTION_STATE_ID,
            ),
        )

        processing_result_after_create = PythonOperator(
            task_id="processing_result_after_create",
            python_callable=lambda: custom_methods.processing_result_create_and_update_execution(
                result("capture_conf")
            ),
        )

        # ------------------------------------------------------------
        # 3. Join both branches, then shared failure logging + end.
        # ------------------------------------------------------------

        join_result = EmptyOperator(
            task_id="join_result",
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

        # Yes branch — project exists, update + transition
        (project_found
            >> Label("Yes")
            >> modify_existing_project
            >> update_project_workflow_state
            >> processing_result
            >> join_result)

        # No branch — project missing, create it, then transition
        (project_found
            >> Label("No")
            >> resolve_project_template_name
            >> search_client_in_polaris
            >> client_exists)
        client_exists >> Label("No") >> create_client_in_polaris >> collect_client_uri
        client_exists >> Label("Yes") >> collect_client_uri

        (collect_client_uri
            >> get_project_template
            >> guard_template_found
            >> create_duplicate_project
            >> processing_batch_in_background
            >> wait_for_duplicate_project_batch
            >> modify_duplicate_project
            >> update_client
            >> update_project_workflow_state_after_create
            >> processing_result_after_create
            >> join_result)

        join_result >> log_failure >> end_task

    return dag


for_each_instance(create_source_opportunities_project_sync_update_project_dag)
