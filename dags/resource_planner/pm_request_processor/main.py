"""
### PM Request Processor DAG

Triggered by the webhook receiver (resource_planner_pm_request_submitted_webhook)
when a PM submits a resource request in the RP tool. Receives ``webhook.data``
forwarded as ``dag_run.conf``:

```json
{
  "booking_start_date":     "2026-08-07",
  "effort_hours_design":    null,
  "effort_hours_execution": 8,
  "estimation_name":        "est 1",
  "jira_id":                "IWMTEST1-1097",
  "project_country":        null,
  "project_name":           "49437 - iPipeline Limited",
  "project_timezone":       null,
  "rp_request_number":      "RP000281",
  "status":                 "Resource Planning (Execution)",
  "target_due_date":        null
}
```

#### Task-creation flow

```
view_dag_run_conf
  → capture_conf (read dag_run.conf — already webhook.data)
  → validate_conf
  → find_project (BulkGetProjectDetails2 by project_name)
  → guard_project_found
  → is_project_contains_task_hierarchy_for_creation
      Yes → build_estimation_task_payload (actual project's own DPS TCoE)
      No  → get_project_template_tasks (template's DPS TCoE) → build_estimation_task_payload
  → build_estimation_task_payload → create_project_tasks → log_result → end_task
```

#### How the task hierarchy is built (2026-08-12)

Confirmed against ``sample_tasks_response.json``: under DPS TCoE there is
exactly one reference task, "Custom work - Placeholder 1", whose own
children are "Design" and "Execution" (each with their own real, deep
sub-tasks — workshops, dev/QA/UAT phases, etc.). This reference task is
NEVER modified or renamed.

``create_estimation_task`` locates that reference (in the actual project's
own tree if present, else the template's — falling back requires
``project_override`` since the template's tree is anchored to the template
project) and clones its shape into a brand-new sibling node named
``estimation_name`` via
``custom_methods.build_estimation_hierarchy_node``, keeping only the
Execution/Design branch(es) whose effort hours are non-null, each carrying
the real requested hours. ``isClosed`` is forced open (``False``)
throughout — production template placeholders are closed by design, but
every task actually created must be usable.

``create_project_tasks`` sends the resulting payload to
``/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications``.

#### Still open / not wired up

- ``booking_start_date`` / ``target_due_date`` are not applied anywhere yet
  (the cloned tasks keep the reference's own template dates).
- ``jira_id`` / ``rp_request_number`` are not attached anywhere —
  ``custom_methods._task_modification``'s ``customFieldsToApply`` is
  intentionally left empty (no confirmed *ToApply schema for custom fields
  yet, per that function's own note).
- The exact wire shape Polaris expects for ``estimatedHoursToApply.value``
  (a ``{hours,minutes,seconds}`` duration dict, matching what the read API
  returns, vs. a plain number) is inferred, not independently confirmed.
"""
import uuid

import rail
from airflow.models import Variable
from resource_planner.pm_request_processor.utils import custom_methods


def _safe_result(task_id, key=None):
    """Best-effort XCom pull — returns None if the task was skipped (e.g. by
    the is_project_contains_task_hierarchy_for_creation branch) instead of
    raising."""
    try:
        return rail.result(task_id, key) if key is not None else rail.result(task_id)
    except Exception as e:
        print(f"_safe_result({task_id!r}): task was skipped or XCom unavailable: {e!r}")
        return None


def create_pm_request_processor_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.pm_request_processor_dag_id,
        description="Processor: creates Polaris project tasks from a PM resource request",
        start_date=config.start_date,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        is_paused_upon_creation=True,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.resource_planner_pm_request_processor_enable_batch_task, "true"
            ).lower() == "true",
            yes_task="batch_task",
            no_task="capture_conf",
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="capture_conf",
            end_task="end_task",
        )

        # ----------------------------------------------------------------
        # 0. dag_run.conf is already webhook.data (forwarded by receiver).
        # ----------------------------------------------------------------

        def capture_conf_callable(**context):
            return (context.get("dag_run").conf if context.get("dag_run") else None) or {}

        capture_conf = rail.PythonOperator(
            task_id="capture_conf",
            python_callable=capture_conf_callable,
        )

        # ----------------------------------------------------------------
        # 1. Validate required fields before any Polaris call.
        # ----------------------------------------------------------------

        def validate_conf_callable():
            data = rail.result("capture_conf") or {}
            project_name = data.get("project_name")
            if not project_name:
                raise ValueError(
                    "project_name is required but missing or empty — "
                    "cannot identify which Polaris project to add tasks to."
                )
            if not data.get("estimation_name"):
                raise ValueError(
                    f"Project '{project_name}': estimation_name is required but "
                    "missing or empty — cannot name the new task."
                )
            if not data.get("effort_hours_execution") and not data.get("effort_hours_design"):
                raise ValueError(
                    f"Project '{project_name}': both effort_hours_execution and "
                    "effort_hours_design are null — nothing to create."
                )
            print(
                f"validate_conf: project={project_name!r} "
                f"effort_execution={data.get('effort_hours_execution')} "
                f"effort_design={data.get('effort_hours_design')} "
                f"estimation_name={data.get('estimation_name')!r}"
            )
            return data

        validate_conf = rail.PythonOperator(
            task_id="validate_conf",
            python_callable=validate_conf_callable,
        )

        # ----------------------------------------------------------------
        # 2. Find the Polaris project by project_name.
        # ----------------------------------------------------------------

        def find_project_data_handler(response):
            results = (response or {}).get('results') or []
            if not results:
                return None
            tasks = results[0].get('tasks') or []
            rail.set_result(key="task_details", val=rail.write_json_artifact(tasks))
            task_hierarchy_for_creation = rail.find_first_by_attr_and_get_attr(
                tasks,
                'task.name',
                config.TASK_HIERARCHY_ROOT_NAME,
                default={}
            )
            rail.set_result(key="task_hierarchy_for_creation", val=task_hierarchy_for_creation)
            project = results[0].get('project') or {}
            return {
                "uri": project.get("uri"),
                "name": project.get("name"),
                "code": project.get("code"),
                "project_details": rail.write_json_artifact(project)
            }


        find_project = rail.RepliconServiceOperator(
            task_id="find_project",
            endpoint="/services/ImportService1.svc/BulkGetProjects2",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: {
                "projects": [
                    {
                        "uri": None,
                        "name": (rail.result("capture_conf") or {}).get("project_name", "").split(" - ")[1],
                        "code": None,
                        "parameterCorrelationId": None,
                    }
                ]
            },
            data_handler=find_project_data_handler
        )

        # ----------------------------------------------------------------
        # 3. Guard — the Polaris project must already exist.
        # ----------------------------------------------------------------

        def guard_project_found_callable():
            projects = rail.result("find_project") or []
            if not projects:
                project_name = (rail.result("capture_conf") or {}).get("project_name", "")
                raise ValueError(
                    f"No Polaris project found with name '{project_name}'."
                )
            return projects

        guard_project_found = rail.PythonOperator(
            task_id="guard_project_found",
            python_callable=guard_project_found_callable,
        )

        # ----------------------------------------------------------------
        # 4. Branch on whether the target project already carries its own
        #    DPS TCoE hierarchy (captured by find_project's data_handler);
        #    fall back to the template's hierarchy if it doesn't.
        # ----------------------------------------------------------------

        is_project_contains_task_hierarchy_for_creation = rail.IfOperator(
            task_id="is_project_contains_task_hierarchy_for_creation",
            test=lambda: bool(
                (rail.result("find_project", "task_hierarchy_for_creation"))
            ),
            yes_task="build_estimation_task_payload",
            no_task="get_project_template_tasks",
        )


        def get_project_template_tasks_callable(response):
            results = (response or {}).get('results') or []
            if not results:
                raise ValueError(
                    f"No tasks found in project template {config.PROJECT_TEMPLATE_NAME!r}."
                )
            tasks = results[0].get('tasks') or []
            task_hierarchy_for_creation = rail.find_first_by_attr_and_get_attr(
                tasks,
                'task.name',
                config.TASK_HIERARCHY_ROOT_NAME,
                default={}
            )
            if not task_hierarchy_for_creation:
                raise ValueError(
                    f"No {config.TASK_HIERARCHY_ROOT_NAME!r} task hierarchy found in "
                    f"project template {config.PROJECT_TEMPLATE_NAME!r}."
                )
            return task_hierarchy_for_creation

        get_project_template_tasks = rail.RepliconServiceOperator(
            task_id="get_project_template_tasks",
            endpoint="/services/ImportService1.svc/BulkGetProjects2",
            replicon_conn_id=config.replicon_conn_id,
            data={
                "projects": [
                    {
                        "uri": None,
                        "name": config.PROJECT_TEMPLATE_NAME,
                        "code": None,
                        "parameterCorrelationId": None,
                    }
                ]
            },
            data_handler=get_project_template_tasks_callable,
        )

        # ----------------------------------------------------------------
        # 5. Build the real task-creation payload.
        #
        #    Locates "Custom work - Placeholder 1" under DPS TCoE (whichever
        #    tree applied — the actual project's own, or the template's
        #    fallback) and clones ITS SHAPE (never modifying it) into a new
        #    sibling node named estimation_name, keeping only the
        #    Execution/Design branch(es) whose effort hours are present —
        #    see custom_methods.build_estimation_hierarchy_node.
        #
        #    Falling back to the template requires project_override, since
        #    the template's own tree is anchored to the template project,
        #    not the real target project.
        # ----------------------------------------------------------------

        def build_task_creation_payload_callable():
            conf = rail.result("capture_conf") or {}
            project = rail.result("guard_project_found") or {}

            dps_tcoe_node = (
                _safe_result("find_project", "task_hierarchy_for_creation")
                or _safe_result("get_project_template_tasks")
            )
            if not dps_tcoe_node:
                raise ValueError(
                    "No 'DPS TCoE' task hierarchy found in either the actual "
                    "project or the project template — cannot build the "
                    "task-creation payload."
                )

            reference_placeholder = custom_methods.find_node_by_name(
                dps_tcoe_node, config.REFERENCE_PLACEHOLDER_TASK_NAME
            )
            if reference_placeholder is None:
                raise ValueError(
                    f"No {config.REFERENCE_PLACEHOLDER_TASK_NAME!r} reference task found "
                    f"under {config.TASK_HIERARCHY_ROOT_NAME!r} — cannot build the "
                    "task-creation payload."
                )

            project_override = None
            if not _safe_result("find_project", "task_hierarchy_for_creation"):
                # Falling back to the template's shape — redirect the
                # created tasks to the actual project, not the template.
                project_override = {"name": project.get("name"), "code": project.get("code")}

            estimation_node = custom_methods.build_estimation_hierarchy_node(
                reference_placeholder, conf
            )

            if project_override is None:
                # Yes-path: DPS TCoE already exists in the actual project;
                # estimation_node's target already points to it correctly.
                return custom_methods.build_task_hierarchy_payload(
                    estimation_node,
                    unit_of_work_id=str(uuid.uuid4()),
                )

            # No-path: DPS TCoE doesn't exist in the actual project yet (template
            # fallback). Include DPS TCoE as the payload root so it gets created
            # first, with only the estimation node as its child. project_override
            # redirects the project reference from the template to the actual project.
            dps_tcoe_payload_root = {
                "task": {
                    **dps_tcoe_node.get("task", {}),
                    "uri": None,
                    "parent": None,  # top-level in actual project
                },
                "childTasks": [estimation_node],
                "resourceAssignments": [],
            }
            return custom_methods.build_task_hierarchy_payload(
                dps_tcoe_payload_root,
                unit_of_work_id=str(uuid.uuid4()),
                project_override=project_override,
            )

        # none_failed_min_one_success: this task converges both edges of
        # is_project_contains_task_hierarchy_for_creation (Yes skips
        # get_project_template_tasks, No runs it) — the default
        # all_success trigger rule would treat that skip as unsatisfied
        # and cascade-skip this task (and everything after it) on the
        # Yes path, silently creating nothing.
        build_estimation_task_payload = rail.PythonOperator(
            task_id="build_estimation_task_payload",
            python_callable=build_task_creation_payload_callable,
            trigger_rule="none_failed_min_one_success",
        )

        # ----------------------------------------------------------------
        # 6. Send the task-creation payload to Polaris.
        # ----------------------------------------------------------------

        create_project_tasks = rail.RepliconServiceOperator(
            task_id="create_project_tasks",
            endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
            replicon_conn_id=config.replicon_conn_id,
            data=lambda: rail.result("build_estimation_task_payload"),
        )

        # ----------------------------------------------------------------
        # 7. JIT-populate rp_source_time_codes (fire-and-forget).
        # ----------------------------------------------------------------
        # The project already exists in rp_source_time_codes (put there when
        # create_project created it). force_refresh=True bypasses the
        # checkPresence short-circuit so ensure_project_tasks fetches all
        # tasks from Polaris and upserts the full hierarchy — including the
        # estimation tasks just created above.

        trigger_ensure_project_tasks = rail.TriggerDagRunOperator(
            task_id="trigger_ensure_project_tasks",
            trigger_dag_id=f"resource_planner_ensure_project_tasks_{config.instance}",
            conf=lambda dag_run, dag: {
                "project_id":       ((rail.result("find_project") or {}).get("uri") or "").split(":")[-1],
                "task_ids":         [],
                "sourceSystem":     "Polaris",
                "force_refresh":    True,
                "masterRunId":      dag_run.run_id if dag_run else "",
                "triggeredByDagId": dag.dag_id if dag else "",
            },
            wait_for_completion=False,
        )

        end_task = rail.EmptyOperator(task_id="end_task")

        # ----------------------------------------------------------------
        # Dependencies
        # ----------------------------------------------------------------

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> end_task
        can_run_batch_task >> rail.Label("No") >> capture_conf

        (capture_conf
            >> validate_conf
            >> find_project
            >> guard_project_found >> is_project_contains_task_hierarchy_for_creation >> rail.Label("No")
            >> get_project_template_tasks >> build_estimation_task_payload)
        is_project_contains_task_hierarchy_for_creation >> rail.Label("Yes") >> build_estimation_task_payload

        build_estimation_task_payload >> create_project_tasks >> trigger_ensure_project_tasks >> end_task

    return dag


rail.for_each_instance(create_pm_request_processor_dag)
