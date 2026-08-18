"""
### Ensure Project Tasks — Async JIT resolver

Triggered (fire-and-forget) by allocation-writing DAGs whenever they see
an allocation whose ``time_code`` references a project/task that may not
yet be in ``rp_source_time_codes``. Allocations are polled more
frequently than the project_task snapshot, so brand-new projects/tasks
would otherwise leave orphan allocation rows until the next
project_task export cycle.

#### Flow

```
view_dag_run_conf
        │
   capture_conf
        │
   check_presence  (POST gateway /sourceTimeCodesProjectTasks/checkPresence)
        │
   has_missing  (IfOperator: not projectPresent OR missingTaskIds)
   ┌────┴────┐
 No│         │Yes
   │         ▼
   │   fetch_from_polaris  (RepliconServicePageOperator →
   │         │              /services/TaskListService1.svc/GetData,
   │         │              paginated, pagesize=1000)
   │   prepare_upsert_payload
   │         │
   │   upsert_time_codes  (PUT gateway /sourceTimeCodesProjectTasks)
   │         │
   └────► end_task  (trigger_rule="none_failed")
```

#### Input — dag_run.conf

```json
{
  "project_id":   "P-100",
  "task_ids":     ["T-501", "T-502"],
  "sourceSystem": "Polaris"
}
```

#### Idempotency

The upsert is naturally idempotent (MERGE in the gateway). If two
allocation DAGs trigger this DAG for the same project concurrently,
both runs may fetch Polaris and upsert — harmless, just extra work.

#### Polaris fetch

``fetch_from_polaris`` is a ``RepliconServicePageOperator`` calling
``/services/TaskListService1.svc/GetData``. Same pattern as
``project_task_export_delta`` — proven against Polaris's rate-limit /
timeout behaviour. Filters by single project URI (built from
``conf.project_id`` + the ``rp_tenant_id_*`` Airflow Variable); pagesize
1000 covers almost every project in a single request.

#### Task hierarchy

Each task row's ``fullPathItems`` (built from the full-path column's
``cellCollection`` in the TaskListService response) captures the chain
from outermost ancestor to leaf. We derive:

- ``taskLevel`` = ``len(fullPathItems)`` (1 for top-level, 2+ for nested)
- ``predecessor_time_code`` = ``project_id`` for level-1 tasks, else
  the bare id of the second-to-last item in ``fullPathItems`` (the
  immediate parent task)
- ``time_code_name`` = ``project_name`` joined with every name in
  ``fullPathItems`` by ``~``

#### Row format

Matches ``project_task_export_delta`` exactly so the MERGE in
``/sourceTimeCodesProjectTasks`` updates existing rows instead of
creating duplicates:

Project row → ``type='project'``, ``taskLevel=0``,
``actual_time_code``/``actual_time_code_name``/``predecessor_time_code``
are NULL.

Task row → ``type='task'``, ``taskLevel`` from hierarchy depth,
``time_code='{project_id}~{task_id}'``,
``time_code_name='{project_name}~{ancestor1}~...~{leaf}'``,
``actual_time_code=task_id`` (bare, last URN segment),
``actual_time_code_name=task_name`` (leaf only),
``predecessor_time_code`` = bare id of immediate parent (project for level 1).
"""
import itertools
import json
from datetime import timedelta

from rail import (
    for_each_instance, create_airflow_dag, result, Label,
    PythonOperator, IfOperator, SimpleHttpOperator, EmptyOperator,
    ViewDagRunConfOperator, RepliconServicePageOperator, RepliconServiceOperator,
)

from airflow.models import Variable

API_HEADERS = {"Content-Type": "application/json"}


# -----------------------------------------------------------------------------
# Polaris task fetch — TaskListService1.svc/GetData (paginated).
#
# Same operator and pattern as project_task_export_delta — proven against
# Polaris's rate-limit/timeout behaviour. Filters by single project URI;
# pagesize 1000 (enough for almost every project in one request).
#
# Response shape per row (columnUris=[full-path, task, project]):
#   cells[0]  full-path  — cellCollection: ordered list of ancestor tasks,
#                          leaf is last. Single-item for top-level tasks.
#                          Each item has: uri, textValue, slug, objectType.
#   cells[1]  task       — uri, textValue, slug (the leaf task itself)
#   cells[2]  project    — uri, textValue, slug
#
# all_result_data_handler flattens pages into a list of plain dicts whose
# ``fullPathItems`` captures the hierarchy so prepare_upsert_payload can
# compute taskLevel and predecessor_time_code correctly.
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# DAG factory
# -----------------------------------------------------------------------------

def create_ensure_project_tasks_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    with create_airflow_dag(
        dag_id=f"resource_planner_ensure_project_tasks_{config.instance}",
        description="Async JIT resolver: ensures rp_source_time_codes has the project+tasks referenced by an allocation",
        start_date=config.start_date,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        schedule_interval=None,
        is_paused_upon_creation=True,
        default_args={
            "owner": "resource_planner",
            "retries": 2,
            "retry_delay": timedelta(minutes=1),
        },
    ) as dag:

        ViewDagRunConfOperator(task_id="view_dag_run_conf")

        # ----------------------------------------------------------------
        # 0. Capture conf into XCom for downstream callables.
        # ----------------------------------------------------------------

        def capture_conf_callable(**context):
            return (context.get("dag_run").conf if context.get("dag_run") else None) or {}

        capture_conf = PythonOperator(
            task_id="capture_conf",
            python_callable=capture_conf_callable,
        )

        # ----------------------------------------------------------------
        # 1. Ask gateway which of (project_id, task_ids) is missing.
        # ----------------------------------------------------------------

        def prepare_check_payload_callable():
            conf = result("capture_conf") or {}
            payload = {
                "sourceSystem": conf.get("sourceSystem", "Polaris"),
                "projectId":    conf["project_id"],
                "taskIds":      conf.get("task_ids", []),
            }
            if getattr(config, "rp_api_target_table", None):
                payload["targetTable"] = config.rp_api_target_table
            return json.dumps(payload)

        prepare_check_payload = PythonOperator(
            task_id="prepare_check_payload",
            python_callable=prepare_check_payload_callable,
        )

        check_presence = SimpleHttpOperator(
            task_id="check_presence",
            method="POST",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/sourceTimeCodesProjectTasks/checkPresence",
            headers=_api_headers,
            data="{{ result('prepare_check_payload') }}",
            response_filter=lambda response: response.json(),
            log_response=True,
            extra_options={"verify": False},
        )

        # ----------------------------------------------------------------
        # 2. Branch — does anything need fetching from Polaris?
        # ----------------------------------------------------------------

        def has_missing_callable():
            conf = result("capture_conf") or {}
            if conf.get("force_refresh"):
                return True
            data = result("check_presence") or {}
            project_present = bool(data.get("projectPresent"))
            missing_tasks = data.get("missingTaskIds") or []
            print(
                f"has_missing: projectPresent={project_present} "
                f"missingTaskIds={missing_tasks}"
            )
            return (not project_present) or len(missing_tasks) > 0

        has_missing = IfOperator(
            task_id="has_missing",
            test=has_missing_callable,
            yes_task="get_project_details",
            no_task="end_task",
        )

        # ----------------------------------------------------------------
        # 3. Fetch ALL tasks for the project via TaskListService (paginated).
        # ----------------------------------------------------------------

        def get_task_list_payload():
            conf = result("capture_conf") or {}
            project_id = conf.get("project_id", "")
            tenant_id = Variable.get(config.tenant_id_variable)
            project_uri = f"urn:replicon-tenant:{tenant_id}:project:{project_id}"
            return {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    "urn:replicon:task-list-column:full-path",
                    "urn:replicon:task-list-column:task",
                    "urn:replicon:task-list-column:project",
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:task-list-filter:project"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "uri": project_uri
                        }
                    }
                }
            }

        def task_page_handler(request, response):
            """Continue paginating while a full page comes back."""
            if response.get("rows") and len(response["rows"]) >= request["pagesize"]:
                return {**request, "page": request["page"] + 1}
            return None

        def task_result_handler(results):
            """Flatten pages → list of plain dicts the upsert builder consumes.

            Each task dict carries ``fullPathItems`` — the ordered hierarchy
            of ancestor tasks (leaf last). For a top-level task it has
            exactly one item (the task itself); for nested tasks it's the
            chain from outermost parent → ... → leaf.
            """
            print("results", results)
            all_rows = list(itertools.chain(*[r.get("rows", []) for r in results]))
            tasks = []
            for row in all_rows:
                cells = row.get("cells") or []
                if len(cells) < 3:
                    continue
                full_path_cell = cells[0] or {}
                task_cell = cells[1] or {}
                project_cell = cells[2] or {}

                # full-path is a cellCollection (list of ancestor tasks).
                full_path_items = [
                    {
                        "uri":  (item or {}).get("uri"),
                        "name": (item or {}).get("textValue") or "",
                    }
                    for item in (full_path_cell.get("cellCollection") or [])
                ]

                tasks.append({
                    "taskUri":       task_cell.get("uri"),
                    "taskName":      task_cell.get("textValue") or "",
                    "fullPathItems": full_path_items,
                    "projectUri":    project_cell.get("uri"),
                    "projectName":   project_cell.get("textValue") or "",
                })
            return tasks

        fetch_from_polaris = RepliconServicePageOperator(
            task_id="fetch_from_polaris",
            endpoint="/services/TaskListService1.svc/GetData",
            data=get_task_list_payload,
            page_handler=task_page_handler,
            all_result_data_handler=task_result_handler,
        )

        def get_project_details_payload():
            conf = result("capture_conf") or {}
            project_id = conf.get("project_id", "")
            tenant_id = Variable.get(config.tenant_id_variable)
            project_uri = f"urn:replicon-tenant:{tenant_id}:project:{project_id}"
            return {
                "projects": [{
                    "uri": project_uri,
                    "name": None,
                    "code": None,
                    "parameterCorrelationId": None,
                }]
            }

        get_project_details = RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=get_project_details_payload,
        )

        # ----------------------------------------------------------------
        # 4. Build the upsert payload (mirrors what project_task_export
        #    writes). One Project row + N Task rows.
        # ----------------------------------------------------------------

        def prepare_upsert_payload_callable():
            """Shape the TaskListService response into the canonical
            ``rp_source_time_codes`` record format. Matches what
            ``project_task_export_delta`` writes so MERGE updates existing
            rows rather than creating duplicates.

            Canonical format (per row):
              project row:
                type='project', taskLevel=0
                time_code=project_id,  parent_time_code=project_id
                actual_time_code / actual_time_code_name / predecessor_time_code = NULL
              task row:
                type='task',    taskLevel=1 (JIT simplification — see below)
                time_code='{project_id}~{task_id}'
                parent_time_code=project_id
                time_code_name='{project_name}~{task_name}'
                actual_time_code=task_id          (bare, NOT the URN)
                actual_time_code_name=task_name   (leaf only)
                predecessor_time_code=project_id  (for level-1)
            """
            conf = result("capture_conf") or {}
            project_id = conf.get("project_id", "")
            tasks = result("fetch_from_polaris") or []

            # All tasks belong to one project — take project name from any
            # task row that has it.
            project_name = ""
            for t in tasks:
                if t.get("projectName"):
                    project_name = t["projectName"]
                    break
            if not project_name:
                project_name = project_id  # safe fallback

            # Extract client info from BulkGetProjectDetails3 response.
            project_response = result("get_project_details") or []
            project_details = project_response[0].get("projectDetails", {}) if project_response else {}
            clients = project_details.get("clients", []) or []
            client_obj = (clients[0].get("client", {}) or {}) if clients else {}
            client_name = client_obj.get("name", "") or ""
            client_uri = client_obj.get("uri", "") or ""
            client_id = client_uri.split(":")[-1] if client_uri else ""

            records = []

            # Project row.
            records.append({
                "sourceSystem":         "Polaris",
                "parentTimeCode":       project_id,
                "timeCode":             project_id,
                "timeCodeName":         project_name[:255],
                "parentTimeCodeName":   project_name,
                "projectManagerId":     "",
                "type":                 "project",
                "taskLevel":            0,
                "timeEntryEnabled":     False,
                # Project rows leave the new columns NULL per canonical;
                # UQ_rp_src_tc_actual filters on actual_time_code IS NOT NULL.
                "actualTimeCode":       project_id,
                "actualTimeCodeName":   project_name,
                "predecessorTimeCode":  None,
                "clientName":           client_name,
                "clientId":             client_id,
            })

            # Task rows.
            # taskLevel = depth of the ancestor chain returned by the
            # full-path cellCollection. predecessor_time_code = project_id
            # for level-1 tasks, else the bare id of the immediate parent.
            for t in tasks:
                task_uri = t.get("taskUri") or ""
                task_id = task_uri.split(":")[-1] if task_uri else ""
                if not task_id:
                    continue
                task_name = t.get("taskName") or ""
                full_path_items = t.get("fullPathItems") or []

                # At minimum the leaf task is present; nested tasks add ancestors.
                task_level = max(len(full_path_items), 1)

                if task_level <= 1:
                    predecessor_id = project_id
                else:
                    parent_uri = (full_path_items[-2] or {}).get("uri") or ""
                    predecessor_id = (
                        parent_uri.split(":")[-1] if parent_uri else project_id
                    )

                # time_code_name = project_name~ancestor1~...~leaf
                if full_path_items:
                    hierarchy_names = [
                        (item or {}).get("name") or "" for item in full_path_items
                    ]
                    time_code_name = "~".join([project_name, *hierarchy_names])
                else:
                    time_code_name = f"{project_name}~{task_name}"

                time_code = f"{project_id}~{task_id}"
                records.append({
                    "sourceSystem":         "Polaris",
                    "parentTimeCode":       project_id,
                    "timeCode":             time_code,
                    "timeCodeName":         time_code_name[:255],
                    "parentTimeCodeName":   project_name,
                    "projectManagerId":     "",
                    "type":                 "task",
                    "taskLevel":            task_level,
                    "timeEntryEnabled":     True,
                    "actualTimeCode":       task_id,
                    "actualTimeCodeName":   task_name,
                    "predecessorTimeCode":  predecessor_id,
                    "clientName":           client_name,
                    "clientId":             client_id,
                })

            payload = {"records": records}
            if getattr(config, "rp_api_target_table", None):
                payload["targetTable"] = config.rp_api_target_table

            return json.dumps(payload)

        prepare_upsert_payload = PythonOperator(
            task_id="prepare_upsert_payload",
            python_callable=prepare_upsert_payload_callable,
        )

        upsert_time_codes = SimpleHttpOperator(
            task_id="upsert_time_codes",
            method="PUT",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/sourceTimeCodesProjectTasks",
            headers=_api_headers,
            data="{{ result('prepare_upsert_payload') }}",
            response_check=lambda response: response.status_code == 200,
            log_response=True,
            extra_options={"verify": False},
        )

        # none_failed: fires when all upstream tasks succeeded or were skipped
        # (branch not taken). If any task fails, Airflow marks the DAG run
        # FAILED before reaching here — no log_failure wrapper needed.
        end_task = EmptyOperator(task_id="end_task", trigger_rule="none_failed")

        # ----------------------------------------------------------------
        # Dependencies
        # ----------------------------------------------------------------
        (capture_conf
            >> prepare_check_payload
            >> check_presence
            >> has_missing)

        has_missing >> Label("No") >> end_task
        (has_missing >> Label("Yes")
            >> get_project_details
            >> fetch_from_polaris
            >> prepare_upsert_payload
            >> upsert_time_codes
            >> end_task)

    return dag


for_each_instance(create_ensure_project_tasks_dag)
