"""
### Confirmed Bookings Export — Per-mutation op-DAGs

Three op-DAGs spawn one DAG run per Polaris mutation. The page child DAG
triggers them via ``TriggerDagRunForEachItemOperator`` so:

- Each Polaris call is its own Airflow run with isolated retries / logs
- One failed mutation never blocks siblings on the same page
- Re-trigger a failed run from the UI to retry just that mutation
- Each op-DAG markPushes its own slice of rows on success, so a partial
  page-success leaves the gateway/db consistent (no all-or-nothing
  markPushed call)

DAGs:
- ``..._op_create_{instance}``  — one CREATE allocation per run
- ``..._op_update_{instance}``  — one PARTIAL update per run, bulk
                                  per booking with mixed UPDATE/DELETE
                                  days driven by scheduleRules

#### Why no DELETE op-DAG

DELETE is just an UPDATE with ``setHours: 0`` for that day's rule, and
``updateTaskResourceUserAllocation`` PARTIAL accepts a ``scheduleRules``
array — so a single mutation can apply mixed UPDATE/DELETE day values
in one call. We collapse them into the UPDATE op-DAG (one run per
booking).

#### Idempotency

CREATE: if Polaris returns "already exists" (allocation was created on a
prior attempt that lost its response), we treat the call as success so
markPushed runs and the row signal clears.

UPDATE: PARTIAL with explicit ``setHours`` per scheduleRule is naturally
idempotent — re-issuing sets the same final value.

#### Input — dag_run.conf

CREATE op:
```json
{
  "tenant_id":         "...",
  "source_booking_id": "BK-3001",
  "project_id":        "P-4400",
  "task_id":           "T-9905",
  "user_uri":          "300",
  "schedule_rules":    [...],
  "pushed_rows":       [{"sourceBookingId": "...", "workDate": "..."}, ...],
  "sourceSystem":      "Polaris",
  "targetTable":       "..."  (optional)
}
```

UPDATE op (per-booking, bulk):
```json
{
  "tenant_id":         "...",
  "source_booking_id": "BK-3001",
  "project_id":        "P-4400",
  "task_id":           "T-9905",
  "user_uri":          "urn:replicon-tenant:...:user:300",
  "schedule_rules":    [...],   // mixed UPDATE (setHours>0) + DELETE (setHours=0)
  "pushed_rows":       [{"sourceBookingId": "...", "workDate": "..."}, ...],
  "sourceSystem":      "Polaris",
  "targetTable":       "..."  (optional)
}
```

The page child also passes through ``lastModifiedAfter / upperBound /
pageSize / pageNumber / masterRunId`` so ``log_sync_failure`` (the
trigger_rule="one_failed" task) can log a meaningful sync-failure record.
"""
import json
from datetime import datetime, timedelta

from rail import (
    for_each_instance, create_airflow_dag, result,
    PythonOperator, SimpleHttpOperator, EmptyOperator,
    ViewDagRunConfOperator, RepliconServiceCallForEachItemOperator,
)
from resource_planner.confirmed_bookings_export.utils import build_uri

API_HEADERS = {"Content-Type": "application/json"}

CREATE_MUTATION_TEMPLATE = """mutation CreateTaskResourceUserAllocation($input: CreateTaskResourceUserAllocationInput!) {
  createTaskResourceUserAllocation(input: $input) {
    taskResourceUserAllocation {
      id
      taskUri
      projectUri
      allocationUserUri
      totalHours
      startDate
      endDate
    }
  }
}"""

PUT_TASK_RESOURCE_ESTIMATE_TEMPLATE = """mutation PutTaskResourceEstimate($input: PutTaskResourceEstimateInput!) {
  putTaskResourceEstimate(input: $input) {
    taskResourceEstimateId
  }
}"""

FETCH_ALLOCATION_QUERY = """query FetchAllocationById(
    $projectUri: String!,
    $userUri: String!,
    $taskUris: [String!]!
) {
    taskResourceUserAllocationsForUser(
        filter: {
            projectUri: $projectUri,
            userUri: $userUri,
            taskUris: $taskUris
        }
    ) {
        id
        scheduleRules {
            dateRange {
                startDate
                endDate
            }
        }
    }
}"""


# -----------------------------------------------------------------------------
# Failure logging — ``log_failure`` runs with trigger_rule="all_done" at the
# end of every op-DAG run (linear, no fan-in). It inspects this run's task
# states and returns either:
#   - a structured failure record (if any task is in FAILED state), OR
#   - None (if everything succeeded)
# The parent page-child gathers these via GatherResultsFromDagRunsOperator
# and filters out None entries; the master then aggregates + emails.
# -----------------------------------------------------------------------------

def _common_dag_args(config):
    return {
        "start_date":              config.start_date,
        "company_key":             config.company_key,
        "replicon_conn_id":        config.replicon_conn_id,
        "max_active_runs":         getattr(config, "max_active_runs_op", 10),
        "schedule_interval":       None,
        "is_paused_upon_creation": True,
        "default_args": {
            "owner": "resource_planner",
            "retries": 2,
            "retry_delay": timedelta(minutes=1),
        },
    }


def _log_failure_callable(**context):
    """Push a structured failure record to XCom (or None if no failures).

    Always runs (trigger_rule="all_done"), inspects dag_run task states,
    and returns None when nothing failed. The parent page-child gathers
    these XComs and filters None entries.
    """
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run else None) or {}

    failed_task_ids = []
    if dag_run:
        for ti in dag_run.get_task_instances():
            if str(ti.state) == "failed":
                failed_task_ids.append(ti.task_id)

    if not failed_task_ids:
        return None

    record = {
        "level":             "op",
        "source_booking_id": conf.get("source_booking_id", ""),
        "project_id":        conf.get("project_id", ""),
        "task_id":           conf.get("task_id", ""),
        "employee_id":       conf.get("employee_id", ""),
        "child_dag_id":      dag.dag_id if dag else "",
        "child_run_id":      dag_run.run_id if dag_run else "",
        "page_number":       int(conf.get("pageNumber") or 0),
        "master_run_id":     conf.get("masterRunId", ""),
        "failed_task_ids":   failed_task_ids,
        "error_excerpt":     f"Failed tasks: {', '.join(failed_task_ids)}"[:500],
    }
    print(f"log_failure (op): {record}")
    return record


# -----------------------------------------------------------------------------
# Shared callable: capture dag_run.conf into XCom so downstream callables
# (which don't get **context) can read it via result().
# -----------------------------------------------------------------------------

def _capture_conf_callable(**context):
    return (context.get("dag_run").conf if context.get("dag_run") else None) or {}


def _build_mark_pushed_payload():
    """Returns a callable for prepare_mark_pushed. Reads conf["pushed_rows"]
    (always a list) — both CREATE and UPDATE op-DAGs pass the rows that way."""
    def _callable(**context):
        conf = result("capture_conf") or {}
        payload = {
            "sourceSystem": conf.get("sourceSystem", "Polaris"),
            "rows":         conf.get("pushed_rows", []),
        }
        if conf.get("targetTable"):
            payload["targetTable"] = conf["targetTable"]
        return json.dumps(payload)

    return _callable


def _mark_pushed_operator(config, headers=None):
    return SimpleHttpOperator(
        task_id="mark_pushed",
        method="PATCH",
        http_conn_id=config.rp_api_conn_id,
        endpoint="/api/v1/rp/confirmedBookings/markPushed",
        headers=headers if headers is not None else API_HEADERS,
        data="{{ result('prepare_mark_pushed') }}",
        response_filter=lambda response: response.json(),
        log_response=True,
        extra_options={"verify": False},
    )


# =============================================================================
# CREATE op-DAG
# =============================================================================

def create_confirmed_bookings_op_create_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    with create_airflow_dag(
        dag_id=f"resource_planner_confirmed_bookings_op_create_{config.instance}",
        description="Op-DAG: one Polaris CREATE allocation per run",
        **_common_dag_args(config),
    ) as dag:

        ViewDagRunConfOperator(task_id="view_dag_run_conf")

        capture_conf = PythonOperator(
            task_id="capture_conf",
            python_callable=_capture_conf_callable,
        )

        def get_create_items():
            return [result("capture_conf")]

        def build_create_payload(item):
            tenant_id = item["tenant_id"]
            input_payload = {
                "taskAllocationId":  build_uri(tenant_id, "psa-task-allocation", item["source_booking_id"]),
                "taskUri":           build_uri(tenant_id, "task", item["task_id"]),
                "projectUri":        build_uri(tenant_id, "project", item["project_id"]),
                # user_uri is already a full URN resolved from BulkGetUsers3
                "allocationUserUri": item["user_uri"],
                "scheduleRules":     item["schedule_rules"],
            }
            # Only include roleUri when labor_code resolved to a Polaris role.
            # Misses are tracked in the page-child artifact (missing_roles) and
            # surfaced in the master's alert email.
            if item.get("role_uri"):
                input_payload["roleUri"] = item["role_uri"]
            return {
                "query": CREATE_MUTATION_TEMPLATE,
                "variables": {"input": input_payload},
            }

        def handle_create_response(data, item):
            errors = data.get("errors") or []
            if errors:
                # Self-heal: a prior attempt may have created the allocation
                # but lost its response. Treat duplicate as success so
                # markPushed runs and the row signal clears.
                for err in errors:
                    msg = (err.get("message") or "").lower()
                    if "already exists" in msg or "duplicate" in msg:
                        print(
                            f"CREATE {item['source_booking_id']}: "
                            f"allocation already exists — treating as success"
                        )
                        return data
                print(f"ERROR create {item['source_booking_id']}: {errors}")
                raise Exception(f"Polaris CREATE failed: {errors}")
            alloc = (data.get("data", {})
                         .get("createTaskResourceUserAllocation", {})
                         .get("taskResourceUserAllocation", {}))
            print(f"CREATE {item['source_booking_id']} -> totalHours={alloc.get('totalHours')}")
            return data

        execute_create = RepliconServiceCallForEachItemOperator(
            task_id="execute_create",
            replicon_conn_id=config.replicon_conn_id,
            app="polaris",
            endpoint=config.graphql_endpoint,
            items=get_create_items,
            data=build_create_payload,
            data_handler=handle_create_response,
            flatten=True,
        )

        # ---- After CREATE succeeds, register the task resource estimate.
        # ``putTaskResourceEstimate`` is naturally idempotent (PUT) — repeat
        # calls are safe and just return the existing taskResourceEstimateId.
        # If this mutation fails, markPushed won't run, the row signal stays
        # non-null, and the next master cycle retries the whole booking.

        def build_put_estimate_payload(item):
            tenant_id = item["tenant_id"]
            input_payload = {
                "taskId":         build_uri(tenant_id, "task", item["task_id"]),
                # user_uri is already a full URN resolved from BulkGetUsers3
                "resourceUserId": item["user_uri"],
            }
            # if item.get("role_uri"):
            #     input_payload["roleUri"] = item["role_uri"]
            return {
                "query": PUT_TASK_RESOURCE_ESTIMATE_TEMPLATE,
                "variables": {"input": input_payload},
            }

        def handle_put_estimate_response(data, item):
            if data.get("errors"):
                print(
                    f"ERROR putTaskResourceEstimate {item['source_booking_id']} "
                    f"task={item['task_id']} user={item['user_uri']}: {data['errors']}"
                )
                raise Exception(f"Polaris putTaskResourceEstimate failed: {data['errors']}")
            estimate_id = (data.get("data", {})
                               .get("putTaskResourceEstimate", {})
                               .get("taskResourceEstimateId"))
            print(
                f"PUT estimate {item['source_booking_id']} "
                f"task={item['task_id']} -> taskResourceEstimateId={estimate_id}"
            )
            return data

        put_task_resource_estimate = RepliconServiceCallForEachItemOperator(
            task_id="put_task_resource_estimate",
            replicon_conn_id=config.replicon_conn_id,
            app="polaris",
            endpoint=config.graphql_endpoint,
            items=get_create_items,
            data=build_put_estimate_payload,
            data_handler=handle_put_estimate_response,
            flatten=True,
        )

        prepare_mark_pushed = PythonOperator(
            task_id="prepare_mark_pushed",
            python_callable=_build_mark_pushed_payload(),
        )

        mark_pushed = _mark_pushed_operator(config, headers=_api_headers)

        log_failure = PythonOperator(
            task_id="log_failure",
            python_callable=_log_failure_callable,
            trigger_rule="all_done",
        )

        end_task = EmptyOperator(task_id="end_task", trigger_rule="all_done")

        (capture_conf
            >> put_task_resource_estimate
            >> execute_create
            >> prepare_mark_pushed
            >> mark_pushed
            >> log_failure
            >> end_task)

    return dag


# =============================================================================
# UPDATE op-DAG
# =============================================================================

def create_confirmed_bookings_op_update_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    with create_airflow_dag(
        dag_id=f"resource_planner_confirmed_bookings_op_update_{config.instance}",
        description="Op-DAG: one Polaris PARTIAL update per run (bulk per booking via scheduleRules)",
        **_common_dag_args(config),
    ) as dag:

        ViewDagRunConfOperator(task_id="view_dag_run_conf")

        capture_conf = PythonOperator(
            task_id="capture_conf",
            python_callable=_capture_conf_callable,
        )

        # ---- Step 1: fetch current Polaris allocation for this booking ----

        def get_fetch_items():
            conf = result("capture_conf") or {}
            tenant_id = conf["tenant_id"]
            return [{
                "project_uri":        build_uri(tenant_id, "project", conf["project_id"]),
                "task_uri":           build_uri(tenant_id, "task", conf["task_id"]),
                "user_uri":           conf.get("user_uri", ""),
                "task_allocation_id": build_uri(tenant_id, "psa-task-allocation", conf["source_booking_id"]),
            }]

        def build_fetch_payload(item):
            return {
                "query": FETCH_ALLOCATION_QUERY,
                "variables": {
                    "projectUri": item["project_uri"],
                    "userUri":    item["user_uri"],
                    "taskUris":   [item["task_uri"]],
                },
            }

        def handle_fetch_response(data, item):
            allocations = (data.get("data", {})
                               .get("taskResourceUserAllocationsForUser")) or []
            target_id = item["task_allocation_id"]
            for alloc in allocations:
                if alloc.get("id") == target_id:
                    return {
                        "exists":         True,
                        "schedule_rules": alloc.get("scheduleRules") or [],
                    }
            return {"exists": False, "schedule_rules": []}

        fetch_current_allocation = RepliconServiceCallForEachItemOperator(
            task_id="fetch_current_allocation",
            replicon_conn_id=config.replicon_conn_id,
            app="polaris",
            endpoint=config.graphql_endpoint,
            items=get_fetch_items,
            data=build_fetch_payload,
            data_handler=handle_fetch_response,
            flatten=True,
        )

        # ---- Step 2: decide allocationEditMode ----
        # All rules setHours==0 means a full delete. PARTIAL fails in that case
        # (Polaris server bug). Cross-reference Polaris totalHours to confirm
        # whether there's anything to delete before switching to FULL.

        def determine_edit_mode_callable():
            conf = result("capture_conf") or {}
            schedule_rules = conf.get("schedule_rules") or []
            all_zero = bool(schedule_rules) and all(
                r["do"]["setHours"] == 0 for r in schedule_rules
            )

            if not all_zero:
                return "PARTIAL"

            # All our rules are zero — determine whether to use FULL or PARTIAL
            # by comparing our date set against Polaris's current date set.
            # FULL is only correct when our rules cover every day Polaris has
            # (no extra days outside our range). If Polaris has days we are NOT
            # zeroing out, use PARTIAL so those days are left untouched.

            def expand_to_dates(rules):
                dates = set()
                for rule in rules:
                    dr = (rule.get("dateRange") or {})
                    start = (dr.get("startDate") or "")[:10]
                    end   = (dr.get("endDate")   or "")[:10]
                    if not start or not end:
                        continue
                    d     = datetime.strptime(start, "%Y-%m-%d").date()
                    end_d = datetime.strptime(end,   "%Y-%m-%d").date()
                    while d <= end_d:
                        dates.add(d.isoformat())
                        d += timedelta(days=1)
                return dates

            our_dates = expand_to_dates(schedule_rules)

            fetched = result("fetch_current_allocation") or []
            current = fetched[0] if fetched else {}
            polaris_rules = current.get("schedule_rules") or []

            if not polaris_rules:
                # Allocation not found or empty in Polaris — PARTIAL is a no-op
                print("determine_edit_mode: no polaris allocation found, mode=PARTIAL")
                return "PARTIAL"

            polaris_dates = expand_to_dates(polaris_rules)
            extra_in_polaris = polaris_dates - our_dates

            mode = "PARTIAL" if extra_in_polaris else "FULL"
            print(
                f"determine_edit_mode: our_dates={len(our_dates)}, "
                f"polaris_dates={len(polaris_dates)}, "
                f"extra_in_polaris={len(extra_in_polaris)}, mode={mode}"
            )
            return mode

        determine_edit_mode = PythonOperator(
            task_id="determine_edit_mode",
            python_callable=determine_edit_mode_callable,
        )

        # ---- Step 3: build and execute the update mutation ----

        def get_update_items():
            return [result("capture_conf")]

        def build_update_payload(item):
            tenant_id = item["tenant_id"]
            project_uri = build_uri(tenant_id, "project", item["project_id"])
            task_uri = build_uri(tenant_id, "task", item["task_id"])
            task_allocation_id = build_uri(tenant_id, "psa-task-allocation", item["source_booking_id"])
            schedule_rules = item["schedule_rules"]
            allocation_edit_mode = result("determine_edit_mode") or "PARTIAL"

            def fmt_rules(rules):
                parts = []
                for rule in rules:
                    dr = rule["dateRange"]
                    do = rule["do"]
                    exclude = do.get("excludeWeekdays") or []
                    exclude_str = "[" + ", ".join(str(d) for d in exclude) + "]"
                    parts.append(
                        "{"
                        f'\n          dateRange: {{'
                        f'\n            startDate: "{dr["startDate"]}",'
                        f'\n            endDate: "{dr["endDate"]}"'
                        f'\n          }}'
                        f'\n          do: {{'
                        f'\n            load: {do["load"]},'
                        f'\n            setHours: {do["setHours"]},'
                        f'\n            excludeWeekdays: {exclude_str}'
                        f'\n          }}'
                        "\n        }"
                    )
                return "[\n        " + "\n        ".join(parts) + "\n      ]"

            role_line = f'\n      roleUri: "{item["role_uri"]}",' if item.get("role_uri") else ""

            inline_mutation = (
                "mutation {\n"
                "  updateTaskResourceUserAllocation(\n"
                "    input: {\n"
                f"      allocationEditMode: {allocation_edit_mode},\n"
                "      allocationHours: 0\n"
                f'      projectUri: "{project_uri}",\n'
                f'      taskUri: "{task_uri}",\n'
                f'      taskAllocationId: "{task_allocation_id}",{role_line}\n'
                f"      scheduleRules: {fmt_rules(schedule_rules)}\n"
                "    }\n"
                "  ) {\n"
                "    taskResourceUserAllocation {\n"
                "      id\n"
                "      taskUri\n"
                "      projectUri\n"
                "      allocationUserUri\n"
                "      totalHours\n"
                "      startDate\n"
                "      endDate\n"
                "    }\n"
                "  }\n"
                "}"
            )
            return {"query": inline_mutation}

        def handle_update_response(data, item):
            if data.get("errors"):
                print(f"ERROR UPDATE {item['source_booking_id']}: {data['errors']}")
                raise Exception(f"Polaris UPDATE failed: {data['errors']}")
            alloc = (data.get("data", {})
                         .get("updateTaskResourceUserAllocation", {})
                         .get("taskResourceUserAllocation", {}))
            n_rules = len(item.get("schedule_rules") or [])
            print(f"UPDATE {item['source_booking_id']} ({n_rules} rule(s)) -> totalHours={alloc.get('totalHours')}")
            return data

        execute_update = RepliconServiceCallForEachItemOperator(
            task_id="execute_update",
            replicon_conn_id=config.replicon_conn_id,
            app="polaris",
            endpoint=config.graphql_endpoint,
            items=get_update_items,
            data=build_update_payload,
            data_handler=handle_update_response,
            flatten=True,
        )

        prepare_mark_pushed = PythonOperator(
            task_id="prepare_mark_pushed",
            python_callable=_build_mark_pushed_payload(),
        )

        mark_pushed = _mark_pushed_operator(config, headers=_api_headers)

        log_failure = PythonOperator(
            task_id="log_failure",
            python_callable=_log_failure_callable,
            trigger_rule="all_done",
        )

        end_task = EmptyOperator(task_id="end_task", trigger_rule="all_done")

        (capture_conf
            >> fetch_current_allocation
            >> determine_edit_mode
            >> execute_update
            >> prepare_mark_pushed
            >> mark_pushed
            >> log_failure
            >> end_task)

    return dag


# Register both op-DAGs per configured instance.
for_each_instance(create_confirmed_bookings_op_create_dag)
for_each_instance(create_confirmed_bookings_op_update_dag)
