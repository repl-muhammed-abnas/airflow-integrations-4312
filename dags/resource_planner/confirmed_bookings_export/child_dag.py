"""
### Confirmed Bookings Export — Page Child DAG template (3 copies created)

Triggered by the master DAG with a specific ``pageNumber`` in ``dag_run.conf``.
Fetches that page from the gateway, classifies each row by
``outboundPendingOp``, then **triggers one op-DAG run per Polaris mutation**
and waits for all of them to complete.

#### Why per-mutation op-DAGs?

Earlier the page child issued all mutations from inside one Airflow task. A
single failure mid-page would block siblings and force a full-page replay,
during which already-succeeded CREATEs would fail with "duplicate
allocation" — a manual-cleanup hole.

Now every Polaris call is its own DAG run (see ``op_dags.py``). One failure
is isolated; retries are per-mutation; each op-DAG markPushes its own slice
of rows on success, so partial-page success leaves the gateway/db
consistent.

#### Per-row dispatch

Each day in the page carries its own ``outboundPendingOp``:
- ``ADD``    → all ADD days for a booking → ONE op-create-DAG run with
              collapsed schedule rules
- ``UPDATE`` / ``DELETE`` → all non-ADD days for a booking → ONE
              op-update-DAG run with collapsed scheduleRules. PARTIAL
              accepts a ``scheduleRules`` array, so a single mutation
              applies the per-day setHours value to every rule. DELETE
              days are encoded as ``setHours: 0`` rules within the same
              array.

A booking can mix ops across days (day 1 ADD, day 5 UPDATE, day 6
DELETE). The triggers fire creates → updates sequentially so a CREATE
has settled in Polaris before any PARTIAL on the same allocation runs.

#### Routing

Master routes pages as ``child_num = ((pageNumber - 1) % 3) + 1``. Three
page-child DAGs are parsed at import time:
- ``..._child_{instance}_1``  → pages 1, 4, 7, 10, ...
- ``..._child_{instance}_2``  → pages 2, 5, 8, 11, ...
- ``..._child_{instance}_3``  → pages 3, 6, 9, 12, ...

#### Concurrency

Each page-child is ``max_active_runs=3``. Op-DAGs are
``max_active_runs_op=10`` (configurable). Peak Polaris pressure ≈
3 × 3 × 10 = 90 simultaneous calls — usually clamped lower by op-DAG
queueing.

#### Failure

A dedicated ``log_failure`` task wired with ``trigger_rule="one_failed"``
fires only when an upstream task ends in FAILED state, after Airflow's task
retries are exhausted. It does **not** write to any DB or call any gateway
endpoint — it gathers the op-DAG failure XComs (via
``gather_create_failures`` / ``gather_update_failures``) and returns one
structured page-level record to its own XCom. The master DAG collects all
page-children's records, formats a report, and emails on-call. Op-DAGs
have a sibling ``log_failure`` task on the same pattern.

#### Input (dag_run.conf — set by master)

```json
{
  "pageNumber":        5,
  "lastModifiedAfter": "2026-04-22T10:00:00.000+05:30",
  "upperBound":        "2026-04-22T14:30:15.123+05:30",
  "pageSize":          100,
  "masterRunId":       "manual__2026-04-23T09:00:00+05:30",
  "sourceSystem":      "Polaris",
  "targetTable":       "dummy_..." (optional)
}
```
"""
import json
from datetime import datetime, timedelta

from rail import (
    for_each_instance, create_airflow_dag, result, Label,
    write_json_artifact, load_json_artifact, set_result,
    PythonOperator, IfOperator, BatchTaskRunOperator, SimpleHttpOperator, EmptyOperator,
    ViewDagRunConfOperator, TriggerDagRunForEachItemOperator,
    WaitForDagRunsSensor, RepliconServiceOperator,
    GatherResultsFromDagRunsOperator,
)
from airflow.models import Variable
from resource_planner.confirmed_bookings_export.utils import (
    collapse_daily_rows_to_schedule_rules,
)

API_HEADERS = {"Content-Type": "application/json"}


def _pushed_rows_from_schedule_rules(item):
    """Self-heal helper: derive pushed_rows by walking each scheduleRule's
    date range, one row per day. Used as a fallback when an item from a
    stale ``classify_and_build`` artifact is missing ``pushed_rows`` (i.e.
    classify ran with older code that didn't attach the per-item field).
    """
    sbid = item.get("source_booking_id", "")
    rows = []
    for rule in item.get("schedule_rules", []) or []:
        dr = rule.get("dateRange") or {}
        start = (dr.get("startDate") or "")[:10]
        end   = (dr.get("endDate")   or "")[:10]
        if not start or not end:
            continue
        try:
            d     = datetime.strptime(start, "%Y-%m-%d").date()
            end_d = datetime.strptime(end,   "%Y-%m-%d").date()
        except ValueError:
            continue
        while d <= end_d:
            rows.append({"sourceBookingId": sbid, "workDate": d.isoformat()})
            d += timedelta(days=1)
    return rows


# -----------------------------------------------------------------------------
# Factory: loops config.child_count times to create N sibling page-child DAGs
# -----------------------------------------------------------------------------

def create_confirmed_bookings_export_child_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    op_create_dag_id = f"resource_planner_confirmed_bookings_op_create_{config.instance}"
    op_update_dag_id = f"resource_planner_confirmed_bookings_op_update_{config.instance}"

    for child_num in range(1, config.child_count + 1):
        with create_airflow_dag(
            dag_id=f"resource_planner_confirmed_bookings_export_child_{config.instance}_{child_num}",
            description=(
                f"Child #{child_num}: fetches one page of confirmed bookings "
                f"and triggers per-mutation op-DAG runs"
            ),
            start_date=config.start_date,
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_child,
            schedule_interval=None,
            is_paused_upon_creation=True,
            default_args={
                "owner": "resource_planner",
                "retries": 2,
                "retry_delay": timedelta(minutes=1),
            },
        ) as dag:

            ViewDagRunConfOperator(task_id="view_dag_run_conf")

            can_run_batch_task = IfOperator(
                task_id="can_run_batch_task",
                test=lambda: Variable.get(
                    config.resource_planner_confirmed_bookings_export_enable_batch_task, "true"
                ).lower() == "true",
                yes_task="batch_task",
                no_task="capture_page_conf",
            )

            batch_task = BatchTaskRunOperator(
                task_id="batch_task",
                start_task="capture_page_conf",
                end_task="end_task",
            )

            # ----------------------------------------------------------------
            # 0. Capture this run's conf into XCom so downstream callables
            #    (which don't get **context) can read it via result().
            # ----------------------------------------------------------------

            def capture_page_conf_callable(**context):
                return (context.get("dag_run").conf if context.get("dag_run") else None) or {}

            capture_page_conf = PythonOperator(
                task_id="capture_page_conf",
                python_callable=capture_page_conf_callable,
            )

            # ----------------------------------------------------------------
            # 1. Fetch this page from the gateway
            # ----------------------------------------------------------------

            def prepare_page_payload(**context):
                conf = (context.get("dag_run").conf if context.get("dag_run") else None) or {}
                payload = {
                    "sourceSystem":      conf.get("sourceSystem", "Polaris"),
                    "lastModifiedAfter": conf.get("lastModifiedAfter"),
                    "upperBound":        conf.get("upperBound"),
                    "pageSize":          int(conf.get("pageSize", config.page_size)),
                    "pageNumber":        int(conf.get("pageNumber")),
                }
                if conf.get("targetTable"):
                    payload["targetTable"] = conf["targetTable"]
                return json.dumps(payload)

            prepare_page_request = PythonOperator(
                task_id="prepare_page_request",
                python_callable=prepare_page_payload,
            )

            fetch_page = SimpleHttpOperator(
                task_id="fetch_page",
                method="POST",
                http_conn_id=config.rp_api_conn_id,
                endpoint="/api/v1/rp/confirmedBookings",
                headers=_api_headers,
                data="{{ result('prepare_page_request') }}",
                response_filter=lambda response: response.json(),
                log_response=True,
                extra_options={"verify": False},
            )

            # ----------------------------------------------------------------
            # 1b. Resolve user URIs from Polaris by employeeId
            #
            # rp_source's users_user_id is unreliable (deleted users break
            # the mapping). Source-of-truth is Polaris. We pull every
            # unique employeeId from ADD days on this page and call
            # BulkGetUsers3, then build a {employeeId: full-URN} map for
            # classify_and_build to consume.
            #
            # Caching across pages/runs is intentionally avoided — a stale
            # cache would silently push allocations to deleted users.
            # ----------------------------------------------------------------

            def build_resolve_users_payload():
                page = result("fetch_page")
                bookings = page.get("data", []) if page else []
                eids = set()
                for booking in bookings:
                    for day in booking.get("days", []) or []:
                        eid = day.get("employeeId")
                        if eid:
                            eids.add(str(eid))
                return {
                    "users": [
                        {
                            "uri": None,
                            "loginName": None,
                            "employeeId": eid,
                            "parameterCorrelationId": None,
                        }
                        for eid in sorted(eids)
                    ],
                    "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission",
                }

            def handle_resolve_users_response(response_data):
                """Build {employeeId: user_uri} map from the BulkGetUsers3
                response. Raise if the same employeeId resolves to multiple
                distinct URIs (data quality)."""
                users = response_data
                user_uri_map: dict = {}
                duplicates: list = []
                for u in users:
                    details = (u or {}).get("userDetails") or {}
                    eid = details.get("employeeId")
                    uri = details.get("uri")
                    if not eid or not uri:
                        continue
                    eid = str(eid)
                    existing = user_uri_map.get(eid)
                    if existing and existing != uri:
                        duplicates.append(eid)
                        continue
                    user_uri_map[eid] = uri

                if duplicates:
                    raise Exception(
                        "Multiple distinct user URIs returned for employeeId(s): "
                        f"{', '.join(sorted(set(duplicates)))} — refusing to pick one"
                    )

                print(
                    f"resolved {len(user_uri_map)} users from "
                    f"{len(users)} response entries"
                )
                return user_uri_map

            resolve_user_uris = RepliconServiceOperator(
                task_id="resolve_user_uris",
                endpoint="/services/ImportService1.svc/BulkGetUsers3",
                data=build_resolve_users_payload,
                data_handler=handle_resolve_users_response,
            )

            # ----------------------------------------------------------------
            # 2. Classify rows by outboundPendingOp.
            #    Artifact has tenant_id at top level (one source of truth)
            #    and per-op item lists. Conf builders read tenant_id from
            #    the top level and per-item fields from the item.
            # ----------------------------------------------------------------

            def classify_and_build_callable(**context):
                page = result("fetch_page")
                bookings = page.get("data", []) if page else []
                conf = (context.get("dag_run").conf if context.get("dag_run") else None) or {}
                # Map keyed by role display name → roleUri. Sourced from
                # Polaris ProjectRoleListService1.svc by the master DAG and
                # passed in via conf. Inverse of the one used in
                # task_resource_allocation_export (Polaris→RP direction).
                role_uri_map = conf.get("role_uri_map") or {}

                if not bookings:
                    set_result(key="total_count", val=0)
                    return write_json_artifact({
                        "tenant_id": "",
                        "creates": [], "updates": [],
                        "missing_roles": [], "skipped_bookings": [],
                    })

                tenant_id = Variable.get(config.tenant_id_variable)
                user_uri_map = result("resolve_user_uris") or {}

                creates: list[dict] = []
                updates: list[dict] = []
                # Bookings where labor_code was supplied but did not resolve
                # to a Polaris roleUri. The allocation still pushes (sans
                # roleUri) and the master surfaces these in the alert email.
                missing_roles: list[dict] = []
                # Bookings whose CREATE was skipped — surfaced in log_failure
                # so the master alert email can flag them for investigation.
                skipped_bookings: list[dict] = []
                skipped_no_user = 0
                skipped_no_time_code = 0
                skipped_no_employee_id = 0

                for booking in bookings:
                    source_booking_id = booking.get("sourceBookingId", "")
                    days = booking.get("days", []) or []
                    # laborCode is a per-day field in the API response — read
                    # from the first day that carries it (all days in a booking
                    # share the same laborCode).
                    labor_code = ""
                    for _d in days:
                        if _d.get("laborCode"):
                            labor_code = str(_d["laborCode"])
                            break
                    booking_role_uri = role_uri_map.get(labor_code, "") if labor_code else ""
                    # Deferred — only flag missing-role AFTER we know an
                    # allocation actually pushed (so the "allocation pushed
                    # without roleUri" alert isn't false for bookings that
                    # got skipped for other reasons).
                    role_lookup_missed = bool(labor_code) and not booking_role_uri
                    pushed_any = False

                    # Time code (project~task) is the same for all days in a booking.
                    time_code = ""
                    for d in days:
                        if d.get("timeCode"):
                            time_code = d["timeCode"]
                            break
                    if not time_code:
                        skipped_no_time_code += 1
                        skipped_bookings.append({
                            "source_booking_id": source_booking_id,
                            "reason": "no_time_code",
                        })
                        continue

                    parts = time_code.split("~")
                    project_id = parts[0] if len(parts) >= 1 else ""
                    task_id    = parts[1] if len(parts) >= 2 else ""

                    add_days: list[dict] = []
                    add_pushed_rows: list[dict] = []
                    mod_days: list[dict] = []           # UPDATE + DELETE combined
                    mod_pushed_rows: list[dict] = []
                    add_employee_id = ""                # tracked from first ADD day with non-empty employeeId
                    mod_employee_id = ""                # tracked from first UPDATE/DELETE day with non-empty employeeId

                    for day in days:
                        op = (day.get("outboundPendingOp") or "").upper()
                        work_date = str(day.get("workDate") or "")[:10]
                        hours = float(day.get("hours") or 0)

                        if not work_date or not op:
                            continue

                        if op == "ADD":
                            eid = str(day.get("employeeId") or "")
                            if eid and not add_employee_id:
                                add_employee_id = eid
                            if hours > 0:
                                add_days.append({
                                    "workDate":    work_date,
                                    "hoursPerDay": hours,
                                })
                            # Even zero-hour ADD rows get the row marked
                            # pushed (no work to dispatch but signal is consumed).
                            add_pushed_rows.append({
                                "sourceBookingId": source_booking_id,
                                "workDate":        work_date,
                            })
                        elif op == "UPDATE":
                            eid = str(day.get("employeeId") or "")
                            if eid and not mod_employee_id:
                                mod_employee_id = eid
                            mod_days.append({
                                "workDate":    work_date,
                                "hoursPerDay": hours,
                            })
                            mod_pushed_rows.append({
                                "sourceBookingId": source_booking_id,
                                "workDate":        work_date,
                            })
                        elif op == "DELETE":
                            # DELETE is just setHours=0 inside the same
                            # PARTIAL update mutation — no separate op-DAG.
                            eid = str(day.get("employeeId") or "")
                            if eid and not mod_employee_id:
                                mod_employee_id = eid
                            mod_days.append({
                                "workDate":    work_date,
                                "hoursPerDay": 0,
                            })
                            mod_pushed_rows.append({
                                "sourceBookingId": source_booking_id,
                                "workDate":        work_date,
                            })
                        # Unknown op → ignore (don't mark pushed; surface for review)

                    if add_days:
                        if not add_employee_id:
                            skipped_no_employee_id += 1
                            skipped_bookings.append({
                                "source_booking_id": source_booking_id,
                                "reason": "no_employee_id",
                            })
                            continue
                        user_uri_for_booking = user_uri_map.get(add_employee_id, "")
                        if not user_uri_for_booking:
                            skipped_no_user += 1
                            skipped_bookings.append({
                                "source_booking_id": source_booking_id,
                                "employee_id":       add_employee_id,
                                "reason":            "unresolved_user_uri",
                            })
                            continue
                        schedule_rules = collapse_daily_rows_to_schedule_rules(add_days)
                        creates.append({
                            "source_booking_id": source_booking_id,
                            "project_id":        project_id,
                            "task_id":           task_id,
                            "employee_id":       add_employee_id,
                            "user_uri":          user_uri_for_booking,
                            "role_uri":          booking_role_uri,
                            "labor_code":        labor_code,
                            "schedule_rules":    schedule_rules,
                            "pushed_rows":       add_pushed_rows,
                        })
                        pushed_any = True

                    if mod_days:
                        schedule_rules = collapse_daily_rows_to_schedule_rules(mod_days)
                        mod_user_uri = user_uri_map.get(mod_employee_id, "") if mod_employee_id else ""
                        updates.append({
                            "source_booking_id": source_booking_id,
                            "project_id":        project_id,
                            "task_id":           task_id,
                            "employee_id":       mod_employee_id,
                            "user_uri":          mod_user_uri,
                            "role_uri":          booking_role_uri,
                            "labor_code":        labor_code,
                            "schedule_rules":    schedule_rules,
                            "pushed_rows":       mod_pushed_rows,
                        })
                        pushed_any = True

                    if pushed_any and role_lookup_missed:
                        missing_roles.append({
                            "source_booking_id": source_booking_id,
                            "labor_code":        labor_code,
                        })

                total = len(creates) + len(updates)
                set_result(key="total_count", val=total)
                set_result(key="create_count", val=len(creates))
                set_result(key="update_count", val=len(updates))
                set_result(key="missing_role_count", val=len(missing_roles))
                set_result(key="skipped_count", val=len(skipped_bookings))

                print(
                    f"classified {len(bookings)} bookings -> "
                    f"{len(creates)} creates, {len(updates)} updates "
                    f"(skipped: {skipped_no_user} unresolved user, "
                    f"{skipped_no_employee_id} missing employee_id, "
                    f"{skipped_no_time_code} missing time_code; "
                    f"{len(missing_roles)} bookings with unresolved labor_code)"
                )
                if skipped_bookings:
                    for s in skipped_bookings:
                        print(
                            f"  SKIPPED booking={s['source_booking_id']} "
                            f"reason={s['reason']}"
                            + (f" employee_id={s['employee_id']}" if s.get("employee_id") else "")
                        )

                return write_json_artifact({
                    "tenant_id":        tenant_id,
                    "creates":          creates,
                    "updates":          updates,
                    "missing_roles":    missing_roles,
                    "skipped_bookings": skipped_bookings,
                })

            classify_and_build = PythonOperator(
                task_id="classify_and_build",
                python_callable=classify_and_build_callable,
            )

            has_work = IfOperator(
                task_id="has_work",
                test="{{ result('classify_and_build', 'total_count') > 0 }}",
                yes_task="trigger_creates",
                no_task="join_after_work",
            )

            # ----------------------------------------------------------------
            # 3. Trigger one op-DAG run per Polaris mutation
            # ----------------------------------------------------------------

            def _items_for(op_key: str):
                def _inner():
                    data = load_json_artifact(result("classify_and_build"))
                    return data.get(op_key, [])
                return _inner

            def _conf_builder_for_booking(include_user_uri: bool):
                """Builder for op-DAG runs whose key is the booking and whose
                payload is a scheduleRules array. Both CREATE and UPDATE need
                user_uri: CREATE to set allocationUserUri, UPDATE to cross-reference
                the current Polaris allocation when deciding FULL vs PARTIAL mode."""
                def _build(item):
                    page_conf = result("capture_page_conf") or {}
                    artifact = load_json_artifact(result("classify_and_build"))
                    conf = {
                        "tenant_id":         artifact.get("tenant_id", ""),
                        "source_booking_id": item["source_booking_id"],
                        "project_id":        item["project_id"],
                        "task_id":           item["task_id"],
                        "employee_id":       item.get("employee_id", ""),
                        "role_uri":          item.get("role_uri", ""),
                        "schedule_rules":    item["schedule_rules"],
                        "pushed_rows":       (item.get("pushed_rows")
                                              or _pushed_rows_from_schedule_rules(item)),
                        "sourceSystem":      page_conf.get("sourceSystem", "Polaris"),
                        "lastModifiedAfter": page_conf.get("lastModifiedAfter", ""),
                        "upperBound":        page_conf.get("upperBound", ""),
                        "pageSize":          int(page_conf.get("pageSize", config.page_size)),
                        "pageNumber":        int(page_conf.get("pageNumber", 0)),
                        "masterRunId":       page_conf.get("masterRunId", ""),
                    }
                    if include_user_uri:
                        conf["user_uri"] = item["user_uri"]
                    if page_conf.get("targetTable"):
                        conf["targetTable"] = page_conf["targetTable"]
                    return conf
                return _build

            trigger_creates = TriggerDagRunForEachItemOperator(
                task_id="trigger_creates",
                trigger_dag_id=op_create_dag_id,
                items=_items_for("creates"),
                conf=_conf_builder_for_booking(include_user_uri=True),
            )

            trigger_updates = TriggerDagRunForEachItemOperator(
                task_id="trigger_updates",
                trigger_dag_id=op_update_dag_id,
                items=_items_for("updates"),
                conf=_conf_builder_for_booking(include_user_uri=True),
            )

            # ----------------------------------------------------------------
            # 4. Wait for the spawned op-DAG runs to terminate. Sequential
            #    waits (creates -> updates) mean a CREATE has settled
            #    before any PARTIAL on the same allocation runs.
            # ----------------------------------------------------------------

            wait_for_create_runs = WaitForDagRunsSensor(
                task_id="wait_for_create_runs",
                dag_runs="{{ result('trigger_creates') }}",
                execution_timeout=timedelta(hours=2),
            )

            wait_for_update_runs = WaitForDagRunsSensor(
                task_id="wait_for_update_runs",
                dag_runs="{{ result('trigger_updates') }}",
                execution_timeout=timedelta(hours=2),
            )

            # ----------------------------------------------------------------
            # 5. Gather op-DAG failure XComs.
            #
            # Each spawned op-DAG run pushes a failure record (only when its
            # own ``log_failure`` task fired). These gathers collect those
            # records across all spawned runs into a list.
            # ----------------------------------------------------------------

            gather_create_failures = GatherResultsFromDagRunsOperator(
                task_id="gather_create_failures",
                dag_runs="{{ result('trigger_creates') }}",
                dagrun_task_id="log_failure",
                flatten=False,
            )

            gather_update_failures = GatherResultsFromDagRunsOperator(
                task_id="gather_update_failures",
                dag_runs="{{ result('trigger_updates') }}",
                dagrun_task_id="log_failure",
                flatten=False,
            )

            # ----------------------------------------------------------------
            # 6. log_failure — runs at end of every page-child run (linear,
            #    trigger_rule="all_done"). Returns a structured failure record
            #    (this page's failed tasks + non-empty op-DAG failures gathered
            #    above) if anything failed; otherwise returns None. The master
            #    gathers these and filters None.
            # ----------------------------------------------------------------

            def log_failure_callable(**context):
                conf = (context.get("dag_run").conf if context.get("dag_run") else None) or {}
                dag = context.get("dag")
                dag_run = context.get("dag_run")

                failed_task_ids = []
                if dag_run:
                    for ti in dag_run.get_task_instances():
                        if str(ti.state) == "failed":
                            failed_task_ids.append(ti.task_id)

                # Gathers may have been skipped (e.g. the "No" branch where
                # has_work=False). Defaults to [] in that case; None entries
                # from individual op-DAGs (no failure) are filtered out.
                op_failures = []
                for gather_task in ("gather_create_failures", "gather_update_failures"):
                    try:
                        gathered = result(gather_task) or []
                    except Exception:
                        gathered = []
                    op_failures.extend(r for r in gathered if r)

                # Bookings whose labor_code didn't match a Polaris role. The
                # allocation still pushed (sans roleUri); we surface these so
                # the master alert email flags them for follow-up.
                missing_roles = []
                skipped_bookings = []
                try:
                    artifact = load_json_artifact(result("classify_and_build"))
                    missing_roles    = artifact.get("missing_roles", []) or []
                    skipped_bookings = artifact.get("skipped_bookings", []) or []
                except Exception:
                    missing_roles = []
                    skipped_bookings = []

                if not failed_task_ids and not op_failures and not missing_roles and not skipped_bookings:
                    return None

                error_msg = (
                    f"Failed tasks: {', '.join(failed_task_ids)}"
                    if failed_task_ids else
                    ("op-DAG failure(s) only" if op_failures else
                     ("missing role(s) only" if missing_roles else "skipped booking(s) only"))
                )

                record = {
                    "level":             "page",
                    "child_dag_id":      dag.dag_id if dag else "",
                    "child_run_id":      dag_run.run_id if dag_run else "",
                    "page_number":       int(conf.get("pageNumber") or 0),
                    "master_run_id":     conf.get("masterRunId", ""),
                    "last_modified_after": conf.get("lastModifiedAfter", ""),
                    "upper_bound":       conf.get("upperBound", ""),
                    "failed_task_ids":   failed_task_ids,
                    "error_excerpt":     error_msg[:500],
                    "op_failures":       op_failures,
                    "missing_roles":     missing_roles,
                    "skipped_bookings":  skipped_bookings,
                }
                print(f"log_failure (page): {record}")
                return record

            log_failure = PythonOperator(
                task_id="log_failure",
                python_callable=log_failure_callable,
                trigger_rule="all_done",
            )

            end_task = EmptyOperator(task_id="end_task", trigger_rule="all_done")

            # ----------------------------------------------------------------
            # Dependencies
            # ----------------------------------------------------------------
            join_after_work = EmptyOperator(task_id="join_after_work", trigger_rule="none_failed_min_one_success")

            can_run_batch_task >> Label("Yes") >> batch_task >> end_task
            can_run_batch_task >> Label("No") >> capture_page_conf

            (capture_page_conf
                >> prepare_page_request
                >> fetch_page
                >> resolve_user_uris
                >> classify_and_build
                >> has_work)

            (has_work >> Label("Yes")
                >> trigger_creates
                >> wait_for_create_runs
                >> gather_create_failures
                >> trigger_updates
                >> wait_for_update_runs
                >> gather_update_failures
                >> join_after_work)
            has_work >> Label("No") >> join_after_work
            join_after_work >> log_failure >> end_task


for_each_instance(create_confirmed_bookings_export_child_dag)
