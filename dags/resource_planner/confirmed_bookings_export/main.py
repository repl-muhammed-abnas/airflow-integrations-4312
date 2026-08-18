"""
### Confirmed Bookings Export — Master DAG (RP → Polaris)

#### Purpose
Fans out confirmed booking pages to three partitioned child DAGs based on
``(pageNumber - 1) % 3`` routing. Waits for all triggered child runs to
finish, then advances the cursor (Airflow Variable) to the server-provided
``upperBound``.

#### Input (dag_run.conf)
None — uses the Airflow Variable ``rp_confirmed_bookings_cursor_{instance}``
as the cursor. Set it manually once at deploy time:

```
airflow variables set rp_confirmed_bookings_cursor_dev "2026-04-22T10:00:00.000+05:30"
```

#### Concurrency
Master is ``max_active_runs=1`` so cursor reads/writes don't race.
Each child is ``max_active_runs=3`` (see ``child_dag.py``). Peak pushes to
Polaris in flight = 3 × 3 = 9.
"""
import json

from rail import (
    for_each_instance, create_airflow_dag, result, Label,
    PythonOperator, IfOperator, BatchTaskRunOperator, SimpleHttpOperator, EmptyOperator,
    ViewDagRunConfOperator, TriggerDagRunForEachItemOperator,
    WaitForDagRunsSensor, GatherResultsFromDagRunsOperator,
    EmailOperator, RepliconServicePageOperator, WriteCSVFileOperator, set_result, write_json_artifact
)
from airflow.models import Variable
from datetime import timedelta

API_HEADERS = {"Content-Type": "application/json"}


def _get_cursor_or_fail(config):
    """Read the cursor from Airflow Variable.

    Fails the task with a clear, actionable message if the Variable is not
    set. Deliberately does NOT fall back to a default — silently starting
    from ``now()`` would cause the DAG to skip every unpushed change from
    the previous window, a data-loss failure mode that's hard to detect.
    """
    value = Variable.get(config.cursor_variable_key, default_var=None)
    if not value:
        raise ValueError(
            f"Cursor Airflow Variable '{config.cursor_variable_key}' is not set. "
            f"This is a one-time bootstrap — set it to the earliest timestamp "
            f"you want this DAG's first run to consider. Example:\n"
            f"  airflow variables set {config.cursor_variable_key} "
            f"'2026-04-23T00:00:00.000+05:30'\n"
            f"The DAG will manage the Variable automatically on subsequent runs."
        )
    return value


def create_confirmed_bookings_export_master_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    with create_airflow_dag(
        dag_id=f"resource_planner_confirmed_bookings_export_{config.instance}",
        description="Master DAG: fans out confirmed booking pages to child DAGs",
        start_date=config.start_date,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
        schedule_interval=config.schedule_interval,
    ) as dag:

        ViewDagRunConfOperator(task_id="view_dag_run_conf")

        can_run_batch_task = IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.resource_planner_confirmed_bookings_export_enable_batch_task, "true"
            ).lower() == "true",
            yes_task="batch_task",
            no_task="capture_master_run_id",
        )

        batch_task = BatchTaskRunOperator(
            task_id="batch_task",
            start_task="capture_master_run_id",
            end_task="end_task",
        )

        # ---------------------------------------------------------------------
        # 1. Capture master run_id for correlation into child conf
        # ---------------------------------------------------------------------

        def capture_master_run_id_callable(**context):
            return context["run_id"]

        capture_master_run_id = PythonOperator(
            task_id="capture_master_run_id",
            python_callable=capture_master_run_id_callable,
        )

        # ---------------------------------------------------------------------
        # 2. Fetch batch metadata (API 1)
        # ---------------------------------------------------------------------

        def prepare_metadata_payload():
            return json.dumps({
                "sourceSystem": "Polaris",
                "lastModifiedAfter": _get_cursor_or_fail(config),
                "pageSize": config.page_size,
                **(
                    {"targetTable": config.rp_api_target_table}
                    if getattr(config, "rp_api_target_table", None) else {}
                ),
            })

        prepare_metadata_request = PythonOperator(
            task_id="prepare_metadata_request",
            python_callable=prepare_metadata_payload,
        )

        fetch_batch_metadata = SimpleHttpOperator(
            task_id="fetch_batch_metadata",
            method="POST",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/confirmedBookings/batches",
            headers=_api_headers,
            data="{{ result('prepare_metadata_request') }}",
            response_filter=lambda response: response.json(),
            log_response=True,
            extra_options={"verify": False},
        )

        # ---------------------------------------------------------------------
        # 2b. Fetch Polaris project roles → build {displayName: roleUri} map.
        # This is the inverse of the map task_resource_allocation_export uses;
        # here labor_code (a role display name) on incoming RP bookings must
        # resolve back to a Polaris roleUri for the GraphQL mutations.
        # Only enabled roles are kept.
        # ---------------------------------------------------------------------

        def _role_page_handler(request, response):
            if response.get('rows') and len(response['rows']) >= int(request['pagesize']):
                return {**request, 'page': str(int(request['page']) + 1)}
            return None

        def _role_result_handler(results):
            import itertools
            all_rows = list(itertools.chain(*[r.get('rows', []) for r in results]))
            display_to_uri = {}
            for row in all_rows:
                cells = row.get('cells', [])
                if len(cells) < 2:
                    continue
                if not cells[1].get('boolValue', False):
                    continue
                uri = cells[0].get('uri', '')
                display_name = cells[0].get('textValue', '')
                if uri and display_name:
                    display_to_uri[display_name] = uri
            return display_to_uri

        fetch_project_roles = RepliconServicePageOperator(
            task_id="fetch_project_roles",
            endpoint="/services/ProjectRoleListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:project-role-list-column:project-role",
                    "urn:replicon:project-role-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": None
            },
            page_handler=_role_page_handler,
            all_result_data_handler=_role_result_handler,
        )

        # ---------------------------------------------------------------------
        # 3. Branch on page count
        # ---------------------------------------------------------------------

        def test_has_pages():
            meta = result('fetch_batch_metadata')
            print(f"Batch metadata: {meta}")
            if not meta:
                meta = {}
            page_count = int(meta.get('pageCount') or 0)
            print(f"Page count: {page_count}")
            return page_count > 0

        has_pages = IfOperator(
            task_id="has_pages",
            test=test_has_pages,
            yes_task="compute_page_groups",
            no_task="join_after_pages",
        )

        # ---------------------------------------------------------------------
        # 4. Split [1..pageCount] into three modulo groups
        # ---------------------------------------------------------------------

        def compute_page_groups_callable():
            meta = result("fetch_batch_metadata")
            page_count = int(meta.get("pageCount") or 0)
            # JSON keys are strings for safe round-tripping
            groups = {"1": [], "2": [], "3": []}
            for p in range(1, page_count + 1):
                key = str(((p - 1) % config.child_count) + 1)
                groups[key].append(p)
            return groups

        compute_page_groups = PythonOperator(
            task_id="compute_page_groups",
            python_callable=compute_page_groups_callable,
        )

        # ---------------------------------------------------------------------
        # 5. Three trigger tasks — one per child DAG
        # ---------------------------------------------------------------------

        def _items_for(group_key: str):
            def _inner():
                return result("compute_page_groups").get(group_key, [])
            return _inner

        def _conf_builder():
            """Returns the per-item conf dict passed into the child's dag_run.conf."""
            def _build(item):
                meta = result("fetch_batch_metadata")
                return {
                    "pageNumber":        int(item),
                    "lastModifiedAfter": _get_cursor_or_fail(config),
                    "upperBound":        meta["upperBound"],
                    "pageSize":          meta["pageSize"],
                    "masterRunId":       result("capture_master_run_id"),
                    "sourceSystem":      "Polaris",
                    "role_uri_map":      result("fetch_project_roles") or {},
                    **(
                        {"targetTable": config.rp_api_target_table}
                        if getattr(config, "rp_api_target_table", None) else {}
                    ),
                }
            return _build

        trigger_child_1 = TriggerDagRunForEachItemOperator(
            task_id="trigger_child_1",
            trigger_dag_id=f"resource_planner_confirmed_bookings_export_child_{config.instance}_1",
            items=_items_for("1"),
            conf=_conf_builder(),
        )

        trigger_child_2 = TriggerDagRunForEachItemOperator(
            task_id="trigger_child_2",
            trigger_dag_id=f"resource_planner_confirmed_bookings_export_child_{config.instance}_2",
            items=_items_for("2"),
            conf=_conf_builder(),
        )

        trigger_child_3 = TriggerDagRunForEachItemOperator(
            task_id="trigger_child_3",
            trigger_dag_id=f"resource_planner_confirmed_bookings_export_child_{config.instance}_3",
            items=_items_for("3"),
            conf=_conf_builder(),
        )

        # ---------------------------------------------------------------------
        # 6. Combine trigger outputs, wait for every child run to finish
        # ---------------------------------------------------------------------

        def combine_child_run_ids_callable():
            return [
                *(result("trigger_child_1") or []),
                *(result("trigger_child_2") or []),
                *(result("trigger_child_3") or []),
            ]

        combine_child_run_ids = PythonOperator(
            task_id="combine_child_run_ids",
            python_callable=combine_child_run_ids_callable,
        )

        wait_for_all_children = WaitForDagRunsSensor(
            task_id="wait_for_all_children",
            dag_runs="{{ result('combine_child_run_ids') }}",
            execution_timeout=timedelta(hours=2),
        )

        # ---------------------------------------------------------------------
        # 7. Gather failures from every triggered page-child, format a
        #    report, and email on-call. No DB write — failures live only in
        #    XCom and the emailed log.
        # ---------------------------------------------------------------------

        gather_child_failures = GatherResultsFromDagRunsOperator(
            task_id="gather_child_failures",
            dag_runs="{{ result('combine_child_run_ids') }}",
            dagrun_task_id="log_failure",
            flatten=False,
            trigger_rule="all_done",
        )

        def format_failure_report_callable(**context):
            """Aggregate master + page + op failures into one report.

            - Master failures: read directly from dag_run.get_task_instances()
            - Page + op failures: gathered from children via XCom (None
              entries from no-failure runs are filtered out)
            """
            dag_run = context.get("dag_run")
            master_failed_task_ids = []
            if dag_run:
                for ti in dag_run.get_task_instances():
                    if str(ti.state) == "failed":
                        master_failed_task_ids.append(ti.task_id)

            pages = [
                p for p in (result("gather_child_failures") or [])
                if isinstance(p, dict)
            ]

            op_failures_total = sum(len(p.get("op_failures") or []) for p in pages)
            missing_roles_total = sum(len(p.get("missing_roles") or []) for p in pages)
            page_failures_total = len(pages)
            master_failed_count = len(master_failed_task_ids)
            total = master_failed_count + op_failures_total + page_failures_total + missing_roles_total

            if total == 0:
                return {"failure_count": 0, "has_failures": False}

            # --- CSV rows (one per op-DAG failure) ---
            failure_rows = []
            for p in pages:
                for op in p.get("op_failures") or []:
                    failure_rows.append({
                        "allocation_id": op.get("source_booking_id", ""),
                        "employee_id":   op.get("employee_id", ""),
                        "project":       op.get("project_id", ""),
                        "task":          op.get("task_id", ""),
                        "error_details": ", ".join(op.get("failed_task_ids") or []) or op.get("error_excerpt", ""),
                        "job_id":        op.get("child_run_id", ""),
                    })
                # Page-child task failures (rare — not from op-DAGs)
                for tid in p.get("failed_task_ids") or []:
                    failure_rows.append({
                        "allocation_id": "",
                        "employee_id":   "",
                        "project":       "",
                        "task":          "",
                        "error_details": f"page task failed: {tid}",
                        "job_id":        p.get("child_run_id", ""),
                    })
            for tid in master_failed_task_ids:
                failure_rows.append({
                    "allocation_id": "",
                    "employee_id":   "",
                    "project":       "",
                    "task":          "",
                    "error_details": f"master task failed: {tid}",
                    "job_id":        dag_run.run_id if dag_run else "",
                })
            set_result(key="failure_rows", val=write_json_artifact(failure_rows))

            return {
                "failure_count": total,
                "has_failures": True,
                "op_failures_total": op_failures_total,
                "page_failures_total": page_failures_total,
                "master_failed_count": master_failed_count,
                "master_failed_task_ids": master_failed_task_ids,
                "missing_roles_total": missing_roles_total,
                "pages_summary": [
                    {
                        "page_number": p.get("page_number"),
                        "op_failure_count": len(p.get("op_failures") or []),
                        "missing_roles_count": len(p.get("missing_roles") or []),
                        "failed_task_ids_str": ", ".join(p.get("failed_task_ids") or []) or "(op-only)",
                        "error_excerpt": (p.get("error_excerpt") or "")[:200],
                    }
                    for p in pages
                ],
            }

        format_failure_report = PythonOperator(
            task_id="format_failure_report",
            python_callable=format_failure_report_callable,
            trigger_rule="all_done",
        )

        write_failure_csv = WriteCSVFileOperator(
            task_id="write_failure_csv",
            source="{{ result('format_failure_report', 'failure_rows') }}",
            header=["allocation_id", "employee_id", "project", "task", "error_details", "job_id"],
            row=["{{ item.allocation_id }}", "{{ item.employee_id }}", "{{ item.project }}", "{{ item.task }}", "{{ item.error_details }}", "{{ item.job_id }}"],
        )

        has_failures_branch = IfOperator(
            task_id="has_failures",
            test=lambda: bool((result("format_failure_report") or {}).get("has_failures")),
            yes_task="write_failure_csv",
            no_task="join_before_advance",
        )

        email_failure_report = EmailOperator(
            task_id="email_failure_report",
            to=config.email_failure_recipients,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | ResourcePlanner Confirmed Bookings Export completed with error at '{{ current_time_in_specified_tz() }}'",
            html_content="templates/emails/failure_report.html",
            files=[
                ("failures.csv", "{{ result('write_failure_csv') }}"),
            ],
        )

        # ---------------------------------------------------------------------
        # 8. Advance cursor — writes upperBound to the Airflow Variable,
        #    runs even if some child runs failed (failures are emailed; the
        #    next master cycle picks up retries naturally via the cursor).
        # ---------------------------------------------------------------------

        def advance_cursor_callable():
            meta = result("fetch_batch_metadata")
            if not meta:
                print("advance_cursor: no metadata available, skipping")
                return
            next_cursor = meta.get("nextCursor") or meta.get("upperBound")
            if next_cursor:
                Variable.set(config.cursor_variable_key, next_cursor)
                print(f"advance_cursor: set {config.cursor_variable_key} = {next_cursor}")
            else:
                print("advance_cursor: nextCursor missing from metadata, skipping")

        advance_cursor = PythonOperator(
            task_id="advance_cursor",
            python_callable=advance_cursor_callable,
        )

        end_task = EmptyOperator(task_id="end_task")

        # ---------------------------------------------------------------------
        # Dependencies
        # ---------------------------------------------------------------------
        join_before_advance = EmptyOperator(task_id="join_before_advance", trigger_rule="none_failed_min_one_success")
        join_after_pages = EmptyOperator(task_id="join_after_pages", trigger_rule="none_failed_min_one_success")

        can_run_batch_task >> Label("Yes") >> batch_task >> end_task
        can_run_batch_task >> Label("No") >> capture_master_run_id

        capture_master_run_id >> prepare_metadata_request >> fetch_batch_metadata >> has_pages

        (has_pages >> Label("Yes") >> compute_page_groups
            >> fetch_project_roles
            >> trigger_child_1
            >> trigger_child_2
            >> trigger_child_3
            >> combine_child_run_ids
            >> wait_for_all_children
            >> gather_child_failures
            >> format_failure_report
            >> has_failures_branch)

        has_failures_branch >> Label("Yes") >> write_failure_csv >> email_failure_report >> join_before_advance
        has_failures_branch >> Label("No") >> join_before_advance
        join_before_advance >> join_after_pages
        has_pages >> Label("No") >> join_after_pages
        join_after_pages >> advance_cursor >> end_task

    return dag


for_each_instance(create_confirmed_bookings_export_master_dag)
