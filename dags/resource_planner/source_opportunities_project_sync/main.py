"""
### Source Opportunities Project Sync — Master DAG (RP → Polaris)

#### Purpose
Fans out sourceOpportunities pages to a single page-child DAG. Waits for all
triggered child runs to finish, then advances the cursor (Airflow Variable)
to the server-provided ``upperBound`` — but ONLY when this run had zero
master-task or page/op-DAG failures. There is no per-row mark-synced call
in this integration (unlike confirmed_bookings_export's markPushed), so the
watermark is the only commit point: if anything failed, the next scheduled
run must re-issue ``/batches`` with the *same* ``lastModifiedAfter`` so the
whole window is retried (paired with ``check_project_exists`` in the op-DAG,
which makes that retry safe against re-creating projects that already
succeeded).

#### Input (dag_run.conf)
None — uses the Airflow Variable ``rp_source_opportunities_cursor_{instance}``
as the cursor. Set it manually once at deploy time:

```
airflow variables set rp_source_opportunities_cursor_dev "2026-04-22T10:00:00.000+05:30"
```

#### Concurrency
Master is ``max_active_runs=1`` so cursor reads/writes don't race.
The page-child is ``max_active_runs_child`` (see ``child_dag.py``).
"""
import json

from rail import (
    for_each_instance, create_airflow_dag, result, Label,
    PythonOperator, IfOperator, BatchTaskRunOperator, SimpleHttpOperator, EmptyOperator,
    ViewDagRunConfOperator, TriggerDagRunForEachItemOperator,
    WaitForDagRunsSensor, GatherResultsFromDagRunsOperator,
    EmailOperator, WriteCSVFileOperator, set_result,write_json_artifact
)
from airflow.models import Variable
from datetime import timedelta

def _get_cursor_or_fail(config):
    """Read the cursor from Airflow Variable.

    Fails the task with a clear, actionable message if the Variable is not
    set. Deliberately does NOT fall back to a default — silently starting
    from ``now()`` would cause the DAG to skip every unpushed opportunity
    from the previous window, a data-loss failure mode that's hard to detect.
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


def create_source_opportunities_project_sync_master_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    with create_airflow_dag(
        dag_id=f"resource_planner_source_opportunities_project_sync_{config.instance}",
        description="Master DAG: fans out sourceOpportunities pages to a page-child DAG",
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
                config.resource_planner_source_opportunities_project_sync_enable_batch_task, "true"
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
        # 2. Fetch batch metadata (API 1). No ``sourceSystem`` field — the
        #    real SourceOpportunitiesBatchesRequest model only accepts
        #    lastModifiedAfter, pageSize, type, targetTable.
        # ---------------------------------------------------------------------

        def prepare_metadata_payload():
            return json.dumps({
                "lastModifiedAfter": _get_cursor_or_fail(config),
                "pageSize": config.page_size,
                "minProbability": config.MIN_PROBABILITY,
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
            endpoint="/api/v1/rp/sourceOpportunities/batches",
            headers=_api_headers,
            data="{{ result('prepare_metadata_request') }}",
            response_filter=lambda response: response.json(),
            log_response=True,
            extra_options={"verify": False},
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
            yes_task="trigger_page_children",
            no_task="join_after_pages",
        )

        # ---------------------------------------------------------------------
        # 4. Trigger one page-child run per page
        # ---------------------------------------------------------------------

        def get_page_items():
            meta = result("fetch_batch_metadata")
            page_count = int(meta.get("pageCount") or 0)
            return list(range(1, page_count + 1))

        def build_page_conf(item):
            meta = result("fetch_batch_metadata")
            return {
                "pageNumber":        int(item),
                "lastModifiedAfter": _get_cursor_or_fail(config),
                "upperBound":        meta["upperBound"],
                "pageSize":          meta["pageSize"],
                "masterRunId":       result("capture_master_run_id"),
                **(
                    {"targetTable": config.rp_api_target_table}
                    if getattr(config, "rp_api_target_table", None) else {}
                ),
            }

        trigger_page_children = TriggerDagRunForEachItemOperator(
            task_id="trigger_page_children",
            trigger_dag_id=f"resource_planner_source_opportunities_project_sync_child_{config.instance}",
            items=get_page_items,
            conf=build_page_conf,
        )

        # ---------------------------------------------------------------------
        # 5. Wait for every page-child run to finish
        # ---------------------------------------------------------------------

        wait_for_page_children = WaitForDagRunsSensor(
            task_id="wait_for_page_children",
            dag_runs="{{ result('trigger_page_children') }}",
            execution_timeout=timedelta(hours=2),
        )

        # ---------------------------------------------------------------------
        # 6. Gather failures from every triggered page-child, format a
        #    report, and email on-call. No DB write — failures live only in
        #    XCom and the emailed log.
        # ---------------------------------------------------------------------

        gather_page_failures = GatherResultsFromDagRunsOperator(
            task_id="gather_page_failures",
            dag_runs="{{ result('trigger_page_children') }}",
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
                p for p in (result("gather_page_failures") or [])
                if isinstance(p, dict)
            ]

            op_failures_total = sum(len(p.get("op_failures") or []) for p in pages)
            page_failures_total = len(pages)
            master_failed_count = len(master_failed_task_ids)
            total = master_failed_count + op_failures_total + page_failures_total

            if total == 0:
                return {"failure_count": 0, "has_failures": False}

            # --- CSV rows (one per op-DAG failure) ---
            failure_rows = []
            for p in pages:
                for op in p.get("op_failures") or []:
                    failure_rows.append({
                        "opportunity_id":   op.get("opportunity_id", ""),
                        "opportunity_name": op.get("opportunity_name", ""),
                        "error_details":    ", ".join(op.get("failed_task_ids") or []) or op.get("error_excerpt", ""),
                        "job_id":           op.get("child_run_id", ""),
                    })
                # Page-child task failures (rare — not from op-DAGs)
                for tid in p.get("failed_task_ids") or []:
                    failure_rows.append({
                        "opportunity_id":   "",
                        "opportunity_name": "",
                        "error_details":    f"page task failed: {tid}",
                        "job_id":           p.get("child_run_id", ""),
                    })
            for tid in master_failed_task_ids:
                failure_rows.append({
                    "opportunity_id":   "",
                    "opportunity_name": "",
                    "error_details":    f"master task failed: {tid}",
                    "job_id":           dag_run.run_id if dag_run else "",
                })
            set_result(key="failure_rows", val=write_json_artifact(failure_rows))

            return {
                "failure_count":        total,
                "has_failures":         True,
                "op_failures_total":    op_failures_total,
                "page_failures_total":  page_failures_total,
                "master_failed_count":  master_failed_count,
                "master_failed_task_ids": master_failed_task_ids,
                "pages_summary": [
                    {
                        "page_number":       p.get("page_number"),
                        "op_failure_count":  len(p.get("op_failures") or []),
                        "failed_task_ids_str": ", ".join(p.get("failed_task_ids") or []) or "(op-only)",
                        "error_excerpt":     (p.get("error_excerpt") or "")[:200],
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
            header=["opportunity_id", "opportunity_name", "error_details", "job_id"],
            row=["{{ item.opportunity_id }}", "{{ item.opportunity_name }}", "{{ item.error_details }}", "{{ item.job_id }}"],
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
            subject="{{ get_company_key() }} | ResourcePlanner Source Opportunities Project Sync completed with error at '{{ current_time_in_specified_tz() }}'",
            html_content="templates/emails/failure_report.html",
            files=[
                ("failures.csv", "{{ result('write_failure_csv') }}"),
            ],
        )

        # ---------------------------------------------------------------------
        # 7. Advance cursor — writes upperBound to the Airflow Variable, but
        #    ONLY when this run had zero master/page/op failures (see module
        #    docstring — there is no per-row mark-synced call in this
        #    integration, so the watermark is the only commit point).
        # ---------------------------------------------------------------------

        def advance_cursor_callable(**context):
            meta = result("fetch_batch_metadata")
            if not meta:
                print("advance_cursor: no metadata available, skipping")
                return

            dag_run = context.get("dag_run")
            master_failed = False
            if dag_run:
                for ti in dag_run.get_task_instances():
                    if str(ti.state) == "failed":
                        master_failed = True
                        break

            report = result("format_failure_report") or {}
            if master_failed or report.get("has_failures"):
                print(
                    "advance_cursor: failures occurred this run — NOT advancing "
                    "the cursor. The next run will re-issue /batches with the "
                    "same lastModifiedAfter and retry this whole window."
                )
                return

            next_cursor = meta.get("nextCursor") or meta.get("upperBound")
            if next_cursor:
                Variable.set(config.cursor_variable_key, next_cursor)
                print(f"advance_cursor: set {config.cursor_variable_key} = {next_cursor}")
            else:
                print("advance_cursor: nextCursor missing from metadata, skipping")

        advance_cursor = PythonOperator(
            task_id="advance_cursor",
            python_callable=advance_cursor_callable
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

        (has_pages >> Label("Yes")
            >> trigger_page_children
            >> wait_for_page_children
            >> gather_page_failures
            >> format_failure_report
            >> has_failures_branch)

        has_failures_branch >> Label("Yes") >> write_failure_csv >> email_failure_report >> join_before_advance
        has_failures_branch >> Label("No") >> join_before_advance
        join_before_advance >> join_after_pages
        has_pages >> Label("No") >> join_after_pages
        join_after_pages >> advance_cursor >> end_task

    return dag


for_each_instance(create_source_opportunities_project_sync_master_dag)
