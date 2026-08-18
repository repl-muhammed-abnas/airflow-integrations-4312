"""
### Source Opportunities Project Sync — Page Child DAG

Triggered by the master DAG with a specific ``pageNumber`` in ``dag_run.conf``.
Fetches that page from the gateway, filters by ``probability``, then
**triggers one project op-DAG run per qualifying opportunity** and
waits for all of them to complete.

#### Why per-opportunity op-DAGs?

Template selection depends on each individual opportunity's
``engagementContractType``, and each project creation is its own
multi-step Polaris workflow (client resolve → duplicate-from-template →
modify → attach client → workflow transition). Running these from inside
one page task would mean a single opportunity's failure blocks every
sibling on the page and forces a full-page replay. Making each one its own
DAG run (see ``create_project.py``, ``update_project.py``,
``close_project.py``) isolates failures and lets a single opportunity
be replayed from the Airflow UI without touching the rest of the page.

#### Failure

A dedicated ``log_failure`` task wired with ``trigger_rule="all_done"``
gathers the op-DAG failure XComs (via ``gather_op_failures``) and returns
one structured page-level record if anything failed; otherwise it returns
``None``. The master DAG collects all page-children's records, formats a
report, and emails on-call — and does NOT advance the watermark this run.

#### Input (dag_run.conf — set by master)

```json
{
  "pageNumber":        5,
  "lastModifiedAfter": "2026-04-22T10:00:00.000+05:30",
  "upperBound":        "2026-04-22T14:30:15.123+05:30",
  "pageSize":          100,
  "masterRunId":       "manual__2026-04-23T09:00:00+05:30",
  "targetTable":       "dummy_..." (optional)
}
```
"""
import json
from datetime import timedelta

from rail import (
    for_each_instance, create_airflow_dag, result, Label,
    write_json_artifact, load_json_artifact,
    PythonOperator, IfOperator, BatchTaskRunOperator, SimpleHttpOperator, EmptyOperator,
    ViewDagRunConfOperator, TriggerDagRunForEachItemOperator,
    WaitForDagRunsSensor, GatherResultsFromDagRunsOperator,
)
from airflow.models import Variable

def create_source_opportunities_project_sync_child_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    create_project_dag_id = f"resource_planner_source_opportunities_project_sync_create_project_{config.instance}"
    update_project_dag_id = f"resource_planner_source_opportunities_project_sync_update_project_{config.instance}"
    close_project_dag_id = f"resource_planner_source_opportunities_project_sync_close_project_{config.instance}"

    with create_airflow_dag(
        dag_id=f"resource_planner_source_opportunities_project_sync_child_{config.instance}",
        description="Page child: fetches one page of sourceOpportunities and triggers per-opportunity op-DAG runs",
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
                config.resource_planner_source_opportunities_project_sync_enable_batch_task, "true"
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

        def prepare_page_payload():
            conf = result("capture_page_conf") or {}
            payload = {
                "lastModifiedAfter": conf.get("lastModifiedAfter"),
                "upperBound":        conf.get("upperBound"),
                "pageSize":          int(conf.get("pageSize", config.page_size)),
                "pageNumber":        int(conf.get("pageNumber")),
                "minProbability":    config.MIN_PROBABILITY,
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
            endpoint="/api/v1/rp/sourceOpportunities",
            headers=_api_headers,
            data="{{ result('prepare_page_request') }}",
            response_filter=lambda response: response.json(),
            log_response=True,
            extra_options={"verify": False},
        )

        # ----------------------------------------------------------------
        # 2. Classify opportunities into three buckets using both stage AND
        #    probability — SourceOpportunitiesPageResponse wraps rows as
        #    {pageNumber, opportunityCount, data: [...]} — must read ['data'].
        #
        #    Buckets:
        #      creates          – stage="Closing"    AND probability >= MIN_PROBABILITY (70)
        #      update_executions – stage="Closed Won" AND probability == 100
        #      skipped          – everything else
        # ----------------------------------------------------------------

        def classify_opportunities_callable():
            page = result("fetch_page")
            opportunities = (page or {}).get("data", []) or []

            creates, update_executions, close_outs, skipped = [], [], [], []
            for opp in opportunities:
                # Keep prob_raw separate so we can distinguish explicit 0 from null.
                # A null probability must NOT match the close-out condition (prob == 0).
                prob_raw = opp.get("probability")
                probability = float(prob_raw) if prob_raw is not None else None
                stage = (opp.get("stageName") or "").strip()

                if (probability is not None
                        and probability >= config.CLOSING_MIN_PROBABILITY
                        and stage == config.CLOSING_STAGE):
                    creates.append(opp)
                elif (probability == config.CLOSED_WON_PROBABILITY
                        and stage == config.CLOSED_WON_STAGE):
                    update_executions.append(opp)
                elif (prob_raw is not None
                        and probability == config.CLOSE_OUT_PROBABILITY
                        and stage in config.CLOSE_OUT_STAGES):
                    close_outs.append(opp)
                else:
                    skipped.append({
                        "opportunity_id":   opp.get("opportunityId", ""),
                        "opportunity_name": opp.get("opportunityName", ""),
                        "probability":      probability,
                        "stage":            stage,
                    })

            print(
                f"classify_opportunities: {len(opportunities)} fetched — "
                f"{len(creates)} creates (stage={config.CLOSING_STAGE!r}, "
                f"probability>={config.CLOSING_MIN_PROBABILITY}), "
                f"{len(update_executions)} update_executions "
                f"(stage={config.CLOSED_WON_STAGE!r}, "
                f"probability={config.CLOSED_WON_PROBABILITY}), "
                f"{len(close_outs)} close_outs "
                f"(stages={sorted(config.CLOSE_OUT_STAGES)!r}, "
                f"probability={config.CLOSE_OUT_PROBABILITY}), "
                f"{len(skipped)} skipped"
            )
            return write_json_artifact({
                "creates":           creates,
                "update_executions": update_executions,
                "close_outs":        close_outs,
                "skipped":           skipped,
            })

        classify_opportunities = PythonOperator(
            task_id="classify_opportunities",
            python_callable=classify_opportunities_callable,
        )

        # ----------------------------------------------------------------
        # 3a. Creates branch — one create_project run per qualifying opportunity.
        # ----------------------------------------------------------------

        has_creates = IfOperator(
            task_id="has_creates",
            test=lambda: len(load_json_artifact(result("classify_opportunities")).get("creates", [])) > 0,
            yes_task="trigger_create_project",
            no_task="join_creates",
        )

        def get_create_project_items():
            return load_json_artifact(result("classify_opportunities")).get("creates", [])

        def build_create_project_conf(item):
            page_conf = result("capture_page_conf") or {}
            conf = dict(item)
            conf["masterRunId"] = page_conf.get("masterRunId", "")
            conf["pageNumber"] = int(page_conf.get("pageNumber", 0))
            if page_conf.get("targetTable"):
                conf["targetTable"] = page_conf["targetTable"]
            return conf

        trigger_create_project = TriggerDagRunForEachItemOperator(
            task_id="trigger_create_project",
            trigger_dag_id=create_project_dag_id,
            items=get_create_project_items,
            conf=build_create_project_conf,
        )

        wait_for_create_project = WaitForDagRunsSensor(
            task_id="wait_for_create_project",
            dag_runs="{{ result('trigger_create_project') }}",
            execution_timeout=timedelta(hours=2),
        )

        gather_create_failures = GatherResultsFromDagRunsOperator(
            task_id="gather_create_failures",
            dag_runs="{{ result('trigger_create_project') }}",
            dagrun_task_id="log_failure",
            flatten=False,
            trigger_rule="all_done",
        )

        join_creates = EmptyOperator(
            task_id="join_creates",
            trigger_rule="all_done",
        )

        # ----------------------------------------------------------------
        # 3b. Update-project branch — one update_project run per
        #     Closed Won opportunity.
        # ----------------------------------------------------------------

        has_update_executions = IfOperator(
            task_id="has_update_executions",
            test=lambda: len(load_json_artifact(result("classify_opportunities")).get("update_executions", [])) > 0,
            yes_task="trigger_update_project",
            no_task="join_updates",
        )

        def get_update_project_items():
            return load_json_artifact(result("classify_opportunities")).get("update_executions", [])

        def build_update_project_conf(item):
            page_conf = result("capture_page_conf") or {}
            conf = dict(item)
            conf["masterRunId"] = page_conf.get("masterRunId", "")
            conf["pageNumber"] = int(page_conf.get("pageNumber", 0))
            if page_conf.get("targetTable"):
                conf["targetTable"] = page_conf["targetTable"]
            return conf

        trigger_update_project = TriggerDagRunForEachItemOperator(
            task_id="trigger_update_project",
            trigger_dag_id=update_project_dag_id,
            items=get_update_project_items,
            conf=build_update_project_conf,
        )

        wait_for_update_project = WaitForDagRunsSensor(
            task_id="wait_for_update_project",
            dag_runs="{{ result('trigger_update_project') }}",
            execution_timeout=timedelta(hours=2),
        )

        gather_update_failures = GatherResultsFromDagRunsOperator(
            task_id="gather_update_failures",
            dag_runs="{{ result('trigger_update_project') }}",
            dagrun_task_id="log_failure",
            flatten=False,
            trigger_rule="all_done",
        )

        join_updates = EmptyOperator(
            task_id="join_updates",
            trigger_rule="all_done",
        )

        # ----------------------------------------------------------------
        # 3c. Close-project branch — one close_project run per opportunity
        #     whose stageName is in CLOSE_OUT_STAGES (Closed Lost /
        #     Closed/No Decision / Sales Rejected) with probability == 0.
        #     Project-not-found is NOT a failure for these (logged + skipped).
        # ----------------------------------------------------------------

        has_close_outs = IfOperator(
            task_id="has_close_outs",
            test=lambda: len(load_json_artifact(result("classify_opportunities")).get("close_outs", [])) > 0,
            yes_task="trigger_close_project",
            no_task="join_close_outs",
        )

        def get_close_project_items():
            return load_json_artifact(result("classify_opportunities")).get("close_outs", [])

        def build_close_project_conf(item):
            page_conf = result("capture_page_conf") or {}
            conf = dict(item)
            conf["masterRunId"] = page_conf.get("masterRunId", "")
            conf["pageNumber"] = int(page_conf.get("pageNumber", 0))
            if page_conf.get("targetTable"):
                conf["targetTable"] = page_conf["targetTable"]
            return conf

        trigger_close_project = TriggerDagRunForEachItemOperator(
            task_id="trigger_close_project",
            trigger_dag_id=close_project_dag_id,
            items=get_close_project_items,
            conf=build_close_project_conf,
        )

        wait_for_close_project = WaitForDagRunsSensor(
            task_id="wait_for_close_project",
            dag_runs="{{ result('trigger_close_project') }}",
            execution_timeout=timedelta(hours=2),
        )

        gather_close_out_failures = GatherResultsFromDagRunsOperator(
            task_id="gather_close_out_failures",
            dag_runs="{{ result('trigger_close_project') }}",
            dagrun_task_id="log_failure",
            flatten=False,
            trigger_rule="all_done",
        )

        join_close_outs = EmptyOperator(
            task_id="join_close_outs",
            trigger_rule="all_done",
        )

        # ----------------------------------------------------------------
        # 4. log_failure — runs at end of every page-child run (linear,
        #    trigger_rule="all_done"). Collects failures from all three
        #    branches. Returns a structured failure record if anything
        #    failed; otherwise None. The master gathers these and filters None.
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

            # Gather failures from all three op-DAG branches.  Any may have been
            # skipped (No branch of has_creates / has_update_executions / has_close_outs).
            op_failures = []
            for gather_task in ("gather_create_failures", "gather_update_failures", "gather_close_out_failures"):
                try:
                    gathered = result(gather_task) or []
                    op_failures.extend(r for r in gathered if r)
                except Exception as e:
                    print(f"log_failure: could not read {gather_task} results: {e!r}")

            skipped_opportunities = []
            try:
                artifact = load_json_artifact(result("classify_opportunities"))
                skipped_opportunities = artifact.get("skipped", []) or []
            except Exception as e:
                print(f"log_failure: could not read classify_opportunities artifact: {e!r}")

            if not failed_task_ids and not op_failures:
                return None

            error_msg = (
                f"Failed tasks: {', '.join(failed_task_ids)}"
                if failed_task_ids else "op-DAG failure(s) only"
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
                "skipped_opportunities": skipped_opportunities,
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
        can_run_batch_task >> Label("Yes") >> batch_task >> end_task
        can_run_batch_task >> Label("No") >> capture_page_conf

        (capture_page_conf
            >> prepare_page_request
            >> fetch_page
            >> classify_opportunities)

        # Branches are SEQUENTIAL (required by BatchTaskRunOperator — no parallel fan-out).
        # Each branch completes via its join before the next branch starts.

        # Create-project branch
        classify_opportunities >> has_creates
        has_creates >> Label("Yes") >> trigger_create_project >> wait_for_create_project >> gather_create_failures >> join_creates
        has_creates >> Label("No") >> join_creates

        # Update-project branch — starts after create-project branch joins
        join_creates >> has_update_executions
        has_update_executions >> Label("Yes") >> trigger_update_project >> wait_for_update_project >> gather_update_failures >> join_updates
        has_update_executions >> Label("No") >> join_updates

        # Close-project branch — starts after update-project branch joins
        join_updates >> has_close_outs
        has_close_outs >> Label("Yes") >> trigger_close_project >> wait_for_close_project >> gather_close_out_failures >> join_close_outs
        has_close_outs >> Label("No") >> join_close_outs

        # log_failure always runs (trigger_rule="all_done") — no join_all_work needed
        join_close_outs >> log_failure >> end_task

    return dag


for_each_instance(create_source_opportunities_project_sync_child_dag)
