import json
from datetime import timedelta
from functools import lru_cache
from rail import (for_each_instance, create_airflow_dag, result, Label, run_report2, write_json_artifact,
                    load_all_records, set_result,
                    LoadCSVFileOperator, PythonOperator, IfOperator, BatchTaskRunOperator,
                    EmptyOperator, RepliconReportDetailsOperator, CreateCollectionOperator,
                    QueryCollectionOperator, EmailOperator, GatherResultsFromDagRunsOperator,
                    TriggerDagRunForEachItemOperator, WaitForDagRunsSensor, WriteCSVFileOperator)
from airflow.models import Variable


def create_project_task_export_bulk_dag(config):
    with create_airflow_dag(
        dag_id=f"resource_planner_project_task_export_bulk_{config.instance}",
        description="Exports Project Tasks from Polaris to Resource Planner database",
        schedule_interval=config.schedule_interval,
        start_date=config.start_date,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        can_run_batch_task = IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(config.resource_planner_project_task_export_enable_batch_task, "true").lower() == "true",
            yes_task="batch_task",
            no_task="get_project_uri_report"
        )

        batch_task = BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_project_uri_report",
            end_task="end_task"
        )

        get_project_uri_report = RepliconReportDetailsOperator(
            task_id="get_project_uri_report",
            report_name=config.project_uri_report_name,
        )

        get_user_report = RepliconReportDetailsOperator(
            task_id="get_user_report",
            report_name=config.user_report_name,
        )

        run_user_report = run_report2(
            group_id="run_user_report",
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_user_report').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        load_user_report_data = LoadCSVFileOperator(
            task_id='load_user_report_data',
            document="{{ result('run_user_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_user_collection = CreateCollectionOperator(
            task_id="create_user_collection",
            name="user_collection",
            source="{{ result('load_user_report_data') }}",
            columns={
                "User Name": "user_name",
                "Employee ID": "employee_id",
                "User Status": "status"
            }
        )

        run_project_uri_report = run_report2(
            group_id="run_project_uri_report",
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_project_uri_report').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        report_has_data = IfOperator(
            task_id="report_has_data",
            test="{{ result('run_project_uri_report.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='log_failure'
        )

        load_report_data = LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_project_uri_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_project_uri_collection = CreateCollectionOperator(
            task_id="create_project_uri_collection",
            name="project_uri_collection",
            source="{{ result('load_report_data') }}",
            columns={
                "project_uri": "project_uri",
                "Project Manager": "project_manager"
            }
        )

        create_indexed_table = QueryCollectionOperator(
            task_id="create_indexed_table",
            query="""SELECT
                    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_index,
                    project_uri
                FROM project_uri_collection"""
        )

        get_project_manager_employee_id = QueryCollectionOperator(
            task_id="get_project_manager_employee_id",
            query="""SELECT user_name, employee_id FROM user_collection WHERE
                    user_name IN (SELECT DISTINCT project_manager FROM project_uri_collection)"""
        )

        def create_artifact_for_batch_callable():
            user_manager_data = load_all_records(result('get_project_manager_employee_id'))
            manager_employee_id_map = {record['user_name']: record['employee_id'] for record in user_manager_data}
            return write_json_artifact(manager_employee_id_map)

        create_artifact_for_batch = PythonOperator(
            task_id = "create_artifact_for_batch",
            python_callable=create_artifact_for_batch_callable
        )

        def get_batch_indexes():
            from math import ceil
            total_projects = result('create_indexed_table', 'length')
            batches_count = total_projects / config.BATCH_SIZE
            return [i for i in range(ceil(batches_count))]

        @lru_cache(maxsize=8)
        def get_cached_data_for_user_name_id_map():
            return result('create_artifact_for_batch')

        trigger_batch_processing_for_project_tasks = TriggerDagRunForEachItemOperator(
            task_id = "trigger_batch_processing_for_project_tasks",
            trigger_dag_id= f"resource_planner_project_task_export_bulk_child_{config.instance}",
            items = get_batch_indexes,
            conf = lambda item: {
                "instance": config.instance,
                "batch_index": item, 
                "start_index": (item * config.BATCH_SIZE) + 1,
                "end_index": (item + 1) * config.BATCH_SIZE,
                "batch_size": config.BATCH_SIZE,
                "user_name_id_map": get_cached_data_for_user_name_id_map()
            }
        )

        wait_for_completion_of_batch_tasks = WaitForDagRunsSensor(
            task_id="wait_for_completion_of_batch_tasks",
            dag_runs="{{ result('trigger_batch_processing_for_project_tasks') }}",
            execution_timeout = timedelta(days=14)
        )

        # --- Failure-notification email (no DB write) ---
        gather_child_failures = GatherResultsFromDagRunsOperator(
            task_id="gather_child_failures",
            dag_runs="{{ result('trigger_batch_processing_for_project_tasks') }}",
            dagrun_task_id="log_failure",
            flatten=False,
            trigger_rule="all_done",
        )

        def log_failure_callable(**context):
            dag = context.get("dag")
            dag_run = context.get("dag_run")
            conf = (dag_run.conf if dag_run else None) or {}
            failed_task_ids = []
            if dag_run:
                for ti in dag_run.get_task_instances():
                    if str(ti.state) == "failed":
                        failed_task_ids.append(ti.task_id)
            error_msg = (
                f"Failed tasks: {', '.join(failed_task_ids)}"
                if failed_task_ids else "task failed"
            )
            return {
                "level":           "master",
                "dag_id":          dag.dag_id if dag else "",
                "run_id":          dag_run.run_id if dag_run else "",
                "conf":            conf,
                "failed_task_ids": failed_task_ids,
                "error_excerpt":   error_msg[:500],
            }

        log_failure = PythonOperator(
            task_id="log_failure",
            python_callable=log_failure_callable,
            trigger_rule="all_done",
        )

        def format_failure_report_callable():
            master_record = result("log_failure") if isinstance(result("log_failure"), dict) else None
            child_failures = result("gather_child_failures") or []
            master_failed_count = len(master_record.get("failed_task_ids") or []) if master_record else 0
            total = master_failed_count + len(child_failures)
            if total == 0:
                return {"failure_count": 0, "has_failures": False,
                        "html_summary": ""}

            set_result(key="failure_rows", val=[
                {
                    "level":         "master",
                    "dag_id":        master_record.get("child_dag_id", ""),
                    "run_id":        master_record.get("child_run_id", ""),
                    "project_index": "",
                    "project_uri":   "",
                    "failed_tasks":  tid,
                    "error_excerpt": "",
                }
                for tid in (master_record.get("failed_task_ids") or [])
            ] + [
                {
                    "level":         "child",
                    "dag_id":        c.get("child_dag_id", ""),
                    "run_id":        c.get("child_run_id", ""),
                    "project_index": str(c.get("project_index", "")),
                    "project_uri":   c.get("project_uri", ""),
                    "failed_tasks":  ", ".join(c.get("failed_task_ids") or []),
                    "error_excerpt": (c.get("error_excerpt") or "")[:200],
                }
                for c in child_failures
            ])

            child_rows_html = "".join(
                f"<tr><td>{c.get('child_dag_id')}</td>"
                f"<td>{c.get('batch_index')}</td>"
                f"<td>{', '.join(c.get('failed_task_ids') or [])}</td>"
                f"<td><code>{(c.get('error_excerpt') or '')[:200]}</code></td></tr>"
                for c in child_failures
            )
            html = (
                f"<p>Project Task Export Bulk run had failures: "
                f"<strong>{master_failed_count}</strong> master task(s), "
                f"<strong>{len(child_failures)}</strong> child batch(es). "
                "Full per-row detail attached as <code>failures.csv</code>.</p>"
                "<table border='1' cellpadding='6' cellspacing='0'>"
                "<tr><th>Child DAG</th><th>Batch</th><th>Failed tasks</th><th>Error excerpt</th></tr>"
                f"{child_rows_html}"
                "</table>"
            )
            return {"failure_count": total, "has_failures": True,
                    "html_summary": html}

        format_failure_report = PythonOperator(
            task_id="format_failure_report",
            python_callable=format_failure_report_callable,
            trigger_rule="all_done",
        )

        write_failure_csv = WriteCSVFileOperator(
            task_id="write_failure_csv",
            source="{{ result('format_failure_report', 'failure_rows') }}",
            header=["level", "dag_id", "run_id", "project_index", "project_uri", "failed_tasks", "error_excerpt"],
            row=["{{ item.level }}", "{{ item.dag_id }}", "{{ item.run_id }}", "{{ item.project_index }}", "{{ item.project_uri }}", "{{ item.failed_tasks }}", "{{ item.error_excerpt }}"],
        )

        has_failures_branch = IfOperator(
            task_id="has_failures",
            test=lambda: bool((result("format_failure_report") or {}).get("has_failures")),
            yes_task="write_failure_csv",
            no_task="end_task",
        )

        email_failure_report = EmailOperator(
            task_id="email_failure_report",
            to=config.email_failure_recipients,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | ResourcePlanner Project Task Export Bulk completed with error at '{{ current_time_in_specified_tz() }}'",
            html_content="{{ result('format_failure_report').get('html_summary') }}",
            files=[
                ("failures.csv", "{{ result('write_failure_csv') }}"),
            ],
        )

        end_task = EmptyOperator(
            task_id="end_task",
            trigger_rule="all_done",
        )

        # Task dependencies
        can_run_batch_task >> Label("Yes") >> batch_task >> end_task
        can_run_batch_task >> Label("No") >> get_project_uri_report

        get_project_uri_report >> run_project_uri_report >> report_has_data
        report_has_data >> Label("Yes") >> load_report_data >> get_user_report >> run_user_report >>load_user_report_data >> create_user_collection >> create_project_uri_collection >> create_indexed_table >> get_project_manager_employee_id >> create_artifact_for_batch
        create_artifact_for_batch >> trigger_batch_processing_for_project_tasks >> wait_for_completion_of_batch_tasks >> gather_child_failures >> log_failure
        report_has_data >> Label("No") >> log_failure

        log_failure >> format_failure_report >> has_failures_branch
        has_failures_branch >> Label("Yes") >> write_failure_csv >> email_failure_report >> end_task
        has_failures_branch >> Label("No") >> end_task

    return dag


for_each_instance(
    create_project_task_export_bulk_dag
)
