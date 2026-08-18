import json
from datetime import timedelta
from functools import lru_cache
from rail import (for_each_instance, create_airflow_dag, result, Label, run_report2, write_json_artifact,
                    load_all_records, load_json_artifact, set_result,
                    LoadCSVFileOperator, PythonOperator, IfOperator, BatchTaskRunOperator,
                    EmptyOperator, RepliconReportDetailsOperator, CreateCollectionOperator,
                    QueryCollectionOperator, EmailOperator, GatherResultsFromDagRunsOperator,
                    TriggerDagRunForEachItemOperator, WaitForDagRunsSensor, WriteCSVFileOperator)
from airflow.models import Variable


def create_project_task_export_delta_dag(config):
    with create_airflow_dag(
        dag_id=f"resource_planner_project_task_export_delta_{config.instance}",
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
            report_name=config.project_uri_for_delta_report_name,
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
            no_task='join_before_format'
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
                "ProjectUri": "project_uri",
                "Project Name": "project_name",
                "Project Manager": "project_manager",
                "Action": "action",
                "Field": "field"
            }
        )

        # Identify projects marked for deletion in the audit report
        def identify_deletes_callable():
            records = load_all_records(result('create_project_uri_collection'))

            delete_count = 0
            for record in records:
                if record.get('action', '').strip().lower() == 'delete':
                    delete_count += 1

            # Deduplicate: count distinct (project_uri, project_name) pairs with delete action
            delete_pairs = set()
            for record in records:
                if record.get('action', '').strip().lower() == 'delete':
                    uri = record.get('project_uri', '').strip() if record.get('project_uri') else ''
                    name = record.get('project_name', '').strip() if record.get('project_name') else ''
                    if uri or name:
                        delete_pairs.add((uri, name))

            set_result(key="delete_count", val=len(delete_pairs))

        identify_deletes = PythonOperator(
            task_id="identify_deletes",
            python_callable=identify_deletes_callable,
        )

        has_deletes_to_process = IfOperator(
            task_id="has_deletes_to_process",
            test="{{ result('identify_deletes', 'delete_count') > 0 }}",
            yes_task="create_delete_indexed_table",
            no_task="join_after_deletes"
        )

        # Index delete projects (both URI-present and name-only)
        create_delete_indexed_table = QueryCollectionOperator(
            task_id="create_delete_indexed_table",
            query="""SELECT
                    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_index,
                    project_uri, project_name
                FROM (
                    SELECT DISTINCT project_uri, project_name
                    FROM project_uri_collection
                    WHERE LOWER(action) = 'delete'
                ) AS delete_projects"""
        )

        def get_delete_batch_indexes():
            from math import ceil
            total = result('create_delete_indexed_table', 'length')
            if total == 0:
                return []
            batches_count = total / config.BATCH_SIZE
            return [i for i in range(ceil(batches_count))]

        trigger_delete_processing = TriggerDagRunForEachItemOperator(
            task_id="trigger_delete_processing",
            trigger_dag_id=f"resource_planner_project_task_export_delta_delete_child_{config.instance}",
            items=get_delete_batch_indexes,
            conf=lambda item: {
                "instance": config.instance,
                "batch_index": item,
                "start_index": (item * config.BATCH_SIZE) + 1,
                "end_index": (item + 1) * config.BATCH_SIZE,
                "batch_size": config.BATCH_SIZE
            }
        )

        wait_for_delete_completion = WaitForDagRunsSensor(
            task_id="wait_for_delete_completion",
            dag_runs="{{ result('trigger_delete_processing') }}",
            execution_timeout=timedelta(days=14)
        )

        # Index only non-delete projects for upsert processing (delete takes priority)
        create_indexed_table = QueryCollectionOperator(
            task_id="create_indexed_table",
            query="""SELECT
                    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_index,
                    project_uri
                FROM (
                    SELECT DISTINCT project_uri
                    FROM project_uri_collection
                    WHERE project_uri IS NOT NULL
                    AND project_uri != ''
                    AND project_uri NOT IN (
                        SELECT project_uri
                        FROM project_uri_collection
                        WHERE LOWER(action) = 'delete'
                        AND project_uri IS NOT NULL
                        AND project_uri != ''
                    )
                ) AS upsert_projects"""
        )

        has_upserts_to_process = IfOperator(
            task_id="has_upserts_to_process",
            test="{{ result('create_indexed_table', 'length') > 0 }}",
            yes_task="get_project_manager_employee_id",
            no_task="join_after_upserts"
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
            if total_projects == 0:
                return []
            batches_count = total_projects / config.BATCH_SIZE
            return [i for i in range(ceil(batches_count))]

        @lru_cache(maxsize=8)
        def get_cached_data_for_user_name_id_map():
            return result('create_artifact_for_batch')

        trigger_batch_processing_for_project_tasks = TriggerDagRunForEachItemOperator(
            task_id = "trigger_batch_processing_for_project_tasks",
            trigger_dag_id= f"resource_planner_project_task_export_delta_child_{config.instance}",
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
        gather_delta_failures = GatherResultsFromDagRunsOperator(
            task_id="gather_delta_failures",
            dag_runs="{{ result('trigger_batch_processing_for_project_tasks') }}",
            dagrun_task_id="log_failure",
            flatten=False
        )

        gather_delete_failures = GatherResultsFromDagRunsOperator(
            task_id="gather_delete_failures",
            dag_runs="{{ result('trigger_delete_processing') }}",
            dagrun_task_id="log_failure",
            flatten=False
        )

        def format_failure_report_callable(**context):
            """Read master task states directly + gather child failure XComs.

            Filters None entries from the gathers (children return None for
            no-failure runs).
            """
            dag_run = context.get("dag_run")
            master_failed_task_ids = []
            if dag_run:
                for ti in dag_run.get_task_instances():
                    if str(ti.state) == "failed":
                        master_failed_task_ids.append(ti.task_id)

            child_failures = [
                c for c in (result("gather_delta_failures") or [])
                if isinstance(c, dict)
            ] + [
                c for c in (result("gather_delete_failures") or [])
                if isinstance(c, dict)
            ]

            master_failed_count = len(master_failed_task_ids)
            total = master_failed_count + len(child_failures)
            if total == 0:
                return {"failure_count": 0, "has_failures": False,
                        "html_summary": ""}

            dag_id = dag_run.dag_id if dag_run else ""
            run_id = dag_run.run_id if dag_run else ""

            set_result(key="failure_rows", val=[
                {
                    "level":         "master",
                    "dag_id":        dag_id,
                    "run_id":        run_id,
                    "project_index": "",
                    "project_uri":   "",
                    "failed_tasks":  tid,
                    "error_excerpt": "",
                }
                for tid in master_failed_task_ids
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
                f"<p>Project Task Export Delta run had failures: "
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
            subject="{{ get_company_key() }} | ResourcePlanner Project Task Export Delta completed with error at '{{ current_time_in_specified_tz() }}'",
            html_content="{{ result('format_failure_report').get('html_summary') }}",
            files=[
                ("failures.csv", "{{ result('write_failure_csv') }}"),
            ],
        )

        join_after_deletes = EmptyOperator(task_id="join_after_deletes", trigger_rule="none_failed_min_one_success")
        join_after_upserts = EmptyOperator(task_id="join_after_upserts", trigger_rule="none_failed_min_one_success")
        join_before_format = EmptyOperator(task_id="join_before_format", trigger_rule="none_failed_min_one_success")
        end_task = EmptyOperator(
            task_id="end_task",
            trigger_rule="all_done",
        )

        # Task dependencies
        can_run_batch_task >> Label("Yes") >> batch_task >> end_task
        can_run_batch_task >> Label("No") >> get_project_uri_report

        get_project_uri_report >> run_project_uri_report >> report_has_data
        report_has_data >> Label("Yes") >> load_report_data >> get_user_report >> run_user_report >> load_user_report_data >> create_user_collection >> create_project_uri_collection >> identify_deletes

        # Delete path: trigger delete child DAG, then proceed to upsert
        identify_deletes >> has_deletes_to_process
        has_deletes_to_process >> Label("Yes") >> create_delete_indexed_table >> trigger_delete_processing >> wait_for_delete_completion >> gather_delete_failures >> join_after_deletes
        has_deletes_to_process >> Label("No") >> join_after_deletes
        join_after_deletes >> create_indexed_table

        # Upsert path
        create_indexed_table >> has_upserts_to_process
        has_upserts_to_process >> Label("Yes") >> get_project_manager_employee_id >> create_artifact_for_batch >> trigger_batch_processing_for_project_tasks >> wait_for_completion_of_batch_tasks >> gather_delta_failures >> join_after_upserts
        has_upserts_to_process >> Label("No") >> join_after_upserts
        join_after_upserts >> join_before_format
        report_has_data >> Label("No") >> join_before_format
        join_before_format >> format_failure_report

        # --- Failure-notification path ---
        format_failure_report >> has_failures_branch
        has_failures_branch >> Label("Yes") >> write_failure_csv >> email_failure_report >> end_task
        has_failures_branch >> Label("No") >> end_task

    return dag


for_each_instance(
    create_project_task_export_delta_dag
)
