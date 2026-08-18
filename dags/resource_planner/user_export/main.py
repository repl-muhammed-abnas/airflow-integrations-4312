import json
from rail import (for_each_instance, create_airflow_dag, run_report2, result, load_all_records, Label,
                    PythonOperator, CreateCollectionOperator, IfOperator, LoadCSVFileOperator,
                    RepliconReportDetailsOperator, BatchTaskRunOperator, SimpleHttpOperator, EmptyOperator,
                    EmailOperator, WriteCSVFileOperator, set_result)
from airflow.models import Variable

API_HEADERS = {"Content-Type": "application/json"}


def create_user_export_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    with create_airflow_dag(
        dag_id=f"resource_planner_user_export_{config.instance}",
        description="Exports Resource Planner Users to a CSV file",
        schedule_interval=config.schedule_interval,
        start_date=config.start_date,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        can_run_batch_task = IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(config.resource_planner_user_export_enable_batch_task, "false").lower() == "true",
            yes_task="batch_task",
            no_task="get_eligible_employee_ids"
        )

        batch_task = BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_eligible_employee_ids",
            end_task="end_task"
        )

        get_report_details = RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name="Resource Planner Users Report"
        )

        run_user_report = run_report2(
            group_id="export_users_report",
            report_params={
                "reportParameters": [{
                    "reportUri": "{{ result('get_report_details').uri }}",
                    "filterValues": [],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }]
            }
        )

        load_user_report = LoadCSVFileOperator(
            task_id="load_user_report",
            document="{{ result('export_users_report.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_user_collection = CreateCollectionOperator(
            task_id="create_user_collection",
            name="polaris_user",
            source="{{ result('load_user_report') }}",
        )

        # Get eligible employees via RP Backend API.
        # If an instance points at a dummy source_resources table, pass it along
        # so the NOT EXISTS check queries that dummy instead of prod.
        def _eligible_endpoint():
            base = "/api/v1/rp/eligiblePolarisEmployees"
            override = getattr(config, 'rp_api_target_table', None)
            if override:
                return f"{base}?sourceResourcesTable={override}"
            return base

        get_eligible_employee_ids = SimpleHttpOperator(
            task_id="get_eligible_employee_ids",
            method="GET",
            http_conn_id=config.rp_api_conn_id,
            endpoint=_eligible_endpoint(),
            headers=_api_headers,
            response_filter=lambda response: response.json(),
            log_response=True,
            extra_options={"verify": False},
        )

        # Fetch resource mappings via RP Backend API
        fetch_resource_map = SimpleHttpOperator(
            task_id="fetch_resource_map",
            method="POST",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/resources",
            headers=_api_headers,
            data=json.dumps({"employeeIds": []}),
            response_filter=lambda response: response.json(),
            log_response=True,
            extra_options={"verify": False},
        )

        def identify_users_to_add_function():
            polaris_users = load_all_records(result(create_user_collection.task_id))

            eligible_data = result(get_eligible_employee_ids.task_id)
            eligible_ids = {item['employeeId'] for item in eligible_data.get('data', [])}

            resources_data = result(fetch_resource_map.task_id)
            employee_to_resource = {item['employeeId']: item for item in resources_data.get('data', [])}

            users_to_add = []
            for user in polaris_users:
                employee_id = user['Employee_ID']
                if employee_id in eligible_ids:
                    resource_info = employee_to_resource.get(str(employee_id), {})
                    users_to_add.append({
                        'employeeId': employee_id,
                        'sourceSystem': 'Polaris',
                        'usersUserId': resource_info.get('usersUserId', ''),
                        'resourceId': resource_info.get('resourceId', ''),
                    })
            return users_to_add

        identify_users_to_add = PythonOperator(
            task_id="identify_users_to_add",
            python_callable=identify_users_to_add_function,
        )

        has_any_users_to_add = IfOperator(
            task_id="has_any_users_to_add",
            test="{{ result('identify_users_to_add') | length > 0 }}",
            yes_task="prepare_insert_users_request",
            no_task="join_before_format"
        )

        def prepare_insert_users_payload():
            return json.dumps({
                "targetTable": getattr(config, 'rp_api_target_table', None),
                "records": result('identify_users_to_add'),
            })

        prepare_insert_users_request = PythonOperator(
            task_id="prepare_insert_users_request",
            python_callable=prepare_insert_users_payload,
        )

        insert_users_to_rp_source_resources = SimpleHttpOperator(
            task_id="insert_users_to_rp_source_resources",
            method="POST",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/sourceResources",
            headers=_api_headers,
            data="{{ result('prepare_insert_users_request') }}",
            response_check=lambda response: response.status_code == 200,
            log_response=True,
            extra_options={"verify": False},
        )

        # --- Failure-notification email (no DB write) ---
        # format_failure_report runs with trigger_rule="all_done" so it always
        # fires, then inspects the current DagRun's task states directly to
        # find any failed tasks. No separate log_failure task / fan-in needed.

        def format_failure_report_callable(**context):
            dag = context.get("dag")
            dag_run = context.get("dag_run")

            failed_task_ids = []
            if dag_run:
                for ti in dag_run.get_task_instances():
                    if str(ti.state) == "failed":
                        failed_task_ids.append(ti.task_id)

            failed_count = len(failed_task_ids)
            if failed_count == 0:
                return {"failure_count": 0, "has_failures": False,
                        "html_summary": ""}

            dag_id = dag.dag_id if dag else ""
            run_id = dag_run.run_id if dag_run else ""

            set_result(key="failure_rows", val=[
                {"dag_id": dag_id, "run_id": run_id, "task_id": tid}
                for tid in failed_task_ids
            ])

            failed_html = "".join(
                f"<li><code>{tid}</code></li>" for tid in failed_task_ids
            )
            html = (
                f"<p>User Export had failures. "
                f"<strong>{failed_count}</strong> task(s) failed in "
                f"<code>{dag_id}</code> run <code>{run_id}</code>.</p>"
                "<p>Failed tasks:</p>"
                f"<ul>{failed_html}</ul>"
                "<p>Full per-task detail attached as <code>failures.csv</code>.</p>"
            )
            return {
                "failure_count": failed_count,
                "has_failures": True,
                "html_summary": html,
            }

        format_failure_report = PythonOperator(
            task_id="format_failure_report",
            python_callable=format_failure_report_callable,
            trigger_rule="all_done",
        )

        write_failure_csv = WriteCSVFileOperator(
            task_id="write_failure_csv",
            source="{{ result('format_failure_report', 'failure_rows') }}",
            header=["dag_id", "run_id", "task_id"],
            row=["{{ item.dag_id }}", "{{ item.run_id }}", "{{ item.task_id }}"],
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
            subject="{{ get_company_key() }} | ResourcePlanner User Export completed with error at '{{ current_time_in_specified_tz() }}'",
            html_content="{{ result('format_failure_report').get('html_summary') }}",
            files=[
                ("failures.csv", "{{ result('write_failure_csv') }}"),
            ],
        )

        join_before_format = EmptyOperator(task_id="join_before_format", trigger_rule="none_failed_min_one_success")
        end_task = EmptyOperator(task_id="end_task", trigger_rule="all_done")

        # Task dependencies
        can_run_batch_task >> Label("Yes") >> batch_task >> end_task
        can_run_batch_task >> Label("No") >> get_eligible_employee_ids

        get_eligible_employee_ids >> get_report_details >> run_user_report >> load_user_report >> create_user_collection
        create_user_collection >> fetch_resource_map >> identify_users_to_add
        identify_users_to_add >> has_any_users_to_add
        has_any_users_to_add >> Label("Yes") >> prepare_insert_users_request >> insert_users_to_rp_source_resources >> join_before_format
        has_any_users_to_add >> Label("No") >> join_before_format
        join_before_format >> format_failure_report

        # --- Failure-notification path (linear, no fan-in) ---
        format_failure_report >> has_failures_branch
        has_failures_branch >> Label("Yes") >> write_failure_csv >> email_failure_report >> end_task
        has_failures_branch >> Label("No") >> end_task

    return dag


for_each_instance(create_user_export_dag)
