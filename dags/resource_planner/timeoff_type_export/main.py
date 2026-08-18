import json
from rail import (for_each_instance, create_airflow_dag, RepliconServicePageOperator, result, Label,
                    PythonOperator, IfOperator, BatchTaskRunOperator, SimpleHttpOperator, EmptyOperator,
                    EmailOperator, WriteCSVFileOperator, set_result)
from airflow.models import Variable

API_HEADERS = {"Content-Type": "application/json"}


def create_timeoff_type_export_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    with create_airflow_dag(
        dag_id=f"resource_planner_timeoff_type_export_{config.instance}",
        description="Exports Timeoff Types from Polaris to Resource Planner database",
        schedule_interval=config.schedule_interval,
        start_date=config.start_date,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        can_run_batch_task = IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(config.resource_planner_timeoff_type_export_enable_batch_task, "false").lower() == "true",
            yes_task="batch_task",
            no_task="get_all_timeoff_types"
        )

        batch_task = BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_all_timeoff_types",
            end_task="end_task"
        )

        null = None

        def page_handler(request, response):
            if response.get('rows', []) and len(response['rows']) >= int(request['pagesize']):
                return {**request, 'page': str(int(request['page']) + 1)}
            return None

        def all_result_data_handler(results):
            import itertools
            all_rows = list(itertools.chain(*[r.get('rows', []) for r in results]))
            return list(map(lambda row: {
                "timeoff_type_id": row['cells'][0]['uri'].split(":")[-1],
                "timeoff_type_name": row['cells'][0]['textValue'],
                "enabled": row['cells'][1]['textValue'],
                "uri": row['cells'][0]['uri']
            }, all_rows))

        get_all_timeoff_types = RepliconServicePageOperator(
            task_id="get_all_timeoff_types",
            endpoint="/services/TimeOffTypeListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:time-off-type-list-column:name",
                    "urn:replicon:time-off-type-list-column:enabled"
                ],
                "sort": [
                    {
                        "columnUri": "urn:replicon:time-off-type-list-column:name",
                        "isAscending": "true"
                    }
                ],
                "filterExpression": null
            },
            page_handler=page_handler,
            all_result_data_handler=all_result_data_handler,
        )

        # Get existing timeoff types via RP Backend API
        get_existing_timeoff_types = SimpleHttpOperator(
            task_id="get_existing_timeoff_types",
            method="GET",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/sourceTimeCodesTimeOffTypes?sourceSystem=Polaris",
            headers=_api_headers,
            response_filter=lambda response: response.json(),
            log_response=True,
            extra_options={"verify": False},
        )

        def identify_timeoff_types_to_add_function():
            polaris_timeoff_types = result(get_all_timeoff_types.task_id)

            existing_data = result(get_existing_timeoff_types.task_id)
            existing_ids = {item['timeCode'] for item in existing_data.get('data', [])}

            return [tt for tt in polaris_timeoff_types if tt['timeoff_type_id'] not in existing_ids]

        identify_timeoff_types_to_add = PythonOperator(
            task_id="identify_timeoff_types_to_add",
            python_callable=identify_timeoff_types_to_add_function,
        )

        has_any_timeoff_types_to_add = IfOperator(
            task_id="has_any_timeoff_types_to_add",
            test="{{ result('identify_timeoff_types_to_add') | length > 0 }}",
            yes_task="prepare_insert_timeoff_types_payload",
            no_task="join_before_format"
        )

        def prepare_insert_timeoff_types_payload_callable():
            from rail import write_json_artifact

            timeoff_types_to_add = result(identify_timeoff_types_to_add.task_id)
            target_table = getattr(config, 'rp_api_target_table', None)

            api_records = [
                {
                    'sourceSystem': 'Polaris',
                    'parentTimeCode': tt['timeoff_type_id'],
                    'timeCode': tt['timeoff_type_id'],
                    'timeCodeName': tt['timeoff_type_name'],
                    'parentTimeCodeName': tt['timeoff_type_name'],
                    'taskLevel': 0,
                    'timeEntryEnabled': tt['enabled'].lower() == 'true'
                }
                for tt in timeoff_types_to_add
            ]

            payload = {"records": api_records}
            if target_table:
                payload["targetTable"] = target_table

            print(f"Prepared {len(api_records)} timeoff types for insert")
            return json.dumps(payload)

        prepare_insert_timeoff_types_payload = PythonOperator(
            task_id="prepare_insert_timeoff_types_payload",
            python_callable=prepare_insert_timeoff_types_payload_callable,
        )

        insert_timeoff_types = SimpleHttpOperator(
            task_id="insert_timeoff_types",
            method="POST",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/sourceTimeCodesTimeOffTypes",
            headers=_api_headers,
            data="{{ result('prepare_insert_timeoff_types_payload') }}",
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
                f"<p>TimeOff Type Export had failures. "
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
            subject="{{ get_company_key() }} | ResourcePlanner TimeOff Type Export completed with error at '{{ current_time_in_specified_tz() }}'",
            html_content="{{ result('format_failure_report').get('html_summary') }}",
            files=[
                ("failures.csv", "{{ result('write_failure_csv') }}"),
            ],
        )

        join_before_format = EmptyOperator(task_id="join_before_format", trigger_rule="none_failed_min_one_success")
        end_task = EmptyOperator(task_id="end_task", trigger_rule="all_done")

        # Task dependencies
        can_run_batch_task >> Label("Yes") >> batch_task >> end_task
        can_run_batch_task >> Label("No") >> get_all_timeoff_types

        get_all_timeoff_types >> get_existing_timeoff_types >> identify_timeoff_types_to_add
        identify_timeoff_types_to_add >> has_any_timeoff_types_to_add
        has_any_timeoff_types_to_add >> Label("Yes") >> prepare_insert_timeoff_types_payload >> insert_timeoff_types >> join_before_format
        has_any_timeoff_types_to_add >> Label("No") >> join_before_format
        join_before_format >> format_failure_report

        # --- Failure-notification path (linear, no fan-in) ---
        format_failure_report >> has_failures_branch
        has_failures_branch >> Label("Yes") >> write_failure_csv >> email_failure_report >> end_task
        has_failures_branch >> Label("No") >> end_task

    return dag


for_each_instance(create_timeoff_type_export_dag)
