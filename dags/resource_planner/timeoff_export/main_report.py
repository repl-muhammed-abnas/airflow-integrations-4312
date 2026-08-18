import json
from datetime import datetime, timedelta, timezone
from rail import (
    for_each_instance, create_airflow_dag, run_report2, result, load_all_records, Label,
    PythonOperator, CreateCollectionOperator, IfOperator, LoadCSVFileOperator,
    RepliconReportDetailsOperator, SimpleHttpOperator,
    BatchTaskRunOperator, EmptyOperator, FailOperator,
    WriteCSVFileOperator, GeneratePresignedDownloadUrlOperator, EmailOperator,
    set_result, write_json_artifact, load_json_artifact, find_first_by_attr_and_get_attr,
)
from airflow.models import Variable

API_HEADERS = {"Content-Type": "application/json"}


def _resolve_target_date():
    """Resolve the target date used to filter the timeoff booking reports.

    Priority:
      1. ``dag_run.conf['modified_date']`` (ISO YYYY-MM-DD) — QA can
         manually trigger the DAG with a specific date to test
         bookings modified/deleted on that day, without waiting for
         a real run window. Useful for backfills and reproducible
         tests.
      2. Yesterday during 00:00-01:59 UTC — midnight-boundary fix.
         The 23:00 hourly run completes before any 23:00-24:00
         modifications, and the 00:00 run with "today" filter
         wouldn't see them either. Falling back to yesterday's
         date during the first two UTC hours closes that gap.
      3. Today otherwise.

    Returned as ``mm/dd/YYYY`` to match the Polaris report filter format.
    """
    from airflow.operators.python import get_current_context
    try:
        _ctx = get_current_context()
        _conf = (_ctx.get("dag_run").conf if _ctx.get("dag_run") else None) or {}
    except Exception:
        _conf = {}

    override_date = _conf.get("modified_date")
    if override_date:
        try:
            _dt = datetime.strptime(str(override_date), '%Y-%m-%d')
        except ValueError as e:
            raise ValueError(
                f"Invalid 'modified_date' in dag_run.conf: {override_date!r}. "
                f"Expected ISO date format YYYY-MM-DD (e.g. '2026-05-13')."
            ) from e
        target_date = _dt.strftime('%m/%d/%Y')
        print(f"_resolve_target_date: using override modified_date={override_date!r} -> {target_date}")
        return target_date

    now_utc = datetime.now(timezone.utc)
    if now_utc.hour < 2:
        return (now_utc - timedelta(days=1)).strftime('%m/%d/%Y')
    return now_utc.strftime('%m/%d/%Y')


def create_timeoff_export_report_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    with create_airflow_dag(
        dag_id=f"resource_planner_timeoff_export_report_{config.instance}",
        description="Exports Timeoff booking data from Polaris report to Resource Planner database",
        schedule_interval=config.schedule_interval,
        start_date=config.start_date,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        # --- Batch task toggle ---
        can_run_batch_task = IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.resource_planner_timeoff_export_enable_batch_task, "true"
            ).lower() == "false",
            yes_task="batch_task",
            no_task="get_report_details"
        )

        batch_task = BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_report_details",
            end_task="end_task"
        )

        # --- Phase 1: Run report and load data ---
        get_report_details = RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        def get_report_params():
            modified_on_filter_uri = find_first_by_attr_and_get_attr(
                result('get_report_details')['filterConfiguration']['enabledFilters'],
                'displayText',
                'ModifiedOnUtcDateRangeFilter',
                'uri'
            )

            if not modified_on_filter_uri:
                raise ValueError("Could not find ModifiedOnUtcDateRangeFilter in report details")

            target_date = _resolve_target_date()

            filter_values = [
                {
                    "reportFilterUri":  modified_on_filter_uri,
                    "value": None
                },
                {
                    "reportFilterUri":  modified_on_filter_uri,
                    "value": target_date
                },
                {
                    "reportFilterUri":  modified_on_filter_uri,
                    "value": target_date
                }
            ]

            return {
                "reportParameters": [
                    {
                        "reportUri": result('get_report_details')['uri'],
                        "filterValues": filter_values,
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }

        run_timeoff_report = run_report2(
            group_id='run_timeoff_report',
            report_params=get_report_params
        )

        is_report_failed = IfOperator(
            task_id="is_report_failed",
            test='{{ result("run_timeoff_report.get_report_result").reportGenerationResults[0].error | is_truthy }}',
            yes_task="fail_report_generation",
            no_task="load_report_data"
        )

        fail_report_generation = FailOperator(
            task_id='fail_report_generation',
            message='Report generation failed: {{ result("run_timeoff_report.get_report_result").reportGenerationResults[0].error }}',
        )

        load_report_data = LoadCSVFileOperator(
            task_id='load_report_data',
            document='{{ result("run_timeoff_report.get_report_result").reportGenerationResults[0].payload }}',
        )

        create_report_collection = CreateCollectionOperator(
            task_id='create_report_collection',
            source="{{ result('load_report_data') }}",
            columns={
                "User Name":         "user_name",
                "Employee ID":       "employee_id",
                "Time Off Type":     "timeoff_type_name",
                "Time Off Date":     "timeoff_date",
                "Time Off Hrs":      "timeoff_hrs",
                "Time Off Comments": "timeoff_comments",
                "TimeOffBookingUri": "timeoff_booking_uri",
                "TimeOffTypeUri":    "timeoff_type_uri",
                "Modified On":       "modified_on",
            },
            name='report_collection'
        )

        # --- Phase 1b: Run deleted bookings report (parallel with booking report) ---
        get_deleted_report_details = RepliconReportDetailsOperator(
            task_id='get_deleted_report_details',
            report_name=config.deleted_report_name,
        )

        def get_deleted_report_params():
            modified_by_filter_uri = find_first_by_attr_and_get_attr(
                result('get_deleted_report_details')['filterConfiguration']['enabledFilters'],
                'displayText',
                'ModifiedOnUtcDateRangeFilter',
                'uri'
            )

            if not modified_by_filter_uri:
                raise ValueError("Could not find ModifiedOnUtcDateRangeFilter in deleted report details")

            target_date = _resolve_target_date()

            filter_values = [
                {
                    "reportFilterUri": modified_by_filter_uri,
                    "value": None
                },
                {
                    "reportFilterUri": modified_by_filter_uri,
                    "value": target_date
                },
                {
                    "reportFilterUri": modified_by_filter_uri,
                    "value": target_date
                }
            ]

            return {
                "reportParameters": [
                    {
                        "reportUri": result('get_deleted_report_details')['uri'],
                        "filterValues": filter_values,
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }

        run_deleted_report = run_report2(
            group_id='run_deleted_report',
            report_params=get_deleted_report_params
        )

        is_deleted_report_failed = IfOperator(
            task_id="is_deleted_report_failed",
            test='{{ result("run_deleted_report.get_report_result").reportGenerationResults[0].error | is_truthy }}',
            yes_task="fail_deleted_report_generation",
            no_task="load_deleted_report_data"
        )

        fail_deleted_report_generation = FailOperator(
            task_id='fail_deleted_report_generation',
            message='Deleted report generation failed: {{ result("run_deleted_report.get_report_result").reportGenerationResults[0].error }}',
        )

        load_deleted_report_data = LoadCSVFileOperator(
            task_id='load_deleted_report_data',
            document='{{ result("run_deleted_report.get_report_result").reportGenerationResults[0].payload }}',
        )

        create_deleted_collection = CreateCollectionOperator(
            task_id='create_deleted_collection',
            source="{{ result('load_deleted_report_data') }}",
            columns={
                "Employee ID": "employee_id",
                "Time Off ID": "time_off_id",
            },
            name='deleted_collection'
        )

        # --- Phase 2: Fetch lookup data via RP Backend API ---
        fetch_user_id_map = SimpleHttpOperator(
            task_id="fetch_user_id_map",
            method="POST",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/users",
            headers=_api_headers,
            data=json.dumps({"employeeIds": []}),
            response_filter=lambda response: response.json(),
            log_response=True,
            extra_options={"verify": False},
        )

        # --- Phase 3: Process records ---
        def identify_records_to_process_callable():
            import pendulum

            records = load_all_records(result('create_report_collection'))

            # Build employee_id → user_id lookup from users API
            users_data = result('fetch_user_id_map')
            employee_to_user_id = {}
            for item in users_data.get('data', []):
                employee_to_user_id[str(item['employeeId'])] = str(item['userId'])

            records_to_insert = []
            skipped_records = []

            for record in records:
                employee_id = str(record.get('employee_id', '')).strip()
                timeoff_type_name = str(record.get('timeoff_type_name', ''))
                timeoff_date = str(record.get('timeoff_date', '')).strip()
                hours_str = str(record.get('timeoff_hrs', '0')).strip()
                user_name = str(record.get('user_name', ''))
                booking_uri = str(record.get('timeoff_booking_uri', ''))
                timeoff_type_uri = str(record.get('timeoff_type_uri', ''))
                modified_on_str = str(record.get('modified_on', '')).strip()
                # Read for log visibility only — rp_source has no column for it.
                timeoff_comments = str(record.get('timeoff_comments', '') or '')

                source_booking_id = booking_uri.split(':')[-1] if booking_uri else ''

                try:
                    hours = float(hours_str) if hours_str else 0.0
                except ValueError:
                    hours = 0.0

                # Convert Modified On from MST (America/Denver) to UTC
                last_modified_utc = ''
                if modified_on_str:
                    try:
                        mst_tz = pendulum.timezone('America/Denver')
                        parsed = pendulum.parse(modified_on_str, tz=mst_tz, strict=False)
                        last_modified_utc = parsed.in_tz('UTC').to_iso8601_string()
                    except Exception:
                        last_modified_utc = modified_on_str

                # Skip records without employee_id
                if not employee_id:
                    skipped_records.append({
                        'source_booking_id': source_booking_id,
                        'user_name': user_name,
                        'timeoff_type_name': timeoff_type_name,
                        'timeoff_date': timeoff_date,
                        'hours': hours_str,
                        'timeoff_comments': timeoff_comments,
                        'reason': 'Missing employee_id'
                    })
                    continue

                # Derive time_code from TimeOffTypeUri last segment
                time_code = timeoff_type_uri.split(':')[-1] if timeoff_type_uri else ''

                # Determine hours_type
                hours_type = 'Holiday' if 'holiday' in timeoff_type_name.lower() else 'Absence'

                if hours > 0:
                    records_to_insert.append({
                        'source_booking_id': source_booking_id,
                        'source_system': 'Polaris',
                        'time_code': time_code,
                        'users_user_id': employee_to_user_id.get(employee_id) or None,
                        'hours': hours,
                        'work_date': timeoff_date,
                        'hours_type': hours_type,
                        'last_updated_date': last_modified_utc,
                        'employee_id': employee_id
                    })
                else:
                    skipped_records.append({
                        'source_booking_id': source_booking_id,
                        'user_name': user_name,
                        'timeoff_type_name': timeoff_type_name,
                        'timeoff_date': timeoff_date,
                        'hours': hours_str,
                        'timeoff_comments': timeoff_comments,
                        'reason': 'Hours is zero or negative'
                    })

            set_result(key="insert_count", val=len(records_to_insert))
            set_result(key="skipped_count", val=len(skipped_records))

            return write_json_artifact({
                'records_to_insert': records_to_insert,
                'skipped_records': skipped_records
            })

        identify_records_to_process = PythonOperator(
            task_id="identify_records_to_process",
            python_callable=identify_records_to_process_callable,
        )

        has_records_to_insert = IfOperator(
            task_id="has_records_to_insert",
            test="{{ result('identify_records_to_process', 'insert_count') > 0 }}",
            yes_task="prepare_insert_payload",
            no_task="has_skipped_records"
        )

        def prepare_insert_payload_callable():
            data = load_json_artifact(result('identify_records_to_process'))
            records = data.get('records_to_insert', [])
            target_table = getattr(config, 'rp_api_target_table', None)

            # Group records by source_booking_id so each booking becomes one
            # MERGE replacement block. This prevents duplicate rows when the
            # DAG re-runs on the same day and finds the same modified bookings.
            by_booking = {}
            for r in records:
                sbi = str(r['source_booking_id'])
                if sbi not in by_booking:
                    by_booking[sbi] = []
                by_booking[sbi].append({
                    'sourceBookingId': sbi,
                    'sourceSystem': str(r['source_system']),
                    'timeCode': str(r['time_code']),
                    'laborCode': '',
                    'usersUserId': int(r['users_user_id']) if r.get('users_user_id') else None,
                    'hours': float(r['hours']),
                    'workDate': str(r['work_date']),
                    'hoursType': str(r['hours_type']),
                    'lastUpdatedDate': str(r['last_updated_date']),
                    'employeeId': str(r['employee_id'])
                })

            replacements = [
                {
                    'sourceBookingIdPrefix': sbi,
                    'sourceSystem': 'Polaris',
                    'records': day_rows,
                }
                for sbi, day_rows in by_booking.items()
            ]

            payload = {"replacements": replacements}
            if target_table:
                payload["targetTable"] = target_table

            print(f"Prepared {len(replacements)} replacements ({len(records)} records) for upsert")
            return json.dumps(payload)

        prepare_insert_payload = PythonOperator(
            task_id="prepare_insert_payload",
            python_callable=prepare_insert_payload_callable,
        )

        insert_records = SimpleHttpOperator(
            task_id="insert_records",
            method="PUT",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/sourceAllocations",
            headers=_api_headers,
            data="{{ result('prepare_insert_payload') }}",
            response_check=lambda response: response.status_code == 200,
            log_response=True,
            extra_options={"verify": False},
        )

        # --- Deleted bookings processing ---
        def identify_deleted_bookings_callable():
            deleted_records = load_all_records(result('create_deleted_collection'))

            time_off_ids = []
            for record in deleted_records:
                time_off_id = str(record.get('time_off_id', '')).strip()
                if time_off_id:
                    time_off_ids.append(time_off_id)

            set_result(key="delete_count", val=len(time_off_ids))
            return time_off_ids

        identify_deleted_bookings = PythonOperator(
            task_id="identify_deleted_bookings",
            python_callable=identify_deleted_bookings_callable,
        )

        has_records_to_delete = IfOperator(
            task_id="has_records_to_delete",
            test="{{ result('identify_deleted_bookings', 'delete_count') > 0 }}",
            yes_task="prepare_delete_payload",
            no_task="join_before_format"
        )

        def prepare_delete_payload_callable():
            time_off_ids = result('identify_deleted_bookings')
            target_table = getattr(config, 'rp_api_target_table', None)

            deletions = [
                {
                    "sourceBookingIdPrefix": str(time_off_id),
                    "sourceSystem": "Polaris",
                    "hoursTypeFilter": ["Absence", "Holiday"]
                }
                for time_off_id in time_off_ids
            ]

            payload = {"deletions": deletions}
            if target_table:
                payload["targetTable"] = target_table

            print(f"Prepared delete payload for {len(time_off_ids)} bookings")
            return json.dumps(payload)

        prepare_delete_payload = PythonOperator(
            task_id="prepare_delete_payload",
            python_callable=prepare_delete_payload_callable,
        )

        delete_records = SimpleHttpOperator(
            task_id="delete_records",
            method="DELETE",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/sourceAllocations",
            headers=_api_headers,
            data="{{ result('prepare_delete_payload') }}",
            response_check=lambda response: response.status_code == 200,
            log_response=True,
            extra_options={"verify": False},
        )

        # --- Skipped records log and notification ---
        has_skipped_records = IfOperator(
            task_id="has_skipped_records",
            test="{{ result('identify_records_to_process', 'skipped_count') > 0 }}",
            yes_task="prepare_skipped_log",
            no_task="join_after_skipped"
        )

        def prepare_skipped_log_callable():
            data = load_json_artifact(result('identify_records_to_process'))
            skipped = data.get('skipped_records', [])
            return [
                {
                    'source_booking_id': r['source_booking_id'],
                    'user_name': r['user_name'],
                    'timeoff_type_name': r['timeoff_type_name'],
                    'timeoff_date': r['timeoff_date'],
                    'hours': r['hours'],
                    'timeoff_comments': r.get('timeoff_comments', ''),
                    'reason': r['reason']
                }
                for r in skipped
            ]

        prepare_skipped_log = PythonOperator(
            task_id="prepare_skipped_log",
            python_callable=prepare_skipped_log_callable,
        )

        render_skipped_csv = WriteCSVFileOperator(
            task_id='render_skipped_csv',
            source=lambda: result('prepare_skipped_log'),
            header=[
                'Source Booking ID',
                'User Name',
                'Timeoff Type Name',
                'Timeoff Date',
                'Hours',
                'Time Off Comments',
                'Reason',
            ],
            row=[
                '{{ item.source_booking_id }}',
                '{{ item.user_name }}',
                '{{ item.timeoff_type_name }}',
                '{{ item.timeoff_date }}',
                '{{ item.hours }}',
                '{{ item.timeoff_comments }}',
                '{{ item.reason }}',
            ],
        )

        generate_skipped_download_link = GeneratePresignedDownloadUrlOperator(
            task_id='generate_skipped_download_link',
            artifact_name="{{ result('render_skipped_csv') }}",
            output_file_name='timeoff_export_skipped_records.csv',
            expires_in_seconds=7 * 24 * 60 * 60,
        )

        send_skipped_records_email = EmailOperator(
            task_id='send_skipped_records_email',
            to=['sammedkawade@deltek.com', 'DPS-Ops-RP-Support@deltek.com'],
            subject='{{ get_company_key() }} | Timeoff Export Report - Skipped Records (Missing Employee ID)',
            html_content="""
                <p>The timeoff export report DAG has skipped <b>{{ result('identify_records_to_process', 'skipped_count') }}</b> record(s) due to missing employee ID.</p>
                <p><a href="{{ result('generate_skipped_download_link') }}">Download Skipped Records CSV</a></p>
            """,
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
                f"<p>TimeOff Bookings Export had failures. "
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
            subject="{{ get_company_key() }} | ResourcePlanner TimeOff Bookings Export completed with error at '{{ current_time_in_specified_tz() }}'",
            html_content="{{ result('format_failure_report').get('html_summary') }}",
            files=[
                ("failures.csv", "{{ result('write_failure_csv') }}"),
            ],
        )

        end_task = EmptyOperator(
            task_id="end_task",
            trigger_rule="all_done",
        )

        # --- Task dependencies ---

        # Batch task toggle
        can_run_batch_task >> Label("Yes") >> batch_task >> end_task
        can_run_batch_task >> Label("No") >> get_report_details

        # Phase 1: Run booking report and load
        get_report_details >> run_timeoff_report >> is_report_failed
        is_report_failed >> Label("Yes") >> fail_report_generation
        is_report_failed >> Label("No") >> load_report_data >> create_report_collection

        # Phase 2: Fetch lookup data (parallel with report load) and process records
        create_report_collection >> fetch_user_id_map >> identify_records_to_process

        # Insert path → then skipped path (sequential)
        identify_records_to_process >> has_records_to_insert
        has_records_to_insert >> Label("Yes") >> prepare_insert_payload >> insert_records >> has_skipped_records
        has_records_to_insert >> Label("No") >> has_skipped_records

        # Skipped records notification path → then deleted report
        join_after_skipped = EmptyOperator(task_id="join_after_skipped", trigger_rule="none_failed_min_one_success")
        has_skipped_records >> Label("Yes") >> prepare_skipped_log >> render_skipped_csv >> generate_skipped_download_link >> send_skipped_records_email >> join_after_skipped
        has_skipped_records >> Label("No") >> join_after_skipped
        join_after_skipped >> get_deleted_report_details

        # Phase 3: Run deleted bookings report (sequential after insert/skip)
        get_deleted_report_details >> run_deleted_report >> is_deleted_report_failed
        is_deleted_report_failed >> Label("Yes") >> fail_deleted_report_generation
        is_deleted_report_failed >> Label("No") >> load_deleted_report_data >> create_deleted_collection

        # Delete path
        join_before_format = EmptyOperator(task_id="join_before_format", trigger_rule="none_failed_min_one_success")
        create_deleted_collection >> identify_deleted_bookings
        identify_deleted_bookings >> has_records_to_delete
        has_records_to_delete >> Label("Yes") >> prepare_delete_payload >> delete_records >> join_before_format
        has_records_to_delete >> Label("No") >> join_before_format
        join_before_format >> format_failure_report

        # --- Failure-notification path (linear, no fan-in) ---
        # format_failure_report runs with trigger_rule="all_done" so it
        # always fires after the work chain settles, then queries dag_run
        # task states directly to decide whether to email.
        format_failure_report >> has_failures_branch
        has_failures_branch >> Label("Yes") >> write_failure_csv >> email_failure_report >> end_task
        has_failures_branch >> Label("No") >> end_task

    return dag


for_each_instance(create_timeoff_export_report_dag)
