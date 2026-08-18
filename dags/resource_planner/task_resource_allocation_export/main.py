import json
from datetime import datetime, timedelta
from rail import (for_each_instance, create_airflow_dag, result, Label, run_report2,
                  write_json_artifact, load_all_records, load_json_artifact, set_result,
                  LoadCSVFileOperator, PythonOperator, IfOperator, BatchTaskRunOperator,
                  SimpleHttpOperator, EmptyOperator, RepliconReportDetailsOperator, CreateCollectionOperator,
                  TriggerDagRunForEachItemOperator, WaitForDagRunsSensor, GatherResultsFromDagRunsOperator,
                  EmailOperator, RepliconServicePageOperator,
                  SFTPDownloadFileOperator, SFTPListFilesOperator,
                  SFTPUploadFileOperator, SFTPMoveFileOperator, WriteCSVFileOperator)
from airflow.models import Variable

API_HEADERS = {"Content-Type": "application/json"}


def create_task_resource_allocation_export_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    sftp_reference_path = f"{config.sftp_reference_base_path}/{config.sftp_reference_file}"

    with create_airflow_dag(
        dag_id=f"resource_planner_task_resource_allocation_export_{config.instance}",
        description="Exports task resource allocations from Polaris to Resource Planner database",
        schedule_interval=config.schedule_interval,
        start_date=config.start_date,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        can_run_batch_task = IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.resource_planner_task_resource_allocation_export_enable_batch_task, "true"
            ).lower() == "true",
            yes_task="batch_task",
            no_task="get_user_report_details"
        )

        batch_task = BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_user_report_details",
            end_task="end_task"
        )

        # =====================================================================
        # Phase 1: Fetch reports from Polaris
        # =====================================================================

        # --- User Report ---
        get_user_report_details = RepliconReportDetailsOperator(
            task_id="get_user_report_details",
            report_name=config.user_report_name,
        )

        run_user_report = run_report2(
            group_id="run_user_report",
            report_params={
                "reportParameters": [{
                    "reportUri": "{{ result('get_user_report_details').uri }}",
                    "filterValues": [],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }]
            }
        )

        user_report_has_data = IfOperator(
            task_id="user_report_has_data",
            test="{{ result('run_user_report.get_report_result', 'has_data') }}",
            yes_task='load_user_report_data',
            no_task='join_after_user_report'
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
                "Project Name": "project_name",
                "ProjectUri": "project_uri",
                "Employee ID": "employee_id",
                "UserUri": "user_uri",
                "Primary Role (Current)": "primary_role",
                "Type": "hours_type",
                "Billing Status": "billing_status",
                "Client Name": "client_name"
            }
        )

        # --- Task Report ---
        get_task_report_details = RepliconReportDetailsOperator(
            task_id="get_task_report_details",
            report_name=config.task_report_name,
        )

        run_task_report = run_report2(
            group_id="run_task_report",
            report_params={
                "reportParameters": [{
                    "reportUri": "{{ result('get_task_report_details').uri }}",
                    "filterValues": [],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }]
            }
        )

        task_report_has_data = IfOperator(
            task_id="task_report_has_data",
            test="{{ result('run_task_report.get_report_result', 'has_data') }}",
            yes_task='load_task_report_data',
            no_task='join_after_task_report'
        )

        load_task_report_data = LoadCSVFileOperator(
            task_id='load_task_report_data',
            document="{{ result('run_task_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_task_collection = CreateCollectionOperator(
            task_id="create_task_collection",
            name="task_collection",
            source="{{ result('load_task_report_data') }}",
            columns={
                "ProjectUri": "project_uri",
                "TaskUri": "task_uri"
            }
        )

        # =====================================================================
        # Phase 2: Database lookups + load previous reference from SFTP
        # =====================================================================

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

        sftp_list_reference_dir = SFTPListFilesOperator(
            task_id="sftp_list_reference_dir",
            sftp_conn_id=config.sftp_conn_id,
            paths=[config.sftp_reference_base_path],
        )

        has_previous_reference = IfOperator(
            task_id="has_previous_reference",
            test=lambda: any(
                f['name'] == config.sftp_reference_file
                for f in result('sftp_list_reference_dir').get(config.sftp_reference_base_path, [])
            ),
            yes_task="download_reference",
            no_task="build_reference_dict"
        )

        download_reference = SFTPDownloadFileOperator(
            task_id="download_reference",
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=sftp_reference_path,
        )

        load_reference_csv = LoadCSVFileOperator(
            task_id="load_reference_csv",
            document="{{ result('download_reference') }}",
        )

        def build_reference_dict_callable():
            """Transform CSV records into {project_uri: {allocation_id: hash}} dict."""
            csv_result = result('load_reference_csv')
            reference_by_project = {}
            if csv_result:
                records = load_all_records(csv_result)
                for row in records:
                    proj_uri = row['project_uri']
                    if proj_uri not in reference_by_project:
                        reference_by_project[proj_uri] = {}
                    reference_by_project[proj_uri][row['allocation_id']] = row['allocation_hash']
                print(f"Loaded previous reference: {sum(len(v) for v in reference_by_project.values())} "
                      f"allocations across {len(reference_by_project)} projects")
            else:
                print("No previous reference file — first run, all allocations treated as new")
            return write_json_artifact(reference_by_project)

        build_reference_dict = PythonOperator(
            task_id="build_reference_dict",
            python_callable=build_reference_dict_callable,
        )

        def _role_page_handler(request, response):
            if response.get('rows') and len(response['rows']) >= int(request['pagesize']):
                return {**request, 'page': str(int(request['page']) + 1)}
            return None

        def _role_result_handler(results):
            import itertools
            all_rows = list(itertools.chain(*[r.get('rows', []) for r in results]))
            role_map = {}
            for row in all_rows:
                cells = row.get('cells', [])
                if len(cells) < 2:
                    continue
                if not cells[1].get('boolValue', False):
                    continue
                uri = cells[0].get('uri', '')
                display_name = cells[0].get('textValue', '')
                if uri and display_name:
                    role_map[uri] = display_name
            return role_map

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

        # =====================================================================
        # Phase 3: Build per-project payloads and trigger children
        # =====================================================================

        def build_project_payloads_callable():
            user_records = load_all_records(result('create_user_collection'))
            task_records = load_all_records(result('create_task_collection'))

            role_uri_map = result('fetch_project_roles') or {}

            # Build employee_id → user_id lookup from users API
            users_data = result('fetch_user_id_map')
            employee_to_user_id = {}
            for item in users_data.get('data', []):
                employee_to_user_id[str(item['employeeId'])] = str(item['userId'])

            # Load previous reference from SFTP artifact
            reference_by_project = load_json_artifact(result('build_reference_dict'))

            # Group users by project, resolving labor codes
            users_by_project = {}
            project_names = {}
            for user in user_records:
                project_uri = user.get('project_uri', '')
                if not project_uri:
                    continue

                if project_uri not in project_names:
                    project_names[project_uri] = user.get('project_name', '')

                if project_uri not in users_by_project:
                    users_by_project[project_uri] = []
                users_by_project[project_uri].append({
                    'user_uri': user['user_uri'],
                    'employee_id': user['employee_id'],
                    'hours_type': user.get('hours_type', ''),
                    'billing_status': user.get('billing_status', ''),
                    'client_name': user.get('client_name', ''),
                    'users_user_id': employee_to_user_id.get(str(user['employee_id']), ''),
                    'primary_role': user.get('primary_role', ''),
                })

            # Group tasks by project (deduplicated, preserving order)
            tasks_by_project = {}
            seen_tasks = {}
            for task in task_records:
                project_uri = task.get('project_uri', '')
                task_uri = task.get('task_uri', '')
                if not project_uri or not task_uri:
                    continue
                if project_uri not in seen_tasks:
                    seen_tasks[project_uri] = set()
                    tasks_by_project[project_uri] = []
                if task_uri not in seen_tasks[project_uri]:
                    seen_tasks[project_uri].add(task_uri)
                    tasks_by_project[project_uri].append(task_uri)

            # Build payloads for projects that have both users and tasks
            all_project_uris = set(users_by_project.keys()) | set(tasks_by_project.keys())
            project_payloads = []
            project_index = 0
            for project_uri in all_project_uris:
                users = users_by_project.get(project_uri, [])
                task_uris = tasks_by_project.get(project_uri, [])
                if not users or not task_uris:
                    continue
                project_payloads.append({
                    'instance': config.instance,
                    'project_index': project_index,
                    'project_uri': project_uri,
                    'project_name': project_names.get(project_uri, ''),
                    'users': users,
                    'task_uris': task_uris,
                    'previous_reference': reference_by_project.get(project_uri, {}),
                    'task_batch_size': config.task_batch_size,
                    'role_uri_map': role_uri_map,
                })
                project_index += 1

            set_result(key="project_count", val=len(project_payloads))

            print(f"Built {len(project_payloads)} project payloads from "
                  f"{len(user_records)} user records and {len(task_records)} task records")

            return write_json_artifact({
                'payloads': project_payloads,
            })

        build_project_payloads = PythonOperator(
            task_id="build_project_payloads",
            python_callable=build_project_payloads_callable,
        )

        has_projects_to_process = IfOperator(
            task_id="has_projects_to_process",
            test="{{ result('build_project_payloads', 'project_count') > 0 }}",
            yes_task="trigger_child_per_project",
            no_task="join_after_projects"
        )

        # --- Trigger Child DAGs (distributed across N instances) ---
        # Each project is routed to one of N child DAG instances via modulo.
        # E.g., 40 projects with child_batch_count=5 → distributed across 5 child DAG instances.
        def get_project_items():
            data = load_json_artifact(result('build_project_payloads'))
            return data['payloads']

        def get_child_trigger_dag_id(dag_run, item):
            """Select child DAG instance using modulo of project index."""
            batch_number = (item['project_index'] % config.child_batch_count) + 1
            prefix = f"_{batch_number}"
            if batch_number == 1:
                prefix = ""
            return f"resource_planner_task_resource_allocation_export_child_{config.instance}{prefix}"

        trigger_child_per_project = TriggerDagRunForEachItemOperator(
            task_id="trigger_child_per_project",
            trigger_dag_id=get_child_trigger_dag_id,
            items=get_project_items,
            conf=lambda item: item,
        )

        wait_for_children = WaitForDagRunsSensor(
            task_id="wait_for_children",
            dag_runs="{{ result('trigger_child_per_project') }}",
            execution_timeout=timedelta(days=14)
        )

        # =====================================================================
        # Phase 4: After children — consolidate reference and upload to SFTP
        # =====================================================================

        gather_child_hashes = GatherResultsFromDagRunsOperator(
            task_id="gather_child_hashes",
            dag_runs="{{ result('trigger_child_per_project') }}",
            dagrun_task_id="publish_hashes",
            target='artifact',
        )

        def consolidate_hashes_callable():
            """
            Merge child hashes with previous reference, build CSV artifact for upload.
            """
            from rail.lib.artifact import new_artifact
            import csv
            import io

            # Load previous reference (to preserve entries for unprocessed projects)
            reference_by_project = load_json_artifact(result('build_reference_dict'))

            # Load gathered child results: each child returns a single
            # {project_uri, current_hashes} dict
            child_results = load_json_artifact(result('gather_child_hashes'))
            for hashes_data in child_results:
                if not hashes_data:
                    continue
                project_uri = hashes_data['project_uri']
                current_hashes = hashes_data['current_hashes']
                if current_hashes:
                    reference_by_project[project_uri] = current_hashes
                else:
                    reference_by_project.pop(project_uri, None)

            # Build CSV content and write to artifact
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(['project_uri', 'allocation_id', 'allocation_hash'])
            total_entries = 0
            for proj_uri, allocations in sorted(reference_by_project.items()):
                for alloc_id, alloc_hash in sorted(allocations.items()):
                    writer.writerow([proj_uri, alloc_id, alloc_hash])
                    total_entries += 1

            with new_artifact() as artifact:
                artifact.file.write(output.getvalue().encode('utf-8'))

            set_result(key="reference_entries", val=total_entries)
            print(f"Consolidated reference: {total_entries} allocations "
                  f"across {len(reference_by_project)} projects")
            return artifact.name

        consolidate_hashes = PythonOperator(
            task_id="consolidate_hashes",
            python_callable=consolidate_hashes_callable,
        )

        # Archive previous reference file (only if it existed)
        should_archive_reference = IfOperator(
            task_id="should_archive_reference",
            test=lambda: any(
                f['name'] == config.sftp_reference_file
                for f in result('sftp_list_reference_dir').get(config.sftp_reference_base_path, [])
            ),
            yes_task="archive_reference",
            no_task="join_pre_upload"
        )

        archive_reference = SFTPMoveFileOperator(
            task_id="archive_reference",
            sftp_conn_id=config.sftp_conn_id,
            existing_filename=sftp_reference_path,
            new_filename=f"{config.sftp_reference_base_path}/archive/ref_allocation_data_{{{{ ts_nodash }}}}.csv",
        )

        upload_reference = SFTPUploadFileOperator(
            task_id="upload_reference",
            sftp_conn_id=config.sftp_conn_id,
            content="{{ result('consolidate_hashes') }}",
            remote_filepath=sftp_reference_path,
        )

        # --- Failure-notification email (no DB write) ---
        gather_child_failures = GatherResultsFromDagRunsOperator(
            task_id="gather_child_failures",
            dag_runs="{{ result('trigger_child_per_project') }}",
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
            trigger_rule="one_failed",
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
                f"<td>{c.get('project_index')}</td>"
                f"<td><code>{c.get('project_uri')}</code></td>"
                f"<td>{', '.join(c.get('failed_task_ids') or [])}</td>"
                f"<td><code>{(c.get('error_excerpt') or '')[:200]}</code></td></tr>"
                for c in child_failures
            )
            html = (
                f"<p>Task Resource Allocation Export run had failures: "
                f"<strong>{master_failed_count}</strong> master task(s), "
                f"<strong>{len(child_failures)}</strong> child project(s). "
                "Full per-row detail attached as <code>failures.csv</code>.</p>"
                "<table border='1' cellpadding='6' cellspacing='0'>"
                "<tr><th>Child DAG</th><th>Project idx</th><th>Project URI</th><th>Failed tasks</th><th>Error excerpt</th></tr>"
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
            subject="{{ get_company_key() }} | ResourcePlanner Task Resource Allocation Export completed with error at '{{ current_time_in_specified_tz() }}'",
            html_content="{{ result('format_failure_report').get('html_summary') }}",
            files=[
                ("failures.csv", "{{ result('write_failure_csv') }}"),
            ],
        )

        end_task = EmptyOperator(
            task_id="end_task",
            trigger_rule="all_done",
        )

        # =====================================================================
        # Task Dependencies
        # =====================================================================
        join_pre_upload = EmptyOperator(task_id="join_pre_upload", trigger_rule="none_failed_min_one_success")
        join_after_projects = EmptyOperator(task_id="join_after_projects", trigger_rule="none_failed_min_one_success")
        join_after_task_report = EmptyOperator(task_id="join_after_task_report", trigger_rule="none_failed_min_one_success")
        join_after_user_report = EmptyOperator(task_id="join_after_user_report", trigger_rule="none_failed_min_one_success")

        can_run_batch_task >> Label("Yes") >> batch_task >> end_task
        can_run_batch_task >> Label("No") >> get_user_report_details

        # User report chain
        get_user_report_details >> run_user_report >> user_report_has_data
        user_report_has_data >> Label("Yes") >> load_user_report_data >> create_user_collection

        # Task report chain (after user collection)
        create_user_collection >> get_task_report_details >> run_task_report >> task_report_has_data
        task_report_has_data >> Label("Yes") >> load_task_report_data >> create_task_collection

        # DB lookups, SFTP reference download, and payload building
        fetch_user_id_map >> sftp_list_reference_dir
        create_task_collection >> fetch_user_id_map
        sftp_list_reference_dir >> has_previous_reference
        has_previous_reference >> Label("Yes") >> download_reference >> load_reference_csv >> build_reference_dict
        has_previous_reference >> Label("No") >> build_reference_dict
        build_reference_dict >> fetch_project_roles >> build_project_payloads

        # Trigger children
        build_project_payloads >> has_projects_to_process
        has_projects_to_process >> Label("Yes") >> trigger_child_per_project >> wait_for_children
        wait_for_children >> gather_child_hashes >> consolidate_hashes >> should_archive_reference
        should_archive_reference >> Label("Yes") >> archive_reference >> join_pre_upload
        should_archive_reference >> Label("No") >> join_pre_upload
        join_pre_upload >> upload_reference >> gather_child_failures >> log_failure >> join_after_projects
        has_projects_to_process >> Label("No") >> join_after_projects
        join_after_projects >> join_after_task_report
        task_report_has_data >> Label("No") >> join_after_task_report
        join_after_task_report >> join_after_user_report
        user_report_has_data >> Label("No") >> join_after_user_report
        join_after_user_report >> format_failure_report

        # --- Failure-notification path ---
        format_failure_report >> has_failures_branch
        has_failures_branch >> Label("Yes") >> write_failure_csv >> email_failure_report >> end_task
        has_failures_branch >> Label("No") >> end_task

    return dag


for_each_instance(create_task_resource_allocation_export_dag)
