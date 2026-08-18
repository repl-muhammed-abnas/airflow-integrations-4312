import json
from datetime import timedelta
from rail import (for_each_instance, create_airflow_dag, result, Label, run_report2, load_all_records,
                    find_first_by_attr_and_get_attr, load_json_artifact, write_json_artifact, set_result,
                    LoadCSVFileOperator, PythonOperator, IfOperator, SimpleHttpOperator,
                    EmptyOperator, RepliconReportDetailsOperator, CreateCollectionOperator,
                    QueryCollectionOperator, ViewDagRunConfOperator, WebhookConf)

API_HEADERS = {"Content-Type": "application/json"}


def create_project_task_export_delta_delete_child_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    with create_airflow_dag(
        dag_id=f"resource_planner_project_task_export_delta_delete_child_{config.instance}",
        description="Handles deletion of Project Tasks from Resource Planner database",
        start_date=config.start_date,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        webhook_conf=WebhookConf(bearer_token_var="demo_test_webhook_token")
    ) as dag:

        ViewDagRunConfOperator(task_id="view_dag_run_conf")

        get_delete_batch_details = QueryCollectionOperator(
            task_id="get_delete_batch_details",
            name="delete_batch_collection",
            query="""SELECT project_uri, project_name
                     FROM create_delete_indexed_table
                     WHERE CAST(row_index AS INTEGER) BETWEEN {{ dag_run.conf.start_index }} AND {{ dag_run.conf.end_index }}"""
        )

        def separate_delete_types_function():
            records = load_all_records(result('get_delete_batch_details'))
            uri_projects = []
            name_only_projects = []
            for r in records:
                uri = r.get('project_uri', '').strip() if r.get('project_uri') else ''
                name = r.get('project_name', '').strip() if r.get('project_name') else ''
                if uri:
                    uri_projects.append({'project_uri': uri, 'project_name': name})
                elif name:
                    name_only_projects.append({'project_name': name})
            set_result(key="uri_delete_count", val=len(uri_projects))
            set_result(key="name_delete_count", val=len(name_only_projects))
            return write_json_artifact({
                'uri_projects': uri_projects,
                'name_only_projects': name_only_projects
            })

        separate_delete_types = PythonOperator(
            task_id="separate_delete_types",
            python_callable=separate_delete_types_function,
        )

        # =====================================================================
        # URI PATH: project still exists in Polaris, tasks were deleted
        # =====================================================================

        has_uri_deletes = IfOperator(
            task_id="has_uri_deletes",
            test="{{ result('separate_delete_types', 'uri_delete_count') > 0 }}",
            yes_task="get_project_task_report",
            no_task="log_failure"
        )

        get_project_task_report = RepliconReportDetailsOperator(
            task_id="get_project_task_report",
            report_name=config.project_task_report_name,
        )

        def run_project_task_report_payload():
            data = load_json_artifact(result('separate_delete_types'))
            uri_projects = data['uri_projects']
            project_filter = find_first_by_attr_and_get_attr(
                result("get_project_task_report")['filterConfiguration']['enabledFilters'],
                'displayText',
                'ProjectFilter',
                'uri'
            )
            filter_vals = [
                {
                    "reportFilterUri": project_filter,
                    "value": p['project_uri'].split(':')[-1]
                } for p in uri_projects
            ]
            return {
                "reportParameters": [
                    {
                        "reportUri": result('get_project_task_report')['uri'],
                        "filterValues": filter_vals,
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }

        run_project_task_report = run_report2(
            group_id="run_project_task_report",
            report_params=run_project_task_report_payload
        )

        report_has_data = IfOperator(
            task_id="report_has_data",
            test="{{ result('run_project_task_report.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='prepare_cascade_delete_payload'
        )

        load_report_data = LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_project_task_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_project_task_collection = CreateCollectionOperator(
            task_id="create_project_task_collection",
            name="project_task_collection",
            source="{{ result('load_report_data') }}",
            columns={
                "Project Name": "Project_Name",
                "project_uri": "project_uri",
                "Task Name (Full Path)": "Task_Full_Path",
                "Task Name": "Task_Name",
                "task_uri": "task_uri",
            }
        )

        # For the URI path, we still need to know what's in the DB so we can
        # compare against the report to find deletions.
        # We use the upsert approach for adds/updates, but deletions require
        # knowing the current DB state. We build the report keys and send
        # specific deletes for anything not in the report.
        def prepare_upsert_and_delete_payloads_callable():
            report_data = load_all_records(result('create_project_task_collection'))
            data = load_json_artifact(result('separate_delete_types'))
            uri_projects = data['uri_projects']
            target_table = getattr(config, 'rp_api_target_table', None)

            # Build full_path -> task_name lookup for hierarchy walk.
            full_path_to_name = {}
            for row in report_data:
                if row.get('task_uri'):
                    full_path_to_name[row.get('Task_Full_Path', '')] = row.get('Task_Name', '')

            level_cache = {}
            display_cache = {}

            def resolve(full_path, task_name, project_name):
                if full_path in level_cache:
                    return level_cache[full_path], display_cache[full_path]
                if full_path == task_name:
                    lvl = 1
                    disp = f"{project_name}~{task_name}"
                else:
                    parent_path = full_path[:-(len(task_name) + 3)]
                    parent_name = full_path_to_name.get(parent_path)
                    if parent_name is None:
                        lvl = 1
                        disp = f"{project_name}~{task_name}"
                    else:
                        p_lvl, p_disp = resolve(parent_path, parent_name, project_name)
                        lvl = p_lvl + 1
                        disp = f"{p_disp}~{task_name}"
                level_cache[full_path] = lvl
                display_cache[full_path] = disp
                return lvl, disp

            report_keys = set()
            upsert_records = []
            for row in report_data:
                project_uri = row.get('project_uri', '')
                task_uri = row.get('task_uri', '')
                project_id = project_uri.split(':')[-1] if project_uri else ''
                project_name = row.get('Project_Name', '')

                if not task_uri:
                    report_keys.add((project_id, 'project'))
                    upsert_records.append({
                        'sourceSystem': 'Polaris',
                        'parentTimeCode': project_id,
                        'timeCode': project_id,
                        'timeCodeName': project_name[:255],
                        'parentTimeCodeName': project_name,
                        'projectManagerId': '',
                        'type': 'project',
                        'taskLevel': 0,
                        'timeEntryEnabled': False
                    })
                else:
                    task_id = task_uri.split(':')[-1]
                    report_keys.add((f"{project_id}~{task_id}", 'task'))
                    full_path = row.get('Task_Full_Path', '')
                    task_name = row.get('Task_Name', '')
                    task_level, time_code_name = resolve(full_path, task_name, project_name) if task_name else (0, project_name)
                    upsert_records.append({
                        'sourceSystem': 'Polaris',
                        'parentTimeCode': project_id,
                        'timeCode': f"{project_id}~{task_id}",
                        'timeCodeName': time_code_name[:255],
                        'parentTimeCodeName': project_name,
                        'projectManagerId': '',
                        'type': 'task',
                        'taskLevel': task_level,
                        'timeEntryEnabled': False
                    })

            project_ids_in_report = {tc.split('~')[0] for tc, _ in report_keys}
            deletions = []
            for p in uri_projects:
                project_id = p['project_uri'].split(':')[-1]
                if project_id not in project_ids_in_report:
                    deletions.append({
                        "mode": "projectCascade",
                        "sourceSystem": "Polaris",
                        "projectTimeCode": project_id
                    })

            upsert_payload = {"records": upsert_records}
            if target_table:
                upsert_payload["targetTable"] = target_table

            delete_payload = {"deletions": deletions}
            if target_table:
                delete_payload["targetTable"] = target_table

            set_result(key="upsert_payload_json", val=json.dumps(upsert_payload))
            set_result(key="delete_payload_json", val=json.dumps(delete_payload))
            print(f"Prepared {len(upsert_records)} upserts, {len(deletions)} cascade deletes")

        prepare_upsert_and_delete_payloads = PythonOperator(
            task_id="prepare_upsert_and_delete_payloads",
            python_callable=prepare_upsert_and_delete_payloads_callable,
        )

        upsert_existing_records = SimpleHttpOperator(
            task_id="upsert_existing_records",
            method="PUT",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/sourceTimeCodesProjectTasks",
            headers=_api_headers,
            data="{{ result('prepare_upsert_and_delete_payloads', 'upsert_payload_json') }}",
            response_check=lambda response: response.status_code == 200,
            log_response=True,
            extra_options={"verify": False},
        )

        delete_missing_projects = SimpleHttpOperator(
            task_id="delete_missing_projects",
            method="DELETE",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/sourceTimeCodesProjectTasks",
            headers=_api_headers,
            data="{{ result('prepare_upsert_and_delete_payloads', 'delete_payload_json') }}",
            response_check=lambda response: response.status_code == 200,
            log_response=True,
            extra_options={"verify": False},
        )

        # When report has no data for URI projects, they were fully deleted from Polaris
        def prepare_cascade_delete_payload_callable():
            data = load_json_artifact(result('separate_delete_types'))
            uri_projects = data['uri_projects']
            target_table = getattr(config, 'rp_api_target_table', None)

            deletions = [
                {
                    "mode": "projectCascade",
                    "sourceSystem": "Polaris",
                    "projectTimeCode": str(p['project_uri'].split(':')[-1])
                }
                for p in uri_projects
            ]

            payload = {"deletions": deletions}
            if target_table:
                payload["targetTable"] = target_table

            print(f"Prepared cascade delete for {len(uri_projects)} projects (no report data)")
            return json.dumps(payload)

        prepare_cascade_delete_payload = PythonOperator(
            task_id="prepare_cascade_delete_payload",
            python_callable=prepare_cascade_delete_payload_callable,
        )

        delete_all_for_uri_projects = SimpleHttpOperator(
            task_id="delete_all_for_uri_projects",
            method="DELETE",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/sourceTimeCodesProjectTasks",
            headers=_api_headers,
            data="{{ result('prepare_cascade_delete_payload') }}",
            response_check=lambda response: response.status_code == 200,
            log_response=True,
            extra_options={"verify": False},
        )

        # =====================================================================
        # NAME PATH: project deleted from Polaris, no URI available
        # =====================================================================

        has_name_deletes = IfOperator(
            task_id="has_name_deletes",
            test="{{ result('separate_delete_types', 'name_delete_count') > 0 }}",
            yes_task="prepare_delete_by_name_payload",
            no_task="log_failure"
        )

        def prepare_delete_by_name_payload_callable():
            data = load_json_artifact(result('separate_delete_types'))
            name_only_projects = data['name_only_projects']
            target_table = getattr(config, 'rp_api_target_table', None)

            deletions = [
                {
                    "mode": "byProjectName",
                    "sourceSystem": "Polaris",
                    "projectName": str(p['project_name'])
                }
                for p in name_only_projects
            ]

            payload = {"deletions": deletions}
            if target_table:
                payload["targetTable"] = target_table

            print(f"Prepared delete by name for {len(name_only_projects)} projects")
            return json.dumps(payload)

        prepare_delete_by_name_payload = PythonOperator(
            task_id="prepare_delete_by_name_payload",
            python_callable=prepare_delete_by_name_payload_callable,
        )

        delete_by_name = SimpleHttpOperator(
            task_id="delete_by_name",
            method="DELETE",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/sourceTimeCodesProjectTasks",
            headers=_api_headers,
            data="{{ result('prepare_delete_by_name_payload') }}",
            response_check=lambda response: response.status_code == 200,
            log_response=True,
            extra_options={"verify": False},
        )

        # --- Failure logging to XCom (master gathers this) ---
        # Runs at end of every child run (trigger_rule="all_done", linear).
        # Returns None when nothing failed so the master can filter cleanly.
        def log_failure_callable(**context):
            dag = context.get("dag")
            dag_run = context.get("dag_run")
            conf = (dag_run.conf if dag_run else None) or {}
            failed_task_ids = []
            if dag_run:
                for ti in dag_run.get_task_instances():
                    if str(ti.state) == "failed":
                        failed_task_ids.append(ti.task_id)
            if not failed_task_ids:
                return None
            return {
                "level":           "child",
                "child_dag_id":    dag.dag_id if dag else "",
                "child_run_id":    dag_run.run_id if dag_run else "",
                "batch_index":     conf.get("batch_index"),
                "start_index":     conf.get("start_index"),
                "end_index":       conf.get("end_index"),
                "failed_task_ids": failed_task_ids,
                "error_excerpt":   f"Failed tasks: {', '.join(failed_task_ids)}"[:500],
            }

        log_failure = PythonOperator(
            task_id="log_failure",
            python_callable=log_failure_callable,
            trigger_rule="all_done",
        )

        end_task = EmptyOperator(
            task_id="end_task",
            trigger_rule="all_done",
        )

        # Task dependencies (linear — no fan-in)
        get_delete_batch_details >> separate_delete_types

        # URI path → log_failure
        separate_delete_types >> has_uri_deletes
        has_uri_deletes >> Label("Yes") >> get_project_task_report >> run_project_task_report >> report_has_data
        report_has_data >> Label("Yes") >> load_report_data >> create_project_task_collection >> prepare_upsert_and_delete_payloads >> upsert_existing_records >> delete_missing_projects >> log_failure
        report_has_data >> Label("No") >> prepare_cascade_delete_payload >> delete_all_for_uri_projects >> log_failure
        has_uri_deletes >> Label("No") >> log_failure

        # Name path → log_failure
        separate_delete_types >> has_name_deletes
        has_name_deletes >> Label("Yes") >> prepare_delete_by_name_payload >> delete_by_name >> log_failure
        has_name_deletes >> Label("No") >> log_failure

        log_failure >> end_task

    return dag


for_each_instance(
    create_project_task_export_delta_delete_child_dag
)
