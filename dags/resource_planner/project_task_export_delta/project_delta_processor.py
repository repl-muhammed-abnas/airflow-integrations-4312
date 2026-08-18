import json
from datetime import timedelta
from functools import lru_cache
from rail import (for_each_instance, create_airflow_dag, result, Label, run_report2, load_all_records, find_first_by_attr_and_get_attr, load_json_artifact, write_json_artifact,set_result,
                    LoadCSVFileOperator, PythonOperator, IfOperator, BatchTaskRunOperator,
                    SimpleHttpOperator, EmptyOperator, RepliconReportDetailsOperator, CreateCollectionOperator,
                    QueryCollectionOperator, ViewDagRunConfOperator,
                    TriggerDagRunForEachItemOperator, WaitForDagRunsSensor, WebhookConf,
                    RepliconServicePageOperator)
from airflow.models import Variable

API_HEADERS = {"Content-Type": "application/json"}

def create_project_task_export_delta_child_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    with create_airflow_dag(
        dag_id=f"resource_planner_project_task_export_delta_child_{config.instance}",
        description="Exports Project Tasks from Polaris to Resource Planner database",
        start_date=config.start_date,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        ViewDagRunConfOperator(task_id = "view_dag_run_conf")
        
        can_run_batch_task = IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(config.resource_planner_project_task_export_enable_batch_task, "true").lower() == "false",
            yes_task="batch_task",
            no_task="get_batch_project_details"
        )

        batch_task = BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_batch_project_details",
            end_task="end_task"
        )

        get_batch_project_details = QueryCollectionOperator(
            task_id="get_batch_project_details",
            name="batch_collection",
            query="""SELECT project_uri
                     FROM create_indexed_table
                     WHERE project_uri IS NOT NULL
                     AND CAST(row_index AS INTEGER) BETWEEN {{ dag_run.conf.start_index }} AND {{ dag_run.conf.end_index }}"""
        )


        get_project_task_report = RepliconReportDetailsOperator(
            task_id="get_project_task_report",
            report_name=config.project_task_report_name,
        )

        def run_project_task_report_payload():
            projects = load_all_records(result('get_batch_project_details'))
            project_filter = find_first_by_attr_and_get_attr(
                                    result("get_project_task_report")['filterConfiguration']['enabledFilters'],
                                    'displayText',
                                    'ProjectFilter',
                                    'uri'
                                    )
            filter_vals = [
                {
                    "reportFilterUri": project_filter,
                    "value": project['project_uri'].split(':')[-1]
                } for project in projects
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
            no_task='join_after_report'
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
                "Project Code": "Project_Code",
                "project_uri": "project_uri",
                "Task Name (Full Path)": "Task_Full_Path",
                "Task Name": "Task_Name",
                "Task Code": "Task_Code",
                "task_uri": "task_uri",
                "Project Manager": "Project_Manager",
                "Client Name": "Client_Name",
                "ClientUri": "Client_Uri",
            }
        )

        null = None

        # Fetch tasks with time entry enabled from Polaris API
        def get_time_entry_enabled_payload():
            projects = load_all_records(result('get_batch_project_details'))
            project_uris = [p['project_uri'] for p in projects if p.get('project_uri')]
            return {
                "page": 1,
                "pagesize": 250,
                "columnUris": [
                    "urn:replicon:task-list-column:task"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:task-list-filter:project"
                        },
                        "operatorUri": "urn:replicon:filter-operator:in",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": null,
                                "uris": project_uris,
                                "bool": null,
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "dateTimeUtc": null,
                                "dateTimeUtcRange": null,
                                "numberRange": null
                            },
                            "filterDefinitionUri": null
                        },
                            "value": null,
                            "filterDefinitionUri": null
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:task-list-filter:time-entry-allowed"
                        },
                        "operatorUri": "urn:replicon:filter-operator:equal",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": null,
                                "uris": [],
                                "bool": "true",
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "dateTimeUtc": null,
                                "dateTimeUtcRange": null,
                                "numberRange": null
                            },
                            "filterDefinitionUri": null
                        },
                        "value": null,
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }

        def time_entry_page_handler(request, response):
            if response.get('rows', []) and len(response['rows']) >= request['pagesize']:
                return {**request, 'page': request['page'] + 1}
            return None

        def time_entry_result_handler(results):
            import itertools
            all_rows = list(itertools.chain(*[r.get('rows', []) for r in results]))
            return {row['cells'][0]['uri'] for row in all_rows if row.get('cells')}

        fetch_time_entry_enabled_tasks = RepliconServicePageOperator(
            task_id="fetch_time_entry_enabled_tasks",
            endpoint='/services/TaskListService1.svc/GetData',
            data=get_time_entry_enabled_payload,
            page_handler=time_entry_page_handler,
            all_result_data_handler=time_entry_result_handler,
        )

        # No need to fetch existing records — upsert API handles insert vs update

        @lru_cache(maxsize=8)
        def load_user_name_id_map(user_name_id_map_artifact_name) -> dict:
            return load_json_artifact(user_name_id_map_artifact_name)

        @lru_cache(maxsize=8)
        def get_project_manager_id(manager_name, user_name_id_map_artifact_name):
            ret_val = load_user_name_id_map(user_name_id_map_artifact_name).get(manager_name, None) if manager_name else None
            return str(ret_val) if ret_val is not None else ''

        def build_upsert_records_function(dag_run):
            report_data = load_all_records(result('create_project_task_collection'))
            from collections import defaultdict

            time_entry_enabled_tasks = result('fetch_time_entry_enabled_tasks')
            records_to_upsert = []
            user_name_id_map_artifact_name = dag_run.conf['user_name_id_map']

            # Group rows per project so the full_path lookup and resolve()
            # caches stay scoped to a single project. A global cache lets
            # two projects with colliding Task_Full_Paths (e.g. both have
            # "DICE - Integrations" in their tree) overwrite each other's
            # display_name — the first project wins the cache and the
            # second gets the wrong project prefix in its timeCodeName.
            rows_by_project = defaultdict(list)
            for row in report_data:
                rows_by_project[row.get('project_uri', '')].append(row)

            for project_rows in rows_by_project.values():
                full_path_to_name = {}
                # full_path -> task_uri for predecessor_time_code lookup
                full_path_to_task_uri = {}
                for row in project_rows:
                    if row.get('task_uri'):
                        path = row.get('Task_Full_Path', '')
                        full_path_to_name[path] = row.get('Task_Name', '')
                        full_path_to_task_uri[path] = row.get('task_uri', '')

                level_cache = {}
                display_cache = {}

                def resolve(full_path, task_name, project_name,
                            _level_cache=level_cache, _display_cache=display_cache,
                            _full_path_to_name=full_path_to_name):
                    if full_path in _level_cache:
                        return _level_cache[full_path], _display_cache[full_path]
                    if full_path == task_name:
                        lvl = 1
                        disp = f"{project_name}~{task_name}"
                    else:
                        parent_path = full_path[:-(len(task_name) + 3)]  # strip " / {task_name}"
                        parent_name = _full_path_to_name.get(parent_path)
                        if parent_name is None:
                            lvl = 1
                            disp = f"{project_name}~{task_name}"
                        else:
                            p_lvl, p_disp = resolve(parent_path, parent_name, project_name)
                            lvl = p_lvl + 1
                            disp = f"{p_disp}~{task_name}"
                    _level_cache[full_path] = lvl
                    _display_cache[full_path] = disp
                    return lvl, disp

                for row in project_rows:
                    project_name = row['Project_Name']
                    project_manager = row['Project_Manager']
                    project_uri = row.get('project_uri', '')
                    task_uri = row.get('task_uri', '')

                    project_id = project_uri.split(':')[-1] if project_uri else ''
                    manager_id = get_project_manager_id(project_manager, user_name_id_map_artifact_name)
                    client_name = row.get('Client_Name', '') or ''
                    client_uri = row.get('Client_Uri', '') or ''
                    client_id = client_uri.split(':')[-1] if client_uri else ''

                    if not task_uri:
                        records_to_upsert.append({
                            'sourceSystem': 'Polaris',
                            'parentTimeCode': project_id,
                            'timeCode': project_id,
                            'timeCodeName': project_name[:255],
                            'parentTimeCodeName': project_name,
                            'projectManagerId': manager_id,
                            'type': 'project',
                            'taskLevel': 0,
                            'timeEntryEnabled': False,
                            # Project rows leave the new columns NULL — see bulk
                            # processor for rationale. UQ_rp_src_tc_actual filters
                            # on `actual_time_code IS NOT NULL`, so NULL is fine.
                            'actualTimeCode': None,
                            'actualTimeCodeName': None,
                            'predecessorTimeCode': None,
                            'clientName': client_name,
                            'clientId': client_id,
                        })
                    else:
                        task_id = task_uri.split(':')[-1]
                        time_code = f"{project_id}~{task_id}"
                        full_path = row.get('Task_Full_Path', '')
                        task_name = row.get('Task_Name', '')
                        task_level, time_code_name = resolve(full_path, task_name, project_name)
                        time_entry_enabled = task_uri in time_entry_enabled_tasks

                        # Predecessor = immediate parent's id (last URN segment)
                        if task_level <= 1:
                            predecessor_id = project_id
                        else:
                            parent_suffix_len = len(task_name) + 3
                            parent_path = full_path[:-parent_suffix_len]
                            parent_task_uri = full_path_to_task_uri.get(parent_path, '')
                            predecessor_id = (
                                parent_task_uri.split(':')[-1]
                                if parent_task_uri else project_id
                            )

                        records_to_upsert.append({
                            'sourceSystem': 'Polaris',
                            'parentTimeCode': project_id,
                            'timeCode': time_code,
                            'timeCodeName': time_code_name[:255],
                            'parentTimeCodeName': project_name,
                            'projectManagerId': manager_id,
                            'type': 'task',
                            'taskLevel': task_level,
                            'timeEntryEnabled': time_entry_enabled,
                            'actualTimeCode': task_id,
                            'actualTimeCodeName': task_name,
                            'predecessorTimeCode': predecessor_id,
                            'clientName': client_name,
                            'clientId': client_id,
                        })

            set_result(key="total_record_count", val=len(records_to_upsert))
            return write_json_artifact(records_to_upsert)

        identify_records_to_upsert = PythonOperator(
            task_id="identify_records_to_upsert",
            python_callable=build_upsert_records_function,
        )

        has_records_to_upsert = IfOperator(
            task_id="has_records_to_upsert",
            test="{{ result('identify_records_to_upsert', 'total_record_count') > 0 }}",
            yes_task="prepare_upsert_payload",
            no_task="join_after_upsert"
        )

        def prepare_upsert_payload_callable():
            records = load_json_artifact(result('identify_records_to_upsert'))
            target_table = getattr(config, 'rp_api_target_table', None)

            payload = {"records": records}
            if target_table:
                payload["targetTable"] = target_table

            print(f"Prepared {len(records)} records for upsert")
            return json.dumps(payload)

        prepare_upsert_payload = PythonOperator(
            task_id="prepare_upsert_payload",
            python_callable=prepare_upsert_payload_callable,
        )

        upsert_records = SimpleHttpOperator(
            task_id="upsert_records",
            method="PUT",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/sourceTimeCodesProjectTasks",
            headers=_api_headers,
            data="{{ result('prepare_upsert_payload') }}",
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

        join_after_upsert = EmptyOperator(task_id="join_after_upsert", trigger_rule="none_failed_min_one_success")
        join_after_report = EmptyOperator(task_id="join_after_report", trigger_rule="none_failed_min_one_success")
        end_task = EmptyOperator(
            task_id="end_task",
            trigger_rule="all_done",
        )

        # Task dependencies
        can_run_batch_task >> Label("Yes") >> batch_task >> end_task
        can_run_batch_task >> Label("No") >> get_batch_project_details

        get_batch_project_details >> get_project_task_report >> run_project_task_report >> report_has_data
        report_has_data >> Label("Yes") >> load_report_data >> create_project_task_collection
        create_project_task_collection >> fetch_time_entry_enabled_tasks >> identify_records_to_upsert

        identify_records_to_upsert >> has_records_to_upsert
        has_records_to_upsert >> Label("Yes") >> prepare_upsert_payload >> upsert_records >> join_after_upsert
        has_records_to_upsert >> Label("No") >> join_after_upsert
        join_after_upsert >> join_after_report
        report_has_data >> Label("No") >> join_after_report
        join_after_report >> log_failure >> end_task

    return dag


for_each_instance(
    create_project_task_export_delta_child_dag
)
