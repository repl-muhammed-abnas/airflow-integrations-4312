import json
from rail import (for_each_instance, create_airflow_dag, result, Label,
                  write_json_artifact, load_json_artifact, set_result,
                  PythonOperator, IfOperator, BatchTaskRunOperator, SimpleHttpOperator,
                  EmptyOperator, ViewDagRunConfOperator,
                  RepliconServiceOperator, RepliconServiceCallForEachItemOperator,
                  RepliconServicePageOperator, TriggerDagRunForEachItemOperator)
from airflow.models import Variable
from resource_planner.task_resource_allocation_export_webhooks.utils import (
    GRAPHQL_QUERY, API_HEADERS, expand_allocations_to_rows, derive_hours_type,
    get_project_custom_field, get_project_client_name, build_api_payload,
    get_current_project_role,
)


def create_new_allocation_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    with create_airflow_dag(
        dag_id=config.new_allocation_dag_id,
        description="Processes ProjectPolarisTaskAllocationCreated webhook events",
        start_date=config.start_date,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.new_max_active_runs,
    ) as dag:

        ViewDagRunConfOperator(task_id="view_dag_run_conf")

        can_run_batch_task = IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                "resource_planner_task_alloc_webhook_new_enable_batch_task", "true"
            ).lower() == "true",
            yes_task="batch_task",
            no_task="get_user_details"
        )

        batch_task = BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_user_details",
            end_task="end_task"
        )

        # =====================================================================
        # Phase 1: Get user and project details via Replicon API
        # =====================================================================

        get_user_details = RepliconServiceOperator(
            task_id="get_user_details",
            endpoint='/services/UserService1.svc/GetUserDetails',
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri']
            }
        )

        get_project_details = RepliconServiceOperator(
            task_id="get_project_details",
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=lambda dag_run: {
                "projects": [{
                    "uri": dag_run.conf['project_uri'],
                    "name": None,
                    "code": None,
                    "parameterCorrelationId": None
                }]
            }
        )

        # =====================================================================
        # Phase 2: Lookup labor codes and resources via RP Backend API
        # =====================================================================

        # Payloads are static (no runtime dependencies) — compute at DAG parse time.
        fetch_labor_codes = SimpleHttpOperator(
            task_id="fetch_labor_codes",
            method="POST",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/laborCodes",
            headers=_api_headers,
            data=json.dumps(build_api_payload(
                config.rp_api_target_table,
                mappingToSourceValues=[],
            )),
            response_filter=lambda response: response.json(),
            log_response=True,
            extra_options={"verify": False},
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

        fetch_resources = SimpleHttpOperator(
            task_id="fetch_resources",
            method="POST",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/resources",
            headers=_api_headers,
            data=json.dumps(build_api_payload(
                config.rp_api_target_table,
                employeeIds=[],
            )),
            response_filter=lambda response: response.json(),
            log_response=True,
            extra_options={"verify": False},
        )

        # =====================================================================
        # Phase 3: Resolve user metadata from API responses + lookups
        # =====================================================================

        def resolve_user_metadata_callable(dag_run):
            user_details = result('get_user_details')
            project_response = result('get_project_details')
            project_details = project_response[0].get('projectDetails', {}) if project_response else {}

            employee_id = user_details.get('employeeId', '')
            raw_hours_type = get_project_custom_field(project_details, 'Type')
            billing_status = get_project_custom_field(project_details, 'Billing Status')
            client_name = get_project_client_name(project_details)

            # Resolve users_user_id from API response
            resources_data = result('fetch_resources')
            employee_to_resource = {item['employeeId']: item for item in resources_data.get('data', [])}
            resource_info = employee_to_resource.get(str(employee_id), {})
            users_user_id = resource_info.get('usersUserId') or None

            hours_type = derive_hours_type(raw_hours_type, billing_status, client_name)
            primary_role = get_current_project_role(user_details.get('projectRoleSchedule', []))

            print(f"Resolved user metadata: employee_id={employee_id}, "
                  f"users_user_id={users_user_id}, hours_type={hours_type}, "
                  f"primary_role={primary_role!r}")

            return write_json_artifact({
                'user_uri': dag_run.conf['user_uri'],
                'employee_id': employee_id,
                'users_user_id': users_user_id,
                'hours_type': hours_type,
                'primary_role': primary_role,
            })

        resolve_user_metadata = PythonOperator(
            task_id="resolve_user_metadata",
            python_callable=resolve_user_metadata_callable,
        )

        # =====================================================================
        # Phase 4: Fetch allocation details via GraphQL
        # =====================================================================

        def get_allocation_items(dag_run):
            user_metadata = load_json_artifact(result('resolve_user_metadata'))
            return [{
                'project_uri': dag_run.conf['project_uri'],
                'user': user_metadata,
                'task_uris_batch': [dag_run.conf['task_uri']]
            }]

        def build_graphql_payload(item):
            return {
                "operationName": "TaskResourceUserAllocationsQuery",
                "variables": {
                    "projectUri": item['project_uri'],
                    "userUri": item['user']['user_uri'],
                    "taskUris": item['task_uris_batch']
                },
                "query": GRAPHQL_QUERY
            }

        def handle_allocation_response(data, item):
            allocations = data.get('data', {}).get('taskResourceUserAllocationsForUser', [])
            user = item['user']
            for alloc in allocations:
                alloc['_user_uri'] = user['user_uri']
                alloc['_employee_id'] = user['employee_id']
                alloc['_users_user_id'] = user.get('users_user_id') or None
                alloc['_hours_type'] = user.get('hours_type', '')
            set_result(key="alloc_count", val=len(allocations))
            return allocations

        fetch_allocation = RepliconServiceCallForEachItemOperator(
            task_id="fetch_allocation",
            replicon_conn_id=config.replicon_conn_id,
            app='polaris',
            endpoint=config.graphql_endpoint,
            items=get_allocation_items,
            data=build_graphql_payload,
            data_handler=handle_allocation_response,
            flatten=True,
            target='artifact',
        )

        # =====================================================================
        # Phase 5: Expand schedule rules and write via API
        # =====================================================================

        def expand_and_filter_callable(dag_run):
            allocations = load_json_artifact(result('fetch_allocation'))
            target_allocation_uuid = dag_run.conf['allocation_uuid']

            target_allocations = [
                a for a in allocations
                if a['id'].split(':')[-1] == target_allocation_uuid
            ]

            if not target_allocations:
                print(f"WARNING: Allocation {target_allocation_uuid} not found in GraphQL response")
                set_result(key="row_count", val=0)
                return write_json_artifact([])

            labor_codes_data = result('fetch_labor_codes')
            known_labor_codes = {item['mappingToSource'] for item in labor_codes_data.get('data', [])}
            role_uri_map = result('fetch_project_roles') or {}
            user_metadata = load_json_artifact(result('resolve_user_metadata'))
            primary_role = user_metadata.get('primary_role', '')
            for alloc in target_allocations:
                role_uri = alloc.get('roleUri') or ''
                display_name = role_uri_map.get(role_uri, '') if role_uri else primary_role
                labor_code_id = display_name if display_name in known_labor_codes else ''
                if not labor_code_id:
                    print(f"WARNING: no labor code for role {display_name!r} on allocation {alloc.get('id', '')}")
                alloc['_labor_code_id'] = labor_code_id

            rows = expand_allocations_to_rows(target_allocations, dag_run.conf['project_uri'])
            set_result(key="row_count", val=len(rows))
            return write_json_artifact(rows)

        expand_schedule_rules = PythonOperator(
            task_id="expand_schedule_rules",
            python_callable=expand_and_filter_callable,
        )

        has_rows = IfOperator(
            task_id="has_rows",
            test="{{ result('expand_schedule_rules', 'row_count') > 0 }}",
            yes_task="prepare_write_request",
            no_task="end_task"
        )

        def prepare_write_payload(**context):
            dag_run = context["dag_run"]
            return json.dumps(build_api_payload(
                config.rp_api_target_table,
                replacements=[{
                    "sourceBookingIdPrefix": dag_run.conf['allocation_uuid'],
                    "sourceSystem": "Polaris",
                    "records": load_json_artifact(result('expand_schedule_rules')),
                }],
            ))

        prepare_write_request = PythonOperator(
            task_id="prepare_write_request",
            python_callable=prepare_write_payload,
        )

        write_to_api = SimpleHttpOperator(
            task_id="write_to_api",
            method="PUT",
            http_conn_id=config.rp_api_conn_id,
            endpoint="/api/v1/rp/sourceAllocations",
            headers=_api_headers,
            data="{{ result('prepare_write_request') }}",
            response_check=lambda response: response.status_code == 200,
            log_response=True,
            extra_options={"verify": False},
        )

        end_task = EmptyOperator(task_id="end_task")

        # =====================================================================
        # Task Dependencies
        # =====================================================================
        can_run_batch_task >> Label("Yes") >> batch_task >> end_task
        can_run_batch_task >> Label("No") >> get_user_details

        get_user_details >> get_project_details >> fetch_labor_codes >> fetch_project_roles >> fetch_resources
        # ----------------------------------------------------------------
        # JIT-resolve project/task hierarchy in rp_source_time_codes.
        # Fire-and-forget — does not block the allocation write.
        # ----------------------------------------------------------------
        def extract_project_task_pairs_callable():
            rows = load_json_artifact(result('expand_schedule_rules'))
            by_project: dict = {}
            for r in rows:
                time_code = r.get('timeCode') or ''
                parts = time_code.split('~')
                if len(parts) < 2:
                    continue
                project_id, task_id = parts[0], parts[1]
                by_project.setdefault(project_id, set()).add(task_id)
            return [
                {"project_id": pid, "task_ids": sorted(tids)}
                for pid, tids in by_project.items()
            ]

        extract_project_task_pairs = PythonOperator(
            task_id="extract_project_task_pairs",
            python_callable=extract_project_task_pairs_callable,
        )

        def _ensure_conf_builder():
            def _build(item, dag_run=None, dag=None):
                return {
                    "project_id":       item["project_id"],
                    "task_ids":         item["task_ids"],
                    "sourceSystem":     "Polaris",
                    # Correlation context so JIT sync-failures can be
                    # traced back to the webhook run that triggered them.
                    "masterRunId":      dag_run.run_id if dag_run else "",
                    "triggeredByDagId": dag.dag_id if dag else "",
                }
            return _build

        trigger_ensure_project_tasks = TriggerDagRunForEachItemOperator(
            task_id="trigger_ensure_project_tasks",
            trigger_dag_id=f"resource_planner_ensure_project_tasks_{config.instance}",
            items=lambda: result("extract_project_task_pairs"),
            conf=_ensure_conf_builder(),
        )

        # JIT trigger is serialized into the main chain (no parallel branch)
        # so BatchTaskRunOperator's linear-chain requirement is satisfied.
        # The trigger task itself is sub-second — the spawned
        # ensure_project_tasks DAG runs still execute asynchronously in parallel
        # with write_to_api.
        (fetch_resources
            >> resolve_user_metadata
            >> fetch_allocation
            >> expand_schedule_rules
            >> extract_project_task_pairs
            >> trigger_ensure_project_tasks
            >> has_rows)
        has_rows >> Label("Yes") >> prepare_write_request >> write_to_api >> end_task
        has_rows >> Label("No") >> end_task

    return dag


for_each_instance(create_new_allocation_dag)
