from datetime import datetime, timedelta
import hashlib
import json
from rail import (for_each_instance, create_airflow_dag, result, Label,
                  write_json_artifact, load_json_artifact, set_result,
                  PythonOperator, IfOperator, BatchTaskRunOperator, SimpleHttpOperator,
                  EmptyOperator, ViewDagRunConfOperator, WebhookConf,
                  RepliconServiceCallForEachItemOperator)
from airflow.models import Variable

API_HEADERS = {"Content-Type": "application/json"}

WEEKDAY_ABBRS = ['mo', 'tu', 'we', 'th', 'fr', 'sa', 'su']

GRAPHQL_QUERY = """query TaskResourceUserAllocationsQuery(
    $projectUri: String!,
    $userUri: String!,
    $taskUris: [String!]!
) {
    taskResourceUserAllocationsForUser(
        filter: {
            projectUri: $projectUri,
            userUri: $userUri,
            taskUris: $taskUris
        }
    ) {
        taskUri
        totalHours
        id
        roleUri
        lastModifiedTimestamp
        scheduleRules {
            dateRange {
                startDate
                endDate
            }
            do
        }
    }
}"""


def create_task_resource_allocation_export_child_dag(config):
    _api_headers = {"Content-Type": "application/json", **({"X-RP-Database": config.rp_api_db_env} if getattr(config, 'rp_api_db_env', None) else {})}
    _dags = []
    for batch_index in range(1, config.child_batch_count + 1):
        prefix = f"_{batch_index}"
        if batch_index == 1:
            prefix = ""
        with create_airflow_dag(
            dag_id=f"resource_planner_task_resource_allocation_export_child_{config.instance}{prefix}",
            description="Processes one project - fetches allocations, detects deltas, writes to DB",
            start_date=config.start_date,
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_child,
            webhook_conf=WebhookConf(bearer_token_var="demo_test_webhook_token")
        ) as dag:

            ViewDagRunConfOperator(task_id="view_dag_run_conf")

            can_run_batch_task = IfOperator(
                task_id="can_run_batch_task",
                test=lambda: Variable.get(
                    config.resource_planner_task_resource_allocation_export_enable_batch_task, "true"
                ).lower() == "true",
                yes_task="batch_task",
                no_task="fetch_allocations"
            )

            batch_task = BatchTaskRunOperator(
                task_id="batch_task",
                start_task="fetch_allocations",
                end_task="end_task"
            )

            # =================================================================
            # Step 1: Fetch allocations via GraphQL API
            #         Iterates over (user, task_batch) combinations with
            #         built-in retry on throttle/transient errors.
            # =================================================================

            def get_allocation_items(dag_run):
                """Build flat list of (user, task_batch) combinations to iterate over."""
                conf = dag_run.conf
                project_uri = conf['project_uri']
                users = conf['users']
                task_uris = conf['task_uris']
                batch_size = conf.get('task_batch_size', config.task_batch_size)

                items = []
                for user in users:
                    for i in range(0, len(task_uris), batch_size):
                        items.append({
                            'project_uri': project_uri,
                            'user': user,
                            'task_uris_batch': task_uris[i:i + batch_size]
                        })
                return items

            def build_graphql_payload(item):
                """Build GraphQL query payload for a single (user, task_batch) combination."""
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
                """Extract allocations from GraphQL response and attach user metadata."""
                allocations = data.get('data', {}).get('taskResourceUserAllocationsForUser', [])
                user = item['user']
                for alloc in allocations:
                    alloc['_user_uri'] = user['user_uri']
                    alloc['_employee_id'] = user['employee_id']
                    alloc['_users_user_id'] = user.get('users_user_id') or None
                    alloc['_primary_role'] = user.get('primary_role', '')
                    raw_type = user.get('hours_type', '')
                    billing_status = user.get('billing_status', '')
                    client_name = user.get('client_name', '')
                    if raw_type == 'NCVP' and client_name == 'Deltek':
                        alloc['_hours_type'] = 'Internal Non-Billable'
                    elif billing_status == 'Billable':
                        alloc['_hours_type'] = 'Client Project'
                    elif billing_status == 'Non-Billable':
                        alloc['_hours_type'] = 'Internal Non-Billable'
                    else:
                        alloc['_hours_type'] = raw_type
                set_result(key="alloc_count", val=len(allocations))
                return allocations

            fetch_allocations = RepliconServiceCallForEachItemOperator(
                task_id="fetch_allocations",
                replicon_conn_id=config.replicon_conn_id,
                app='polaris',
                endpoint=config.graphql_endpoint,
                items=get_allocation_items,
                data=build_graphql_payload,
                data_handler=handle_allocation_response,
                flatten=True,
                target='artifact',
            )

            fetch_labor_codes = SimpleHttpOperator(
                task_id="fetch_labor_codes",
                method="POST",
                http_conn_id=config.rp_api_conn_id,
                endpoint="/api/v1/rp/laborCodes",
                headers=_api_headers,
                data=json.dumps({
                    **({"targetTable": config.rp_api_target_table} if getattr(config, 'rp_api_target_table', None) else {}),
                    "mappingToSourceValues": [],
                }),
                response_filter=lambda response: response.json(),
                log_response=True,
                extra_options={"verify": False},
            )

            # =================================================================
            # Step 2: Expand schedule rules into daily rows
            # =================================================================
            def expand_schedule_rules_callable(dag_run):
                allocations = load_json_artifact(result('fetch_allocations'))
                project_uri = dag_run.conf['project_uri']
                project_id = project_uri.split(':')[-1]

                labor_codes_data = result('fetch_labor_codes')
                known_labor_codes = {item['mappingToSource'] for item in labor_codes_data.get('data', [])}
                role_uri_map = dag_run.conf.get('role_uri_map') or {}

                expanded_rows = []

                for alloc in allocations:
                    allocation_id = alloc['id'].split(':')[-1]  # Extract UUID
                    task_uri = alloc['taskUri']
                    task_id = task_uri.split(':')[-1]
                    time_code = f"{project_id}~{task_id}"
                    user_id = alloc['_users_user_id']
                    role_uri = alloc.get('roleUri') or ''
                    display_name = role_uri_map.get(role_uri, '') if role_uri else alloc.get('_primary_role', '')
                    labor_code = display_name if display_name in known_labor_codes else ''
                    if not labor_code:
                        print(f"WARNING: no labor code for role {display_name!r} on allocation {allocation_id}")

                    for rule in alloc.get('scheduleRules', []):
                        start_str = rule['dateRange']['startDate'][:10]
                        end_str = rule['dateRange']['endDate'][:10]
                        start_date = datetime.strptime(start_str, '%Y-%m-%d')
                        end_date = datetime.strptime(end_str, '%Y-%m-%d')
                        hours = rule['do']['setHours']
                        exclude = set(rule['do'].get('excludeWeekdays', []))

                        current = start_date
                        while current <= end_date:
                            weekday_abbr = WEEKDAY_ABBRS[current.weekday()]
                            if weekday_abbr not in exclude:
                                expanded_rows.append({
                                    'allocation_id': allocation_id,
                                    'sourceBookingId': allocation_id,
                                    'sourceSystem': 'Polaris',
                                    'timeCode': time_code,
                                    'laborCode': labor_code,
                                    'usersUserId': user_id,
                                    'hours': hours,
                                    'workDate': current.strftime('%Y-%m-%d'),
                                    'hoursType': alloc['_hours_type'],
                                    'employeeId': alloc['_employee_id'],
                                    'lastUpdatedDate': alloc.get('lastModifiedTimestamp', ''),
                                })
                            current += timedelta(days=1)

                print(f"Expanded {len(allocations)} allocations into {len(expanded_rows)} daily rows")
                set_result(key="row_count", val=len(expanded_rows))
                return write_json_artifact(expanded_rows)

            expand_schedule_rules = PythonOperator(
                task_id="expand_schedule_rules",
                python_callable=expand_schedule_rules_callable,
            )

            # =================================================================
            # Step 3: Detect deltas using SHA256 hash comparison
            # =================================================================
            def detect_deltas_callable(dag_run):
                expanded_rows = load_json_artifact(result('expand_schedule_rules'))
                previous_reference = dag_run.conf.get('previous_reference', {})
                project_uri = dag_run.conf['project_uri']

                # Group rows by allocation_id
                allocations_map = {}
                for row in expanded_rows:
                    alloc_id = row['allocation_id']
                    if alloc_id not in allocations_map:
                        allocations_map[alloc_id] = []
                    allocations_map[alloc_id].append(row)

                # Compute SHA256 hash per allocation (deterministic: sorted by date)
                current_hashes = {}
                for alloc_id, rows in allocations_map.items():
                    hash_input = sorted(
                        [{"date": r['workDate'], "hours": r['hours']} for r in rows],
                        key=lambda x: x['date']
                    )
                    hash_str = json.dumps(hash_input, sort_keys=True)
                    current_hashes[alloc_id] = hashlib.sha256(hash_str.encode()).hexdigest()

                # Compare current vs previous reference
                to_insert = []
                to_delete = []

                for alloc_id, current_hash in current_hashes.items():
                    prev_hash = previous_reference.get(alloc_id)
                    if prev_hash is None:
                        # New allocation
                        to_insert.extend(allocations_map[alloc_id])
                    elif prev_hash != current_hash:
                        # Changed allocation — delete old rows, insert new
                        to_delete.append(alloc_id)
                        to_insert.extend(allocations_map[alloc_id])
                    # Unchanged allocations (hash matches) — skip

                # Deleted allocations (existed in previous but not in current)
                for alloc_id in previous_reference:
                    if alloc_id not in current_hashes:
                        to_delete.append(alloc_id)

                change_count = len(to_insert) + len(to_delete)
                set_result(key="change_count", val=change_count)
                set_result(key="insert_count", val=len(to_insert))
                set_result(key="delete_count", val=len(to_delete))

                print(f"Delta detection for {project_uri}: "
                      f"{len(to_insert)} rows to insert, "
                      f"{len(to_delete)} allocations to delete, "
                      f"{len(current_hashes)} total current allocations")

                return write_json_artifact({
                    'to_insert': to_insert,
                    'to_delete': to_delete,
                    'current_hashes': current_hashes,
                    'project_uri': project_uri
                })

            detect_deltas = PythonOperator(
                task_id="detect_deltas",
                python_callable=detect_deltas_callable,
            )

            has_changes = IfOperator(
                task_id="has_changes",
                test="{{ result('detect_deltas', 'change_count') > 0 }}",
                yes_task="prepare_api_payload",
                no_task="join_after_changes"
            )

            # =================================================================
            # Step 4: Prepare API payloads — PUT for changes, PATCH for deletes
            # =================================================================
            def prepare_api_payload_callable():
                delta_data = load_json_artifact(result('detect_deltas'))
                to_insert = delta_data['to_insert']
                to_delete = delta_data['to_delete']
                target_table = getattr(config, 'rp_api_target_table', None)

                # Group to_insert by allocation_id
                insert_by_alloc = {}
                for r in to_insert:
                    alloc_id = r['allocation_id']
                    if alloc_id not in insert_by_alloc:
                        insert_by_alloc[alloc_id] = []
                    row = {k: v for k, v in r.items() if k != 'allocation_id'}
                    insert_by_alloc[alloc_id].append(row)

                # Replacements (PUT): new or changed allocations — delete old rows + insert new
                replacements = []
                for alloc_id, rows in insert_by_alloc.items():
                    replacements.append({
                        "sourceBookingIdPrefix": alloc_id,
                        "sourceSystem": "Polaris",
                        "records": rows
                    })

                # Mark-deleted (PATCH): allocations removed entirely — set hours=0
                mark_deleted = []
                for alloc_id in to_delete:
                    if alloc_id not in insert_by_alloc:
                        mark_deleted.append({
                            "sourceBookingIdPrefix": alloc_id,
                            "sourceSystem": "Polaris"
                        })

                put_payload = {"replacements": replacements}
                patch_payload = {"markDeleted": mark_deleted}
                if target_table:
                    put_payload["targetTable"] = target_table
                    patch_payload["targetTable"] = target_table

                set_result(key="put_payload_json", val=json.dumps(put_payload))
                set_result(key="patch_payload_json", val=json.dumps(patch_payload))
                set_result(key="has_replacements", val=len(replacements) > 0)
                set_result(key="has_mark_deleted", val=len(mark_deleted) > 0)

                print(f"Prepared API payloads: {len(replacements)} replacements (PUT), {len(mark_deleted)} mark-deleted (PATCH)")

            prepare_api_payload = PythonOperator(
                task_id="prepare_api_payload",
                python_callable=prepare_api_payload_callable,
            )

            write_to_database = SimpleHttpOperator(
                task_id="write_to_database",
                method="PUT",
                http_conn_id=config.rp_api_conn_id,
                endpoint="/api/v1/rp/sourceAllocations",
                headers=_api_headers,
                data="{{ result('prepare_api_payload', 'put_payload_json') }}",
                response_check=lambda response: response.status_code == 200,
                log_response=True,
                extra_options={"verify": False},
            )

            mark_deleted_allocations = SimpleHttpOperator(
                task_id="mark_deleted_allocations",
                method="PATCH",
                http_conn_id=config.rp_api_conn_id,
                endpoint="/api/v1/rp/sourceAllocations",
                headers=_api_headers,
                data="{{ result('prepare_api_payload', 'patch_payload_json') }}",
                response_check=lambda response: response.status_code == 200,
                log_response=True,
                extra_options={"verify": False},
            )

            # =================================================================
            # Step 5: Publish current hashes as task result
            #         (master will collect from all children via XCom)
            # =================================================================
            def publish_hashes_callable():
                delta_data = load_json_artifact(result('detect_deltas'))
                current_hashes = delta_data['current_hashes']
                project_uri = delta_data['project_uri']

                print(f"Publishing {len(current_hashes)} hashes for project {project_uri}")
                return {
                    'project_uri': project_uri,
                    'current_hashes': current_hashes,
                }

            publish_hashes = PythonOperator(
                task_id="publish_hashes",
                python_callable=publish_hashes_callable,
            )

            # --- Failure logging to XCom (master gathers this) ---
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
                    "level":           "child",
                    "child_dag_id":    dag.dag_id if dag else "",
                    "child_run_id":    dag_run.run_id if dag_run else "",
                    "project_index":   conf.get("project_index"),
                    "project_uri":     conf.get("project_uri"),
                    "failed_task_ids": failed_task_ids,
                    "error_excerpt":   error_msg[:500],
                }

            log_failure = PythonOperator(
                task_id="log_failure",
                python_callable=log_failure_callable,
                trigger_rule="one_failed",
            )

            end_task = EmptyOperator(
                task_id="end_task",
                trigger_rule="all_done",
            )

            # =================================================================
            # Task Dependencies
            # =================================================================
            can_run_batch_task >> Label("Yes") >> batch_task >> end_task
            can_run_batch_task >> Label("No") >> fetch_allocations

            join_after_changes = EmptyOperator(task_id="join_after_changes", trigger_rule="none_failed_min_one_success")

            (fetch_allocations
                >> fetch_labor_codes
                >> expand_schedule_rules
                >> detect_deltas
                >> has_changes)
            has_changes >> Label("Yes") >> prepare_api_payload >> write_to_database >> mark_deleted_allocations >> join_after_changes
            has_changes >> Label("No") >> join_after_changes
            join_after_changes >> publish_hashes

            publish_hashes >> log_failure >> end_task

            _dags.append(dag)
    return _dags


for_each_instance(create_task_resource_allocation_export_child_dag)
