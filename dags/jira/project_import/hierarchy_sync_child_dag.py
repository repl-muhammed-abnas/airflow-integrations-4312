import json
import logging
from datetime import timedelta
import rail


null = None

def create_hierarchy_sync_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.hierarchy_sync_child_dag_id,
        description=f'Jira {config.region} Hierarchy Sync Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        multi_tenant=True
    ) as dag:

        def _is_feature_flag_on():
            """Returns True when wbsSyncSetting is present in dag_run.conf (feature flag enabled)."""
            try:
                conf_str = rail.render_template('{{ dag_run.conf | tojson }}')
                conf = json.loads(conf_str) if isinstance(conf_str, str) else conf_str
                wbs = (conf.get('customSettings') or {}).get('wbsSyncSetting') or {}
                return bool(wbs)
            except Exception as exc:
                logging.warning(f"_is_feature_flag_on: failed to parse dag_run.conf, defaulting to False: {exc}")
                return False

        def _is_mode_b():
            """Returns True when feature flag is on AND projectSyncSetting is not ['Project'] (issue-as-project mode)."""
            if not _is_feature_flag_on():
                return False
            try:
                conf_str = rail.render_template('{{ dag_run.conf | tojson }}')
                conf = json.loads(conf_str) if isinstance(conf_str, str) else conf_str
                wbs = (conf.get('customSettings') or {}).get('wbsSyncSetting') or {}
                project_sync = (wbs.get('mappings') or {}).get('projectSyncSetting', ['Project'])
                return 'Project' not in project_sync
            except Exception as exc:
                logging.warning(f"_is_mode_b: failed to parse dag_run.conf, defaulting to False: {exc}")
                return False

        # Parse UI mapping config from dag_run.conf
        def fetch_issue_mapping_config():
            ui_config = None
            try:
                conf_str = rail.render_template('{{ dag_run.conf | tojson }}')
                conf = json.loads(conf_str) if isinstance(conf_str, str) else conf_str
                custom_settings = conf.get('customSettings', {}) if conf else {}
                ui_config = custom_settings.get('wbsSyncSetting') if custom_settings else None
            except Exception as e:
                logging.exception(f"Failed to parse dag_run.conf: {e}")

            if not ui_config:
                return {
                    'mappings': [],
                    'sync_plan': {'level_1': [], 'level_2': [], 'level_3': []}
                }

            hierarchy_levels = ui_config.get('hierarchyLevels', [])

            replicon_level_to_hierarchy = config.replicon_level_to_hierarchy

            dag_mappings = []
            for level_info in hierarchy_levels:
                if not level_info.get('enabled', False):
                    continue
                replicon_level = level_info.get('repliconLevel', '')
                hierarchy_level = replicon_level_to_hierarchy.get(replicon_level)
                if hierarchy_level is None:
                    continue
                dag_mappings.append({
                    'jiraIssueTypeName': level_info.get('name', ''),
                    'enabled': True,
                    'repliconObjectType': replicon_level,
                    'repliconHierarchyLevel': hierarchy_level,
                    'jiraHierarchyLevel': level_info.get('hierarchyLevel')
                })

            sync_plan = {'level_1': [], 'level_2': [], 'level_3': []}
            for m in dag_mappings:
                key = f"level_{m['repliconHierarchyLevel']}"
                if key in sync_plan:
                    sync_plan[key].append(m)

            level_names = {1: 'Task', 2: 'SubTask-L1', 3: 'SubTask-L2'}
            for lvl in [1, 2, 3]:
                entries = sync_plan[f'level_{lvl}']
                if entries:
                    names = ', '.join(e['jiraIssueTypeName'] for e in entries)
                    logging.info(f" L{lvl} ({level_names[lvl]}): {names}")

            return {'mappings': dag_mappings, 'sync_plan': sync_plan}

        fetch_mapping_config = rail.PythonOperator(
            task_id='fetch_mapping_config',
            python_callable=fetch_issue_mapping_config
        )

        # Branch on whether any issue levels are configured
        def check_if_issue_sync_enabled():
            mapping_config = rail.result('fetch_mapping_config')
            sync_plan = mapping_config.get('sync_plan', {})
            has_issue_sync = any(
                len(sync_plan.get(f'level_{lvl}', [])) > 0 for lvl in [1, 2, 3]
            )
            return has_issue_sync

        is_issue_sync_enabled = rail.IfOperator(
            task_id='is_issue_sync_enabled',
            test=check_if_issue_sync_enabled,
            yes_task='get_replicon_project_list',
            no_task='finish'
        )

        def replicon_project_list_page_handler(request, result):
            if len(result.get('rows', [])) > 0:
                request['page'] += 1
                return request
            return None

        def replicon_project_list_data_handler(result):
            flat_rows = [row for page in result for row in page.get('rows', [])]
            projects = []
            for row in flat_rows:
                cells = row.get('cells', [])
                if len(cells) >= 2:
                    projects.append({
                        'uri': cells[0].get('uri'),
                        'name': cells[1].get('textValue'),
                        'code': cells[3].get('textValue') if len(cells) > 3 else None,
                    })
            return projects

        get_replicon_project_list = rail.RepliconServicePageOperator(
            task_id='get_replicon_project_list',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/ProjectListService1.svc/GetData',
            data={
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    "urn:replicon:project-list-column:name",
                    "urn:replicon:project-list-column:project-leader",
                    "urn:replicon:project-list-column:code"
                ]
            },
            page_handler=replicon_project_list_page_handler,
            all_result_data_handler=replicon_project_list_data_handler
        )

        def _get_jira_project_details(response):
            projects = []
            for project in response:
                projects.append({
                    'project_key': project.get('key'),
                    'project_id': project.get('id'),
                    'project_name': project.get('name'),
                    'project_leader': (project.get('lead') or {}).get('displayName'),
                    'project_leader_acc_id': (project.get('lead') or {}).get('accountId'),
                })
            return projects

        search_jira_project_list = rail.JiraAPIOperator(
            task_id='search_jira_project_list',
            request_method='GET',
            endpoint='/rest/api/3/project/search',
            query_params={'expand': 'description,lead'},
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
            data_handler=_get_jira_project_details
        )

        def _parse_mode_b_issues(response):
            return [
                {
                    'project_key': issue['fields']['project']['key'],
                    'project_id': issue['key'],
                    'project_name': issue['fields']['summary'],
                    'project_leader': (issue['fields'].get('assignee') or {}).get('displayName'),
                    'project_leader_acc_id': (issue['fields'].get('assignee') or {}).get('accountId'),
                }
                for issue in response
                if isinstance(issue, dict)
            ]

        def _build_mode_b_jql():
            conf_str = rail.render_template('{{ dag_run.conf | tojson }}')
            conf = json.loads(conf_str) if isinstance(conf_str, str) else conf_str
            wbs = (conf.get('customSettings') or {}).get('wbsSyncSetting') or {}
            hierarchy_levels = wbs.get('hierarchyLevels', [])
            project_level = next(
                (lvl for lvl in hierarchy_levels
                 if lvl.get('repliconLevel') == 'project' and lvl.get('enabled')), None
            )
            if not project_level:
                return 'issueKey is EMPTY'
            name = project_level['name']
            jira_hierarchy_level = project_level.get('hierarchyLevel')
            if jira_hierarchy_level == -1:
                type_clause = 'issueType in subTaskIssueTypes()'
            else:
                type_clause = f'issuetype = "{name}"'
            return f'{type_clause} ORDER BY updated ASC'

        search_jira_issue_projects = rail.JiraAPIOperator(
            task_id='search_jira_issue_projects',
            request_method='GET',
            endpoint='/rest/api/3/search/jql',
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
            query_params=lambda: {
                'jql': _build_mode_b_jql(),
                'fields': 'key,summary,assignee,project'
            },
            data_handler=_parse_mode_b_issues
        )

        # Helper: builds a factored JQL for all projects x issue types at a given level.
        # Uses `project in (...)` and `issuetype in (...)` instead of an OR-expanded
        # cartesian product to keep the JQL string short (avoids HTTP 414 / CloudFront
        # URI-too-long when there are many projects).
        def build_combined_jql_for_level(level):
            mapping_config = rail.result('fetch_mapping_config')
            sync_plan = mapping_config.get('sync_plan', {})
            issue_type_mappings = sync_plan.get(f'level_{level}', [])
            if not issue_type_mappings:
                return None

            conf_str = rail.render_template('{{ dag_run.conf | tojson }}')
            conf = json.loads(conf_str) if isinstance(conf_str, str) else conf_str
            jira_project_list = rail.result('search_jira_project_list') or []
            new_project_keys = set(conf.get('new_project_keys', []))
            last_sync_time = conf.get('last_synctime')

            named_types = []
            seen_names = set()
            has_subtask = False
            for mapping in issue_type_mappings:
                if mapping.get('jiraHierarchyLevel') == -1:
                    has_subtask = True
                    continue
                name = mapping.get('jiraIssueTypeName')
                if name and name not in seen_names:
                    seen_names.add(name)
                    named_types.append(name)

            type_clauses = []
            if named_types:
                quoted = ', '.join(f'"{n}"' for n in named_types)
                type_clauses.append(f'issuetype in ({quoted})')
            if has_subtask:
                type_clauses.append('issueType in subTaskIssueTypes()')
            if not type_clauses:
                return None
            type_clause = type_clauses[0] if len(type_clauses) == 1 else '(' + ' OR '.join(type_clauses) + ')'

            new_projects = []
            existing_projects = []
            seen_projects = set()
            for jira_proj in jira_project_list:
                proj_key = jira_proj.get('project_key')
                if not proj_key or proj_key in seen_projects:
                    continue
                seen_projects.add(proj_key)
                if proj_key in new_project_keys or not last_sync_time:
                    new_projects.append(proj_key)
                else:
                    existing_projects.append(proj_key)

            project_clauses = []
            if new_projects:
                quoted = ', '.join(f'"{p}"' for p in new_projects)
                project_clauses.append(f'project in ({quoted})')
            if existing_projects:
                quoted = ', '.join(f'"{p}"' for p in existing_projects)
                sync_ts = str(last_sync_time)[:16].replace('T', ' ')
                project_clauses.append(f'(project in ({quoted}) AND updated >= "{sync_ts}")')

            if not project_clauses:
                return None
            project_clause = project_clauses[0] if len(project_clauses) == 1 else '(' + ' OR '.join(project_clauses) + ')'

            return f'{type_clause} AND {project_clause} ORDER BY updated ASC'

        # Helper: annotates fetched Jira issues with mapping metadata for a given level
        def annotate_issues_for_level(issues, level):
            mapping_config = rail.result('fetch_mapping_config')
            sync_plan = mapping_config.get('sync_plan', {})
            issue_type_mappings = sync_plan.get(f'level_{level}', [])

            type_name_to_mapping = {m['jiraIssueTypeName']: m for m in issue_type_mappings}

            annotated = []
            for issue in (issues or []):
                fields = issue.get('fields', {})
                issuetype = fields.get('issuetype', {})
                issue_type_name = issuetype.get('name', '')
                proj_key = fields.get('project', {}).get('key')

                mapping = type_name_to_mapping.get(issue_type_name)
                if not mapping:
                    continue

                issue['_mapping'] = mapping
                issue['_replicon_level'] = level
                issue['_jira_project_key'] = proj_key
                annotated.append(issue)

            return annotated

        # Fetch issues per level using JiraAPIOperator (handles OAuth2 + pagination automatically).
        # POST is used so the JQL travels in the request body and is not subject to URL length
        # limits (CloudFront returns HTTP 414 on long URIs for tenants with many projects).
        issue_fields = ['key', 'summary', 'issuetype', 'status', 'project', 'parent', 'created', 'updated']

        fetch_level_1_issues = rail.JiraAPIOperator(
            task_id='fetch_level_1_issues',
            request_method='POST',
            endpoint='/rest/api/3/search/jql',
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
            request_body=lambda: {
                'jql': build_combined_jql_for_level(1),
                'fields': issue_fields,
            },
            data_handler=lambda response: annotate_issues_for_level(response, 1)
        )

        fetch_level_2_issues = rail.JiraAPIOperator(
            task_id='fetch_level_2_issues',
            request_method='POST',
            endpoint='/rest/api/3/search/jql',
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
            request_body=lambda: {
                'jql': build_combined_jql_for_level(2),
                'fields': issue_fields,
            },
            data_handler=lambda response: annotate_issues_for_level(response, 2)
        )

        fetch_level_3_issues = rail.JiraAPIOperator(
            task_id='fetch_level_3_issues',
            request_method='POST',
            endpoint='/rest/api/3/search/jql',
            jira_conn_id='{{ dag_run.conf.jira_conn_id }}',
            request_body=lambda: {
                'jql': build_combined_jql_for_level(3),
                'fields': issue_fields,
            },
            data_handler=lambda response: annotate_issues_for_level(response, 3)
        )

        # Pre-fetch all existing Replicon tasks for all relevant projects (shared across all levels)
        # Replaces per-issue _lookup_existing_task_uri RepliconHook calls
        def build_prefetch_task_data():
            fresh_replicon_projects = rail.result('get_replicon_project_list')
            conf_str = rail.render_template('{{ dag_run.conf | tojson }}')
            conf = json.loads(conf_str) if isinstance(conf_str, str) else conf_str
            if _is_mode_b():
                jira_issue_project_list = rail.result('search_jira_issue_projects') or []
                replicon_code_to_uri = {p['code']: p['uri'] for p in fresh_replicon_projects if p.get('code')}
                project_uris = []
                for issue in jira_issue_project_list:
                    issue_key = issue.get('project_id')
                    uri = replicon_code_to_uri.get(issue_key)
                    if uri and uri not in project_uris:
                        project_uris.append(uri)
            else:
                jira_project_list = rail.result('search_jira_project_list') or []
                replicon_name_to_uri = {p['name']: p['uri'] for p in fresh_replicon_projects}
                project_uris = []
                for jira_proj in jira_project_list:
                    proj_name = jira_proj.get('project_name')
                    uri = replicon_name_to_uri.get(proj_name)
                    if uri and uri not in project_uris:
                        project_uris.append(uri)

            return {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    "urn:replicon:task-list-column:task",
                    "urn:replicon:task-list-column:code"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:task-list-filter:project"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uris": project_uris
                        }
                    }
                }
            }

        def prefetch_task_page_handler(request, result):
            if len(result.get('rows', [])) > 0:
                request['page'] += 1
                return request
            return None

        def prefetch_task_data_handler(result):
            # Build lookup dict: task code → uri, task name → uri (mirrors _lookup_existing_task_uri logic)
            task_lookup = {}
            flat_rows = [row for page in result for row in page.get('rows', [])]
            for row in flat_rows:
                cells = row.get('cells', [])
                if len(cells) >= 2:
                    task_uri = cells[0].get('uri')
                    task_name = cells[0].get('textValue')
                    task_code = cells[1].get('textValue')
                    if task_code and task_uri:
                        task_lookup[task_code] = task_uri
                    if task_name and task_uri and task_name not in task_lookup:
                        task_lookup[task_name] = task_uri
            return task_lookup

        prefetch_existing_tasks = rail.RepliconServicePageOperator(
            task_id='prefetch_existing_tasks',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/TaskListService1.svc/GetData',
            data=build_prefetch_task_data,
            page_handler=prefetch_task_page_handler,
            all_result_data_handler=prefetch_task_data_handler
        )

        # Derives the unique Jira project keys present at this level by inspecting put_items.
        # Returns {project_key: representative_issue_key} — one issue per project, sufficient
        # because permission/search returns project-scoped permissions.
        def get_project_to_issue_map_for_level(level):
            put_items = (rail.result(f'prepare_level_{level}_payloads') or {}).get('put_items', [])
            project_to_issue = {}
            for item in put_items:
                issue_key = item['issue_key']
                proj_key = issue_key.split('-', 1)[0]
                if proj_key and proj_key not in project_to_issue:
                    project_to_issue[proj_key] = issue_key
            return project_to_issue

        # Extracts active Atlassian user emails from a permission/search response list.
        def extract_level_member_emails(response) -> list:
            seen = set()
            emails = []
            for item in (response or []):
                if item is None:
                    continue
                # Production RAIL appends each page's values list wholesale,
                # so item may be a list (page) or a single user dict.
                users = item if isinstance(item, list) else [item]
                for user in users:
                    if (
                        user.get('accountType') == 'atlassian'
                        and user.get('active') is True
                        and (user.get('emailAddress') or '').strip()
                    ):
                        email = user['emailAddress'].strip()
                        if email not in seen:
                            seen.add(email)
                            emails.append(email)
            return emails

        # Fetches members for every unique project at the level by invoking JiraAPIOperator
        # once per project (one representative issue key per project).
        # Returns {project_key: [email, ...]}.
        def fetch_members_for_level(level, **context):
            project_to_issue = get_project_to_issue_map_for_level(level)
            if not project_to_issue:
                return {}

            conf_str = rail.render_template('{{ dag_run.conf | tojson }}')
            conf = json.loads(conf_str) if isinstance(conf_str, str) else conf_str
            jira_conn_id = conf.get('jira_conn_id')

            result = {}
            for proj_key, issue_key in project_to_issue.items():
                op = rail.JiraAPIOperator(
                    task_id=f'_inline_fetch_members_l{level}_{proj_key}',
                    request_method='GET',
                    endpoint='/rest/api/3/user/permission/search',
                    query_params={
                        'issueKey': issue_key,
                        'permissions': 'CREATE_ISSUES,EDIT_ISSUES,WORK_ON_ISSUES'
                    },
                    jira_conn_id=jira_conn_id,
                    data_handler=extract_level_member_emails,
                    dag=None
                )
                try:
                    emails = op.execute(context) or []
                except Exception as e:
                    logging.warning(f"permission/search for project {proj_key} (issue {issue_key}) failed: {e}")
                    emails = []
                result[proj_key] = emails
            return result

        # Builds BulkGetUsers2 request payload from the union of all per-project email lists.
        def get_resolve_resources_data_for_level(level):
            project_to_emails = rail.result(f'fetch_members_level_{level}') or {}
            seen = set()
            emails = []
            for email_list in project_to_emails.values():
                for email in email_list:
                    if email not in seen:
                        seen.add(email)
                        emails.append(email)
            return {'users': [{'loginName': e} for e in emails]}

        # Builds {email: uri} lookup from BulkGetUsers2 response (preserves submission order).
        def extract_email_uri_map_for_level(level, response):
            project_to_emails = rail.result(f'fetch_members_level_{level}') or {}
            seen = set()
            emails = []
            for email_list in project_to_emails.values():
                for email in email_list:
                    if email not in seen:
                        seen.add(email)
                        emails.append(email)
            email_to_uri = {}
            unresolved = 0
            for email, user in zip(emails, response or []):
                if user and user.get('uri'):
                    email_to_uri[email] = user['uri']
                else:
                    unresolved += 1
            if unresolved:
                logging.warning(f"{unresolved} user(s) at level {level} could not be resolved to a Replicon URI and will be skipped.")
            return email_to_uri

        # Injects per-project assignedResources into each put_item based on its issue's project key.
        def finalize_payloads_for_level(level):
            prepare_result = rail.result(f'prepare_level_{level}_payloads') or {}
            put_items = prepare_result.get('put_items', [])
            project_to_emails = rail.result(f'fetch_members_level_{level}') or {}
            email_to_uri = rail.result(f'resolve_resources_level_{level}') or {}

            for item in put_items:
                issue_key = item['issue_key']
                proj_key = issue_key.split('-', 1)[0]
                emails = project_to_emails.get(proj_key, [])
                user_uris = [email_to_uri[e] for e in emails if e in email_to_uri]
                item['payload']['task']['assignedResources'] = (
                    [
                        {
                            "uri": null,
                            "resourcePlaceholderParameterCorrelationId": null,
                            "user": {"uri": uri},
                            "department": null,
                            "placeholder": null, "location": null, "division": null,
                            "costCenter": null, "serviceCenter": null,
                            "departmentGroup": null, "employeeTypeGroup": null
                        }
                        for uri in user_uris
                    ] or [{
                        "uri": null,
                        "resourcePlaceholderParameterCorrelationId": null,
                        "user": null,
                        "department": {
                            "uri": null, "name": "Company",
                            "parent": null, "parameterCorrelationId": null
                        },
                        "placeholder": null, "location": null, "division": null,
                        "costCenter": null, "serviceCenter": null,
                        "departmentGroup": null, "employeeTypeGroup": null
                    }]
                )

            return {'skip_map': prepare_result.get('skip_map', {}), 'put_items': put_items}

        # Helper: builds replicon_project_map from conf + pre-fetched project list.
        # Mode A: {container_key: project_uri} matched by name.
        # Mode B: {issue_key: project_uri} matched by code (issue key = project code).
        def build_replicon_project_map():
            conf_str = rail.render_template('{{ dag_run.conf | tojson }}')
            conf = json.loads(conf_str) if isinstance(conf_str, str) else conf_str
            fresh_replicon_projects = rail.result('get_replicon_project_list')
            if _is_mode_b():
                jira_issue_project_list = rail.result('search_jira_issue_projects') or []
                replicon_code_to_uri = {p['code']: p['uri'] for p in fresh_replicon_projects if p.get('code')}
                replicon_project_map = {}
                for jira_issue in jira_issue_project_list:
                    issue_key = jira_issue.get('project_id')
                    uri = replicon_code_to_uri.get(issue_key)
                    if issue_key and uri:
                        replicon_project_map[issue_key] = uri
                return replicon_project_map
            jira_project_list = rail.result('search_jira_project_list') or []
            replicon_name_to_uri = {p['name']: p['uri'] for p in fresh_replicon_projects}
            replicon_project_map = {}
            for jira_proj in jira_project_list:
                proj_key = jira_proj.get('project_key')
                proj_name = jira_proj.get('project_name')
                uri = replicon_name_to_uri.get(proj_name)
                if proj_key and uri:
                    replicon_project_map[proj_key] = uri
            return replicon_project_map

        # Helper: splits issues into skip_map (no action) and put_items (need PutTask)
        # Uses pre-fetched task lookup dict — no RepliconHook calls needed
        def prepare_payloads_for_level(level):
            issues = rail.result(f'fetch_level_{level}_issues') or []
            if not issues:
                return {'skip_map': {}, 'put_items': []}

            existing_task_lookup = rail.result('prefetch_existing_tasks') or {}
            replicon_project_map = build_replicon_project_map()

            parent_task_map = {}
            if level == 2:
                parent_task_map = (rail.result('sync_level_1_to_replicon') or {}).get('synced_task_map', {})
            elif level == 3:
                parent_task_map = (rail.result('sync_level_2_to_replicon') or {}).get('synced_task_map', {})

            skip_map = {}
            put_items = []

            for issue in issues:
                issue_key = issue['key']
                fields = issue.get('fields', {})
                proj_key = fields.get('project', {}).get('key')
                project_uri = replicon_project_map.get(proj_key, '')

                parent_uri = None

                if level == 1:
                    if _is_mode_b():
                        # Mode B: the Replicon project comes from the parent issue, not the container
                        jira_parent = fields.get('parent', {})
                        parent_issue_key = jira_parent.get('key') if jira_parent else None
                        project_uri = replicon_project_map.get(parent_issue_key, '') if parent_issue_key else ''
                    if not project_uri:
                        continue  # skip: no matching Replicon project

                elif level in (2, 3):
                    jira_parent = fields.get('parent', {})
                    jira_parent_key = jira_parent.get('key') if jira_parent else None
                    if jira_parent_key:
                        parent_uri = parent_task_map.get(jira_parent_key)
                    if not parent_uri and jira_parent_key:
                        parent_uri = existing_task_lookup.get(jira_parent_key)
                    if not parent_uri:
                        continue  # skip: parent not found in Replicon

                existing_uri = existing_task_lookup.get(issue_key)

                # Skip if already exists and doesn't need re-parenting
                if existing_uri and (level == 1 or not parent_uri):
                    skip_map[issue_key] = existing_uri
                    continue

                created_date = (fields.get('created') or '')[:10]
                created_parts = created_date.split('-') if created_date else ['1970', '01', '01']
                target_uri = existing_uri if existing_uri else null

                put_items.append({
                    'issue_key': issue_key,
                    'existing_uri': existing_uri,
                    'payload': {
                        "project": {"uri": project_uri, "name": null, "parameterCorrelationId": null},
                        "task": {
                            "target": {
                                "uri": target_uri,
                                "name": issue_key,
                                "parent": {"uri": parent_uri} if parent_uri else null,
                                "parameterCorrelationId": null
                            },
                            "name": issue_key,
                            "code": issue_key,
                            "description": null,
                            "timeEntryDateRange": {
                                "startDate": {
                                    "year": created_parts[0],
                                    "month": created_parts[1],
                                    "day": created_parts[2]
                                },
                                "endDate": null,
                                "relativeDateRangeUri": null,
                                "relativeDateRangeAsOfDate": null
                            },
                            "percentCompleted": "0",
                            "isTimeEntryAllowed": "1",
                            "estimatedHours": null,
                            "isClosed": "0",
                            "customFieldValues": [],
                            "extensionFieldValues": [],
                            "estimatedCost": null,
                            "costTypeUri": null,
                            "timeAndExpenseEntryTypeUri": null,
                            "assignedResources": []
                        }
                    }
                })

            return {'skip_map': skip_map, 'put_items': put_items}

        # Helper: aggregates PutTask execution results into the synced_task_map return structure
        def aggregate_sync_results_for_level(level):
            finalize_result = rail.result(f'finalize_level_{level}_payloads') or {}
            skip_map = finalize_result.get('skip_map', {})
            put_items = finalize_result.get('put_items', [])
            execute_results = rail.result(f'execute_level_{level}_put_tasks') or []

            synced_task_map = dict(skip_map)
            synced = 0
            failed = 0
            skipped = len(skip_map)

            for item, response in zip(put_items, execute_results):
                issue_key = item['issue_key']
                existing_uri = item.get('existing_uri')
                if response is None:
                    logging.warning(f" {issue_key}: FAILED")
                    failed += 1
                    continue
                task_uri = (
                    response.get('d', {}).get('uri') or
                    response.get('uri') or
                    response.get('d', {}).get('task', {}).get('uri')
                )
                synced_task_map[issue_key] = task_uri or existing_uri
                action = "re-parented" if existing_uri else "created"
                logging.info(f" {issue_key}: {action}")
                synced += 1

            logging.info(f" Level {level} sync complete — synced: {synced}, skipped: {skipped}, failed: {failed}")
            return {
                'synced': synced, 'failed': failed,
                'skipped': skipped, 'total': len(put_items) + skipped,
                'synced_task_map': synced_task_map
            }

        def has_level_1_issues():
            return len(rail.result('fetch_mapping_config').get('sync_plan', {}).get('level_1', [])) > 0

        is_level_1_enabled = rail.IfOperator(
            task_id='is_level_1_enabled',
            test=has_level_1_issues,
            yes_task='fetch_level_1_issues',
            no_task='is_level_2_enabled'
        )

        prepare_level_1_payloads = rail.PythonOperator(
            task_id='prepare_level_1_payloads',
            python_callable=lambda: prepare_payloads_for_level(1)
        )

        fetch_members_level_1 = rail.PythonOperator(
            task_id='fetch_members_level_1',
            python_callable=fetch_members_for_level,
            op_kwargs={'level': 1}
        )

        resolve_resources_level_1 = rail.RepliconServiceOperator(
            task_id='resolve_resources_level_1',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/UserService1.svc/BulkGetUsers2',
            data=lambda: get_resolve_resources_data_for_level(1),
            data_handler=lambda response: extract_email_uri_map_for_level(1, response)
        )

        finalize_level_1_payloads = rail.PythonOperator(
            task_id='finalize_level_1_payloads',
            python_callable=lambda: finalize_payloads_for_level(1)
        )

        execute_level_1_put_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='execute_level_1_put_tasks',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/ProjectService1.svc/PutTask',
            items=lambda: rail.result('finalize_level_1_payloads')['put_items'],
            flatten=True,
            data=lambda item: item['payload'],
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        sync_level_1 = rail.PythonOperator(
            task_id='sync_level_1_to_replicon',
            python_callable=lambda: aggregate_sync_results_for_level(1)
        )

        def has_level_2_issues():
            return len(rail.result('fetch_mapping_config').get('sync_plan', {}).get('level_2', [])) > 0

        is_level_2_enabled = rail.IfOperator(
            task_id='is_level_2_enabled',
            test=has_level_2_issues,
            yes_task='fetch_level_2_issues',
            no_task='is_level_3_enabled'
        )

        prepare_level_2_payloads = rail.PythonOperator(
            task_id='prepare_level_2_payloads',
            python_callable=lambda: prepare_payloads_for_level(2)
        )

        fetch_members_level_2 = rail.PythonOperator(
            task_id='fetch_members_level_2',
            python_callable=fetch_members_for_level,
            op_kwargs={'level': 2}
        )

        resolve_resources_level_2 = rail.RepliconServiceOperator(
            task_id='resolve_resources_level_2',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/UserService1.svc/BulkGetUsers2',
            data=lambda: get_resolve_resources_data_for_level(2),
            data_handler=lambda response: extract_email_uri_map_for_level(2, response)
        )

        finalize_level_2_payloads = rail.PythonOperator(
            task_id='finalize_level_2_payloads',
            python_callable=lambda: finalize_payloads_for_level(2)
        )

        execute_level_2_put_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='execute_level_2_put_tasks',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/ProjectService1.svc/PutTask',
            items=lambda: rail.result('finalize_level_2_payloads')['put_items'],
            flatten=True,
            data=lambda item: item['payload'],
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        sync_level_2 = rail.PythonOperator(
            task_id='sync_level_2_to_replicon',
            python_callable=lambda: aggregate_sync_results_for_level(2)
        )

        def has_level_3_issues():
            return len(rail.result('fetch_mapping_config').get('sync_plan', {}).get('level_3', [])) > 0

        is_level_3_enabled = rail.IfOperator(
            task_id='is_level_3_enabled',
            test=has_level_3_issues,
            yes_task='fetch_level_3_issues',
            no_task='finish'
        )

        prepare_level_3_payloads = rail.PythonOperator(
            task_id='prepare_level_3_payloads',
            python_callable=lambda: prepare_payloads_for_level(3)
        )

        fetch_members_level_3 = rail.PythonOperator(
            task_id='fetch_members_level_3',
            python_callable=fetch_members_for_level,
            op_kwargs={'level': 3}
        )

        resolve_resources_level_3 = rail.RepliconServiceOperator(
            task_id='resolve_resources_level_3',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/UserService1.svc/BulkGetUsers2',
            data=lambda: get_resolve_resources_data_for_level(3),
            data_handler=lambda response: extract_email_uri_map_for_level(3, response)
        )

        finalize_level_3_payloads = rail.PythonOperator(
            task_id='finalize_level_3_payloads',
            python_callable=lambda: finalize_payloads_for_level(3)
        )

        execute_level_3_put_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id='execute_level_3_put_tasks',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/ProjectService1.svc/PutTask',
            items=lambda: rail.result('finalize_level_3_payloads')['put_items'],
            flatten=True,
            data=lambda item: item['payload'],
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        sync_level_3 = rail.PythonOperator(
            task_id='sync_level_3_to_replicon',
            python_callable=lambda: aggregate_sync_results_for_level(3)
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        fetch_mapping_config >> is_issue_sync_enabled

        is_issue_sync_enabled >> rail.Label('Yes') >> get_replicon_project_list >> search_jira_project_list >> search_jira_issue_projects >> prefetch_existing_tasks >> is_level_1_enabled
        is_issue_sync_enabled >> rail.Label('No') >> finish

        is_level_1_enabled >> rail.Label('Yes') >> fetch_level_1_issues >> prepare_level_1_payloads >> fetch_members_level_1 >> resolve_resources_level_1 >> finalize_level_1_payloads >> execute_level_1_put_tasks >> sync_level_1 >> is_level_2_enabled
        is_level_1_enabled >> rail.Label('No') >> is_level_2_enabled

        is_level_2_enabled >> rail.Label('Yes') >> fetch_level_2_issues >> prepare_level_2_payloads >> fetch_members_level_2 >> resolve_resources_level_2 >> finalize_level_2_payloads >> execute_level_2_put_tasks >> sync_level_2 >> is_level_3_enabled
        is_level_2_enabled >> rail.Label('No') >> is_level_3_enabled

        is_level_3_enabled >> rail.Label('Yes') >> fetch_level_3_issues >> prepare_level_3_payloads >> fetch_members_level_3 >> resolve_resources_level_3 >> finalize_level_3_payloads >> execute_level_3_put_tasks >> sync_level_3 >> finish
        is_level_3_enabled >> rail.Label('No') >> finish

    return dag


rail.for_each_instance(create_hierarchy_sync_child_dag)
