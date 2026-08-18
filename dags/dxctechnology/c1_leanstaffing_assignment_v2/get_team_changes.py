import datetime
from datetime import timedelta
import pytz
from airflow.models import Variable
import rail

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_leanstaffing_assignment_v2/config.py


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.export_get_team_changes_child_dag_id,
        description=f'DXC C1 Leanstaff Assignment - Get Team Changes {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime.datetime(2022, 1, 1),
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='parse_projects'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='parse_projects',
            end_task='finish',
        )

        # OPTIMIZATION: Handle batch of projects instead of single project
        def get_projects_from_conf(dag_run):
            """Extract projects from conf - handles both single and batch mode"""
            if 'projects' in dag_run.conf:
                # Batch mode - new optimized approach
                import json
                projects = json.loads(dag_run.conf['projects']) if isinstance(dag_run.conf['projects'], str) else dag_run.conf['projects']
                return projects
            else:
                # Single project mode - backward compatibility
                return [{'project_uri': dag_run.conf['project_uri']}]

        parse_projects = rail.PythonOperator(
            task_id='parse_projects',
            python_callable=get_projects_from_conf
        )

        # OPTIMIZATION: Process multiple projects in parallel
        get_project_team_change_summary2 = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_project_team_change_summary2',
            endpoint='/services/ProjectService1.svc/GetProjectTeamChangeSummary2',
            items="{{ result('parse_projects') | tojson }}",  # FIX: Convert to JSON string
            execution_timeout=datetime.timedelta(hours=2),
            data={
                "projectUri": "{{ item.project_uri }}",
                "startTimestampUtc": {
                    "year": "{{ macros.datetime.fromisoformat(dag_run.conf.get_changes_since).year }}",
                    "month": "{{ macros.datetime.fromisoformat(dag_run.conf.get_changes_since).month }}",
                    "day": "{{ macros.datetime.fromisoformat(dag_run.conf.get_changes_since).day }}",
                    "hour": "{{ macros.datetime.fromisoformat(dag_run.conf.get_changes_since).hour }}",
                    "minute": "{{ macros.datetime.fromisoformat(dag_run.conf.get_changes_since).minute }}",
                    "second": "{{ macros.datetime.fromisoformat(dag_run.conf.get_changes_since).second }}",
                    "millisecond": "0",
                }
            },
        )

        # OPTIMIZATION: Process batch results for added and removed users

        def aggregate_added_users():
            """Aggregate added users from all projects in the batch"""
            all_users = []
            results = rail.result('get_project_team_change_summary2')
            projects = rail.result('parse_projects')

            # Handle both single result and batch results
            if not isinstance(results, list):
                results = [results]

            for idx, result in enumerate(results):
                project_uri = projects[idx]['project_uri'] if idx < len(projects) else None
                if project_uri and 'teamMembersAdded' in result:
                    for user in result['teamMembersAdded']:
                        if user.get('resource', {}).get('user', {}).get('uri'):
                            all_users.append({
                                'user_uri': user['resource']['user']['uri'],
                                'project_uri': project_uri
                            })
            return all_users

        added_users = rail.PythonOperator(
            task_id='added_users',
            python_callable=aggregate_added_users
        )

        def aggregate_removed_users():
            """Aggregate removed users from all projects in the batch"""
            all_users = []
            results = rail.result('get_project_team_change_summary2')
            projects = rail.result('parse_projects')

            # Handle both single result and batch results
            if not isinstance(results, list):
                results = [results]

            for idx, result in enumerate(results):
                project_uri = projects[idx]['project_uri'] if idx < len(projects) else None
                if project_uri and 'teamMembersRemoved' in result:
                    for user in result['teamMembersRemoved']:
                        if user.get('resource', {}).get('user'):
                            all_users.append({
                                'user_uri': user['resource']['user']['uri'],
                                'project_uri': project_uri,
                                'assignmentDateRange': user.get('assignmentDateRange')
                            })
            return all_users

        removed_users = rail.PythonOperator(
            task_id='removed_users',
            python_callable=aggregate_removed_users
        )

        removed_users_has_data = rail.IfOperator(
            task_id="removed_users_has_data",
            test="{{ result('removed_users') | length > 0 }}",
            yes_task='get_billing_rate_assignment_revision'
        )

        # OPTIMIZATION: Get billing rate revisions for all projects in batch
        def get_billing_rate_data(dag_run):
            """Build GraphQL query for billing rate revisions"""
            return {
                "query": '''
                query GetPageOfProjectTeamMemberBillingRateAssignmentRevision($cursor: String, $limit: Int!, $filter: ProjectTeamMemberBillingRateAssignmentFilter) {
                projectTeamMemberBillingRateAssignmentRevisions(limit: $limit, cursor: $cursor, filter: $filter) {
                items {
                    timestampUtc
                    actualUser {
                    uri
                    displayText
                    }
                    clientIp
                    project {
                    uri
                    displayText
                    }
                    user {
                    uri
                    displayText
                    }
                    currentBillingRate {
                    uri
                    displayText
                    }
                    previousBillingRate {
                    uri
                    displayText
                    }
                }
                cursor
                }
            }''',
                "variables": {
                    "limit": 100,  # Increased limit for batch processing
                    "cursor": "",
                    "filter": {
                        "projectUris": [p['project_uri'] for p in rail.result('parse_projects')],
                        "dateRange": {
                            "startDate": dag_run.conf['get_changes_since'],
                            "endDate": datetime.datetime.now(pytz.timezone("Etc/UTC")).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                        }
                    }
                }
            }

        get_billing_rate_assignment_revision = rail.RepliconServiceOperator(
            task_id='get_billing_rate_assignment_revision',
            endpoint='/audittrail-api/graphql',
            data=get_billing_rate_data,
        )

        # OPTIMIZATION: Bulk fetch all projects in the batch
        bulk_get_projects3 = rail.RepliconServiceOperator(
            task_id='bulk_get_projects3',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=lambda: {
                "projects": [{"uri": p['project_uri']} for p in rail.result('parse_projects')]
            },
        )

        # OPTIMIZATION: Bulk fetch all unique users across all projects
        bulk_get_users = rail.RepliconServiceOperator(
            task_id='bulk_get_users',
            endpoint='/services/UserService1.svc/BulkGetUserDetails',
            data=lambda: {
                "userUri": list(set(x['user_uri'] for x in rail.result('removed_users')))  # Use set to avoid duplicates
            },
        )

        get_effective_user_groups = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_effective_user_groups',
            endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
            items=lambda: rail.result('removed_users'),
            execution_timeout=datetime.timedelta(days=14),
            data=lambda item: {'userUri': item['user_uri']}
        )

        def convert_data_to_target_format(item):
            if not item:
                return None

            # OPTIMIZATION: Find the correct project details from batch results
            all_projects = rail.result('bulk_get_projects3')
            project_details = None
            for proj in all_projects:
                if proj['projectDetails']['uri'] == item['project_uri']:
                    project_details = proj['projectDetails']
                    break

            if not project_details:
                return None

            employeeid = rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users'),
                                                              'uri', item['user_uri'], 'employeeId')
            username = rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users'),
                                                            'uri', item['user_uri'], 'displayText')
            usercompanycode = rail.find_first_by_attr_and_get_attr(rail.result('get_effective_user_groups'),
                                                                   'userUri', item['user_uri'], 'divisions')[0]['division']['division']['displayText']

            def format_date(date):
                if not date:
                    return None

                # 27 September 2021
                return datetime.datetime(
                    **{'day': date['day'], 'month': date['month'], 'year': date['year']}).strftime('%d %B %Y')

            def get_project_oef_value(oef_name, attr):
                return rail.find_first_by_attr_and_get_attr(project_details['extensionFieldValues'],
                                                            'definition.displayText', oef_name, attr)

            project_details['assignmentstartdate'] = format_date(
                item['assignmentDateRange']['startDate'])
            project_details['assignmentenddate'] = format_date(
                item['assignmentDateRange']['endDate'])
            project_details['projectstartdate'] = format_date(
                project_details['timeEntryDateRange']['startDate'])
            project_details['projectenddate'] = format_date(
                project_details['timeEntryDateRange']['endDate'])

            # OPTIMIZATION: Find billing rate for this specific project and user
            billing_rate_items = rail.result('get_billing_rate_assignment_revision').get(
                'data', {}).get('projectTeamMemberBillingRateAssignmentRevisions', {}).get('items', [])

            billing_rate = None
            for br_item in billing_rate_items:
                if br_item.get('project', {}).get('uri') == item['project_uri'] and \
                   br_item.get('user', {}).get('uri') == item['user_uri']:
                    billing_rate = br_item.get('currentBillingRate') or br_item.get('previousBillingRate')
                    break
            project_details['labortype'] = billing_rate['displayText'] if billing_rate else ''

            return {
                'projectname': project_details['name'],
                'masterwbs': get_project_oef_value('Master WBS (SO, WO)', 'textValue'),
                'internalsapobjectid': get_project_oef_value('WBS internal object number', 'textValue'),
                'employeeid': employeeid,
                'username': username,
                'labortype': project_details['labortype'],
                'currency': project_details['defaultBillingCurrency']['displayText'],
                'datechangeflag': None,
                'projecturi': project_details['uri'],
                'companycode': None,
                'projectstartdate': project_details['projectstartdate'],
                'projectenddate': project_details['projectenddate'],
                'projectype': None,
                'taskassignment_billingratechangedate': None,
                'useruri': item['user_uri'],
                'usercompanycode': usercompanycode,
                'assignmentstartdate': project_details['assignmentstartdate'],
                'assignmentenddate': project_details['assignmentenddate'],
            }

        def do_map_removed_users():
            return list(map(convert_data_to_target_format,
                        rail.result('removed_users')))

        map_removed_users = rail.PythonOperator(
            task_id='map_removed_users',
            python_callable=do_map_removed_users
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> parse_projects

        # SERIALIZED EXECUTION: All tasks run sequentially to avoid parallel execution
        parse_projects >> get_project_team_change_summary2 >> \
            added_users >> removed_users >> \
            removed_users_has_data >> rail.Label('Yes') >> \
            get_billing_rate_assignment_revision >> \
            bulk_get_projects3 >> \
            bulk_get_users >> \
            get_effective_user_groups >> \
            map_removed_users >> finish

    return dag


rail.for_each_instance(create_dag)
