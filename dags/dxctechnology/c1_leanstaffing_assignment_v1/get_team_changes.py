import datetime
import pytz
import rail

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_leanstaffing_assignment_v1/config.py


def create_dag(config):
    dag_id_postfix = f'_{config.instance}_v1'
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_c1_leanstaffassignment_team_changes{dag_id_postfix}',
        description=f'DXC C1 Leanstaff Assignment - Get Team Changes {config.instance} v1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime.datetime(2022, 1, 1),
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_project_team_change_summary2 = rail.RepliconServiceOperator(
            task_id='get_project_team_change_summary2',
            endpoint='/services/ProjectService1.svc/GetProjectTeamChangeSummary2',
            data={
                "projectUri": "{{ dag_run.conf.project_uri }}",
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

        # added_users and removed_users both do the same thing but in different ways. Just to show 2 possible
        # approaches to manipulate data from one form into another

        added_users = rail.RenderTemplateOperator(
            task_id='added_users',
            template="""
                {% set users = [] %}
                {% for user in result('get_project_team_change_summary2').teamMembersAdded | filter_by_attr('resource.user.uri', 'exists') %}
                    {% do users.append({ 'user_uri': user.resource.user.uri, 'project_uri': dag_run.conf.project_uri }) %}
                {% endfor %}
                {{ users | tojson }}
                """,
            target='result',
            json=True,
        )

        def do_removed_users(*, dag_run):
            users = [r for r in rail.result('get_project_team_change_summary2')[
                'teamMembersRemoved'] if r['resource'].get('user')]
            return [{'user_uri': user['resource']['user']['uri'],
                    'project_uri': dag_run.conf['project_uri'],
                     'assignmentDateRange': user['assignmentDateRange']} for user in users]

        removed_users = rail.PythonOperator(
            task_id='removed_users',
            python_callable=do_removed_users
        )

        removed_users_has_data = rail.IfOperator(
            task_id="removed_users_has_data",
            test="{{ result('removed_users') | length > 0 }}",
            yes_task=['get_billing_rate_assignment_revision',
                      'bulk_get_projects3', 'bulk_get_users', 'get_effective_user_groups']
        )

        get_billing_rate_assignment_revision = rail.RepliconServiceOperator(
            task_id='get_billing_rate_assignment_revision',
            endpoint='/audittrail-api/graphql',
            data={
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
                    "limit": 1,
                    "cursor": "",
                    "filter": {
                        "projectUris": [
                            "{{ dag_run.conf.project_uri }}"
                        ],
                        "dateRange": {
                            "startDate": "{{ dag_run.conf.get_changes_since }}",
                            "endDate": datetime.datetime.now(pytz.timezone("Etc/UTC")).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                        }
                    }
                }
            },
        )

        bulk_get_projects3 = rail.RepliconServiceOperator(
            task_id='bulk_get_projects3',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "uri": "{{ dag_run.conf.project_uri }}",
                    }
                ]
            },
        )

        bulk_get_users = rail.RepliconServiceOperator(
            task_id='bulk_get_users',
            endpoint='/services/UserService1.svc/BulkGetUserDetails',
            data=lambda: {
                "userUri": list(map(lambda x: x['user_uri'], rail.result('removed_users')))
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

            project_details = rail.result('bulk_get_projects3')[
                0]['projectDetails']
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
            billing_rate_items = rail.result('get_billing_rate_assignment_revision')[
                'data']['projectTeamMemberBillingRateAssignmentRevisions']['items']
            billing_rate = billing_rate_items[0]['currentBillingRate'] or billing_rate_items[0]['previousBillingRate'] \
                if len(billing_rate_items) > 0 else None
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

        get_project_team_change_summary2 >> [
            added_users, removed_users]
        removed_users >> removed_users_has_data >> rail.Label('Yes') >> [
            get_billing_rate_assignment_revision, bulk_get_projects3, bulk_get_users, get_effective_user_groups] >> map_removed_users

    return dag


rail.for_each_instance(create_dag)
