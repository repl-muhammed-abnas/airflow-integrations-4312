
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'velaw_project_import_velaw_projectsync_disable_chid_v1_{config.instance}',
        description=f'Velaw_ProjectSync_Disable_Chid_V1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_projectsync_disable_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_projectsync_disable_logs',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_projectsync_disable_logs = rail.CreateLogOperator(
            task_id='create_projectsync_disable_logs',
        )

        if_request_clientname_present_3 = rail.IfOperator(
            task_id='if_request_clientname_present_3',
            test='''{{ dag_run.conf.clientname | is_truthy and dag_run.conf.type == 'client' }}''',
            yes_task="search_clients_4",
            no_task="if_request_clientname_blank_12",
        )

        def check_client_data(response, dag_run):
            response = response.json()['d']
            if not response:
                return []

            return list(filter(lambda x: x['clientname'] == dag_run.conf['clientname'], list(map(lambda item: {
                "clienturi": item['cells'][1]['uri'],
                "clientname": item['cells'][0]['textValue'] if item['cells'][0]['dataType'] != 'urn:replicon:list-type:null' else null,
            }, response['rows']))))

        search_clients_4 = rail.RepliconServiceOperator(
            task_id='search_clients_4',
            endpoint='/services/ClientListService1.svc/GetData',
            data={
                "page": 1,
                "pagesize": 100000000,
                "columnUris": [
                    "urn:replicon:client-list-column:name",
                    "urn:replicon:client-list-column:client"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:client-list-filter:name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": "{{ dag_run.conf.clientname }}",
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            response_filter=check_client_data
        )

        if_search_clients_present = rail.IfOperator(
            task_id='if_search_clients_present',
            test=lambda: bool(rail.result('search_clients_4')),
            yes_task="sendrequest_listofprojectsassociated_5",
            no_task="if_request_clientname_blank_12"
        )

        def get_filtered_projects(response):
            data = response.json()['d']['rows']
            return list(filter(lambda x: x['status'] and x['status'] == 'In Progress', map(lambda item: {
                "projectname": item['cells'][0].get('textValue'),
                "status": item['cells'][1].get('textValue')
            }, data)))

        sendrequest_listofprojectsassociated_5 = rail.RepliconServiceOperator(
            task_id='sendrequest_listofprojectsassociated_5',
            endpoint="/services/ProjectListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:project-list-column:name",
                    "urn:replicon:project-list-column:status"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:project-list-filter:client"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ result('search_clients_4')[0].clienturi }}",
                            "uris": [],
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
                            "dateTimeUtcRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            response_filter=get_filtered_projects
        )

        if_output_projects_less_than_1_7 = rail.IfOperator(
            task_id='if_output_projects_less_than_1_7',
            test='''{{ result('sendrequest_listofprojectsassociated_5') | length < 1 }}''',
            yes_task="inactivate_disabling_client_8",
            no_task="if_output_projects_greater_than_0_10",
        )

        inactivate_disabling_client_8 = rail.RepliconServiceOperator(
            task_id='inactivate_disabling_client_8',
            endpoint="/services/ClientService1.svc/Inactivate",
            data={
                "clientUri": "{{ result('search_clients_4')[0].clienturi }}"
            }
        )

        velawg3_projectsync_logs_add_entry_9 = rail.WriteLogOperator(
            task_id='velawg3_projectsync_logs_add_entry_9',
            log="{{ result('create_projectsync_disable_logs') }}",
            message="na",
            severity="Success",
            properties={
                "project_name": "",
                "client_name": "{{ dag_run.conf.clientname }}",
                "action": "Disable",
                "details": "Client disabled successfully"
            }
        )

        if_output_projects_greater_than_0_10 = rail.IfOperator(
            task_id='if_output_projects_greater_than_0_10',
            test='''{{ result('sendrequest_listofprojectsassociated_5') |length > 0 }}''',
            yes_task="velawg3_projectsync_logs_add_entry_11",
            no_task="if_request_clientname_blank_12",
        )

        velawg3_projectsync_logs_add_entry_11 = rail.WriteLogOperator(
            task_id='velawg3_projectsync_logs_add_entry_11',
            log="{{ result('create_projectsync_disable_logs') }}",
            message="na",
            severity="fixme",
            properties={
                "project_name": "{{ dag_run.conf.projectname }}",
                "client_name": "{{ dag_run.conf.clientname }}",
                "action": "Disable",
                "details": "Client disabling skipped as client is associated with one or more inprogress projects"
            }
        )

        if_request_clientname_blank_12 = rail.IfOperator(
            task_id='if_request_clientname_blank_12',
            test='''{{ dag_run.conf.clientname | is_falsy and dag_run.conf.type == 'client' }}''',
            yes_task="velawg3_projectsync_logs_add_entry_13",
            no_task="if_request_projectname_present_14",
        )

        velawg3_projectsync_logs_add_entry_13 = rail.WriteLogOperator(
            task_id='velawg3_projectsync_logs_add_entry_13',
            log="{{ result('create_projectsync_disable_logs') }}",
            message="na",
            severity="fixme",
            properties={
                "project_name": "{{ dag_run.conf.projectname }}",
                "client_name": "{{ dag_run.conf.clientname }}",
                "action": "Disable",
                "details": "Client disabling skipped as no client name found"
            }
        )

        if_request_projectname_present_14 = rail.IfOperator(
            task_id='if_request_projectname_present_14',
            test='''{{ dag_run.conf.projectname | is_truthy and dag_run.conf.type == 'project' }}''',
            yes_task="search_projects_15",
            no_task="if_request_projectname_blank_22",
        )

        def get_filtered_data(response, dag_run):
            data = response.json()['d']['rows']
            projectinfo = list(filter(lambda x: x['projectname'] == dag_run.conf['projectname'], map(lambda item: {
                "projecturi": item['cells'][0].get('uri'),
                "projectname": item['cells'][2].get('textValue'),
                "status": item['cells'][1].get('textValue')
            }, data)))
            return projectinfo[0] if projectinfo else {}

        search_projects_15 = rail.RepliconServiceOperator(
            task_id='search_projects_15',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    "urn:replicon:project-list-column:status",
                    "urn:replicon:project-list-column:name"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:project-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": dag_run.conf['projectname'],
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
                }
            },
            response_filter=get_filtered_data
        )

        if_log_exact_match_16_present_17 = rail.IfOperator(
            task_id='if_log_exact_match_16_present_17',
            test=lambda: rail.result('search_projects_15'),
            yes_task="sendrequest_changingstatustocomplete_18",
            no_task="if_log_exact_match_16_blank_20",
        )

        sendrequest_changingstatustocomplete_18 = rail.RepliconServiceOperator(
            task_id='sendrequest_changingstatustocomplete_18',
            endpoint="/services/ProjectService1.svc/UpdateStatus",
            data={
                "projectUri": "{{ result('search_projects_15').projecturi }}",
                "projectStatusUri": "urn:replicon:project-status-type:completed"
            }
        )

        velawg3_projectsync_logs_add_entry_19 = rail.WriteLogOperator(
            task_id='velawg3_projectsync_logs_add_entry_19',
            log="{{ result('create_projectsync_disable_logs') }}",
            message="na",
            severity="Success",
            properties={
                "project_name": "{{ dag_run.conf.projectname }}",
                "client_name": "{{ dag_run.conf.clientname }}",
                "action": "Disable",
                "details": "Project marked as completed"
            }
        )

        if_log_exact_match_16_blank_20 = rail.IfOperator(
            task_id='if_log_exact_match_16_blank_20',
            test=lambda: bool(not rail.result(
                'search_projects_15')),
            yes_task="velawg3_projectsync_logs_add_entry_21",
            no_task="if_request_projectname_blank_22",
        )

        velawg3_projectsync_logs_add_entry_21 = rail.WriteLogOperator(
            task_id='velawg3_projectsync_logs_add_entry_21',
            log="{{ result('create_projectsync_disable_logs') }}",
            message="na",
            severity="fixme",
            properties={
                "project_name": "{{ dag_run.conf.projectname }}",
                "client_name": "{{ dag_run.conf.clientname }}",
                "action": "Disable",
                "details": "Project not found in Replicon"
            }
        )

        if_request_projectname_blank_22 = rail.IfOperator(
            task_id='if_request_projectname_blank_22',
            test=lambda dag_run: bool(
                not dag_run.conf['projectname'] and dag_run.conf['type'] == 'project'),
            yes_task="velawg3_projectsync_logs_add_entry_23",
            no_task="velawg3_projectsync_logs_add_entry_25",
        )

        velawg3_projectsync_logs_add_entry_23 = rail.WriteLogOperator(
            task_id='velawg3_projectsync_logs_add_entry_23',
            log="{{ result('create_projectsync_disable_logs') }}",
            message="na",
            severity="fixme",
            properties={
                "project_name": "{{ dag_run.conf.projectname }}",
                "client_name": "{{ dag_run.conf.clientname }}",
                "action": "Skipped",
                "details": "Project name not present"
            }
        )

        velawg3_projectsync_logs_add_entry_25 = rail.WriteLogOperator(
            task_id='velawg3_projectsync_logs_add_entry_25',
            trigger_rule='one_failed',
            log="{{ result('create_projectsync_disable_logs') }}",
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "project_name": "{{ dag_run.conf.projectname }}",
                "client_name": "{{ dag_run.conf.clientname }}",
                "action": "Error",
                "details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> create_projectsync_disable_logs >> if_request_clientname_present_3
        if_request_clientname_present_3 >> rail.Label(
            'Yes') >> search_clients_4 >> if_search_clients_present
        if_search_clients_present >> rail.Label(
            'Yes') >> sendrequest_listofprojectsassociated_5 >> if_output_projects_less_than_1_7
        if_search_clients_present >> rail.Label(
            'No') >> if_request_clientname_blank_12
        if_output_projects_less_than_1_7 >> rail.Label(
            'Yes') >> inactivate_disabling_client_8 >> velawg3_projectsync_logs_add_entry_9 >> if_output_projects_greater_than_0_10
        if_output_projects_less_than_1_7 >> rail.Label(
            'No') >> if_output_projects_greater_than_0_10
        if_output_projects_greater_than_0_10 >> rail.Label(
            'Yes') >> velawg3_projectsync_logs_add_entry_11 >> if_request_clientname_blank_12
        if_output_projects_greater_than_0_10 >> rail.Label(
            'No') >> if_request_clientname_blank_12
        if_request_clientname_present_3 >> rail.Label(
            'No') >> if_request_clientname_blank_12
        if_request_clientname_blank_12 >> rail.Label(
            'Yes') >> velawg3_projectsync_logs_add_entry_13 >> if_request_projectname_present_14
        if_request_clientname_blank_12 >> rail.Label(
            'No') >> if_request_projectname_present_14
        if_request_projectname_present_14 >> rail.Label(
            'Yes') >> search_projects_15 >> if_log_exact_match_16_present_17
        if_log_exact_match_16_present_17 >> rail.Label(
            'Yes') >> sendrequest_changingstatustocomplete_18 >> velawg3_projectsync_logs_add_entry_19 >> if_log_exact_match_16_blank_20
        if_log_exact_match_16_present_17 >> rail.Label(
            'No') >> if_log_exact_match_16_blank_20
        if_log_exact_match_16_blank_20 >> rail.Label(
            'Yes') >> velawg3_projectsync_logs_add_entry_21 >> if_request_projectname_blank_22
        if_log_exact_match_16_blank_20 >> rail.Label(
            'No') >> if_request_projectname_blank_22
        if_request_projectname_present_14 >> rail.Label(
            'No') >> if_request_projectname_blank_22
        if_request_projectname_blank_22 >> rail.Label(
            'Yes') >> velawg3_projectsync_logs_add_entry_23 >> velawg3_projectsync_logs_add_entry_25
        if_request_projectname_blank_22 >> rail.Label(
            'No') >> velawg3_projectsync_logs_add_entry_25 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
