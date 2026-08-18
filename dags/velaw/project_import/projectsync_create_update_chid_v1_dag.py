
from datetime import timedelta
from airflow.models import Variable
import rail
from rail import get_current_context

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'velaw_project_import_velaw_projectsync_create_update_chid_v1_{config.instance}',
        description=f'Velaw_ProjectSync_Create/Update_Chid_V1 {config.instance}',
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
            no_task='create_projectsync_create_update_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_projectsync_create_update_logs',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_projectsync_create_update_logs = rail.CreateLogOperator(
            task_id='create_projectsync_create_update_logs'
        )

        if_request_clientname_present_3 = rail.IfOperator(
            task_id='if_request_clientname_present_3',
            test='''{{ dag_run.conf.clientname | is_truthy }}''',
            yes_task="search_clients_4",
            no_task="if_request_projectname_present_11",
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

        if_client_uri_present_5 = rail.IfOperator(
            task_id='if_client_uri_present_5',
            test=lambda: bool(rail.result('search_clients_4') and rail.result(
                'search_clients_4')[0]['clienturi']),
            yes_task="get_client_details_6",
            no_task="if_get_client_details_6_name_blank_9",
        )

        get_client_details_6 = rail.RepliconServiceOperator(
            task_id='get_client_details_6',
            endpoint="/services/ClientService1.svc/GetClientDetails",
            data=lambda: {
                "clientUri": rail.result('search_clients_4')[0]['clienturi']
            }
        )

        if_get_client_details_6_isactive_is_not_true_7 = rail.IfOperator(
            task_id='if_get_client_details_6_isactive_is_not_true_7',
            test=lambda: (not rail.result('get_client_details_6')['isActive']),
            yes_task="activate_client_8",
            no_task="if_get_client_details_6_name_blank_9",
        )

        activate_client_8 = rail.RepliconServiceOperator(
            task_id='activate_client_8',
            endpoint="/services/ClientService1.svc/Activate",
            data={
                "clientUri": "{{ result('get_client_details_6').uri }}"
            }
        )

        if_get_client_details_6_name_blank_9 = rail.IfOperator(
            task_id='if_get_client_details_6_name_blank_9',
            test=lambda: (not rail.result('search_clients_4')),
            yes_task="create_client_10",
            no_task="if_request_projectname_present_11",
        )

        create_client_10 = rail.RepliconServiceOperator(
            task_id='create_client_10',
            endpoint='/services/ClientService1.svc/PutClient',
            data={
                "client": {
                    "target": {
                        "uri": null,
                        "name": "{{ dag_run.conf.clientname }}",
                        "code": null,
                        "parameterCorrelationId": null
                    },
                    "name": "{{ dag_run.conf.clientname }}",
                    "code": "{{ dag_run.conf.clientcode }}",
                    "comment": null,
                    "clientManager": null,
                    "billingContact": null,
                    "clientAddress": null,
                    "billingAddress": null,
                    "isActive": True,
                    "customFieldValues": [],
                    "billingRates": [],
                    "expenseCodesAllowedByDefaultOnNewProjects": [],
                    "defaultBillingCurrency": null
                }
            }
        )

        if_request_projectname_present_11 = rail.IfOperator(
            task_id='if_request_projectname_present_11',
            test=lambda dag_run: bool(dag_run.conf['projectname']),
            yes_task="search_projects_12",
            no_task="velawg3_projectsync_logs_add_entry_31",
        )

        def get_filtered_data(response, dag_run):
            data = response.json()['d']['rows']
            projectinfo = list(filter(lambda x: x['projectname'] == dag_run.conf['projectname'], map(lambda item: {
                "projecturi": item['cells'][0].get('uri'),
                "projectname": item['cells'][3].get('textValue'),
                "projectcode": item['cells'][1].get('textValue'),
                "status": item['cells'][2].get('textValue')
            }, data)))
            return projectinfo[0] if projectinfo else {}

        search_projects_12 = rail.RepliconServiceOperator(
            task_id='search_projects_12',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    "urn:replicon:project-list-column:code",
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

        if_log_project_exact_match_14_present_15 = rail.IfOperator(
            task_id='if_log_project_exact_match_14_present_15',
            test=lambda: bool(rail.result('search_projects_12') and rail.result('search_projects_12')['projecturi'] and rail.result(
                'search_projects_12')['status'] != 'In Progress'),
            yes_task="update_status_16",
            no_task="if_log_project_status_13_blank_22",
        )

        update_status_16 = rail.RepliconServiceOperator(
            task_id='update_status_16',
            endpoint="/services/ProjectService1.svc/UpdateStatus",
            data={
                "projectUri": "{{ result('search_projects_12').projecturi }}",
                "projectStatusUri": "urn:replicon:project-status-type:in-progress"
            }
        )

        def get_task_state():
            return get_current_context()['dag_run'].get_task_instance('update_status_16').current_state() == 'success'
        update_status_success = rail.PythonOperator(
            task_id='update_status_success',
            python_callable=get_task_state
        )

        bulk_get_projects2_17 = rail.RepliconServiceOperator(
            task_id='bulk_get_projects2_17',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data={
                "projects": [
                    {
                        "uri": "{{ result('search_projects_12').projecturi }}",
                        "name": null,
                        "code": null,
                        "parameterCorrelationId": null
                    }
                ]
            }
        )

        if_projectdetails_description_not_equals_to_dataworkato_service08c89a34requestprojectdescription_18 = rail.IfOperator(
            task_id='if_projectdetails_description_not_equals_to_dataworkato_service08c89a34requestprojectdescription_18',
            test='''{{ result('bulk_get_projects2_17')[0].projectDetails.description != dag_run.conf.projectdescription and dag_run.conf.projectdescription | is_truthy }}''',
            yes_task="update_description_20",
            no_task="velawg3_projectsync_logs_add_entry_21",
        )

        update_description_20 = rail.RepliconServiceOperator(
            task_id='update_description_20',
            endpoint="/services/ProjectService1.svc/UpdateDescription",
            data={
                "projectUri": "{{ result('search_projects_12').projecturi }}",
                "description": "{{ dag_run.conf.projectdescription }}"
            }
        )

        velawg3_projectsync_logs_add_entry_21 = rail.WriteLogOperator(
            task_id='velawg3_projectsync_logs_add_entry_21',
            log="{{ result('create_projectsync_create_update_logs') }}",
            message="Project updated successfully",
            severity="Info",
            properties={
                "project_name": "{{ dag_run.conf.projectname }}",
                "client_name": "{{ dag_run.conf.clientname }}",
                "action": "Update",
                "details": "Project updated successfully"
            }
        )

        if_log_project_status_13_blank_22 = rail.IfOperator(
            task_id='if_log_project_status_13_blank_22',
            test=lambda: bool(not rail.result('search_projects_12')),
            yes_task="create_project_23",
            no_task="if_create_project_23_uri_blank_28",
        )

        create_project_23 = rail.RepliconServiceOperator(
            task_id='create_project_23',
            endpoint="/services/ProjectService1.svc/PutProjectInfo2",
            data={
                    "target": {
                        "uri": null,
                        "name": "{{ dag_run.conf.projectname }}",
                        "code": null,
                        "parameterCorrelationId": null
                    },
                "projectInfo": {
                        "name": "{{ dag_run.conf.projectname }}",
                        "code": "{{ dag_run.conf.projectcode }}",
                        "description": "{{ dag_run.conf.projectdescription }}",
                        "timeEntryDateRange": null,
                        "projectStatusLabel": {
                            "uri": null,
                            "name": "In Progress"
                        },
                        "client": {
                            "uri": null,
                            "name": "{{ dag_run.conf.clientname }}",
                            "code": null,
                            "parameterCorrelationId": null
                        },
                        "clientRepresentative": null,
                        "program": null,
                        "projectLeader": null,
                        "customFieldValues": [],
                        "isTimeEntryAllowed": "1",
                        "costTypeUri": null,
                        "estimatedHours": null,
                        "estimatedCost": null,
                        "estimatedExpenses": null,
                        "budget": null,
                        "estimationModeUri": null,
                        "billingTypeUri": "urn:replicon:billing-type:time-and-material",
                        "timeAndMaterials": {
                            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                            "billingRateFrequency": null,
                            "billingRateFrequencyDuration": null,
                            "billingRates": []
                        },
                        "defaultBillingCurrency": null
                    }
            }
        )

        update_time_entry_date_range_24 = rail.RepliconServiceOperator(
            task_id='update_time_entry_date_range_24',
            endpoint="/services/ProjectService1.svc/UpdateTimeEntryDateRange",
            data={
                "projectUri": "{{ result('create_project_23').uri }}",
                "dateRange": null
            }
        )

        update_project_team_member_assignment_25 = rail.RepliconServiceOperator(
            task_id='update_project_team_member_assignment_25',
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            data={
                "projectUri": "{{ result('create_project_23').uri }}",
                "resourceUri": "urn:replicon-tenant:{{ dag_run.conf.slug }}:department:1",
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            }
        )

        update_billing_rate_is_available_for_assignment_to_team_members_26 = rail.RepliconServiceOperator(
            task_id='update_billing_rate_is_available_for_assignment_to_team_members_26',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers",
            data={
                "projectUri": "{{ result('create_project_23').uri }}",
                "billingRateUri": "urn:replicon:project-specific-billing-rate",
                "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available"
            }
        )

        velawg3_projectsync_logs_add_entry_27 = rail.WriteLogOperator(
            task_id='velawg3_projectsync_logs_add_entry_27',
            log="{{ result('create_projectsync_create_update_logs') }}",
            message="na",
            severity="Info",
            properties={
                "project_name": "{{ dag_run.conf.projectname }}",
                "client_name": "{{ dag_run.conf.clientname }}",
                "action": "Add",
                "details": "Project created successfully"
            }
        )

        if_create_project_23_uri_blank_28 = rail.IfOperator(
            task_id='if_create_project_23_uri_blank_28',
            test=lambda: bool(not rail.result('create_project_23')
                              and not rail.result('update_status_success')),
            yes_task="velawg3_projectsync_logs_add_entry_29",
            no_task="velawg3_projectsync_logs_add_entry_31",
        )

        velawg3_projectsync_logs_add_entry_29 = rail.WriteLogOperator(
            task_id='velawg3_projectsync_logs_add_entry_29',
            log="{{ result('create_projectsync_create_update_logs') }}",
            message="na",
            severity="Info",
            properties={
                "project_name": "{{ dag_run.conf.projectname }}",
                "client_name": "{{ dag_run.conf.clientname }}",
                "action": "Skipped",
                "details": "No changes done"
            }
        )

        velawg3_projectsync_logs_add_entry_31 = rail.WriteLogOperator(
            task_id='velawg3_projectsync_logs_add_entry_31',
            trigger_rule='one_failed',
            log="{{ result('create_projectsync_create_update_logs') }}",
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
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> create_projectsync_create_update_logs >> if_request_clientname_present_3
        if_request_clientname_present_3 >> rail.Label(
            'Yes') >> search_clients_4 >> if_client_uri_present_5
        if_client_uri_present_5 >> rail.Label(
            'Yes') >> get_client_details_6 >> if_get_client_details_6_isactive_is_not_true_7
        if_get_client_details_6_isactive_is_not_true_7 >> rail.Label(
            'Yes') >> activate_client_8 >> if_get_client_details_6_name_blank_9
        if_get_client_details_6_isactive_is_not_true_7 >> rail.Label(
            'No') >> if_get_client_details_6_name_blank_9
        if_client_uri_present_5 >> rail.Label(
            'No') >> if_get_client_details_6_name_blank_9
        if_get_client_details_6_name_blank_9 >> rail.Label(
            'Yes') >> create_client_10 >> if_request_projectname_present_11
        if_get_client_details_6_name_blank_9 >> rail.Label(
            'No') >> if_request_projectname_present_11
        if_request_clientname_present_3 >> rail.Label(
            'No') >> if_request_projectname_present_11
        if_request_projectname_present_11 >> rail.Label(
            'Yes') >> search_projects_12 >> if_log_project_exact_match_14_present_15
        if_log_project_exact_match_14_present_15 >> rail.Label(
            'Yes') >> update_status_16 >> update_status_success >> bulk_get_projects2_17 \
            >> if_projectdetails_description_not_equals_to_dataworkato_service08c89a34requestprojectdescription_18
        if_projectdetails_description_not_equals_to_dataworkato_service08c89a34requestprojectdescription_18 >> rail.Label(
            'Yes') >> update_description_20 >> velawg3_projectsync_logs_add_entry_21
        if_projectdetails_description_not_equals_to_dataworkato_service08c89a34requestprojectdescription_18 >> rail.Label(
            'No') >> velawg3_projectsync_logs_add_entry_21 >> if_log_project_status_13_blank_22
        if_log_project_exact_match_14_present_15 >> rail.Label(
            'No') >> if_log_project_status_13_blank_22
        if_log_project_status_13_blank_22 >> rail.Label(
            'Yes') >> create_project_23 >> update_time_entry_date_range_24 >> update_project_team_member_assignment_25 >> update_billing_rate_is_available_for_assignment_to_team_members_26 >> velawg3_projectsync_logs_add_entry_27 >> if_create_project_23_uri_blank_28
        if_log_project_status_13_blank_22 >> rail.Label(
            'No') >> if_create_project_23_uri_blank_28
        if_create_project_23_uri_blank_28 >> rail.Label(
            'Yes') >> velawg3_projectsync_logs_add_entry_29 >> velawg3_projectsync_logs_add_entry_31
        if_create_project_23_uri_blank_28 >> rail.Label(
            'No') >> velawg3_projectsync_logs_add_entry_31
        if_request_projectname_present_11 >> rail.Label(
            'No') >> velawg3_projectsync_logs_add_entry_31 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
