from datetime import timedelta
import rail
from airflow.models import Variable
null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_jira_{config.region.replace('-', '_')}_create_task_child_dag_{config.instance}",
        description=f'Jira {config.region} Create Task Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_projects'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_projects',
            end_task='catch_create_task_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def get_filtered_data(response, dag_run):
            projectname = dag_run.conf['projectname']
            data = response.json()['d']['rows']
            projectinfo = list(filter(lambda x: x['projectname'] == projectname, map(lambda item: {
                "projecturi": item['cells'][0]['uri'],
                "projectname": item['cells'][0].get('textValue'),
            }, data)))
            return projectinfo[0] if projectinfo else {}

        search_projects = rail.RepliconServiceOperator(
            task_id='search_projects',
            endpoint='/services/ProjectListService1.svc/GetData',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    "urn:replicon:project-list-column:code"
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

        if_project_uri_present = rail.IfOperator(
            task_id='if_project_uri_present',
            test=lambda: rail.result('search_projects') and rail.result(
                'search_projects')['projecturi'],
            yes_task="if_legacy_department_enabled",
            no_task="create_project",
        )

        create_project = rail.RepliconServiceOperator(
            task_id='create_project',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
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
                        "code": "{{ dag_run.conf.issues[0].project_id }}",
                        "description": null,
                        "timeEntryDateRange": null,
                        "projectStatusLabel": {
                            "uri": null,
                            "name": "In Progress"
                        },
                        "percentCompleted": "0",
                        "client": null,
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
                        "isProjectLeaderApprovalRequired": "1",
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

        should_update_project_type = rail.IfOperator(
            task_id='should_update_project_type',
            test="{{ dag_run.conf.is_polaris_permissions_present | is_truthy \
                and get_task_state('create_project') == 'success' }}",
            yes_task='update_project_type',
            no_task='if_legacy_department_enabled'
        )

        update_project_type = rail.RepliconServiceOperator(
            task_id='update_project_type',
            endpoint='/services/ProjectService1.svc/PutKeyValueForProject',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            # pylint: disable=line-too-long
            data=lambda: {
                'projectUri': rail.result('create_project')['uri'],
                'keyValue': {
                    'keyUri': 'urn:replicon:project-key-value-key:project-management-type',
                    'value': {
                        'uri': 'urn:replicon:project-management-type:managed'
                    }
                }
            }
        )

        if_legacy_department_enabled = rail.IfOperator(
            task_id='if_legacy_department_enabled',
            test=lambda dag_run: dag_run.conf['legacy_department'] is True,
            yes_task="create_tasks_by_legacy",
            no_task="create_tasks",
        )

        create_tasks_by_legacy = rail.RepliconServiceCallForEachItemOperator(
            task_id="create_tasks_by_legacy",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint="/services/ProjectService1.svc/PutTask",
            items='{{ dag_run.conf.issues | to_json }}',
            execution_timeout=timedelta(days=14),
            flatten=True,
            data=lambda item: {
                "project": {
                    "uri": rail.result(
                        'search_projects')['projecturi'] if rail.result(
                        'search_projects') and rail.result(
                        'search_projects')['projecturi'] else rail.result(
                        'create_project')['uri'],
                    "name": null,
                    "parameterCorrelationId": null
                },
                "task": {
                    "target": {
                        "uri": null,
                        "name": item['key'],
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "name": item["key"],
                    "code": item["issue_id"],
                    "description": null,
                    "timeEntryDateRange": {
                        "startDate": {
                            "year": item['created'].split('-')[0],
                            "month": item['created'].split('-')[1],
                            "day": item['created'].split('-')[2]
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
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": null,
                    "assignedResources": [
                        {
                            "uri": null,
                            "resourcePlaceholderParameterCorrelationId": null,
                            "user": null,
                            "department": {
                                "uri": null,
                                "name": "Company",
                                "parent": null,
                                "parameterCorrelationId": null
                            },
                            "placeholder": null,
                            "location": null,
                            "division": null,
                            "costCenter": null,
                            "serviceCenter": null,
                            "departmentGroup": null,
                            "employeeTypeGroup": null
                        }
                    ]
                }
            }
        )

        create_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id="create_tasks",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint="/services/ProjectService1.svc/PutTask",
            items='{{ dag_run.conf.issues | to_json }}',
            execution_timeout=timedelta(days=14),
            flatten=True,
            data=lambda item: {
                "project": {
                    "uri": rail.result(
                        'search_projects')['projecturi'] if rail.result(
                        'search_projects') and rail.result(
                        'search_projects')['projecturi'] else rail.result(
                        'create_project')['uri'],
                    "name": null,
                    "parameterCorrelationId": null
                },
                "task": {
                    "target": {
                        "uri": null,
                        "name": item['key'],
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "name": item["key"],
                    "code": item["issue_id"],
                    "description": null,
                    "timeEntryDateRange": {
                        "startDate": {
                            "year": item['created'].split('-')[0],
                            "month": item['created'].split('-')[1],
                            "day": item['created'].split('-')[2]
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
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": null,
                    "assignedResources": [
                        {
                            "uri": null,
                            "resourcePlaceholderParameterCorrelationId": null,
                            "user": null,
                            "department": {
                                "uri": null,
                                "name": "Company",
                                "parent": null,
                                "parameterCorrelationId": null
                            },
                            "placeholder": null,
                            "location": null,
                            "division": null,
                            "costCenter": null,
                            "serviceCenter": null,
                            "departmentGroup": null,
                            "employeeTypeGroup": null
                        }
                    ]
                }
            }
        )

        def get_downstreamtasks_error(project_name, error_message):
            return {
                'error': f'Error with {project_name} - {error_message}'
            }
        catch_create_task_error = rail.PythonOperator(
            task_id='catch_create_task_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.projectname }}',
                     '{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> rail.Label(
                'on Error') >> catch_create_task_error

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_create_task_error
        can_run_batch_task >> rail.Label(
            'No') >> search_projects >> if_project_uri_present
        if_project_uri_present >> rail.Label(
            'Yes') >> if_legacy_department_enabled
        if_legacy_department_enabled >> rail.Label(
            'Yes') >> create_tasks_by_legacy >> rail.Label(
            'on Error') >> catch_create_task_error
        if_legacy_department_enabled >> rail.Label(
            'No') >> create_tasks >> rail.Label(
            'on Error') >> catch_create_task_error
        if_project_uri_present >> rail.Label(
            'No') >> create_project >> should_update_project_type
        should_update_project_type >> rail.Label(
            'Yes') >> update_project_type >> if_legacy_department_enabled
        should_update_project_type >> rail.Label(
            'No') >> if_legacy_department_enabled

    return dag


rail.for_each_instance(create_child_dag)
