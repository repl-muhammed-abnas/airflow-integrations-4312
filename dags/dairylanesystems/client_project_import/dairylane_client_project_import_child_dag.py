
from datetime import timedelta
import uuid
import pycountry
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dairy_lane_client_project_import_child_{config.instance}',
        description=f'DairyLane Client/Project import_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_clients'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_clients',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_clients = rail.RepliconServiceOperator(
            task_id='get_clients',
            endpoint="/services/ClientListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
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
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=lambda response,dag_run: list(filter(lambda x: x['cells'][0]['textValue'] == dag_run.conf['clientname'],response['rows']))
        )

        create_dairylane_project_import_lookup = rail.CreateLogOperator(
            task_id="create_dairylane_project_import_lookup",
            tenant_wide_name="dairylane_project_import_prod_logs",
            existing_log_mode="append",
        )

        if_no_clients_present = rail.IfOperator(
            task_id='if_no_clients_present',
            test=lambda dag_run: bool( len(rail.result('get_clients')) < 1 or
                                    rail.result('get_clients')[0]['cells'][0]['textValue'] != dag_run.conf['clientname']),
            yes_task="get_all_countries",
            no_task="is_projectname_present",
        )

        get_all_countries = rail.RepliconServiceOperator(
            task_id='get_all_countries',
            endpoint='/services/InternationalizationService1.svc/GetAllCountries'
        )

        def get_client_country_uri(country):
            country_name = (pycountry.countries.get(alpha_2=country)).name if (pycountry.countries.get(alpha_2=country)) else ''
            return rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_countries'), 'displayText', country_name, 'uri', null)

        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint="/services/ClientService1.svc/PutClient",
            data=lambda dag_run:{
                "client": {
                    "target": {
                        "uri": null,
                        "name": dag_run.conf['clientname']
                    },
                    "name": dag_run.conf['clientname'],
                    "code": dag_run.conf['clientcode'],
                    "comment": null,
                    "clientManager": null,
                    "billingContact": dag_run.conf['clientcontact'],
                    "clientAddress": {
                        "address": dag_run.conf['street'],
                        "city": dag_run.conf['city'],
                        "stateProvince": dag_run.conf['stateprovince'],
                        "country": {
                            "uri": get_client_country_uri(dag_run.conf['country']),
                            "name": null
                        } if get_client_country_uri(dag_run.conf['country']) else null,
                        "zipPostalCode": dag_run.conf['zippostalcode'],
                        "phoneNumber": dag_run.conf['clientphone'],
                        "faxNumber": null,
                        "email": dag_run.conf['clientemail'],
                        "website": null
                    },
                    "billingAddress": {
                        "address": dag_run.conf['streetbilling'] ,
                        "city": dag_run.conf['citybilling'],
                        "stateProvince": dag_run.conf['stateprovincebilling'],
                        "country": {
                            "uri": get_client_country_uri(dag_run.conf['country']),
                            "name": null
                        } if get_client_country_uri(dag_run.conf['country']) else null,
                        "zipPostalCode": dag_run.conf['zippostalcodebilling'],
                        "phoneNumber": dag_run.conf['clientcontact'],
                        "faxNumber": null,
                        "email": dag_run.conf['clientemail'],
                        "website": null
                    },
                    "isActive": "true",
                    "customFieldValues": [],
                    "billingRates":  [
                        {
                            "billingRate": {
                                "uri": "urn:replicon:project-specific-billing-rate",
                                "name": null
                            },
                            "rateSchedule": null
                        },
                        {
                            "billingRate": {
                                "uri": "urn:replicon:user-specific-billing-rate",
                                "name": null
                            },
                            "rateSchedule": null
                        }
                    ],
                    "expenseCodesAllowedByDefaultOnNewProjects": [],
                    "defaultBillingCurrency": {
                        "uri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":currency:1",
                        "name": null,
                        "symbol": null
                    }
                }
            }
        )

        is_projectname_present = rail.IfOperator(
            task_id='is_projectname_present',
            test='''{{ dag_run.conf.projectname | is_truthy }}''',
            yes_task="get_projects",
            no_task="if_client_uri_present",
        )

        get_projects = rail.RepliconServiceOperator(
            task_id='get_projects',
            endpoint="/services/ImportService1.svc/BulkGetProjects2",
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": null,
                        "code": "{{ dag_run.conf.projectcode }}",
                        "parameterCorrelationId": null
                    }
                ]
            }
        )

        if_no_project_present = rail.IfOperator(
            task_id='if_no_project_present',
            test=lambda: not bool(rail.result('get_projects') and rail.result('get_projects')['results']
                and rail.result('get_projects')['results'][0] and rail.result('get_projects')['results'][0]['project']['uri']),
            yes_task="create_new_project",
            no_task="update_status",
        )

        create_new_project = rail.RepliconServiceOperator(
            task_id='create_new_project',
            endpoint="/services/ImportService1.svc/PutProject3",
            data=lambda dag_run: {
                "project": {
                    "target": {
                        "uri": null,
                        "name": dag_run.conf['projectcode'] + " - " + dag_run.conf['projectname'],
                        "code": null,
                        "parameterCorrelationId": null
                    },
                    "projectInfo": {
                        "name": dag_run.conf['projectcode'] + " - " + dag_run.conf['projectname'],
                        "code": dag_run.conf['projectcode'],
                        "description": null,
                        "timeEntryDateRange": {
                            "startDate": {
                                "year": dag_run.conf['startdateyear'],
                                "month": dag_run.conf['startdatemonth'],
                                "day": dag_run.conf['startdateday']
                            },
                            "endDate": null,
                            "relativeDateRangeUri": null,
                            "relativeDateRangeAsOfDate": null
                        },
                        "projectStatusLabel": {
                            "uri": dag_run.conf['projectstatusuri'],
                            "name": null
                        },
                        "percentCompleted": "0",
                        "client": {
                            "uri": rail.result('get_clients')[0]['cells'][0]['uri'] if (rail.result('get_clients') and
                                    rail.result('get_clients')[0]['cells'][0]['textValue'] == dag_run.conf['clientname'])
                                    else rail.result('create_client')['uri'],
                            "name": null,
                            "code": null,
                            "parameterCorrelationId": null
                        },
                        "program": null,
                        "projectLeader": null,
                        "customFieldValues": [],
                        "isTimeEntryAllowed": "false",
                        "costTypeUri": null,
                        "estimatedHours": null,
                        "estimatedCost": null,
                        "estimatedExpenses": null,
                        "budget": null,
                        "isProjectLeaderApprovalRequired": "true",
                        "estimationModeUri": "urn:replicon:project-estimation-mode:task-based",
                        "billingTypeUri": "urn:replicon:billing-type:time-and-material",
                        "timeAndMaterials": {
                            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                            "billingRateFrequency": null,
                            "billingRateFrequencyDuration": null,
                            "billingRates": []
                        },
                        "defaultBillingCurrency": null
                    },
                    "tasks": [
                        {
                            "task": {
                                "target": {
                                    "uri": null,
                                    "name": dag_run.conf['taskname'],
                                    "parent": null,
                                    "parameterCorrelationId": null
                                },
                                "name": dag_run.conf['taskname'],
                                "code": null,
                                "description": null,
                                "timeEntryDateRange": {
                                    "startDate": {
                                        "year": dag_run.conf['startdateyear'],
                                        "month": dag_run.conf['startdatemonth'],
                                        "day": dag_run.conf['startdateday']
                                    },
                                    "endDate": null,
                                    "relativeDateRangeUri": null,
                                    "relativeDateRangeAsOfDate": null
                                },
                                "percentCompleted": "0",
                                "isTimeEntryAllowed": "false",
                                "estimatedHours": null,
                                "isClosed": "false",
                                "customFieldValues": [],
                                "extensionFieldValues": [],
                                "estimatedCost": null,
                                "costTypeUri": null,
                                "assignedResources": [],
                                "timeAndMaterials": null,
                                "keyValues": [],
                                "historicalKeyValues": []
                            },
                            "childTasks": [
                                {
                                    "task": {
                                        "target": {
                                            "uri": null,
                                            "name": dag_run.conf['tasknamelevel2'],
                                            "parent": null,
                                            "parameterCorrelationId": null
                                        },
                                        "name": dag_run.conf['tasknamelevel2'],
                                        "code": null,
                                        "description": null,
                                        "timeEntryDateRange": {
                                            "startDate": {
                                                "year": dag_run.conf['startdateyear'],
                                                "month": dag_run.conf['startdatemonth'],
                                                "day": dag_run.conf['startdateday']
                                            },
                                            "endDate": null,
                                            "relativeDateRangeUri": null,
                                            "relativeDateRangeAsOfDate": null
                                        },
                                        "percentCompleted": "0",
                                        "isTimeEntryAllowed": "true",
                                        "estimatedHours": null,
                                        "isClosed": "false",
                                        "customFieldValues": [],
                                        "extensionFieldValues": [],
                                        "estimatedCost": null,
                                        "costTypeUri": null,
                                        "assignedResources": [
                                            {
                                                "uri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:department:1",
                                                "resourcePlaceholderParameterCorrelationId": null,
                                                "user": null,
                                                "department": null,
                                                "placeholder": null,
                                                "location": null,
                                                "division": null,
                                                "costCenter": null,
                                                "serviceCenter": null,
                                                "departmentGroup": null,
                                                "employeeTypeGroup": null
                                            }
                                        ],
                                        "timeAndMaterials": null,
                                        "keyValues": [],
                                        "historicalKeyValues": []
                                    },
                                    "childTasks": []
                                }
                            ]
                        }
                    ],
                    "team": {
                            "teamMembers": [
                                {
                                "resource": {
                                    "uri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:department:1",
                                    "resourcePlaceholderParameterCorrelationId": null,
                                    "user": null,
                                    "department": null,
                                    "placeholder": null,
                                    "location": null,
                                    "division": null,
                                    "costCenter": null,
                                    "serviceCenter": null,
                                    "departmentGroup": null,
                                    "employeeTypeGroup": null
                                },
                                "resourcePlaceholder": null,
                                "timeAndMaterials": {
                                    "billingRatesAllowedForBillingTimeUris": [
                                    "urn:replicon:project-specific-billing-rate"
                                    ]
                                }
                                }
                            ]
                            },
                    "expenses": null,
                    "timeAndMaterials": {
                                        "billingRates": [
                                            {
                                            "billingRate": {
                                                "uri": "urn:replicon:project-specific-billing-rate",
                                                "name": null
                                            },
                                            "rateSchedule": null
                                            },
                                            {
                                            "billingRate": {
                                                "uri": "urn:replicon:user-specific-billing-rate",
                                                "name": null
                                            },
                                            "rateSchedule": null
                                            }
                                        ]
                                        },
                    "fixedBid": null
                }
            }
        )

        update_project_oef = rail.RepliconServiceOperator(
            task_id='update_project_oef',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: {
                "target": {
                    "uri": rail.result('create_new_project')['uri']
                },
                "modifications": {
                    "objectExtensionFieldsToApply": [
                        {
                            "definition": {
                                "uri": dag_run.conf['addressoefuri']
                            },
                            "tag": {
                                "uri": dag_run.conf['projectoefvalueuri'],
                                "tagName": null
                            },
                            "textValue": null
                        },
                        {
                            "tag": {
                                "tagName": null,
                                "uri": null
                            },
                            "definition": {
                                "uri": dag_run.conf['clientnameoefuri']
                            },
                            "textValue": dag_run.conf['clientname']
                        }
                    ]
                },
                "unitOfWorkId": str(uuid.uuid4()),
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save"
            }
        )

        add_entry_project_client_created = rail.WriteLogOperator(
            task_id='add_entry_project_client_created',
            log="{{ result('create_dairylane_project_import_lookup') }}",
            message="na",
            severity="Success Client and Project Created",
            properties={
                "jobid": "{{ dag_run.conf.callingdagrunid }}",
                "projectname": "{{ dag_run.conf.projectname }}|{{ dag_run.conf.clientname }}",
                "clientname": "{{ dag_run.conf.clientname }}",
                "status": "Success Client and Project Created",
                "reason": "{{ dag_run_ecid()}}" + "|" + "Project/Task created", 
                "filename": "{{ dag_run.conf.filename }}",
            }
        )

        update_status = rail.RepliconServiceOperator(
            task_id='update_status',
            endpoint="/services/ProjectService1.svc/UpdateStatus",
            retries=0,
            data={
                "projectUri": "{{ result('get_projects').results[0].project.uri }}",
                "projectStatusUri": "{{ dag_run.conf.projectstatusuri }}"
            }
        )

        add_entry_project_status_updated = rail.WriteLogOperator(
            task_id='add_entry_project_status_updated',
            log="{{ result('create_dairylane_project_import_lookup') }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{ dag_run.conf.callingdagrunid }}",
                "projectname": "{{ dag_run.conf.projectname }}|{{ dag_run.conf.clientname }}",
                "clientname": "{{ dag_run.conf.clientname }}",
                "status": "Success",
                "reason": "{{ dag_run_ecid()}}" + "|" + "Project status updated",
                "filename": "{{ dag_run.conf.filename }}",
            }
        )

        if_client_uri_present = rail.IfOperator(
            task_id='if_client_uri_present',
            test='''{{ dag_run.conf.projectname | is_falsy  and result('create_client').uri | is_truthy }}''',
            yes_task="add_entry_client_created",
            no_task="on_error",
        )

        add_entry_client_created = rail.WriteLogOperator(
            task_id='add_entry_client_created',
            log="{{ result('create_dairylane_project_import_lookup') }}",
            message="na",
            severity="Success Client Created",
            properties={
                "jobid": "{{ dag_run.conf.callingdagrunid }}",
                "projectname": "{{ dag_run.conf.projectname }}|{{ dag_run.conf.clientname }}",
                "clientname": "{{ dag_run.conf.clientname }}",
                "status": "Success Client Created",
                "reason": "{{ dag_run_ecid()}}" + "|" + "Project/Task created",
                "filename": "{{ dag_run.conf.filename }}"
            }
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed',
        )

        add_failure_entry = rail.WriteLogOperator(
            task_id='add_failure_entry',
            log="{{ result('create_dairylane_project_import_lookup') }}",
            message="na",
            severity="Failed",
            properties={
                "jobid": "{{ dag_run.conf.callingdagrunid }}",
                "projectname": "{{ dag_run.conf.projectname }}|{{ dag_run.conf.clientname }}",
                "clientname": "{{dag_run.conf.clientname}}",
                "status": "Failed",
                "reason": "{{ dag_run_ecid()}}"+ "|" + "{{ get_error_message()}}",
                "filename": "{{ dag_run.conf.filename }}"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_clients
        get_clients >> create_dairylane_project_import_lookup >> if_no_clients_present
        if_no_clients_present >> rail.Label(
            'Yes') >> get_all_countries >> create_client >> is_projectname_present
        if_no_clients_present >> rail.Label('No') >> is_projectname_present
        is_projectname_present >> rail.Label(
            'Yes') >> get_projects >> if_no_project_present
        if_no_project_present >> rail.Label(
            'Yes') >> create_new_project >> update_project_oef >> add_entry_project_client_created >> if_client_uri_present
        if_no_project_present >> rail.Label(
            'No') >> update_status >> add_entry_project_status_updated >> if_client_uri_present
        is_projectname_present >> rail.Label('No') >> if_client_uri_present
        if_client_uri_present >> rail.Label(
            'Yes') >> add_entry_client_created >> on_error
        if_client_uri_present >> rail.Label(
            'No') >> on_error >> add_failure_entry >> finish

    return dag

rail.for_each_instance(create_dag)
