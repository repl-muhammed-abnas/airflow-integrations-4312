import uuid
import rail

null = None


def get_create_client_payload(dag_run):
    webhook_data = dag_run.conf.get('webhook', '').get('data', '')
    return {
        'clientname': webhook_data.get('Client_Name', ''),
        'clientcode': webhook_data.get('Client_Code', ''),
        'addressline': webhook_data.get('Address_Line_1', ''),
        'state': webhook_data.get('State', ''),
        'clientzip': webhook_data.get('Client_Zip_/_Postal_Code', ''),
        'clientcountry': rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_countries'), 'displayText', webhook_data.get('Client_Country', ''), 'uri'),
        'phone': webhook_data.get('Phone', ''),
        'fax': webhook_data.get('Fax', ''),
        'email': webhook_data.get('email', ''),
        'website': webhook_data.get('Website', ''),
        'db': webhook_data.get('mill_mpc', '')
    }


def search_client_data(dag_run):
    return {
        "page": "1",
        "pagesize": "111",
        "columnUris": [
                "urn:replicon:client-list-column:client"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:client-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['clientname'].strip(),
                },
                "filterDefinitionUri": null
            },
        }
    }


def create_client_payload(dag_run):
    def get_clientcountry():
        return {
            "value": {
                "uri": dag_run.conf['clientcountry'],
                "name": null
            }
        } if dag_run.conf['clientcountry'] else null

    return {
        "target": null,
        "modifications": {
            "nameToApply": {
                "value": dag_run.conf['clientname'].strip()
            },
            "codeToApply": {
                "value": dag_run.conf['clientcode']
            },
            "descriptionToApply": null,
            "statusToApply": "1",
            "clientContactToApply": null,
            "clientAddressToApply": {
                "address": {
                    "value": dag_run.conf['addressline']
                },
                "city": null,
                "stateProvince": {
                    "value": dag_run.conf['state']
                },
                "country": get_clientcountry(),
                "zipPostalCode": {
                    "value": dag_run.conf['clientzip']
                },
                "phoneNumber": {
                    "value": dag_run.conf['phone']
                },
                "faxNumber": {
                    "value": dag_run.conf['fax']
                },
                "email": {
                    "value": dag_run.conf['email']
                },
                "website": {
                    "value": dag_run.conf['website']
                }
            },
            "billingAddressToApply": null,
            "billingRatesToApply": null,
            "clientManagerToApply": null,
            "clientSharingToApply": null,
            "expenseCodesToApply": null,
            "customFieldsToApply": []
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def search_projects_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:project-list-column:project",
            "urn:replicon:project-list-column:code"
        ],
        "sort": [],
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
                    "text": dag_run.conf['webhook']['data']['Project_Code'],
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
    }


def get_project_payload(dag_run):
    webhook_data = dag_run.conf['webhook']['data']
    return {
        'clientname': (webhook_data['Client_Name']).strip(),
        'clientcode': str(webhook_data['Client_Code']),
        'projectname': (webhook_data['Project_Name']).strip(),
        'projectcode': str(webhook_data['Project_Code']),
        'projectleadername': (webhook_data['Project_Manager']).strip(),
        'projectid': str(webhook_data['Project_ID']),
        'millmpc': (webhook_data['mill_mpc']).strip(),
        'projectstatus': (webhook_data['Project_Status']).strip(),
        'projectmanagerid': str(webhook_data['Project_Producer_Global_ID']),
        'projecttype': (webhook_data['Project_Type']).strip(),
        'projectclassification': (webhook_data['Project_Classification']).strip(),
        'productname': (webhook_data['Product_Name']).strip(),
        'clienturi': rail.result('gather_client_uri_to_process')[0],
        'projecturi': rail.result('search_projects')[0]['uri'] if rail.result('search_projects') else null
    }


def get_search_users_payload(dag_run):
    return {
        "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled"
                ],
        "sort": [],
        "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
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
                            "text": dag_run.conf['projectmanagerid'],
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
    }


def get_department_uri_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:department-group-list-column:department-group",
            "urn:replicon:department-group-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:department-group-list-filter:text"
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
                    "text": 'MPC - Advertising' if dag_run.conf['millmpc'] == 'mpc' else 'The Mill (Advertising)',
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
    }


def get_create_project_payload(dag_run, caller):
    projectstatus = dag_run.conf['projectstatus'].lower()
    departmenturi = rail.result('get_department_uri_for_teammember_assignment')[
        0]['uri'] if rail.result('get_department_uri_for_teammember_assignment') else null

    def get_customfields_to_apply():
        customfields_list = []
        if rail.result('get_enabled_dropdown_mill_mpc'):
            customfields_list.append(rail.result(
                'get_enabled_dropdown_mill_mpc'))
        if rail.result('get_enabled_dropdown_project_buckets'):
            customfields_list.append(rail.result(
                'get_enabled_dropdown_project_buckets'))
        if rail.result('add_project_type_customfields'):
            customfields_list.append(rail.result(
                'add_project_type_customfields'))
        if rail.result('add_project_classification_customfields'):
            customfields_list.append(rail.result(
                'add_project_classification_customfields'))
        if rail.result('get_enabled_dropdown_product_name'):
            customfields_list.append(rail.result(
                'get_enabled_dropdown_product_name'))
        return customfields_list

    return {
        "target": null,
        "modifications": {
            "nameToApply": {
                "value": dag_run.conf['projectname'] + " | " + dag_run.conf['productname'] if dag_run.conf['productname'] else dag_run.conf['projectname']
            },
            "codeToApply": {
                "value": str(dag_run.conf['projectcode'])
            },
            "descriptionToApply": null,
            "percentCompletedToApply": "0",
            "startDateToApply": null,
            "endDateToApply": null,
            "billingTypeToApply": {
                "value": "urn:replicon:billing-type:time-and-material"
            },
            "clientBillingAllocationMethodToApply": "urn:replicon:client-billing-allocation-method:split",
            "clientAssignmentsSchedulesToApply": null,
            "statusToApply": {
                "uri": null,
                "name": 'In Progress' if projectstatus in ('new', 'confirmed') else 'Completed'
            },
            "clientRepresentativeToApply": null,
            "programToApply": null,
            "projectLeaderToApply": rail.result(f'update_project_manager_{caller}'),
            "isProjectLeaderApprovalRequired": "true",
            "costTypeToApply": null,
            "isTimeEntryAllowed": "false",
            "estimatedHoursToApply": null,
            "estimatedCostToApply": null,
            "defaultBillingCurrencyToApply": null,
            "timeAndMaterials": {
                "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                "billingRateFrequency": {
                    "uri": null,
                    "name": "Hourly"
                },
                "billingRateFrequencyDuration": null,
                "billingRates": [
                    {
                        "billingRate": {
                            "uri": "urn:replicon:project-specific-billing-rate",
                            "name": null
                        },
                        "rateSchedule": null
                    }
                ]
            },
            "fixedBid": null,
            "customFieldsToApply": get_customfields_to_apply(),
            "resourceAssignmentModifications": {
                "resourcesToAdd": [
                    {
                        "user": null,
                        "department": null,
                        "placeholder": null,
                        "location": null,
                        "division": null,
                        "costCenter": null,
                        "serviceCenter": null,
                        "employeeType": null,
                        "departmentGroup": {
                            "uri": departmenturi,
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        }
                    }
                ],
                "resourcesToRemove": []
            },
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": []
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_update_department_payload():
    return {
        "projectUri": rail.result('create_project')['uri'],
        "departmentGroup": {
            "uri": rail.result('get_department_uri_for_teammember_assignment')[
                0]['uri'] if rail.result('get_department_uri_for_teammember_assignment') else null,
            "parent": null,
            "name": null,
            "parameterCorrelationId": null
        }
    }


def get_non_billable_task_add_payload(item):
    return {
        'projectname': rail.result('create_project')['name'],
        'projecturi': rail.result('create_project')['uri'],
        'taskname': item['Taskname'],
        'millmpc': rail.result('get_department_uri_for_teammember_assignment')[0]['uri']
    }


def get_put_task_payload(dag_run):
    return {
        "project": {
            "uri": dag_run.conf['projecturi'],
            "name": null,
            "parameterCorrelationId": null
        },
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
            "timeEntryDateRange": null,
            "percentCompleted": "0",
            "isTimeEntryAllowed": "1",
            "estimatedHours": null,
            "isClosed": "0",
            "customFieldValues": [],
            "estimatedCost": null,
            "costTypeUri": null,
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:non-billable",
            "assignedResources": [
                {
                    "uri": null,
                    "resourcePlaceholderParameterCorrelationId": null,
                    "user": null,
                    "department": null,
                    "placeholder": null,
                    "location": null,
                    "division": null,
                    "costCenter": null,
                    "serviceCenter": null,
                    "departmentGroup": {
                        "uri": dag_run.conf['millmpc'],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    },
                    "employeeTypeGroup": null
                }
            ]
        }
    }


def get_apply_new_client_payload(dag_run):
    return {
        "projectUri": rail.result('create_project')['uri'],
        "clientUri": dag_run.conf['clienturi'],
        "optionUri": "urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes"
    }


def get_update_dropdown_value_projecttype(dag_run):
    project_type_dropdown_uri = rail.result('get_dropdownoptions_project_type')['project_type_dropdown_uri'] \
        if rail.result('get_dropdownoptions_project_type') else \
        rail.result('get_enabled_dropdown_project_buckets')[
        'project_type_dropdown_uri'] if rail.result('get_enabled_dropdown_project_buckets') else null
    return {
        "objectUri": dag_run.conf['projecturi'],
        "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customfields'], 'name',
                                                               'Project Type', 'customfielduri'),
        "customFieldDropDownOptionUri": project_type_dropdown_uri
    }


def get_update_dropdown_value_project_classification(dag_run):
    project_classification_dropdown_uri = rail.result('get_dropdownoptions_project_classification')['project_classification_dropdown_uri'] \
        if rail.result('get_dropdownoptions_project_classification') else \
        rail.result('get_enabled_dropdown_project_classification')[
        'project_classification_dropdown_uri'] if rail.result('get_enabled_dropdown_project_classification') else null
    return {
        "objectUri": dag_run.conf['projecturi'],
        "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customfields'], 'name',
                                                               'Project Classification', 'customfielduri'),
        "customFieldDropDownOptionUri": project_classification_dropdown_uri
    }


def get_update_product_name_payload(dag_run):
    return {
        "objectUri": dag_run.conf['projecturi'],
        "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customfields'], 'name', 'Product Name', 'customfielduri'),
        "value": dag_run.conf['productname']
    }


def get_update_project_name_payload(dag_run):
    return {
        "projectUri": dag_run.conf['projecturi'],
        "name": dag_run.conf['projectname'] + " | " + dag_run.conf['productname'] if dag_run.conf['productname'] else dag_run.conf['projectname']
    }
