import uuid
import rail

DATE_FORMAT = "%Y/%m/%d"

def get_client_data(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:client-list-column:client",
            "urn:replicon:client-list-column:code",
            "urn:replicon:client-list-column:client-manager",
            "urn:replicon:client-list-column:name"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:client-list-filter:code"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['client_code']
                }
            }
        }
    }

def get_users_payload():
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:employee-id"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "bool": "true"
                }
            }
        }
    }

def does_wbs_exist():
    return bool(rail.result('get_project_details'))

def get_create_project_target_param():
    if does_wbs_exist():
        return {
            "uri": rail.result('get_project_details')['uri']
        }
    return None

def create_projectorapply_modifications(dag_run):
    status= {'In Execution': 'In Progress', 'Completed': 'Completed', 'Closed': 'Archived'}
    def get_client_payload():
        return {
            "clients": [
                {
                "client": {
                    "code": dag_run.conf['Customer'],
                },
                "costAllocationPercentage": "100"
                }
            ],
            "effectiveDate": None
        } if dag_run.conf['CustomerName'] and dag_run.conf['Customer'] else None

    def get_oef_fields_definitions():
        oef_list = []

        def check_existing_oef_value(oef_id):
            if does_wbs_exist() and rail.result("get_project_details")['extensionFieldValues']:
                return bool(rail.find_first_by_attr_and_get_attr(rail.result(
                    "get_project_details")['extensionFieldValues'], 'definition.displayText', oef_id, 'textValue'))
            return False

        def get_oef_payload(oef_uri, value):
            oef_list.append(
                {
                    "definition": {
                        "uri": oef_uri
                    },
                    "textValue": value
                }
            )

        get_oef_payload(dag_run.conf['controlling_area_oef_uri'],'A000')

        if dag_run.conf['ProjectID'] and not check_existing_oef_value('ProjectID'):
            get_oef_payload(dag_run.conf['project_id_oef_uri'],dag_run.conf[
                'ProjectID']) if not does_wbs_exist() else None

        if dag_run.conf['ProjectName']:
            get_oef_payload(dag_run.conf['project_name_oef_uri'],dag_run.conf[
                'ProjectName'])

        if dag_run.conf['ProjectCategory'] and not check_existing_oef_value('ProjectCategory'):
            get_oef_payload(dag_run.conf['project_category_oef_uri'],dag_run.conf[
                'ProjectCategory']) if not does_wbs_exist() else None

        return oef_list

    modifications = {
        "nameToApply": {
            "value": dag_run.conf["WorkPackagename"]
        },
        "codeToApply":  {
            "value": dag_run.conf["WorkPackageID"]
        } if not does_wbs_exist() else None,
        "startDateToApply": {
            "date": rail.parse_date(dag_run.conf["StartDate"], DATE_FORMAT)
        } if dag_run.conf["StartDate"] else None,
        "endDateToApply": {
            "date": rail.parse_date(dag_run.conf["EndDate"], DATE_FORMAT)
        } if dag_run.conf["EndDate"] else None,
        "clientAssignmentsSchedulesToApply": get_client_payload(),
        "clientRepresentativeToApply": {
            "user": {
                "uri": dag_run.conf['client_manager'],
            }
        } if dag_run.conf['client_manager'] else None,
        "statusToApply": {
            "name": status[dag_run.conf['StageDesc']] if dag_run.conf[
                'StageDesc'] and dag_run.conf['StageDesc'] in ['In Execution','Completed','Closed'] else 'In Progress'
        },
        "projectLeaderToApply": {
            "user": {
                "uri": dag_run.conf['manager_uri'],
            }
        } if dag_run.conf['manager_uri'] else None,
        "isProjectLeaderApprovalRequired": bool(dag_run.conf[
            'SkipProjectManagerApproval'] == "No") if not does_wbs_exist() else None,
        "timeAndMaterials": {
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable"
        } if not does_wbs_exist() else None,
        "defaultBillingCurrencyToApply": {
            "currency": {
                "symbol": "€"
            }
        },
        "serviceCenterToApply": {
            "serviceCenter": {
                "uri": dag_run.conf['service_center_uri'],
            }
        } if dag_run.conf['service_center_uri'] else None,
        "costCenterToApply": {
            "costCenter": {
                "uri": dag_run.conf['cost_center_uri'],
            }
        } if dag_run.conf['cost_center_uri'] else None,
        "isTimeEntryAllowed": "0",
        "objectExtensionFieldsToApply": get_oef_fields_definitions()
    }

    return {
        "target": get_create_project_target_param(),
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_task_payload(dag_run,data):
    return list(map(lambda task: {
        "target": {
            "uri": task['uri'],
        },
        "taskModificationToApply": {
                "name": task['task_name'],
                "isClosed": 0,
                "timeAndExpenseEntryTypeToApply": {
                    "value": "urn:replicon:time-and-expense-entry-type:billable"
                },
                "isTimeEntryAllowed": "1" if task['allow_time_entry'] and task['allow_time_entry'] == "TRUE" else "0",
                "timeEntryStartDateToApply": {
                    "date": rail.parse_date(task['start_date'], DATE_FORMAT)
                } if task['start_date'] else None,
                "timeEntryEndDateToApply": {
                    "date": rail.parse_date(task['end_date'], DATE_FORMAT)
                } if task['end_date'] else None,
                "resourceAssignmentModifications": {
                    "resourcesToAdd": [{
                        "department": {
                                "uri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":department:1"
                            }
                        }] if (dag_run.conf['RestrictTimePosting'] == 'N' and task['work_package']) else [
                        {
                            "user": {
                                "uri": uri,
                            },
                        } for uri in task['resource_uri']
                    ] if task['resource_uri'] else [],
                    "resourcesToRemove": []
                },
                "estimatedHoursToApply": {
                "duration": {
                    "hours": "0",
                    "minutes": "0",
                    "seconds": int(float(task['efforts']) * 3600)
                }
            } if task['efforts'] else None,
        }
    }, data))

def get_update_task_payload(dag_run):
    return {
        "project": {
            "uri": rail.result('create_or_update_project')['uri'],
        },
        "taskHierarchy": get_task_payload(dag_run,rail.result("get_all_task_to_add_update")['tasks_to_update']),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_create_client_payload(dag_run):
    return {
        "client": {
            "target": {
                "code": dag_run.conf['client_code']
            },
            "name": dag_run.conf['client_name'],
            "code": dag_run.conf['client_code'],
            "isActive": True
        }
    }

def get_put_task_data(item,dag_run):
    return {
        "project": {"uri": rail.result("create_or_update_project")['uri']},
        "task": {
            "target": {
                "name":item['task_name'],
                "parent": {
                    "name": item['parent_task_name']
                    } if item['parent_task_name'] else None
            },
            "name": item['task_name'],
            "timeEntryDateRange": {
                "startDate": rail.parse_date(item['start_date'], DATE_FORMAT),
                "endDate": rail.parse_date(item['end_date'], DATE_FORMAT)
            } if item['start_date'] or item['end_date'] else None,
            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
            "percentCompleted": 0,
            "isTimeEntryAllowed": "1" if item['allow_time_entry'] and item['allow_time_entry'] == "TRUE" else "0",
            "isClosed": False,
            "assignedResources": [{
                "department": {
                        "uri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":department:1"
                    }
            }] if (dag_run.conf['RestrictTimePosting'] == 'N' and item['work_package']) else [
                {
                    "user": {
                        "uri": uri,
                    },
                } for uri in item['resource_uri']
            ] if item['resource_uri'] else [],
            "estimatedHours": {
                "hours": '0',
                "minutes": "0",
                "seconds": int(float(item['efforts']) * 3600)
            } if item['efforts'] else None,
        }
    }

def get_restricted_users_billing_rates_payload(item, dag_run):
    resource_list = dag_run.conf.get('task_resource_list', [])
    billing_rates = dag_run.conf.get('billing_rates', [])
    
    all_billing_rates_per_user = [
        data for data in resource_list if data.get('resource_uri') == item.get('resource_uri')
    ]
    
    billing_rate_uri_list = [
        rail.find_first_by_attr_and_get_attr(billing_rates, 'displayText', user_billing.get('role_name'), 'uri') or
        rail.find_first_by_attr_and_get_attr(rail.result("create_resource_billing_rates"), 'displayText', user_billing.get('role_name'), 'uri')
        for user_billing in all_billing_rates_per_user
    ]

    return {
        "projectTeamMemberBillingRate": {
            "projectUri": rail.result("create_or_update_project")['uri'],
            "resourceUri": item['resource_uri'],
            "billingRateUris": billing_rate_uri_list,
            "billingRateCopyOptionUri":"urn:replicon:billing-rate-copy-option:do-not-copy-billing-rates-from-client",
        }
    }

def get_all_users_billing_rates_payload(dag_run):
    billing_rates = dag_run.conf.get('billing_rates', [])
    project_resource_list = dag_run.conf.get('project_resource_list', [])
    
    billing_rate_uri_list = [
        rail.find_first_by_attr_and_get_attr(billing_rates, 'displayText', user_billing.get('role_name'), 'uri') or
        rail.find_first_by_attr_and_get_attr(rail.result("create_resource_billing_rates"), 'displayText', user_billing.get('role_name'), 'uri')
        for user_billing in project_resource_list
    ]

    return {
        "projectTeamMemberBillingRate": {
            "projectUri": rail.result("create_or_update_project")['uri'],
            "resourceUri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":department:1",
            "billingRateUris": billing_rate_uri_list if billing_rate_uri_list else [],
            "billingRateCopyOptionUri":"urn:replicon:billing-rate-copy-option:do-not-copy-billing-rates-from-client",
        }
    }
