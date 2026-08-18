# pylint:disable = too-many-statements
from datetime import datetime
import uuid
import rail

null = None
DATE_FORMAT = "%m/%d/%Y"

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def get_replicon_date(input_date):
    return rail.parse_date(input_date, DATE_FORMAT)

def get_today_date():
    now = datetime.now()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }

def get_client_data():
    return {
            "page": "1",
            "pagesize": "10000",
            "columnUris": [
                "urn:replicon:client-list-column:client",
                "urn:replicon:client-list-column:code"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                "filterDefinitionUri": "urn:replicon:client-list-filter:code"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                "value": {
                    "text": rail.result("get_query_data")['clientcode']
                }
                }
            }
        }

def get_create_client_payload(dag_run):
    return {
        "modifications": {
            "nameToApply": {
                "value": rail.result('get_query_data')['clientname']
            },
            "codeToApply": {
                "value": rail.result('get_query_data')['clientcode']
            },
            "customFieldsToApply": [{
                "customField": {
                    "uri": dag_run.conf['clientpriduri']
                },
                "text": rail.result('get_query_data')['clientprid']
                }] if dag_run.conf['clientpriduri'] and rail.result('get_query_data')['clientprid'] else []
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_update_client_payload(dag_run):
    return {
        "target": {
            "uri": rail.result('get_clients_in_replicon')[0]['uri']
        },
        "modifications": {
            "nameToApply": {
                "value": rail.result('get_query_data')['clientname']
            },
            "customFieldsToApply": [{
                "customField": {
                    "uri": dag_run.conf['clientpriduri']
                },
                "text": rail.result('get_query_data')['clientprid']
                }] if dag_run.conf['clientpriduri'] and rail.result('get_query_data')['clientprid'] else []
        },
        "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_process_project_conf(item, location):
    return {
        **item,
        **{
            'project_type_oef_uri': rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_project_object_extension_field_details"),  "name", "Type", "uri"),
            'engagement_partner_udf_uri': rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_project_custom_fields"),  "displayText", "Engagement Partner", "uri"),
            'project_type': rail.find_first_by_attr_and_get_attr(
                        rail.result('get_object_extension_tag_definition_details')['tags'],
                        "name", item['type'], "uri"),
            'project_manager_permission_uri': rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_permission_set"),  "displayText", "Engagement Manager", "uri"
            ),
            'project_co_manager_permission_uri': rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_permission_set"),  "displayText", "Engagement Partner", "uri"
            ),
            'lanaclos_uri': rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_project_custom_fields"),
                "displayText", "LAN AC LOS", "uri") if item['lanaclos'] else null,
            'lanaclos_value_uri': rail.find_first_by_attr_and_get_attr(
                        rail.result('get_lan_ac_los_custom_field_dropdown_options'),
                        "displayText", item['lanaclos'], "uri"),
            'lanacprojecttype_uri': rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_project_custom_fields"),
                "displayText", "LAN AC Project Type", "uri") if item['lanacprojecttype'] else null,
            'lanacprojecttype_value_uri': rail.find_first_by_attr_and_get_attr(
                        rail.result('get_lan_ac_project_type_custom_field_dropdown_options'),
                        "displayText", item['lanacprojecttype'], "uri"),
            'project_belongs_to': rail.find_first_by_attr_and_get_attr(
                rail.result("get_all_locations"),
                "displayText", location, "uri"),
            'exception_log' : rail.result('create_exception_log')
        }
    }

def get_process_client_conf(item):
    get_client_udfs = rail.result('get_client_udfs')
    return {
        **item,
        **{
            'clientpriduri': get_client_udfs['clientpriduri'],
            'exception_log' : rail.result('create_exception_log')
        }
    }

def does_project_code_exist():
    return bool(rail.result('get_project_details'))

def get_create_project_target_param():
    if does_project_code_exist():
        return {
            "uri": rail.result('get_project_details')['uri']
        }
    return None

def get_time_and_expense_entry(time_expenseentry):
    if time_expenseentry and time_expenseentry.lower() == "billable only":
        return "urn:replicon:time-and-expense-entry-type:billable"
    if time_expenseentry and time_expenseentry.lower() == "non-billable":
        return "urn:replicon:time-and-expense-entry-type:non-billable"
    return "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"

def get_projectleadertoapply_param():
    if get_task_state('log_project_manager_present_and_enabled') == 'success':
        if not does_project_code_exist():
            return {
                    "user": {
                        "uri": rail.result('search_projectmanager_by_partyid_and_legal_entity')[0]['projectmanager_uri']
                    }
                }
        if does_project_code_exist() and (not rail.result('get_project_details')['projectLeader'] or \
            rail.result('get_project_details')['projectLeader']['uri'] != rail.result(
            'search_projectmanager_by_partyid_and_legal_entity')[0]['projectmanager_uri']):
            return {
                "user": {
                    "uri": rail.result('search_projectmanager_by_partyid_and_legal_entity')[0]['projectmanager_uri']
                }
            }
    return null

def get_custom_fields_params(dag_run):
    resp = []
    if dag_run.conf['engagementpartner_partyid'] or dag_run.conf['engagementpartner_legalentity']:
        resp.append({
            "customField": {
                "uri": dag_run.conf['engagement_partner_udf_uri']
            },
            "text": "".join([dag_run.conf['engagementpartner_partyid'],dag_run.conf['engagementpartner_legalentity']])
        })
    if dag_run.conf['lanaclos_uri'] and dag_run.conf['lanaclos_value_uri']:
        resp.append({
            "customField": {
                "uri": dag_run.conf['lanaclos_uri']
            },
            "dropDownOption": {
                "uri": dag_run.conf['lanaclos_value_uri']
            }
        })
    if dag_run.conf['lanacprojecttype_uri'] and dag_run.conf['lanacprojecttype_value_uri']:
        resp.append({
            "customField": {
                "uri": dag_run.conf['lanacprojecttype_uri']
            },
            "dropDownOption": {
                "uri": dag_run.conf['lanacprojecttype_value_uri']
            }
        })
    return resp

def create_or_update_project_payload(dag_run, project_belongs_to):
    status= {'Open': 'In Progress', 'Closed': 'Completed'}
    def get_client_payload():
        return {
            "clients": [
                {
                    "client": {
                        "code": dag_run.conf['clientcode'],
                    },
                    "costAllocationPercentage": "100"
                }
            ],
            "effectiveDate": None
        } if dag_run.conf['clientcode'] else None

    modifications = {
        "nameToApply": {
            "value": dag_run.conf["projectname"]
        },
        "codeToApply":  {
            "value": dag_run.conf["projectcode"]
        } if not does_project_code_exist() else None,
        "startDateToApply": {
            "date": get_replicon_date(dag_run.conf['startdate'])
        } if dag_run.conf['startdate'] else null,
        "endDateToApply": {
            "date": get_replicon_date(dag_run.conf['enddate'])
        } if dag_run.conf['enddate'] else null,
        "statusToApply": {
            "name": status.get(dag_run.conf['status'], 'In Progress')
        },
        "clientAssignmentsSchedulesToApply": get_client_payload(),
        "isProjectLeaderApprovalRequired": "false",
        "projectLeaderToApply": get_projectleadertoapply_param(),
        "isTimeEntryAllowed": "false",
        "timeAndMaterials": {
            "timeAndExpenseEntryTypeUri": get_time_and_expense_entry(dag_run.conf['time_expenseentry'])
        },
        "objectExtensionFieldsToApply": [
            {
                "definition": {
                    "uri": dag_run.conf['project_type_oef_uri']
                },
                "tag": {
                    "uri": dag_run.conf['project_type'],
                }
            }
        ] if dag_run.conf['project_type'] else null,
        "locationToApply": {
            "location": {
                "name": project_belongs_to
            }
        },
        "keyValuesToApply": [
            {
                "keyUri": "urn:replicon:project-key-value-key:project-team-member-assignment-type",
                "value": {
                    "uri": "urn:replicon:project-team-member-assignment-type:automatically-assign-task",
                    "collection": []
                }
            }
        ],
        "customFieldsToApply": get_custom_fields_params(dag_run),
    }

    return {
        "target": get_create_project_target_param(),
        "modifications": modifications,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_projectmanager_by_partyid_and_legal_entity_uri_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:division"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": dag_run.conf['projectmanager_partyid']
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:divisions"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "text": dag_run.conf['projectmanager_legalentityid']
                    }
                }
            }
        }
    }

def get_engagementpartner_by_partyid_and_legal_entity_uri_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "1000000",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:enabled",
            "urn:replicon:user-list-column:division"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                    "value": {
                        "text": dag_run.conf['engagementpartner_partyid']
                    }
                }
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:divisions"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "value": {
                        "text": dag_run.conf['engagementpartner_legalentity']
                    }
                }
            }
        }
    }

def get_project_log_status(dag_run):
    status = "Success"
    if not dag_run.conf['project_type']:
        status = "Exception"
    if dag_run.conf['lanaclos'] and not dag_run.conf['lanaclos_value_uri']:
        status = "Exception"
    if dag_run.conf['lanacprojecttype'] and not dag_run.conf['lanacprojecttype_value_uri']:
        status = "Exception"
    return status

def get_project_log_details(dag_run):
    details = []
    if does_project_code_exist():
        details.append("Project Updated Successfully")
    else:
        details.append("Project Added Successfully")
    if not dag_run.conf['project_type']:
        msg = f"Type {dag_run.conf['type']} does not present in Replicon"
        details.append(msg)
    if dag_run.conf['lanaclos'] and not dag_run.conf['lanaclos_value_uri']:
        msg = f"LAN AC LOS {dag_run.conf['lanaclos']} does not present in Replicon"
        details.append(msg)
    if dag_run.conf['lanacprojecttype'] and not dag_run.conf['lanacprojecttype_value_uri']:
        msg = f"LAN AC Project Type {dag_run.conf['lanacprojecttype']} does not present in Replicon"
        details.append(msg)
    return "-".join(details)

def get_task_target(action,task,project_uri):
    taskname = task['taskname']
    if action == "add" and len(taskname) == 1:
        return null
    return {
        "uri": None if action == "add" else task['uri'],
        "parent": {
          "name": taskname[0],
          "project": {
            "uri": project_uri
          }
        } if len(taskname) == 2 and action == "add" else null
    }

def get_task_payload(action,data,project_uri, project_belongs_to=None):
    return list(map(lambda task: {
        "target": get_task_target(action,task,project_uri),
        "taskModificationToApply": {
                "name": null if action == "update" else task['taskname'][1] \
                    if len(task['taskname']) == 2 else task['taskname'][0],
                "codeToApply": {
                    "value": task['taskcode']
                },
                "isClosed": 0,
                "timeAndExpenseEntryTypeToApply": {
                    "value": get_time_and_expense_entry(task['time_expenseentry'])
                } if action == "add" else null,
                "isTimeEntryAllowed": "1",
                "timeEntryStartDateToApply": {
                    "date": get_replicon_date(task['startdate'])
                },
                "timeEntryEndDateToApply": {
                    "date": get_replicon_date(task['enddate']) if task['allowtimeentry'] == "Yes" else get_today_date()
                },
                "resourceTaskAssignmentModifications": {
                    "resourceAllocationsToAdd": [
                        {
                            "resource": {
                                "location": {
                                    "name": project_belongs_to
                                },
                            }
                        }
                    ]
                } if action == "add" else null,
            }
    }, data))

def get_update_task_payload():
    project_uri = rail.result('update_project')['uri'] if does_project_code_exist() else rail.result('create_project')['uri']
    return {
        "project": {
            "uri": project_uri,
        },
        "taskHierarchy": get_task_payload("update",
                                          rail.result("get_all_task_to_add_update")['tasks_to_update'],
                                          project_uri),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_add_task_payload(project_belongs_to):
    project_uri = rail.result('update_project')['uri'] if does_project_code_exist() else rail.result('create_project')['uri']
    return {
        "project": {
            "uri": project_uri,
        },
        "taskHierarchy": get_task_payload("add",
                                          rail.result("get_all_task_to_add_update")['tasks_to_add'],
                                          project_uri, project_belongs_to),
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }
