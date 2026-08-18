from datetime import datetime
import uuid
import rail
from dateutil.relativedelta import relativedelta


null=None

def get_user_payload(dag_run):
    return {
            "page": "1",
            "pagesize": "10",
            "columnUris": [
                "urn:replicon:user-list-column:user"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                "value": {
                    "text": dag_run.conf["projectleader"]
                },
                "filterDefinitionUri": null
                },
                "value": null,
                "filterDefinitionUri": null
            }
        }

def create_project_request(dag_run):
    project_request = {
        "modifications": {
            "nameToApply": {
                "value": dag_run.conf["projectdescription"]
            },
            "codeToApply": {
                "value": dag_run.conf["projectcode"]
            },
            "statusToApply": {
                "name": "In Progress" if dag_run.conf["projectstatus"] == "AVAILABLE" else "Completed"
            },
            "costTypeToApply": {
                "uri": "urn:replicon:cost-type:operational" if dag_run.conf["costtype"] == "OPEX" else "urn:replicon:cost-type:capital"
            },
            "isTimeEntryAllowed": "false",
            "objectExtensionFieldsToApply": [
                {
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_definitions_for_project"),
                                "displayText", "Source System", "uri"),
                    },
                    "tag": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_tags_for_source_system"),
                                "displayText", dag_run.conf["originsystem"], "uri"),
                    },
                },
                {
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_definitions_for_project"),
                                "displayText", "PS Family", "uri"),
                    },
                    "tag": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_tags_for_psfamily"),
                                "displayText", dag_run.conf["psfamily"], "uri"),
                    },
                },
                {
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_definitions_for_project"),
                                "displayText", "BFC Global Business Unit", "uri"),
                    },
                    "tag": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_tags_for_gbu"),
                                "displayText", dag_run.conf["gbu"], "uri"),
                    },
                },
                {
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_definitions_for_project"),
                                "displayText", "Business Unit", "uri"),
                    },
                    "textValue": dag_run.conf["bu"],
                }
            ],
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }
    if rail.result("get_project_leader_uri_if_present"):
        project_request["modifications"].update({"projectLeaderToApply": {
                "user": {"uri": rail.result("get_project_leader_uri_if_present") }
            }
        })
    if rail.load_all_records(rail.result("query_service_center_collection")):
        project_request["modifications"].update({"resourceAssignmentModifications": {
                "resourcesToAdd": [
                    {
                        "serviceCenter": {
                            "name":rail.load_all_records(rail.result("query_service_center_collection"))[0]["name"]
                        },
                    }
                ]
            }})
    return project_request

def update_project_request(dag_run):
    project_update_request = {}
    project_update_request = {
        "target": {
            "uri": rail.result("get_bulk_project_details"),
            "name": null,
            "code": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "costTypeToApply": {
                "uri": "urn:replicon:cost-type:operational" if dag_run.conf["costtype"] == "OPEX" else "urn:replicon:cost-type:capital"
            },
            "isTimeEntryAllowed": "true",
            "objectExtensionFieldsToApply": [
                {
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_definitions_for_project"),
                                "displayText", "BFC Global Business Unit", "uri"),
                    },
                    "tag": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_tags_for_gbu"),
                                "displayText", dag_run.conf["gbu"], "uri"),
                    },
                }
            ],
        },
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }

    if rail.result("get_project_leader_uri_if_present"):
        project_update_request["modifications"].update({"projectLeaderToApply": {
                "user": {"uri": rail.result("get_project_leader_uri_if_present") }
            }
        })
    if rail.load_all_records(rail.result("query_service_center_collection")):
        project_update_request["modifications"].update({"resourceAssignmentModifications": {
                "resourcesToAdd": [
                    {
                        "serviceCenter": {
                            "name":rail.load_all_records(rail.result("query_service_center_collection"))[0]["name"]
                        },
                    }
                ]
            }})

    return project_update_request

def get_status(dag_run):
    if dag_run.conf["wbsstatus"] != "AVAILABLE":
        return "true"
    return "false"

def create_task_request(dag_run):
    task_request = {
        "target": null,
        "project": {
            "uri": rail.result("create_project")
        },
        "modifications": {
            "name": dag_run.conf["wbsdescription"],
            "codeToApply": {
                "value": dag_run.conf["wbscode"]
            },
            "descriptionToApply": null,
            "isClosed": get_status(dag_run),
            "timeEntryStartDateToApply": null,
            "timeEntryEndDateToApply": null,
            "timeAndExpenseEntryTypeToApply": {
                "value": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
            },
            "isTimeEntryAllowed": "true",
            "costTypeToApply": {
                "value": "urn:replicon:cost-type:operational" if dag_run.conf["costtype"] == "OPEX" else "urn:replicon:cost-type:capital"
            },
            "customFieldsToApply": [],
            "keyValuesToApply": [],
            "objectExtensionFieldsToApply": [
                {
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_definitions_for_task"),
                                "displayText", "BFC Business Unit", "uri")
                    },
                    "textValue": dag_run.conf["bu"]
                },
                {
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_definitions_for_task"),
                                "displayText", "BFC GBU code", "uri")
                    },
                    "textValue": dag_run.conf["gbu"]
                },
                {
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_definitions_for_task"),
                                "displayText", "Legal Entity Code", "uri")
                    },
                    "textValue": dag_run.conf["companycode"]
                },
                {
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_definitions_for_task"),
                                "displayText", "Controlling Area", "uri")
                    },
                    "textValue": dag_run.conf["controllingarea"]
                },
                {
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_definitions_for_task"),
                                "displayText", "Obj Class", "uri")
                    },
                    "textValue": dag_run.conf["objectclass"]
                },
                {
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_definitions_for_task"),
                                "displayText", "WEGO Project NR", "uri")
                    },
                    "textValue": "",
                },
                {
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_definitions_for_task"),
                                "displayText", "Log System", "uri")
                    },
                    "tag": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_tags_for_log_system"),
                                "displayText", dag_run.conf["originsystem"], "uri")
                        }
                },
            ]
        },
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }
    if "gbu" in dag_run.conf:
        task_request["modifications"]["objectExtensionFieldsToApply"].append({
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_definitions_for_task"),
                                "displayText", "BFC GBU code", "uri")
                    },
                    "tag": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_tags_for_log_system"),
                                "displayText", dag_run.conf["gbu"], "uri")
                        }
                })
    if "bu" in dag_run.conf:
        task_request["modifications"]["objectExtensionFieldsToApply"].append({
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_definitions_for_task"),
                                "displayText", "BFC Business Unit", "uri")
                    },
                    "tag": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_tags_for_log_system"),
                                "displayText", dag_run.conf["bu"], "uri")
                        }
                })
    if "projectcode" in dag_run.conf and dag_run.conf["projectcode"]:
        task_request["modifications"]["objectExtensionFieldsToApply"].append({
                    "definition": {
                        "uri": rail.find_first_by_attr_and_get_attr(
                                rail.result("get_object_extension_definitions_for_task"),
                                "displayText", "Project ID", "uri")
                    },
                    "textValue": dag_run.conf["projectcode"],
                })
    if rail.load_all_records(rail.result("query_service_center_collection")):
        task_request["modifications"].update({"resourceTaskAssignmentModifications": {
            "resourceAllocationsToAdd": [
                {
                    "resource":{
                        "serviceCenter": {
                            "name":rail.load_all_records(rail.result("query_service_center_collection"))[0]["name"]
                        },
                    },
                },
            ]
        }})
    return task_request

def time_entry_end_date():
    previous_month_date = (datetime.today()+relativedelta(months=-1))
    current_date = datetime.today()
    if datetime.today().day > 24:
        return {
            "date": {
                "year": previous_month_date.year,
                "month": previous_month_date.month,
                "day": 24
            }
        }
    return {
        "date": {
            "year": current_date.year,
            "month": current_date.month,
            "day": 1
        }
    }


def update_task_request(dag_run):
    task_request=  {
                "project": {
                    "uri": rail.result("get_bulk_project_details")
                },
                "modifications": {
                    "name": dag_run.conf["wbsdescription"],
                    "codeToApply": {
                        "value": rail.find_first_by_attr_and_get_attr(rail.result("get_bulk_task_details"),
                                                                      "code", dag_run.conf["wbscode"], "code") or dag_run.conf["wbscode"]
                    },
                    "descriptionToApply": null,
                    "isClosed": get_status(dag_run),
                    "timeEntryEndDateToApply":  time_entry_end_date() if dag_run.conf["wbsstatus"] == "CLOSED" else null,
                    "timeAndExpenseEntryTypeToApply": {
                        "value": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
                    },
                    "isTimeEntryAllowed": "true" if not dag_run.conf["accoladeprojectid"] else "false",
                    "costTypeToApply": {
                        "value": "urn:replicon:cost-type:operational" if dag_run.conf["costtype"] == "OPEX"
                          else "urn:replicon:cost-type:capital"
                    },
                    "estimatedHoursToApply": null,
                    "estimatedCostToApply": null,
                    "resourceAssignmentModifications": null,
                    "customFieldsToApply": [],
                    "keyValuesToApply": [],
                    "objectExtensionFieldsToApply": [
                        {
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "BFC Business Unit", "uri")
                            },
                            "textValue": dag_run.conf["bu"]
                        },
                        {
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "Legal Entity Code", "uri")
                            },
                            "textValue": dag_run.conf["companycode"]
                        },
                        {
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "Controlling Area", "uri")
                            },
                            "textValue": dag_run.conf["controllingarea"]
                        },
                        {
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "Obj Class", "uri")
                            },
                            "textValue": dag_run.conf["objectclass"]
                        },
                        {
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "Project ID", "uri")
                            },
                            "textValue": dag_run.conf["projectcode"] if not dag_run.conf["accoladeprojectid"] else dag_run.conf["accoladeprojectid"],
                        },
                        {
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "Log System", "uri")
                            },
                            "tag": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_tags_for_log_system"),
                                        "displayText", dag_run.conf["originsystem"], "uri")
                                }
                        },
                    ]
                },
                "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }
    if rail.result("get_bulk_task_details") and rail.find_first_by_attr_and_get_attr(
        rail.result("get_bulk_task_details"), "code", dag_run.conf["wbscode"], "uri"):
        task_request.update(
            {
                "target": {
                    "uri": rail.find_first_by_attr_and_get_attr(
                            rail.result("get_bulk_task_details"), "code", dag_run.conf["wbscode"], "uri")
                }
        })
    else:
        task_request.update(
            {
                "target": null
        })
    if rail.load_all_records(rail.result("query_service_center_collection")):
        task_request["modifications"].update({"resourceTaskAssignmentModifications": {
            "resourceAllocationsToAdd": [
                {
                    "resource":{
                        "serviceCenter": {
                            "name":rail.load_all_records(rail.result("query_service_center_collection"))[0]["name"]
                        },
                    },
                },
            ]
        }})
    oef = validate_and_add_oef(dag_run)
    if oef:
        task_request["modifications"]["objectExtensionFieldsToApply"].extend(oef)
    return task_request

def validate_and_add_oef(dag_run):
    update_oef_request = []
    if dag_run.conf["gbu"]:
        update_oef_request.append({
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "BFC GBU code", "uri")
                            },
                            "textValue": dag_run.conf["gbu"]
                        })
    if dag_run.conf["wegoprojectnr"]:
        update_oef_request.append(                {
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "WEGO Project NR", "uri")
                            },
                            "textValue": dag_run.conf["wegoprojectnr"],
                        })
    if dag_run.conf["sapwbsstatus"] and rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_tags_for_log_system"),
                                        "displayText", dag_run.conf["sapwbsstatus"], "uri"):
        update_oef_request.append({
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "WBS Status", "uri")
                            },
                            "tag": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_tags_for_log_system"),
                                        "displayText", dag_run.conf["sapwbsstatus"], "uri")
                                }
                        })
    if dag_run.conf["originsystem"] and rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_tags_for_log_system"),
                                        "displayText", dag_run.conf["originsystem"], "uri"):
        update_oef_request.append({
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "Log System", "uri")
                            },
                            "tag": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_tags_for_log_system"),
                                        "displayText", dag_run.conf["originsystem"], "uri")
                                }
                        })
    if dag_run.conf["gbu"] and rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_tags_for_log_system"),
                                        "displayText", dag_run.conf["gbu"], "uri"):
        update_oef_request.append({
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "BFC GBU description", "uri")
                            },
                            "tag": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_tags_for_log_system"),
                                        "displayText", dag_run.conf["gbu"], "uri")
                                }
                        })
    if dag_run.conf["psfamily"] and rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_tags_for_psfamily"),
                                        "displayText", dag_run.conf["psfamily"], "uri"):
        update_oef_request.append({
                            "definition":{
                            "uri": rail.find_first_by_attr_and_get_attr(
                                    rail.result("get_object_extension_definitions_for_task"),
                                    "displayText", "PS Family", "uri")
                            },
                            "tag": {
                            "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_tags_for_psfamily"),
                                        "displayText", dag_run.conf["psfamily"], "uri")
                            }
                        })
    return update_oef_request

def get_all_hierarchial_tasks(dag_run):
    task_hierarchy = []
    for i in rail.result("search_object_type_in_mapper"):
        task = {
                "target": {
                    "name": i if rail.find_first_by_attr_and_get_attr(rail.result("get_updated_task_details"), "name", i ,"name") else null,
                    "parent": {
                        "name": dag_run.conf["wbsdescription"],
                        "project": {"uri": rail.result("get_bulk_project_details"),}
                    },
                },
                "taskModificationToApply": {
                        "name": i,
                        "codeToApply": {
                        "value": rail.find_first_by_attr_and_get_attr(rail.result("get_bulk_task_details"),
                                                                      "code", dag_run.conf["wbscode"], "code") or dag_run.conf["wbscode"]
                        },
                    "descriptionToApply": null,
                    "isClosed": get_status(dag_run),
                    "timeEntryEndDateToApply": time_entry_end_date() if dag_run.conf["wbsstatus"] == "CLOSED" else null,
                    "timeAndExpenseEntryTypeToApply": {
                        "value": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
                    },
                    "isTimeEntryAllowed": "true",
                    "costTypeToApply": {
                        "value": "urn:replicon:cost-type:operational" if dag_run.conf["costtype"] == "OPEX" else "urn:replicon:cost-type:capital"
                    },
                    "estimatedHoursToApply": null,
                    "estimatedCostToApply": null,
                    "resourceAssignmentModifications": null,
                    "customFieldsToApply": [],
                    "keyValuesToApply": [],
                    "objectExtensionFieldsToApply": [
                        {
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "Legal Entity Code", "uri")
                            },
                            "textValue": dag_run.conf["companycode"]
                        },
                        {
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "Controlling Area", "uri")
                            },
                            "textValue": dag_run.conf["controllingarea"]
                        },
                        {
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "Obj Class", "uri")
                            },
                            "textValue": dag_run.conf["objectclass"]
                        },
                        {
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "Project ID", "uri")
                            },
                            "textValue": dag_run.conf["projectcode"] if not dag_run.conf["accoladeprojectid"] else dag_run.conf["accoladeprojectid"],
                        },
                        {
                            "definition": {
                                "uri": rail.find_first_by_attr_and_get_attr(
                                        rail.result("get_object_extension_definitions_for_task"),
                                        "displayText", "BFC Business Unit", "uri")
                            },
                            "textValue": dag_run.conf["bu"]
                        },

                    ]
                },
        }
        oef = validate_and_add_oef(dag_run)
        if oef:
            task["taskModificationToApply"]["objectExtensionFieldsToApply"].extend(oef)
        if rail.load_all_records(rail.result("query_service_center_collection")):
            task["taskModificationToApply"].update({
                    "resourceTaskAssignmentModifications": {
                            "resourceAllocationsToAdd": [
                                {
                                    "resource":{
                                        "serviceCenter": {
                                            "name":rail.load_all_records(rail.result("query_service_center_collection"))[0]["name"]
                                        },
                                    },
                                },
                            ]
                        },
                })
        task_hierarchy.append(task)
    return task_hierarchy

def accolade_task_hierarchy_request(dag_run):
    return {
                "project": {
                    "uri": rail.result("get_bulk_project_details")
                },
                "taskHierarchy": get_all_hierarchial_tasks(dag_run),
                "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }

def update_project_with_date_request(dag_run):
    project_request = {}

    project_request = {"target": {
        "uri": rail.result("get_bulk_project_details"),
        "name": null,
        "code": null,
        "parameterCorrelationId": null
    },
    "modifications": {
        "endDateToApply" : time_entry_end_date() if dag_run.conf["projectstatus"] == "CLOSED" else null,
        "statusToApply": {
            "name": "In Progress" if dag_run.conf["projectstatus"] == "AVAILABLE" else "Completed"
        },
        "costTypeToApply": {
            "uri": "urn:replicon:cost-type:operational" if dag_run.conf["costtype"] == "OPEX" else "urn:replicon:cost-type:capital"
        },
        "isTimeEntryAllowed": "false",
        "objectExtensionFieldsToApply": [
            {
                "definition": {
                    "uri": rail.find_first_by_attr_and_get_attr(
                            rail.result("get_object_extension_definitions_for_project"),
                            "displayText", "BFC Global Business Unit", "uri"),
                },
                "tag": {
                    "uri": rail.find_first_by_attr_and_get_attr(
                            rail.result("get_object_extension_tags_for_gbu"),
                            "displayText", dag_run.conf["gbu"], "uri"),
                },
            },
        ],
    },
    "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
    "unitOfWorkId": str(uuid.uuid4())
    }
    if rail.result("get_project_leader_uri_if_present"):
        project_request["modifications"].update({"projectLeaderToApply": {
                "user": {"uri": rail.result("get_project_leader_uri_if_present") }
            }
        })
    if rail.load_all_records(rail.result("query_service_center_collection")):
        project_request["modifications"].update({"resourceAssignmentModifications": {
                "resourcesToAdd": [
                    {
                        "serviceCenter": {
                            "name":rail.load_all_records(rail.result("query_service_center_collection"))[0]["name"]
                        },
                    }
                ]
            }})
    if dag_run.conf["projectstatus"] != "AVAILABLE":
        project_request["modifications"].update({"endDateToApply" : time_entry_end_date()} )
    return project_request
