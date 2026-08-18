import uuid
import rail
from pimco.create_new_task_consultant.utils import custom_methods

null = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_model_task_payload():
    return {
        "taskUris": list(map(lambda item: item["taskuri"], custom_methods.read_collection(get_dag_run_conf()["creatable_tasks"])))
    }


def get_resource_assignment_payload():
    return {
        "taskUris": list(map(lambda item: item["taskuri"], custom_methods.read_collection(get_dag_run_conf()["creatable_tasks"]))),
        "asOfDate": null
    }


def get_custom_fields_payload():
    return {
        "page": "1",
        "pagesize": "5000",
        "columnUris": [
            "urn:replicon:task-custom-field-list-column:task-custom-field"
        ],
        "sort": [],
        "filterExpression": null
    }


def get_bulk_tasks_payload():
    return {
        "pageIndex": "1",
        "pageSize": "10000",
        "projectUris": [
            get_dag_run_conf()["project_task_data"]["project_uri"]
        ],
        "taskDataInclusionOptionUris": []
    }


def get_parent_uri(parent_task):
    return " ".join(list(map(lambda all_tasks: all_tasks["uri"], filter(lambda all_tasks: parent_task == all_tasks["name"], rail.result("get_task_details")))))


def get_task_hierarchy():
    return {
        "taskHierarchy": [
            {
                "target": {
                    "parent": {
                        "uri": get_parent_uri(task_details["parent_task"]),
                    }
                } if task_details["parent_task"] and get_parent_uri(task_details["parent_task"]) is not null
                and get_parent_uri(task_details["parent_task"]) != "" else null,
                "taskModificationToApply": {
                    "name": task_details["task_name"],
                    "codeToApply": {
                        "value": task_details["task_code"]
                    },
                    "descriptionToApply": {
                        "value": task_details["task_description"]
                    },
                    "isClosed": bool(task_details["task_status"] != "Open"),
                    "timeEntryStartDateToApply": {
                        "date": {
                            "year": int(task_details["start_date_year"]),
                            "month": int(task_details["start_date_month"]),
                            "day": int(task_details["start_date_day"])
                        }
                    } if task_details["start_date"] is not None and task_details["start_date"] != "" else null,
                    "timeEntryEndDateToApply": {
                        "date": {
                            "year": int(task_details["end_date_year"]),
                            "month": int(task_details["end_date_month"]),
                            "day": int(task_details["end_date_day"])
                        }
                    } if task_details["end_date"] is not None and task_details["end_date"] != "" else null,
                    "timeAndExpenseEntryTypeToApply": {
                        "value": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable" if task_details["entry_type"] == "Billable & Non-Billable"
                        else ("urn:replicon:time-and-expense-entry-type:non-billable" if task_details["entry_type"] == "Non-Billable"
                              else ("urn:replicon:time-and-expense-entry-type:billable" if task_details["entry_type"] == "Billable Only" else null))
                    },
                    "isTimeEntryAllowed": task_details["is_time_entry_allowed"],
                    "costTypeToApply": {
                        "value": ("urn:replicon:cost-type:capital" if task_details["cost_type"] == "CapEx" else "urn:replicon:cost-type:operational")
                        if task_details["cost_type"] is not null and task_details["cost_type"] != "" else null
                    },
                    "estimatedHoursToApply": {
                        "duration": {
                            "hours": int(float(task_details["estimated_hours"])) if task_details["estimated_hours"] is not null
                                    and task_details["estimated_hours"] != "" else 0,
                            "minutes": 0,
                            "seconds": 0
                        }
                    },
                    "estimatedCostToApply": {
                        "value": {
                            "amount": float(task_details["market_rate"].replace("USD$", "").replace(",", "")) if task_details["market_rate"] is not null
                                    and task_details["market_rate"] != "" else null,
                            "currency": {
                                "uri": task_details["currency_uri"]
                            }
                        }
                    },
                    "resourceTaskAssignmentModifications": {
                        "resourceAllocationsToAdd": [
                            {
                                "resource": {
                                    "departmentGroup": {
                                        "uri": task_details["resource_assignment"]
                                    }
                                }
                            }
                        ]
                    } if task_details["resource_assignment"] is not null and task_details["resource_assignment"] != "" else null,
                    "customFieldsToApply": [
                        {
                            "customField": {
                                "uri": task_details["org_cost_uri"]
                            },
                            "text": null,
                            "dropDownOption": {
                                "name": task_details["org_costs"]
                            }
                        },
                        {
                            "customField": {
                                "uri": task_details["lux_flag_uri"]
                            },
                            "text": task_details["lux_flag"],
                            "dropDownOption": {
                                "name": null
                            }
                        }
                    ]
                }
            } for task_details in get_dag_run_conf()["project_task_data"]["task_details"]
        ]
    }


def get_add_task_payload():
    return {
        "project": {
            "uri": get_dag_run_conf()["project_task_data"]["project_uri"],
        },
        "taskHierarchy": get_task_hierarchy()["taskHierarchy"],
        "taskModificationOptionUri": "urn:replicon:task-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }
