null = None
import rail

def build_task_resource_payload(item):
    task_name_chain = [task.strip() for task in item["task_name_full_path"].split("/")]
    task_name = task_name_chain[-1]
    parent_task_name_chain = task_name_chain[:-1]
    project_code = item["project_code"]
    employee_id = item["employee_id"]

    project = {
        "uri": null,
        "name": null,
        "code": project_code,
        "parameterCorrelationId": null,
    }

    parent = null
    for index, name in enumerate(parent_task_name_chain):
        node = {
            "uri": null,
            "name": name,
            "parent": parent,
            "project": project if index == 0 else null,
            "parameterCorrelationId": null,
        }
        parent = node

    return {
        "task": {
            "uri": null,
            "name": task_name,
            "parent": parent,
            "project": project if not parent_task_name_chain else null,
            "parameterCorrelationId": null,
        },
        "user": {
            "uri": null,
            "loginName": null,
            "employeeId": employee_id,
            "parameterCorrelationId": null,
        },
        "parameterCorrelationId": null,
    }


def get_download_batch(script_uri, batch_uri):
    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export"
            },
            "operatorUri": "urn:replicon:filter-operator:in",
            "rightExpression": {
                "value": {
                    "uris": [batch_uri],
                },
            },
        },
        "fileFormatScriptUri": script_uri,
    }

def time_export_generate_request(dag_run,payload_type="time_export", financial_system="PeopleSoft"):
    start_date = rail.parse_date(
        dag_run.conf["start_date"], "%Y-%m-%d"
    )
    end_date = rail.parse_date(dag_run.conf["end_date"], "%Y-%m-%d")

    filterexpression = {
        "leftExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:entry-date-range"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "dateRange": {
                                "startDate": start_date,
                                "endDate": end_date,
                                "relativeDateRangeUri": null,
                                "relativeDateRangeAsOfDate": null,
                            }
                        }
                    },
                },
                "operatorUri": "urn:replicon:filter-operator:and",
                "rightExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export-status"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "value": {
                            "uris": [
                                "urn:replicon:time-data-item-time-data-export-status:none"
                            ]
                        }
                    },
                },
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:timesheet-only-approval-status"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {"uris": ["urn:replicon:approval-status:approved"]}
                },
            },
        },
        "operatorUri": "urn:replicon:filter-operator:and",
        "rightExpression": {
            "leftExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:service-center"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "uris": [
                            rail.find_first_by_attr_and_get_attr(
                                rail.result("get_all_service_centers"),
                                "displayText",
                                financial_system,
                                "uri",
                            )
                        ]
                    }
                },
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-entry-type"
                },
                "operatorUri": "urn:replicon:filter-operator:in",
                "rightExpression": {
                    "value": {
                        "uris": [
                            "urn:replicon:time-entry-type:worked-time",
                            "urn:replicon:time-entry-type:time-off",
                        ]
                    }
                },
            },
        },
    }

    if payload_type == "row_count":
        return {
            "columnUris": [
                "urn:replicon:time-data-export-column:user",
                "urn:replicon:time-data-export-column:entry-date",
            ],
            "sort": [],
            "filterExpressions": [filterexpression],
            "fileFormatScriptUri": null,
        }

    return {
        "columnUris": [],
        "sort": [],
        "filterExpression": filterexpression,
        "fileFormatScriptUri": null,
    }