from uuid import uuid4
from cobalt.cobaltcare_timesync import custom_methods
import rail
null = None


def get_time_entry_request(dag_run):
    time_entry_req = {
        "timeEntry": {
            "target": {
                "parameterCorrelationId": str(uuid4()),
            },
            "user": {
                "uri": rail.result("search_for_users"),
                "parameterCorrelationId": str(uuid4()),
            },
            "interval": {
                "hours": null,
                "timePair": {
                    "startTime": custom_methods.get_time(dag_run.conf["webhook"]["data"]["starttime"]),
                    "endTime": custom_methods.get_time(dag_run.conf["webhook"]["data"]["endtime"])
                }
            },
            "timeAllocationTypeUris": [
                "urn:replicon:time-allocation-type:attendance",
                "urn:replicon:time-allocation-type:project"
            ],
            "entryDate": {
                "year": custom_methods.compare_start_end_time(dag_run).year,
                "month": custom_methods.compare_start_end_time(dag_run).month,
                "day": custom_methods.compare_start_end_time(dag_run).day
            },
            "customMetadata": [
                {
                    "keyUri": "urn:replicon:time-entry-metadata-key:task",
                    "value": {
                        "uri": rail.result("create_task") or rail.load_all_records(rail.result("query_to_check_if_task_present"))[0]["taskuri"]
                    }
                },
                {
                    "keyUri": "urn:replicon:widget-ui-metadata-key:initial-row-number",
                    "value": {
                        "number": 1000
                    }
                }
            ],
            "extensionFieldValues": []
        },
        "unitOfWorkId": str(uuid4())
    }
    if "Non" not in dag_run.conf["webhook"]["data"]["billing"]:
        time_entry_req["timeEntry"]["customMetadata"].append({
            "keyUri": "urn:replicon:time-entry-metadata-key:billing-rate",
            "value": {
                "uri": "urn:replicon:project-specific-billing-rate"
            }
        })
    if dag_run.conf["webhook"]["data"]["comment"]:
        time_entry_req["timeEntry"]["customMetadata"].append({
            "keyUri": "urn:replicon:time-entry-metadata-key:comments",
            "value": {
                "text": dag_run.conf["webhook"]["data"]["comment"]
            }
        })
    return time_entry_req

def get_create_task_request(dag_run):
    return {
                "project": {
                    "uri": rail.result("search_for_projects"),
                    "name": null,
                    "code": null,
                    "parameterCorrelationId": null
                },
                "task": {
                    "target": {
                        "uri": null,
                        "name": dag_run.conf["webhook"]["data"]["ticketid"],
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "name": dag_run.conf["webhook"]["data"]["ticketid"],
                    "code": null,
                    "description": null,
                    "timeEntryDateRange": null,
                    "percentCompleted": 0,
                    "isTimeEntryAllowed": "true",
                    "estimatedHours": null,
                    "isClosed": "false",
                    "customFieldValues": [],
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                    "assignedResources": [
                        {
                            "uri": "urn:replicon-tenant:" + rail.get_tenant_slug() + ":department:1"
                        }
                    ],
                    "keyValues": [],
                    "historicalKeyValues": [],
                    "extensionFieldValues": []
                }
            }
