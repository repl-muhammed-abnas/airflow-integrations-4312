import json
from datetime import datetime
from uuid import uuid4
import rail

null = None


def get_custom_fields_request(dag_run, logs, action="add"):
    project_details = rail.result("get_project_details")
    numeric_fields = [
        "estimatedengineeringcost",
        "estimatedengineeringhours",
        "estimatedpmcost",
        "estimatedpmhours",
        "projectvalue",
    ]
    dropdown_fields = ["categorization", "type", "underwarranty"]
    date_field = ["deliverydate"]
    custom_fields_list = []

    for i in numeric_fields:
        if dag_run.conf[i] and (
            action == "add" or project_details[i] != dag_run.conf[i]
        ):
            custom_fields_list.append(
                {
                    "customField": {
                        "uri": dag_run.conf[i + "uri"],
                        "name": null,
                        "groupUri": "urn:replicon:object-type:project",
                    },
                    "text": null,
                    "date": null,
                    "dropDownOption": null,
                    "number": dag_run.conf[i],
                }
            )

            logs += i + ";"
    for i in dropdown_fields:
        if dag_run.conf[i] and (
            action == "add" or project_details[i] != dag_run.conf[i]
        ):
            custom_fields_list.append(
                {
                    "customField": {
                        "uri": dag_run.conf[i + "uri"],
                        "name": null,
                        "groupUri": "urn:replicon:object-type:project",
                    },
                    "text": null,
                    "date": null,
                    "dropDownOption": {"uri": null, "name": dag_run.conf[i]},
                    "number": null,
                }
            )
            logs += i + ";"
    for i in date_field:
        if dag_run.conf[i] and (
            action == "add" or project_details[i] != dag_run.conf[i]
        ):
            custom_fields_list.append(
                {
                    "customField": {
                        "uri": dag_run.conf[i + "uri"],
                        "name": null,
                        "groupUri": "urn:replicon:object-type:project",
                    },
                    "text": null,
                    "date": rail.parse_date(dag_run.conf[i], "%m/%d/%Y"),
                    "dropDownOption": null,
                    "number": null,
                }
            )
            logs += i + ";"
    return custom_fields_list


def get_custom_field_update_request(dag_run):
    project_details = rail.result("get_project_details")
    client = project_details["clients"] if project_details["clients"] else null
    logs = ""
    startdate, enddate = "", ""
    currbudget_hours = rail.result("get_project_details")["budgethours"]
    currbudget_cost = rail.result("get_project_details")["budgetcost"]
    budget_hours = (
        float(dag_run.conf["estimatedengineeringhours"].strip())
        if dag_run.conf["estimatedengineeringhours"]
        else 0
    ) + (
        float(dag_run.conf["estimatedpmhours"].strip())
        if dag_run.conf["estimatedpmhours"]
        else 0
    )
    budget_amount = (
        float(dag_run.conf["estimatedengineeringcost"].strip())
        if dag_run.conf["estimatedengineeringcost"]
        else 0
    ) + (
        float(dag_run.conf["estimatedpmcost"].strip())
        if dag_run.conf["estimatedpmcost"]
        else 0
    )
    h_flag, c_flag = currbudget_hours != budget_hours, currbudget_cost != budget_amount
    project_leader = ""
    client_uri = (
        rail.result("create_client_in_replicon")["uri"]
        if rail.result("create_client_in_replicon")
        else (
            rail.result("get_client_uri")[0]["uri"]
            if rail.result("get_client_uri")
            else null
        )
    )
    if rail.result("get_project_manager") and rail.result("get_project_details")[
        "projectmanageruri"
    ] != rail.result("get_project_manager"):
        project_leader = rail.result("get_project_manager")
        logs += "Project manager;"
    if h_flag:
        logs += "Budget hours;"
    if c_flag:
        logs += "Budget cost;"
    if dag_run.conf["startdate"] and rail.result("get_project_details")["startdate"] and\
        datetime(**rail.result("get_project_details")["startdate"]) != \
        datetime(**rail.parse_date(dag_run.conf["startdate"],"%m/%d/%Y")):
        startdate = rail.parse_date(dag_run.conf["startdate"],"%m/%d/%Y")
        logs += "Start date;"
    if dag_run.conf["enddate"] and rail.result("get_project_details")["enddate"] and\
        datetime(**rail.result("get_project_details")["enddate"]) !=\
        datetime(**rail.parse_date(dag_run.conf["enddate"],"%m/%d/%Y")):
        enddate = rail.parse_date(dag_run.conf["enddate"],"%m/%d/%Y")
        logs += "End date;"
    if client_uri and client_uri != client:
        logs += "Client;"

    return {
        "target": {
            "uri": project_details["uri"],
        },
        "modifications": {
            "startDateToApply": {
                "date": {**startdate}
            } if startdate else null,
            "endDateToApply": {
                "date": {**enddate}
            } if enddate else null,
            "customFieldsToApply": get_custom_fields_request(dag_run, logs, "update"),
            "budgetedHoursToApply": (
                {"duration": {"hours": int(budget_hours), "minutes": 0, "seconds": 0}}
                if h_flag
                else null
            ),
            "estimatedCostToApply": null,
            "budgetedCostToApply": (
                {
                    "value": {
                        "amount": budget_amount,
                        "currency": {"uri": null, "name": null, "symbol": "€"},
                    }
                }
                if c_flag
                else null
            ),
            "projectLeaderToApply": (
                {
                    "user": {
                        "uri": project_leader,
                        "loginName": null,
                        "employeeId": null,
                        "parameterCorrelationId": null,
                    }
                }
                if project_leader
                else null
            ),
            "clientAssignmentsSchedulesToApply": {
                "clients": [
                    {
                    "client": {
                        "uri": client_uri,
                        "name": null,
                        "code": null,
                        "parameterCorrelationId": null
                    },
                    "costAllocationPercentage": "100"
                    }
                ],
                "effectiveDate": null
            },
        } if client_uri else null,
        "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
        "unitOfWorkId": str(uuid4()),
    }, logs


def put_task(item):
    project_details = ""
    if rail.result("get_project_details"):
        project_details = rail.result("get_project_details").get("uri")
    if rail.result("create_project_in_replicon"):
        project_details = rail.result("create_project_in_replicon").get("uri")
    return {
        "project": {
            "uri": project_details,
            "name": null,
            "code": null,
            "parameterCorrelationId": null,
        },
        "task": {
            "target": {
                "uri": null,
                "name": item,
                "parent": null,
                "parameterCorrelationId": null,
            },
            "name": item,
            "code": null,
            "description": null,
            "timeEntryDateRange": null,
            "percentCompleted": "0",
            "isTimeEntryAllowed": "true",
            "estimatedHours": null,
            "isClosed": "false",
            "customFieldValues": [],
            "estimatedCost": null,
            "costTypeUri": null,
            "timeAndExpenseEntryTypeUri": null,
            "assignedResources": [],
            "keyValues": [],
            "historicalKeyValues": [],
            "extensionFieldValues": [],
            "resourceEstimates": [],
        },
    }


def create_project(dag_run):
    budget_amount = (
        float(dag_run.conf["estimatedengineeringcost"].strip())
        if dag_run.conf["estimatedengineeringcost"]
        else 0
    ) + (
        float(dag_run.conf["estimatedpmcost"].strip())
        if dag_run.conf["estimatedpmcost"]
        else 0
    )
    budget_hours = (
        float(dag_run.conf["estimatedengineeringhours"].strip())
        if dag_run.conf["estimatedengineeringhours"]
        else 0
    ) + (
        float(dag_run.conf["estimatedpmhours"].strip())
        if dag_run.conf["estimatedpmhours"]
        else 0
    )
    
    start_date = rail.parse_date(datetime.strptime
        (dag_run.conf["startdate"], "%m/%d/%Y").strftime("%Y-%m-%d"),"%Y-%m-%d") if dag_run.conf["startdate"]  else null
    
    end_date = rail.parse_date(datetime.strptime
        (dag_run.conf["enddate"], "%m/%d/%Y").strftime("%Y-%m-%d"),"%Y-%m-%d") if dag_run.conf["enddate"]  else null

    # Get client URI from either existing client or newly created one
    client_uri = (
        rail.result("create_client_in_replicon")["uri"]
        if rail.result("create_client_in_replicon")
        else (
            rail.result("get_client_uri")[0]["uri"]
            if rail.result("get_client_uri")
            else null
        )
    )

    return {
  "target": null,
  "modifications": {
    "nameToApply": {
      "value": dag_run.conf["name"]
    },
    "codeToApply": {
      "value": dag_run.conf["projectcode"]
    },
    "descriptionToApply": null,
    "percentCompletedToApply": null,
    "startDateToApply": {
      "date": {**start_date}
    } if dag_run.conf["startdate"]  else null,
    "endDateToApply": {
      "date": {**end_date} 
    } if dag_run.conf["enddate"]  else null,
    "billingTypeToApply": null,
    "clientBillingAllocationMethodToApply": null,
    "clientAssignmentsSchedulesToApply": {
      "clients": [
        {
          "client": {
            "uri": client_uri,
            "name": null,
            "code": null,
            "parameterCorrelationId": null
          },
          "costAllocationPercentage": "100"
        }
      ],
      "effectiveDate": null
    } if client_uri else null,
    "statusToApply": null,
    "projectWorkflowStateToApply": null,
    "clientRepresentativeToApply": null,
    "programToApply": null,
    "projectLeaderToApply": {
      "user": {
        "uri": rail.result("get_project_manager"),
        "loginName": null,
        "employeeId": null,
        "parameterCorrelationId": null
      }
    },
    "budgetedHoursToApply": {
      "duration": {
        "hours": int(budget_hours),
        "minutes": "0",
        "seconds": "0"
      }
    },
    "budgetedCostToApply": {
      "value": {
        "amount": int(budget_amount),
        "currency": {
          "uri": null,
          "name": null,
          "symbol": "€"
        }
      }
    },
    "keyValuesToApply": [
      {
        "keyUri": "urn:replicon:project-key-value-key:project-management-type",
        "value": {
          "uri": "urn:replicon:project-management-type:managed",
        }
      }
    ],
    "customFieldsToApply": get_custom_fields_request(dag_run, ""),
  },
    "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
    "unitOfWorkId": str(uuid4()),
}
