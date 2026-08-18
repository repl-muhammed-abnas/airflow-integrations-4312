import uuid
import rail
from datetime import datetime, date
from sideplate.project_records_sync.utils import custom_function
from sideplate.project_records_sync import config

null = None

def parse_project_manager_login(full_name):
    """
    Convert full name to login format (firstname.lastname).
    Handles edge cases like single names and middle names.

    Args:
        full_name: Full name string (e.g., "John Doe")

    Returns:
        Login name in format "firstname.lastname" or original name if parsing fails
    """
    if not full_name:
        return None

    # Split name and remove empty strings
    name_parts = [part.strip() for part in full_name.split() if part.strip()]

    if len(name_parts) == 0:
        return None
    elif len(name_parts) == 1:
        # Single name: just use it as-is
        return name_parts[0].lower()
    elif len(name_parts) == 2:
        # First and last name: firstname.lastname
        return f"{name_parts[1].lower()}.{name_parts[0].lower()}"
    else:
        # Multiple names: use first and last, ignore middle names
        return f"{name_parts[-1].lower()}.{name_parts[0].lower()}"

def search_client_by_name_payload(data):
    return {
            "page": "1",
            "pagesize": "1000",
            "columnUris": [
                "urn:replicon:client-list-column:active",
                "urn:replicon:client-list-column:code",
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
                    "text": data["records"][0]['Name'],
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


def get_project_details_payload(project_data):
    return {
        "projectUri": project_data[0].get("uri")
    }

def get_client_details_payload(client_data):
    return {
        "clientUri": client_data
    }

def search_client_code_payload(client_data):
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:client-list-column:client",
            "urn:replicon:client-list-column:code",
            null
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:client-list-filter:code"
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
                "text": client_data.get("Account__c"),
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
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:client-list-filter:active"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                "uri": null,
                "uris": [],
                "bool": "true",
                "date": null,
                "money": null,
                "number": null,
                "text": null,
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
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }

def update_client_name_payload(client_data):
    client_details = client_data[0]

    return {
        "clientUri": client_details["uri"],
        "name": client_details["name"]
    }

def create_client_payload(account_data, country_data):
    account_details = account_data['records'][0]
    client_name = f"""{account_details.get('Name')} - {account_details.get('BillingCity')}, {account_details.get('BillingState')}"""
    if account_details.get("BillingCountry"):
        country_uri = rail.find_first_by_attr_and_get_attr(country_data, "displayText", account_details.get("BillingCountry"), "uri")
    return {
            "target": null,
            "modifications": {
                "nameToApply": {
                "value": client_name
                },
                "codeToApply": {
                "value": account_details.get("Id")
                },
                "descriptionToApply": null,
                "statusToApply": "true",
                "clientContactToApply": null,
                "clientAddressToApply": {
                "address": {
                    "value": account_details.get("BillingStreet")
                } if account_details.get("BillingStreet") else null,
                "city": {
                    "value": account_details.get("BillingCity")
                } if account_details.get("BillingCity") else null,
                "stateProvince": {
                    "value": account_details.get("BillingState")
                } if account_details.get("BillingState") else null,
                "country": {
                    "value": {
                    "uri": country_uri,
                    "name": null
                    }
                } if account_details.get("BillingCountry") else null,
                "zipPostalCode": {
                    "value": account_details.get("BillingPostalCode")
                } if account_details.get("BillingPostalCode") else null,
                "phoneNumber": null,
                "faxNumber": null,
                "email": null,
                "website": null
                },
            },
            "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
            "unitOfWorkId": str(uuid.uuid4())
        }


def update_billing_rate_is_allowed_by_default_on_new_projects_payload(new_client_details):
    return {
            "clientUri": new_client_details.get("uri"),
            "billingRateUri": "urn:replicon:project-specific-billing-rate",
            "isAllowedByDefaultOnNewProjects": "true"
        }

def project_resource_list_payload():
    return {
            "page": "1",
            "pagesize": "1000",
            "columnUris": [
                "urn:replicon:resource-list-column:resource",
                "urn:replicon:resource-list-column:resource-type"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:resource-list-filter:resource-type"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": "urn:replicon:resource-type:user",
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": null,
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

def search_project_by_code_payload(project_data):
    return {
            "page": "1",
            "pagesize": "1000",
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
                "filterDefinitionUri": "urn:replicon:project-list-filter:code"
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
                    "text": project_data.get("Id"),
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

def update_project_description_payload(project_data, project_info):
    return{
        "projectUri": project_data[0].get("uri"),
        "description": project_info.get("MPM4_BASE_Description__c")
        }

def assign_project_manager_payload(project_data, leader_data):
    return {
        "projectUri": project_data[0].get("uri"),
        "userUri": leader_data
    }

def update_project_name_payload(project_data, list_data):
    return{
        "projectUri": list_data[0].get("uri"),
        "name": project_data.get("Project_Number_and_Name__c")
    }

def update_project_status_payload(project_data, list_data):
    project_status = project_data.get("MPM4_BASE_Status__c")
    status_uri = None
    if project_status == "Archived":
        status_uri = "urn:replicon:project-status-type:archived"
    elif project_status == "In Progress":
        status_uri = "urn:replicon:project-status-type:in-progress"
    elif project_status == "Tentative":
        status_uri = "urn:replicon:project-status-type:tentative"
    elif project_status == "Completed":
        status_uri = "urn:replicon:project-status-type:completed"
    elif project_status == "Cancelled":
        status_uri = "urn:replicon:project-status-type:cancelled"
    else:
        status_uri = "urn:replicon:project-status-type:deferred"
    return{
        "projectUri": list_data[0].get("uri"),
        "projectStatusUri": status_uri
    }

def update_project_fixed_bid_rate_payload(project_data, opportunity_data, currency_data):
    currency_uri = next(item["uri"] for item in currency_data if item["name"] == "US Dollar")
    amount = opportunity_data.get("Sum_of_Project_Amounts__c") or "0"
    return {
        "projectUri": project_data[0].get("uri"),
        "rate": {
            "amount": str(amount),
            "currencyUri": currency_uri
        },
        "projectFixedBidBillingFrequencyUri": "urn:replicon:fixed-bid-frequency:end-of-project"
    }

def update_estimation_mode_payload(project_data):
    return{
        "projectUri": project_data[0].get("uri"),
        "estimationModeUri": "urn:replicon:project-estimation-mode:task-based"
    }

def apply_new_client2_payload(client_detail, project_detail):
    return{
    "projectUri": project_detail["uri"],
    "clientUri": client_detail["uri"],
    "optionUri": "urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes"
    }

def create_project_payload_non_billable(opportunity_data, leader_data):
    today = date.today().isoformat()
    return{
  "target": null,
  "modifications": {
    "nameToApply": {
      "value": opportunity_data["Project_Number_and_Name__c"]
    },
    "codeToApply": {
      "value": opportunity_data["Id"]
    },
    "descriptionToApply": {
      "value": opportunity_data["MPM4_BASE_Description__c"]
    },
    "percentCompletedToApply": null,
    "startDateToApply": custom_function.get_formatted_date(today),
    "endDateToApply": {
      "date": {
        "year": 2099,
        "month": 12,
        "day": 31
      }
    },
    "billingTypeToApply": {
      "value": "urn:replicon:billing-type:non-billable"
    },
    "statusToApply": {
      "uri": custom_function.get_project_status_uri_by_display_text(opportunity_data["MPM4_BASE_Status__c"]),
      "name": null
    },
    "projectLeaderToApply": {
      "user": {
        "uri": leader_data,
        "loginName": null,
        "employeeId": null,
        "parameterCorrelationId": null
      }
    }if leader_data else null,

  },
  "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
  "unitOfWorkId": str(uuid.uuid4())
}

def create_project_payload_time_and_material(opportunity_data, leader_data):
    today = date.today().isoformat()
    return{
  "target": null,
  "modifications": {
    "nameToApply": {
      "value": opportunity_data["Project_Number_and_Name__c"]
    },
    "codeToApply": {
      "value": opportunity_data["Id"]
    },
    "descriptionToApply": {
      "value": opportunity_data["MPM4_BASE_Description__c"]
    },
    "percentCompletedToApply": null,
    "startDateToApply": custom_function.get_formatted_date(today),
    "endDateToApply": {
      "date": {
        "year": 2099,
        "month": 12,
        "day": 31
      }
    },
    "billingTypeToApply": {
      "value": "urn:replicon:billing-type:time-and-material"
    },
    "timeAndMaterials": {
      "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
      "billingRateFrequency": null,
      "billingRateFrequencyDuration": null,
      "billingRates": []
    },
    "statusToApply": {
      "uri": custom_function.get_project_status_uri_by_display_text(opportunity_data["MPM4_BASE_Status__c"]),
      "name": null
    },
    "projectLeaderToApply": {
      "user": {
        "uri": leader_data,
        "loginName": null,
        "employeeId": null,
        "parameterCorrelationId": null
      }
    }if leader_data else null,

  },
  "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
  "unitOfWorkId": str(uuid.uuid4())
}

def create_project_payload(opportunity_data, leader_data):
    today = date.today().isoformat()
    return{
  "target": null,
  "modifications": {
    "nameToApply": {
      "value": opportunity_data["Project_Number_and_Name__c"]
    },
    "codeToApply": {
      "value": opportunity_data["Id"]
    },
    "descriptionToApply": {
      "value": opportunity_data["MPM4_BASE_Description__c"]
    },
    "percentCompletedToApply": null,
    "startDateToApply": custom_function.get_formatted_date(today),
    "endDateToApply": {
      "date": {
        "year": 2099,
        "month": 12,
        "day": 31
      }
    },
    "billingTypeToApply": {
      "value": "urn:replicon:billing-type:fixed-bid"
    },
    "statusToApply": {
      "uri": custom_function.get_project_status_uri_by_display_text(opportunity_data["MPM4_BASE_Status__c"]),
      "name": null
    },
    "projectLeaderToApply": {
      "user": {
        "uri": leader_data,
        "loginName": null,
        "employeeId": null,
        "parameterCorrelationId": null
      }
    }if leader_data else null,

  },
  "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
  "unitOfWorkId": str(uuid.uuid4())
}

def apply_new_client2_via_http_payload(project_details, client_details):
    project_uri = project_details["uri"]
    return{
        "projectUri": project_uri,
        "clientUri": client_details,
        "optionUri": "urn:replicon:project-apply-new-client-option:keep-existing-billing-rates-and-expense-codes"
        }

def update_clients_payload(project_details, client_details):
    project_uri = project_details["uri"]
    return {
        "projectUri": project_uri,
        "clients": [
                {
                    "client": {
                    "uri": client_details,
                    "name": null,
                    "code": null,
                    "parameterCorrelationId": null
                    },
                    "costAllocationPercentage": "100"
                }
            ]
        }

def update_allow_time_entry_against_tasks_only_payload(project_details):
    project_uri = project_details["uri"]
    return {
            "projectUri": project_uri,
            "allowTimeEntryAgainstTasksOnly": "true"
        }

def bulk_update_project_team_members_assignment_payload(resource_details, project_details):
    resource_uri = []
    if resource_details is not None:
        resource_uri = [item["uri"] for item in resource_details]
    project_uri = project_details["uri"]
    return {
        "projectUri": project_uri,
        "resourceUri": resource_uri,
        "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
    }

def update_estimation_mode_via_http_payload():
    project_uri = rail.result("store_project_uri2") if rail.result("store_project_uri2") else rail.result("store_project_uri")
    return {
        "projectUri": project_uri,
        "estimationModeUri": "urn:replicon:project-estimation-mode:task-based"
    }

def get_project_details_via_http_payload(project_uri):
    return {
        "projectUri": project_uri
    }

def put_task_hierarchy_via_http_payload(project_uri):
    return {
  "project": {
    "uri": project_uri,
    "name": null,
    "parameterCorrelationId": null
  },
  "taskHierarchy": [
    {
      "task": {
        "target": {
          "uri": null,
          "name": "DEV",
          "parent": null,
          "parameterCorrelationId": null
        },
        "name": "DEV",
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
        "assignedResources": []
      },
      "childTasks": []
    },
    {
      "task": {
        "target": {
          "uri": null,
          "name": "DESIGN",
          "parent": null,
          "parameterCorrelationId": null
        },
        "name": "DESIGN",
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
        "assignedResources": []
      },
      "childTasks": [
        {
          "task": {
            "target": {
              "uri": null,
              "name": "Model Review",
              "parent": null,
              "parameterCorrelationId": null
            },
            "name": "Model Review",
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
            "assignedResources": []
          },
          "childTasks": []
        },
        {
          "task": {
            "target": {
              "uri": null,
              "name": "Deliverables",
              "parent": null,
              "parameterCorrelationId": null
            },
            "name": "Deliverables",
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
            "assignedResources": []
          },
          "childTasks": []
        },
        {
          "task": {
            "target": {
              "uri": null,
              "name": "Coordination",
              "parent": null,
              "parameterCorrelationId": null
            },
            "name": "Coordination",
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
            "assignedResources": []
          },
          "childTasks": []
        }
      ]
    },
    {
      "task": {
        "target": {
          "uri": null,
          "name": "PLAN CHECK",
          "parent": null,
          "parameterCorrelationId": null
        },
        "name": "PLAN CHECK",
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
        "assignedResources": []
      },
      "childTasks": [
        {
          "task": {
            "target": {
              "uri": null,
              "name": "Model Review",
              "parent": null,
              "parameterCorrelationId": null
            },
            "name": "Model Review",
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
            "assignedResources": []
          },
          "childTasks": []
        },
        {
          "task": {
            "target": {
              "uri": null,
              "name": "Deliverables",
              "parent": null,
              "parameterCorrelationId": null
            },
            "name": "Deliverables",
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
            "assignedResources": []
          },
          "childTasks": []
        },
        {
          "task": {
            "target": {
              "uri": null,
              "name": "Coordination",
              "parent": null,
              "parameterCorrelationId": null
            },
            "name": "Coordination",
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
            "assignedResources": []
          },
          "childTasks": []
        }
      ]
    },
    {
      "task": {
        "target": {
          "uri": null,
          "name": "CONSTRUCTION",
          "parent": null,
          "parameterCorrelationId": null
        },
        "name": "CONSTRUCTION",
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
        "assignedResources": []
      },
      "childTasks": [
        {
          "task": {
            "target": {
              "uri": null,
              "name": "RFIs",
              "parent": null,
              "parameterCorrelationId": null
            },
            "name": "RFIs",
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
            "assignedResources": []
          },
          "childTasks": []
        },
        {
          "task": {
            "target": {
              "uri": null,
              "name": "Shops",
              "parent": null,
              "parameterCorrelationId": null
            },
            "name": "Shops",
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
            "assignedResources": []
          },
          "childTasks": []
        },
        {
          "task": {
            "target": {
              "uri": null,
              "name": "Coordination",
              "parent": null,
              "parameterCorrelationId": null
            },
            "name": "Coordination",
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
            "assignedResources": []
          },
          "childTasks": []
        }
      ]
    }
  ]
}

def update_object_extension_field_value_project_via_http_payload(project_data, field_value, field_name =None, field_uri =None):
    project_uri = project_data.get("Repliconprojecturi")

    field_value_uri = next(
        (item["uri"] for item in field_value if item.get("name") == field_name), None
        )

    return{
    "objectUri": project_uri,
    "value": {
        "definition": {
            "uri": field_value_uri,
            "name": null
            },
        "tag": null,
        "numericValue": null,
        "textValue": field_uri,
        "fileValue": null,
        "jsonValue": null
        }
    }

def get_object_extension_tag_definition_details_via_http_payload(field_value, field_name =None):
    field_value_uri = next(
        (item["uri"] for item in field_value if item.get("name") == field_name), None
        )
    return{
        "objectExtensionTagDefinitionUri": field_value_uri
        }

def enable_draft_uri_payload(field_uri):
    return{
        "objectExtensionTagUri": field_uri
        }


def put_object_extension_tags_via_http_payload(category_list, object_extension, argument=None, new_value=None):
    field_value_uri = next(
        (item["uri"] for item in object_extension if item.get("name") == argument), None
        )
    tags = list(category_list)
    if new_value:
        tags.append({
            "target": {"uri": None, "slug": None, "tagName": None},
            "code": new_value,
            "name": new_value,
            "description": None,
            "isEnabled": "true"
        })
    return {
        "objectExtensionTagDefinition": {
            "uri": field_value_uri,
            "name": null
        },
        "objectExtensionTags": tags
    }
    
def update_object_extension_field_value_project_via_http_1_payload(project_data, field_value, field_name =None, field_uri =None, salesforce_field_name =None):
    project_uri = project_data.get("Repliconprojecturi")

    field_value_uri = next(
        (item["uri"] for item in field_value if item.get("name") == field_name), None
        )
    if isinstance(field_uri, dict):
        if "updated" in field_uri.keys():
            tag_value = field_uri["updated"][0]["uri"]
        elif "tags" in field_uri.keys():
            tag_list = field_uri["tags"]
            tag_value = next(
                (item["uri"] for item in tag_list if item["name"] == salesforce_field_name), None
            )
    elif field_uri:
        tag_value = field_uri
    return{
    "objectUri": project_uri,
    "value": {
        "definition": {
            "uri": field_value_uri,
            "name": null
            },
        "tag": {
            "uri": tag_value,
            "slug": null,
            "tagName": null
                },
        "numericValue": null,
        "textValue": null,
        "fileValue": null,
        "jsonValue": null
        }
    }

def update_object_extension_field_value_project_via_http_1_numeric_payload(project_data, field_value, project_details, field_name =None, salesforce_field_name=None):
    project_uri = project_data.get("Repliconprojecturi")

    field_value_uri = next(
        (item["uri"] for item in field_value if item.get("name") == field_name), None
        )
    numeric_value = project_details['records'][0][f"{salesforce_field_name}"]
    return{
    "objectUri": project_uri,
    "value": {
        "definition": {
            "uri": field_value_uri,
            "name": null
            },
        "tag": null,
        "numericValue": str(numeric_value),
        "textValue": null,
        "fileValue": null
        }
    }

def update_name_of_new_OEF_drop_down_payload(extension_uri, contact_info, field_name=None):
    contact_name = contact_info["records"][0][f"{field_name}"] 
    return {
        "objectExtensionTagUri": extension_uri,
        "name": contact_name
    }

def update_project_fixed_bid_rate_in_replicon_payload(project_details, project_data, currency_data):
    currency_uri = next(item["uri"] for item in currency_data if item["name"] == "US Dollar")
    project_amount = project_data['Sum_of_Project_Amounts__c']
    project_uri = project_details["uri"]
    return {
    "projectUri": project_uri,
        "rate": {
            "amount": str(int(project_amount)),
            "currencyUri": currency_uri
        },
    "projectFixedBidBillingFrequencyUri": "urn:replicon:fixed-bid-frequency:end-of-project"
}

def get_update_date_payload(project_uri, field_uri, date_value):
    parsed_date = datetime.strptime(date_value, "%Y-%m-%d")
    return {
            "objectUri": project_uri,
            "customFieldUri": field_uri,
            "value": {
                "year": parsed_date.year,
                "month": parsed_date.month,
                "day": parsed_date.day
            }
        }