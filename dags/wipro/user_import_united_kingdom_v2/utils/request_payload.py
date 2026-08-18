from datetime import datetime as dt
import calendar
from uuid import uuid4
from wipro.user_import_united_kingdom_v2.utils import custom_methods
import rail
INVALID_DATES = ["9999-12-31", "0000-00-00"]
null = None
true = "true"
false = "false"


def get_parent_location_payload():
    return {
        "page": "1",
        "pageSize": "10",
        "textSearch": {
            "queryText": "United Kingdom",
            "searchInDisplayText": "false",
            "searchInName": "true",
            "searchInDescription": "false",
            "searchInCode": "false"
        }
    }


def get_location_hierarchy_payload():
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:name"
        ],
        "parentUri": rail.result("get_united_kingdom_parent_location_details")
    }


def get_legal_entity_payload():
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:division-list-column:division",
            "urn:replicon:division-list-column:code",
            null
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:division-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": null
                },
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": true,
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


def get_payrule_payload():
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:script-list-column:name",
            "urn:replicon:script-list-column:script"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:script-list-filter:is-active"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": true,
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


def get_timesheet_period_payload():
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:timesheet-period-list-column:timesheet-period"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:timesheet-period-list-filter:enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": true,
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


def get_location_payload(dag_run):
    return {
        "location": {
            "name": null,
            "uri": null,
            "parent": {
                "name": null,
                "uri": dag_run.conf["locationcountryuri"],
                "parent": null,
                "parameterCorrelationId": null
            },
            "parameterCorrelationId": null
        },
        "modifications": {
            "name": rail.result("for_each_location_add_hierarchy")["location"],
            "codeToApply": null,
            "descriptionToApply": null,
            "isEnabled": true
        },
        "unitOfWorkId": str(uuid4())
    }


def get_update_supervisor_permission_payload(dag_run):
    permission_set_uris = [dag_run.conf["l1_manager_uri"],dag_run.conf["project_manager_uri"],
                           dag_run.conf["end_user_manager_uri"]]
    return {
        "user": {
            "uri": rail.result("get_supervisor_user_details")[0]["userDetails"]["uri"]
        },
        "modifications": {
            "permissionSetsToApply": {
                "permissionSetUrisToAssign": permission_set_uris,
                "policyUrisToRemovePermissionSet": []
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_supervisor_first_name_last_name(dag_run):
    firstname, lastname = "", ""
    if dag_run.conf["primary_supervisor_mailid"]:
        name = dag_run.conf["primary_supervisor_mailid"].split('@')[0]
        if "." in name:
            firstname, lastname = name.split(".")
        else:
            firstname = name
            lastname = "."
    return [firstname, lastname]


def get_supervisor_create_payload(dag_run):
    name = get_supervisor_first_name_last_name(dag_run)
    firstname, lastname = name[0], name[1]
    loginname = dag_run.conf["primary_supervisor_adid"]
    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": loginname,
                "employeeId": dag_run.conf["primary_supervisor_id"],
                "parameterCorrelationId": null
            },
            "firstname": firstname,
            "lastname": lastname,
            "emailAddress": dag_run.conf["primary_supervisor_mailid"],
            "employeeId": dag_run.conf["primary_supervisor_id"],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": [],
            "workWeekStartDayUri": null,
            "employmentDateRange": {
                "startDate": null,
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [],
                "isLoginEnabled": true,
                "loginName": loginname,
                "SSOName": loginname,
                "password": null
            },
            "holidayCalendar": null,
            "holidayCalendarAssignmentSchedule": null,
            "timeOffPolicy": null,
            "permissionSets": [
                {
                    "uri": dag_run.conf["l1_manager_uri"],
                    "name": null
                },
                {
                    "uri": dag_run.conf["end_user_manager_uri"],
                    "name": null
                }
            ],
            "policySets": [],
            "employeeType": {
                "uri": dag_run.conf["foreign_manager_emp_type_uri"],
                "name": null
            } if dag_run.conf["foreign_manager_emp_type_uri"] else null,
            "displayNameParameter": {
                "displayName": firstname + " " + lastname + " " + dag_run.conf["primary_supervisor_id"]
            }
        }
    }


def get_put_column_settings_for_user_timesheet_tab_data(user_uri):
    return {
        "userUri": user_uri,
        "listId": "timesheet_list",
        "columnSettings": [
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 220
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet-period",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 220
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet-status-2",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 220
                        }
                    },
                    {
                        "key": "addColumnValueToHiddenValues",
                        "value": {
                            "bool": true
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:total-working-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 100
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:time-off-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true,
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 100
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:total-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 100
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:project-time-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 100
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:regular-time-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 100
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:overtime-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 100
                        }
                    }
                ]
            }
        ]
    }


def get_put_column_settings_for_user_timesheets_data(user_uri, list_name,dag_run):
    return {
  "userUri": user_uri,
  "listId": list_name,
  "columnSettings": [
    {
      "columnUri": "urn:replicon:timesheet-list-column:timesheet",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": false,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 0,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:timesheet-owner",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 220,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:timesheet-status-2",
      "settings": [
        {
          "key": "addColumnValueToHiddenValues",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 220,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:timesheet-period",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 190,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:employee-id-of-timesheet-owner",
      "settings": [
        {
          "key": "visible",
          "value": {
            "bool": true
          }
        },
        {
          "key": "width",
          "value": {
            "number": 120
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-info",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 0,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-warning",
      "settings": [
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 0,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-error",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 170,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:time-off-duration",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 100,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:total-payable-duration",
      "settings": [
        {
          "key": "visible",
          "value": {
            "bool": true
          }
        },
        {
          "key": "width",
          "value": {
            "number": 100
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:billable-time-duration",
      "settings": [
        {
          "key": "visible",
          "value": {
            "bool": true
          }
        },
        {
          "key": "width",
          "value": {
            "number": 100
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:non-billable-time-duration",
      "settings": [
        {
          "key": "visible",
          "value": {
            "bool": true
          }
        },
        {
          "key": "width",
          "value": {
            "number": 100
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:regular-time-duration",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 100,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:overtime-duration",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 100,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:total-count-time-entry-waiting-for-approval-by-approver",
      "settings": [
        {
          "key": "addColumnValueToHiddenValues",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": false,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 0,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    }
  ]
}

def get_put_column_settings_for_user_approvals_data(user_uri):
    return {
        "userUri": user_uri,
        "listId": "timeSheetForApproval_list",
        "columnSettings": [
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 0
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet-owner",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 220
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:timesheet-period",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 190
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-info",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 0
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-warning",
                "settings": [
                    {
                        "key": "width",
                        "value": {
                            "number": 0
                        }
                    },
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-error",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 170
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:time-off-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 100
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:total-working-duration",
                "settings": [
                    {
                        "key": "width",
                        "value": {
                            "number": 100
                        }
                    },
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:billable-time-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 100
                        }
                    }
                ]
            },
            {
                "columnUri": "urn:replicon:timesheet-list-column:non-billable-time-duration",
                "settings": [
                    {
                        "key": "visible",
                        "value": {
                            "bool": true
                        }
                    },
                    {
                        "key": "width",
                        "value": {
                            "number": 100
                        }
                    }
                ]
            },
        ],
    }


def get_restrict_hr_manager_payload(dag_run):
    policy_data_access = []
    if dag_run.conf["hr_manager_flg"] == "Y":
        policy_data_access.append({
            "policyUri": "urn:replicon:policy:payroll-management",
            "location": null,
            "divisions": [
                {
                    "division": {
                        "uri": dag_run.conf["legalentityuri"],
                        "parentUri": null,
                        "name": null
                    },
                    "groupSpecificationModeUri": null,
                    "groupDescendantModeUri": null
                }
            ],
            "serviceCenter": null,
            "costCenter": null,
            "departmentGroup": null,
            "employeeTypeGroup": null
        }
        )
    return {
        "userUri": rail.result('get_update_user_details')["userDetails"]["uri"],
        "policyDataAccessScopes": policy_data_access
    }


def get_united_kingdom_create_payload(dag_run):
    loginname = dag_run.conf["adid"] + "@wipro.com"

    return {
        "user": {
            "target": {
                "uri": null,
                "loginName": loginname,
                "employeeId": null,
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf["employee_first_name"],
            "lastname": dag_run.conf["employee_last_name"],
            "emailAddress": dag_run.conf["employee_email_id"],
            "employeeId": dag_run.conf["employee_id"],
            "department": null,
            "supervisorAssignmentSchedule": {
                "initialSupervisor": {
                    "uri": rail.result("get_supervisor_uri"),
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                },
            } if rail.result("get_supervisor_uri") else null,
            "schedulePolicySchedule": [{
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": null,
                    "officeSchedule": null,
                    "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                },
                "effectiveDate": null
            }],
            "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
            "employmentDateRange": {
                "startDate": rail.parse_date(dag_run.conf["date_of_joining"], "%Y-%m-%d"),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [],
                "isLoginEnabled": true,
                "loginName": loginname,
                "SSOName": loginname,
                "password": null
            },
            "holidayCalendar": {
                "uri": dag_run.conf["holidaycalendaruri"],
                "name": null
            } if dag_run.conf["holidaycalendaruri"] else null,
            "holidayCalendarAssignmentSchedule": null,
            "timeOffPolicy": null,
            "permissionSets": custom_methods.get_united_kingdom_user_create_permissions(dag_run),
            "policySets": custom_methods.get_united_kingdom_user_create_policy_sets(dag_run),
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": {
                "uri": dag_run.conf["timesheet_system_approval_path_uri"],
                "name": null
            } if dag_run.conf["employee_band"] in ["GROUP D1", "GROUP D2", "GROUP E"] else {
                "uri": dag_run.conf["timesheet_approval_pathuri"],
                "name": null
            },
            "expenseApprovalPath": null,
            "timeOffApprovalPath": {
                "uri": dag_run.conf["timeoff_approval_path_group_uri"],
                "name": null
            } if dag_run.conf["employee_band"] in ["GROUP D1", "GROUP D2", "GROUP E"] else {
                "uri": dag_run.conf["timeoff_approval_path_uri"],
                "name": null
            },
            "workAuthorizationApprovalPath": null,
            "timeOffBalancePayoutApprovalPath": null,
            "customFieldValues": custom_methods.get_united_kingdom_user_create_custom_fields(dag_run),
            "assignedActivities": [],
            "timeZone":  {
                "uri": dag_run.conf["timezoneuri"],
                "IANAName": null
            },
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": custom_methods.get_united_kingdom_user_create_location(dag_run),
            "divisionSchedule": [
                {
                    "division": {
                        "uri": dag_run.conf["legalentityuri"],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ],
            "costCenterSchedule": [
                {
                    "costCenter": {
                    "uri": null,
                    "parentUri": null,
                    "name": "United Kingdom"
                    },
                    "effectiveDate": null
                }
            ],
            "payrollRateSchedule": {
                "initialHourlyRate": {
                    "amount": "0",
                    "currency": {
                    "uri": null,
                    "name": null,
                    "symbol": "£"
                    }
                },
                "scheduleEntries": []
            },
            "serviceCenterSchedule": [
                {
                    "serviceCenter": {
                        "uri": dag_run.conf["countryuri"],
                        "parentUri": null,
                        "name": null
                    },
                    "effectiveDate": null
                }
            ],
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                    "uri": null,
                    "parent": {
                        "name": "Wipro",
                        "parameterCorrelationId": null
                    },
                    "name": dag_run.conf["department"],
                    "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf["department"] and dag_run.conf["department_flag"] else [],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "uri": dag_run.conf["employee_type_uri"]
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf["employee_type_uri"] else [],
            "timesheetPeriodSchedule": [
                {
                    "timesheetPeriod": {
                        "uri": dag_run.conf["timesheet_perioduri"],
                        "name": null
                    },
                    "effectiveDate":  null if dag_run.conf["onsite_direct_recruit"].lower() == "local_hire" else
                    (rail.parse_date(dag_run.conf["onsite_start_date"], "%Y-%m-%d") if dag_run.conf["onsite_start_date"]
                     and dag_run.conf["onsite_start_date"] not in INVALID_DATES else null)
                }
            ],
            "policyDataAccessScopes": custom_methods.get_united_kingdom_user_create_policy_data_access(dag_run),
            "payRuleScriptSchedule": custom_methods.get_united_kingdom_user_payrule_script(dag_run),
            "displayNameParameter": {
                "displayName": dag_run.conf["employee_first_name"] + " " + dag_run.conf["employee_last_name"] + " " + dag_run.conf["employee_id"]
            },
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": "urn:replicon:number-group-separator:language-default",
            "extensionFieldValues": custom_methods.get_united_kingdom_user_create_oefs(dag_run)
        }
    }


def get_oef_text_field_update_payload(oef_uri, oef_value):
    return {
        "value": {
            "definition": {
                "uri": oef_uri,
                "name": null
            },
            "tag": null,
            "numericValue": null,
            "textValue": oef_value,
            "fileValue": null,
            "jsonValue": null
        }
    }


def get_user_annual_leaves_payload(dag_run):
    end_date = {}
    user_uri = ""
    if rail.result("get_user_details"):
        user_uri = rail.result("get_user_details")["userDetails"]["uri"]
    else:
        user_uri = rail.result("get_update_user_details")["userDetails"]["uri"]
    if not dag_run.conf["ard_lrd"] or dag_run.conf["ard_lrd"] in custom_methods.INVALID_DATES:
        end_date = custom_methods.get_today_date()
    else:
        end_date = rail.parse_date(dag_run.conf["ard_lrd"], "%Y-%m-%d")

    return {
        "userUri": user_uri,
        "dateRange": {
            "startDate": rail.parse_date(dag_run.conf["date_of_joining"], "%Y-%m-%d"),
            "endDate": end_date
        }
    }


def get_end_date(ard_lrd):
    if not ard_lrd or ard_lrd in custom_methods.INVALID_DATES:
        end_date = custom_methods.get_today_date()
    else:
        end_date = rail.parse_date(ard_lrd, "%Y-%m-%d")
    return end_date


def get_add_end_date_to_tranferred_user_payload(dag_run):
    company_code = rail.find_first_by_attr_and_get_attr(
        dag_run.conf["legalentities"],
        "uri",
        rail.result("get_user_details")[
            "divisionSchedule"][0]["division"]["uri"],
        "code")
    login_name = company_code + "_" + dag_run.conf["employee_id"] + "_T"
    return {
        "user": {
            "uri": rail.result("get_user_details")["userDetails"]["uri"],
        },
        "modifications": {
            "securitySettingsToApply": {
                "loginName": login_name,
                "ssoName": login_name,
            },
            "userDetailsToApply": {
                "employmentDateRange": {
                    "startDate": rail.get_replicon_date(dt.strptime(dag_run.conf["date_of_joining"], "%Y-%m-%d")),
                    "endDate": rail.get_replicon_date(dt.now()),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "employeeId": {
                    "employeeId": company_code + "_" + dag_run.conf["employee_id"]
                },
            },
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_unassign_products_payload():
    return {
        "user": {
            "uri": rail.result("create_supervisor_in_replicon"),
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "productAssignmentsToApply": {
                "productUrisToUnassign": [
                    "urn:replicon-saas:product:time-bill-plus",
                    "urn:replicon-saas:product:time-intelligence"
                ]
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_basic_user_details_update(dag_run):
    first_name = null
    last_name = null
    basic_details_logs = ""
    display_name = null
    email_id = null
    if dag_run.conf["employee_first_name"] != rail.result("get_update_user_details")["userDetails"]["firstName"]:
        first_name = dag_run.conf["employee_first_name"]
        basic_details_logs = "First name updated"
    if dag_run.conf["employee_last_name"] != rail.result("get_update_user_details")["userDetails"]["lastName"]:
        last_name = dag_run.conf["employee_last_name"]
        basic_details_logs += "Last name updated"
    if first_name or last_name:
        display_name = dag_run.conf["employee_first_name"] + " " + \
            dag_run.conf["employee_last_name"] + \
            " " + dag_run.conf["employee_id"]
        basic_details_logs += "Display Name updated"
    if dag_run.conf["employee_email_id"] != rail.result("get_update_user_details")["userDetails"]["emailAddress"]:
        email_id = dag_run.conf["employee_email_id"]
        basic_details_logs += "Email updated"
    basic_details = {
        "firstName": {
            "value": first_name
        } if first_name else null,
        "lastName": {
            "value": last_name
        }if last_name else null,
        "loginName": null,
        "displayName": {
            "value": display_name
        }if display_name else null,
        "emailAddress": {
            "value": email_id
        }if email_id else null
    }

    return basic_details, basic_details_logs

def get_employee_type_update(dag_run):
    employee_type_update = []
    logs = ""
    if (not rail.result("get_current_location_for_the_user")["existingemployeetypeuri"] and dag_run.conf["employee_type_uri"]) or\
        (dag_run.conf["employee_type_uri"] and
            rail.result("get_current_location_for_the_user")["existingemployeetypeuri"] != dag_run.conf["employee_type_uri"]):
        employee_type_update.append({
            "dateRange": {
                "relativeDateRangeAsOfDate": custom_methods.get_today_date()
            },
            "item": {
                "uri": dag_run.conf["employee_type_uri"]
            }
        })
        logs = "Employee type updated;"
    return employee_type_update, logs


def get_current_schedule_uri(script_schedule, script_uri):

    if not rail.result("get_update_user_details")[script_schedule]:
        return null
    payrule_schedule = rail.result("get_update_user_details")[script_schedule]
    schedule_list = []
    for i in payrule_schedule:
        if not i.get("effectiveDate"):
            schedule_list.append(i)
        if i.get("effectiveDate") and dt(**i["effectiveDate"]) < dt.utcnow():
            schedule_list.append(i)
    return schedule_list[-1][script_uri]["uri"]

def get_department_update(dag_run):
    if dag_run.conf["department"] and dag_run.conf["department"] != dag_run.conf["existingdepartment"] and dag_run.conf["department_flag"]:
        _date ={
          "startDate": custom_methods.get_today_date(),
          "endDate": null,
          "relativeDateRangeUri": null,
          "relativeDateRangeAsOfDate": null
        }  if dag_run.conf["existingdepartment"] else null
        return [
                {
                    "dateRange": _date,
                    "item": {
                    "uri": null,
                    "parent": {
                        "uri": null,
                        "parent": null,
                        "name": "Wipro",
                        "parameterCorrelationId": null
                    },
                    "name": dag_run.conf["department"],
                    "parameterCorrelationId": null
                    }
                }
            ] , "Department updated;"
    return [],""

def get_update_user_req(dag_run):
    return {
        "target": {
            "uri": rail.result('get_update_user_details')["userDetails"]['uri'],
        },
        "modifications": {
            **get_basic_user_details_update(dag_run)[0],
            "employeeTypeGroupSchedule": get_employee_type_update(dag_run)[0]if not dag_run.conf["new_entity_flag"] else null,
            "locationSchedule": custom_methods.check_if_united_kingdom_user_location_update(dag_run)[0],
            "extensionFields": custom_methods.get_extension_field_values_updates(dag_run)[0],
            "customFields": custom_methods.get_united_kingdom_user_update_custom_fields(dag_run)[0],
            "departmentGroupSchedule": get_department_update(dag_run)[0],
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }


def get_put_column_settings_for_pm_timesheets_data(user_uri,list_name, dag_run):
    return {
  "userUri": user_uri,
  "listId": list_name,
  "columnSettings": [
    {
      "columnUri": "urn:replicon:timesheet-list-column:timesheet",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": false,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 0,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:timesheet-owner",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 220,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:timesheet-status-2",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 220,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "addColumnValueToHiddenValues",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:timesheet-period",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 190,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-info",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 0,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-warning",
      "settings": [
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 0,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:validation-message-count-error",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 170,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:total-working-duration",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 100,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:time-off-duration",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 100,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:billable-time-duration",
      "settings": [
        {
          "key": "visible",
          "value": {
            "bool": true
          }
        },
        {
          "key": "width",
          "value": {
            "number": 100
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:non-billable-time-duration",
      "settings": [
        {
          "key": "visible",
          "value": {
            "bool": true
          }
        },
        {
          "key": "width",
          "value": {
            "number": 100
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:regular-time-duration",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 100,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:overtime-duration",
      "settings": [
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 100,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":timesheet-user-payroll-details-list-pay-code-hours-column:"+ dag_run.conf["paycode_d_oncall"],
      "settings": [
        {
          "key": "visible",
          "value": {
            "bool": true
          }
        },
        {
          "key": "width",
          "value": {
            "number": 100
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":timesheet-user-payroll-details-list-pay-code-hours-column:"+ dag_run.conf["paycode_f_oncall"],
      "settings": [
        {
          "key": "visible",
          "value": {
            "bool": true
          }
        },
        {
          "key": "width",
          "value": {
            "number": 100
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":timesheet-user-payroll-details-list-pay-code-hours-column:"+ dag_run.conf["paycode_e_callout"],
      "settings": [
        {
          "key": "visible",
          "value": {
            "bool": true
          }
        },
        {
          "key": "width",
          "value": {
            "number": 100
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":timesheet-user-payroll-details-list-pay-code-hours-column:"+ dag_run.conf["paycode_g_callout"],
      "settings": [
        {
          "key": "visible",
          "value": {
            "bool": true
          }
        },
        {
          "key": "width",
          "value": {
            "number": 100
          }
        }
      ]
    },
    {
      "columnUri": "urn:replicon:timesheet-list-column:total-count-time-entry-waiting-for-approval-by-approver",
      "settings": [
        {
          "key": "addColumnValueToHiddenValues",
          "value": {
            "uri": null,
            "slug": null,
            "bool": true,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "visible",
          "value": {
            "uri": null,
            "slug": null,
            "bool": false,
            "date": null,
            "money": null,
            "number": null,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        },
        {
          "key": "width",
          "value": {
            "uri": null,
            "slug": null,
            "bool": null,
            "date": null,
            "money": null,
            "number": 0,
            "text": null,
            "time": null,
            "calendarDayDurationValue": null,
            "workdayDurationValue": null,
            "dateRange": null
          }
        }
      ]
    }
  ]
}


def get_uk_annual_acquistion_payload(dag_run):
    effective_date = dict()
    effective_date = rail.parse_date(
        dag_run.conf["date_of_joining"], "%Y-%m-%d")
    parsed_doj = dt.strptime(dag_run.conf["date_of_joining"], "%Y-%m-%d")
    no_of_days_diff = parsed_doj.replace(day=31, month=12) - parsed_doj
    no_of_days_diff = no_of_days_diff.days + 1
    starting_balance = 20
    if parsed_doj.day != 1 and parsed_doj.month != 1:
        if calendar.isleap(parsed_doj.year):
            starting_balance =  0.054644809 * no_of_days_diff
        else:
            starting_balance = 0.054794520 * no_of_days_diff
    default_policy = rail.result("get_annual_leave_policy")
    policy_set_schedule = default_policy[0]["policySet"]
    policy_set_schedule["timeOffBalanceEventScripts"].append({
        "scriptTarget": {
            "uri": dag_run.conf["starting_balance_uri"]
        },
        "additionalParameters": [
            {
                "keyUri": "urn:replicon:script-key:parameter:amount",
                "value": {
                    "number": str(round(starting_balance, 4))
                }
            },
            {
                "keyUri": "urn:replicon:script-key:parameter:precedence",
                "value": {
                    "number": "10"
                }
            }
        ]
    })
    _date = str(effective_date["year"]) + "-" + str(effective_date["month"]) + "-" + str(effective_date["day"])
    # print(policy_set_schedule, effective_date)
    import json
    return {
        "timeOffAccount": {
            "userUri": rail.result("create_united_kingdom_user") or rail.result("get_user_details")["userDetails"]["uri"],
            "timeOffTypeUri": dag_run.conf["annual_accrual_timeoff_uri"]
        },
        "policySetScheduleEntries": [
            {
                "effectiveDate": effective_date,
                "description": f"Added by Integration on {_date}",
                "policySet": json.loads(json.dumps(policy_set_schedule)
                                                       .replace('"script"', '"scriptTarget"')
                                                       .replace('"description": null', '"description": "effective"'))
            }
        ]
    }
                


def get_hr_manager_with_location_payload(dag_run):
    payload = {
        "page": "1",
        "pagesize": "10",
        "columnUris": [
            "urn:replicon:user-list-column:user",
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:email-address"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
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
                        "text": dag_run.conf["hr_adid"] + "@wipro.com",
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
            },
            "operatorUri": "urn:replicon:filter-operator:and",
            "rightExpression": {
                "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:user-list-filter:service-center"
                },
                "operatorUri": "urn:replicon:filter-operator:equal",
                "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri":  dag_run.conf["countryuri"],
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
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }

    return payload

def get_new_entity_user_create_payload(dag_run, user_template_name):
    loginname = dag_run.conf["adid"] + "@wipro.com"
    return {
        "target": null,
        "template": {
            "templateTarget": {
                "uri": null,
                "name": user_template_name,
                "parameterCorrelationId": null
            }
        },
        "modifications": {
            "firstName": {
                "value": dag_run.conf["employee_first_name"],
            },
            "lastName": {
                "value": dag_run.conf["employee_last_name"] if dag_run.conf["employee_last_name"] else '.',
            },
            "loginName": {
                "value": loginname
            },
            "displayName": {
                "value": dag_run.conf["employee_first_name"] + " " + \
                (dag_run.conf["employee_last_name"] \
                 if dag_run.conf["employee_last_name"] else ".") + " " + dag_run.conf["employee_id"],
            },
            "emailAddress": {
                "value": dag_run.conf["employee_email_id"]
            },
            "employeeId": {
                "value": dag_run.conf["employee_id"]
            },
            "employmentDateRange": {
                "value": {
                    "startDate": rail.parse_date(dag_run.conf["date_of_joining"], "%Y-%m-%d"),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": "true"
                    },
                    "forcePasswordChange": null,
                    "ssoName": {
                        "value": loginname
                    },
                    "ssoNameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name"
                }
            },
            "timesheetApprovalPath": null,
            "timeEntryApprovalPath": null,
            "workAuthorizationApprovalPath": null,
            "timeoffApprovalPath": null,
            "timeOffBalancePayoutApprovalPath": null,
            "defaultActivity": null,
            "expenseApprovalPath": null,
            "timeZone": null,
            "workWeekStartDay": null,
            "defaultBillingRate": null,
            "userPreferences": null,
            "formattings": null,
            "notificationPreferences": null,
            "timesheetTemplate": null,
            "timeoffTemplate": null,
            "timeOffCalendarVisibility": null,
            "expenseTemplate": null,
            "workAuthorizationTemplate": null,
            "punchEntryPolicy": null,
            "holidayCalendar": {"value": {
                "uri": dag_run.conf["holidaycalendaruri"],
                "name": null
            }}if dag_run.conf["holidaycalendaruri"] else null,
            "extensionFields":  list(map(lambda i: {"value": i}, custom_methods.get_united_kingdom_user_create_oefs(dag_run,))),
            "customFields": list(map(lambda i: {"value": i}, custom_methods.get_united_kingdom_user_create_custom_fields(dag_run))),
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": list(map(lambda i: {"permissionSetPolicy": i}, custom_methods.get_united_kingdom_user_create_permissions(dag_run))),
                }],
            "locationSchedule": [{"item": {
                "uri": dag_run.conf["locationuri"]}
            }
            ] if dag_run.conf["locationuri"] else [],
            "divisionSchedule":  [
                {"item": {
                    "uri": dag_run.conf["legalentityuri"],
                    "parentUri": null,
                    "name": null
                },
                }
            ],
            "serviceCenterSchedule": [
                {"item": {
                    "uri": dag_run.conf["countryuri"],
                    "parentUri": null,
                    "name": null
                },
                }
            ],
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                    "uri": null,
                    "parent": {
                        "name": "Wipro",
                        "parameterCorrelationId": null
                    },
                    "name": dag_run.conf["department"],
                    "parameterCorrelationId": null
                    },
                    "effectiveDate": null
                }
            ] if dag_run.conf["department"] and dag_run.conf["department_flag"] else [],
            "employeeTypeGroupSchedule": [],
            "supervisorSchedule": [],
            "timesheetPeriodSchedule": [],
            "holidayCalendarSchedule": [],
            "scheduleTypeSchedule": [],
            "payRuleSchedule": [],
            "placeSchedule": [],
            "payRateSchedule": [],
            "projectRoleSchedule": [],
            "costNormalizationRuleSchedule": [],
            "hourlyRatesSchedule": [],
            "substituteUserSchedule": []
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }
