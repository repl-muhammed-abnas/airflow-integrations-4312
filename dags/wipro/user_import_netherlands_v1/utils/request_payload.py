import calendar
from datetime import datetime as dt
from uuid import uuid4
from wipro.user_import_netherlands_v1.utils import custom_methods
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
            "queryText": "netherlands",
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
      "parentUri": rail.result("get_netherlands_parent_location_details")
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
    permission_set_uris = [dag_run.conf["l1_manager_uri"],
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
    loginname = dag_run.conf["primary_supervisor_adid"] + "@wipro.com"
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


def get_put_column_settings_for_user_timesheets_data(user_uri):
    return {
  "userUri": user_uri,
  "listId": "myTeamTimeSheet_list",
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
      "columnUri": "urn:replicon:timesheet-list-column:total-break-duration",
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
            "division": null,
            "serviceCenters": [
                {
                "serviceCenter": {
                    "uri": dag_run.conf["countryuri"],
                    "parentUri": null,
                    "name": null
                },
                "groupSpecificationModeUri": null,
                "groupDescendantModeUri": null
                }
            ],
            "costCenter": null,
            "departmentGroup": null,
            "employeeTypeGroup": null
        }
        )
    if dag_run.conf["project_manager_flg"] == "Y":
        policy_data_access.append({
            "policyUri": "urn:replicon:policy:project-management",
            "locations": null,
            "divisions": null,
            "serviceCenters": [
                {
                "serviceCenter": {
                    "uri": dag_run.conf["countryuri"],
                    "parentUri": null,
                    "name": null
                },
                "groupSpecificationModeUri": null,
                "groupDescendantModeUri": null
                }
            ],
            "costCenters": null,
            "departmentGroups": null,
            "employeeTypeGroups": null
        }
        )
    return {
        "userUri": rail.result('get_update_user_details')["userDetails"]["uri"],
        "policyDataAccessScopes": policy_data_access
    }


def get_netherlands_create_payload(dag_run, instance):
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
            "supervisorAssignmentSchedule": null,
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
            },
            "holidayCalendarAssignmentSchedule": null,
            "timeOffPolicy": null,
            "permissionSets": custom_methods.get_netherlands_user_create_permissions(dag_run),
            "policySets": custom_methods.get_netherlands_user_create_policy_sets(dag_run, instance),
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": {
                "uri": dag_run.conf["timesheet_approval_pathuri"],
                "name": null
            },
            "expenseApprovalPath": null,
            "timeOffApprovalPath": {
                "uri": dag_run.conf["timeoff_approval_path_uri"],
                "name": null
            },
            "workAuthorizationApprovalPath": {
                "uri": dag_run.conf["ot_request_approval_path_uri"]
            } if dag_run.conf["ot_request_approval_path_uri"] else null ,
            "timeOffBalancePayoutApprovalPath": null,
            "customFieldValues": custom_methods.get_netherlands_user_create_custom_fields(dag_run),
            "assignedActivities": [],
            "timeZone":  {
                "uri": dag_run.conf["timezoneuri"],
                "IANAName": null
            },
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": custom_methods.get_netherlands_user_create_location(dag_run),
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
            "costCenterSchedule": [],
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
                    "effectiveDate": null if dag_run.conf["onsite_direct_recruit"].lower() == "local_hire" else \
                    (rail.parse_date(dag_run.conf["onsite_start_date"], "%Y-%m-%d") if dag_run.conf["onsite_start_date"] \
                    and dag_run.conf["onsite_start_date"] not in INVALID_DATES else null)
                }
            ],
            "policyDataAccessScopes": custom_methods.get_netherlands_user_create_policy_data_access(dag_run),
            "payRuleScriptSchedule": custom_methods.get_netherlands_user_payrule_script(dag_run),
            "displayNameParameter": {
                "displayName": dag_run.conf["employee_first_name"] + " " + dag_run.conf["employee_last_name"] + " " + dag_run.conf["employee_id"]
            },
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": "urn:replicon:number-group-separator:language-default",
            "extensionFieldValues": custom_methods.get_netherlands_user_create_oefs(dag_run)
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
        last_name= dag_run.conf["employee_last_name"]
        basic_details_logs += "Last name updated"
    if first_name or last_name:
        display_name = dag_run.conf["employee_first_name"] + " " + dag_run.conf["employee_last_name"] + " " + dag_run.conf["employee_id"]
        basic_details_logs += "Display Name updated"
    if dag_run.conf["employee_email_id"] != rail.result("get_update_user_details")["userDetails"]["emailAddress"]:
        email_id = dag_run.conf["employee_email_id"]
        basic_details_logs += "Email updated"
    basic_details = {
        "firstName": {
            "value": first_name
            } if first_name else null,
        "lastName": {
            "value":last_name
            }if last_name else null,
        "loginName": null,
        "displayName":{
            "value":display_name
        }if display_name else null,
        "emailAddress": {
        "value": email_id
        }if email_id else null
        }

    return basic_details, basic_details_logs

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
    "locationSchedule":custom_methods.check_if_netherlands_user_location_update(dag_run)[0],
    "extensionFields": custom_methods.get_extension_field_values_updates(dag_run)[0],
    "customFields": custom_methods.get_netherlands_user_update_custom_fields(dag_run)[0],
    "departmentGroupSchedule": get_department_update(dag_run)[0],
  },
  "userModificationOptionUri": "urn:replicon:user-modification-option:save",
  "unitOfWorkId": str(uuid4())
}

def get_put_column_settings_for_pm_timesheets_data(user_uri):
    return {
  "userUri": user_uri,
  "listId": "myTeamTimeSheet_list",
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
      "columnUri": "urn:replicon:timesheet-list-column:project-time-duration",
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
      "columnUri": "urn:replicon:timesheet-list-column:project-leader-projects-total-time-duration",
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

def get_nl_annual_acquistion_payload(dag_run):
    effective_date = dt.strptime(dag_run.conf["date_of_joining"], "%Y-%m-%d")
    _year = effective_date.year
    total_days = 365 + calendar.isleap(_year)
    days_remaining = (dt(day=31,month=12,year=_year) - effective_date).days + 1
    yearly_entitlement = 25
    balance = (days_remaining * yearly_entitlement) / total_days
    return {
        "timeOffAccount": {
            "userUri": rail.result("create_netherlands_user"),
            "timeOffTypeUri": dag_run.conf["annual_leave_uri"]
        },
  "policySetScheduleEntries": [
    {
      "effectiveDate": rail.parse_date(dag_run.conf["date_of_joining"], "%Y-%m-%d"),
      "description": "Effective On"+ dag_run.conf["date_of_joining"],
      "policySet": {
        "timeOffBalanceEventScripts": [
          {
            "scriptTarget": {
              "uri": dag_run.conf["starting_balance_uri"]
            },
            "additionalParameters": [
              {
                "keyUri": "urn:replicon:script-key:parameter:amount",
                "value": {
                  "number": str(round(balance,2))
                }
              },
              {
                "keyUri": "urn:replicon:script-key:parameter:precedence",
                "value": {
                  "number": "10"
                }
              }
            ]
          },
          {
            "scriptTarget": {
              "uri": dag_run.conf["yearly_accrual_uri"]
            },
            "additionalParameters": [
              {
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                  "number": "25"
                }
              },
              {
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-month",
                "value": {
                  "uri": "urn:replicon:month:january"
                }
              },
              {
                "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month",
                "value": {
                  "uri": "urn:replicon:monthly-frequency-start-day-option:1st"
                }
              },
              {
                "keyUri": "urn:replicon:script-key:parameter:proration-option",
                "value": {
                  "uri": "urn:replicon:time-off-policy-proration-option:start-and-end-of-policy"
                }
              },
              {
                "keyUri": "urn:replicon:script-key:parameter:precedence",
                "value": {
                  "number": "30"
                }
              }
            ]
          },
          {
            "scriptTarget": {
              "uri": dag_run.conf["yearly_reset_uri"]
            },
            "additionalParameters": [
              {
                "keyUri": "urn:replicon:script-key:parameter:reset-on-month",
                "value": {
                  "uri": "urn:replicon:month:january"
                }
              },
              {
                "keyUri": "urn:replicon:script-key:parameter:reset-on-day-of-month",
                "value": {
                  "uri": "urn:replicon:monthly-frequency-start-day-option:1st"
                }
              },
              {
                "keyUri": "urn:replicon:script-key:parameter:periodic-reset-option",
                "value": {
                  "uri": "urn:replicon:time-off-policy-reset-option:reset-balance-to-specific-value"
                }
              },
              {
                "keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                "value": {
                  "number": "25"
                }
              },
              {
                "keyUri": "urn:replicon:script-key:parameter:precedence",
                "value": {
                  "number": "20"
                }
              }
            ]
          }
        ],
        "timeOffValidationScripts": [
          {
            "additionalParameters": [
              {
                "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                "value": {
                  "number": 0
                }
              }
            ],
           "scriptTarget": {
               "uri": dag_run.conf["prevent_balance_overdraw_uri"]
            }
          },
          {
            "additionalParameters": [],
           "scriptTarget": {
              "uri": dag_run.conf["nl_past_booking_uri"]
            }
          },
          {
            "additionalParameters": [
              {
                "keyUri": "urn:replicon:script-key:parameter:probationary-period",
                "value": {
                  "number": 90
                }
              },
              {
                "keyUri": "urn:replicon:script-key:parameter:probationary-period-unit",
                "value": {
                  "uri": "urn:replicon:time-off-expire-after-unit:days"
                }
              }
            ],
           "scriptTarget": {
              "uri": dag_run.conf["prevent_user_during_probation_uri"]
            }
          }
        ]
      },
    }
  ]
}

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
            "extensionFields":  list(map(lambda i: {"value": i}, custom_methods.get_netherlands_user_create_oefs(dag_run,))),
            "customFields": list(map(lambda i: {"value": i}, custom_methods.get_netherlands_user_create_custom_fields(dag_run))),
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": list(map(lambda i: {"permissionSetPolicy": i}, custom_methods.get_netherlands_user_create_permissions(dag_run))),
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
            "departmentGroupSchedule": [],
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
