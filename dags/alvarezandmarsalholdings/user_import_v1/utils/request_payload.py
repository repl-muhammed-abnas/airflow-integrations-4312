import rail
from datetime import datetime
import uuid
import json

null = None
true = "true"
false = "false"

MANDATORY_FIELDS = {"employee_id": "Employee_ID", "workday_user_name": "Workday_User_Name", "preferred_first_name": "Preferred_First_Name", "preferred_last_name": "Preferred_Last_Name", "email": "Email", "login_status": "Worker_Status", "employee_type": "Worker_Sub_Type",
                    "employee_type_effective_date": "Worker_Sub_Type_Effective_Date", "start_date": "Hire_Date", "office_country": "Office_Country", "office_location_code": "Office_Location_Code",
                    "office_location_effective_date": "Office_Location_Effective_Date", "cost_center_code": "Cost_Center_Code", "cost_center_description": "Cost_Center_Description",
                    "cost_center_effective_date": "Cost_Center_Effective_Date", "job_category": "Job_Category", "job_category_code": "Job_Category_Code", "job_category_effective_date": "Job_Category_Effective_Date", "pay_rate_type": "Pay_Rate_Type",
                    "pay_rate_type_effective_date": "Pay_Rate_Type_Effective_Date", "job_exempt": "Job_Exempt", "job_exempt_effective_date": "Job_Exempt_Effective_Date"}

DEFAULT_USER_PERMISSIONS = ['ZT User', 'End User without Report Edit']

DATE_FORMAT = "%m/%d/%Y"


def get_replicon_date(date_str):
    if not date_str:
        return None

    try:
        date = datetime.strptime(date_str, DATE_FORMAT)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def get_date_from_replicon_date(replicon_date):
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])


def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")

    return rail.smartjoin_by_delim(missing_fields, ";")


def test_valid_fields(dag_run):
    # pylint: disable=too-many-return-statements
    startdate = get_replicon_date(dag_run.conf['start_date'])
    if not startdate:
        return False
    if dag_run.conf['end_date']:
        enddate = get_replicon_date(dag_run.conf['end_date'])
        if not enddate:
            return False
    if dag_run.conf['login_status'].lower() not in ['active', 'terminated']:
        return False
    if dag_run.conf['office_country'].lower() == 'us' and not dag_run.conf['office_state']:
        return False
    if not dag_run.conf['location_uri']:
        return False
    return True


def get_invalid_fields_message(dag_run):
    log = []
    startdate = get_replicon_date(dag_run.conf['start_date'])
    if not startdate:
        log.append('Invalid format for Hire Date')

    if dag_run.conf['end_date']:
        enddate = get_replicon_date(dag_run.conf['end_date'])
        if not enddate:
            log.append('Invalid format for Last worked day')

    if dag_run.conf['login_status'].lower() not in ['active', 'terminated']:
        log.append('Worker Status should be either Active or Terminated')

    if dag_run.conf['office_country'].lower() == 'us' and not dag_run.conf['office_state']:
        log.append('Office State is a mandatory field for country US')

    if not dag_run.conf['location_uri']:
        log.append('Location is not present')

    return rail.smartjoin_by_delim(log, ";")


def get_process_users_conf(item, batch_count):
    return {
        **item,
        **{
            "modulo": int(item['record_id']) % batch_count,
            'supervisor_log': rail.result('create_supervisor_log'),
            'placeholder_timeoffuri': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_timeoff_types'), 'displayText', "Placeholder Time Off", 'uri'),
            'cost_center_name_by_code': rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_updated_cost_centers'), 'code', item['cost_center_code'], 'name'),
            'location_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_location_groups'), 'displayText', item['office_location_code'], 'uri'),
            'run_date': datetime.strftime(datetime.today(), DATE_FORMAT),
            'time_off_type_list': rail.result('get_all_timeoff_types')
        }
    }


def update_old_profile_login_name(dag_run):
    def get_end_date_for_oldprofile():
        end_date = rail.result('get_user_data_based_on_login_name')[
            0]['userDetails']["employmentDateRange"]['endDate']
        return get_date_from_replicon_date(end_date).strftime("%d%m%Y")
    return {
        'userUri': rail.result('get_user_data_based_on_login_name')[0]['userDetails']['uri'],
        'loginName': str(dag_run.conf['workday_user_name'])+"." + get_end_date_for_oldprofile()
    }


def validate_enddate_for_old_profile():
    return bool(rail.result('get_user_data_based_on_login_name')[0]['userDetails']["employmentDateRange"]['endDate'])


def get_timesheet_template(dag_run, config):

    def get_timesheet_template(country, state, pay_rate_type, job_exempt, fte_percentage):
        if not fte_percentage:
            return null
        return config.TIMESHEET_TEMPLATE_MAPPER.get(country, {}).get(state, {}).get(pay_rate_type, {}).get(job_exempt, {}).get(str(fte_percentage), None)

    country = dag_run.conf["office_country"] if dag_run.conf["office_country"] in [
        'US', 'DE', 'ES'] else "Other Countries"
    state = dag_run.conf["office_state"] if dag_run.conf["office_country"] == 'US' else 'All'
    pay_rate_type = dag_run.conf["pay_rate_type"] if dag_run.conf["office_country"] == "DE" else "All"
    job_exempt = dag_run.conf["job_exempt"] if dag_run.conf["office_country"] == 'US' else "All"

    fte_percentage = ("<100" if int(float(
        dag_run.conf["fte_percentage"])) < 100 else "100") if dag_run.conf["fte_percentage"] else ""

    template = get_timesheet_template(
        country, state, pay_rate_type, job_exempt, fte_percentage)

    if template:
        return {
            "value": {
                "uri": null,
                "name": template
            }
        }
    return null


def get_timesheet_template_update(dag_run, config):
    existing_timesheet_template = rail.result("get_user_details")["timesheetTemplate"].get("displayText", '') if rail.result(
        "get_user_details")["timesheetTemplate"] else ''

    def get_timesheet_template(country, state, pay_rate_type, job_exempt, fte_percentage):
        if not fte_percentage:
            return null
        return config.TIMESHEET_TEMPLATE_MAPPER.get(country, {}).get(state, {}).get(pay_rate_type, {}).get(job_exempt, {}).get(str(fte_percentage), None)

    country = dag_run.conf["office_country"] if dag_run.conf["office_country"] in [
        'US', 'DE', 'ES'] else "Other Countries"
    state = dag_run.conf["office_state"] if dag_run.conf["office_country"] == 'US' else 'All'
    pay_rate_type = dag_run.conf["pay_rate_type"] if dag_run.conf["office_country"] == "DE" else "All"
    job_exempt = dag_run.conf["job_exempt"] if dag_run.conf["office_country"] == 'US' else "All"
    fte_percentage = ("<100" if int(float(
        dag_run.conf["fte_percentage"])) < 100 else "100") if dag_run.conf["fte_percentage"] else ""

    template = get_timesheet_template(
        country, state, pay_rate_type, job_exempt, fte_percentage)

    if template and (template != existing_timesheet_template):
        return {
            "value": {
                "uri": null,
                "name": template
            }
        }

    return null


def get_timezone_uri(dag_run, config):
    for obj in config.TIMEZONE_MAPPER:
        if obj["office_location_code"] == dag_run.conf['office_location_code']:
            return obj['uri']
    return None


def get_timezone_uri_update(dag_run, config):
    existing_timezone_uri = rail.result("get_user_details")[
        "timeZone"].get("uri", "") if rail.result("get_user_details")["timeZone"] else ''
    for obj in config.TIMEZONE_MAPPER:
        if obj["office_location_code"] == dag_run.conf['office_location_code']:
            if existing_timezone_uri != obj['uri']:
                return {
                    "value": {
                        "uri": obj['uri'],
                        "IANAName": null
                    }
                }
    return null


def get_holiday_calendar(config, dag_run):
    holiday_cal = None
    for obj in config.HOLIDAY_CALENDAR_MAPPER:
        if obj["office_location_code"] == dag_run.conf['office_location_code']:
            holiday_cal = obj['holiday_calendar_name']
    if holiday_cal:
        return [
            {
                "dateRange": null,
                "item": {
                    "uri": null,
                    "name": holiday_cal
                }
            }
        ]
    return []


def get_holiday_calendar_update(config, dag_run):
    existing_holiday_cal = rail.result("get_user_details")[
        'holidayCalendar'].get('name', '') if rail.result("get_user_details")[
        'holidayCalendar'] else ''

    holiday_cal = None

    for obj in config.HOLIDAY_CALENDAR_MAPPER:
        if obj["office_location_code"] == dag_run.conf['office_location_code']:
            holiday_cal = obj['holiday_calendar_name']

    if holiday_cal and existing_holiday_cal != holiday_cal:
        return [
            {
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["office_location_effective_date"], DATE_FORMAT),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                } if dag_run.conf["office_location_effective_date"] else null,
                "item": {
                    "uri": null,
                    "name": holiday_cal
                }
            }
        ]
    return []


def get_time_off_types(config, dag_run):
    if not dag_run.conf['fte_percentage']:
        return []

    country_data = config.TIMEOFF_MAPPER.get(
        dag_run.conf['office_country'], {})

    if isinstance(country_data, dict):
        worker_data = country_data.get(dag_run.conf['employee_type'], {})
        time_off_100 = worker_data.get("100", [])
        time_off_any = worker_data.get("Any", [])

        # Get the list of available timeoff types from dag_run.conf
        available_timeoff_types = []
        if dag_run.conf.get('time_off_type_list'):
            available_timeoff_types = [timeoff['displayText'] for timeoff in dag_run.conf['time_off_type_list']]

        # Filter timeoff types to only include those present in the available list
        if available_timeoff_types:
            time_off_100 = list(filter(lambda timeoff: timeoff in available_timeoff_types, time_off_100))
            time_off_any = list(filter(lambda timeoff: timeoff in available_timeoff_types, time_off_any))

        if int(float(dag_run.conf['fte_percentage'])) == 100:
            return {
                'timeoff_for_100_fte': list(set(time_off_100+time_off_any)),
                'timeoff_for_any_fte': []
            }

        return {
            'timeoff_for_100_fte': [],
            'timeoff_for_any_fte': list(time_off_any)
        }
    return {
        'timeoff_for_100_fte': [],
        'timeoff_for_any_fte': []
    }


def get_schedule_name(dag_run):
    if dag_run.conf["schedule_type"]:
        return dag_run.conf["schedule_type"]
    if dag_run.conf["weekly_working_hours"]:
        day_hour = round(float(dag_run.conf["weekly_working_hours"])/5, 2)
        schedule_type_string = "0.00|" + \
            "|".join([str(day_hour) for i in range(5)])+"|0.00"
        return schedule_type_string
    return "8 hours/day; Mon-Fri"


def get_schedule_name_update(dag_run):
    if dag_run.conf["profile_status"] == "LOA":
        schedule = "0.00|0.00|0.00|0.00|0.00|0.00|0.00"
    elif dag_run.conf["schedule_type"]:
        schedule = dag_run.conf["schedule_type"]
    elif dag_run.conf["weekly_working_hours"]:
        day_hour = round(float(dag_run.conf["weekly_working_hours"])/5, 2)
        schedule_type_string = "0.00|" + \
            "|".join([str(day_hour) for i in range(5)])+"|0.00"
        schedule = schedule_type_string
    else:
        schedule = "8 hours/day; Mon-Fri"

    return [
        {
            "dateRange": {
                "startDate": rail.parse_date(dag_run.conf["schedule_type_effective_date"], DATE_FORMAT),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            } if dag_run.conf["schedule_type_effective_date"] else null,
            "item": {
                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                "officeSchedule": {
                    "officeScheduleUri": null,
                    "name": schedule
                }
            }
        }
    ]


def get_add_user_timeoff_types_with_policies(config, dag_run, fte_percentage, start_date, placeholder_policyset):
    time_off_types = get_time_off_types(config, dag_run)
    final_timeoff_types_with_policies = []
    if time_off_types['timeoff_for_100_fte']:
        final_timeoff_types_with_policies.extend([{
            "timeOffType": {
                "uri": null,
                "name": timeoff_type
            },
            "isTimeOffAllowedAgainstThisTimeOffType": true,
            "applyDefaultTimeOffTypePolicy": true,
            "defaultTimeOffTypePolicyEffectiveDate": rail.parse_date(start_date, DATE_FORMAT),
            "policySchedule": []
        } for timeoff_type in time_off_types['timeoff_for_100_fte']])

    if time_off_types['timeoff_for_any_fte']:
        final_timeoff_types_with_policies.extend([{
            "timeOffType": {
                "uri": null,
                "name": timeoff_type
            },
            "isTimeOffAllowedAgainstThisTimeOffType": true,
            "applyDefaultTimeOffTypePolicy": false,
            "defaultTimeOffTypePolicyEffectiveDate": null,
            "policySchedule": [
                {
                    "dateRange": {
                        "startDate": rail.parse_date(start_date, DATE_FORMAT)
                    },
                    "item": {
                        "description": f"Effective on - {start_date}",
                        "policySet": placeholder_policyset
                    }
                }
            ]
        } for timeoff_type in time_off_types['timeoff_for_any_fte']])

    return final_timeoff_types_with_policies


def get_extension_fields(dag_run):
    extension_fields = []
    dropdown_custom_fields = {"Management Level": "management_level",
                              "Event Identifier": "event_identifier", "Profile Status": "profile_status"}
    text_custom_fields = {"Performance Manager": "performance_manager",
                          "Weekly Working Hours": "weekly_working_hours", "FTE Percentage": "fte_percentage"}
    for field, val in text_custom_fields.items():
        if dag_run.conf[val]:
            extension_fields.append({
                "value": {
                    "definition": {
                        "uri": null,
                        "name": field
                    },
                    "tag": null,
                    "numericValue": None,
                    "textValue": dag_run.conf[val],
                    "fileValue": None,
                    "jsonValue": None
                }
            })
    for field, val in dropdown_custom_fields.items():
        if dag_run.conf[val]:
            if field == 'Profile Status':
                if dag_run.conf[val] in ['Active', 'LOA']:
                    extension_fields.append({
                        "value": {
                            "definition": {
                                "uri": null,
                                "name": field
                            },
                            "tag": {
                                "uri": null,
                                "slug": null,
                                "tagName": {
                                    "name": dag_run.conf[val],
                                    "tagDefinitionUri": null
                                }
                            },
                            "numericValue": null,
                            "textValue": null,
                            "fileValue": null,
                            "jsonValue": null
                        }
                    })
                continue

            extension_fields.append({
                "value": {
                    "definition": {
                        "uri": null,
                        "name": field
                    },
                    "tag": {
                        "uri": null,
                        "slug": null,
                        "tagName": {
                            "name": dag_run.conf[val],
                            "tagDefinitionUri": null
                        }
                    },
                    "numericValue": null,
                    "textValue": null,
                    "fileValue": null,
                    "jsonValue": null
                }
            })
    return extension_fields


def get_create_new_user_payload(dag_run, config, placeholder_policyset):
    return {
        "target": null,
        "template": {
            "templateTarget": null
        },
        "modifications": {
            "firstName": {
                "value": dag_run.conf["preferred_first_name"]
            },
            "lastName": {
                "value": dag_run.conf["preferred_last_name"] + (dag_run.conf["suffix"] if dag_run.conf["suffix"] else "")
            },
            "loginName": {
                "value": dag_run.conf["workday_user_name"]
            },
            "displayName": null,
            "emailAddress": {
                "value": dag_run.conf["email"]
            },
            "employeeId": {
                "value": dag_run.conf["employee_id"]
            },
            "employmentDateRange": {
                "value": {
                    "startDate": rail.parse_date(dag_run.conf["start_date"], DATE_FORMAT),
                    "endDate": rail.parse_date(dag_run.conf["end_date"], DATE_FORMAT) if (dag_run.conf["end_date"] and dag_run.conf["login_status"].lower() == 'terminated') else null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": "true" if dag_run.conf["login_status"].lower() == "active" else "false"
                    },
                    "forcePasswordChange": null,
                    "ssoName": {
                        "value": dag_run.conf["workday_user_name"]
                    },
                    "ssoNameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name",
                    "password": null,
                    "authenticationProviders": [],
                    "emailMFAResendVerificationEmail": null,
                    "emailMFATryAddMethodFromUsersEmail": null,
                    "isMFAMethodRequired": null,
                    "clearIsLockedOut": null
                }
            },
            "timesheetApprovalPath": {
                "value": {
                    "uri": null,
                    "name": "A&M Global Timesheet Approval Path"
                }
            },
            "timeEntryApprovalPath": {
                "value": {
                    "uri": null,
                    "name": "A&M Global Time Entry Approval Path"
                }
            },
            "workAuthorizationApprovalPath": null,
            "timeoffApprovalPath": {
                "value": {
                    "uri": null,
                    "name": "A&M Global Time Off Approval Path"
                }
            },
            "timeOffBalancePayoutApprovalPath": null,
            "defaultActivity": null,
            "expenseApprovalPath": null,
            "timeZone": {
                "value": {
                    "uri": get_timezone_uri(dag_run, config),
                    "IANAName": null
                }
            } if get_timezone_uri(dag_run, config) else null,
            "workWeekStartDay": {
                "value": {
                    "uri": "urn:replicon:day-of-week:sunday"
                }
            },
            "defaultBillingRate": null,
            "userPreferences": null,
            "formattings": null,
            "notificationPreferences": null,
            "timesheetTemplate": get_timesheet_template(dag_run, config),
            "timeoffTemplate": {
                "value": {
                    "uri": null,
                    "name": "Time Off"
                }
            },
            "timeOffCalendarVisibility": {
                "value": {
                    "locations": [],
                    "divisions": [],
                    "costCenters": [],
                    "serviceCenters": [],
                    "departmentGroups": [
                        {
                            "departmentGroup": {
                                "uri": null,
                                "parent": {
                                    "uri": null,
                                    "parent": null,
                                    "name": "Alvarez and Marsal Holdings",
                                    "parameterCorrelationId": null
                                },
                                "name": dag_run.conf["office_country"],
                                "parameterCorrelationId": null
                            },
                            "groupSpecificationModeUri": null,
                            "groupDescendantModeUri": null
                        }
                    ],
                    "employeeTypeGroups": []
                }
            },
            "expenseTemplate": null,
            "workAuthorizationTemplate": null,
            "punchEntryPolicy": null,
            "holidayCalendar": null,
            "extensionFields": get_extension_fields(dag_run),
            "customFields": [{
                "value": {
                    "customField": {
                        "uri": null,
                        "name": "Profile Status Effective Date"
                    },
                    "text": null,
                    "date": rail.parse_date(dag_run.conf["profile_status_effective_date"], DATE_FORMAT),
                    "dropDownOption": null,
                    "number": null
                }
            }] if dag_run.conf["profile_status_effective_date"] else [],
            "products": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "uri": "urn:replicon-saas:product:time-off-enterprise",
                            "name": null
                        },
                        {
                            "uri": "urn:replicon-saas:product:time-intelligence",
                            "name": null
                        },
                        {
                            "uri": "urn:replicon-saas:product:wfm-enterprise",
                            "name": null
                        },
                        {
                            "uri": "urn:replicon-saas:product:time-bill-plus",
                            "name": null
                        }
                    ]
                }
            ],
            "skills": [],
            "activities": [],
            "policySets": [],
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "uri": null,
                                "name": permission_name
                            },
                            "groupAccessFilter": null
                        } for permission_name in DEFAULT_USER_PERMISSIONS
                    ]
                }
            ],
            "bankedTimePolicies": [],
            "timeOffTypes": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": get_add_user_timeoff_types_with_policies(config, dag_run, dag_run.conf['fte_percentage'], dag_run.conf['start_date'], placeholder_policyset)
                }
            ] if dag_run.conf["fte_percentage"] and dag_run.conf['event_identifier'] not in ['HOME_IA', 'IA_HOME', 'HOME_ADDJOB', 'ADDJOB_HOME'] else [],
            "locationSchedule": [
                {
                    "dateRange": null,
                    "item": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf["job_category"]
                    }
                }
            ] if dag_run.conf["job_category"] else [],
            "divisionSchedule":  [
                {
                    "dateRange": null,
                    "item": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf["pay_rate_type"]
                    }
                }
            ] if dag_run.conf["pay_rate_type"] else [],
            "costCenterSchedule": [
                {
                    "dateRange": null,
                    "item": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf["cost_center_name_by_code"]
                    }
                }
            ] if dag_run.conf["cost_center_name_by_code"] else [],
            "serviceCenterSchedule": [
                {
                    "dateRange": null,
                    "item": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf["job_exempt"]
                    }
                }
            ] if dag_run.conf["job_exempt"] else [],
            "departmentGroupSchedule": [
                {
                    "dateRange": null,
                    "item": {
                        "uri": dag_run.conf["location_uri"],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                    }
                }
            ] if dag_run.conf["location_uri"] else [],
            "employeeTypeGroupSchedule": [
                {
                    "dateRange": null,
                    "item": {
                        "uri": null,
                        "parent": null,
                        "name": dag_run.conf["employee_type"],
                        "parameterCorrelationId": null
                    }
                }
            ] if dag_run.conf["employee_type"] else [],
            "timesheetPeriodSchedule": [
                {
                    "dateRange": {
                        "startDate": null,
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "item": {
                        "uri": null,
                        "name": "A&M Global Timesheet Period"
                    }
                }
            ],
            "holidayCalendarSchedule": get_holiday_calendar(config, dag_run),
            "scheduleTypeSchedule":
            [
                {
                    "dateRange": null,
                    "item": {
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                        "officeSchedule": {
                            "officeScheduleUri": null,
                            "name": get_schedule_name(dag_run)
                        }
                    }
                }
            ],
            "payRuleSchedule": [],
            "placeSchedule": [],
            "payRateSchedule": [],
            "projectRoleSchedule": [],
            "costNormalizationRuleSchedule": [],
            "hourlyRatesSchedule": [],
            "substituteUserSchedule": []
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_assign_supervisor_permission_payload(supervisor_permission):
    return {
        "target": {
            "uri": rail.result("get_supervisor_details")["userDetails"]["uri"],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "template": null,
        "modifications": {
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "uri": null,
                                "name": supervisor_permission
                            },
                            "groupAccessFilter": null
                        }
                    ]
                }
            ]
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_basic_user_details_update(dag_run):
    first_name = null
    last_name = null
    basic_details_logs = []
    email_id = null
    status = null
    replicon_start_date = rail.result("get_user_details")[
        "userDetails"]["employmentDateRange"]["startDate"]
    existing_start_date = f'{replicon_start_date["month"]}/{replicon_start_date["day"]}/{replicon_start_date["year"]}' if replicon_start_date else null
    replicon_end_date = rail.result("get_user_details")[
        "userDetails"]["employmentDateRange"]["endDate"]
    existing_end_date = f'{replicon_end_date["month"]}/{replicon_end_date["day"]}/{replicon_end_date["year"]}' if replicon_end_date else null
    start_date = rail.parse_date(
        existing_start_date, DATE_FORMAT) if existing_start_date else null
    end_date = rail.parse_date(
        existing_end_date, DATE_FORMAT) if existing_end_date else null
    current_user_status_is_enabled = rail.result("get_user_details")["userDetails"]["isEnabled"]
    if dag_run.conf["preferred_first_name"] != rail.result("get_user_details")["userDetails"]["firstName"]:
        first_name = dag_run.conf["preferred_first_name"]
        basic_details_logs.append("First name updated")
    if dag_run.conf["preferred_last_name"] + (dag_run.conf["suffix"] if dag_run.conf["suffix"] else "") != rail.result("get_user_details")["userDetails"]["lastName"]:
        last_name = dag_run.conf["preferred_last_name"] + \
            (dag_run.conf["suffix"] if dag_run.conf["suffix"] else "")
        basic_details_logs.append("Last name updated")
    if dag_run.conf["email"] != rail.result("get_user_details")["userDetails"]["emailAddress"]:
        email_id = dag_run.conf["email"]
        basic_details_logs.append("Email updated")
    if not existing_start_date:
        start_date = rail.parse_date(dag_run.conf["start_date"], DATE_FORMAT)
        basic_details_logs.append("Start date updated")
    if existing_start_date and rail.parse_date(dag_run.conf["start_date"], DATE_FORMAT) != rail.parse_date(existing_start_date, DATE_FORMAT):
        start_date = rail.parse_date(dag_run.conf["start_date"], DATE_FORMAT)
        basic_details_logs.append("Start date updated")
    if not existing_end_date and dag_run.conf["end_date"] and dag_run.conf["login_status"].lower() == 'terminated':
        end_date = rail.parse_date(dag_run.conf["end_date"], DATE_FORMAT)
        basic_details_logs.append("End date updated")
    if dag_run.conf["end_date"] and existing_end_date and rail.parse_date(dag_run.conf["end_date"],
                                                                          DATE_FORMAT) != rail.parse_date(existing_end_date, DATE_FORMAT):
        end_date = rail.parse_date(dag_run.conf["end_date"], DATE_FORMAT)
        basic_details_logs.append("End date updated")
    if existing_end_date and dag_run.conf["login_status"] == "Active" and not current_user_status_is_enabled:
        end_date = null
        basic_details_logs.append("End date updated")

    basic_details = {
        "firstName": {
            "value": first_name
        } if first_name else null,
        "lastName": {
            "value": last_name
        } if last_name else null,
        "emailAddress": {
            "value": email_id
        } if email_id else null,
        "securitySettings": {
            "value": {
                "loginEnabled": {
                    "value": "true" if dag_run.conf["login_status"] == "Active" else "false"
                },
                "ssoName": {
                    "value": dag_run.conf["workday_user_name"]
                },
            }
        } if dag_run.conf["login_status"] else null,
        "employmentDateRange": {
            "value": {
                "startDate": start_date,
                "endDate": end_date,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            }
        } if start_date or end_date else null,
    }

    return basic_details, basic_details_logs


def get_employeetype_group(dag_run):
    if dag_run.conf['employee_type'] and (not (rail.result('get_effective_user_group_membership') and rail.result(
        'get_effective_user_group_membership')['employeeTypes'] and rail.result(
        'get_effective_user_group_membership')['employeeTypes'][0]['employeeType']) or (
        rail.result(
            'get_effective_user_group_membership')['employeeTypes'][0]['employeeType']['employeeType']['displayText'] != dag_run.conf['employee_type'])):
        return [
            {
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["employee_type_effective_date"], DATE_FORMAT),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                } if dag_run.conf["employee_type_effective_date"] else null,
                "item": {
                    "uri": null,
                    "parent": null,
                    "name": dag_run.conf["employee_type"],
                    "parameterCorrelationId": null
                }
            }
        ]

    return []


def get_cost_center_group(dag_run):
    if dag_run.conf['cost_center_name_by_code'] and (not (rail.result('get_effective_user_group_membership') and rail.result(
        'get_effective_user_group_membership')['costCenters'] and rail.result(
        'get_effective_user_group_membership')['costCenters'][0]['costCenter']) or (
        rail.result(
            'get_effective_user_group_membership')['costCenters'][0]['costCenter']['costCenter']['displayText'] != dag_run.conf['cost_center_name_by_code'])):
        return [
            {
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["cost_center_effective_date"], DATE_FORMAT),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                } if dag_run.conf["cost_center_effective_date"] else null,
                "item": {
                    "uri": null,
                    "parentUri": null,
                    "name": dag_run.conf["cost_center_name_by_code"]
                }
            }
        ]

    return []


def get_service_center_group(dag_run):
    if dag_run.conf["job_exempt"] and (not (rail.result('get_effective_user_group_membership') and rail.result(
        'get_effective_user_group_membership')['serviceCenters'] and rail.result(
        'get_effective_user_group_membership')['serviceCenters'][0]['serviceCenter']) or (
        rail.result(
            'get_effective_user_group_membership')['serviceCenters'][0]['serviceCenter']['serviceCenter']['displayText'] != dag_run.conf["job_exempt"])):
        return [
            {
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["job_exempt_effective_date"], DATE_FORMAT),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                } if dag_run.conf["job_exempt_effective_date"] else null,
                "item": {
                    "uri": null,
                    "parentUri": null,
                    "name": dag_run.conf["job_exempt"]
                }
            }
        ]

    return []


def get_division_group_schedule(dag_run):
    if dag_run.conf['pay_rate_type'] and (not (rail.result('get_effective_user_group_membership') and rail.result(
        'get_effective_user_group_membership')['divisions'] and rail.result(
        'get_effective_user_group_membership')['divisions'][0]['division']) or (
        rail.result(
            'get_effective_user_group_membership')['divisions'][0]['division']['division']['displayText'] != dag_run.conf['pay_rate_type'])):
        return [
            {
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["pay_rate_type_effective_date"], DATE_FORMAT),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                } if dag_run.conf["pay_rate_type_effective_date"] else null,
                "item": {
                    "uri": null,
                    "parentUri": null,
                    "name": dag_run.conf["pay_rate_type"]
                }
            }
        ]

    return []


def get_department_group_schedule(dag_run):
    if dag_run.conf["office_location_code"] and (not (rail.result('get_effective_user_group_membership') and rail.result(
        'get_effective_user_group_membership')['departments'] and rail.result(
        'get_effective_user_group_membership')['departments'][0]['department']) or (
        rail.result(
            'get_effective_user_group_membership')['departments'][0]['department']['department']['displayText'] != dag_run.conf["office_location_code"])):
        return [
            {
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["office_location_effective_date"], DATE_FORMAT),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                } if dag_run.conf["office_location_effective_date"] else null,
                "item":
                    {
                        "uri": dag_run.conf["location_uri"],
                        "parent": null,
                        "name": null,
                        "parameterCorrelationId": null
                }
            }
        ]

    return []


def get_location_schedule(dag_run):
    if dag_run.conf["job_category"] and (not (rail.result('get_effective_user_group_membership') and rail.result(
        'get_effective_user_group_membership')['locations'] and rail.result(
        'get_effective_user_group_membership')['locations'][0]['location']) or (
        rail.result(
            'get_effective_user_group_membership')['locations'][0]['location']['location']['displayText'] != dag_run.conf["job_category"])):
        return [
            {
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["job_category_effective_date"], DATE_FORMAT),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                } if dag_run.conf["job_category_effective_date"] else null,
                "item": {
                    "uri": null,
                    "parentUri": null,
                    "name": dag_run.conf["job_category"]
                }
            }
        ]

    return []


def get_update_user_payload(dag_run, config, timeoff_payload_update_user):
    basic_details, basic_details_logs = get_basic_user_details_update(dag_run)
    return {
        "target": {
            "uri": rail.result("get_user_details")["userDetails"]['uri'],
        },
        "template": {
            "templateTarget": null
        },
        "modifications": {
            **basic_details,
            "timeOffBalancePayoutApprovalPath": null,
            "defaultActivity": null,
            "expenseApprovalPath": null,
            "timeZone": get_timezone_uri_update(dag_run, config),
            "defaultBillingRate": null,
            "userPreferences": null,
            "formattings": null,
            "notificationPreferences": null,
            "timesheetTemplate": get_timesheet_template_update(dag_run, config),
            "timeOffCalendarVisibility": {
                "value": {
                    "locations": [],
                    "divisions": [],
                    "costCenters": [],
                    "serviceCenters": [],
                    "departmentGroups": [
                        {
                            "departmentGroup": {
                                "uri": null,
                                "parent": {
                                    "uri": null,
                                    "parent": null,
                                    "name": "Alvarez and Marsal Holdings",
                                    "parameterCorrelationId": null
                                },
                                "name": dag_run.conf["office_country"],
                                "parameterCorrelationId": null
                            },
                            "groupSpecificationModeUri": null,
                            "groupDescendantModeUri": null
                        }
                    ],
                    "employeeTypeGroups": []
                }
            },
            "expenseTemplate": null,
            "workAuthorizationTemplate": null,
            "punchEntryPolicy": null,
            "holidayCalendar": null,
            "extensionFields": get_extension_fields(dag_run),
            "customFields": [{
                "value": {
                    "customField": {
                        "uri": null,
                        "name": "Profile Status Effective Date"
                    },
                    "text": null,
                    "date": rail.parse_date(dag_run.conf["profile_status_effective_date"], DATE_FORMAT),
                    "dropDownOption": null,
                    "number": null
                }
            }] if dag_run.conf["profile_status_effective_date"] else [],
            "skills": [],
            "activities": [],
            "policySets": [],
            "permissionSets": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": [
                        {
                            "permissionSetPolicy": {
                                "uri": null,
                                "name": permission_name
                            },
                            "groupAccessFilter": null
                        } for permission_name in DEFAULT_USER_PERMISSIONS
                    ]
                }
            ],
            "bankedTimePolicies": [],
            "timeOffTypes": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:replace",
                    "items": timeoff_payload_update_user
                }
            ] if dag_run.conf['event_identifier'] not in ['HOME_IA', 'IA_HOME', 'HOME_ADDJOB', 'ADDJOB_HOME'] or rail.find_first_by_attr_and_get_attr(rail.result("get_user_details")['userDetails']['extensionFieldValues'], 'definition.displayText',  "FTE Percentage", 'textValue') != dag_run.conf['fte_percentage'] else [],
            "locationSchedule": get_location_schedule(dag_run),
            "divisionSchedule":  get_division_group_schedule(dag_run),
            "costCenterSchedule": get_cost_center_group(dag_run),
            "serviceCenterSchedule": get_service_center_group(dag_run),
            "departmentGroupSchedule": get_department_group_schedule(dag_run),
            "employeeTypeGroupSchedule": get_employeetype_group(dag_run),
            "holidayCalendarSchedule": get_holiday_calendar_update(config, dag_run),
            "scheduleTypeSchedule": get_schedule_name_update(dag_run),
            "payRuleSchedule": [],
            "placeSchedule": [],
            "payRateSchedule": [],
            "projectRoleSchedule": [],
            "costNormalizationRuleSchedule": [],
            "hourlyRatesSchedule": [],
            "substituteUserSchedule": []
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_search_supervisor_payload(dag_run):
    return {
        "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled"
                ],
        "sort": [],
        "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
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
                            "text": dag_run.conf['reporting_manager'],
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


def get_update_supervisor_permission_payload(dag_run):
    permission_set_uris = [dag_run.conf["supervisor_permission_uri"],
                           dag_run.conf["report_user_permission_uri"]]
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


def get_process_new_users_conf(dag_run):
    return {
        **dag_run.conf,
        **{
            'user_log': rail.result('create_user_log')
        }
    }


def get_process_update_users_conf(dag_run):
    return {
        **dag_run.conf,
        **{
            'user_log': rail.result('create_user_log'),
            'useruri': rail.result('get_user_data')[0]["userDetails"]['uri']
        }
    }
