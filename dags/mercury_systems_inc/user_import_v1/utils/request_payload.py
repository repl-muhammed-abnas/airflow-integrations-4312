from datetime import datetime
import rail
import json
from functools import lru_cache
from uuid import uuid4
from mercury_systems_inc.user_import_v1.utils import custom_methods

null = None

FEED_DATE_FORMAT = "%m/%d/%Y"
DATE_FORMAT = "%Y-%m-%d"


@lru_cache(maxsize=128)
def get_date(entry_date):
    try:
        if entry_date and datetime.strptime(entry_date, FEED_DATE_FORMAT):
            return datetime.strftime(datetime.strptime(entry_date, FEED_DATE_FORMAT), DATE_FORMAT)
    except ValueError:
        return "Invalid Date Format"


def row_data_for_input_file(item):
    return [
        item["Employee ID"].strip() if item["Employee ID"] else null,
        item["First Name"].strip() if item["First Name"] else null,
        item["Last Name"].strip() if item["Last Name"] else null,
        item["Preferred Name"].strip() if item["Preferred Name"] else null,
        item["Email"].strip() if item["Email"] else null,
        item["Authentication ID"].strip() if item["Authentication ID"] else null,
        item["Login Name"].strip() if item["Login Name"] else null,
        get_date(item["Hire Date"].strip()) if item["Hire Date"] else null,
        get_date(item["Termination Date"].strip()
                 ) if item["Termination Date"] else null,
        item["Pay Group"].strip() if item["Pay Group"] else null,
        item["Operating Unit"].strip() if item["Operating Unit"] else null,
        item["Business Unit"].strip() if item["Business Unit"] else null,
        item["Chargeable Flag"].strip() if item["Chargeable Flag"] else null,
        item["Job Union ID"].strip() if item["Job Union ID"] else null,
        item["Location Class Description"].strip(
        ) if item["Location Class Description"] else null,
        item["Supervisor ADP Person ID"].strip(
        ) if item["Supervisor ADP Person ID"] else null,
        item["Hourly Cost"].strip() if item["Hourly Cost"] else null,
        item["Pay Type"].strip() if item["Pay Type"] else null,
        item["Emp Status"].strip() if item["Emp Status"] else null,
        item["Work Schedule"].strip() if item["Work Schedule"] else null,
        item["Flexible Vacation Eligible"].strip(
        ) if item["Flexible Vacation Eligible"] else null,
        item["Expected Time Zone"].strip(
        ) if item["Expected Time Zone"] else null,
        item["VMS ID"].strip() if item["VMS ID"] else null,
        item["File ID"].strip() if item["File ID"] else null,
        item["Job Code"].strip() if item["Job Code"] else null,
        item["Department"].strip() if item["Department"] else null,
        item["Full Part Time"].strip() if item["Full Part Time"] else null,
        item["FLSA"].strip() if item["FLSA"] else null,
        item["Job Function"].strip() if item["Job Function"] else null,
        item["Job Family"].strip() if item["Job Family"] else null,
        item["Work Location State"].strip(
        ) if item["Work Location State"] else null,
        item["Work Location Country"].strip(
        ) if item["Work Location Country"] else null,
        item["Work Location Name"].strip(
        ) if item["Work Location Name"] else null,
        item["Work Location Code"].strip(
        ) if item["Work Location Code"] else null,
        item["Work Schedule Start Time"].strip(
        ) if item["Work Schedule Start Time"] else null,
        get_date(item["Effective Date"].strip()
                 ) if item["Effective Date"] else null,
        item["Employee Classification"].strip(
        ) if item["Employee Classification"] else null,
        item["Job Title"].strip() if item["Job Title"] else null,
        item["Manager Type"].strip() if item["Manager Type"] else null,
        item["Punch Entry Policy"].strip(
        ) if item["Punch Entry Policy"] else null,
        item["Timesheet Template"].strip(
        ) if item["Timesheet Template"] else null,
        item["Timesheet Approval Path"].strip(
        ) if item["Timesheet Approval Path"] else null,
        item["Timesheet Period"].strip() if item["Timesheet Period"] else null,
        item["Time Off Template"].strip() if item["Time Off Template"] else null,
        item["Time Off Types"].strip() if item["Time Off Types"] else null,
        item["Holiday Calendar"].strip() if item["Holiday Calendar"] else null,
        item["Pay Rule"].strip() if item["Pay Rule"] else null,
        item["Office Schedule"].strip() if item["Office Schedule"] else null,
        item["Work Week"].strip() if item["Work Week"] else null,
        item['Permissions'].strip() if item['Permissions'] else null
    ]


MANDATORY_FIELDS = {
    "Employee ID": "Employee_ID",
    "First Name": "First_Name",
    "Last Name": "Last_Name",
    "Email": "Email",
    "Hire Date": "Hire_Date",
    "Pay Group": "Pay_Group",
    "Operating Unit": "Operating_Unit",
    "Business Unit": "Business_Unit",
    "Chargeable Flag": "Chargeable_Flag",
    "Job Union ID": "Job_Union_ID",
    "Location Class Description": "Location_Class_Description",
    "Employee Classification": "Employee_Classification",
    "Supervisor ADP Person ID": "Supervisor_ADP_Person_ID",
    "Pay Type": "Pay_Type",
    "Job Function": "Job_Function",
    "FLSA": "FLSA",
    "Work Location State": "Work_Location_State",
    "Job Family": "Job_Family",
    "Work Schedule": "Work_Schedule",
    "Full Part Time": "Full_Part_Time",
    "Work Location Country": "Work_Location_Country",
    "Employee Status": "Emp_Status",
    "Flexible Vacation Eligible": "Flexible_Vacation_Eligible",
    "Expected Time Zone": "Expected_Time_Zone",
    "Work Location Name": "Work_Location_Name",
    "Job Code": "Job_Code",
    "Department": "Department",
    "Work Location Code": "Work_Location_Code",
    "Effective Date": "Effective_Date",
    "Timesheet Template": "Timesheet_Template",
    "Timesheet Period": "Timesheet_Period",
    "Timesheet Approval Path": "Timesheet_Approval_Path",
    "Work Week": "Work_Week",
    "Office Schedule": "Office_Schedule"
}

DATE_VALIDATION_FIELDS = ["Hire_Date", "Termination_Date", "Effective_Date"]


def get_mandatory_fields_exception_message(item, config):
    missing_fields = []
    for log_value, payload_key in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(
                f"{log_value} is not present in feed file for the user")

    for field_name in DATE_VALIDATION_FIELDS:
        if item[field_name] == 'Invalid Date Format':
            missing_fields.append(
                f"{field_name.replace('_', ' ')} is in Invalid Format for the user")

    return (" User Processing is skipped as - " + rail.smartjoin_by_delim(missing_fields, ";"))


def get_add_location_payload(dag_run):
    return {
        "location": {
            "parent": {
                "uri": dag_run.conf['parent_uri']
            }
        },
        "modifications": {
            "name": dag_run.conf['location_name'],
            "codeToApply": {
                "value": dag_run.conf['location_code']
            },
            "isEnabled": "1"
        },
        "unitOfWorkId": str(uuid4())
    }


def validate_supervisor_end_date(dag_run, effective_date=None):
    return custom_methods.compare_dates((dag_run.conf['effectivedate'] if not (effective_date) else effective_date), '>', (rail.result(
        'search_supervisor_in_replicon')['end_date'])) if rail.result('search_supervisor_in_replicon')['end_date'] else False


def validate_supervisor_changed():
    if not rail.result('get_effective_supervisor_of_user'):
        return True
    if rail.result('search_supervisor_in_replicon') and rail.result('get_effective_supervisor_of_user') and \
            rail.result('search_supervisor_in_replicon')['uri'] == rail.result('get_effective_supervisor_of_user')['supervisor']['user']['uri']:
        return False
    return True


def get_process_disable_users_conf(dag_run):
    return {
        **dag_run.conf,
        "action": "disable",
        "user_uri": rail.result('get_user_data')['userDetails']['uri'],
        "user_log": rail.result('create_user_log')
    }


def get_process_each_user_payload(item, config):
    # Parse time off types and get URIs
    eligible_timeoffs = custom_methods.parse_time_off_types_from_csv(
        item.get('Time_Off_Types', ''), rail.result('get_all_time_off_types'))

    permissions_from_feed, permission_validation_errors = custom_methods.parse_permissions_from_csv(
        item.get('Permissions', ''), rail.result('get_all_permission_sets'))

    # Capture URIs for all direct field names
    timesheet_template_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_policy_sets'), 'name',
        item.get('Timesheet_Template'), 'uri', '') if item.get('Timesheet_Template') else ''

    punch_entry_policy_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_policy_sets'), 'name',
        item.get('Punch_Entry_Policy'), 'uri', '') if item.get('Punch_Entry_Policy') else ''

    holiday_calendar_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_holiday_calendars'), 'name',
        item.get('Holiday_Calendar'), 'uri', '') if item.get('Holiday_Calendar') else ''

    pay_rule_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_pay_rules'), 'displayText',
        item.get('Pay_Rule'), 'uri', '') if item.get('Pay_Rule') else ''

    office_schedule_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_office_schedules'), 'displayText',
        item.get('Office_Schedule'), 'uri', '') if item.get('Office_Schedule') else ''

    timesheet_period_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_timesheet_periods'), 'name',
        item.get('Timesheet_Period'), 'uri', '') if item.get('Timesheet_Period') else ''

    timesheet_approval_path_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_timesheet_approval_paths'), 'displayText',
        item.get('Timesheet_Approval_Path'), 'uri', '') if item.get('Timesheet_Approval_Path') else ''

    time_off_template_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_policy_sets'), 'name',
        item.get('Time_Off_Template'), 'uri', '') if item.get('Time_Off_Template') else ''

    return {
        **item,
        'integration_run_date': rail.result('log_integration_run_date'),
        'process': 'add/update',
        "location_to_apply_uri": rail.find_first_by_attr_and_get_attr(
            rail.result('get_updated_location_grps'), 'full_path_code', str(
                item['Work_Location_Country'] + "|" + item['Work_Location_State'] + "|" + item['Work_Location_Code']), 'uri'),
        "department_to_apply_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_updated_department_grps'), 'full_path_code', str(
            "MRCY|" + item['Operating_Unit'] + "|" + item['Business_Unit'] + "|" + item['Chargeable_Flag'] + "|" +
            item['Job_Union_ID']), 'uri'),
        "employeetype_group_to_apply_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_employee_types_grp'), 'full_path_code', str(
            item['Pay_Type'] + '|' + item['Full_Part_Time'] + '|' + item['FLSA']), 'uri'),
        "timezone_to_apply_uri": rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_timezones'), 'displayText', item['Expected_Time_Zone'], 'uri'),
        "supervisor_permission_set_uri": rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_permission_sets'), 'permission_set_name', 'Supervisor', 'permission_set_uri'),
        "eligible_timeoffs_for_user": eligible_timeoffs,
        "starting_balance_set_to_script_uri": rail.result('get_timeoff_policy_starting_balance_set_to_script'),
        "prevent_balance_overdraw_script_uri": rail.result('get_timeoff_policy_prevent_balance_overdraw_script'),
        "supervisor_log": rail.result('supervisor_assignment_log'),
        # Add the captured URIs
        "timesheet_template_uri": timesheet_template_uri,
        "punch_entry_policy_uri": punch_entry_policy_uri,
        "holiday_calendar_uri": holiday_calendar_uri,
        "pay_rule_uri": pay_rule_uri,
        "office_schedule_uri": office_schedule_uri,
        "timesheet_period_uri": timesheet_period_uri,
        "timesheet_approval_path_uri": timesheet_approval_path_uri,
        "time_off_template_uri": time_off_template_uri,
        "permissions_from_feed": permissions_from_feed,
        "permission_validation_error": permission_validation_errors
    }


def get_process_new_users_conf(dag_run):
    return {
        **dag_run.conf,
        "action": "add",
        "user_log": rail.result('create_user_log'),
    }


def get_process_update_users_conf(dag_run):
    return {
        **dag_run.conf,
        "action": "update",
        "user_uri": rail.result('get_user_data')['userDetails']['uri'],
        "user_details_artifact": rail.result('get_user_details_artifact'),
        "user_log": rail.result('create_user_log'),
    }


def get_process_rehire_users_conf(dag_run):
    return {
        **dag_run.conf,
        "action": "rehire",
        "user_uri": rail.result('get_user_data')['userDetails']['uri'],
        "user_details_artifact": rail.result('get_user_details_artifact_rehire_user'),
        "user_log": rail.result('create_user_log'),
    }


def disable_and_update_end_date_payload(dag_run, config):
    return {
        "target": {
            "uri": dag_run.conf['user_uri']
        },
        "modifications": {
            "employmentDateRange": {
                "value": {
                    "startDate": rail.parse_date(
                        dag_run.conf["Hire_Date"], config.DATE_FORMAT),
                    "endDate": rail.parse_date(
                        dag_run.conf["Termination_Date"] if dag_run.conf['Termination_Date'] else dag_run.conf['Effective_Date'], config.DATE_FORMAT),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": {
                        "value": "false"
                    }
                }
            },
            "extensionFields": [
                {
                    "value": {
                        "definition": {
                            "uri": null,
                            "name": "Emp Status"
                        },
                        "tag": {
                            "uri": null,
                            "slug": null,
                            "tagName": {
                                "name": dag_run.conf["Emp_Status"],
                                "tagDefinitionUri": null
                            }
                        },
                        "numericValue": null,
                        "textValue": null,
                        "fileValue": null,
                        "jsonValue": null
                    }
                }
            ]
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }


def get_user_oefs_for_add_update(dag_run):
    extension_fields_payload = []
    dropdown_custom_fields = {
        "Chargeable Flag": "Chargeable_Flag",
        "Flexible Vacation Eligible": "Flexible_Vacation_Eligible",
        "FLSA": "FLSA",
        "Full Part Time": "Full_Part_Time",
        "Labor Classification": "Job_Union_ID",
        "Pay Group": "Pay_Group",
        "Pay Type": "Pay_Type",
        "Work Location Country": "Work_Location_Country",
        "Emp Status": "Emp_Status"

    }
    text_custom_fields = {
        "ADP Department": "Department",
        "Business Unit": "Business_Unit",
        "Employee Classification": "Employee_Classification",
        "File ID": "File_ID",
        "Job Code": "Job_Code",
        "Job Family": "Job_Family",
        "Job Function": "Job_Function",
        "Job Title": "Job_Title",
        "Location Class Desc": "Location_Class_Description",
        "Operating Unit": "Operating_Unit",
        "VMS ID": "VMS_ID",
        "Work Location Code": "Work_Location_Code",
        "Work Location Name ": "Work_Location_Name",
        "Work Location State": "Work_Location_State",
        "Work Schedule Start Time": "Work_Schedule_Start_Time",
        "Hourly Cost": "Hourly_Cost"
    }

    for field, val in text_custom_fields.items():
        if dag_run.conf[val]:
            extension_fields_payload.append({
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
            extension_fields_payload.append({
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

    return extension_fields_payload


def get_add_user_timeoff_types_with_default_policies(dag_run):
    final_timeoff_policies_payload = [{
        "timeOffType": {
            "uri": null,
            "name": timeoff['name']
        },
        "isTimeOffAllowedAgainstThisTimeOffType": "true",
        "applyDefaultTimeOffTypePolicy": "true",
        "defaultTimeOffTypePolicyEffectiveDate": null,
        "policySchedule": []
    } for timeoff in dag_run.conf['eligible_timeoffs_for_user']]

    return final_timeoff_policies_payload


def get_schedule_type_schedule_payload_for_new_user(dag_run):
    # Check if this is a shift schedule
    if dag_run.conf.get("Work_Schedule") == "Shift":
        return [{
            "dateRange": null,
            "item": {
                "scheduleTypeUri": "urn:replicon:schedule-type:shift",
                "officeSchedule": null
            }
        }]

    # Otherwise, use office schedule if available
    if dag_run.conf.get("office_schedule_uri"):
        return [{
            "dateRange": null,
            "item": {
                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                "officeSchedule": {
                    "officeScheduleUri": dag_run.conf.get("office_schedule_uri"),
                    "name": null
                }
            }
        }]

    # If no office schedule URI is available, return empty
    return []


def get_hourly_cost_for_user(dag_run, config):
    # Hourly Cost is the governing field for getting hourly cost of user
    if dag_run.conf.get("Hourly_Cost"):
        return float(dag_run.conf["Hourly_Cost"])
    return None


def get_location_schedule_for_new_user(dag_run):
    return [
        {
            "dateRange": null,
            "item": {
                "uri": dag_run.conf["location_to_apply_uri"],
                "parentUri": null,
                "name": null
            }
        }
    ]


def get_department_group_schedule_for_new_user(dag_run):
    return [
        {
            "dateRange": null,
            "item": {
                "uri": dag_run.conf["department_to_apply_uri"],
                "parent": null,
                "name": null,
                "parameterCorrelationId": null
            }
        }
    ]


def get_employee_type_group_schedule_for_new_user(dag_run):
    return [
        {
            "dateRange": null,
            "item": {
                "uri": dag_run.conf["employeetype_group_to_apply_uri"]
            }
        }
    ]

# for Update Users, the new permissions being added if they are for same role as an existing one, new one will overwrite the existing


def get_permission_set_assignment_payload_for_new_update_user(dag_run, update_logs=[]):

    permission_sets_to_assign_list = []
    supervision_role_permission_assigned = False
    permission_logs = []

    for item in dag_run.conf.get('permissions_from_feed'):
        if item['permission_policy_uri'] == 'urn:replicon:policy:supervision':
            supervision_role_permission_assigned = True
        permission_sets_to_assign_list.append({
            "permissionSetPolicy": {
                "uri": item['permission_set_uri'],
                "name": null
            },
            "groupAccessFilter": null
        })
        permission_logs.append(item['permission_set_name'])

    if not (supervision_role_permission_assigned):
        if dag_run.conf["Manager_Type"] == "Yes":
            permission_sets_to_assign_list.append({
                "permissionSetPolicy": {
                    "uri": dag_run.conf["supervisor_permission_set_uri"],
                    "name": null
                },
                "groupAccessFilter": null
            })
            permission_logs.append('Supervisor')

    if permission_logs:
        update_logs.append(
            f"User Permissions Updated - {','.join(permission_logs)}")

    return [{
        "modificationOptionUri": "urn:replicon:collection-modification-option:add",
        "items": permission_sets_to_assign_list
    }] if permission_sets_to_assign_list else []


def get_create_user_payload(dag_run, config):
    supervisor_to_assign = rail.result(
        'get_final_result_from_supervisor_assignment_workflow')['supervisor_to_assign_uri']
    add_user_payload = {
        "target": null,
        "template": null,
        "modifications": {
            "firstName": {
                "value": dag_run.conf["First_Name"]
            },
            "lastName": {
                "value": dag_run.conf["Last_Name"]
            },
            "loginName": {
                "value": dag_run.conf["Login_Name"]
            },
            "displayName": {
                "value": dag_run.conf["Preferred_Name"]
            } if dag_run.conf["Preferred_Name"] else null,
            "emailAddress": {
                "value": dag_run.conf["Email"]
            },
            "employeeId": {
                "value": dag_run.conf["Employee_ID"]
            },
            "employmentDateRange": {
                "value": {
                    "startDate": rail.parse_date(dag_run.conf["Hire_Date"], config.DATE_FORMAT),
                    "endDate": rail.parse_date(dag_run.conf["Termination_Date"], config.DATE_FORMAT) if dag_run.conf["Termination_Date"] else null,
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
                        "value": dag_run.conf["Authentication_ID"]
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
                    "uri": dag_run.conf.get('timesheet_approval_path_uri'),
                    "name": null
                }
            } if dag_run.conf.get('timesheet_approval_path_uri') else null,
            "timeEntryApprovalPath": null,
            "workAuthorizationApprovalPath": null,
            "timeoffApprovalPath": null,
            "timeOffBalancePayoutApprovalPath": null,
            "defaultActivity": null,
            "expenseApprovalPath": null,
            "timeZone":  {
                "value": {
                    "uri": dag_run.conf["timezone_to_apply_uri"],
                    "IANAName": null
                }
            } if dag_run.conf["timezone_to_apply_uri"] else null,
            "workWeekStartDay": {
                "value": {
                    "uri": "urn:replicon:day-of-week:" + dag_run.conf["Work_Week"].split(" ")[0].lower(),
                }
            },
            "defaultBillingRate": null,
            "userPreferences": null,
            "formattings": null,
            "notificationPreferences": null,
            "timesheetTemplate": {
                "value": {
                    "uri": dag_run.conf.get('timesheet_template_uri'),
                    "name": null
                }
            } if dag_run.conf.get('timesheet_template_uri') else null,
            "timeoffTemplate": {
                "value": {
                    "uri": dag_run.conf.get('time_off_template_uri'),
                    "name": null
                }
            } if dag_run.conf.get('time_off_template_uri') else null,
            "timeOffCalendarVisibility": null,
            "expenseTemplate": null,
            "workAuthorizationTemplate": null,
            "punchEntryPolicy": {
                "value": {
                    "uri": dag_run.conf.get('punch_entry_policy_uri'),
                    "name": null
                }
            } if dag_run.conf.get('punch_entry_policy_uri') else null,
            "holidayCalendar": null,
            "extensionFields": get_user_oefs_for_add_update(dag_run),
            "customFields": [],
            "products": [],
            "skills": [],
            "activities": [],
            "policySets": [],
            "permissionSets": get_permission_set_assignment_payload_for_new_update_user(dag_run),
            "bankedTimePolicies": [],
            "timeOffTypes": [
                {
                    "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                    "items": get_add_user_timeoff_types_with_default_policies(dag_run)
                }
            ] if dag_run.conf.get("eligible_timeoffs_for_user") else [],
            "locationSchedule": get_location_schedule_for_new_user(dag_run),
            "divisionSchedule": [],
            "costCenterSchedule": [],
            "serviceCenterSchedule": [],
            "departmentGroupSchedule": get_department_group_schedule_for_new_user(dag_run),
            "employeeTypeGroupSchedule": get_employee_type_group_schedule_for_new_user(dag_run),
            "supervisorSchedule": [{
                "dateRange": null,
                "item": {
                    "uri": supervisor_to_assign,
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                }
            }]if supervisor_to_assign else [],
            "timesheetPeriodSchedule": [
                {
                    "dateRange": null,
                    "item": {
                        "uri": dag_run.conf.get('timesheet_period_uri'),
                        "name": null
                    }
                }
            ] if dag_run.conf.get('timesheet_period_uri') else [],
            "holidayCalendarSchedule": [{
                "dateRange": null,
                "item": {
                    "uri": dag_run.conf.get('holiday_calendar_uri'),
                    "name": null
                }
            }] if dag_run.conf.get('holiday_calendar_uri') else null,
            "scheduleTypeSchedule": get_schedule_type_schedule_payload_for_new_user(dag_run),
            "payRuleSchedule": [
                {
                    "dateRange": null,
                    "item": {
                        "uri": dag_run.conf.get('pay_rule_uri'),
                        "name": null
                    }
                }
            ] if dag_run.conf.get('pay_rule_uri') else [],
            "placeSchedule": [],
            "payRateSchedule": [],
            "projectRoleSchedule": [],
            "costNormalizationRuleSchedule": [],
            "hourlyRatesSchedule": [{
                "dateRange": null,
                "item": {
                    "hourlyRate": {
                        "amount": get_hourly_cost_for_user(dag_run, config),
                        "currency": {
                            # Set to USD$ as per requirement
                            "uri": null,
                            "name": "US Dollar",
                            "symbol": null
                        }
                    }
                }
            }] if dag_run.conf.get("Hourly_Cost") else [],
            "substituteUserSchedule": [],
            "policySetsScheduleToApply": []
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

    return add_user_payload


def get_update_user_timeoff_types_with_default_policies(dag_run, config):
    final_timeoff_policies_payload = [{
        "timeOffType": {
            "uri": null,
            "name": timeoff['name']
        },
        "isTimeOffAllowedAgainstThisTimeOffType": "true",
        "applyDefaultTimeOffTypePolicy": "true",
        "defaultTimeOffTypePolicyEffectiveDate": rail.parse_date(dag_run.conf["Effective_Date"], config.DATE_FORMAT),
        "policySchedule": []
    } for timeoff in rail.result('get_new_and_existing_timeoff_types_for_user')['new_eligible_timeoffs']]

    return final_timeoff_policies_payload


def get_basic_user_details_update(dag_run, config):
    updated_first_name = null
    updated_last_name = null
    updated_disaply_name = null
    updated_email = null
    updated_login_name = null
    updated_auth_id = null
    start_date_updated = False
    updated_start_date = rail.result("get_user_details")[
        "userDetails"]["employmentDateRange"]["startDate"]
    end_date_updated = False
    updated_end_date = rail.result("get_user_details")[
        "userDetails"]["employmentDateRange"]["endDate"]
    update_logs = []

    if dag_run.conf["First_Name"] != rail.result("get_user_details")["userDetails"]["firstName"]:
        updated_first_name = dag_run.conf["First_Name"]
        update_logs.append("First name updated")
    if dag_run.conf["Last_Name"] != rail.result("get_user_details")["userDetails"]["lastName"]:
        updated_last_name = dag_run.conf["Last_Name"]
        update_logs.append("Last name updated")
    if dag_run.conf["Preferred_Name"] != rail.result("get_user_details")["userDetails"]["customDisplayName"]:
        updated_disaply_name = dag_run.conf["Preferred_Name"]
        update_logs.append("Display name updated")
    if dag_run.conf["Email"] != rail.result("get_user_details")["userDetails"]["emailAddress"]:
        updated_email = dag_run.conf["Email"]
        update_logs.append("Email updated")
    if dag_run.conf["Login_Name"] != rail.result("get_user_details")["securityConfiguration"]["user"]["loginName"]:
        updated_login_name = dag_run.conf["Login_Name"]
        update_logs.append("Login name updated")
    if dag_run.conf["Authentication_ID"] != rail.result("get_user_details")["securityConfiguration"]["user"]["loginName"]:
        updated_auth_id = dag_run.conf["Authentication_ID"]
        update_logs.append("Authentication ID updated")

    if not (rail.result("get_user_details")[
        "userDetails"]["employmentDateRange"]["startDate"]) or not (custom_methods.compare_dates(
            dag_run.conf["Hire_Date"], "=", rail.result("get_user_details")["userDetails"]["employmentDateRange"]["startDate"])):
        update_logs.append("Start Date updated")
        start_date_updated = True
        updated_start_date = rail.parse_date(
            dag_run.conf["Hire_Date"], config.DATE_FORMAT)

    if not (dag_run.conf["Termination_Date"]) and updated_end_date and dag_run.conf['action'] == "rehire":
        update_logs.append("Termination Date removed")
        end_date_updated = True
        updated_end_date = null

    if bool(dag_run.conf["Termination_Date"]) and (not (rail.result("get_user_details")[
        "userDetails"]["employmentDateRange"]["endDate"]) or not (custom_methods.compare_dates(
            dag_run.conf["Termination_Date"], "=", rail.result("get_user_details")["userDetails"]["employmentDateRange"]["endDate"]))):
        update_logs.append("Termination Date updated")
        end_date_updated = True
        updated_end_date = rail.parse_date(
            dag_run.conf["Termination_Date"], config.DATE_FORMAT)

    basic_details = {
        "firstName": {
            "value": updated_first_name
        } if updated_first_name else null,
        "lastName": {
            "value": updated_last_name
        } if updated_last_name else null,
        "displayName": {
            "value": updated_disaply_name
        } if updated_disaply_name else null,
        "emailAddress": {
            "value": updated_email
        } if updated_email else null,
        "loginName": {
            "value": updated_login_name
        } if updated_login_name else null,
        "securitySettings": {
            "value": {
                "loginEnabled": {
                    "value": "true"
                } if dag_run.conf['action'] == "rehire" else null,
                "ssoName": {
                    "value": updated_auth_id
                } if updated_auth_id else null,
            }
        } if (updated_auth_id or dag_run.conf['action'] == "rehire") else null,
        "employmentDateRange": {
            "value": {
                "startDate": updated_start_date,
                "endDate": updated_end_date,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            }
        } if (bool(start_date_updated) or bool(end_date_updated)) else null,
    }

    return basic_details, update_logs


def get_employeetype_update_schedule(dag_run, config, update_logs):
    if dag_run.conf["employeetype_group_to_apply_uri"] and (not (rail.result('get_effective_user_groupmembership')['current_employeetype_uri']) or (
            rail.result('get_effective_user_groupmembership')['current_employeetype_uri'] != dag_run.conf["employeetype_group_to_apply_uri"])):
        update_logs.append("Employee Type Updated")
        return [
            {
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["Effective_Date"], config.DATE_FORMAT),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "item": {
                    "uri": dag_run.conf["employeetype_group_to_apply_uri"],
                    "parentUri": null,
                    "name": null
                }
            }
        ]

    return []


def get_department_update_schedule(dag_run, config, update_logs):
    if dag_run.conf["department_to_apply_uri"] and (not (rail.result('get_effective_user_groupmembership')['current_departmentgroup_uri']) or (
            rail.result('get_effective_user_groupmembership')['current_departmentgroup_uri'] != dag_run.conf["department_to_apply_uri"])):
        update_logs.append("Department Updated")
        return [
            {
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["Effective_Date"], config.DATE_FORMAT),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "item": {
                    "uri": dag_run.conf["department_to_apply_uri"],
                    "parentUri": null,
                    "name": null
                }
            }
        ]

    return []


def get_location_update_schedule(dag_run, config, update_logs):
    if dag_run.conf["location_to_apply_uri"] and (not (rail.result('get_effective_user_groupmembership')['current_location_uri']) or (
            rail.result('get_effective_user_groupmembership')['current_location_uri'] != dag_run.conf["location_to_apply_uri"])):
        update_logs.append("Location Updated")
        return [
            {
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["Effective_Date"], config.DATE_FORMAT),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "item": {
                    "uri": dag_run.conf["location_to_apply_uri"],
                    "parentUri": null,
                    "name": null
                }
            }
        ]

    return []


def get_timezone_uri_update(dag_run, update_logs):
    existing_timezone_uri = rail.result("get_user_details")[
        "timeZone"].get("uri", "") if rail.result("get_user_details")["timeZone"] else ''
    if dag_run.conf["timezone_to_apply_uri"] and (not (existing_timezone_uri) or dag_run.conf["timezone_to_apply_uri"] != existing_timezone_uri):
        update_logs.append("Time Zone Updated")
        return {
            "value": {
                "uri": dag_run.conf["timezone_to_apply_uri"],
                "IANAName": null
            }
        }
    return null


def get_workweek_uri_for_update(dag_run, update_logs):
    existing_workweek_uri = rail.result("get_user_details")['userDetails'][
        "workWeekStartDay"].get("uri", "") if rail.result("get_user_details")['userDetails']["workWeekStartDay"] else ''
    workweek_to_apply = "urn:replicon:day-of-week:" + \
        dag_run.conf["Work_Week"].split(" ")[0].lower()
    if workweek_to_apply != existing_workweek_uri:
        update_logs.append("Work week Updated")
        return {
            "value": {
                "uri": workweek_to_apply,
            }
        }
    return null


def get_timeoff_template_update_for_user(dag_run, update_logs):
    existing_timeoff_template_uri = rail.result("get_user_details")['timeOffTemplate'].get("uri", "") if rail.result(
        "get_user_details")['timeOffTemplate'] else ''
    if dag_run.conf.get('time_off_template_uri') and dag_run.conf.get('time_off_template_uri') != existing_timeoff_template_uri:
        update_logs.append("Time Off Template Updated")
        return {
            "value": {
                "uri": dag_run.conf.get('time_off_template_uri'),
                "name": null
            }
        }
    return null


def get_updated_policysetschedule_timesheet_template_for_user(dag_run, config, update_logs):
    existing_timesheet_template = rail.result("get_user_details")[
        "timesheetTemplate"].get("displayText", "") if rail.result("get_user_details")["timesheetTemplate"] else ''
    timesheet_template_to_apply = dag_run.conf.get('Timesheet_Template')
    if timesheet_template_to_apply and dag_run.conf.get('timesheet_template_uri') and (not (
            existing_timesheet_template) or timesheet_template_to_apply != existing_timesheet_template):
        update_logs.append("Timesheet Template Updated")
        return [
            {
                "policyUri": "urn:replicon:policy:timesheet",
                "schedule": [
                    {
                        "policySetUri": dag_run.conf.get('timesheet_template_uri'),
                        "effectiveDate": rail.parse_date(dag_run.conf["Effective_Date"], config.DATE_FORMAT)
                    }
                ]
            }
        ]
    return []


def get_current_value_from_schedule_list_for_user(user_schedule, scrpit_name, required_key,  dag_run, config):
    current_value = null
    initial_value = null
    current_min_day_diff = "*"
    if 'urn' in json.dumps(user_schedule):
        for item in user_schedule:

            if not (item['startDate'] if scrpit_name == 'holidayCalendar' else item['effectiveDate']):
                initial_value = item
                continue

            daydiff = (datetime.strptime(dag_run.conf['Effective_Date'], config.DATE_FORMAT).date()) - custom_methods.to_datetime(
                (item['startDate'] if scrpit_name == 'holidayCalendar' else item['effectiveDate']), config.DATE_FORMAT).date()

            # ignore the future ones
            if daydiff.days < 0:
                continue

            if current_min_day_diff == "*":
                current_value = item
                current_min_day_diff = daydiff
                continue

            if current_min_day_diff > daydiff:
                current_min_day_diff = daydiff
                current_value = item

    return current_value[scrpit_name][required_key] if current_value else (initial_value[scrpit_name][required_key] if initial_value else '')


def get_updated_holiday_calendar_for_user(dag_run, config, update_logs):
    existing_holiday_calendar_uri = get_current_value_from_schedule_list_for_user(
        rail.result("get_user_details")['holidayCalendarAssignmentSchedule'], 'holidayCalendar', 'uri', dag_run, config)
    new_holiday_cal_uri = dag_run.conf.get('holiday_calendar_uri')
    if new_holiday_cal_uri and (not (existing_holiday_calendar_uri) or new_holiday_cal_uri != existing_holiday_calendar_uri):
        update_logs.append("Holiday Calendar Updated")
        return [
            {
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["Effective_Date"], config.DATE_FORMAT)
                },
                "item": {
                    "uri": dag_run.conf.get('holiday_calendar_uri'),
                    "name": null
                }
            }
        ]
    return null


def get_scheduletype_officeschedule_payload_for_update_user(dag_run, config, update_logs):
    current_office_schedule_uri_for_user = get_current_value_from_schedule_list_for_user(
        rail.result("get_user_details")['schedulePolicies'], 'officeSchedule', 'uri', dag_run, config)
    if dag_run.conf.get("office_schedule_uri") and (
            not (current_office_schedule_uri_for_user) or dag_run.conf.get("office_schedule_uri") != current_office_schedule_uri_for_user):
        update_logs.append("Office Schedule Updated")
        return [{
            "dateRange": {
                "startDate": rail.parse_date(dag_run.conf["Effective_Date"], config.DATE_FORMAT)
            },
            "item": {
                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
                "officeSchedule": {
                    "officeScheduleUri": dag_run.conf.get("office_schedule_uri"),
                    "name": null
                }
            }
        }]
    return []


def get_hourlycost_for_update_user(dag_run, config, update_logs):
    current_hourly_cost_for_user = get_current_value_from_schedule_list_for_user(
        rail.result("get_user_details")['costRateSchedule'], 'hourlyRate', 'amount', dag_run, config)
    new_hourly_cost_for_user = get_hourly_cost_for_user(dag_run, config)
    if new_hourly_cost_for_user and (not (current_hourly_cost_for_user) or (float(current_hourly_cost_for_user) != float(new_hourly_cost_for_user))):
        update_logs.append("Hourly Cost Updated")
        return [{
            "dateRange": {
                "startDate": rail.parse_date(dag_run.conf["Effective_Date"], config.DATE_FORMAT)
            },
            "item": {
                "hourlyRate": {
                    "amount": new_hourly_cost_for_user,
                    "currency": {
                        # currently set to USD$
                        "uri": null,
                        "name": "US Dollar",
                        "symbol": null
                    }
                }
            }
        }]
    return []


def get_payrule_schedule_for_update_user(dag_run, config, update_logs, applyusermodifications3_payload):
    current_payrule_for_user = get_current_value_from_schedule_list_for_user(
        rail.result("get_user_details")['payRuleScriptSchedule'], 'payRuleScript', 'displayText', dag_run, config)
    new_payrule_for_user = dag_run.conf.get('Pay_Rule')
    if new_payrule_for_user and dag_run.conf.get('pay_rule_uri'):
        if not (current_payrule_for_user):
            update_logs.append("Pay Rule Updated")
            applyusermodifications3_payload.update(
                {
                    "payRulesToApply": {
                        "initialPayRule": null,
                        "scheduleEntries": [{
                            "payRuleScript": {
                                "uri": dag_run.conf.get('pay_rule_uri'),
                                "name": null
                            },
                            "effectiveDate": rail.parse_date(dag_run.conf["Effective_Date"], config.DATE_FORMAT),
                        }]
                    }
                }
            )
            return []
        if current_payrule_for_user != new_payrule_for_user:
            update_logs.append("Pay Rule Updated")
            return [{
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["Effective_Date"], config.DATE_FORMAT),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "item": {
                    "uri": dag_run.conf.get('pay_rule_uri'),
                    "name": null
                }
            }]
    return []


def get_updated_timesheet_approval_path_for_user(dag_run, update_logs):
    existing_timesheet_approval_path_uri = rail.result("get_user_details")[
        "timesheetApprovalPath"].get("uri", "") if rail.result("get_user_details")["timesheetApprovalPath"] else ''
    new_timesheet_approval_path_uri_to_apply = dag_run.conf.get(
        'timesheet_approval_path_uri')
    if new_timesheet_approval_path_uri_to_apply and new_timesheet_approval_path_uri_to_apply != existing_timesheet_approval_path_uri:
        update_logs.append("Timesheet Approval Path Updated")
        return {
            "value": {
                "uri": dag_run.conf.get('timesheet_approval_path_uri'),
                "name": null
            }
        }
    return null


def get_timesheet_period_schedule_for_update_user(dag_run, config, update_logs):
    current_timesheet_period_uri_for_user = get_current_value_from_schedule_list_for_user(
        rail.result("get_user_details")['timesheetPeriodSchedule'], 'timesheetPeriod', 'uri', dag_run, config)
    new_timesheet_period_uri_for_user = dag_run.conf["timesheet_period_uri"]
    if new_timesheet_period_uri_for_user and (not (current_timesheet_period_uri_for_user) or (
            new_timesheet_period_uri_for_user != current_timesheet_period_uri_for_user)):

        update_logs.append("Timesheet Period Updated")
        return [{
            "dateRange": {
                "startDate": rail.parse_date(dag_run.conf["Effective_Date"], config.DATE_FORMAT),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "item": {
                "uri": new_timesheet_period_uri_for_user,
                "name": null
            }
        }]

    return []


def get_supervisor_schedule_for_update_user(dag_run, config, update_logs, applyusermodifications3_payload):
    supervisor_to_assign = rail.result(
        'get_final_result_from_supervisor_assignment_workflow')['supervisor_to_assign_uri']
    current_supervisor_schedule = rail.result("get_user_details")[
        'supervisorAssignmentSchedule']
    if supervisor_to_assign:
        update_logs.append("Supervisor Updated")
        if not (current_supervisor_schedule):
            applyusermodifications3_payload.update(
                {
                    "supervisorsToApply": {
                        "initialSupervisor": null,
                        "supervisorScheduleEntries": [{
                            "supervisor": {
                                "uri": supervisor_to_assign,
                            },
                            "effectiveDate": rail.parse_date(dag_run.conf["Effective_Date"], config.DATE_FORMAT),
                        }]
                    }
                }
            )
            return []

        return [{
            "dateRange": {
                "startDate": rail.parse_date(dag_run.conf['Effective_Date'], config.DATE_FORMAT),
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "item": {
                "uri": rail.result('search_supervisor_in_replicon')['uri'],
                "loginName": null,
                "employeeId": null,
                "parameterCorrelationId": null
            }
        }]

    return []


def get_update_rehire_user_payload(dag_run, config):
    applyusermodifications3_payload = {}
    basic_details, update_logs = get_basic_user_details_update(
        dag_run, config)
    payrule_schedule_for_update_user_createuserorapplymodifications = get_payrule_schedule_for_update_user(
        dag_run, config, update_logs, applyusermodifications3_payload)
    supervisor_schedule_for_update_user_createuserorapplymodifications = get_supervisor_schedule_for_update_user(
        dag_run, config, update_logs, applyusermodifications3_payload)
    return {
        'payload': {
            "target": {
                "uri": dag_run.conf["user_uri"]
            },
            "template": null,
            "modifications": {
                **basic_details,
                "timesheetApprovalPath": get_updated_timesheet_approval_path_for_user(dag_run, update_logs),
                "timeEntryApprovalPath": null,
                "workAuthorizationApprovalPath": null,
                "timeoffApprovalPath": null,
                "timeOffBalancePayoutApprovalPath": null,
                "defaultActivity": null,
                "expenseApprovalPath": null,
                "timeZone":  get_timezone_uri_update(dag_run, update_logs),
                "workWeekStartDay": get_workweek_uri_for_update(dag_run, update_logs),
                "defaultBillingRate": null,
                "userPreferences": null,
                "formattings": null,
                "notificationPreferences": null,
                "timesheetTemplate": null,
                "timeoffTemplate": get_timeoff_template_update_for_user(dag_run, update_logs),
                "timeOffCalendarVisibility": null,
                "expenseTemplate": null,
                "workAuthorizationTemplate": null,
                "punchEntryPolicy": {
                    "value": {
                        "uri": dag_run.conf.get('punch_entry_policy_uri'),
                        "name": null
                    }
                } if dag_run.conf.get('punch_entry_policy_uri') else null,
                "holidayCalendar": null,
                "extensionFields": get_user_oefs_for_add_update(dag_run),
                "customFields": [],
                "products": [],
                "skills": [],
                "activities": [],
                "policySets": [],
                "permissionSets": get_permission_set_assignment_payload_for_new_update_user(dag_run, update_logs),
                "bankedTimePolicies": [],
                "timeOffTypes": [
                    {
                        "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                        "items": get_update_user_timeoff_types_with_default_policies(dag_run, config)
                    }
                ] if rail.result('get_new_and_existing_timeoff_types_for_user')['new_eligible_timeoffs'] else [],
                "locationSchedule": get_location_update_schedule(dag_run, config, update_logs),
                "divisionSchedule": [],
                "costCenterSchedule": [],
                "serviceCenterSchedule": [],
                "departmentGroupSchedule": get_department_update_schedule(dag_run, config, update_logs),
                "employeeTypeGroupSchedule": get_employeetype_update_schedule(dag_run, config, update_logs),
                "supervisorSchedule": supervisor_schedule_for_update_user_createuserorapplymodifications,
                "timesheetPeriodSchedule": get_timesheet_period_schedule_for_update_user(dag_run, config, update_logs),
                "holidayCalendarSchedule": get_updated_holiday_calendar_for_user(dag_run, config, update_logs),
                "scheduleTypeSchedule": get_scheduletype_officeschedule_payload_for_update_user(dag_run, config, update_logs),
                "payRuleSchedule": payrule_schedule_for_update_user_createuserorapplymodifications,
                "placeSchedule": [],
                "payRateSchedule": [],
                "projectRoleSchedule": [],
                "costNormalizationRuleSchedule": [],
                "hourlyRatesSchedule": get_hourlycost_for_update_user(dag_run, config, update_logs),
                "substituteUserSchedule": [],
                "policySetsScheduleToApply": get_updated_policysetschedule_timesheet_template_for_user(dag_run, config, update_logs)
            },
            "userModificationOptionUri": "urn:replicon:user-modification-option:save",
            "unitOfWorkId": str(uuid4())
        },
        'field_update_logs': update_logs,
        "applyusermodifications3_payload_for_modifications": applyusermodifications3_payload
    }
