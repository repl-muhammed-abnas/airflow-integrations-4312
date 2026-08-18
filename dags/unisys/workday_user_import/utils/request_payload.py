"""
Request Payload Utility - Unisys Workday User Import

Constructs request payloads for Replicon API calls.
This module provides functions to build properly formatted request payloads for various
Replicon service endpoints, handling data transformation, date formatting, and complex
nested structures required by the Replicon API.

Key features:
    - Date format conversion (string to Replicon date dict)
    - User creation/modification payload generation
    - Organizational hierarchy payload construction
    - User group membership schedule generation
    - Timesheet and approval path assignment
    - Custom field value handling
    - Schedule and policy assignment
    - Supervisor assignment logic
    - Pagination request formatting

Constants:
    DATE_FORMAT: Standard date format "%m/%d/%Y"
    INSTANCE_FORMAT: Instance date format "%B %d, %Y"

Functions:
    get_replicon_date(date_str, dt_format): Convert date string to Replicon date dict
    get_today_date(): Get current date as Replicon date dict
    get_supervisor_data_payload(dag_run): Build supervisor search payload
    get_division_payload(): Build division list request payload
    get_location_payload(): Build location list request payload
    get_employeetype_group_payload(): Build employee type group request payload
    get_ts_period_payload(): Build timesheet period request payload
    get_user_data_payload(dag_run): Build user search payload
    get_employee_types_hierarchy_payload(dag_run): Build user type hierarchy payload
    get_locations_hierarchy_payload(dag_run): Build location hierarchy payload
    get_timesheet_approvalpath(log, dag_run, user_action): Generate approval path assignment
    get_timezone_uri(log, dag_run, user_action): Generate timezone assignment
    get_workweek_start_day(log, dag_run, user_action): Generate work week assignment
    get_timesheet_template_to_assign(dag_run, user_action, effective_date, log): Generate template assignment
    get_holiday_calendar_to_assign(dag_run, holiday_calendar_uri, user_action, effective_date): Generate holiday calendar
    get_udfs(user_action, dag_run): Generate custom field values
    get_timesheet_period_to_assign(dag_run, timesheetperioduri, user_action, effective_date): Generate period assignment
    get_user_groupmembership_to_assign(group_uri, user_action, effective_date, group): Generate group membership
    get_updated_holiday_calendar_for_user(dag_run, holiday_cal_uri, user_action, effective_date, log): Update holiday calendar
    get_schedule_type_to_assign(dag_run, schedule_uri, user_action, effective_date): Generate schedule assignment
    get_login_enabled(dag_run, exceptions_co_code, user_action): Determine login enabled status
    get_pay_rule_to_assign(dag_run, pay_rule, user_action, effective_date): Generate pay rule assignment
    get_create_update_user_payload(config, dag_run, user_action): Build complete user creation/update payload
    get_employee_conversion_payload(): Build employee conversion payload
"""
import pendulum
import json
from datetime import datetime
from uuid import uuid4
from functools import lru_cache
import rail

null = None
DATE_FORMAT = "%m/%d/%Y"
INSTANCE_FORMAT = "%B %d, %Y"

def get_replicon_date(date_str, dt_format=DATE_FORMAT):
    """
    Convert date string to Replicon date dictionary format.

    Args:
        date_str (str): Date string to convert
        dt_format (str, optional): Date format pattern. Defaults to "%m/%d/%Y"

    Returns:
        dict or None: Replicon date dictionary with keys 'year', 'month', 'day'
            Returns None if date_str is empty or parsing fails

    Example:
        >>> get_replicon_date("10/14/2025")
        {'year': 2025, 'month': 10, 'day': 14}

        >>> get_replicon_date("January 15, 2025", "%B %d, %Y")
        {'year': 2025, 'month': 1, 'day': 15}
    """
    if not date_str:
        return None

    try:
        date = datetime.strptime(date_str, dt_format)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None

def get_today_date():
    now = pendulum.now()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }

def get_supervisor_data_payload(dag_run):
    return {
        "users": [
            {
            "uri": null,
            "loginName": null,
            "employeeId": dag_run.conf['supervisor_id'],
            "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_division_payload():
    return {
        "page": 1,
        "pagesize": 200,
        "columnUris": [
            "urn:replicon:division-list-column:division",
            "urn:replicon:division-list-column:full-path",
            "urn:replicon:division-list-column:effectively-enabled",
            "urn:replicon:division-list-column:code"
        ],
        "sort": [],
        "filterExpression": null
    }

def get_location_payload():
    return {
        "page": 1,
        "pagesize": 200,
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:full-path",
            "urn:replicon:location-list-column:code"

        ],
        "sort": [],
        "filterExpression": null
    }

def get_employeetype_group_payload():
    return {
        "page": 1,
        "pagesize": 200,
        "columnUris": [
            "urn:replicon:employee-type-group-list-column:employee-type-group",
            "urn:replicon:employee-type-group-list-column:full-path"

        ],
        "sort": [],
        "filterExpression": null
    }

def get_ts_period_payload():
    return {
        "page": 1,
        "pagesize": 200,
        "columnUris": [
            "urn:replicon:timesheet-period-list-column:timesheet-period",
            "urn:replicon:timesheet-period-list-column:name"

        ],
        "sort": [],
        "filterExpression": null
    }

def get_user_data_payload(dag_run):
    return {
        "users": [
            {
            "uri": null,
            "loginName": null,
            "employeeId": dag_run.conf['employee_id'],
            "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }

def get_employee_types_hierarchy_payload(dag_run):

    names = dag_run.conf['full_path'].split('|')
    
    # Build hierarchy list
    hierarchy = []
    
    for i, name in enumerate(names):
        item = {}
        
        # Build target with nested parents (skip for first item)
        if i > 0:
            target = {"parent": {}}
            current = target["parent"]
            
            # Add nested parents from root to immediate parent
            for j in reversed(range(i)):
                current["name"] = names[j]
                if j > 0:  # Not the last parent
                    current["parent"] = {}
                    current = current["parent"]
            
            item["target"] = target
        
        item["modificationToApply"] = {
            "name": name,
            "isEnabled": True
        }
        hierarchy.append(item)
    
    # Return final structure
    return {
        "hierarchy": hierarchy,
        "modificationOptionUri": "urn:replicon:hierarchy-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

def get_locations_hierarchy_payload(dag_run):
    return get_employee_types_hierarchy_payload(dag_run)

@lru_cache(maxsize=8)
def _get_user_data(dag_run, user_action='add_user'):
    return rail.load_all_records(dag_run.conf['get_user_data'])[0] if user_action == 'add_user' else rail.result('get_user_data')[0]

def get_timesheet_approvalpath(log, dag_run, user_action):
    if not dag_run.conf['timesheetapprovalpath']:
        return null
    if dag_run.conf['timesheetapprovalpath'] and not dag_run.conf['timesheetapprovalpathuri']:
        log.append(f"Timesheet Approval Path - {dag_run.conf['timesheetapprovalpath']} is not available in Replicon")
        return null
    if user_action == "add_user":
        return {
                "value": {
                    "uri": dag_run.conf['timesheetapprovalpathuri'],
                    "name": null
                }
            }
    else:
        current_timesheet_approvalpath = _get_user_data(dag_run, user_action)['timesheetApprovalPath']
        if not current_timesheet_approvalpath or (current_timesheet_approvalpath and (
            dag_run.conf['timesheetapprovalpath'] != current_timesheet_approvalpath['displayText'])):
            return {
                "value": {
                    "uri": dag_run.conf['timesheetapprovalpathuri'],
                    "name": null
                }
            }
    return null

def get_timezone_uri(log, dag_run, user_action):
    if not dag_run.conf['timezone']:
        return null
    if dag_run.conf['timezone'] and not dag_run.conf['timezoneuri']:
        log.append(f"Timezone - {dag_run.conf['timezone']} is not available in Replicon")
        return null
    if user_action == "add_user":
        return {
            "value": {
                "uri": dag_run.conf['timezoneuri'],
                "IANAName": null
            }
        }
    else:
        current_timezone = _get_user_data(dag_run, user_action)['timeZone']
        if not current_timezone or (current_timezone and (
            dag_run.conf['timezone'] != current_timezone['displayText'])):
            return {
                "value": {
                    "uri": dag_run.conf['timezoneuri'],
                    "IANAName": null
                }
            }
    return null

def get_workweek_start_day(log, dag_run, user_action):
    if not dag_run.conf['work_week']:
        return null
    if dag_run.conf['work_week'] and not dag_run.conf['work_week_uri']:
        log.append(f"work_week - {dag_run.conf['work_week']} is not available in Replicon")
        return null
    if user_action == "add_user":
        return {
            "value": {
                "uri": dag_run.conf['work_week_uri']
            }
        }
    else:
        current_workweek = _get_user_data(dag_run, user_action)['userDetails']['workWeekStartDay']
        if not current_workweek or (current_workweek and (
            dag_run.conf['work_week_uri'] != current_workweek['uri'])):
            return {
                "value": {
                    "uri": dag_run.conf['work_week_uri']
                }
            }
    return null


def get_timesheet_template_to_assign(dag_run, user_action, effective_date, log):
    if not dag_run.conf['timesheettemplate']:
        return []
    if dag_run.conf['timesheettemplate'] and not dag_run.conf['timesheettemplateuri']:
        log.append(f"Timesheet Template - {dag_run.conf['timesheettemplate']} is not available in Replicon")
        return null
    if user_action == "add_user":
        return [
            {
                "policyUri": "urn:replicon:policy:timesheet",
                "schedule": [
                    {
                        "policySetUri": dag_run.conf['timesheettemplateuri'],
                        "effectiveDate": null
                    }
                ]
            }
        ]
    else:
        current_timesheet_template = _get_user_data(dag_run, user_action)['timesheetTemplate']
        if not current_timesheet_template or (current_timesheet_template and (
            dag_run.conf['timesheettemplate'] != current_timesheet_template['displayText'])):
            return [
            {
                "policyUri": "urn:replicon:policy:timesheet",
                "schedule": [
                    {
                        "policySetUri": dag_run.conf['timesheettemplateuri'],
                        "effectiveDate": get_replicon_date(effective_date)
                    }
                ]
            }
        ]
    return []

def get_holiday_calendar_to_assign(dag_run, holiday_calendar_uri, user_action, effective_date):
    if not holiday_calendar_uri:
        return []
    if user_action == "add_user":
        return {
            "value": {
                "uri": holiday_calendar_uri,
                "name": null
            }
        }
    else:
        current_holiday_calendar = _get_user_data(dag_run, user_action)['holidayCalendar']
        if not current_holiday_calendar or (current_holiday_calendar and (
            holiday_calendar_uri != current_holiday_calendar['uri'])):
            return {
                "value": {
                    "uri": holiday_calendar_uri,
                    "name": null
                }
            }
    return []

def get_udfs(log, user_action, dag_run):
    if dag_run.conf['leave_type'] and not dag_run.conf['leave_type_value_uri']:
        log.append(f"Leave Type {dag_run.conf['leave_type']} is not present in Replicon")
        return []
    udfs = []
    def add_udf_field_values(definitionuri, textvalue = null , dropdownuri = null, date = null):
        if definitionuri:
            if dropdownuri or date or textvalue:
                udfs.append({
                    "value": {
                        "customField": {
                            "uri": definitionuri,
                            "name": null
                        },
                        "text": textvalue,
                        "date": get_replicon_date(date) if date else null,
                        "dropDownOption": {
                            "uri": dropdownuri,
                            "name": null
                        } if dropdownuri else None,
                        "number": null
                    }
                })
    rplcn_udfs = dag_run.conf['replicon_user_udfs']
    if user_action =='add_user':
        if dag_run.conf['department_id']:
            add_udf_field_values(definitionuri = rplcn_udfs['department_id_uri'], textvalue = dag_run.conf['department_id'])
        if dag_run.conf['change_effective_date']:
            add_udf_field_values(definitionuri = rplcn_udfs['change_effective_date_uri'], date = dag_run.conf['change_effective_date'])
        if dag_run.conf['user_status']:
            add_udf_field_values(definitionuri = rplcn_udfs['user_status_uri'], dropdownuri = dag_run.conf['user_status_value_uri'])
        if dag_run.conf['job_code']:
            add_udf_field_values(definitionuri = rplcn_udfs['job_code_uri'], textvalue = dag_run.conf['job_code'])
        if dag_run.conf['pay_group']:
            add_udf_field_values(definitionuri = rplcn_udfs['pay_group_uri'], textvalue = dag_run.conf['pay_group'])
        if dag_run.conf['fusion_business_unit']:
            add_udf_field_values(definitionuri = rplcn_udfs['fusion_business_unit_uri'], textvalue = dag_run.conf['fusion_business_unit'])
        if dag_run.conf['union_employee']:
            add_udf_field_values(definitionuri = rplcn_udfs['union_employee_uri'], textvalue = dag_run.conf['union_employee'])
        if dag_run.conf['premium_pay_eligible']:
            add_udf_field_values(definitionuri = rplcn_udfs['premium_pay_eligible_uri'], textvalue = dag_run.conf['premium_pay_eligible'])
        if dag_run.conf['latest_leave_start_date']:
            add_udf_field_values(definitionuri = rplcn_udfs['leave_start_date_uri'], date = dag_run.conf['latest_leave_start_date'])
        if dag_run.conf['latest_leave_end_date']:
            add_udf_field_values(definitionuri = rplcn_udfs['leave_end_date_uri'], date = dag_run.conf['latest_leave_end_date'])
        if dag_run.conf['shift_eligible']:
            add_udf_field_values(definitionuri = rplcn_udfs['shift_eligible_uri'], textvalue = dag_run.conf['shift_eligible'])
        if dag_run.conf['supplier_number']:
            add_udf_field_values(definitionuri = rplcn_udfs['supplier_number_uri'], textvalue = dag_run.conf['supplier_number'])
        if dag_run.conf['leave_type']:
            add_udf_field_values(definitionuri = rplcn_udfs['leave_type_uri'], dropdownuri = dag_run.conf['leave_type_value_uri'])

    if user_action == 'update_user':
        custom_field_values = _get_user_data(dag_run, user_action)['userDetails']['customFieldValues']
        current_dept_id = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Department ID', 'text')
        current_chng_effective_date = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Change Effective Date', 'text')
        current_user_status = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'User Status', 'text')
        current_job_code = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Job Code', 'text')
        current_pay_grp = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Pay Group', 'text')
        current_fusion_business_unit = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Fusion Business Unit', 'text')
        current_union_empl = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Union Employee', 'text')
        current_premium_pay = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Premium Pay Eligible', 'text')
        current_leave_strt_dt = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Leave Start Date', 'text')
        current_leave_end_dt = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Leave End Date', 'text')
        current_shift_eligible = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Shift Eligible', 'text')
        current_supplier_number = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Supplier Number', 'text')
        current_leave_type = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Leave Type', 'text')

        if dag_run.conf['department_id'] and dag_run.conf['department_id'] != current_dept_id:
            add_udf_field_values(definitionuri = rplcn_udfs['department_id_uri'], textvalue = dag_run.conf['department_id'])
        if dag_run.conf['change_effective_date'] and dag_run.conf['change_effective_date'] != current_chng_effective_date:
            add_udf_field_values(definitionuri = rplcn_udfs['change_effective_date_uri'], date = dag_run.conf['change_effective_date'])
        if dag_run.conf['user_status'] and dag_run.conf['user_status'] != current_user_status:
            add_udf_field_values(definitionuri = rplcn_udfs['user_status_uri'], dropdownuri = dag_run.conf['user_status_value_uri'])
        if dag_run.conf['job_code'] and dag_run.conf['job_code'] != current_job_code:
            add_udf_field_values(definitionuri = rplcn_udfs['job_code_uri'], textvalue = dag_run.conf['job_code'])
        if dag_run.conf['pay_group'] and dag_run.conf['pay_group'] != current_pay_grp:
            add_udf_field_values(definitionuri = rplcn_udfs['pay_group_uri'], textvalue = dag_run.conf['pay_group'])
        if dag_run.conf['fusion_business_unit'] and dag_run.conf['fusion_business_unit'] != current_fusion_business_unit:
            add_udf_field_values(definitionuri = rplcn_udfs['fusion_business_unit_uri'], textvalue = dag_run.conf['fusion_business_unit'])
        if dag_run.conf['union_employee'] and dag_run.conf['union_employee'] != current_union_empl:
            add_udf_field_values(definitionuri = rplcn_udfs['union_employee_uri'], textvalue = dag_run.conf['union_employee'])
        if dag_run.conf['premium_pay_eligible'] and dag_run.conf['premium_pay_eligible'] != current_premium_pay:
            add_udf_field_values(definitionuri = rplcn_udfs['premium_pay_eligible_uri'], textvalue = dag_run.conf['premium_pay_eligible'])
        if dag_run.conf['latest_leave_start_date'] and dag_run.conf['latest_leave_start_date'] != current_leave_strt_dt:
            add_udf_field_values(definitionuri = rplcn_udfs['leave_start_date_uri'], date = dag_run.conf['latest_leave_start_date'])
        if dag_run.conf['latest_leave_end_date'] and dag_run.conf['latest_leave_end_date'] != current_leave_end_dt:
            add_udf_field_values(definitionuri = rplcn_udfs['leave_end_date_uri'], date = dag_run.conf['latest_leave_end_date'])
        if dag_run.conf['shift_eligible'] and dag_run.conf['shift_eligible'] != current_shift_eligible:
            add_udf_field_values(definitionuri = rplcn_udfs['shift_eligible_uri'], textvalue = dag_run.conf['shift_eligible'])
        if dag_run.conf['supplier_number'] and dag_run.conf['supplier_number'] != current_supplier_number:
            add_udf_field_values(definitionuri = rplcn_udfs['supplier_number_uri'], textvalue = dag_run.conf['supplier_number'])
        if dag_run.conf['leave_type'] and dag_run.conf['leave_type'] != current_leave_type:
            add_udf_field_values(definitionuri = rplcn_udfs['leave_type_uri'], dropdownuri = dag_run.conf['leave_type_value_uri'])
        
    return udfs

def get_timesheet_period_to_assign(dag_run, timesheetperioduri, user_action, log, effective_date):
    """
    Determine timesheet period assignment based on user status and action.
    
    Returns list of timesheet period assignments with date ranges.
    """
    if not dag_run.conf['timesheetperiod']:
        return []
    if dag_run.conf['timesheetperiod'] and not timesheetperioduri:
        log.append(f"Timesheet Period {dag_run.conf['timesheetperiod']} is not present in Replicon")
        return []
    
    user_status = dag_run.conf['user_status']
    
    # Handle Active - No Time and Unpaid Leave statuses
    if user_status in ['Active - No Time', 'Unpaid Leave']:
        date_source = effective_date if user_status == 'Active - No Time' else dag_run.conf['latest_leave_start_date']
        effective_date = get_replicon_date(date_source) or get_today_date()
        
        return [{
            "dateRange": {"startDate": effective_date},
            "item": None
        }]
    
    # Handle add_user action
    if user_action == "add_user":
        return [{
            "dateRange": None,
            "item": {"uri": timesheetperioduri, "name": None}
        }]
    
    # Handle other user actions
    user_data = _get_user_data(dag_run, user_action)
    current_timesheetperiod = user_data['timesheetPeriodSchedule']
    custom_field_values = user_data['userDetails']['customFieldValues']
    
    current_leave_end_dt = rail.find_first_by_attr_and_get_attr(
        custom_field_values, 'customField.displayText', 'Leave End Date', 'text'
    )
    current_user_status = rail.find_first_by_attr_and_get_attr(
        custom_field_values, 'customField.displayText', 'User Status', 'text'
    )
    
    # Handle Unpaid Leave to Active transition
    if (current_user_status == 'Unpaid Leave' and 
        user_status == 'Active' and 
        (current_leave_end_dt or dag_run.conf.get('latest_leave_end_date'))):
        
        leave_end_date = (
            datetime.strptime(current_leave_end_dt, INSTANCE_FORMAT).strftime(DATE_FORMAT) 
            if current_leave_end_dt 
            else dag_run.conf['latest_leave_end_date']
        )
        
        return [{
            "dateRange": {
                "startDate": get_replicon_date(leave_end_date),
                "endDate": None,
                "relativeDateRangeUri": None,
                "relativeDateRangeAsOfDate": None
            },
            "item": {"uri": timesheetperioduri, "name": None}
        }]
    
    if (current_user_status == 'Active - No Time' and user_status == 'Active'):
        
        return [{
            "dateRange": {
                "startDate": get_replicon_date(effective_date),
                "endDate": None,
                "relativeDateRangeUri": None,
                "relativeDateRangeAsOfDate": None
            },
            "item": {"uri": timesheetperioduri, "name": None}
        }]

    # Handle timesheet period changes
    current_period_uri = (
        current_timesheetperiod[-1]['timesheetPeriod']['uri'] 
        if current_timesheetperiod 
        else None
    )
    
    if not current_timesheetperiod or timesheetperioduri != current_period_uri:
        return [{
            "dateRange": {
                "startDate": get_replicon_date(effective_date),
                "endDate": None,
                "relativeDateRangeUri": None,
                "relativeDateRangeAsOfDate": None
            } if current_timesheetperiod else None,
            "item": {"uri": timesheetperioduri, "name": None}
        }]
    
    return []

def get_user_groupmembership_to_assign(group_val, group_uri, user_action, effective_date, group, log):
    if not group_uri:
        if group_val and group == 'location':
            log.append(f"Location - {group_val} is not available in Replicon")
        elif group_val and group == 'division':
            log.append(f"Co Code_Cost Center - {group_val} is not available in Replicon")
        elif group_val and group == 'department':
            log.append(f"Purchase Order ID - {group_val} is disabled or not available in Replicon")
        elif group_val and group == 'employeetype':
            log.append(f"User Type - {group_val} is not available in Replicon")
        return []
    if user_action == "add_user":
        return [
            {
                "dateRange": null,
                "item": {
                    "uri": group_uri,
                    "parentUri": null,
                    "name": null
                }
            }
        ]
    else:
        current_grp_value = rail.result('get_effective_user_groupmembership', group)
        if not current_grp_value or (current_grp_value and (
            group_uri != current_grp_value['uri'])):
            return [
                {
                    "dateRange": {
                        "startDate": get_replicon_date(effective_date),
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    } if current_grp_value else null,
                    "item": {
                    "uri": group_uri,
                    "parentUri": null,
                    "name": null
                    }
                }
            ]
    return []

def get_updated_holiday_calendar_for_user(dag_run, holiday_cal_uri, user_action, effective_date, log):
    
    if not holiday_cal_uri:
        if dag_run.conf['user_type'].split('|')[0] == "Employee":
            log.append("Holiday Calendar Not Found in Replicon")
        return []

    if user_action == "add_user":
        # log.append("Holiday Calendar Add")
        return [
            {
                "dateRange": null,
                "item": {
                    "uri": holiday_cal_uri,
                    "name": null
                }
            }
        ]
    else:
        current_holiday_calendar = _get_user_data(dag_run, user_action)['holidayCalendarAssignmentSchedule']
        
        if not current_holiday_calendar or (current_holiday_calendar and current_holiday_calendar[-1][
            'holidayCalendar']['uri'] != holiday_cal_uri):
            # log.append("Holiday Calendar Updated")
            return [
                {
                    "dateRange": {
                        "startDate": get_replicon_date(effective_date)
                    },
                    "item": {
                        "uri": holiday_cal_uri,
                        "name": null
                    }
                }
            ]
    return []

def get_effective_date(schedulePolicy, start_date):
    effective_date = str(schedulePolicy["effectiveDate"]['month']) + "/" + str(schedulePolicy["effectiveDate"]['day']) + "/" + str(
        schedulePolicy["effectiveDate"]['year']) if schedulePolicy["effectiveDate"] else str(start_date['month']) + "/" + str(start_date['day']) + "/" + str(start_date['year'])
    return effective_date

def get_day_diff(schedulePolicy, user_start_date):
    todays_date = pendulum.now()
    todays_date_s = todays_date.strftime(DATE_FORMAT)
    current_pst_date = datetime.strptime(todays_date_s, DATE_FORMAT)
    start_date = get_effective_date(schedulePolicy, user_start_date)
    from_start = datetime.strptime(start_date, DATE_FORMAT)
    return (current_pst_date - from_start).days

def get_schedule_policy(dag_run, user_action):
    user_data = _get_user_data(dag_run, user_action)
    schedule_polieces = []
    schedulepolicies = user_data['schedulePolicies']
    start_date = user_data['userDetails']['employmentDateRange']['startDate']
    for schedulePolicy in schedulepolicies:
        schedule_polieces.append({
            "effectivedate": get_effective_date(schedulePolicy, start_date),
            "displayText": schedulePolicy["officeSchedule"]["displayText"],
            "uri": schedulePolicy["officeSchedule"]["uri"],
            "scheduletypeuri": schedulePolicy["scheduleTypeUri"],
            "daydiff": get_day_diff(schedulePolicy, start_date)
        })
    if not schedule_polieces:
        return []
    return min(schedule_polieces, key=lambda x: x['daydiff'])

def get_schedule_type_to_assign(dag_run, schedule_uri, user_action, effective_date=null):
    if not schedule_uri:
        return []
    item = {
        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule",
        "officeSchedule": {
            "officeScheduleUri": schedule_uri,
            "name": null
        }
    }

    if user_action == "add_user":
        return [
            {
                "dateRange": null,
                "item": item
            }
        ]
    else:
        current_schedule_type = get_schedule_policy(dag_run, user_action)
        if not current_schedule_type or (current_schedule_type and (
            schedule_uri != current_schedule_type['uri'])):
            return [
                {
                    "dateRange": {
                        "startDate": get_replicon_date(effective_date),
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    } if current_schedule_type and effective_date else null,
                    "item": item
                }
            ]
    return []

def get_login_enabled(dag_run, exceptions_co_code, user_action):
    if dag_run.conf['companycode_costcenter'] in exceptions_co_code:
        return {
                "value": "false",
            }
    return {
            "value": "true" if dag_run.conf['user_status'] in ["Active", "Active - No Time", "Paid Leave", "Unpaid Leave"] else "false",
        }

def get_pay_rule_to_assign(dag_run, pay_rule, user_action, log, effective_date=null):
    if not pay_rule:
        return []
    if not dag_run.conf['payrule_uri']:
        log.append(f"PayRule {pay_rule} is not present in Replicon")
        return []
    if user_action == "add_user":
        return [
            {
                "dateRange": null,
                "item": {
                    "uri": null,
                    "name": pay_rule
                }
            }
        ]
    else:
        current_pay_rule = _get_user_data(dag_run, user_action)['payRuleScriptSchedule']
        if not current_pay_rule or (current_pay_rule and (
            pay_rule != current_pay_rule[-1]['payRuleScript']['displayText'])):
            return [
                {
                    "dateRange": {
                        "startDate": get_replicon_date(effective_date),
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    } if current_pay_rule and effective_date else null,
                    "item": {
                        "uri": null,
                        "name": pay_rule
                    }
                }
            ]
    return []

def get_permissionsets_to_assign(dag_run, permissionsets, user_action):
    resp = []
    if not permissionsets:
        return resp
    if user_action == "add_user":
        for permissionset in permissionsets:
            resp.append({
                    "permissionSetPolicy": {
                    "uri": permissionset['uri'],
                    "name": null
                    },
                    "groupAccessFilter": null
                }
            )
        if resp:
            resp = [{
                "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                "items": resp
            }]
    else:
        current_permissionsets = _get_user_data(dag_run, user_action)['permissionSets']
        current_permissionsets_names = {}
        for current_permissionset in current_permissionsets:
            current_permissionsets_names[current_permissionset['displayText']] = current_permissionset['uri']
        for permissionset in permissionsets:
            if permissionset['name'] not in current_permissionsets_names:
                resp.append({
                        "permissionSetPolicy": {
                        "uri": permissionset['uri'],
                        "name": null
                        },
                        "groupAccessFilter": null
                    }
                )
        if resp:
            resp = [{
                "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                "items": resp
            }]
    return resp

def get_create_update_user_payload(config, dag_run, user_action):
    """
    Build complete payload for creating or updating a user in Replicon.

    Constructs the comprehensive ImportService2.svc/CreateUserOrApplyModifications
    payload including all user attributes, schedules, group memberships, custom fields,
    and policy assignments. Handles both new user creation and existing user updates.

    Args:
        config: Configuration object containing:
            - EXCEPTIONS_CO_CODE: List of company codes with login exceptions
        dag_run: DAG run context containing all user data in conf
        user_action (str): Either 'add_user' or 'update_user'

    Returns:
        dict: Complete Replicon user modification payload including:
            - target: User identification (URI for updates, null for creates)
            - modifications: All user attributes and schedules
                - Basic info (name, email, employee ID, login)
                - Employment date range
                - Security settings (login enabled, SSO)
                - Group memberships (location, division, department, user type)
                - Timesheet settings (template, period, approval path)
                - Schedule assignments (office schedule, pay rule)
                - Holiday calendar
                - Custom field values
                - Activities and permissions
            - userModificationOptionUri: Save option
            - unitOfWorkId: Unique transaction ID

    Side Effects:
        Sets result key 'exception_logs' with list of warnings/exceptions

    Note:
        This is the primary payload builder for user operations. It performs
        extensive comparison between current and desired states for updates,
        only including modifications that represent actual changes.
    """
    log=[]

    def get_all_enabled_activities_for_assignment():
        """
        Build activity assignment list for user.

        Returns:
            list: Activity assignment structure for Replicon API
        """
        time_types= dag_run.conf['time_types']
        if not time_types:
            return []
        return [
            {
                "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                "items": [
                    {
                        "uri": null,
                        "name": activity,
                        "code": null
                    } for activity in time_types
                ]
            }
        ]


    def get_activities_to_assigned():
        replicon_activity_uris= rail.load_all_records(dag_run.conf['replicon_activity_uris'])
        time_types= dag_run.conf['time_types']
        if not time_types:
            return []
        cnt = len(time_types)
        activities = []
        for activity in replicon_activity_uris:
            if activity in time_types:
                activities.append({
                    'uri': activity['uri']
                })
                cnt -= 1
            if cnt == 0:
                break
        return activities
    
    put_user_payload = {
        "target": {
            "uri": dag_run.conf['useruri'],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        } if user_action == "update_user" else null,
        "template": null,
        "modifications": {
            "firstName": {
                "value": dag_run.conf['first_name']
            },
            "lastName": {
                "value": dag_run.conf['last_name']
            },
            "loginName": {
                "value": dag_run.conf['login_name']
            } if user_action == "add_user" else null,
            "emailAddress": {
                "value": dag_run.conf['email']
            },
            "employeeId": {
                "value": dag_run.conf['employee_id']
            } if user_action == "add_user" else null,
            "employmentDateRange": {
                "value": {
                    "startDate": get_replicon_date(dag_run.conf['start_date']),
                    "endDate": get_replicon_date(dag_run.conf['end_date']) if dag_run.conf['end_date'] else null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
            "securitySettings": {
                "value": {
                    "loginEnabled": get_login_enabled(dag_run, config.EXCEPTIONS_CO_CODE, user_action),
                    "forcePasswordChange": null,
                    "ssoName": {
                        "value": dag_run.conf['login_name']
                    } if user_action == "add_user" else None,
                    "ssoNameModificationOptionUri": null,
                    "password": null,
                    "authenticationProviders": [],
                    "emailMFAResendVerificationEmail": null,
                    "emailMFATryAddMethodFromUsersEmail": null,
                    "isMFAMethodRequired": null,
                    "clearIsLockedOut": null
                },
            },
            'activitiesToApply': get_activities_to_assigned(),
            "timesheetApprovalPath": get_timesheet_approvalpath(log,dag_run, user_action),
            "timeEntryApprovalPath": null,
            "workAuthorizationApprovalPath": null,
            "timeoffApprovalPath": null,
            "timeOffBalancePayoutApprovalPath": null,
            "defaultActivity": null,
            "expenseApprovalPath": null,
            "timeZone": get_timezone_uri(log, dag_run, user_action),
            "workWeekStartDay": get_workweek_start_day(log, dag_run, user_action),
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
            # "holidayCalendar": get_holiday_calendar_to_assign(dag_run, dag_run.conf['holiday_calander_uri'], user_action, dag_run.conf['change_effective_date']),
            "extensionFields": [],
            "customFields": get_udfs(log, user_action, dag_run),
            "products": [],
            "skills": [],
            "activities": get_all_enabled_activities_for_assignment(),
            "policySets": [],
            "permissionSets": get_permissionsets_to_assign(dag_run, dag_run.conf['permissionsetdetails'], user_action),
            "bankedTimePolicies": [],
            "timeOffTypes": [],
            "locationSchedule": get_user_groupmembership_to_assign(dag_run.conf['location'], dag_run.conf['location_uri'], user_action, dag_run.conf['change_effective_date'], 'location', log),
            "divisionSchedule": get_user_groupmembership_to_assign(dag_run.conf['companycode_costcenter'], dag_run.conf['companycode_costcenter_uri'], user_action, dag_run.conf['change_effective_date'], 'division', log),
            "costCenterSchedule": [],
            "serviceCenterSchedule": [],
            "departmentGroupSchedule": get_user_groupmembership_to_assign(dag_run.conf['purchase_order_id'], dag_run.conf['purchase_order_id_uri'], user_action, dag_run.conf['change_effective_date'], 'department', log),
            "employeeTypeGroupSchedule": get_user_groupmembership_to_assign(dag_run.conf['user_type'], dag_run.conf['user_type_uri'], user_action, dag_run.conf['change_effective_date'], 'employeetype', log),
            "supervisorSchedule": [],
            "timesheetPeriodSchedule": get_timesheet_period_to_assign(dag_run, dag_run.conf['timesheet_period_uri'], user_action, log, dag_run.conf['change_effective_date']),
            "holidayCalendarSchedule": get_updated_holiday_calendar_for_user(dag_run, dag_run.conf['holiday_calander_uri'], user_action, dag_run.conf['change_effective_date'], log),
            "scheduleTypeSchedule": get_schedule_type_to_assign(dag_run, dag_run.conf['schedule_uri'], user_action, dag_run.conf['change_effective_date']),
            "payRuleSchedule": get_pay_rule_to_assign(dag_run, dag_run.conf['pay_rule'], user_action, log, dag_run.conf['change_effective_date']),
            "placeSchedule": [],
            "payRateSchedule": [],
            "projectRoleSchedule": [],
            "costNormalizationRuleSchedule": [],
            "hourlyRatesSchedule": [],
            "substituteUserSchedule": [],
            "policySetsScheduleToApply": get_timesheet_template_to_assign(dag_run, user_action, dag_run.conf['change_effective_date'], log)
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

    rail.set_result(key="exception_logs",val= log)

    return put_user_payload

def get_employee_conversion_payload(dag_run):
    udfs = []
    def add_udf_field_values(definitionuri, textvalue = null , dropdownuri = null, date = null):
        if definitionuri:
            if dropdownuri or date or textvalue:
                udfs.append({
                    "value": {
                        "customField": {
                            "uri": definitionuri,
                            "name": null
                        },
                        "text": textvalue,
                        "date": get_replicon_date(date) if date else null,
                        "dropDownOption": {
                            "uri": dropdownuri,
                            "name": null
                        } if dropdownuri else None,
                        "number": null
                    }
                })
    rplcn_udfs = dag_run.conf['replicon_user_udfs']
    user_status_value_uri = rail.find_first_by_attr_and_get_attr(dag_run.conf['replicon_user_status_dropdown'],'displayText','Terminated','uri')
    add_udf_field_values(definitionuri = rplcn_udfs['user_status_uri'], dropdownuri = user_status_value_uri)
    old_data = rail.result('get_user_payload_data')['old_data']
    return {
        "target": {
            "uri": rail.result('get_user_by_empl_id')[0]['userDetails']['uri'],
        },
        "template": null,
        "modifications": {
            "loginName": {
                "value": f"{old_data['login_name']}_{old_data['user_type'].split('|')[0].upper()}"
            },
            "employeeId": {
                "value": f"{old_data['employee_id']}_{old_data['user_type'].split('|')[0].upper()}"
            },
            "employmentDateRange": {
                "value": {
                    "startDate": get_replicon_date(old_data['start_date']),
                    "endDate": get_replicon_date(old_data['end_date']) if old_data['end_date'] else null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
        "customFields": udfs,
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

def generate_bulk_assign_mutation(project_details, resource_uri):
    
    mutation_parts = ["mutation bulkAssignTasksToResourceUser {"]
    
    for idx, project in enumerate(project_details, start=1):
        project_urn = project['uri']
        
        mutation_parts.append(f"""  project{idx}: bulkAssignTasksToResourceUser(input: {{
            projectId: "{project_urn}"
            resourceUserId: "{resource_uri}"
            }}) {{
        failedTaskIds
    }}""")
    
    mutation_parts.append("}")
    
    return "\n".join(mutation_parts)

def generate_bulk_remove_mutation(project_details, resource_uri, is_resource=True):
    
    mutation_parts = ["mutation RemoveProjectTimesheetAccessMember {"]
    
    for idx, project in enumerate(project_details, start=1):
        project_urn = project['uri']
        
        mutation_parts.append(f"""  project{idx}: removeProjectTimesheetAccessMember2(
            projectId: "{project_urn}"
            accessMemberId: "{resource_uri}"
            isResource: {str(is_resource).lower()}
            )""")
    
    mutation_parts.append("}")
    
    return "\n".join(mutation_parts)

def generate_bulk_assign_resource(project_details, resource_uri):
    resp = {
            'operationName': 'bulkAssignTasksToResourceUser',
            'query': f"{generate_bulk_assign_mutation(project_details, resource_uri)}"
        }
    return json.dumps([resp])

def _new_project_assign_resource(resource_uri):
    project_details = rail.result('get_project_details')
    old_project = rail.result('get_project_code_details')['old_projects']
    new_project_details = [item for item in project_details if item['code'] not in {x['code'] for x in old_project}]
    return generate_bulk_assign_resource(new_project_details, resource_uri)

def _old_project_assign_resource(resource_uri):
    project_details = rail.result('get_old_admin_project_uris')
    new_projects = rail.result('get_project_code_details')['new_projects']
    old_project_details = [item for item in project_details if item['code'] not in {x['code'] for x in new_projects}]
    resp = {
        'operationName': 'RemoveProjectTimesheetAccessMember',
        'query': f"{generate_bulk_remove_mutation(old_project_details, resource_uri)}"
    }
    return json.dumps([resp])

def get_project_uris():
    project_uris = [uri for _, uri in rail.result('get_project_details')[0].items()] if rail.result('get_project_details') else []
    return {
            "pageIndex": "1",
            "pageSize": "10000",
            "projectUris": project_uris,
            "taskDataInclusionOptionUris": []
        }