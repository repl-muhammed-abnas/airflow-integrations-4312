import pendulum
from datetime import datetime
from uuid import uuid4
from functools import lru_cache
import rail

null = None
DATE_FORMAT = "%m/%d/%Y"
INSTANCE_FORMAT = "%B %d, %Y"

def get_str_to_date(date_str, dt_format=DATE_FORMAT):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, dt_format).date()
    except:  # pylint: disable=bare-except
        return None

def get_replicon_date(date_str, dt_format=DATE_FORMAT):
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

def get_cost_center_payload():
    return {
        "page": 1,
        "pagesize": 200,
        "columnUris": [
            "urn:replicon:cost-center-list-column:cost-center",
            "urn:replicon:cost-center-list-column:effectively-enabled",
            "urn:replicon:cost-center-list-column:code",
        ],
        "sort": [],
        "filterExpression": null
    }


def get_service_center_payload():
    return {
        "page": 1,
        "pagesize": 200,
        "columnUris": [
            "urn:replicon:service-center-list-column:service-center",
            "urn:replicon:service-center-list-column:full-path",
            "urn:replicon:service-center-list-column:full-path-code",
            "urn:replicon:service-center-list-column:effectively-enabled",
            "urn:replicon:service-center-list-column:code"
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
    names = dag_run.conf['full_path'].split(',')
    hierarchy = []
    for i, name in enumerate(names):
        item = {}
        if i > 0:
            target = {"parent": {}}
            current = target["parent"]
            for j in reversed(range(i)):
                current["name"] = names[j]
                if j > 0:
                    current["parent"] = {}
                    current = current["parent"]
            item["target"] = target
        item["modificationToApply"] = {
            "name": name,
            "isEnabled": True
        }
        hierarchy.append(item)
    return {
        "hierarchy": hierarchy,
        "modificationOptionUri": "urn:replicon:hierarchy-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }


def get_locations_hierarchy_payload(dag_run):
    return get_employee_types_hierarchy_payload(dag_run)


@lru_cache(maxsize=8)
def _get_user_data(dag_run, user_action='add_user'):
    return _get_all_records(dag_run.conf['get_user_data'])[0] if user_action == 'add_user' else rail.result('get_user_data')[0]


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

def get_timeoff_approvalpath(log, dag_run, user_action):
    if not dag_run.conf['timeoffapprovalpath']:
        return null
    if dag_run.conf['timeoffapprovalpath'] and not dag_run.conf['timeoffapprovalpathuri']:
        log.append(f"Time Off Approval Path - {dag_run.conf['timeoffapprovalpath']} is not available in Replicon")
        return null
    if user_action == "add_user":
        return {
            "value": {
                "uri": dag_run.conf['timeoffapprovalpathuri'],
                "name": null
            }
        }
    else:
        current_timeoff_approvalpath = _get_user_data(dag_run, user_action)['timeOffApprovalPath']
        if not current_timeoff_approvalpath or (current_timeoff_approvalpath and (
                dag_run.conf['timeoffapprovalpath'] != current_timeoff_approvalpath['displayText'])):
            return {
                "value": {
                    "uri": dag_run.conf['timeoffapprovalpathuri'],
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

def get_timeoff_template_to_assign(log, dag_run, user_action):
    if not dag_run.conf['timeofftemplate']:
        return null
    if dag_run.conf['timeofftemplate'] and not dag_run.conf['timeofftemplateuri']:
        log.append(f"Time Off Template - {dag_run.conf['timeofftemplate']} is not available in Replicon")
        return null
    if user_action == "add_user":
        return {
            "value": {
                "uri": dag_run.conf['timeofftemplateuri'],
                "name": null
            }
        }
    else:
        current_timeoff_template = _get_user_data(dag_run, user_action)['timeOffTemplate']
        if not current_timeoff_template or (current_timeoff_template and (
            dag_run.conf['timeofftemplate'] != current_timeoff_template['displayText'])):
            return {
                "value": {
                    "uri": dag_run.conf['timeofftemplateuri'],
                    "name": null
                }
            }
    return null

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
    """
    Build Guidehouse-specific UDF values for user create/update.
    Guidehouse UDFs: change_effective_date, seniority_date, job_code,
                     job_description, pay_group, status (user_status), time_profile_name
    """
    udfs = []

    def add_udf_field_values(definitionuri, textvalue=null, dropdownuri=null, date=null):
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

    if user_action == 'add_user':
        if dag_run.conf.get('change_effective_date'):
            add_udf_field_values(definitionuri=rplcn_udfs.get('change_effective_date_uri'),
                                 date=dag_run.conf['change_effective_date'])
        if dag_run.conf.get('seniority_date'):
            add_udf_field_values(definitionuri=rplcn_udfs.get('seniority_date_uri'),
                                 date=dag_run.conf['seniority_date'])
        if dag_run.conf.get('job_code'):
            add_udf_field_values(definitionuri=rplcn_udfs.get('job_code_uri'),
                                 textvalue=dag_run.conf['job_code'])
        if dag_run.conf.get('job_description'):
            add_udf_field_values(definitionuri=rplcn_udfs.get('job_description_uri'),
                                 textvalue=dag_run.conf['job_description'])
        if dag_run.conf.get('pay_group'):
            add_udf_field_values(definitionuri=rplcn_udfs.get('pay_group_uri'),
                                 textvalue=dag_run.conf['pay_group'])
        if dag_run.conf.get('user_status'):
            add_udf_field_values(definitionuri=rplcn_udfs.get('status_uri'),
                                 dropdownuri=dag_run.conf.get('user_status_value_uri'))
        if dag_run.conf.get('time_profile_name'):
            add_udf_field_values(definitionuri=rplcn_udfs.get('time_profile_name_uri'),
                                 textvalue=dag_run.conf['time_profile_name'])

    if user_action == 'update_user':
        custom_field_values = _get_user_data(dag_run, user_action)['userDetails']['customFieldValues']
        current_change_eff = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Change Effective Date', 'text')
        current_seniority_date = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Seniority Date', 'text')
        current_job_code = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Job Code', 'text')
        current_job_desc = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Job Description', 'text')
        current_pay_grp = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Pay Group', 'text')
        current_user_status = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Status', 'text')
        current_time_profile = rail.find_first_by_attr_and_get_attr(custom_field_values,
            'customField.displayText', 'Time Profile Name', 'text')
        
        if dag_run.conf.get('change_effective_date') and (get_str_to_date(
            dag_run.conf['change_effective_date'], DATE_FORMAT) != get_str_to_date(current_change_eff, INSTANCE_FORMAT)):
            add_udf_field_values(definitionuri=rplcn_udfs.get('change_effective_date_uri'),
                                 date=dag_run.conf['change_effective_date'])
        if dag_run.conf.get('seniority_date') and (get_str_to_date(
            dag_run.conf['seniority_date'], DATE_FORMAT) != get_str_to_date(current_seniority_date, INSTANCE_FORMAT)):
            add_udf_field_values(definitionuri=rplcn_udfs.get('seniority_date_uri'),
                                 date=dag_run.conf['seniority_date'])
        if dag_run.conf.get('job_code') and dag_run.conf['job_code'] != current_job_code:
            add_udf_field_values(definitionuri=rplcn_udfs.get('job_code_uri'),
                                 textvalue=dag_run.conf['job_code'])
        if dag_run.conf.get('job_description') and dag_run.conf['job_description'] != current_job_desc:
            add_udf_field_values(definitionuri=rplcn_udfs.get('job_description_uri'),
                                 textvalue=dag_run.conf['job_description'])
        if dag_run.conf.get('pay_group') and dag_run.conf['pay_group'] != current_pay_grp:
            add_udf_field_values(definitionuri=rplcn_udfs.get('pay_group_uri'),
                                 textvalue=dag_run.conf['pay_group'])
        if dag_run.conf.get('user_status') and dag_run.conf['user_status'] != current_user_status:
            add_udf_field_values(definitionuri=rplcn_udfs.get('status_uri'),
                                 dropdownuri=dag_run.conf.get('user_status_value_uri'))
        if dag_run.conf.get('time_profile_name') and dag_run.conf['time_profile_name'] != current_time_profile:
            add_udf_field_values(definitionuri=rplcn_udfs.get('time_profile_name_uri'),
                                 textvalue=dag_run.conf['time_profile_name'])
    return udfs


def get_timesheet_period_to_assign(dag_run, timesheetperioduri, user_action, log, effective_date):
    if not dag_run.conf['timesheetperiod']:
        return []
    if dag_run.conf['timesheetperiod'] and not timesheetperioduri:
        log.append(f"Timesheet Period {dag_run.conf['timesheetperiod']} is not present in Replicon")
        return []

    if user_action == "add_user":
        return [{"dateRange": None, "item": {"uri": timesheetperioduri, "name": None}}]

    user_data = _get_user_data(dag_run, user_action)
    current_timesheetperiod = user_data['timesheetPeriodSchedule']

    current_period_uri = (
        current_timesheetperiod[-1]['timesheetPeriod']['uri']
        if current_timesheetperiod else None
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
        elif group_val and group == 'costcenter':
            log.append(f"Company Code - {group_val} is not available in Replicon")
        elif group_val and group == 'employeetype':
            log.append(f"Employee Type - {group_val} is not available in Replicon")
        elif group_val and group == 'division':
            log.append(f"Cost Center - {group_val} is not available in Replicon")
        elif group_val and group == 'financial_system':
            log.append(f"Financial System - {group_val} is not available in Replicon")
        return []
    if user_action == "add_user":
        return [
            {
                "dateRange": null,
                "item": {
                    "uri": group_uri,
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
                        "name": null
                    }
                }
            ]
    return []


def get_updated_holiday_calendar_for_user(dag_run, holiday_cal_uri, user_action, effective_date):
    incoming_status = dag_run.conf.get('user_status', '')
    on_leave = (incoming_status == 'OnLeave')

    if not on_leave and not holiday_cal_uri:
        return null

    def _build_assignment(cal_uri, eff_date):
        return {
            "holidayCalendarsAssignmentModificationOptionUri": "urn:replicon:holiday-calendar-assignment-modification-option:update-holiday-calendar-assignments-over-date-range",
            "replacementHolidayCalendarAssignments": [],
            "updateHolidayCalendarAssignmentsOverDateRange": {
                "replacementHolidayCalendarAssignments": [{
                    "holidayCalendar": {"uri": cal_uri} if cal_uri else null,
                    "effectiveDate": eff_date
                }],
                "endDate": null
            },
            "holidayCalendarAssignmentBehavior": "urn:replicon:holiday-calendar-assignment-behavior:do-not-delete-holiday-bookings-that-no-longer-apply"
        }

    if user_action == "add_user":
        if on_leave:
            return null
        return _build_assignment(holiday_cal_uri, null)

    else:  # update_user
        user_data = _get_user_data(dag_run, user_action)
        custom_fields = user_data['userDetails']['customFieldValues']
        current_status = rail.find_first_by_attr_and_get_attr(
            custom_fields, 'customField.displayText', 'Status', 'text'
        )

        if current_status != 'OnLeave' and on_leave:
            return _build_assignment(null, get_replicon_date(effective_date))

        if current_status == 'OnLeave' and not on_leave:
            if not holiday_cal_uri:
                return null
            return _build_assignment(holiday_cal_uri, get_replicon_date(effective_date))

        # Still on leave — calendar was already unassigned; leave it that way
        if current_status == 'OnLeave' and on_leave:
            return null

        # No leave transition — existing URI comparison
        current_schedule = user_data['holidayCalendarAssignmentSchedule']
        current_cal = current_schedule[-1].get('holidayCalendar') if current_schedule else null
        current_cal_uri = current_cal.get('uri') if current_cal else null
        if not current_schedule or current_cal_uri != holiday_cal_uri:
            return _build_assignment(holiday_cal_uri, get_replicon_date(effective_date))
        return null

def _dict_to_date(date_dict):
    if not date_dict:
        return None
    try:
        return datetime(date_dict['year'], date_dict['month'], date_dict['day'])
    except Exception:
        return None


def get_active_policy_entry(schedule_list, change_effective_date=null):
    todays_date = datetime.strptime(pendulum.now().strftime(DATE_FORMAT), DATE_FORMAT)
    for entry in schedule_list:
        effective_date = _dict_to_date(entry.get('effectiveDate'))
        end_date = _dict_to_date(entry.get('endDate'))
        if effective_date is None and end_date is not None:
            if todays_date <= end_date:
                return entry
        elif effective_date is None and end_date is None:
            return entry
        elif effective_date is not None and end_date is None:
            if todays_date >= effective_date:
                return entry
        elif effective_date is not None and end_date is not None:
            if effective_date <= todays_date <= end_date:
                return entry
    return None


def get_schedule_policy(dag_run, user_action, change_effective_date):
    user_data = _get_user_data(dag_run, user_action)
    active = get_active_policy_entry(user_data['schedulePolicies'], change_effective_date)
    if not active:
        return None
    return {
        "displayText": active["officeSchedule"]["displayText"],
        "uri": active["officeSchedule"]["uri"],
        "scheduletypeuri": active["scheduleTypeUri"]
    }


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
        return [{"dateRange": null, "item": item}]
    else:
        current_schedule_type = get_schedule_policy(dag_run, user_action, effective_date)
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


def get_login_enabled(dag_run, user_action):
    return {
        "value": "true" if dag_run.conf['user_status'] in ["Active", "OnLeave"] else "false",
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
            })
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
                })
        if resp:
            resp = [{
                "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                "items": resp
            }]
    return resp

def _is_eligible_by_weekly_hours(weekly_hours, min_val):
    if min_val == '' or min_val is None:
        return True
    if isinstance(min_val, (int, float)):
        return min_val == 0 or weekly_hours >= min_val
    min_str = str(min_val).strip().lower()
    if not min_str or min_str == '0':
        return True
    if min_str.startswith('<'):
        try:
            return weekly_hours < float(min_str[1:])
        except ValueError:
            return True
    try:
        return weekly_hours >= float(min_str)
    except ValueError:
        return True

@lru_cache(maxsize=8)
def _get_enabled_timeoff_types(dag_run):
    all_timeoff_types = _get_all_records(dag_run.conf['replicon_enabled_timeoff_types'])
    return {
        'name':set([timeoff_type['displayText'] for timeoff_type in all_timeoff_types]),
        'response': all_timeoff_types
    }

def if_end_date_in_past(dag_run):
    current = pendulum.now().strftime(DATE_FORMAT)
    # current = '09/30/2026'  # Hardcoded for testing purposes
    if not dag_run.conf['end_date']:
        return False
    return datetime.strptime(dag_run.conf['end_date'], DATE_FORMAT) < datetime.strptime(current, DATE_FORMAT)


def get_timeoff_types_to_assign(dag_run, config, user_action, log):
    resp = []

    location_parts = dag_run.conf['location'].split(',')
    loc1 = location_parts[0].strip() if location_parts else ''

    et_parts = dag_run.conf['employee_type'].split(',')
    et1 = et_parts[0].strip() if len(et_parts) > 0 else ''
    et2 = et_parts[1].strip() if len(et_parts) > 1 else ''
    et3 = et_parts[2].strip() if len(et_parts) > 2 else ''
    et4 = et_parts[3].strip() if len(et_parts) > 3 else ''

    schedule = dag_run.conf.get('schedule', '') or ''
    try:
        weekly_hours = float(schedule)
    except (ValueError, TypeError):
        weekly_hours = 0

    matched_types = list(dict.fromkeys(
        row['time_off_type_name']
        for row in config.TIMEOFF_MAPPER
        if row['country'].lower() == loc1.lower()
        and et1 == row['employee_type_hierarchy_level_1']
        and et2 == row['employee_type_hierarchy_level_2']
        and et3 == row['employee_type_hierarchy_level_3']
        and et4 == row['employee_type_hierarchy_level_4']
        and _is_eligible_by_weekly_hours(weekly_hours, row['min_weekly_scheduled_hours_for_eligibility'])
    ))

    if not matched_types:
        return resp
    if if_end_date_in_past(dag_run):
        return resp

    available_timeoff_types = _get_enabled_timeoff_types(dag_run)['name']
    not_available_types = [t for t in matched_types if t not in available_timeoff_types]
    if not_available_types:
        log.append(f"Time Off Types - {', '.join(not_available_types)} are not available in Replicon")
    matched_types = [t for t in matched_types if t in available_timeoff_types]

    if user_action == "add_user":
        resp = [{
            "modificationOptionUri": "urn:replicon:collection-modification-option:add",
            "items": [
                {
                    "timeOffType": {"uri": null, "name": timeoff_type},
                    "isTimeOffAllowedAgainstThisTimeOffType": "true",
                    "applyDefaultTimeOffTypePolicy": "true",
                    "defaultTimeOffTypePolicyEffectiveDate": null,
                    "policySchedule": []
                }
                for timeoff_type in matched_types
            ]
        }]
    else:
        current_names = get_assigned_timeoff_to_users(dag_run, user_action)['enabled']
        new_items = [
            {
                "timeOffType": {"uri": null, "name": timeoff_type},
                "isTimeOffAllowedAgainstThisTimeOffType": "true",
                "applyDefaultTimeOffTypePolicy": "true",
                "defaultTimeOffTypePolicyEffectiveDate": get_replicon_date(dag_run.conf.get('change_effective_date')),
                "policySchedule": []
            }
            for timeoff_type in matched_types
            if timeoff_type not in current_names
        ]
        if new_items:
            resp = [{"modificationOptionUri": "urn:replicon:collection-modification-option:add", "items": new_items}]

    return resp


def _get_non_eligible_disable_types(config, dag_run, user_action):
    """
    Returns timeOffTypes modification items to disable non-eligible types for update_user.
    For add_user, always returns an empty list (no existing types to disable).
    Uses rail.result('get_non_eligible_types') which is a list of type name strings.
    """
    if user_action != "update_user":
        return []

    user_data = rail.result('get_user_data')[0]
    policies_by_type = (user_data.get('timeOffTypePolicySummary') or {}).get('policiesByTimeOffType') or []
    loa_excluded = config.LOA_EXCLUDED_TIMEOFF_TYPES

    if if_end_date_in_past(dag_run):
        items = []
        for policy_entry in policies_by_type:
            type_name = (policy_entry.get('timeOffType') or {}).get('displayText', '')
            type_uri = (policy_entry.get('timeOffType') or {}).get('uri', '')
            if not type_uri or type_name in loa_excluded:
                continue
            items.append({
                "timeOffType": {"uri": null, "name": type_name},
                "isTimeOffAllowedAgainstThisTimeOffType": "false",
                "applyDefaultTimeOffTypePolicy": "false",
                "defaultTimeOffTypePolicyEffectiveDate": null,
                "policySchedule": []
            })
        if not items:
            return []
        return [{
            "modificationOptionUri": "urn:replicon:collection-modification-option:add",
            "items": items
        }]

    non_eligible = rail.result('get_non_eligible_types')
    if not non_eligible:
        return []
    return [{
        "modificationOptionUri": "urn:replicon:collection-modification-option:add",
        "items": [
            {
                "timeOffType": {"uri": null, "name": type_name},
                "isTimeOffAllowedAgainstThisTimeOffType": "false",
                "applyDefaultTimeOffTypePolicy": "false",
                "defaultTimeOffTypePolicyEffectiveDate": null,
                "policySchedule": []
            }
            for type_name in non_eligible
        ]
    }]


def _get_all_records(artifact):
    return rail.load_all_records(artifact)

def get_create_update_user_payload(config, dag_run, user_action):
    log = []

    def get_all_enabled_activities_for_assignment():
        activities = _get_all_records(dag_run.conf['activities'])
        if not activities:
            return []
        return [
            {
                "modificationOptionUri": "urn:replicon:collection-modification-option:add",
                "items": [
                    {
                        "uri": null,
                        "name": activity.get('name'),
                        "code": null
                    } for activity in activities
                ]
            }
        ]

    def get_default_activity_assignment(log):
        default_work_location = dag_run.conf.get('default_work_location')
        if not default_work_location:
            return null
        activities = _get_all_records(dag_run.conf['replicon_activity_uris'])
        activities_normalized = [{**a, 'name': a.get('name', '').lower()} for a in activities]
        default_work_location_uri = rail.find_first_by_attr_and_get_attr(
            activities_normalized, 'name', default_work_location.lower(), 'uri'
        )
        if not default_work_location_uri:
            log.append(f"Default Work Location - {default_work_location} is not available in Replicon")
            return null
        return {
            "value": {
                "uri": default_work_location_uri,
                "name": null
            }
        }

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
                    "loginEnabled": get_login_enabled(dag_run, user_action),
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
            'activitiesToApply': [],
            "timesheetApprovalPath": get_timesheet_approvalpath(log, dag_run, user_action),
            "timeEntryApprovalPath": null,
            "workAuthorizationApprovalPath": null,
            "timeoffApprovalPath": get_timeoff_approvalpath(log, dag_run, user_action),
            "timeOffBalancePayoutApprovalPath": null,
            "defaultActivity": get_default_activity_assignment(log),
            "expenseApprovalPath": null,
            "timeZone": get_timezone_uri(log, dag_run, user_action),
            "workWeekStartDay": get_workweek_start_day(log, dag_run, user_action),
            "defaultBillingRate": null,
            "userPreferences": null,
            "formattings": null,
            "notificationPreferences": null,
            "timesheetTemplate": null,
            "timeoffTemplate": get_timeoff_template_to_assign(log, dag_run, user_action),
            "timeOffCalendarVisibility": null,
            "expenseTemplate": null,
            "workAuthorizationTemplate": null,
            "punchEntryPolicy": {
                "value": {
                    "name": dag_run.conf['punch_policy']
                }
            } if dag_run.conf['punch_policy'] else null,
            "extensionFields": [],
            "customFields": get_udfs(log, user_action, dag_run),
            "products": [],
            "skills": [],
            "activities": get_all_enabled_activities_for_assignment(),
            "policySets": [],
            "permissionSets": get_permissionsets_to_assign(dag_run, dag_run.conf['permissionsetdetails'], user_action),
            "bankedTimePolicies": [],
            "timeOffTypes": get_timeoff_types_to_assign(dag_run, config, user_action, log) + _get_non_eligible_disable_types(config, dag_run, user_action),
            "locationSchedule": get_user_groupmembership_to_assign(
                dag_run.conf['location'], dag_run.conf['location_uri'], user_action,
                dag_run.conf['change_effective_date'], 'location', log),
            "costCenterSchedule": get_user_groupmembership_to_assign(
                dag_run.conf['company_code'], dag_run.conf['company_code_uri'], user_action,
                dag_run.conf['change_effective_date'], 'costcenter', log),
            "divisionSchedule": get_user_groupmembership_to_assign(
                dag_run.conf['cost_center_description'], dag_run.conf['costcenter_uri'], user_action,
                dag_run.conf['change_effective_date'], 'division', log),
            "serviceCenterSchedule": get_user_groupmembership_to_assign(
                dag_run.conf['financial_system'], dag_run.conf['servicecenter_uri'], user_action,
                dag_run.conf['change_effective_date'], 'servicecenter', log),
            "departmentGroupSchedule": [],
            "employeeTypeGroupSchedule": get_user_groupmembership_to_assign(
                dag_run.conf['employee_type'], dag_run.conf['employee_type_uri'], user_action,
                dag_run.conf['change_effective_date'], 'employeetype', log),
            "supervisorSchedule": [],
            "timesheetPeriodSchedule": get_timesheet_period_to_assign(
                dag_run, dag_run.conf['timesheet_period_uri'], user_action, log, dag_run.conf['change_effective_date']),
            # "holidayCalendarSchedule": get_updated_holiday_calendar_for_user(
            #     dag_run, dag_run.conf['holiday_calander_uri'], user_action, dag_run.conf['change_effective_date'], log),
            "scheduleTypeSchedule": get_schedule_type_to_assign(
                dag_run, dag_run.conf['schedule_uri'], user_action, dag_run.conf['change_effective_date']),
            "payRuleSchedule": get_pay_rule_to_assign(
                dag_run, dag_run.conf['pay_rule'], user_action, log, dag_run.conf['change_effective_date']),
            "placeSchedule": [],
            "payRateSchedule": [],
            "projectRoleSchedule": [],
            "costNormalizationRuleSchedule": [],
            "hourlyRatesSchedule": [],
            "substituteUserSchedule": [],
            "policySetsScheduleToApply": get_timesheet_template_to_assign(
                dag_run, user_action, dag_run.conf['change_effective_date'], log)
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save",
        "unitOfWorkId": str(uuid4())
    }

    if not dag_run.conf['holiday_calander_uri']:
        log.append("Holiday Calendar Not Found in Replicon")
    if not dag_run.conf['mapper_data']:
        log.append("Matching Mapper Data Not Found")

    rail.set_result(key="exception_logs", val=log)
    return put_user_payload

def get_assigned_timeoff_to_users(dag_run, user_action):
    current_timeoff_types = (_get_user_data(dag_run, user_action).get('timeOffTypePolicySummary') or {}).get('policiesByTimeOffType') or []
    return {
        'enabled': {t['timeOffType']['name'] for t in current_timeoff_types if t['isTimeOffAllowedAgainstThisTimeOffType'] in ['true', True]},
        'disabled': {t['timeOffType']['name'] for t in current_timeoff_types if t['isTimeOffAllowedAgainstThisTimeOffType'] in ['false', False]}
    }
