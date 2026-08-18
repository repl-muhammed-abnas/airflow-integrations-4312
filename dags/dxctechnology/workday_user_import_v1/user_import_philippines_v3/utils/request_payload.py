from datetime import date, datetime, timedelta
from functools import lru_cache
from json import loads
from pendulum import now as pendulum_now
import rail

from airflow.exceptions import AirflowException
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import get_excluded_udf_clear_payloads, get_excluded_oef_clear_payloads
from dxctechnology.workday_user_import_v1.user_import.common_utils.region_fields_config import DXC_PHILIPPINES


null = None
INPUT_DATE_FORMAT = "%Y-%d-%m"
LOCATION_DELIMITER = " | "
NONE_DEFAULT_VALUE = "NONE"


def _is_international_assignee(is_ia):
    return is_ia in [1, '1']


def _get_assigned_shift_schedule_policy():
    assigned_policies = rail.result('get_user_assigned_policy')
    if assigned_policies:
        for policy in assigned_policies:
            if policy.get('policyUri') == "urn:replicon:policy:shift-schedule":
                return policy
    return None

def get_json_date_from_date(_date):
    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }

def get_todays_minus_specified_days_date_in_json(days_in_number:int, return_type="json"):
    today = datetime.now() -timedelta(days=days_in_number)
    if return_type == "date":
        return today.date()
    return {
        "day": today.day,
        "month": today.month,
        "year": today.year
    }

def get_replicon_date(date_str, return_format= "dict", _date_format= INPUT_DATE_FORMAT):
    _date = datetime.strptime(date_str, _date_format)
    if return_format == "date":
        return _date
    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }

def convert_json_date_to_date(json_date):
    return date(day=json_date['day'], month=json_date['month'], year=json_date['year'])

def get_ia_update_payload_for_udf_update(dag_run, custom_fields_payload, current_custom_fields_values, update_txt_udf:callable, update_date_udf: callable):
    is_ia = dag_run.conf['file_data']['is_ia']
    ia_start_date = dag_run.conf['json_formatted_dates']['ia_start_date']
    ia_end_date = dag_run.conf['json_formatted_dates']['ia_end_date']
    today_minus_five_days = convert_json_date_to_date(get_todays_minus_specified_days_date_in_json(5)) 
    effective_date = null
    if (is_ia != rail.find_first_by_attr_and_get_attr(
                current_custom_fields_values, 'customField.displayText', 'International Assignee', 'text')):
        
        update_txt_udf(dag_run, 'is_ia', 'International Assignee', 'international_assignee', custom_fields_payload, current_custom_fields_values)
        
        if not ia_start_date and is_ia in [1, '1']:
            return False, "User processing skipped as IAStart date not available for IA=1", effective_date

        if not ia_end_date and is_ia in [0, '0']:
            return False, "User processing skipped as IAEnd date not available for IA=0", effective_date

        if is_ia in [1,'1'] and (convert_json_date_to_date(ia_start_date) < today_minus_five_days):
            return False, "User processing skipped as IAStart date in past for IA=1", effective_date

        if is_ia in [0,'0'] and (convert_json_date_to_date(ia_end_date) < today_minus_five_days):
            return False, "User processing skipped as IAEnd date in past for IA=0", effective_date

        if is_ia in [1,'1']:
            rail.set_result(key="effective_date", val=ia_start_date)
            effective_date = ia_start_date
            update_date_udf(dag_run, 'ia_start_date',
                            'ia_start_date', 'International assignee start date',
                            'international_assignee_start_date', custom_fields_payload, current_custom_fields_values)

        if is_ia in [0,'0']:
            rail.set_result(key="effective_date", val=get_json_date_from_date(convert_json_date_to_date(ia_end_date) + timedelta(days=1)))
            update_date_udf(dag_run, 'ia_end_date',
                            'ia_end_date', 'International assignee end date',
                            'international_assignee_end_date', custom_fields_payload, current_custom_fields_values)
            effective_date = ia_end_date

        return True, "", effective_date

    else:
        update_date_udf(dag_run, 'ia_start_date', 'ia_start_date', 'International assignee start date', 'international_assignee_start_date', custom_fields_payload, current_custom_fields_values)
        update_date_udf(dag_run, 'ia_end_date', 'ia_end_date', 'International assignee end date', 'international_assignee_end_date', custom_fields_payload, current_custom_fields_values)

        return False, "", effective_date


def compare_if_two_json_dates_are_same(date_1, date_2):
    if not date_1:
        return False
    if not date_2:
        return True
    return convert_json_date_to_date(date_1) != convert_json_date_to_date(date_2)


@lru_cache(maxsize=16)
def cached_write_json_artifact(data_task_id):
    return rail.write_json_artifact(rail.result(data_task_id))

def get_todays_date_for_timezone_in_json(timezone="America/Los_Angeles"):
    today = pendulum_now(timezone).date()
    return {
        "day": today.day,
        "month": today.month,
        "year": today.year
    }

def get_json_date_from_date_str(date_str, _format=None):
    if not date_str:
        return {}
    if _format:
        _date = datetime.strptime(date_str, _format)
    else:
        _date = datetime.strptime(date_str, INPUT_DATE_FORMAT)
    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }

def get_todays_date_in_json():
    today = datetime.now()
    return {
        "day": today.day,
        "month": today.month,
        "year": today.year
    }

def get_required_formatted_date_from_json_date(json_date, _format=INPUT_DATE_FORMAT):
    if not json_date:
        return None
    if _format:
        _date = datetime.strptime(json_date, _format)
    else:
        _date = datetime.strptime(json_date, INPUT_DATE_FORMAT)
    return _date.strftime(_format)


def cost_center_updated(dag_run, current_user_groups, effective_date):
    return bool(_get_cost_center_update_payload(dag_run, current_user_groups, effective_date))

def department_updated(dag_run, current_user_groups, effective_date):
    return bool(_get_department_update_payload(dag_run, current_user_groups, effective_date))

def get_psa_user_udf_add_update_payload(dag_run, current_udf_value, caller, current_user_groups, effective_date):
    pas_flag = False
    if dag_run.conf['groups']['cost_center'].get('uri'):
        if dag_run.conf['groups']['cost_center']['parent']['parent_available'].lower() == "yes":
            if dag_run.conf['groups']['cost_center']['parent']['textValue'] == "PSA Cost Center":
                pas_flag = True

    if pas_flag == False:
        if dag_run.conf['groups']['department'].get('uri'):
            if dag_run.conf['groups']['department']['parent']['parent_available'].lower() == "yes":
                if dag_run.conf['groups']['department']['parent']['textValue'] == "PSA Org Unit":
                    pas_flag = True

    psa_user_value = "Yes" if pas_flag else "No"
    if caller == "add":
        return psa_user_value
    elif caller == "update":
        if cost_center_updated(dag_run, current_user_groups, effective_date) or department_updated(dag_run, current_user_groups, effective_date):
            if current_udf_value.lower() != psa_user_value.lower():
                return psa_user_value
        return None
    else:
        raise


def _get_user_target(dag_run, caller):
    if caller == "add":
        return {
                "uri": null,
                "loginName": dag_run.conf['file_data']['email_id'],
                "employeeId": null,
                "parameterCorrelationId": null
            }
    if caller == "update":
        return {
            "uri": dag_run.conf['user_uri'],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        }
    raise AirflowException(f"Invalid caller: {caller}. Expected 'add' or 'update'")

def _get_email_to_add(dag_run, config):
    if config.instance in ["prod", "production", "trial"]:
        return dag_run.conf['file_data']['email_id']
    return null

def _get_work_week_to_assign(dag_run):
    if dag_run.conf['work_week']['workweek_uri']:
        return dag_run.conf['work_week']['workweek_uri']
    return null

def _get_schedule_policy_to_assign(dag_run, exception_log:list):
    if dag_run.conf['schedule']['schedule_type'] == "shift":
        return [
            {
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": null,
                    "officeSchedule": null,
                    "scheduleTypeUri": dag_run.conf['schedule']['schedule_type_uri']
                },
                "effectiveDate": null
            }
        ]

    if dag_run.conf['schedule']['schedule_type'] == "office-schedule":
        if dag_run.conf['schedule']['schedule_name']:
            if dag_run.conf['schedule']['office_schedule_details']:
                return [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": null,
                            "name": dag_run.conf['schedule']['schedule_name'],
                            "officeSchedule": {
                                "officeScheduleUri": null,
                                "name": dag_run.conf['schedule']['schedule_name']
                            },
                            "scheduleTypeUri": dag_run.conf['schedule']['schedule_type_uri']
                        },
                        "effectiveDate": null
                    }
                ]
            else:
                exception_log.append(f"""Office schedule "{dag_run.conf['schedule']['schedule_name']}" not available in Replicon. Hence default shift assigned""")
                return [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": null,
                            "name": dag_run.conf['schedule_data']['default_office_schedule']['name'],
                            "officeSchedule": {
                                "officeScheduleUri": null,
                                "name": dag_run.conf['schedule_data']['default_office_schedule']['name']
                            },
                            "scheduleTypeUri": dag_run.conf['schedule_data']['schedule_type_uri']
                        },
                        "effectiveDate": null
                    }
                ]

    return [
        {
            "schedulePolicy": {
                "officeScheduleUri": null,
                "name": "8 hours/day; Mon-Fri",
                "officeSchedule": {
                    "officeScheduleUri": null,
                    "name": "8 hours/day; Mon-Fri"
                },
                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
            },
            "effectiveDate": null
        }
    ]

def _get_is_login_enabled(dag_run):
    user_security_config = dag_run.conf.get('user_security_config', {})
    user_data = dag_run.conf.get('file_data', {})
    
    # Check if required fields exist and have valid values
    allowed_country = user_security_config.get('allowed_country')
    parent_company = user_data.get('parent_company')
    profile_status = user_security_config.get('profile_status')
    
    if user_data['on_leave'] in [1, '1']:
        return False

    if (not allowed_country or allowed_country.lower() != "enable" or
        not parent_company or not profile_status or
        profile_status.lower() != "enabled"):
        return False

    return user_security_config.get('replicon_field', False)

def _get_holiday_calendar_to_assign(dag_run, exception_log):
    # Check for holiday calendar exception from mapper data
    if exception_log and dag_run.conf.get('mapper_data', {}).get('holiday_calendar_exception'):
        exception_log.append(dag_run.conf['mapper_data']['holiday_calendar_exception'])
        return null

    if dag_run.conf['holiday_calendar'].get('holiday_calendar_uri'):
        return {
            "uri": dag_run.conf['holiday_calendar']['holiday_calendar_uri'],
            "name": null
        }

    return null

def _get_user_permission_to_assign(dag_run):
    if dag_run.conf['user_permissions']['end_user_permission']:
        return [
            {
                "uri": dag_run.conf['user_permissions']['end_user_permission']['uri'],
                "name": null
            }
        ]
    return []

def _get_policy_sets_to_assign(dag_run):
    policy_sets = []
    if dag_run.conf['user_policies']['timeoff_template'] and dag_run.conf['user_security_config']['profile_status']=="enabled":
        policy_sets.append(
            {
                "policySet": {
                    "uri": dag_run.conf['user_policies']['timeoff_template']['uri'],
                    "name": null
                }
            }
        )

    # timesheet template assignment effective date is needed add `effectiveDate` key and use  `_get_update_timesheet_template_update_payload`
    if dag_run.conf.get('user_policies', {}).get('timesheet_template', {}).get('uri'):
        policy_sets.append({
                "policySet": {
                    "uri":  dag_run.conf.get('user_policies', {}).get('timesheet_template', {}).get('uri'),
                    "name": null
                }
        })

    if dag_run.conf['user_policies']['punch_entry_policy'].get('uri'):
        policy_sets.append(
            {
                "policySet": {
                    "uri": dag_run.conf['user_policies']['punch_entry_policy']['uri'],
                    "name": null
                }
            }
        )

    if dag_run.conf['user_policies']['schedule_policy'].get('uri'):
        policy_sets.append(
            {
                "policySet": {
                    "uri": dag_run.conf['user_policies']['schedule_policy']['uri'],
                    "name": null
                }
            }
        )

    if dag_run.conf['user_policies']['overtime_requests'].get('uri'):
        policy_sets.append(
            {
                "policySet": {
                    "uri": dag_run.conf['user_policies']['overtime_requests']['uri'],
                    "name": null
                }
            }
        )

    if dag_run.conf['user_policies']['overtime_request_approval_paths'].get('uri'):
        policy_sets.append(
            {
                "policySet": {
                    "uri": dag_run.conf['user_policies']['overtime_request_approval_paths']['uri'],
                    "name": null
                }
            }
        )

    return policy_sets

def _get_timesheet_approval_path(dag_run):
    if dag_run.conf['approval_path']['timesheet_approval_path']['timesheet_approval_path']:
        return {
            "uri": null,
            "name": dag_run.conf['approval_path']['timesheet_approval_path']['timesheet_approval_path']
        }

    return null

def _get_timeoff_approval_to_assign(dag_run):
    if dag_run.conf['approval_path']['timeoff_approval']['time_off_approval_path']:
        return {
            "uri": null,
            "name": dag_run.conf['approval_path']['timeoff_approval']['time_off_approval_path']
        }

    return null

def _add_custom_field(custom_field_uri, text=null, date=null, drop_down_uri=null,drop_down_name=null, number=null):
    return {
        "customField": {
            "uri" : custom_field_uri
        },
        "text": text,
        "date": date,
        "dropDownOption": {
            "uri": drop_down_uri,
            "name": drop_down_name
        } if drop_down_uri or drop_down_name else null,
        "number": number   
    }

def _get_custom_fields_to_assign(dag_run):
    custom_fields = dag_run.conf['udfs']

    udfs_to_assign = []
    # [_add_custom_field(custom_fields['perner']['uri'], text=dag_run.conf['file_data']['emp_id'])]

    if dag_run.conf['file_data']['assignment_type']:
        udfs_to_assign.append(_add_custom_field(custom_fields['assignment_type']['uri'], text=dag_run.conf['file_data']['assignment_type']))
    if dag_run.conf['file_data']['work_shift']:
        udfs_to_assign.append(_add_custom_field(custom_fields['work_shift']['uri'], text=dag_run.conf['file_data']['work_shift']))

    if dag_run.conf['file_data']['dob']:
        udfs_to_assign.append(_add_custom_field(custom_fields['date_of_birth']['uri'], date=dag_run.conf['json_formatted_dates']['date_of_birth']))

    if dag_run.conf['file_data']['time_type']:
        udfs_to_assign.append(_add_custom_field(custom_fields['time_type']['uri'], drop_down_name=dag_run.conf['file_data']['time_type']))

    if dag_run.conf['file_data']['gender']:
        udfs_to_assign.append(_add_custom_field(custom_fields['gender']['uri'], text=dag_run.conf['file_data']['gender']))

    if dag_run.conf['file_data']['on_leave']:
        udfs_to_assign.append(_add_custom_field(custom_fields['on_leave']['uri'], text=dag_run.conf['file_data']['on_leave']))

    if dag_run.conf['file_data']['job_level']:
        udfs_to_assign.append(_add_custom_field(custom_fields['job_level']['uri'], text=f"{dag_run.conf['file_data']['job_level']}"))

    if dag_run.conf['file_data']['fte']:
        udfs_to_assign.append(_add_custom_field(custom_fields['fte']['uri'], text=dag_run.conf['file_data']['fte']))

    if dag_run.conf['file_data']['management_lvl']:
        udfs_to_assign.append(_add_custom_field(custom_fields['management_level']['uri'], text=dag_run.conf['file_data']['management_lvl']))

    if dag_run.conf['file_data']['fte_pct']:
        udfs_to_assign.append(_add_custom_field(custom_fields['ftepct']['uri'], text=dag_run.conf['file_data']['fte_pct']))

    if dag_run.conf['file_data']['is_ia']:
        udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee']['uri'], text=dag_run.conf['file_data']['is_ia']))

    if dag_run.conf['file_data']['service_date']:
        udfs_to_assign.append(_add_custom_field(custom_fields['service_date']['uri'], date=get_replicon_date(dag_run.conf['file_data']['service_date'])))

    if dag_run.conf['file_data']['ia_start_date']:
        udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_start_date']['uri'], date=get_replicon_date(dag_run.conf['file_data']['ia_start_date'])))

    if not dag_run.conf['file_data']['ia_start_date'] and dag_run.conf['file_data']['is_ia'] in [1, '1']:
        udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_start_date']['uri'], date=get_todays_date_in_json()))

    if dag_run.conf['file_data']['ia_end_date']:
        udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_end_date']['uri'], date=get_replicon_date(dag_run.conf['file_data']['ia_end_date'])))

    pas_flag = get_psa_user_udf_add_update_payload(dag_run, '', 'add', [], null)

    if pas_flag:
        udfs_to_assign.append(_add_custom_field(custom_fields['psa_user']['uri'], drop_down_name="Yes"))

    return udfs_to_assign

def _get_activity_list_to_assign(dag_run):
    activity_list = dag_run.conf['activities']['activity_list']

    return list(map(lambda activity: {
        "uri": null,
        "name": activity
    }, activity_list))

def _get_timezone_to_apply(dag_run, exception_log:list):
    if dag_run.conf['timezone']['timezone_uri']:
        return {
            'uri': dag_run.conf['timezone']['timezone_uri'],
            'IANAName': null
        }
    exception_log.append(f"Time Zone not defined for country {dag_run.conf['file_data']['country']} in mapper")

    return null

def _get_cost_center_to_apply(dag_run):
    if dag_run.conf['groups']['cost_center'].get('uri'):
        return [
            {
                "costCenter": {
                    "uri": dag_run.conf['groups']['cost_center']['uri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null
            }
        ]
    return []

def _get_department_to_assign(dag_run):
    if dag_run.conf['groups']['department'].get('uri'):
        return [
            {
                "departmentGroup": {
                    "uri": dag_run.conf['groups']['department']['uri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null
            }
        ]

    return []

def _get_location_to_assign(dag_run, exception_log):
    # Check for location exception from groups data
    if dag_run.conf.get('groups', {}).get('location_exception'):
        exception_log.append(dag_run.conf['groups']['location_exception'])
        return []
        
    if dag_run.conf['groups']['location'].get('uri'):
        return [
            {
                "location": {
                    "uri": dag_run.conf['groups']['location']['uri'],
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }
        ]
    return []

def _get_division_to_assign(dag_run):
    if dag_run.conf['groups']['division'].get("uri"):
        return [
            {
                "division": {
                    "uri":  dag_run.conf['groups']['division']['uri'],
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }
        ]

    return []

def _get_service_center_to_assign(dag_run):
    if dag_run.conf['file_data']['pay_group']:
        return [
            {
                "serviceCenter": {
                    "uri": null,
                    "parentUri": null,
                    "name": dag_run.conf['file_data']['pay_group']
                },
                "effectiveDate": null
            }
        ]
    return []

def _get_employee_type_uri_to_assign(dag_run):
    if dag_run.conf['groups']['employee_type'].get('uri'):
        return [
            {
                "employeeTypeGroup": {
                    "uri": dag_run.conf['groups']['employee_type']['uri']['uri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null
            }
        ]
    return null

def _get_policy_data_access_scope_to_assign():
    return   [
        {
            "policyUri": "urn:replicon:policy:time-off",
            "locations": [
            {
                "location": null,
                "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
                "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:include-descendants"
            }
            ],
            "divisions": [],
            "costCenters": [],
            "serviceCenters": [],
            "departmentGroups": [],
            "employeeTypeGroups": []
        },
        {
            "policyUri": "urn:replicon:policy:user",
            "locations": [
            {
                "location": null,
                "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
                "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:include-descendants"
            }
            ],
            "divisions": [],
            "costCenters": [],
            "serviceCenters": [],
            "departmentGroups": [],
            "employeeTypeGroups": []
        }
    ]

def _get_display_name_to_assign(dag_run):
    return {
        "displayName": f"""{dag_run.conf['file_data']['last_name']}, {dag_run.conf['file_data']['first_name']} {
            dag_run.conf['file_data']['emp_id']} {dag_run.conf['file_data']['email_id']}"""
    }

def _get_payrule_to_assign(dag_run):
    if dag_run.conf['payrule']['payrule']:
        return [
            {
                "payRuleScript": {
                    "uri": null,
                    "name": dag_run.conf['payrule']['payrule']
                },
                "effectiveDate": null
            }
        ]

    return []

def _get_timesheet_period_schedule_to_apply_add_user(dag_run):

    if dag_run.conf['user_policies']['timesheet_period']['timesheet_period']:
        return [
            {
                "timesheetPeriod": {
                    "uri": null,
                    "name": dag_run.conf['user_policies']['timesheet_period']['timesheet_period']
                },
                "effectiveDate": _get_timesheet_template_period_effective_date(
                    hire_date=convert_json_date_to_date(dag_run.conf['json_formatted_dates']['hire_date']),
                    timesheet_period_eff_date=convert_json_date_to_date(dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']),
                    work_week_eff_date=convert_json_date_to_date(dag_run.conf['json_formatted_dates']['work_week']),
                    caller = "add",
                    timesheet_not_assigned_to_user=False
                )
            }
        ]

    return []


def create_user_payload(dag_run, config):
    exception_log = []
    payload = {
        "user": {
            "target": _get_user_target(dag_run, "add"),
            "firstname": dag_run.conf['file_data']['first_name'],
            "lastname": dag_run.conf['file_data']['last_name'],
            "emailAddress": _get_email_to_add(dag_run, config),
            "employeeId": dag_run.conf['file_data']['emp_id'],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": _get_schedule_policy_to_assign(dag_run, exception_log),
            "workWeekStartDayUri": _get_work_week_to_assign(dag_run),
            "employmentDateRange": {
                "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    dag_run.conf['user_security_config']['auth_uri']['URI']
                ],
                "isLoginEnabled": _get_is_login_enabled(dag_run),
                "loginName": dag_run.conf['file_data']['email_id'],
                "SSOName": dag_run.conf['file_data']['email_id'],
                "password": null
            },
            "holidayCalendar": _get_holiday_calendar_to_assign(dag_run, exception_log),
            "holidayCalendarAssignmentSchedule": null,
            "timeOffPolicy": null,
            "permissionSets": _get_user_permission_to_assign(dag_run),
            "policySets": [],
            "policySetsSchedule": _get_policy_sets_to_assign(dag_run),
            "employeeType": null,
            "timesheetPeriodTypeUri": null,
            "costRateSchedule": null,
            "payrollRateSchedule": null,
            "defaultBillingRate": null,
            "timesheetApprovalPath": _get_timesheet_approval_path(dag_run),
            "expenseApprovalPath": null,
            "expenseDefaultReimbursementCurrency": null,
            "timeOffApprovalPath": _get_timeoff_approval_to_assign(dag_run),
            "workAuthorizationApprovalPath": null,
            "timeOffBalancePayoutApprovalPath": null,
            "customFieldValues": _get_custom_fields_to_assign(dag_run),
            "assignedActivities": _get_activity_list_to_assign(dag_run),
            "timeZone": _get_timezone_to_apply(dag_run, exception_log),
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": _get_location_to_assign(dag_run, exception_log),
            "divisionSchedule": _get_division_to_assign(dag_run),
            "costCenterSchedule": _get_cost_center_to_apply(dag_run),
            "serviceCenterSchedule": _get_service_center_to_assign(dag_run),
            "departmentGroupSchedule": _get_department_to_assign(dag_run),
            "employeeTypeGroupSchedule": _get_employee_type_uri_to_assign(dag_run),
            "timesheetPeriodSchedule": _get_timesheet_period_schedule_to_apply_add_user(dag_run),
            "policyDataAccessScopes": [],
            "policyDataAccessScopes2": _get_policy_data_access_scope_to_assign(),
            "payRuleScriptSchedule": _get_payrule_to_assign(dag_run),
            "displayNameParameter": _get_display_name_to_assign(dag_run),
            "decimalSeparatorUri": null,
            "numberGroupSeparatorUri": null,
            "extensionFieldValues": [],
            "workCompliancePolicyAssignmentSchedule": []
        }
    }

    rail.set_result(val=exception_log, key="exception_log")

    return payload

def get_notification_preference_to_assign_payload(dag_run, caller="add"):
    return {
        "user": {
            "uri": rail.result("create_user")["uri"] if caller == "add" else dag_run.conf['user_uri']
        },
        "preferences": {
            "notificationDeliveryPreferences": [
            {
                "objectTypeUri": "urn:replicon:object-type:project",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:user",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:timesheet",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:time-entry-revision-group",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:expense-sheet",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:time-off",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            },
            {
                "objectTypeUri": "urn:replicon:object-type:holiday",
                "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
            }
            ],
            "sharedDeliveryPreferenceOptionUris": [
            "urn:replicon:user-shared-delivery-preference-option:always-deliver"
            ]
        }
    }

def get_product_assignment_payload(dag_run):
    user_uri = rail.result("create_user").get("uri")
    if not user_uri:
        raise AirflowException("Missing user URI from create_user result")
        
    product_uris = dag_run.conf.get('user_security_config', {}).get('product_uri', [])
    if not product_uris:
        raise AirflowException("No product URIs found in user_security_config")
        
    return {
        "userUri" : user_uri,
        "productUris": product_uris
    }

def update_time_entry_path_payload(dag_run):
    return {
        "user": {
            "uri": rail.result("create_user")["uri"],
            "loginName": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "timeEntryRevisionGroupApprovalPathToApply": {
                "uri": null,
                "name": dag_run.conf['approval_path']['time_entry_approval_path']['time_entry_approval_path']
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

def get_timeoff_to_assign_remove_payload(dag_run, mode="assign"): # pylint: disable=unused-argument
    """
    Get the payload for assigning or removing timeoff for a user.
    :param dag_run: The DAG run object containing the configuration.
    :param mode: The mode of operation, can be 'assign', 'hard-remove', or 'soft-remove'.
    :return: A dictionary containing the timeoff assignment payload.
    """
    if mode not in ['assign', 'hard-remove', 'soft-remove']:
        raise AirflowException("Invalid mode provided for timeoff assignment. Use 'assign', 'hard-remove', or 'soft-remove'.")

    # Timeoffs will be assigned
    if mode == "assign":
        timeoff_uris = rail.result("timeoff_to_assign")['timeoff_data_to_assign_uri_list']
    # All timeoffs for the user will be removed / disabled
    if mode == "hard-remove":
        timeoff_uris = []
    # Used only when there are timeoffs that need to be disabled after the assignment
    # If no policy is assigned to the timeoff, it will be removed from the user profile
    if mode == "soft-remove":
        timeoff_uris = rail.result("timeoff_to_assign")['timeoff_data_to_assign_uri_list_disabled_removed']

    return {
        "userUri": rail.result('create_user')['uri'],
        "timeOffTypeUris": timeoff_uris
    }

def get_default_timeoff_policy_payload(dag_run):
    if not dag_run.conf.get('user_uri') or not dag_run.conf.get('timeoff_uri'):
        raise AirflowException("Missing required fields: user_uri or timeoff_uri in dag_run.conf")
        
    return {
        "timeOffAccount":{
            "userUri" : dag_run.conf['user_uri'],
            "timeOffTypeUri": dag_run.conf['timeoff_uri'] # timeoff_uri will have a value present
        }
    }

def get_put_user_timeoff_policy_set_payload(dag_run):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['user_uri'],
            "timeOffTypeUri": dag_run.conf['timeoff_uri']
        },
        "policySetScheduleEntries": rail.result('policy_set_to_assign')
    }

def get_user_end_date_update_payload_15(dag_run):
    return {
        "userUri": dag_run.conf['user_uri'],
        "dateRange": {
            "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
            "endDate": dag_run.conf['json_formatted_dates']['term_date'],
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }

def update_user_start_date_remove_end_date(dag_run):
    return {
        "userUri": dag_run.conf['user_uri'],
        "dateRange": {
            "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
            "endDate": null,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }

def get_timesheet_template_to_remove_payload(dag_run):
    return {
        "userUri" :  dag_run.conf['user_uri'],
        "policySetUri": rail.result("get_user_details")['timesheetTemplate']['uri']
    }

def _get_timesheet_template_period_effective_date(hire_date:date, timesheet_period_eff_date:date, work_week_eff_date: date, caller, timesheet_not_assigned_to_user:bool, true_caller:str="add"):
    if caller == "add":

        if hire_date >= timesheet_period_eff_date:
            return None
        else:
            return {
                "day": timesheet_period_eff_date.day,
                "month": timesheet_period_eff_date.month,
                "year": timesheet_period_eff_date.year
            }

    elif caller == "update":
        if timesheet_not_assigned_to_user:
            return _get_timesheet_template_period_effective_date(hire_date, timesheet_period_eff_date, None, "add", True, true_caller="update")
        else:
            return {
                "day": work_week_eff_date.day,
                "month": work_week_eff_date.month,
                "year": work_week_eff_date.year
            }
    else:
        raise


def _get_update_timesheet_template_update_payload(dag_run, caller):
    # Get user_uri safely with fallback
    if caller == "update":
        user_uri = dag_run.conf['user_uri']
    else:
        user_uri = rail.result('create_user')['uri']
    
    # Get date values with fallbacks
    json_formatted_dates = dag_run.conf.get('json_formatted_dates', {})
    timesheet_period_eff_date = convert_json_date_to_date(json_formatted_dates.get('timesheet_period_effective_date'))
    hire_date = convert_json_date_to_date(json_formatted_dates.get('hire_date'))
    work_week_eff_date = convert_json_date_to_date(json_formatted_dates.get('work_week'))  # mapping to be checked
    job_change_effective_date = convert_json_date_to_date(json_formatted_dates['job_change_effective_date'])

    if caller == "update":
        if not job_change_effective_date:
            effective_date = timesheet_period_eff_date
            # Below logic may be enabled later, hence kept it as comment
            # if pendulum.now().date() > datetime(2025, 9, 1).date():
            #     effective_date = work_week_eff_date
            # else:
            #     effective_date = timesheet_period_eff_date
        else:
            effective_date = job_change_effective_date

    timesheet_not_assigned_to_user = not bool(rail.result('get_user_details')['timesheetTemplate'])
    if caller == "add":
        timesheet_not_assigned_to_user = True

    # Get policy URI with validation
    user_policies = dag_run.conf.get('user_policies', {})
    timesheet_template = user_policies.get('timesheet_template', {})
    policy_set_uri = timesheet_template.get('uri')
    
    if not policy_set_uri:
        raise ValueError("Missing timesheet template URI")

    return {
        "user": {
            "uri": user_uri,
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "policySetsScheduleToApply": [
                {
                    "policyUri": timesheet_template.get('policy_uri', 'urn:replicon:policy:timesheet'),
                    "schedule": [
                        {
                            "policySetUri": policy_set_uri,
                            "effectiveDate": _get_timesheet_template_period_effective_date(hire_date, timesheet_period_eff_date, effective_date, "update", timesheet_not_assigned_to_user, "update")
                        }
                    ]
                }
            ]
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }

    # return {
    #     "target": {
    #         "uri": user_uri,
    #         "loginName": null,
    #         "employeeId": null,
    #         "parameterCorrelationId": null
    #     },
    #     "schedule": [
    #         {
    #             "policySetUri": policy_set_uri,
    #             "effectiveDate": _get_timesheet_template_period_effective_date(
    #                 hire_date, timesheet_period_eff_date, work_week_eff_date, "add", True
    #             )
    #         }
    #     ]
    # }

### Update user logic starts
def _get_custom_fields_payload(uri, txt_value=null, date_value=null, drop_down_value_name=null, drop_down_value_uri=null, number_value=null):
    return {
        "customField": {
            "uri": uri,
            "name": null,
            "groupUri": null
        },
        "text": txt_value,
        "date": date_value,
        "dropDownOption": {
            "uri": drop_down_value_uri,
            "name": drop_down_value_name
            } if drop_down_value_name or drop_down_value_uri else null,
        "number": number_value
    }

def _update_txt_udf(dag_run, input_field_name, udf_display_text_value, udf_key_name, custom_fields_payload, current_custom_fields_values):
    if not dag_run or not hasattr(dag_run, 'conf'):
        return False
        
    mapper_data = dag_run.conf.get('mapper_data', {})
    file_data = dag_run.conf.get('file_data', {})
    udfs = dag_run.conf.get('udfs', {})
    
    # This field is derived by mapper
    if input_field_name == 'termination_reason_code':
        input_data = mapper_data.get('termination_reason_code')
    elif isinstance(input_field_name, list):
        try:
            field_values = [file_data.get(field_name, '') for field_name in input_field_name]
            input_data = rail.smartjoin_by_delim(field_values, separator='|')
        except Exception:
            input_data = None
    else:
        input_data = file_data.get(input_field_name)
        
    if input_data:
        current_value = rail.find_first_by_attr_and_get_attr(
            current_custom_fields_values, "customField.displayText", udf_display_text_value, 'text', default="")
            
        if input_data != current_value:
            udf_uri = udfs.get(udf_key_name, {}).get('uri')
            if udf_uri:
                custom_fields_payload.append(_get_custom_fields_payload(uri=udf_uri, txt_value=input_data))

    return False


def _update_date_udf(dag_run, input_field_name, json_formatted_date_field_name, udf_display_text_value, udf_key_name, custom_fields_payload, current_custom_fields_values):
    if dag_run.conf['file_data'][input_field_name]:
        if compare_if_two_json_dates_are_same(date_1=dag_run.conf['json_formatted_dates'][json_formatted_date_field_name],
                                              date_2=rail.find_first_by_attr_and_get_attr(current_custom_fields_values,
                                                            "customField.displayText", udf_display_text_value, 'date', default="")):
            custom_fields_payload.append(
                _get_custom_fields_payload(
                    uri=dag_run.conf['udfs'][udf_key_name].get('uri'),
                    date_value=dag_run.conf['json_formatted_dates'][json_formatted_date_field_name]
                )
            )

def _update_drop_down_udf(dag_run, input_field_name, udf_display_text_value, udf_key_name, custom_fields_payload, current_custom_fields_values, custom_value_to_compare=null):
    if input_field_name == 'termination_reason_code':
        input_data = dag_run.conf['mapper_data']['termination_reason_code']
    elif isinstance(input_field_name, list):
        input_data = rail.smartjoin_by_delim([dag_run.conf['file_data'][field_name] for field_name in input_field_name], separator='|')
    # the value for work_shift is derived with a custom logic
    # to not create a new logic for that added below logic which will take care of the custom logic
    # without need of a new function
    elif input_field_name == "NA" and custom_value_to_compare:
        input_data = custom_value_to_compare
    else:
        input_data = dag_run.conf['file_data'][input_field_name]
    if input_data:
        if input_data != rail.find_first_by_attr_and_get_attr(
            current_custom_fields_values, "customField.displayText", udf_display_text_value, 'text', default=""):
            custom_fields_payload.append(
                    _get_custom_fields_payload(uri=dag_run.conf['udfs'][udf_key_name].get('uri'), drop_down_value_name=input_data))

def _update_custom_fields_for_user(dag_run, current_user_groups):
    current_custom_fields_values = rail.result("get_user_details")['userDetails']['customFieldValues']
    custom_fields_payload = []
    _update_txt_udf(dag_run, 'assignment_type', 'assignment_type', 'assignment_type', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'work_shift', 'Work Shift', 'work_shift', custom_fields_payload, current_custom_fields_values)
    _update_date_udf(dag_run, 'dob', 'date_of_birth', 'Date of Birth', 'date_of_birth', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'middle_name', 'Middle Name', 'middle_name', custom_fields_payload, current_custom_fields_values)
    _update_drop_down_udf(dag_run, 'time_type', 'Time Type', 'time_type', custom_fields_payload, current_custom_fields_values)
    # _update_txt_udf(dag_run, 'perner_id', 'IA PERNER ID', 'ia_perner_id', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'gender', 'Gender', 'gender', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'management_lvl', 'Management Level', 'management_level', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'on_leave', 'On Leave', 'on_leave', custom_fields_payload, current_custom_fields_values)
    # _update_txt_udf(dag_run, 'area_code', 'Personnel Area Code', 'personnel_area_code', custom_fields_payload, current_custom_fields_values)
    # _update_txt_udf(dag_run, 'area_name', 'Personnel Area Description', 'personnel_area_name', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'job_level', 'Job Activity Type', 'job_level', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'fte', 'FTE', 'fte', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'fte_pct', 'FTE %', 'ftepct', custom_fields_payload, current_custom_fields_values)
    ia_updated, ia_exception_msg, effective_date = get_ia_update_payload_for_udf_update(dag_run, custom_fields_payload=custom_fields_payload, current_custom_fields_values=current_custom_fields_values,
                                                        update_txt_udf=_update_txt_udf, update_date_udf=_update_date_udf)
    rail.set_result(key="ia_updated", val=ia_updated)
    rail.set_result(key="ia_exception_msg", val=ia_exception_msg)
    _update_date_udf(dag_run, 'service_date', 'service_date', 'Continuous Service Date', 'service_date', custom_fields_payload, current_custom_fields_values)

    rail.set_result(key="can_update_notifications_settings", val=True)

    psa_flag = get_psa_user_udf_add_update_payload(dag_run, rail.find_first_by_attr_and_get_attr(
            current_custom_fields_values, "customField.displayText", "PSA User", 'text', default=""), "update",
            current_user_groups, null)
    if psa_flag is not None:
        custom_fields_payload.append(
                    _get_custom_fields_payload(uri=dag_run.conf['udfs']['psa_user'].get('uri'), drop_down_value_name=psa_flag))

    # If user is an International Assignee, clear UDFs that are not applicable to Philippines region
    if _is_international_assignee(dag_run.conf['file_data'].get('is_ia')):
        excluded_udf_payloads = get_excluded_udf_clear_payloads(
            region=DXC_PHILIPPINES,
            current_custom_fields_values=current_custom_fields_values,
            udfs_config=dag_run.conf.get('udfs', {})
        )
        custom_fields_payload.extend(excluded_udf_payloads)

    return custom_fields_payload, effective_date

def _get_timezone_update_payload(dag_run, user_details, logger:list):
    timezone = dag_run.conf.get('timezone', {}).get('timezone')
    timezone_uri = dag_run.conf.get('timezone', {}).get('timezone_uri')
    
    if timezone:
        current_timezone_uri = user_details.get('timeZone', {}).get('uri')
        if timezone_uri and timezone_uri != current_timezone_uri:
            return {
                "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                "timezone": {
                    "uri": timezone_uri,
                    "IANAName": null
                }
            }
        return null
    else:
        country = dag_run.conf.get('file_data', {}).get('country', 'Unknown')
        logger.append(f"Timezone not defined in mapper for Location {country}")
    return null

def _get_work_week_update_payload(dag_run, profile_status_is_enabled):
    if profile_status_is_enabled:
        if dag_run.conf['work_week']['workweek_uri']:
            return {
                "workWeekStartDayUri": dag_run.conf['work_week']['workweek_uri']
            }
    return null

def _get_holiday_calendar_update_payload(dag_run, user_details, profile_status_is_enabled, exception_log):
    if not profile_status_is_enabled:
        return null
    
    # Check for holiday calendar exception from mapper data
    if dag_run.conf.get('mapper_data', {}).get('holiday_calendar_exception'):
        exception_log.append(dag_run.conf['mapper_data']['holiday_calendar_exception'])
        
    user_holiday_calendar = ''
    if user_details.get('holidayCalendar'):
        user_holiday_calendar = user_details['holidayCalendar'].get('displayText', '')
    
    holiday_calendar = dag_run.conf.get('holiday_calendar', {}).get('holiday_calendar')
    holiday_calendar_uri = dag_run.conf.get('holiday_calendar', {}).get('holiday_calendar_uri')
    
    if holiday_calendar and holiday_calendar != user_holiday_calendar:
        if holiday_calendar_uri:
            return {
                "holidayCalendar": {
                    "uri": holiday_calendar_uri,
                    "name": null
                }
            }
        exception_log.append(f"Holiday calendar \"{holiday_calendar}\" not available in Replicon")

    # For IA=1 users without holiday calendar in mapper, assign NONE
    if not holiday_calendar and _is_international_assignee(dag_run.conf['file_data'].get('is_ia')):
        if user_holiday_calendar != NONE_DEFAULT_VALUE:
            exception_log.append(f"Holiday calendar not available in mapper for IA=1. Assigning {NONE_DEFAULT_VALUE} holiday calendar")
            return {
                "holidayCalendar": {
                    "uri": null,
                    "name": NONE_DEFAULT_VALUE
                }
            }

    return null

def _get_location_update_payload(dag_run, current_effective_grps, effective_date, exception_log):
    # Check for location exception from groups data
    if dag_run.conf.get('groups', {}).get('location_exception'):
        exception_log.append(dag_run.conf['groups']['location_exception'])
        return null
        
    if dag_run.conf['groups']['location'] and dag_run.conf['groups']['location'].get('uri' ,'') != current_effective_grps['location'].get('uri' ,''):
        return {
            "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementLocationSchedule": [],
            "updateLocationScheduleOverDateRange": {
                "replacementLocationScheduleEntries": [
                    {
                        "location": {
                            "uri": dag_run.conf['groups']['location'].get('uri'),
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['location_effective_date']
                    }
                ],
                "endDate": null
            }
        }

    return null

def _get_division_update_payload(dag_run, current_effective_grps, effective_date):
    if dag_run.conf['groups']['division'].get('uri' ,'') and dag_run.conf['groups']['division'].get('uri' ,'') != current_effective_grps['division'].get('uri' ,''):
        return {
            "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementDivisionSchedule": [],
            "updateDivisionScheduleOverDateRange": {
                "replacementDivisionScheduleEntries": [
                    {
                        "division": {
                            "uri": dag_run.conf['groups']['division'].get('uri' ,''),
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": effective_date if effective_date else (dag_run.conf['json_formatted_dates']['job_change_effective_date'] if dag_run.conf[
                            'file_data']['job_change_effective_date'] else dag_run.conf['json_formatted_dates']['cost_center_effective_date'])
                    }
                ],
                "endDate": null
            }
        }

    return null

def _get_cost_center_update_payload(dag_run, current_effective_grps, effective_date):

    if dag_run.conf['file_data']['cost_center'] and dag_run.conf['file_data']['cost_center'] != current_effective_grps['costCenter'].get('displayText', ''):
        return {
            "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementCostCenterSchedule": [],
            "updateCostCenterScheduleOverDateRange": {
                "replacementCostCenterScheduleEntries": [
                    {
                        "costCenter": {
                            "uri": dag_run.conf['groups']['cost_center'].get('uri' ,''),
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['cost_center_effective_date']
                    }
                ],
                "endDate": null
            }
        }

    return null

def _get_department_update_payload(dag_run, current_effective_grps, effective_date):
    if dag_run.conf['file_data']['org_code'] and dag_run.conf['file_data']['org_code'] != current_effective_grps['department'].get('displayText', ''):
        return {
            "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementDepartmentGroupSchedule": [],
            "updateDepartmentGroupScheduleOverDateRange": {
                "replacementDepartmentGroupScheduleEntries": [
                    {
                        "departmentGroup": {
                            "uri": dag_run.conf['groups']['department'].get('uri'),
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['work_week']
                    }
                ],
                "endDate": null
            }
        }

    return null

def _get_employee_type_update_payload(dag_run, current_effective_grps, effective_date):
    if dag_run.conf['groups']['employee_type'] and dag_run.conf['groups']['employee_type']['uri'] and (dag_run.conf['groups']['employee_type']['uri']['uri'] != current_effective_grps['employeeType'].get('uri', '')):
        return {
            "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementEmployeeTypeGroupSchedule": [],
            "updateEmployeeTypeGroupScheduleOverDateRange": {
                "replacementEmployeeTypeGroupScheduleEntries": [
                    {
                        "employeeTypeGroup": {
                            "uri": dag_run.conf['groups']['employee_type']['uri']['uri'],
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['employee_type_effective_date']
                    }
                ],
                "endDate": null
            }
        }

    return null

def _get_service_center_update_payload(dag_run, current_effective_grps, effective_date):
    if dag_run.conf['file_data']['pay_group'] and dag_run.conf['file_data']['pay_group'] != current_effective_grps['serviceCenter'].get('displayText', ''):
        return {
            "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementServiceCenterSchedule": [],
            "updateServiceCenterScheduleOverDateRange": {
                "replacementServiceCenterScheduleEntries": [
                {
                    "serviceCenter": {
                        "uri": null,
                        "parentUri": null,
                        "name": dag_run.conf['file_data']['pay_group']
                    },
                    "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['work_week']
                }
                ],
                "endDate": null
            }
        }

    return null

def _get_two_date_diff(effective_date, user_start_date, today, assignment_in_future):
    if effective_date:
        return convert_json_date_to_date(today) - convert_json_date_to_date(effective_date)
    if assignment_in_future:
        return convert_json_date_to_date(user_start_date) - convert_json_date_to_date(effective_date)
    return convert_json_date_to_date(today) -  convert_json_date_to_date(user_start_date)

def _get_current_payrule_schedule_timesheetPeriod(payrule_schedule_details, user_start_date, assignment_in_future=False):
    current_effective_payrule = None
    # as an identifier to process very 1st record
    #! can be optimized
    current_min_day_diff = "*"
    today= get_todays_date_in_json()
    # iter from 2nd item as we have considered the 1st record as current
    for _schedule in payrule_schedule_details:
        day_diff_cnt = _get_two_date_diff(_schedule['effectiveDate'], user_start_date, today, assignment_in_future)

        # ignore the future (this is specific to PHL as to get the TS assigned in future)
        if not assignment_in_future and day_diff_cnt.days < 0:
            continue

        if current_min_day_diff=="*":
            current_effective_payrule = _schedule
            current_min_day_diff = day_diff_cnt
            continue

        if current_min_day_diff > day_diff_cnt:
            current_min_day_diff = day_diff_cnt
            current_effective_payrule = _schedule

    return current_effective_payrule


def _get_timesheet_period_to_apply_profile_status_enabled(dag_run, user_details):
    # user_details['timesheetPeriodSchedule'] will be blank if there is no value assigned to the user
    # this logic is only applicable if the user does not have any timesheet period assigned.
    if not user_details['timesheetPeriodSchedule']:
        if dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']:
            if convert_json_date_to_date(dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']) > convert_json_date_to_date(
                        dag_run.conf['json_formatted_dates']['hire_date']):
                return {
                    "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementTimesheetPeriodSchedule": [],
                    "updateTimesheetPeriodScheduleOverDateRange": {
                        "replacementTimesheetPeriodScheduleEntries": [
                            {
                                "timesheetPeriod": {
                                    "uri": null,
                                    "name": dag_run.conf['mapper_data']['timesheet_period']
                                },
                                "effectiveDate": dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']
                            }
                        ],
                        "endDate": null
                    }
                }
            if ((convert_json_date_to_date(dag_run.conf['json_formatted_dates']['hire_date']) > convert_json_date_to_date(
                        dag_run.conf['json_formatted_dates']['timesheet_period_effective_date'])) or not dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']):
                return {
                    "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementTimesheetPeriodSchedule": [],
                    "updateTimesheetPeriodScheduleOverDateRange": {
                        "replacementTimesheetPeriodScheduleEntries": [
                            {
                                "timesheetPeriod": {
                                    "uri": null,
                                    "name": dag_run.conf['mapper_data']['timesheet_period']
                                },
                                "effectiveDate": null
                            }
                        ],
                        "endDate": null
                    }
                }

    # already a timesheet period is assigned to the user 
    return null

def _get_effective_date_based_on_work_week(work_week: str, work_week_starts_with_check: list, return_as_dict:bool = False):
    today = datetime.now()
    current_weekday = today.weekday()  # Monday=0, Sunday=6
    
    
    # Handle None or empty work_week gracefully
    if not work_week:
        return {}
        
    work_week_parts = work_week.lower().split()
    if not work_week_parts:
        raise ValueError(f"Invalid work week format: {work_week}")
        
    work_week_start = work_week_parts[0]
    
    # Validate work_week_start is a valid weekday
    valid_weekdays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    if work_week_start not in valid_weekdays:
        raise ValueError(f"Invalid work week start: {work_week_start}. Must be one of {valid_weekdays}")

    def days_back_to_target(target_weekday: int) -> int:
        if current_weekday == target_weekday:
            return 0
        return (current_weekday - target_weekday) % 7
    
    # Map day name to weekday number (Monday=0, Sunday=6)
    weekday_map = {"monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3, 
                    "friday": 4, "saturday": 5, "sunday": 6}
    
    # Determine target start day based on configuration
    if work_week_start == "saturday":
        if "saturday" in work_week_starts_with_check:
            target_day = weekday_map["saturday"]
        elif "sunday" in work_week_starts_with_check:
            target_day = weekday_map["sunday"]
        else:
            target_day = weekday_map["monday"]
    else:
        target_day = weekday_map.get(work_week_start, weekday_map["monday"])
    
    days_to_subtract = days_back_to_target(target_day)
    return_date = today - timedelta(days=days_to_subtract)
    if not return_as_dict:
        return return_date
    return {
        "day": return_date.day,
        "month": return_date.month,
        "year": return_date.year
    }



def _get_timesheet_period_schedule_to_apply(dag_run, user_details, effective_date, exception_log=None):
    timesheet_period_name = dag_run.conf['mapper_data']['timesheet_period']

    # For IA=1 users without timesheet period from mapper, assign NONE
    if not timesheet_period_name and _is_international_assignee(dag_run.conf['file_data'].get('is_ia')):
        timesheet_period_name = NONE_DEFAULT_VALUE
        if exception_log:
            exception_log.append(f"Timesheet period not available in mapper for IA=1. Assigning {NONE_DEFAULT_VALUE} timesheet period")

    if user_details['timesheetPeriodSchedule']:
        current_timesheet_period = _get_current_payrule_schedule_timesheetPeriod(user_details['timesheetPeriodSchedule'],
                                                                                 user_details['userDetails']['employmentDateRange']['startDate'])
        if not current_timesheet_period:
            # as long as the timesheet period is there this will not be executed
            # however for philippines the users timesheet period will be in future for the intial load if the
            # effective date is in the past before Sept 1, 2025
            # below is the condition that handles it
            current_timesheet_period = _get_current_payrule_schedule_timesheetPeriod(user_details['timesheetPeriodSchedule'],
                                                                                 dag_run.conf['json_formatted_dates']['timesheet_period_effective_date'], True)
        if timesheet_period_name and timesheet_period_name != current_timesheet_period['timesheetPeriod']['displayText']:
            timesheet_effective_date = _get_effective_date_based_on_work_week(dag_run.conf['mapper_data']['work_week'], [])
            return {
                "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementTimesheetPeriodSchedule": [],
                "updateTimesheetPeriodScheduleOverDateRange": {
                    "replacementTimesheetPeriodScheduleEntries": [
                        {
                            "timesheetPeriod": {
                                "uri": null,
                                "name": timesheet_period_name
                            },
                            "effectiveDate": effective_date if effective_date else {
                                "day": timesheet_effective_date.day,
                                "month": timesheet_effective_date.month,
                                "year": timesheet_effective_date.year
                            }
                        }
                    ],
                    "endDate": null
                }
            }
    return null

def _get_timesheetperiod_update_payload(dag_run, user_details, effective_date):
    if not user_details['timesheetPeriodSchedule'] and dag_run.conf['mapper_data']['profile_status'].lower() == "enabled":
        data = _get_timesheet_period_to_apply_profile_status_enabled(dag_run, user_details)
        if data:
           return data 
    return _get_timesheet_period_schedule_to_apply(dag_run, user_details, effective_date)


def get_current_assigned_policies():
    return rail.result("get_user_assigned_policy")


def _can_update_timesheet_template(dag_run, user_details):
    user_policies = dag_run.conf.get('user_policies', {})
    timesheet_template_config = user_policies.get('timesheet_template', {})
    timesheet_template_name = timesheet_template_config.get('timesheet_template')
    
    if not timesheet_template_name:
        return False
        
    # Get current template name with error handling
    current_template_name = ''
    if user_details and user_details.get('timesheetTemplate'):
        rail.set_result(key='timesheet_present_for_user', val = True)
        current_template_name = user_details['timesheetTemplate'].get('name', '')
    
    # Return True if template name is different
    return (not current_template_name) or (current_template_name != timesheet_template_name)

def _can_update_timeoff_template(dag_run, user_details):
    if dag_run.conf['user_policies']['timeoff_template']['timeoff_template']:
        timeoff_template = user_details['timeOffTemplate'].get('name', '') if user_details['timeOffTemplate'] else ''
        if (not timeoff_template) or (timeoff_template != dag_run.conf['user_policies']['timeoff_template']['timeoff_template']):
            return True
    return False

def _can_update_punch_entry_policies(dag_run):
    if not dag_run.conf['user_policies'].get('punch_entry_policy', {}).get('name', ''):
        return False
    current_assigned_policies = get_current_assigned_policies()
    return not bool(list(filter(lambda time_punch_policy: time_punch_policy['policySet']['name'] == dag_run.conf['policy_sets']['punch_entry_policy'].get('name', ''), 
                filter(lambda policy: policy['policyUri']=="urn:replicon:policy:time-punch", current_assigned_policies))))

def get_user_details_2():
    return rail.result("get_user_details_2")

def _can_update_schedule_policy(dag_run):
    if not dag_run.conf['user_policies'].get('schedule_policy', {}).get('name', ''):
        return False
    current_assigned_policies = get_current_assigned_policies()
    schedule_policy = list(filter(lambda policy: policy['policySet']['name'] == dag_run.conf['user_policies']['schedule_policy']['name'] and policy['policyUri'] == "urn:replicon:policy:shift-schedule", current_assigned_policies))
    if not schedule_policy:
        return True
    return False

def _can_update_overtime_request_template(dag_run):
    if not dag_run.conf['user_policies'].get('overtime_requests', {}).get('name', ''):
        return False

    current_assigned_policies = get_user_details_2()

    if not current_assigned_policies['workAuthorizationTemplate']:
        return True

    return current_assigned_policies['workAuthorizationTemplate'].get('name', '') == dag_run.conf['user_policies']['overtime_requests']['name']

def _can_update_overtime_request_approval_path(dag_run):
    if not dag_run.conf['user_policies'].get('overtime_request_approval_paths', {}).get('overtime_request_approval_paths', ''):
        return False
    current_assigned_policies = get_user_details_2()
    if not current_assigned_policies['workAuthorizationApprovalPath']:
        return True
    return current_assigned_policies['workAuthorizationApprovalPath'].get('name', '') == dag_run.conf['user_policies']['overtime_request_approval_paths']['overtime_request_approval_paths']

def _get_policies_to_update_payload(dag_run, user_details, exception_log: list):
    policies_to_add = []
    if _can_update_punch_entry_policies(dag_run):
        policies_to_add.append(dag_run.conf['user_policies']['punch_entry_policy']['uri'])

    if _can_update_timesheet_template(dag_run, user_details):
        
        if dag_run.conf['user_policies']['timesheet_template'].get('uri'):
            # update will be done separately
            rail.set_result(key="timesheet_template_update", val=True)
        else:
            exception_log.append(f"Timesheet template {dag_run.conf['user_policies']['timesheet_template']['timesheet_template']} not available in Replicon")

    if _can_update_timeoff_template(dag_run, user_details):
        
        if dag_run.conf['user_policies']['timeoff_template'].get('uri'):
            policies_to_add.append(dag_run.conf['user_policies']['timeoff_template']['uri'])
        else:
            exception_log.append(f"Timeoff template {dag_run.conf['user_policies']['timeoff_template']['timeoff_template']} not available in Replicon")

    if _can_update_schedule_policy(dag_run):
        
        if dag_run.conf['user_policies']['schedule_policy'].get('uri'):
            policies_to_add.append(dag_run.conf['user_policies']['schedule_policy']['uri'])
        else:
            exception_log.append(f"Schedule policy {dag_run.conf['user_policies']['schedule_policy']['schedule_policy']} not available in Replicon")

    if _can_update_overtime_request_template(dag_run):
        
        if dag_run.conf['user_policies']['overtime_requests'].get('uri'):
            policies_to_add.append(dag_run.conf['user_policies']['overtime_requests']['uri'])
        else:
            exception_log.append(f"Overtime request template {dag_run.conf['user_policies']['overtime_requests']['overtime_requests']} not available in Replicon")

    if _can_update_overtime_request_approval_path(dag_run):

        if dag_run.conf['user_policies']['overtime_request_approval_paths'].get('overtime_request_approval_paths'):

            policies_to_add.append(dag_run.conf['user_policies']['overtime_request_approval_paths']['overtime_request_approval_paths'])
        else:
            exception_log.append(f"Overtime request approval path {dag_run.conf['user_policies']['overtime_request_approval_paths']['overtime_request_approval_paths']} not available in Replicon")

    # Shift-schedule policy removal logic
    policies_to_remove = []
    shift_schedule_policy = _get_assigned_shift_schedule_policy()
    if shift_schedule_policy and _is_international_assignee(dag_run.conf['file_data'].get('is_ia')) and (not dag_run.conf['user_policies'].get('schedule_policy', {}).get('name', '')):
        policies_to_remove.append(shift_schedule_policy['policySet']['uri'])

    if not dag_run.conf['user_policies']['timeoff_template'].get('timeoff_template') and _is_international_assignee(dag_run.conf['file_data'].get('is_ia')):
        uri = user_details['timeOffTemplate'].get('uri', '') if user_details['timeOffTemplate'] else ''
        if uri:
            exception_log.append("Timeoff template not in mapper for IA=1 user. Removing existing template.")
            policies_to_remove.append(uri)

    if policies_to_add or policies_to_remove:
        return {
            "policySetUrisToAssign": policies_to_add,
            "policyUrisToRemovePolicySet": [],
            "policySetUrisToRemove": policies_to_remove
        }
    return null

def _get_time_entry_approval_path_name(dag_run):
    current_timeentry_approval_path = rail.result("get_time_entry_approval_path")
    if not current_timeentry_approval_path or current_timeentry_approval_path['displayText'] != dag_run.conf['approval_path']['time_entry_approval_path']['time_entry_approval_path']:
        return {
            "uri": null,
            "name":  dag_run.conf['approval_path']['time_entry_approval_path']['time_entry_approval_path']
        }
    return null

def _get_activities_update_payload(dag_run, user_details):
    if dag_run.conf['activities']['activity_list']:
        activity_list = dag_run.conf['activities']['activity_list']
        user_activities = user_details['assignedActivities']
        can_assign_activities = False
        for activity in user_activities:
            if activity['name'] not in activity_list:
                can_assign_activities = True
                break
        for actuality in activity_list:
            if actuality not in user_activities:
                can_assign_activities = True
                break
        if can_assign_activities:
            return list(map(lambda _activity: {
                    "uri": null,
                    "name": _activity,
                }, activity_list))
    return null

def _get_timeoff_approval_update_payload(dag_run, profile_status_is_enabled):
    if profile_status_is_enabled:
        if dag_run.conf['approval_path']['timeoff_approval']['time_off_approval_path']:
            return {
                "uri": null,
                "name": dag_run.conf['approval_path']['timeoff_approval']['time_off_approval_path']
            }
    return null

def _can_update_first_name(dag_run, user_details):
    if dag_run.conf['file_data']['first_name']:
        return dag_run.conf['file_data']['first_name'] != user_details['firstName']
    return False

def _can_update_last_name(dag_run, user_details):
    if dag_run.conf['file_data']['last_name']:
        return dag_run.conf['file_data']['last_name'] != user_details['lastName']
    return False

# this will be called twice in update email and update displayValue
def _can_update_email(dag_run, user_details, config, login_name_check=True):
    if config.instance in ['trial',"prod"]:
        if dag_run.conf['file_data']['email_id']:
            if login_name_check:
                return dag_run.conf['file_data']['email_id'] != user_details['securityConfiguration']['loginName']
            if not login_name_check:
                return (not user_details['emailAddress'])
    return False

def _can_update_display_name(dag_run, user_details, config):
    return _can_update_first_name(dag_run, user_details['userDetails']) or _can_update_last_name(dag_run, user_details['userDetails'])\
          or _can_update_email(dag_run, user_details, config, True) or _can_update_email(dag_run, user_details['userDetails'], config, False)

def _get_payrule_schedule_to_update(dag_run, _user_details, effective_date):
    if dag_run.conf['payrule']['payrule']:
        current_payrule_schedule = _get_current_payrule_schedule_timesheetPeriod(_user_details['payRuleScriptSchedule'], _user_details['userDetails']['employmentDateRange']['startDate'])
        if not current_payrule_schedule or current_payrule_schedule['payRuleScript']['displayText'] != dag_run.conf['payrule']['payrule']:
            payrule_effective_date = _get_effective_date_based_on_work_week(dag_run.conf['mapper_data']['work_week'], ['saturday'])
            return {
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri" : null,
                            "name": dag_run.conf['payrule']['payrule']
                        },
                        "effectiveDate": effective_date if effective_date else {
                            "day": payrule_effective_date.day,
                            "month": payrule_effective_date.month,
                            "year": payrule_effective_date.year
                        }
                    }
                ]
            }
    else:
        if _is_international_assignee(dag_run.conf['file_data']['is_ia']):
            return {
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri" : null,
                            "name": NONE_DEFAULT_VALUE
                        },
                        "effectiveDate": effective_date if effective_date else {
                            "day": payrule_effective_date.day,
                            "month": payrule_effective_date.month,
                            "year": payrule_effective_date.year
                        }
                    }
                ]
            }

    return null

def _get_shift_assignment_to_update(dag_run, user_details, config, exception_log, effective_date):
    current_office_schedule = _get_current_payrule_schedule_timesheetPeriod(user_details['schedulePolicies'],
        user_details['userDetails']['employmentDateRange']['startDate'])
    if dag_run.conf['schedule']['schedule_name'] == "Shift Schedule":
        if not current_office_schedule or (current_office_schedule['scheduleTypeUri'] != dag_run.conf['schedule']['schedule_type_uri']):
            return {
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementSchedule": [],
                "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": null,
                                "officeSchedule": null,
                                "scheduleTypeUri": dag_run.conf['schedule']['schedule_type_uri']
                            },
                            "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['work_shift_effective_date']
                        }
                    ],
                    "endDate": null
                }
            }
        return null
    else:
        if dag_run.conf['schedule']['schedule_name']:
            if not current_office_schedule or (not current_office_schedule['officeSchedule']) or (current_office_schedule['officeSchedule']['displayText'] != dag_run.conf['schedule']['schedule_name']):
                if dag_run.conf['schedule']['office_schedule_details']:
                    return {
                        "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementSchedule": [],
                        "updateScheduleOverDateRange": {
                            "replacementScheduleEntries": [
                                {
                                    "schedulePolicy": {
                                        "officeScheduleUri": null,
                                        "name": dag_run.conf['schedule']['schedule_name'],
                                        "officeSchedule": {
                                            "officeScheduleUri": null,
                                            "name": dag_run.conf['schedule']['schedule_name']
                                        },
                                        "scheduleTypeUri": dag_run.conf['schedule']['schedule_type_uri']
                                    },
                                    "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['work_shift_effective_date']
                                }
                            ],
                            "endDate": null
                        }
                    }
                else:
                    exception_log.append(f"""Office schedule {dag_run.conf['schedule']['schedule_name']} not available in Replicon""")
        else:
            if _is_international_assignee(dag_run.conf['file_data']['is_ia']):
                exception_log.append(f"Schedule not available in mapper for IA=1. Assigning {NONE_DEFAULT_VALUE} schedule.")
                return {
                    "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementSchedule": [],
                    "updateScheduleOverDateRange": {
                        "replacementScheduleEntries": [
                            {
                                "schedulePolicy": {
                                    "officeScheduleUri": null,
                                    "name": NONE_DEFAULT_VALUE,
                                    "officeSchedule": {
                                        "officeScheduleUri": null,
                                        "name": NONE_DEFAULT_VALUE
                                    },
                                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                                },
                                "effectiveDate": dag_run.conf['json_formatted_dates']['ia_start_date']
                            }
                        ],
                        "endDate": null
                    }
                }
    return null

def _get_updated_security_config(dag_run):
    return {
        "loginEnabled": "true",
        "forcePasswordChange": "false",
        "loginName": dag_run.conf['file_data']['email_id'],
        "ssoName": dag_run.conf['file_data']['email_id'],
        "password": null,
        "enabledAuthenticationTypeUris": [
            "urn:replicon:user-authentication-type:sso"
        ],
        "userSSONameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name"
    }

def get_update_user_payload(dag_run, config):

    exceptions = []
    user_details = rail.result("get_user_details")
    current_user_groups = rail.result("get_effective_group_membership")
    profile_status_is_enabled = dag_run.conf['user_security_config']['profile_status'] == "enabled"

    custom_fields, effective_date = _update_custom_fields_for_user(dag_run, current_user_groups)
    payload = {
        "user": {
            "uri": dag_run.conf['user_uri'],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "timezoneToApply": _get_timezone_update_payload(dag_run, user_details, exceptions),
            "workWeekStartToApply": _get_work_week_update_payload(dag_run, profile_status_is_enabled),
            "holidayCalendarToApply": _get_holiday_calendar_update_payload(dag_run, user_details, profile_status_is_enabled, exceptions),
            "holidayCalendarAssignmentsToApply": null,
            "schedulePolicyToApply": _get_shift_assignment_to_update(dag_run, user_details, config, exceptions, effective_date),
            "locationScheduleToApply": _get_location_update_payload(dag_run, current_user_groups, effective_date, exceptions),
            "divisionScheduleToApply": _get_division_update_payload(dag_run, current_user_groups, effective_date),
            "costCenterScheduleToApply": _get_cost_center_update_payload(dag_run, current_user_groups, effective_date),
            "departmentGroupScheduleToApply": _get_department_update_payload(dag_run, current_user_groups, effective_date), # workweek
            "employeeTypeGroupScheduleToApply": _get_employee_type_update_payload(dag_run, current_user_groups, effective_date),
            "timesheetPeriodScheduleToApply": _get_timesheetperiod_update_payload(dag_run, user_details, effective_date), # workweek
            "serviceCenterScheduleToApply": _get_service_center_update_payload(dag_run, current_user_groups, effective_date), #workweek
            "totalBusinessCostScheduleToApply": null,
            "permissionSetsToApply": null,
            "policySetsToApply": _get_policies_to_update_payload(dag_run, user_details, exceptions),
            "policyDataAccessScopesToApply": null,
            "policyDataAccessScopesToApply2": null,
            "notificationPreferencesToApply": null,
            "timesheetPeriodTypeToApply": null,
            "timesheetApprovalPathToApply": {
                "uri": null,
                "name": dag_run.conf['approval_path']['timesheet_approval_path']['timesheet_approval_path']
            } if dag_run.conf['approval_path']['timesheet_approval_path']['timesheet_approval_path'] else null,
            "timeEntryRevisionGroupApprovalPathToApply": _get_time_entry_approval_path_name(dag_run),
            "validationRuleToApply": null,
            "activitiesToApply": _get_activities_update_payload(dag_run, user_details),
            "defaultActivityToApply": null,
            "defaultActivityToApply2": null,
            "defaultTimeOffTypeForBookingsToApply": null,
            "expenseApprovalPathToApply": null,
            "expenseDefaultReimbursementCurrencyToApply": null,
            "timeOffApprovalPathToApply": _get_timeoff_approval_update_payload(dag_run, profile_status_is_enabled),
            "productAssignmentsToApply": null,
            "timeBankPolicyToApply": null,
            "securitySettingsToApply": _get_updated_security_config(dag_run) if _can_update_email(dag_run, user_details, config) else null,
            "supervisorsToApply": null,
            "supervisorsModifications": null,
            "payrollRatesToApply": null,
            "payrollRatesModifications": null,
            "overtimeRulesToApply": null,
            "overtimeRulesModifications": null,
            "customFieldValuesToApply": custom_fields,
            "departmentToApply": null,
            "employeeTypeToApply": null,
            "userDetailsToApply": {
                "firstName": dag_run.conf['file_data']['first_name'] if _can_update_first_name(dag_run, user_details['userDetails']) else null,
                "lastName": dag_run.conf['file_data']['last_name'] if _can_update_last_name(dag_run, user_details['userDetails']) else null,
                "emailAddress": {
                    "emailAddress": dag_run.conf['file_data']['email_id']
                } if _can_update_email(dag_run, user_details, config) or _can_update_email(dag_run, user_details['userDetails'], config, False) else null,
                "language": null,
                "employmentDateRange": null,
                "employmentStartDate": null,
                "employmentEndDate": null,
                "employeeId": null,
                "displayNameParameter": _get_display_name_to_assign(dag_run)
            } if _can_update_display_name(dag_run, user_details, config) else null,
            "payRulesToApply": null,
            "payRulesScheduleModifications": _get_payrule_schedule_to_update(dag_run, user_details, effective_date), #workweek
            "payRatesModifications": null,
            "placeAssignmentsModifications": null,
            "resourceAllocationAfterUserEndDateOptionUri": null,
            "projectRolesToApply": null,
            "projectRoleAssignmentSchedulesToApply": null,
            "decimalSeparatorToApply": null,
            "numberGroupSeparatorToApply": null,
            "dateFormatToApply": null,
            "clockFormatToApply": null,
            "hoursFormatToApply": null,
            "timeZoneFormatToApply": null,
            "objectExtensionFieldsToApply": [],
            "costRateScheduleModifications": null,
            "workAuthorizationApprovalPathToApply": {
                "name":  dag_run.conf.get('user_policies', {}).get('overtime_request_approval_paths', {}).get('overtime_request_approval_paths'),
                "uri": null
            } if (dag_run.conf.get('user_policies', {}).get('overtime_request_approval_paths', {}).get('overtime_request_approval_paths') and dag_run.conf.get('user_policies', {}).get('overtime_request_approval_paths', {}).get('overtime_request_approval_paths').lower() != 'na' ) else null,
            "displayNameFormatSettingsToApply": null,
            "timePunchTimeZoneDisplayOptionToApply": null,
            "defaultTimesheetToDisplayOptionToApply": null,
            "reportSettingsToApply": null,
            "timeOffBalancePayoutApprovalPathToApply": null,
            "workCompliancePolicyAssignmentScheduleToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }

    # If user is an International Assignee, clear OEFs that are not applicable to Philippines region
    if _is_international_assignee(dag_run.conf['file_data'].get('is_ia')):
        current_oef_values = user_details.get('objectExtensionFieldValues', [])
        if current_oef_values:
            excluded_oef_payloads = get_excluded_oef_clear_payloads(
                region=DXC_PHILIPPINES,
                current_oef_values=current_oef_values
            )
            if excluded_oef_payloads:
                payload['modifications']['objectExtensionFieldsToApply'] = excluded_oef_payloads

    rail.set_result(key="exception_log", val=exceptions)
    return payload

def get_update_policy_payload(dag_run):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf["user_uri"],
            "timeOffTypeUri": dag_run.conf["timeoff_type_uri"]
        },
        "policySetScheduleEntries": loads(rail.result("format_timeoff_polices_to_assign"))
    }

def get_user_timeoff_balance_summary_payload(dag_run):
    return {
        "account": {
            "userUri": dag_run.conf["user_uri"],
            "timeOffTypeUri": dag_run.conf["timeoff_type_uri"]
        },
        "asOfDate": dag_run.conf["user_end_date_json"]
    }

