from datetime import datetime
from dateutil.relativedelta import relativedelta
import pendulum
import rail

from dxctechnology.workday_user_import.user_import.common_utils.custom_methods import convert_json_date_to_date
from dxctechnology.workday_user_import.user_import.common_utils.request_payload import get_todays_date_for_timezone_in_json


null = None

INPUT_DATE_FORMAT = "%Y-%d-%m"

def get_replicon_date(date_str, return_format= "dict", _date_format= INPUT_DATE_FORMAT):

    _date = datetime.strptime(date_str, _date_format)

    if return_format == "date":
        return _date

    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }

def _get_shift_from_mapper(dag_run, config):
    country = dag_run.conf['file_data']['country']
    source = dag_run.conf['mapper_data']['parent_company']
    return list(filter(lambda row: row['Type'] == "Schedule Type" and\
                                row['Function'] == "Workday User Sync" and\
                                row['Country'] == country and\
                                row['Source'] == source and\
                                row['Value'] == "Shift" and\
                                row['employeegroup'] == dag_run.conf['file_data']['work_shift']
                                ,config.DXC_WORKDAY_USER_SYNC_USER_MAPPER))

def _get_schedule_policy_to_assign(dag_run, config, exception_log:list):
    if dag_run.conf['file_data']['work_shift'] == "ES-Rotational Work shift":
        shift_data = _get_shift_from_mapper(dag_run, config)
        return [{
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": null,
                    "officeSchedule": null,
                    "scheduleTypeUri": shift_data[0]['URI']
                },
                "effectiveDate": null
            }
        ]
    if dag_run.conf['file_data']['work_shift']:
        if dag_run.conf['schedule_uri']:
            return [
                {
                        "schedulePolicy": {
                            "officeScheduleUri": null,
                            "name": dag_run.conf['file_data']['work_shift'],
                            "officeSchedule": {
                                "officeScheduleUri": null,
                                "name": dag_run.conf['file_data']['work_shift']
                            },
                            "scheduleTypeUri": dag_run.conf['mapper_data']['schedule_type_uri']
                        },
                        "effectiveDate": null
                    }
            ]
        exception_log.append(f"""Office schedule "{dag_run.conf['file_data']['work_shift']}" not available in Replicon. Hence default shift assigned""")
        return [
            {
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": dag_run.conf['mapper_data']['office_schedule'],
                    "officeSchedule": {
                        "officeScheduleUri": null,
                        "name": dag_run.conf['mapper_data']['office_schedule']
                    },
                    "scheduleTypeUri": dag_run.conf['mapper_data']['schedule_type_uri']
                },
                "effectiveDate": null
            }
        ]
    if dag_run.conf['mapper_data']['office_schedule']:
        return [
            {
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": dag_run.conf['mapper_data']['office_schedule'],
                    "officeSchedule": {
                        "officeScheduleUri": null,
                        "name": dag_run.conf['mapper_data']['office_schedule']
                    },
                    "scheduleTypeUri": dag_run.conf['mapper_data']['schedule_type_uri']
                },
                "effectiveDate": null
            }
        ]
    return [
        {
            "schedulePolicy": {
            "officeScheduleUri": null,
            "name": "7.5 hours/day, Fri, Sa off",
            "officeSchedule": {
                "officeScheduleUri": null,
                "name": "7.5 hours/day, Fri, Sa off"
            },
            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
            },
            "effectiveDate": null
        }
        ]

def _get_user_permission_to_assign(dag_run):
    if dag_run.conf['mapper_data']['end_user_permission_uri']:
        return [
            {
                "uri": dag_run.conf['mapper_data']['end_user_permission_uri'],
                "name": null
            }
        ]
    return []

def _get_policy_sets_to_assign(dag_run):
    policy_sets = []
    if dag_run.conf['mapper_data']['timeoff_template'] and dag_run.conf['mapper_data']['profile_status']=="enabled":
        policy_sets.append({
            "name": dag_run.conf['mapper_data']['timeoff_template'],
            "uri": null
        })

    if dag_run.conf['mapper_data']['timesheet_template_name'] and dag_run.conf['mapper_data']['profile_status']=="enabled":
        if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
            policy_sets.append({
                "name": dag_run.conf['mapper_data']['timesheet_template_name'],
                "uri": null
            })

    if dag_run.conf['mapper_data']['punch_entry_policy_uri']:
        policy_sets.append({
            "name": null,
            "uri": dag_run.conf['mapper_data']['punch_entry_policy_uri']
        })

    return policy_sets

def _get_timesheet_approval_path(dag_run):
    if dag_run.conf['mapper_data']['timesheet_approval_path']:
        return {
            "uri": null,
            "name": dag_run.conf['mapper_data']['timesheet_approval_path']
        }

    return null

def _get_timeoff_approval_to_assign(dag_run):
    if dag_run.conf['mapper_data']['timeoff_approval']:
        return {
            "uri": null,
            "name": dag_run.conf['mapper_data']['timeoff_approval']
        }

    return null

def _get_work_week_to_assign(dag_run):
    if dag_run.conf['mapper_data']['workweek_uri']:
        return dag_run.conf['mapper_data']['workweek_uri']
    return null

def _get_holiday_calendar_to_assign(dag_run):
    if dag_run.conf['mapper_data']['holiday_calender_uri']:
        return {
            "uri": dag_run.conf['mapper_data']['holiday_calender_uri'],
            "name": null
        }
    return null

def _get_employee_type_uri_to_assign(dag_run):
    if dag_run.conf['employee_tye_uri']:
        return [
            {
                "employeeTypeGroup": {
                    "uri": dag_run.conf['employee_tye_uri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null
            }
        ]
    return null

def _get_payrule_to_assign(dag_run):
    if dag_run.conf['mapper_data']['payrule']:
        if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
            return [
                {
                    "payRuleScript": {
                        "uri": null,
                        "name": dag_run.conf['mapper_data']['payrule']
                    },
                    "effectiveDate": null
                }
            ]
    return []

def _get_cost_center_to_apply(dag_run):
    if dag_run.conf['cost_center_uri']:
        return [
            {
                "costCenter": {
                    "uri": dag_run.conf['cost_center_uri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null
            }
        ]
    return []

def _get_department_to_assign(dag_run):
    if dag_run.conf['organizational_unit_uri']:
        return [
            {
                "departmentGroup": {
                    "uri": dag_run.conf['organizational_unit_uri'],
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "effectiveDate": null
            }
        ]

    return []

def _get_location_to_assign(dag_run):
    if dag_run.conf['location_uri']:
        return [
            {
                "location": {
                    "uri": dag_run.conf['location_uri'],
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": null
            }
        ]
    return []

def _get_division_to_assign(dag_run):
    if dag_run.conf['company_code_uri']:
        return [
            {
                "division": {
                    "uri":  dag_run.conf['company_code_uri'],
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

def _get_activity_list_to_assign(dag_run):
    activity_list = dag_run.conf['mapper_data']['activity_list']

    return list(map(lambda activity: {
        "uri": null,
        "name": activity
    }, activity_list))

# pylint: disable=too-many-arguments
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

def _compare_if_two_json_dates_are_same(date_1, date_2):
    if not date_1 or not date_2:
        return False
    return convert_json_date_to_date(date_1) != convert_json_date_to_date(date_2)

# pylint: disable=too-many-statements
# pylint: disable=too-many-branches
def _get_custom_fields_to_assign(dag_run, action):
    custom_fields = dag_run.conf['udfs']

    udfs_to_assign = []

    if action =="add":

        udfs_to_assign = [_add_custom_field(custom_fields['perner']['uri'], text=dag_run.conf['file_data']['emp_id'])]

        if dag_run.conf['file_data']['assignment_type']:
            udfs_to_assign.append(_add_custom_field(custom_fields['assignment_type']['uri'], text=dag_run.conf['file_data']['assignment_type']))

        if dag_run.conf['file_data']['perner_id']:
            udfs_to_assign.append(_add_custom_field(custom_fields['ia_perner_id']['uri'], text=dag_run.conf['file_data']['perner_id']))

        if dag_run.conf['file_data']['dob']:
            udfs_to_assign.append(_add_custom_field(custom_fields['date_of_birth']['uri'], date=dag_run.conf['json_formatted_dates']['date_of_birth']))

        if dag_run.conf['file_data']['rut']:
            udfs_to_assign.append(_add_custom_field(custom_fields['rut']['uri'], text=dag_run.conf['file_data']['rut']))

        if dag_run.conf['file_data']['time_type']:
            udfs_to_assign.append(_add_custom_field(custom_fields['time_type']['uri'], drop_down_name=dag_run.conf['file_data']['time_type']))

        if dag_run.conf['file_data']['gender']:
            udfs_to_assign.append(_add_custom_field(custom_fields['gender']['uri'], text=dag_run.conf['file_data']['gender']))

        if dag_run.conf['file_data']['on_leave']:
            udfs_to_assign.append(_add_custom_field(custom_fields['on_leave']['uri'], text=dag_run.conf['file_data']['on_leave']))

        if dag_run.conf['file_data']['area_code']:
            udfs_to_assign.append(_add_custom_field(custom_fields['personnel_area_code']['uri'], text=dag_run.conf['file_data']['area_code']))

        if dag_run.conf['file_data']['area_name']:
            udfs_to_assign.append(_add_custom_field(custom_fields['personnel_area_name']['uri'], text=dag_run.conf['file_data']['area_name']))

        if dag_run.conf['file_data']['job_level']:
            job_level_prefix = "H" if dag_run.conf['file_data']['country']=="Canada" else ""
            udfs_to_assign.append(_add_custom_field(custom_fields['job_level']['uri'], text=f"{job_level_prefix}{dag_run.conf['file_data']['job_level']}"))

        if dag_run.conf['file_data']['fte']:
            udfs_to_assign.append(_add_custom_field(custom_fields['fte']['uri'], text=dag_run.conf['file_data']['fte']))

        if dag_run.conf['file_data']['management_lvl']:
            udfs_to_assign.append(_add_custom_field(custom_fields['management_level']['uri'], text=dag_run.conf['file_data']['management_lvl']))

        if dag_run.conf['file_data']['fte_pct']:
            udfs_to_assign.append(_add_custom_field(custom_fields['ftepct']['uri'], text=dag_run.conf['file_data']['fte_pct']))

        if dag_run.conf['file_data']['is_ia']:
            udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee']['uri'], text=dag_run.conf['file_data']['is_ia']))

        if dag_run.conf['file_data']['service_date']:
            udfs_to_assign.append(_add_custom_field(custom_fields['service_date']['uri'],
                date=get_replicon_date(dag_run.conf['file_data']['service_date'])))

        if dag_run.conf['file_data']['ia_start_date']:
            udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_start_date']['uri'],
                date=get_replicon_date(dag_run.conf['file_data']['ia_start_date'])))

        if not dag_run.conf['file_data']['ia_start_date'] and dag_run.conf['file_data']['is_ia'] in [1, '1']:
            udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_start_date']['uri'], date=dag_run.conf['today']))

        if dag_run.conf['file_data']['ia_end_date']:
            udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_end_date']['uri'],
                date=get_replicon_date(dag_run.conf['file_data']['ia_end_date'])))

    if action == "update":
        current_custom_fields_values = rail.result("get_user_details")['userDetails']['customFieldValues']

        if dag_run.conf['file_data']['assignment_type'] and dag_run.conf['file_data']['assignment_type'] != \
            rail.result("current_assigned_udf_values")['assignment_type']:
            udfs_to_assign.append(_add_custom_field(custom_fields['assignment_type']['uri'], text=dag_run.conf['file_data']['assignment_type']))

        if not dag_run.conf['file_data']['assignment_type'] and rail.result("current_assigned_udf_values")['assignment_type']:
            udfs_to_assign.append(_add_custom_field(custom_fields['assignment_type']['uri'], text=null))

        # if dag_run.conf['file_data']['dob'] and _compare_if_two_json_dates_are_same(date_1=dag_run.conf['json_formatted_dates']['date_of_birth'],
        #     date_2=rail.find_first_by_attr_and_get_attr(current_custom_fields_values, "customField.displayText", 'Date of Birth', 'date', default="")):
        #     udfs_to_assign.append(_add_custom_field(custom_fields['date_of_birth']['uri'], date=dag_run.conf['json_formatted_dates']['date_of_birth'])) need to check

        if dag_run.conf['file_data']['middle_name'] and dag_run.conf['file_data']['middle_name'] != rail.result("current_assigned_udf_values")['middle_name']:
            udfs_to_assign.append(_add_custom_field(custom_fields['middle_name']['uri'], text=dag_run.conf['file_data']['middle_name']))

        if dag_run.conf['file_data']['time_type'] and dag_run.conf['file_data']['time_type'] != rail.result("current_assigned_udf_values")['time_type']:
            udfs_to_assign.append(_add_custom_field(custom_fields['time_type']['uri'], drop_down_name=dag_run.conf['file_data']['time_type']))

        if dag_run.conf['file_data']['perner_id'] and dag_run.conf['file_data']['perner_id'] != rail.result("current_assigned_udf_values")['perner_id']:
            udfs_to_assign.append(_add_custom_field(custom_fields['ia_perner_id']['uri'], text=dag_run.conf['file_data']['perner_id']))

        if dag_run.conf['file_data']['gender'] and dag_run.conf['file_data']['gender'] != rail.result("current_assigned_udf_values")['gender']:
            udfs_to_assign.append(_add_custom_field(custom_fields['gender']['uri'], text=dag_run.conf['file_data']['gender']))

        if dag_run.conf['file_data']['management_lvl'] and dag_run.conf['file_data']['management_lvl'] != \
            rail.result("current_assigned_udf_values")['mgmt_lvl']:
            udfs_to_assign.append(_add_custom_field(custom_fields['management_level']['uri'], text=dag_run.conf['file_data']['management_lvl']))

        if dag_run.conf['file_data']['on_leave'] and dag_run.conf['file_data']['on_leave'] != rail.result("current_assigned_udf_values")['on_leave']:
            udfs_to_assign.append(_add_custom_field(custom_fields['on_leave']['uri'], text=dag_run.conf['file_data']['on_leave']))

        if dag_run.conf['file_data']['area_code'] and dag_run.conf['file_data']['area_code'] != rail.result("current_assigned_udf_values")['personnal_area_code']:
            udfs_to_assign.append(_add_custom_field(custom_fields['personnel_area_code']['uri'], text=dag_run.conf['file_data']['area_code']))

        if dag_run.conf['file_data']['area_name'] and dag_run.conf['file_data']['area_name'] != rail.result("current_assigned_udf_values")['personnal_area_description']:
            udfs_to_assign.append(_add_custom_field(custom_fields['personnel_area_name']['uri'], text=dag_run.conf['file_data']['area_name']))

        job_level_prefix = "H" if dag_run.conf['file_data']['country']=="Canada" else ""
        if dag_run.conf['file_data']['job_level'] and f"{job_level_prefix}{dag_run.conf['file_data']['job_level']}" != \
            rail.result("current_assigned_udf_values")['job_activity_type']:
            udfs_to_assign.append(_add_custom_field(custom_fields['job_level']['uri'], text=f"{job_level_prefix}{dag_run.conf['file_data']['job_level']}"))

        if dag_run.conf['file_data']['fte'] and dag_run.conf['file_data']['fte'] != rail.result("current_assigned_udf_values")['fte']:
            udfs_to_assign.append(_add_custom_field(custom_fields['fte']['uri'], text=dag_run.conf['file_data']['fte']))

        if dag_run.conf['file_data']['fte_pct'] and dag_run.conf['file_data']['fte_pct'] != rail.result("current_assigned_udf_values")['fte']:
            udfs_to_assign.append(_add_custom_field(custom_fields['ftepct']['uri'], text=dag_run.conf['file_data']['fte_pct']))

        if dag_run.conf['file_data']['service_date'] and _compare_if_two_json_dates_are_same(date_1=dag_run.conf['json_formatted_dates']['service_date'],
            date_2=rail.find_first_by_attr_and_get_attr(current_custom_fields_values,
                "customField.displayText", 'Continuous Service Date', 'date', default="")):
            udfs_to_assign.append(_add_custom_field(custom_fields['service_date']['uri'],
                date=get_replicon_date(dag_run.conf['file_data']['service_date'])))
            
        if dag_run.conf['file_data']['is_ia'] and dag_run.conf['file_data']['is_ia'] != rail.result("current_assigned_udf_values")['is_ia']:
            udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee']['uri'], text=dag_run.conf['file_data']['is_ia']))
            if dag_run.conf['mapper_data']['parent_company']=="COMPASS":
                if dag_run.conf['file_data']['is_ia'] in [1, '1']:
                    udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_start_date']['uri'], 
                    date=get_replicon_date(dag_run.conf['file_data']['ia_start_date'])))
                if dag_run.conf['file_data']['is_ia'] in [0, '0']:
                    ia_end_date_to_consider = datetime.strptime(dag_run.conf['file_data']['ia_end_date'],INPUT_DATE_FORMAT) + relativedelta(days=1)
                    udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_end_date']['uri'],
                    date=get_replicon_date(ia_end_date_to_consider.strftime(INPUT_DATE_FORMAT))))
            else:
                if not dag_run.conf['file_data']['ia_start_date'] and _compare_if_two_json_dates_are_same(date_1=dag_run.conf['json_formatted_dates']['ia_start_date'],
                    date_2=rail.find_first_by_attr_and_get_attr(current_custom_fields_values,
                        "customField.displayText", 'International assignee start date', 'date', default="")):
                    udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_start_date']['uri'],
                        date=get_replicon_date(dag_run.conf['file_data']['ia_start_date'])))

                if dag_run.conf['file_data']['ia_end_date'] and _compare_if_two_json_dates_are_same(date_1=dag_run.conf['json_formatted_dates']['ia_end_date'],
                    date_2=rail.find_first_by_attr_and_get_attr(current_custom_fields_values,
                        "customField.displayText", 'International assignee end date', 'date', default="")):
                    udfs_to_assign.append(_add_custom_field(custom_fields['international_assignee_end_date']['uri'],
                        date=get_replicon_date(dag_run.conf['file_data']['ia_end_date'])))


    return udfs_to_assign

def _get_is_login_enabled(dag_run):
    if (not dag_run.conf['mapper_data']['allowed_country']) or (dag_run.conf['mapper_data']['allowed_country'] != "Enable") or\
        (not dag_run.conf['mapper_data']['parent_company']) or (not dag_run.conf['mapper_data']['profile_status']) or\
            (dag_run.conf['mapper_data']['profile_status'] != "enabled"):
        return False
    return dag_run.conf['replicon_field']

def _get_timesheet_period_schedule_to_assign(dag_run):

    if dag_run.conf['mapper_data']['timesheet_period_effective_date']:
        if convert_json_date_to_date(dag_run.conf['mapper_data']['timesheet_period_effective_date_json_format']) >\
            convert_json_date_to_date(dag_run.conf['json_formatted_dates']['hire_date']):
            return [
                {
                    "timesheetPeriod": {
                        "uri": null,
                        "name": dag_run.conf['mapper_data']['timesheet_period']
                    },
                    "effectiveDate": dag_run.conf['mapper_data']['timesheet_period_effective_date_json_format']
                }
            ]
    return [
        {
            "timesheetPeriod": {
            "uri": null,
            "name": dag_run.conf['mapper_data']['timesheet_period']
            },
            "effectiveDate": null
        }
    ]

def _get_timezone_to_apply(dag_run, exception_log):
    if dag_run.conf['mapper_data']['timezone_uri']:
        return {
            'uri': dag_run.conf['mapper_data']['timezone_uri'],
            'IANAName': null
        }
    exception_log.append(f"Time Zone not defined for country {dag_run.conf['file_data']['country']} in mapper")

    return null

def _get_email_to_add(dag_run, config):
    if config.instance in ["prod", "production", "trial"]:
        return dag_run.conf['file_data']['email_id']
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

def create_user_payload(dag_run, config):
    exception_log = []
    payload = {
        "user": {
            "target": {
                "uri": null,
                "loginName": dag_run.conf['file_data']['email_id'],
                "employeeId": null,
                "parameterCorrelationId": null
            },
            "firstname": dag_run.conf['file_data']['first_name'],
            "lastname": dag_run.conf['file_data']['last_name'],
            "emailAddress": _get_email_to_add(dag_run, config),
            "employeeId": dag_run.conf['file_data']['emp_id'],
            "department": null,
            "supervisorAssignmentSchedule": null,
            "schedulePolicySchedule": _get_schedule_policy_to_assign(dag_run, config, exception_log),
            "workWeekStartDayUri": _get_work_week_to_assign(dag_run),
            "employmentDateRange": {
                "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    dag_run.conf['mapper_data']['authentication_uri']
                ],
                "isLoginEnabled": _get_is_login_enabled(dag_run),
                "loginName": dag_run.conf['file_data']['email_id'],
                "SSOName": dag_run.conf['file_data']['email_id'],
                "password": null
            },
            "holidayCalendar": _get_holiday_calendar_to_assign(dag_run),
            "holidayCalendarAssignmentSchedule": null,
            "timeOffPolicy": null,
            "permissionSets": _get_user_permission_to_assign(dag_run),
            "policySets": _get_policy_sets_to_assign(dag_run),
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
            "customFieldValues": _get_custom_fields_to_assign(dag_run,"add"),
            "assignedActivities": _get_activity_list_to_assign(dag_run),
            "timeZone": _get_timezone_to_apply(dag_run, exception_log),
            "overtimeRuleAssignmentSchedule": null,
            "validationRuleAssignmentSchedule": null,
            "locationSchedule": _get_location_to_assign(dag_run),
            "divisionSchedule": _get_division_to_assign(dag_run),
            "costCenterSchedule": _get_cost_center_to_apply(dag_run),
            "serviceCenterSchedule": _get_service_center_to_assign(dag_run),
            "departmentGroupSchedule": _get_department_to_assign(dag_run),
            "employeeTypeGroupSchedule": _get_employee_type_uri_to_assign(dag_run),
            "timesheetPeriodSchedule": _get_timesheet_period_schedule_to_assign(dag_run),
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

    rail.set_result(key="exception_log", val=exception_log)
    return payload

def get_notification_preference_to_assign(dag_run, action):
    return {
        "user": {
            "uri": rail.result("create_user")["uri"] if action =='add' else dag_run.conf['user_uri']
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

def map_mapper_replicon_timeoffs():
    replicon_timeoffs = rail.result("get_all_timeoffs")
    mapper_timeoffs = rail.load_all_records(rail.result('query_timeoff_data'))

    mapped_timeoff_data =  list(map(lambda timeoff: {
            "name": timeoff['Value'],
            "uri": rail.find_first_by_attr_and_get_attr(
                replicon_timeoffs, 'name', timeoff['Value'].strip(), 'uri'),
            "policy_type": timeoff['URI'] if timeoff['URI'] else null
        }, mapper_timeoffs))

    rail.set_result(key = "mapped_timeoff_data", val = mapped_timeoff_data)

    return list(set(map(lambda record: record['uri'], filter(lambda to: bool(to['uri']), mapped_timeoff_data))))

def _can_update_first_name(dag_run, user_details):
    if dag_run.conf['file_data']['first_name']:
        return dag_run.conf['file_data']['first_name'].lower() != user_details['userDetails']['firstName'].lower()
    return False

def _can_update_last_name(dag_run, user_details):
    if dag_run.conf['file_data']['last_name']:
        return dag_run.conf['file_data']['last_name'] != user_details['userDetails']['lastName']
    return False

# this will be called twice in update email and update displayValue
def _can_update_email(dag_run, user_details, config, login_name_check=True):
    if config.instance in ["prod", "production", "trial"]:
        if dag_run.conf['file_data']['email_id']:
            if login_name_check:
                return dag_run.conf['file_data']['email_id'] != user_details['securityConfiguration']['loginName']
            if not login_name_check:
                return not user_details['userDetails']['emailAddress']
    return False

def _can_update_display_name(dag_run, user_details, config):
    return _can_update_first_name(dag_run, user_details) or _can_update_last_name(dag_run, user_details)\
          or _can_update_email(dag_run, user_details, config, True) or _can_update_email(dag_run, user_details, config, False)

def _get_update_for_security_settings(dag_run, config, user_details):
    if dag_run.conf['file_data']['email_id'] and dag_run.conf['file_data']['email_id'] != user_details['securityConfiguration']['loginName']:
        if config.instance in ["prod", "production", "trial"]:
            return {
                "loginEnabled": "true",
                "forcePasswordChange": "false",
                "loginName": dag_run.conf['file_data']['email_id'],
                "ssoName":  dag_run.conf['file_data']['email_id'],
                "password": null,
                "enabledAuthenticationTypeUris": ["urn:replicon:user-authentication-type:sso"],
                "userSSONameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name"
                }

    return null

def _get_two_date_diff(effective_date, user_start_date, today):
    if effective_date:
        return convert_json_date_to_date(today) - convert_json_date_to_date(effective_date)
    return convert_json_date_to_date(today) -  convert_json_date_to_date(user_start_date)

def _get_current_payrule_schedule_timesheetPeriod(payrule_schedule_details, user_start_date):
    current_effective_payrule = None
    # as an identifier to process very 1st record
    #! can be optimized
    current_min_day_diff = "*"
    today= get_todays_date_for_timezone_in_json()
    # iter from 2nd item as we have considered the 1st record as current
    for _schedule in payrule_schedule_details:
        day_diff_cnt = _get_two_date_diff(_schedule['effectiveDate'], user_start_date, today)

        # ignore the future ones
        if day_diff_cnt.days < 0:
            continue

        if current_min_day_diff=="*":
            current_effective_payrule = _schedule
            current_min_day_diff = day_diff_cnt
            continue

        if current_min_day_diff > day_diff_cnt:
            current_min_day_diff = day_diff_cnt
            current_effective_payrule = _schedule

    return current_effective_payrule

def is_ia_update_yes(dag_run):
    custom_fields = dag_run.conf['udfs']
    if dag_run.conf['file_data']['is_ia'] and dag_run.conf['file_data']['is_ia'] != rail.result("current_assigned_udf_values")['is_ia']:
            if dag_run.conf['mapper_data']['parent_company']=="COMPASS":
                return True
    return False
    

def _get_payrule_schedule_to_update(dag_run, _get_user_details):
    if dag_run.conf['mapper_data']['payrule']:
        current_payrule_schedule = _get_current_payrule_schedule_timesheetPeriod(_get_user_details['payRuleScriptSchedule'],
            _get_user_details['userDetails']['employmentDateRange']['startDate'])
        if not current_payrule_schedule or current_payrule_schedule['payRuleScript']['displayText'] != dag_run.conf['mapper_data']['payrule']:
            if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
                payrule_effective_date = datetime.strptime(dag_run.conf['work_week_date'], INPUT_DATE_FORMAT).date()
                return {
                    "scheduleEntries": [
                        {
                            "payRuleScript": {
                                "uri" : null,
                                "name": dag_run.conf['mapper_data']['payrule']
                            },
                            "effectiveDate": {
                                "day": payrule_effective_date.day,
                                "month": payrule_effective_date.month,
                                "year": payrule_effective_date.year
                            } if not is_ia_update_yes(dag_run) else ( get_replicon_date(dag_run.conf['file_data']['ia_start_date']) if dag_run.conf['file_data']['is_ia'] in ['1',1] 
                               else  get_replicon_date(dag_run.conf['file_data']['ia_end_date']))
                        }
                    ]
                }
    return null

def _get_shift_assignment_to_update(dag_run, user_details, config, exception_log):

    current_office_schedule = _get_current_payrule_schedule_timesheetPeriod(user_details['schedulePolicies'],
        user_details['userDetails']['employmentDateRange']['startDate'])
    mapper_shift_details = _get_shift_from_mapper(dag_run, config)

    if not mapper_shift_details:
        # to make sure it has a one element to avoid failure below
        mapper_shift_details = [{}]

    if dag_run.conf['file_data']['work_shift'] == "ES-Rotational Work shift":
        if not current_office_schedule or (current_office_schedule['scheduleTypeUri'] != mapper_shift_details[0].get('URI', '')):
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
                            "scheduleTypeUri": mapper_shift_details[0].get('URI', '')
                        },
                        "effectiveDate": dag_run.conf['json_formatted_dates']['work_shift_effective_date'] if not is_ia_update_yes(dag_run) else 
                            ( get_replicon_date(dag_run.conf['file_data']['ia_start_date']) if dag_run.conf['file_data']['is_ia'] in ['1',1] 
                               else  get_replicon_date(dag_run.conf['file_data']['ia_end_date']))
                    }
                ],
                "endDate": null
            }
        }
    else:
        if dag_run.conf['file_data']['work_shift']:
            if (not current_office_schedule) or (not current_office_schedule['officeSchedule']) or (current_office_schedule['officeSchedule']['displayText'] != dag_run.conf['file_data']['work_shift']):
                if dag_run.conf['schedule_uri']:
                    return {
                        "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementSchedule": [],
                        "updateScheduleOverDateRange": {
                            "replacementScheduleEntries": [
                                {
                                    "schedulePolicy": {
                                        "officeScheduleUri": null,
                                        "name": dag_run.conf['file_data']['work_shift'],
                                        "officeSchedule": {
                                            "officeScheduleUri": null,
                                            "name": dag_run.conf['file_data']['work_shift']
                                        },
                                        "scheduleTypeUri": dag_run.conf['mapper_data']['schedule_type_uri']
                                    },
                                    "effectiveDate": dag_run.conf['json_formatted_dates']['work_shift_effective_date'] if not is_ia_update_yes(dag_run) else 
                                        ( get_replicon_date(dag_run.conf['file_data']['ia_start_date']) if dag_run.conf['file_data']['is_ia'] in ['1',1] 
                                        else  get_replicon_date(dag_run.conf['file_data']['ia_end_date']))
                                }
                            ],
                            "endDate": null
                        }
                    }
                exception_log.append(f"""Office schedule {dag_run.conf['file_data']['work_shift']} not available in Replicon""")

    return null

def _get_time_entry_approval_path_name(dag_run):
    if dag_run.conf['mapper_data']['timeentry_approval_path_name']:
        current_timeentry_approval_path = rail.result("get_time_entry_approval_path_name")
        if not current_timeentry_approval_path or current_timeentry_approval_path['displayText'] != dag_run.conf['mapper_data']['timeentry_approval_path_name']:
            return {
                "uri": null,
                "name":  dag_run.conf['mapper_data']['timeentry_approval_path_name']
            }
    return null

def _can_update_punch_entry_policies(dag_run):
    if not dag_run.conf['mapper_data']['punch_entry_policy_name']:
        return False
    current_assigned_policies = rail.result("get_user_assigned_policy")
    return not bool(list(filter(lambda time_punch_policy: time_punch_policy['policySet']['name'] == dag_run.conf['mapper_data']['punch_entry_policy_name'],
                filter(lambda policy: policy['policyUri']=="urn:replicon:policy:time-punch", current_assigned_policies))))


def _get_timesheet_period_schedule_to_apply(dag_run, user_details):

    if dag_run.conf['mapper_data']['profile_status']== "enabled" and not user_details['timesheetPeriodSchedule']:
        if dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']:
            if convert_json_date_to_date(dag_run.conf['mapper_data']['timesheet_period_effective_date_json_format']) > convert_json_date_to_date(
                        dag_run.conf['json_formatted_dates']['hire_date']):
                return {
                    "userTimesheetPeriodScheduleModificationOptionUri":
                        "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
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
            if convert_json_date_to_date(dag_run.conf['json_formatted_dates']['hire_date']) > convert_json_date_to_date(
                        dag_run.conf['mapper_data']['timesheet_period_effective_date_json_format']):
                return {
                    "userTimesheetPeriodScheduleModificationOptionUri":
                        "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
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
            if not dag_run.conf['mapper_data']['timesheet_period_effective_date']:
                return {
                    "userTimesheetPeriodScheduleModificationOptionUri":
                        "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
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

    if user_details['timesheetPeriodSchedule']:
        current_timesheet_period = _get_current_payrule_schedule_timesheetPeriod(user_details['timesheetPeriodSchedule'],
            user_details['userDetails']['employmentDateRange']['startDate'])

        if dag_run.conf['mapper_data']['timesheet_period'] and dag_run.conf['mapper_data']['timesheet_period'] != \
            current_timesheet_period['timesheetPeriod']['displayText']:
            timesheet_effective_date = datetime.strptime(dag_run.conf['work_week_date'], INPUT_DATE_FORMAT).date()
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
                            "effectiveDate": {
                                "day": timesheet_effective_date.day,
                                "month": timesheet_effective_date.month,
                                "year": timesheet_effective_date.year
                            }  if not is_ia_update_yes(dag_run) else 
                                ( get_replicon_date(dag_run.conf['file_data']['ia_start_date']) if dag_run.conf['file_data']['is_ia'] in ['1',1] 
                                else  get_replicon_date(dag_run.conf['file_data']['ia_end_date']))
                        }
                    ],
                    "endDate": null
                }
            }
    return null

def _get_department_update_payload(dag_run, current_effective_grps):
    if dag_run.conf['file_data']['org_code'] and dag_run.conf['file_data']['org_code'] != current_effective_grps['department'].get('displayText', ''):
        work_week_eff_date = datetime.strptime(dag_run.conf['work_week_date'], INPUT_DATE_FORMAT).date()
        return {
            "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementDepartmentGroupSchedule": [],
            "updateDepartmentGroupScheduleOverDateRange": {
                "replacementDepartmentGroupScheduleEntries": [
                    {
                        "departmentGroup": {
                            "uri": dag_run.conf['organizational_unit_uri'],
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": {
                                "day": work_week_eff_date.day,
                                "month": work_week_eff_date.month,
                                "year": work_week_eff_date.year
                            } if not is_ia_update_yes(dag_run) else 
                                ( get_replicon_date(dag_run.conf['file_data']['ia_start_date']) if dag_run.conf['file_data']['is_ia'] in ['1',1] 
                                else  get_replicon_date(dag_run.conf['file_data']['ia_end_date']))
                    }
                ],
                "endDate": null
            }
        }

    return null

def _get_location_update_payload(dag_run, current_effective_grps):
    if dag_run.conf['location_uri'] and dag_run.conf['location_uri'] != current_effective_grps['location'].get('uri' ,''):
        return {
            "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementLocationSchedule": [],
            "updateLocationScheduleOverDateRange": {
                "replacementLocationScheduleEntries": [
                    {
                        "location": {
                            "uri": dag_run.conf['location_uri'],
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": dag_run.conf['json_formatted_dates']['location_effective_date'] if not is_ia_update_yes(dag_run) else 
                            ( get_replicon_date(dag_run.conf['file_data']['ia_start_date']) if dag_run.conf['file_data']['is_ia'] in ['1',1] 
                            else  get_replicon_date(dag_run.conf['file_data']['ia_end_date']))
                    }
                ],
                "endDate": null
            }
        }

    return null


def _get_cost_center_update_payload(dag_run, current_effective_grps):

    if dag_run.conf['file_data']['cost_center'] and dag_run.conf['file_data']['cost_center'] != current_effective_grps['costCenter'].get('displayText', ''):
        return {
            "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementCostCenterSchedule": [],
            "updateCostCenterScheduleOverDateRange": {
                "replacementCostCenterScheduleEntries": [
                    {
                        "costCenter": {
                            "uri": dag_run.conf['cost_center_uri'],
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": dag_run.conf['json_formatted_dates']['cost_center_effective_date']  if not is_ia_update_yes(dag_run) else 
                            ( get_replicon_date(dag_run.conf['file_data']['ia_start_date']) if dag_run.conf['file_data']['is_ia'] in ['1',1] 
                            else  get_replicon_date(dag_run.conf['file_data']['ia_end_date']))
                    }
                ],
                "endDate": null
            }
        }

    return null


def _get_division_update_payload(dag_run, current_effective_grps):
    if dag_run.conf['company_code_uri'] and dag_run.conf['company_code_uri'] != current_effective_grps['division'].get('uri' ,''):
        return {
            "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementDivisionSchedule": [],
            "updateDivisionScheduleOverDateRange": {
                "replacementDivisionScheduleEntries": [
                    {
                        "division": {
                            "uri": dag_run.conf['company_code_uri'] ,
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": (dag_run.conf['json_formatted_dates']['job_change_effective_date'] if dag_run.conf[
                            'file_data']['job_change_effective_date'] else dag_run.conf['json_formatted_dates']['cost_center_effective_date'])
                             if not is_ia_update_yes(dag_run) else 
                                ( get_replicon_date(dag_run.conf['file_data']['ia_start_date']) if dag_run.conf['file_data']['is_ia'] in ['1',1] 
                                else  get_replicon_date(dag_run.conf['file_data']['ia_end_date']))
                    }
                ],
                "endDate": null
            }
        }

    return null

def _get_employee_type_update_payload(dag_run, current_effective_grps):
    if dag_run.conf['employee_tye_uri'] and dag_run.conf['employee_tye_uri'] != current_effective_grps['employeeType'].get('uri', ''):
        return {
            "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementEmployeeTypeGroupSchedule": [],
            "updateEmployeeTypeGroupScheduleOverDateRange": {
                "replacementEmployeeTypeGroupScheduleEntries": [
                    {
                        "employeeTypeGroup": {
                            "uri": dag_run.conf['employee_tye_uri'],
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": dag_run.conf['json_formatted_dates']['employee_type_effective_date']  if not is_ia_update_yes(dag_run) else 
                            ( get_replicon_date(dag_run.conf['file_data']['ia_start_date']) if dag_run.conf['file_data']['is_ia'] in ['1',1] 
                            else  get_replicon_date(dag_run.conf['file_data']['ia_end_date']))
                    }
                ],
                "endDate": null
            }
        }

    return null


def _get_service_center_update_payload(dag_run, current_effective_grps):
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
                    "effectiveDate": dag_run.conf['json_formatted_dates']['work_week_date']  if not is_ia_update_yes(dag_run) else 
                        ( get_replicon_date(dag_run.conf['file_data']['ia_start_date']) if dag_run.conf['file_data']['is_ia'] in ['1',1] 
                        else  get_replicon_date(dag_run.conf['file_data']['ia_end_date']))
                }
                ],
                "endDate": null
            }
        }

    return null

def _get_timezone_update_payload(dag_run, user_details, logger):
    if dag_run.conf['mapper_data']['timezone_name']:
        if dag_run.conf['mapper_data']['timezone_uri'] != user_details['timeZone']['uri']:
            return {
                "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                "timezone": {
                    "uri": dag_run.conf['mapper_data']['timezone_uri'],
                    "IANAName": null
                }
            }
        return null
    logger.append(f"Timezone not defined in mapper for Location {dag_run.conf['file_data']['country']}")
    return null

def _get_work_week_update_payload(dag_run, profile_status_is_enabled):
    if profile_status_is_enabled:
        if dag_run.conf['mapper_data']['workweek_uri']:
            return {
                "workWeekStartDayUri": dag_run.conf['mapper_data']['workweek_uri']
            }
    return null

def _get_timeoff_approval_update_payload(dag_run, profile_status_is_enabled):
    if profile_status_is_enabled:
        if dag_run.conf['mapper_data']['timeoff_approval']:
            return {
                "uri": null,
                "name": dag_run.conf['mapper_data']['timeoff_approval']
            }
    return null

def _can_update_timesheet_template(dag_run, user_details, profile_status_is_enabled):
    if profile_status_is_enabled:
        if dag_run.conf['mapper_data']['timesheet_template_name']:
            timesheet_template = user_details['timesheetTemplate']['name'] if user_details['timesheetTemplate'] else ''
            if (not timesheet_template) or (timesheet_template != dag_run.conf['mapper_data']['timesheet_template_name']):
                return True
    return False

def _get_holiday_calendar_from_mapper(dag_run, config):
    country = dag_run.conf['file_data']['country']
    return list(filter(lambda row: row['Type'] == "Holiday Calendar" and\
                                row['Function'] == "Workday User Sync" and\
                                row['Country'] == country and\
                                row['Source'] == "IA"
                                ,config.DXC_WORKDAY_USER_SYNC_USER_MAPPER))

def _get_holiday_calendar_update_payload(dag_run, user_details, profile_status_is_enabled, exception_log, config):
    if profile_status_is_enabled:
        holiday_calendar = user_details['holidayCalendar'].get('displayText', '') if rail.result('get_user_details')['holidayCalendar'] else ''
        if dag_run.conf['mapper_data']['holiday_calender_name']:
            if dag_run.conf['mapper_data']['holiday_calender_name'] != holiday_calendar:
                if dag_run.conf['mapper_data']['holiday_calender_uri']:
                    return {
                        "holidayCalendar": {
                            "uri": dag_run.conf['mapper_data']['holiday_calender_uri'],
                            "name": null
                        }
                    }
                exception_log.append(f''''Holiday calendar "{dag_run.conf['mapper_data']['holiday_calender_name']}" not available in Replicon''')
        else:
            if is_ia_update_yes(dag_run) and holiday_calendar:
                mapper_holiday_calendar_details = _get_holiday_calendar_from_mapper(dag_run, config)
                return {
                        "holidayCalendar": {
                            "uri":  mapper_holiday_calendar_details[0].get('uri', ''),
                            "name": null
                        }
                    }

    return null

def _can_update_timeoff_template(dag_run, user_details, profile_status_is_enabled):
    if profile_status_is_enabled:
        if dag_run.conf['mapper_data']['timeoff_template']:
            timeoff_template = user_details['timeOffTemplate'].get('name', '') if user_details['timeOffTemplate'] else ''
            if (not user_details['timeOffTemplate'].get('name', '')) or (timeoff_template != dag_run.conf['mapper_data']['timeoff_template']):
                return True
    return False

def _get_policies_to_update_payload(dag_run, user_details, exception_log, profile_status_is_enabled):
    policies_to_add = []
    if _can_update_punch_entry_policies(dag_run):
        policies_to_add.append(dag_run.conf['punch_entry_policy_uri'])

    if _can_update_timesheet_template(dag_run, user_details, profile_status_is_enabled):
        if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
            if dag_run.conf['mapper_data']['timesheet_template_uri']:
                policies_to_add.append(dag_run.conf['mapper_data']['timesheet_template_uri'])
            else:
                exception_log.append(f"Timesheet template {dag_run.conf['mapper_data']['timesheet_template_name']} not available in Replicon")

    if _can_update_timeoff_template(dag_run, user_details, profile_status_is_enabled):
        if dag_run.conf['mapper_data']['timeoff_template_uri']:
            policies_to_add.append(dag_run.conf['mapper_data']['timeoff_template_uri'])
        else:
            exception_log.append(f"Timeoff template  {dag_run.conf['mapper_data']['timeoff_template']} not available in Replicon")

    return {
                "policySetUrisToAssign": policies_to_add,
                "policyUrisToRemovePolicySet": [],
                "policySetUrisToRemove": []
            } if policies_to_add else null

def _get_activities_update_payload(dag_run, user_details):
    if dag_run.conf['mapper_data']['activity_list']:
        activity_list = dag_run.conf['mapper_data']['activity_list']
        user_activities = user_details['assignedActivities']
        can_assign_activities = False
        if not user_activities:
            can_assign_activities = True
        for activity in user_activities:
            if activity['name'] not in activity_list:
                can_assign_activities = True
                break
        if can_assign_activities:
            return list(map(lambda _activity: {
                    "uri": null,
                    "name": _activity,
                }, activity_list))
    return null

def get_update_user_payload(dag_run, config):
    exceptions = []
    user_details = rail.result("get_user_details")
    current_user_groups = rail.result("get_effective_group_membership")
    profile_status_is_enabled = dag_run.conf['mapper_data']['profile_status'] == "enabled"
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
            "holidayCalendarToApply": _get_holiday_calendar_update_payload(dag_run, user_details, profile_status_is_enabled, exceptions, config),
            "holidayCalendarAssignmentsToApply": null,
            "schedulePolicyToApply": _get_shift_assignment_to_update(dag_run, user_details, config, exceptions),
            "locationScheduleToApply": _get_location_update_payload(dag_run, current_user_groups),
            "divisionScheduleToApply": _get_division_update_payload(dag_run, current_user_groups),
            "costCenterScheduleToApply": _get_cost_center_update_payload(dag_run, current_user_groups),
            "departmentGroupScheduleToApply": _get_department_update_payload(dag_run, current_user_groups),
            "employeeTypeGroupScheduleToApply": _get_employee_type_update_payload(dag_run, current_user_groups),
            "timesheetPeriodScheduleToApply": _get_timesheet_period_schedule_to_apply(dag_run, user_details),
            "serviceCenterScheduleToApply": _get_service_center_update_payload(dag_run, current_user_groups),
            "totalBusinessCostScheduleToApply": null,
            "permissionSetsToApply": null,
            "policySetsToApply": _get_policies_to_update_payload(dag_run, user_details, exceptions, profile_status_is_enabled),
            "policyDataAccessScopesToApply": null,
            "policyDataAccessScopesToApply2": null,
            "notificationPreferencesToApply": null,
            "timesheetPeriodTypeToApply": null,
            "timesheetApprovalPathToApply": {
                "uri": null,
                "name": dag_run.conf['mapper_data']['timesheet_approval_path']
            },
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
            "securitySettingsToApply": _get_update_for_security_settings(dag_run, config, user_details),
            "supervisorsToApply": null,
            "supervisorsModifications": null,
            "payrollRatesToApply": null,
            "payrollRatesModifications": null,
            "overtimeRulesToApply": null,
            "overtimeRulesModifications": null,
            "customFieldValuesToApply": _get_custom_fields_to_assign(dag_run, "update"),
            "departmentToApply": null,
            "employeeTypeToApply": null,
            "userDetailsToApply": {
                "firstName": dag_run.conf['file_data']['first_name'] if _can_update_first_name(dag_run, user_details) else null,
                "lastName": dag_run.conf['file_data']['last_name'] if _can_update_last_name(dag_run, user_details) else null,
                "emailAddress": {
                    "emailAddress": dag_run.conf['file_data']['email_id']
                } if _can_update_email(dag_run, user_details, config) or _can_update_email(dag_run, user_details, config, False) else null,
                "language": null,
                "employmentDateRange": null,
                "employmentStartDate": null,
                "employmentEndDate": null,
                "employeeId": null,
                "displayNameParameter": _get_display_name_to_assign(dag_run) if _can_update_display_name(dag_run, user_details, config) else null
            },
            "payRulesToApply": null,
            "payRulesScheduleModifications": _get_payrule_schedule_to_update(dag_run, user_details),
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
            "workAuthorizationApprovalPathToApply": null,
            "displayNameFormatSettingsToApply": null,
            "timePunchTimeZoneDisplayOptionToApply": null,
            "defaultTimesheetToDisplayOptionToApply": null,
            "reportSettingsToApply": null,
            "timeOffBalancePayoutApprovalPathToApply": null,
            "workCompliancePolicyAssignmentScheduleToApply": null
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
        }


    rail.set_result(key="exception_log", val=exceptions)
    return payload

def validate_log_for_ia_exception_applicable(dag_run):
    if not dag_run.conf['file_data']['ia_start_date'] and dag_run.conf['file_data']['is_ia'] in ['1',1]:
        return True
    if not dag_run.conf['file_data']['ia_end_date'] and dag_run.conf['file_data']['is_ia'] in ['0',0]:
        return True
    if dag_run.conf['file_data']['is_ia'] in ['1',1] and (datetime.strptime(dag_run.conf['file_data']['ia_start_date'], INPUT_DATE_FORMAT).date() < \
        pendulum.now("America/Los_Angeles").subtract(days=5).date()):
        return True
    if dag_run.conf['file_data']['is_ia'] in ['0',0] and (datetime.strptime(dag_run.conf['file_data']['ia_end_date'], INPUT_DATE_FORMAT).date() < \
        pendulum.now("America/Los_Angeles").subtract(days=5).date()):
        return True
    return False


def get_ia_exception_message(dag_run):
    if not dag_run.conf['file_data']['ia_start_date'] and dag_run.conf['file_data']['is_ia'] in ['1',1]:
        return "User processing skipped as IAStart date not available for IA=1"
    if not dag_run.conf['file_data']['ia_end_date'] and dag_run.conf['file_data']['is_ia'] in ['0',0]:
        return "User processing skipped as IAEnd date not available for IA=0"
    if dag_run.conf['file_data']['is_ia'] in ['1',1] and (datetime.strptime(dag_run.conf['file_data']['ia_start_date'], INPUT_DATE_FORMAT).date() < \
        pendulum.now("America/Los_Angeles").subtract(days=5).date()):
        return "User processing skipped as IAStart date in past for IA=1"
    if dag_run.conf['file_data']['is_ia'] in ['0',0] and (datetime.strptime(dag_run.conf['file_data']['ia_end_date'], INPUT_DATE_FORMAT).date() < \
        pendulum.now("America/Los_Angeles").subtract(days=5).date()):
        return "User processing skipped as IAEnd date in past for IA=0"
    return null
