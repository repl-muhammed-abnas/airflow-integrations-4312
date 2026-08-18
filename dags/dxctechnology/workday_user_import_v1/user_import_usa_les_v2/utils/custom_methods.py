from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import rail
from airflow.exceptions import AirflowException
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import convert_json_date_to_date, compare_if_two_json_dates_are_same as _compare_if_two_json_dates_are_same
from dxctechnology.workday_user_import_v1.user_import.common_utils.request_payload import get_todays_date_in_json, get_psa_udf_value
from dxctechnology.workday_user_import_v1.user_import_usa_les_v2.utils.request_payload import _add_custom_field

nil = null = None

INPUT_DATE_FORMAT = "%Y-%d-%m"
null = None

def get_todays_date_in_json():
    today = datetime.now()
    return {
        "day": today.day,
        "month": today.month,
        "year": today.year
    }

def get_replicon_date(date_str, return_format= "dict", _date_format= INPUT_DATE_FORMAT):
    # Return None if date_str is empty or None
    if not date_str:
        return None
    _date = datetime.strptime(date_str, _date_format)
    if return_format == "date":
        return _date
    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }

def map_mapper_replicon_timeoffs(dag_run):
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

def is_user_disabled_for_non_go_live_country_test(dag_run, user_details_task_id):
    user_details = rail.result(user_details_task_id)
    return user_details['userDetails']['isEnabled'] is True \
        and dag_run.conf['mapper_data']['profile_status'] != "enabled"

def user_has_no_project_management_permission_test(dag_run=None):
    return not bool(rail.find_first_by_attr_and_get_attr(
        rail.result("get_assigned_permission_for_user"), "policyUri", "urn:replicon:policy:project-management"
    ))

def is_division_gsap_test():
    if rail.result("get_effective_group_membership")['parent_division']:
        return rail.result("get_effective_group_membership")['parent_division']['division']['displayText'] == "GSAP"
    return False

def user_does_not_have_admin_and_payroll_permission_test():
    return (not bool(rail.find_first_by_attr_and_get_attr(
        rail.result("get_assigned_permission_for_user"), "policyUri", "urn:replicon:policy:administration"
    ))) and ( not bool(rail.find_first_by_attr_and_get_attr(
        rail.result("get_assigned_permission_for_user"), "policyUri", "urn:replicon:policy:payroll-management"
    )))

def is_user_on_leave_test(dag_run):
    return dag_run.conf['file_data']['on_leave'] in [1, '1']

def is_user_for_long_leave_disable_test(dag_run):
    user_details = rail.result('get_user_details')
    return user_details['userDetails']['isEnabled'] is True \
        and is_user_on_leave_test(dag_run)

def should_disabled_user_test(dag_run):
    user_details = rail.result('get_user_details')
    return user_details['userDetails']['isEnabled'] is True and\
        dag_run.conf['replicon_field'] in [False , 'false']

def is_end_date_less_than_today_test(dag_run):
    return get_replicon_date(dag_run.conf['file_data']['term_date'], "date").date() < convert_json_date_to_date(get_todays_date_in_json())


def get_is_user_already_disabled_and_on_leave_test(dag_run):
    user_details = rail.result('get_user_details')
    if user_details['userDetails']['isEnabled'] not in ['true', True, 'True']:
        if is_user_on_leave_test(dag_run):
            if user_has_no_project_management_permission_test(dag_run):
                return True
    return False


def get_disable_user_log_message(dag_run):
    if is_user_disabled_for_non_go_live_country_test(dag_run, "get_user_details"):
        if user_has_no_project_management_permission_test(dag_run):
            if not bool(rail.result('get_direct_reports_for_user')):
                if is_division_gsap_test():
                    if dag_run.conf['file_data']['term_date']:
                        return {
                            "Jobid": "",
                            "Userid": dag_run.conf['file_data']["emp_id"],
                            "Email": dag_run.conf['file_data']["email_id"],
                            "Action": 'Update',
                            "Status": "Success",
                            "Details": '''User disabled in Replicon as the required user's company code and country not in allowed status. User's company code is GSAP. User has an end date in the feed file'''
                        }
                    else:
                        if user_does_not_have_admin_and_payroll_permission_test():
                            return {
                            "Jobid": "",
                            "Userid": dag_run.conf['file_data']["emp_id"],
                            "Email": dag_run.conf['file_data']["email_id"],
                            "Action": 'Update',
                            "Status": "Success",
                            "Details": '''User disabled in Replicon as the required user's company code and country not in allowed status. User's company code is GSAP. User does not have payroll or admin permission'''
                        }
                else:
                    return {
                            "Jobid": "",
                            "Userid": dag_run.conf['file_data']["emp_id"],
                            "Email": dag_run.conf['file_data']["email_id"],
                            "Action": 'Update',
                            "Status": "Success",
                            "Details": '''User disabled in Replicon as the required user's company code and country not in allowed status'''
                        }
    
    if is_user_for_long_leave_disable_test(dag_run):
        return  {
            "Jobid": "",
            "Userid": dag_run.conf['file_data']["emp_id"],
            "Email": dag_run.conf['file_data']["email_id"],
            "Action": 'Update',
            "Status": "Success",
            "Details": '''User disabled in Replicon as "On Leave" is set to 1 for user in feed file'''
        }
    if should_disabled_user_test(dag_run) and is_end_date_less_than_today_test(dag_run):
        return {
            "Jobid": "",
            "Userid": dag_run.conf['file_data']["emp_id"],
            "Email": dag_run.conf['file_data']["email_id"],
            "Action": 'Update',
            "Status": "Success",
            "Details": '''User disabled in Replicon as "status" is set to 0 for user in feed file'''
        }
    raise AirflowException("Disabled log Task was executed without passing any validation")

def can_update_user_end_date_test(dag_run):
    return (bool(dag_run.conf['file_data']['term_date'])
            and (not bool(rail.result("get_user_details")['userDetails']['employmentDateRange'].get('endDate'))))

def is_user_disabled_and_replicon_field_false_test(dag_run):
    user_details = rail.result('get_user_details')
    return user_details['userDetails']['isEnabled'] is False \
            and dag_run.conf['replicon_field'] in ['false', False]

def get_error_message_for_long_leave_or_user_disabled_with_replicon_field_false(dag_run):
    if not is_user_disabled_and_replicon_field_false_test(dag_run):
        return {
            "Jobid": "",
            "Userid": dag_run.conf['file_data']["emp_id"],
            "Email": dag_run.conf['file_data']["email_id"],
            "Action": 'Update',
            "Status": "Skipped",
            "Details": 'User not enabled in Replicon as "On Leave" is set to 1 for user in feed file'
        }
    return {
        "Jobid": "",
        "Userid": dag_run.conf['file_data']["emp_id"],
        "Email": dag_run.conf['file_data']["email_id"],
        "Action": 'Update',
        "Status": "Skipped",
        "Details": 'User already disabled in Replicon'
    }

def is_user_rehire_test(dag_run):
    user_details = rail.result('get_user_details')
    return user_details['userDetails']['isEnabled'] is False \
            and dag_run.conf['replicon_field'] in ['true', True] \
                and dag_run.conf['mapper_data']['profile_status'] == "enabled"

def can_update_user_start_date_test(dag_run):
    user_start_date = rail.result("get_user_details")['userDetails']['employmentDateRange'].get('startDate', False)
    if not user_start_date:
        return True #bool(dag_run.conf['file_data']['hire_date'])
    return dag_run.conf['file_data']['hire_date'] != f"{user_start_date['year']}-{user_start_date['day']}-{user_start_date['month']}"

def _get_timezone_update_payload(dag_run, user_details, logger):
    if dag_run.conf['timezone']['timezone']:
        if dag_run.conf['timezone']['timezone_uri'] != user_details['timeZone']['uri']:
            return {
                "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                "timezone": {
                    "uri": dag_run.conf['timezone']['timezone_uri'],
                    "IANAName": null
                }
            }
        return null
    else:
        logger.append(f"Timezone not defined in mapper for Location {dag_run.conf['file_data']['country']}")
    return null

def _get_work_week_update_payload(dag_run, profile_status_is_enabled):
    if profile_status_is_enabled:
        if dag_run.conf['mapper_data']['work_week_uri']:
            return {
                "workWeekStartDayUri": dag_run.conf['mapper_data']['work_week_uri']
            }
    return null

def _get_holiday_calendar_update_payload(dag_run, user_details, profile_status_is_enabled, exception_log, isia_update_done, mapper):
    if profile_status_is_enabled:
        holiday_calendar = user_details['holidayCalendar'].get('displayText', '') if rail.result('get_user_details')['holidayCalendar'] else ''
        if dag_run.conf['holiday_calendar']['holiday_calendar']:
            if dag_run.conf['holiday_calendar']['holiday_calendar'] != holiday_calendar:
                if dag_run.conf['holiday_calendar']['holiday_calendar_uri']:
                    return {
                        "holidayCalendar": {
                            "uri": dag_run.conf['mapper_data']['holiday_calendar_uri'],
                            "name": null
                        }
                    }
                exception_log.append(f''''Holiday calendar "{dag_run.conf['mapper_data']['holiday_calendar']}" not available in Replicon''')
        else:
            if isia_update_done and holiday_calendar:
                mapper_holiday_calendar = list(filter(lambda item: item['Type'] == "Holiday Calendar"\
                                    and item['Function'] == "Workday User Sync"\
                                    and item['Country'] == dag_run.conf['file_data']['country']\
                                    and item['Source'] == "IA", mapper))
                return {
                    "holidayCalendar": {
                        "uri": null,
                        "name": mapper_holiday_calendar[0]['Value']
                    }
                }
    return null

def _can_update_first_name(dag_run, user_details):
    if dag_run.conf['file_data']['first_name']:
        return dag_run.conf['file_data']['first_name'] != user_details['userDetails']['firstName']
    return False

def _can_update_last_name(dag_run, user_details):
    if dag_run.conf['file_data']['last_name']:
        return dag_run.conf['file_data']['last_name'] != user_details['userDetails']['lastName']
    return False

# this will be called twice in update email and update displayValue
def _can_update_email(dag_run, user_details, config, login_name_check=True):
    if config.instance in ["prod", 'trial']:
        if dag_run.conf['file_data']['email_id']:
            if login_name_check:
                return dag_run.conf['file_data']['email_id'] != user_details['securityConfiguration']['loginName']
            if not login_name_check:
                return (not user_details['userDetails']['emailAddress'])
    return False

def _can_update_display_name(dag_run, user_details, config):
    return _can_update_first_name(dag_run, user_details) or _can_update_last_name(dag_run, user_details)\
          or _can_update_email(dag_run, user_details, config, True) or _can_update_email(dag_run, user_details, config, False)

def _get_display_name_to_assign(dag_run):
    return {
        "displayName": f"""{dag_run.conf['file_data']['last_name']},{dag_run.conf['file_data']['first_name']} {dag_run.conf['file_data']['emp_id']} {dag_run.conf['file_data']['email_id']}"""
        }

def _get_two_date_diff(effective_date, user_start_date, today):
    if effective_date:
        return convert_json_date_to_date(today) - convert_json_date_to_date(effective_date)
    return convert_json_date_to_date(today) -  convert_json_date_to_date(user_start_date)

def _get_current_payrule_schedule_timesheetPeriod(payrule_schedule_details, user_start_date):
    current_effective_payrule = None
    # as an identifier to process very 1st record
    #! can be optimized
    current_min_day_diff = "*"
    today= get_todays_date_in_json()
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

def _get_shift_from_mapper(dag_run, config):
    country = dag_run.conf['file_data']['country']
    source = dag_run.conf['file_data']['parent_company']
    psg = dag_run.conf['mapper_data']['psg']
    emp_group_code = dag_run.conf['file_data']['emp_group_code']
    emp_subgroup_code = dag_run.conf['file_data']['emp_subgroup_code']
    sub_area_code = dag_run.conf['file_data']['sub_area_code']
    return list(filter(lambda row: row['Type'] == "Schedule Type" and\
                                row['Function'] == "Workday User Sync" and\
                                row['Country'] == country and\
                                row['personnelsubarea'] == psg and\
                                row['employeegroup'] == emp_group_code and\
                                row['employeesubgroup'] == emp_subgroup_code and\
                                row['status'] == sub_area_code and\
                                row['Source'] == source,config.MAPPER))


def _get_shift_assignment_to_update(dag_run, user_details, config, exception_log, effective_date):
    current_office_schedule = _get_current_payrule_schedule_timesheetPeriod(user_details['schedulePolicies'], user_details['userDetails']['employmentDateRange']['startDate'])
    mapper_shift_details = _get_shift_from_mapper(dag_run, config)
    if mapper_shift_details:
        if not current_office_schedule or (current_office_schedule['scheduleTypeUri'] != mapper_shift_details[0]['URI']):
            return {
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementSchedule": [],
                "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": dag_run.conf['schedule_data']['office_schedule'],
                                "officeSchedule": null,
                                "scheduleTypeUri": mapper_shift_details[0]['URI']
                            },
                            "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['work_shift_effective_date']
                        }
                    ],
                    "endDate": null
                }
            }
    
    else:
        if dag_run.conf['schedule_data']['work_schedule']:
            if (not current_office_schedule) or (not current_office_schedule['officeSchedule']) or (current_office_schedule['officeSchedule']['displayText'] != dag_run.conf['schedule_data']['work_schedule']):
                if dag_run.conf['schedule_data']['schedule_uri']:
                    return {
                        "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementSchedule": [],
                        "updateScheduleOverDateRange": {
                            "replacementScheduleEntries": [
                                {
                                    "schedulePolicy": {
                                        "officeScheduleUri": null,
                                        "name": dag_run.conf['schedule_data']['work_schedule'],
                                        "officeSchedule": {
                                            "officeScheduleUri": null,
                                            "name": dag_run.conf['schedule_data']['work_schedule']
                                        },
                                        "scheduleTypeUri": dag_run.conf['mapper_data']['schedule_type_uri']
                                    },
                                    "effectiveDate": effective_date if effective_date else dag_run.conf['json_formatted_dates']['work_shift_effective_date']
                                }
                            ],
                            "endDate": null
                        }
                    }
                else:
                    exception_log.append(f"""Office schedule {dag_run.conf['schedule_data']['work_schedule']} not available in Replicon""")

    return null

def _get_location_update_payload(dag_run, current_effective_grps, effective_date):
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

def _get_employee_type_update_payload(dag_run, current_effective_grps, effective_date):
    emp_type_uri = dag_run.conf['groups']['employee_type']['uri'].get('uri', '')
    if emp_type_uri and (emp_type_uri != current_effective_grps['employeeType'].get('uri', '')):
        return {
            "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
            "replacementEmployeeTypeGroupSchedule": [],
            "updateEmployeeTypeGroupScheduleOverDateRange": {
                "replacementEmployeeTypeGroupScheduleEntries": [
                    {
                        "employeeTypeGroup": {
                            "uri": emp_type_uri,
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
                        "effectiveDate": effective_date if effective_date else get_todays_date_in_json()
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
                        "effectiveDate": effective_date if effective_date else get_todays_date_in_json()
                    }
                ],
                "endDate": null
            }
        }
    
    return null

def _get_custom_fields_payload(uri, txt_value=null, date_value=null, drop_down_value_name=null, drop_down_value_uri=null, number_value=null):
    return {
        "customField": {
            "uri": uri,
            "name": null,
            "groupUri": null
        },
        # blank here means removing the value from the UDF for that user
        "text": txt_value if txt_value != "blank" else null,
        "date": date_value,
        "dropDownOption": {
            "uri": drop_down_value_uri,
            "name": drop_down_value_name
            } if drop_down_value_name or drop_down_value_uri else null,
        "number": number_value
    }

def _update_txt_udf(dag_run, input_field_name, udf_display_text_value, udf_key_name, custom_fields_payload, current_custom_fields_values):
    # this field is derived by mapper
    if input_field_name == 'termination_reason_code':
        input_data = dag_run.conf['mapper_data']['termination_reason_code']
    elif input_field_name=="job_level" and dag_run.conf['file_data']['country'] == "Canada":
        input_data = f"H{dag_run.conf['file_data']['job_level']}"
    elif isinstance(input_field_name, list):
        input_data = rail.smartjoin_by_delim([dag_run.conf['file_data'][field_name] for field_name in input_field_name], separator='|')
    else:
        input_data = dag_run.conf['file_data'][input_field_name]
    if input_data:
        if input_data != rail.find_first_by_attr_and_get_attr(
            current_custom_fields_values, "customField.displayText", udf_display_text_value, 'text', default=""):
            custom_fields_payload.append(
                _get_custom_fields_payload(uri=dag_run.conf['udfs'][udf_key_name].get('uri'), txt_value=input_data))
            if input_field_name == "management_lvl":
                if dag_run.conf['file_data']['management_lvl'] in ['L1', 'L2']:
                    return True
                return False

def _get_work_shift_value(work_shift):
    if work_shift.startswith("BPSOT"):
        return "BPSOT"
    if work_shift.startswith("BPS"):
        return "BPS"
    return work_shift

def _update_date_udf(dag_run, input_field_name, json_formatted_date_field_name, udf_display_text_value, udf_key_name, custom_fields_payload, current_custom_fields_values):
    if dag_run.conf['file_data'][input_field_name]:
        if _compare_if_two_json_dates_are_same(date_1=dag_run.conf['json_formatted_dates'][json_formatted_date_field_name],
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


def _update_custom_fields_for_user(dag_run):
    current_custom_fields_values = rail.result("get_user_details")['userDetails']['customFieldValues']
    custom_fields_payload = []
    current_user_groups = rail.result("get_effective_group_membership")
    isia_update_done = False

    if dag_run.conf['file_data']['is_ia']:
        if dag_run.conf['file_data']['is_ia'] != rail.find_first_by_attr_and_get_attr(
            current_custom_fields_values, "customField.displayText", 'International Assignee', 'text', default="") and dag_run.conf['file_data']['parent_company']=="COMPASS":
            _update_txt_udf(dag_run, 'is_ia', 'International Assignee', 'international_assignee', custom_fields_payload, current_custom_fields_values)
            if dag_run.conf['file_data']['is_ia'] in [1,'1']:
                custom_fields_payload.append(
                    _get_custom_fields_payload(
                        uri=dag_run.conf['udfs']['international_assignee_start_date'].get('uri'),
                        date_value=dag_run.conf['json_formatted_dates']['ia_start_date']
                    )
                )
            if dag_run.conf['file_data']['is_ia'] in [0,'0']:
                ia_end_date = convert_json_date_to_date(dag_run.conf['json_formatted_dates']['ia_end_date']) + relativedelta(days=1)
                custom_fields_payload.append(
                    _get_custom_fields_payload(
                        uri=dag_run.conf['udfs']['international_assignee_start_date'].get('uri'),
                        date_value={
                            "day": ia_end_date.day,
                            "month": ia_end_date.month,
                            "year": ia_end_date.year
                        }
                    )
                )
            isia_update_done = True



    if dag_run.conf['file_data']['assignment_type'] and dag_run.conf['file_data']['assignment_type'] != rail.find_first_by_attr_and_get_attr(
            current_custom_fields_values, "customField.displayText", "assignment_type", 'text', default=""):
        custom_fields_payload.append(_get_custom_fields_payload(
            uri=dag_run.conf['udfs']['assignment_type'].get('uri'),
            txt_value=dag_run.conf['file_data']['assignment_type']
            )
        )

    if ((not dag_run.conf['file_data']['assignment_type']) and rail.find_first_by_attr_and_get_attr(
            current_custom_fields_values, "customField.displayText", "assignment_type", 'text', default=None)):
        custom_fields_payload.append(_get_custom_fields_payload(
            uri=dag_run.conf['udfs']['assignment_type'].get('uri'),
            txt_value="blank"
            )
        )


    _update_txt_udf(dag_run, 'perner_id', 'IA PERNER ID', 'ia_perner_id', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'gender', 'Gender', 'gender', custom_fields_payload, current_custom_fields_values)
    can_update_notifications_settings = _update_txt_udf(dag_run, 'management_lvl', 'Management Level', 'management_level', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'on_leave', 'On Leave', 'on_leave', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'area_code', 'Personnel Area Code', 'personnel_area_code', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'area_name', 'Personnel Area Description', 'personnel_area_name', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'job_level', 'Job Activity Type', 'job_level', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'fte', 'FTE', 'fte', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'fte_pct', 'FTE %', 'ftepct', custom_fields_payload, current_custom_fields_values)
    _update_date_udf(dag_run, 'service_date', 'service_date', 'Continuous Service Date', 'service_date', custom_fields_payload, current_custom_fields_values)

    get_psa_udf_value(
            dag_run = dag_run,
            current_custom_fields_values = current_custom_fields_values, 
            current_user_groups = current_user_groups, 
            custom_fields_payload = custom_fields_payload, 
            _get_custom_fields_payload = _add_custom_field, 
            _get_cost_center_update_payload = _get_cost_center_update_payload, 
            _get_department_update_payload = _get_department_update_payload, 
            caller= "update")

    if dag_run.conf['file_data']['is_ia']:
        if dag_run.conf['file_data']['is_ia'] != rail.find_first_by_attr_and_get_attr(
            current_custom_fields_values, "customField.displayText", 'International Assignee', 'text', default="") and dag_run.conf['file_data']['parent_company']=="C1":
            _update_txt_udf(dag_run, 'is_ia', 'International Assignee', 'international_assignee', custom_fields_payload, current_custom_fields_values)
            if ((not dag_run.conf['file_data']['ia_start_date']) and dag_run.conf['file_data']['is_ia'] in [1, '1']):
                custom_fields_payload.append(
                        _get_custom_fields_payload(uri=dag_run.conf['udfs']['international_assignee_start_date'].get('uri'), 
                                                    date_value=get_todays_date_in_json()))

    if isia_update_done is False:
        _update_date_udf(dag_run, 'ia_start_date', 'ia_start_date', 'International assignee start date', 'international_assignee_start_date', custom_fields_payload, current_custom_fields_values)
        _update_date_udf(dag_run, 'ia_end_date', 'ia_end_date', 'International assignee end date', 'international_assignee_end_date', custom_fields_payload, current_custom_fields_values)

    # work_shift:str = dag_run.conf["file_data"]['work_shift']
    # if work_shift:
    #     _update_drop_down_udf(dag_run, "NA", "Employee Group", custom_fields_payload, current_custom_fields_values, _get_work_shift_value(work_shift))
    _update_txt_udf(dag_run, 'work_shift', 'Work Shift', 'work_shift', custom_fields_payload, current_custom_fields_values)
    _update_date_udf(dag_run, 'dob', 'date_of_birth', 'Date of Birth', 'date_of_birth', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'middle_name', 'Middle Name', 'middle_name', custom_fields_payload, current_custom_fields_values)
    _update_drop_down_udf(dag_run, 'time_type', 'Time Type', 'time_type', custom_fields_payload, current_custom_fields_values)

    rail.set_result(key="can_update_notifications_settings", val=can_update_notifications_settings)

    return custom_fields_payload, isia_update_done

def _get_effective_date_based_on_work_week(work_week, work_week_starts_with_check:list, return_as_dict:bool = True):
    is_start_with_saturday = work_week.lower().split(" ")[0] == "saturday"
    today = datetime.now()
    result_date = None
    
    if "saturday" in work_week_starts_with_check and is_start_with_saturday:
        # if today is saturday consider today as effective date
        if today.weekday() == 5:
            result_date = today
        # for sunday we have to remove 1
        # for other days except for saturday we have to add 2
        # Monday= 0, Tuesday=1, .... Sunday=6
        elif today.weekday() == 6:
            result_date = today - timedelta(days=1)
        else:
            result_date = today - timedelta(days=today.weekday()+2)
    elif "sunday" in work_week_starts_with_check and is_start_with_saturday:
        # if today is saturday consider today as effective date
        if today.weekday() == 6:
            result_date = today
        # for monday we have to remove 1
        # for other days except for saturday we have to add 1
        # Monday= 0, Tuesday=1, .... Sunday=6
        elif today.weekday() == 0:
            result_date = today - timedelta(days=1)
        else:
            result_date = today - timedelta(days=today.weekday()+1)
    else:
        # if today is monday consider today as effective date
        if today.weekday() == 0:
            result_date = today
        else:
            # Get the last immediate monday as effective date
            result_date = today - timedelta(days=today.weekday())
    
    # Return based on requested format
    if return_as_dict:
        return {
            "day": result_date.day,
            "month": result_date.month,
            "year": result_date.year
        }
    return result_date

def _get_timesheet_period_schedule_to_apply(dag_run, user_details, effective_date):
    if user_details['timesheetPeriodSchedule']:
        current_timesheet_period = _get_current_payrule_schedule_timesheetPeriod(user_details['timesheetPeriodSchedule'],
                                                                                 user_details['userDetails']['employmentDateRange']['startDate'])

        if dag_run.conf['mapper_data']['timesheet_period'] and dag_run.conf['mapper_data']['timesheet_period'] != current_timesheet_period['timesheetPeriod']['displayText']:
            timesheet_effective_date = _get_effective_date_based_on_work_week(dag_run.conf['mapper_data']['work_week'], ['saturday', 'sunday'])
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
                            "effectiveDate": effective_date if effective_date else timesheet_effective_date
                        }
                    ],
                    "endDate": null
                }
            }
    return null


def _get_timesheet_period_to_apply_profile_status_enabled(dag_run, user_details):
    # user_details['timesheetPeriodSchedule'] will be blank if there is no value assigned to the user
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
            if convert_json_date_to_date(dag_run.conf['json_formatted_dates']['hire_date']) > convert_json_date_to_date(
                        dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']):
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
            if not dag_run.conf['json_formatted_dates']['timesheet_period_effective_date']:
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
    
    return null


def _get_timesheetperiod_update_payload(dag_run, user_details, effective_date):
    if not user_details['timesheetPeriodSchedule'] and dag_run.conf['mapper_data']['profile_status'].lower() == "enabled":
         return _get_timesheet_period_to_apply_profile_status_enabled(dag_run, user_details)
    return _get_timesheet_period_schedule_to_apply(dag_run, user_details, effective_date)

def _can_update_timesheet_template(dag_run, user_details):
    if dag_run.conf['mapper_data']['timesheet_template']:
        timesheet_template = user_details['timesheetTemplate']['name'] if user_details['timesheetTemplate'] else ''
        if (not timesheet_template) or (timesheet_template != dag_run.conf['mapper_data']['timesheet_template']):
            return True
    return False

def _can_update_timeoff_template(dag_run, user_details):
    if dag_run.conf['mapper_data']['timeoff_template']:
        timeoff_template = user_details['timeOffTemplate'].get('name', '') if user_details['timeOffTemplate'] else ''
        if (not user_details['timeOffTemplate'].get('name', '')) or (timeoff_template != dag_run.conf['mapper_data']['timeoff_template']):
            return True
    return False

def _can_update_punch_entry_policies(dag_run):
    if not  dag_run.conf['policy_sets'].get('punch_entry_policy', {}).get('name', ''):
        return False
    current_assigned_policies = rail.result("get_user_assigned_policy")
    return not bool(list(filter(lambda time_punch_policy: time_punch_policy['policySet']['name'] == dag_run.conf['policy_sets']['punch_entry_policy'].get('name', ''), 
                filter(lambda policy: policy['policyUri']=="urn:replicon:policy:time-punch", current_assigned_policies))))


def _get_policies_to_update_payload(dag_run, user_details, exception_log):
    policies_to_add = []
    if _can_update_punch_entry_policies(dag_run):
        policies_to_add.append(dag_run.conf['policy_sets']['punch_entry_policy']['uri'])

    if _can_update_timesheet_template(dag_run, user_details):
        if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']: 
            if dag_run.conf['policy_sets']['timesheet_template'].get('uri'):
                policies_to_add.append(dag_run.conf['policy_sets']['timesheet_template']['uri'])
            else:
                exception_log.append(f"Timesheet template {dag_run.conf['mapper_data']['timesheet_template']} not available in Replicon")

    if _can_update_timeoff_template(dag_run, user_details):
        if dag_run.conf['policy_sets']['timeoff_template'].get('uri'):
            policies_to_add.append(dag_run.conf['policy_sets']['timeoff_template']['uri'])
        else:
            exception_log.append(f"Timesheet template {dag_run.conf['mapper_data']['timesheet_template']} not available in Replicon")

    return {
        "policySetUrisToAssign": policies_to_add,
        "policyUrisToRemovePolicySet": [],
        "policySetUrisToRemove": []
    } if policies_to_add else null


def _get_time_entry_approval_path_name(dag_run):
    current_timeentry_approval_path = rail.result("get_time_entry_approval_path")
    if not current_timeentry_approval_path or current_timeentry_approval_path['displayText'] != dag_run.conf['mapper_data']['timeentry_approval_path_name']:
        return {
            "uri": null,
            "name":  dag_run.conf['mapper_data']['timeentry_approval_path_name']
        }

    return null

def _get_activities_update_payload(dag_run, user_details):
    if dag_run.conf['activities']['activity']:
        activity_list = dag_run.conf['activities']['activity'].split('|')
        user_activities = user_details['assignedActivities']
        can_assign_activities = False
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

def _get_timeoff_approval_update_payload(dag_run, profile_status_is_enabled):
    if profile_status_is_enabled:
        if dag_run.conf['mapper_data']['timeoff_approval']:
            return {
                "uri": null,
                "name": dag_run.conf['mapper_data']['timeoff_approval']
            }
    return null

def _get_payrule_schedule_to_update(dag_run, _get_user_details, effective_date):
    if dag_run.conf['payrule']['payrule']:
        current_payrule_schedule = _get_current_payrule_schedule_timesheetPeriod(_get_user_details['payRuleScriptSchedule'], _get_user_details['userDetails']['employmentDateRange']['startDate'])
        if not current_payrule_schedule or current_payrule_schedule['payRuleScript']['displayText'] != dag_run.conf['payrule']['payrule']:
            if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
                payrule_effective_date = _get_effective_date_based_on_work_week(dag_run.conf['mapper_data']['work_week'], ['saturday'])
                return {
                    "scheduleEntries": [
                        {
                            "payRuleScript": {
                                "uri" : null,
                                "name": dag_run.conf['payrule']['payrule']
                            },
                            "effectiveDate": effective_date if effective_date else payrule_effective_date
                        }
                    ]
                }

def get_update_user_payload(dag_run, config):
    exceptions = []
    user_details = rail.result("get_user_details")
    current_user_groups = rail.result("get_effective_group_membership")
    profile_status_is_enabled = dag_run.conf['mapper_data']['profile_status'] == "enabled"
    update_custom_fields_for_user, isia_update_done = _update_custom_fields_for_user(dag_run)
    rail.set_result(key="ia_updated", val=isia_update_done)

    if isia_update_done:
        if dag_run.conf['file_data']['is_ia'] in [1, '1']:
            effective_date = dag_run.conf['json_formatted_dates']['ia_start_date']
        if dag_run.conf['file_data']['is_ia'] in [0, '0']:
            # Need to add 1 day to ia_end_date
            end_date = convert_json_date_to_date(dag_run.conf['json_formatted_dates']['ia_end_date']) + relativedelta(days=1)
            effective_date = {
                "day": end_date.day,
                "month": end_date.month,
                "year": end_date.year
            }
    else:
        effective_date = null

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
            "holidayCalendarToApply": _get_holiday_calendar_update_payload(dag_run, user_details, profile_status_is_enabled, exceptions, isia_update_done, config.MAPPER),
            "holidayCalendarAssignmentsToApply": null,
            "schedulePolicyToApply": _get_shift_assignment_to_update(dag_run, user_details, config, exceptions, effective_date),
            "locationScheduleToApply": _get_location_update_payload(dag_run, current_user_groups, effective_date),
            "divisionScheduleToApply": _get_division_update_payload(dag_run, current_user_groups, effective_date),
            "costCenterScheduleToApply": _get_cost_center_update_payload(dag_run, current_user_groups, effective_date),
            "departmentGroupScheduleToApply": _get_department_update_payload(dag_run, current_user_groups, effective_date),
            "employeeTypeGroupScheduleToApply": _get_employee_type_update_payload(dag_run, current_user_groups, effective_date),
            "timesheetPeriodScheduleToApply": _get_timesheetperiod_update_payload(dag_run, user_details, effective_date),
            "serviceCenterScheduleToApply": _get_service_center_update_payload(dag_run, current_user_groups, effective_date),
            "totalBusinessCostScheduleToApply": null,
            "permissionSetsToApply": null,
            "policySetsToApply": _get_policies_to_update_payload(dag_run, user_details, exceptions),
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
            "securitySettingsToApply":  {
                "loginEnabled": "true",
                "forcePasswordChange": "false",
                "loginName": dag_run.conf['file_data']['email_id'],
                "ssoName": dag_run.conf['file_data']['email_id'],
                "password": null,
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "userSSONameModificationOptionUri": "urn:replicon:sso-name-modification-option:login-name"
            } if _can_update_email(dag_run, user_details, config) else null,
            "supervisorsToApply": null,
            "supervisorsModifications": null,
            "payrollRatesToApply": null,
            "payrollRatesModifications": null,
            "overtimeRulesToApply": null,
            "overtimeRulesModifications": null,
            "customFieldValuesToApply": update_custom_fields_for_user,
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
                "displayNameParameter": _get_display_name_to_assign(dag_run)
            } if _can_update_display_name(dag_run, user_details, config) else null,
            "payRulesToApply": null,
            "payRulesScheduleModifications": _get_payrule_schedule_to_update(dag_run, user_details, effective_date),
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
