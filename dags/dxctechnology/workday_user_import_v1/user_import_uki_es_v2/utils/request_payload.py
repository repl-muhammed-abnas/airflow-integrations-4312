"""
Request payload builder functions for UK&I CSC user import
"""
from datetime import date, datetime, timedelta
import rail
from json import loads
from pendulum import now as pendulum_now
from dxctechnology.workday_user_import_v1.user_import_uki_es_v2.utils.date_utils import _get_effective_date_based_on_work_week
from airflow.exceptions import AirflowException
from dxctechnology.workday_user_import_v1.user_import_uki_es_v2.utils.region_fields_config import DXC_UKI

null = None
INPUT_DATE_FORMAT = "%Y-%d-%m"
SHIFT_FTE_NON_100_HOLIDAY_CALENDAR_NAME = "None UKI"

# Default fallback value for Schedule Type, Holiday Calendar, Payrule, and Timesheet Period
# when mapper value is not available for IA=1 users. Update this constant if the name changes in Replicon.
NONE_DEFAULT_VALUE = "NONE"


def _is_international_assignee(is_ia):
    return is_ia in [1, '1']


# UDF fields that are NOT applicable to UKI region (should be removed for IA users coming to UKI)
# UKI uses Work Shift, Date of Birth, Time Type, Middle Name, and PERNER
UKI_EXCLUDED_UDFS = (
    "Personnel Area Code",
    "Personnel Area Description",
    "Annual Leave Anni. Date",
    "LSL Anniversary Date",
    "Personal Leave Anni. Date",
    "Weekly Scheduled Hours",
    "Employee Group",
    "Employee Sub Group",
    "Terms and Conditions",
    "Termination Reason",
    "Termination Reason Code",
    "RUT",
    "EE Group",
)


def _get_udf_fields_to_clear_for_ia(dag_run, current_custom_fields_values, exception_log):
    fields_to_clear = []
    is_ia = dag_run.conf['file_data'].get('is_ia')

    if not _is_international_assignee(is_ia):
        return fields_to_clear

    udfs = dag_run.conf.get('udfs', {})

    for field in current_custom_fields_values:
        field_name = field.get('customField', {}).get('displayText', '')
        if field_name in UKI_EXCLUDED_UDFS:
            # Find the URI for this field and clear it
            for udf_key, udf_data in udfs.items():
                if udf_data.get('display_text') == field_name or udf_data.get('name') == field_name:
                    if udf_data.get('uri'):
                        fields_to_clear.append({
                            "customField": {
                                "uri": udf_data['uri'],
                                "name": null
                            },
                            "text": null,
                            "date": null,
                            "number": null,
                            "dropDownValue": null
                        })
                        exception_log.append(f"UDF field '{field_name}' not applicable for IA=1 in UKI. Removing field.")
                    break

    return fields_to_clear


def is_user_eligible_for_holiday_calendar_assignment(dag_run):
    if dag_run.conf['schedule']['schedule_type'] == "office-schedule":
        if dag_run.conf['file_data']['fte_pct'] == "100":
            return True
    return False


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



def get_todays_minus_specified_days_date_in_json(days_in_number:int, return_type="json"):
    today = datetime.now() - timedelta(days=days_in_number)
    if return_type == "date":
        return today.date()
    return {
        "day": today.day,
        "month": today.month,
        "year": today.year
    }

def get_json_date_from_date(_date):
    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
    }


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
            new_end_date = get_json_date_from_date(convert_json_date_to_date(ia_end_date) + timedelta(days=1))
            rail.set_result(key="effective_date", val=new_end_date)
            update_date_udf(dag_run, 'ia_end_date',
                            'ia_end_date', 'International assignee end date',
                            'international_assignee_end_date', custom_fields_payload, current_custom_fields_values)
            effective_date = new_end_date

        return True, "", effective_date

    else:
        update_date_udf(dag_run, 'ia_start_date', 'ia_start_date', 'International assignee start date', 'international_assignee_start_date', custom_fields_payload, current_custom_fields_values)
        update_date_udf(dag_run, 'ia_end_date', 'ia_end_date', 'International assignee end date', 'international_assignee_end_date', custom_fields_payload, current_custom_fields_values)

        return False, "", effective_date

def convert_json_date_to_date(json_date):
    return date(day=json_date['day'], month=json_date['month'], year=json_date['year'])

def get_todays_date_in_json():
    today = datetime.now()
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

def get_todays_date_for_timezone_in_json(timezone="America/Los_Angeles"):
    today = pendulum_now(timezone).date()
    return {
        "day": today.day,
        "month": today.month,
        "year": today.year
    }

def get_all_locations_payload():
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:location-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "bool": "true",
                }
            }
        }
    }

def get_all_employeegroup_payload():
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:employee-type-group-list-column:employee-type-group",
            "urn:replicon:employee-type-group-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:employee-type-group-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "bool": "true",
                }
            }
        }
    }

def get_all_companycode_payload():
    return {
        "page": "1",
        "pagesize": "100000000",
        "columnUris": [
            "urn:replicon:division-list-column:division",
            "urn:replicon:division-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:division-list-filter:effectively-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "bool": "true"
                }
            }
        }
    }

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

def compare_if_two_json_dates_are_same(date_1, date_2):
    if not date_1:
        return False
    if not date_2:
        return True
    return convert_json_date_to_date(date_1) != convert_json_date_to_date(date_2)

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

def can_update_term_exported_aus_test(current_custom_fields_values):
    term_exported_aus = rail.find_first_by_attr_and_get_attr(current_custom_fields_values,
            'customField.displayText', 'Term Exported (AUS)', 'text', default="")
    return term_exported_aus.lower() == 'yes' #

def get_term_exported_aus_udf_uri(dag_run, current_custom_fields_values):
    term_aus_uri = rail.find_first_by_attr_and_get_attr(current_custom_fields_values,
            'customField.displayText', 'Term Exported (AUS)', 'customField', {}).get('uri')
    if term_aus_uri:
        return term_aus_uri
    # before this change for export is implemented the UDF value is not coming from the dag_run.conf
    # if any reprocess of old records is required it will fail with null uri
    # to avoid that the above is added (all udf values gets returned in the API response even if they do not have any values)
    # below is added as a fallback to get the uri from the config
    return dag_run.conf['udfs'].get('term_exported_australia', {}).get('uri')

def _update_custom_fields_for_user(dag_run, current_user_groups, exception_log=None):
    if exception_log is None:
        exception_log = []

    current_custom_fields_values = rail.result("get_user_details")['userDetails']['customFieldValues']
    custom_fields_payload = []
    if can_update_term_exported_aus_test(current_custom_fields_values):
        custom_fields_payload.append(
            _get_custom_fields_payload(
                uri = get_term_exported_aus_udf_uri(dag_run, current_custom_fields_values),
                txt_value="" # value is being updated from "Yes" => ""
            )
        )

    _update_txt_udf(dag_run, 'assignment_type', 'assignment_type', 'assignment_type', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'work_shift', 'Work Shift', 'work_shift', custom_fields_payload, current_custom_fields_values)
    _update_date_udf(dag_run, 'dob', 'dob', 'Date of Birth', 'date_of_birth', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'middle_name', 'Middle Name', 'middle_name', custom_fields_payload, current_custom_fields_values)
    _update_drop_down_udf(dag_run, 'time_type', 'Time Type', 'time_type', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'perner_id', 'IA PERNER ID', 'ia_perner_id', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'gender', 'Gender', 'gender', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'management_lvl', 'Management Level', 'management_level', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'on_leave', 'On Leave', 'on_leave', custom_fields_payload, current_custom_fields_values)
    
    if dag_run.conf['file_data']['parent_company'] and dag_run.conf['file_data']['parent_company'].lower() not in ['compass', 'ftp']:
        _update_txt_udf(dag_run, 'area_code', 'Personnel Area Code', 'personnel_area_code', custom_fields_payload, current_custom_fields_values)
        _update_txt_udf(dag_run, 'area_name', 'Personnel Area Description', 'personnel_area_name', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'job_level', 'Job Activity Type', 'job_level', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'fte', 'FTE', 'fte', custom_fields_payload, current_custom_fields_values)
    _update_txt_udf(dag_run, 'fte_pct', 'FTE %', 'ftepct', custom_fields_payload, current_custom_fields_values)
    ia_updated, ia_exception_msg, effective_date = get_ia_update_payload_for_udf_update(dag_run, custom_fields_payload=custom_fields_payload, current_custom_fields_values=current_custom_fields_values,
                                                        update_txt_udf=_update_txt_udf, update_date_udf=_update_date_udf)
    rail.set_result(key="ia_updated", val=ia_updated)
    rail.set_result(key="ia_exception_msg", val=ia_exception_msg)
    _update_date_udf(dag_run, 'service_date', 'service_date', 'Continuous Service Date', 'service_date', custom_fields_payload, current_custom_fields_values)

    # If user is an International Assignee, clear UDFs that are not applicable to UKI region
    if _is_international_assignee(dag_run.conf['file_data'].get('is_ia')):
        from dxctechnology.workday_user_import_v1.user_import_uki_es_v2.utils.custom_methods import get_excluded_udf_clear_payloads
        excluded_udf_payloads = get_excluded_udf_clear_payloads(
            region=DXC_UKI,
            current_custom_fields_values=current_custom_fields_values,
            udfs_config=dag_run.conf.get('udfs', {})
        )
        custom_fields_payload.extend(excluded_udf_payloads)

    rail.set_result(key="can_update_notifications_settings", val=True)

    # psa_flag = get_psa_user_udf_add_update_payload(dag_run, rail.find_first_by_attr_and_get_attr(
    #         current_custom_fields_values, "customField.displayText", "PSA User", 'text', default=""), "update",
    #         current_user_groups, null)
    # if psa_flag is not None:
    #     custom_fields_payload.append(
    #                 _get_custom_fields_payload(uri=dag_run.conf['udfs']['psa_user'].get('uri'), drop_down_value_name=psa_flag))

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

    holiday_calendar = dag_run.conf.get('file_data', {}).get('holiday_schedule_calendar')

    if not is_user_eligible_for_holiday_calendar_assignment(dag_run):
        holiday_calendar = SHIFT_FTE_NON_100_HOLIDAY_CALENDAR_NAME
    user_holiday_calendar = ''
    if user_details.get('holidayCalendar'):
        user_holiday_calendar = user_details['holidayCalendar'].get('displayText', '')

    if holiday_calendar and holiday_calendar != user_holiday_calendar:
        return {
                "holidayCalendar": {
                    "name": holiday_calendar,
                    "uri": null
                }
            }

    # For IA=1 users without holiday calendar in mapper, assign NONE
    if not holiday_calendar:
        if _is_international_assignee(dag_run.conf['file_data'].get('is_ia')):
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
                        "effectiveDate": effective_date if effective_date else (dag_run.conf['json_formatted_dates']['exempt_effective_date'] if dag_run.conf[
                                'json_formatted_dates']['exempt_effective_date'] else get_todays_date_in_json()),

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
            # For IA=1 users without schedule from mapper, assign NONE with IA start date
            if _is_international_assignee(dag_run.conf['file_data'].get('is_ia')):
                ia_start_date = dag_run.conf['json_formatted_dates'].get('ia_start_date')
                current_schedule_name = ''
                if current_office_schedule and current_office_schedule.get('officeSchedule'):
                    current_schedule_name = current_office_schedule['officeSchedule'].get('displayText', '')
                if current_schedule_name != NONE_DEFAULT_VALUE:
                    exception_log.append(f"Schedule Type not available in mapper for IA=1. Assigning {NONE_DEFAULT_VALUE} schedule type")
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


def _get_timesheet_period_schedule_to_apply(dag_run, user_details, effective_date, exception_log=None):
    if exception_log is None:
        exception_log = []

    timesheet_period_name = dag_run.conf['mapper_data'].get('timesheet_period')
    is_ia = dag_run.conf['file_data'].get('is_ia')

    # For IA=1 without timesheet period from mapper, use NONE
    if _is_international_assignee(is_ia) and not timesheet_period_name:
        timesheet_period_name = NONE_DEFAULT_VALUE
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
            ia_start_date = dag_run.conf['json_formatted_dates'].get('ia_start_date')
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
                            "effectiveDate": ia_start_date if (ia_start_date and _is_international_assignee(is_ia)) else (effective_date if effective_date else {
                                "day": timesheet_effective_date.day,
                                "month": timesheet_effective_date.month,
                                "year": timesheet_effective_date.year
                            })
                        }
                    ],
                    "endDate": null
                }
            }
    return null

def _get_timesheetperiod_update_payload(dag_run, user_details, effective_date):
    if not user_details['timesheetPeriodSchedule'] and dag_run.conf['user_security_config']['profile_status'].lower() == "enabled":
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
    
    if dag_run.conf['file_data']['management_lvl'] in ['L1', 'L2']:
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

def _get_policies_to_update_payload(dag_run, user_details, exception_log: list):
    policies_to_add = []
    policies_to_remove = []
    is_ia = dag_run.conf['file_data'].get('is_ia')

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
    else:
        # For IA=1 users without timeoff template in mapper, remove existing if assigned
        if _is_international_assignee(is_ia) and not dag_run.conf['user_policies'].get('timeoff_template', {}).get('timeoff_template'):
            current_timeoff_template = user_details.get('timeOffTemplate', {})
            if current_timeoff_template and current_timeoff_template.get('uri'):
                policies_to_remove.append(current_timeoff_template['uri'])
                exception_log.append(f"Timeoff template not available in mapper for IA=1. Removing existing timeoff template")

    if _can_update_schedule_policy(dag_run):

        if dag_run.conf['user_policies']['schedule_policy'].get('uri'):
            policies_to_add.append(dag_run.conf['user_policies']['schedule_policy']['uri'])
        else:
            exception_log.append(f"Schedule policy {dag_run.conf['user_policies']['schedule_policy']['schedule_policy']} not available in Replicon")
    else:
        # For IA=1 users without schedule policy in mapper, remove existing if assigned
        if _is_international_assignee(is_ia) and not dag_run.conf['user_policies'].get('schedule_policy', {}).get('name'):
            current_assigned_policies = get_current_assigned_policies()
            # Find and remove any shift-schedule policies
            for policy in current_assigned_policies:
                if policy.get('policyUri') == "urn:replicon:policy:shift-schedule":
                    if policy.get('policySet', {}).get('uri'):
                        policies_to_remove.append(policy['policySet']['uri'])
                        exception_log.append(f"Schedule policy not available in mapper for IA=1. Removing existing schedule policy")

    if policies_to_add or policies_to_remove:
        return {
            "policySetUrisToAssign": policies_to_add,
            "policyUrisToRemovePolicySet": [],
            "policySetUrisToRemove": policies_to_remove
        }
    return null

def _get_time_entry_approval_path_name(dag_run):
    if not dag_run.conf['approval_path']['time_entry_approval_path']['time_entry_approval_path']:
        return null
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


def _get_payrule_schedule_to_update(dag_run, _user_details, effective_date, exception_log=None):
    if exception_log is None:
        exception_log = []

    payrule_name = dag_run.conf['payrule'].get('payrule')
    is_ia = dag_run.conf['file_data'].get('is_ia')

    # For IA=1 without payrule from mapper, use NONE
    if _is_international_assignee(is_ia) and not payrule_name:
        payrule_name = NONE_DEFAULT_VALUE
        exception_log.append(f"Payrule not available in mapper for IA=1. Assigning {NONE_DEFAULT_VALUE} payrule")

    if payrule_name:
        current_payrule_schedule = _get_current_payrule_schedule_timesheetPeriod(_user_details['payRuleScriptSchedule'], _user_details['userDetails']['employmentDateRange']['startDate'])
        if not current_payrule_schedule or current_payrule_schedule['payRuleScript']['displayText'] != payrule_name:
            if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
                payrule_effective_date = _get_effective_date_based_on_work_week(dag_run.conf['mapper_data']['work_week'], ['saturday'])
                # For IA NONE fallback, use ia_start_date; otherwise use work week effective date
                if _is_international_assignee(is_ia) and payrule_name == NONE_DEFAULT_VALUE:
                    payrule_eff_date = dag_run.conf['json_formatted_dates']['ia_start_date']
                else:
                    payrule_eff_date = {
                        "day": payrule_effective_date.day,
                        "month": payrule_effective_date.month,
                        "year": payrule_effective_date.year
                    }
                return {
                    "scheduleEntries": [
                        {
                            "payRuleScript": {
                                "uri" : null,
                                "name": payrule_name
                            },
                            "effectiveDate": payrule_eff_date
                        }
                    ]
                }

    return null

def generate_oef_payload(definition_uri, tag_uri=null, numeric_value=null, text_value=null):
    return {
        "definition": {
            "uri": definition_uri,
            "name": null
        },
        "tag": {
            "uri": tag_uri,
            "slug": null,
            "tagName": null
        } if tag_uri else null,
        "numericValue": numeric_value,
        "textValue": text_value,
        "fileValue": null,
        "jsonValue": null
    }

def _get_oef_payload(dag_run):
    oef_payload = []

    if dag_run.conf['file_data']['additional_job_classifications']:
        oef_payload.append(
            generate_oef_payload(
                definition_uri=dag_run.conf['oefs']['additional_job_classifications']['uri'],
                text_value=dag_run.conf['file_data']['additional_job_classifications']
            )
        )
    if dag_run.conf['file_data']['employee_representative_status']:
        oef_payload.append(
            generate_oef_payload(
                definition_uri=dag_run.conf['oefs']['employee_representative_status']['uri'],
                tag_uri=dag_run.conf['oefs']['employee_representative_status']['drop_down_vals'][dag_run.conf['file_data']['employee_representative_status'].lower()]
            )
        )

    if dag_run.conf['file_data']['employee_representative_effective_date']:
        oef_payload.append(
            generate_oef_payload(
                definition_uri=dag_run.conf['oefs']['employee_representative_effective_date']['uri'],
                text_value=dag_run.conf['file_data']['employee_representative_effective_date']
            )
        )

    # NEW: Default Weekly Hours OEF for overtime users
    if dag_run.conf['file_data']['default_weekly_hours']:
        oef_payload.append(
            generate_oef_payload(
                definition_uri=dag_run.conf['oefs']['default_weekly_hours']['uri'],
                text_value=dag_run.conf['file_data']['default_weekly_hours']
            )
        )

    return oef_payload

def _get_object_extension_fields_to_update_payload(dag_run, current_oef_fields_values, exception_log=None):
    if exception_log is None:
        exception_log = []

    is_ia = dag_run.conf['file_data'].get('is_ia')

    # if not extension fields are present then add all the fields
    if not current_oef_fields_values:
        return _get_oef_payload(dag_run)

    updated_oef_payload = []

    # Additional Job Classifications
    if dag_run.conf['file_data']['additional_job_classifications']:
        if dag_run.conf['file_data']['additional_job_classifications'] != rail.find_first_by_attr_and_get_attr(
            current_oef_fields_values, "definition.displayText", "Additional Job Classifications", 'textValue', default=""):
            updated_oef_payload.append(
                generate_oef_payload(
                    dag_run.conf['oefs']['additional_job_classifications']['uri'],
                    text_value=dag_run.conf['file_data']['additional_job_classifications']
                )
            )
    else:
        # For IA=1 users without additional_job_classifications in mapper, clear existing value
        if _is_international_assignee(is_ia):
            current_value = rail.find_first_by_attr_and_get_attr(
                current_oef_fields_values, "definition.displayText", "Additional Job Classifications", 'textValue', default="")
            if current_value:
                updated_oef_payload.append(
                    generate_oef_payload(
                        dag_run.conf['oefs']['additional_job_classifications']['uri'],
                        text_value=""
                    )
                )
                exception_log.append("OEF 'Additional Job Classifications' not provided for IA=1. Clearing field.")

    # Employee Representative Effective Date
    if dag_run.conf['file_data']['employee_representative_effective_date']:
        if dag_run.conf['file_data']['employee_representative_effective_date'] != rail.find_first_by_attr_and_get_attr(
            current_oef_fields_values, "definition.displayText", "Employee Representative Effective Date", 'textValue', default=""):
            updated_oef_payload.append(
                generate_oef_payload(
                    dag_run.conf['oefs']['employee_representative_effective_date']['uri'],
                    text_value=dag_run.conf['file_data']['employee_representative_effective_date']
                )
            )
    else:
        # For IA=1 users without employee_representative_effective_date in mapper, clear existing value
        if _is_international_assignee(is_ia):
            current_value = rail.find_first_by_attr_and_get_attr(
                current_oef_fields_values, "definition.displayText", "Employee Representative Effective Date", 'textValue', default="")
            if current_value:
                updated_oef_payload.append(
                    generate_oef_payload(
                        dag_run.conf['oefs']['employee_representative_effective_date']['uri'],
                        text_value=""
                    )
                )
                exception_log.append("OEF 'Employee Representative Effective Date' not provided for IA=1. Clearing field.")

    # Employee Representative Status
    if dag_run.conf['file_data']['employee_representative_status']:
        if dag_run.conf['file_data']['employee_representative_status'] != rail.find_first_by_attr_and_get_attr(
            current_oef_fields_values, "definition.displayText", "Employee Representative Status", 'tag.displayText', default=""):
            updated_oef_payload.append(
                generate_oef_payload(
                    dag_run.conf['oefs']['employee_representative_status']['uri'],
                    tag_uri=dag_run.conf['oefs']['employee_representative_status']['drop_down_vals'][dag_run.conf['file_data']['employee_representative_status'].lower()]
                )
            )
    else:
        # For IA=1 users without employee_representative_status in mapper, clear existing value
        if _is_international_assignee(is_ia):
            current_value = rail.find_first_by_attr_and_get_attr(
                current_oef_fields_values, "definition.displayText", "Employee Representative Status", 'tag.displayText', default="")
            if current_value:
                updated_oef_payload.append(
                    generate_oef_payload(
                        dag_run.conf['oefs']['employee_representative_status']['uri'],
                        tag_uri=null
                    )
                )
                exception_log.append("OEF 'Employee Representative Status' not provided for IA=1. Clearing field.")

    # NEW: Default Weekly Hours OEF for overtime users
    if dag_run.conf['file_data']['default_weekly_hours']:
        if dag_run.conf['file_data']['default_weekly_hours'] != rail.find_first_by_attr_and_get_attr(
            current_oef_fields_values, "definition.displayText", "Default Weekly Hours", 'textValue', default=""):
            updated_oef_payload.append(
                generate_oef_payload(
                    dag_run.conf['oefs']['default_weekly_hours']['uri'],
                    text_value=dag_run.conf['file_data']['default_weekly_hours']
                )
            )
    else:
        # For IA=1 users without default_weekly_hours in mapper, clear existing value
        if _is_international_assignee(is_ia):
            current_value = rail.find_first_by_attr_and_get_attr(
                current_oef_fields_values, "definition.displayText", "Default Weekly Hours", 'textValue', default="")
            if current_value:
                updated_oef_payload.append(
                    generate_oef_payload(
                        dag_run.conf['oefs']['default_weekly_hours']['uri'],
                        text_value=""
                    )
                )
                exception_log.append("OEF 'Default Weekly Hours' not provided for IA=1. Clearing field.")

    return updated_oef_payload

def get_update_user_payload_uki_es(dag_run, config):
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
            "objectExtensionFieldsToApply": _get_object_extension_fields_to_update_payload(dag_run, user_details['userDetails']['extensionFieldValues']),
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

    # If user is an International Assignee, clear OEFs that are not applicable to UKI region
    if _is_international_assignee(dag_run.conf['file_data'].get('is_ia')):
        current_oef_values = user_details['userDetails'].get('extensionFieldValues', [])
        if current_oef_values:
            from dxctechnology.workday_user_import_v1.user_import_uki_es_v2.utils.custom_methods import get_excluded_oef_clear_payloads
            excluded_oef_payloads = get_excluded_oef_clear_payloads(
                region=DXC_UKI,
                current_oef_values=current_oef_values
            )
            if excluded_oef_payloads:
                payload['modifications']['objectExtensionFieldsToApply'] = excluded_oef_payloads

    return payload

def get_update_custom_fields_payload_uki_es(dag_run):
    file_data = dag_run.conf['file_data']
    udfs = dag_run.conf.get('udfs', {})
    
    custom_fields = []
    
    # Add custom field mappings based on UK&I requirements
    for field_name, field_value in udfs.items():
        if field_value:
            custom_fields.append({
                "name": field_name,
                "value": field_value
            })
    
    return custom_fields


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

def get_notification_preference_to_assign_payload_uki_es(dag_run, mode="update"):
    return get_notification_preference_to_assign_payload(dag_run, mode)

def get_product_assignment_payload_uki_es(dag_run):
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


def get_user_end_date_update_payload_uki_es(dag_run):
    end_date = dag_run.conf['json_formatted_dates'].get('term_date')
    
    if not end_date:
        return null
    return {
        "userUri": dag_run.conf['user_uri'],
        "dateRange": {
            "startDate": dag_run.conf['json_formatted_dates'].get('hire_date'),
            "endDate": end_date,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }

def get_timeoff_to_assign_remove_payload_uki_es(dag_run, mode="assign"): # pylint: disable=unused-argument
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

def get_default_timeoff_policy_payload_uki_es(dag_run):
    if not dag_run.conf.get('user_uri') or not dag_run.conf.get('timeoff_uri'):
        raise AirflowException("Missing required fields: user_uri or timeoff_uri in dag_run.conf")
        
    return {
        "timeOffAccount":{
            "userUri" : dag_run.conf['user_uri'],
            "timeOffTypeUri": dag_run.conf['timeoff_uri'] # timeoff_uri will have a value present
        }
    }

def get_put_user_timeoff_policy_set_payload_uki_es(dag_run):
    timeoffs = dag_run.conf.get('mapper_data', {}).get('timeoffs', [])
    effective_date = dag_run.conf['json_formatted_dates'].get('hire_date', get_todays_date_in_json())
    
    return {
        "user": {
            "uri": dag_run.conf.get('user_uri', ''),
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "timeOffPolicySet": {
            "effectiveDate": effective_date,
            "policies": [
                {
                    "policyName": timeoff,
                    "uri": null,
                    "enabled": True,
                    "accrualStartDate": effective_date,
                    "balance": 0
                }
                for timeoff in timeoffs
            ]
        }
    }

def get_update_timesheet_template_update_payload_uki_es(dag_run, caller):
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

    if caller == "update":
        if work_week_eff_date < timesheet_period_eff_date:
            effective_date = timesheet_period_eff_date
            # Below logic may be enabled later, hence kept it as comment
            # if pendulum.now().date() > datetime(2025, 9, 1).date():
            #     effective_date = work_week_eff_date
            # else:
            #     effective_date = timesheet_period_eff_date
        else:
            effective_date = work_week_eff_date

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
                            "name": dag_run.conf['schedule']['default_office_schedule']['name'],
                            "officeSchedule": {
                                "officeScheduleUri": null,
                                "name": dag_run.conf['schedule']['default_office_schedule']['name']
                            },
                            "scheduleTypeUri": dag_run.conf['schedule']['schedule_type_uri']
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

    if not is_user_eligible_for_holiday_calendar_assignment(dag_run):
        return null

    if dag_run.conf['file_data']['holiday_schedule_calendar']:
        return {
            "name": dag_run.conf['file_data']['holiday_schedule_calendar'],
            "uri": null
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
    if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
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
    [_add_custom_field(custom_fields['perner']['uri'], text=dag_run.conf['file_data']['emp_id'])]

    if dag_run.conf['file_data']['perner_id']:
        udfs_to_assign.append(_add_custom_field(custom_fields['ia_perner_id']['uri'], text=dag_run.conf['file_data']['perner_id']))

    if dag_run.conf['file_data']['gender']:
        udfs_to_assign.append(_add_custom_field(custom_fields['gender']['uri'], text=dag_run.conf['file_data']['gender']))

    if dag_run.conf['file_data']['assignment_type']:
        udfs_to_assign.append(_add_custom_field(custom_fields['assignment_type']['uri'], text=dag_run.conf['file_data']['assignment_type']))

    if dag_run.conf['file_data']['work_shift']:
        udfs_to_assign.append(_add_custom_field(custom_fields['work_shift']['uri'], text=dag_run.conf['file_data']['work_shift']))

    if dag_run.conf['file_data']['dob']:
        udfs_to_assign.append(_add_custom_field(custom_fields['date_of_birth']['uri'], date=dag_run.conf['json_formatted_dates']['dob']))

    if dag_run.conf['file_data']['time_type']:
        udfs_to_assign.append(_add_custom_field(custom_fields['time_type']['uri'], drop_down_name=dag_run.conf['file_data']['time_type']))

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

    if dag_run.conf['file_data']['parent_company'] and dag_run.conf['file_data']['parent_company'].lower() not in ['compass', 'ftp']:
        if dag_run.conf['file_data']['area_code']:
            udfs_to_assign.append(_add_custom_field(custom_fields['personnel_area_code']['uri'], text=dag_run.conf['file_data']['area_code']))

        if dag_run.conf['file_data']['area_name']:
            udfs_to_assign.append(_add_custom_field(custom_fields['personnel_area_name']['uri'], text=dag_run.conf['file_data']['area_name']))

    # pas_flag = get_psa_user_udf_add_update_payload(dag_run, '', 'add', [], null)

    # if pas_flag:
    #     udfs_to_assign.append(_add_custom_field(custom_fields['psa_user']['uri'], drop_down_name="Yes"))

    return udfs_to_assign

def _get_activity_list_to_assign(dag_run):
    activity_list = dag_run.conf['activities']['activity_list']

    return list(map(lambda activity: {
        "uri": null,
        "name": activity
    }, activity_list))

def _get_timezone_to_apply(dag_run, exception_log:list):
    # no timezone is defined for UKI
    if False: #dag_run.conf['timezone']['timezone_uri']:
        return {
            'uri': "urn:replicon:time-zone:etc-gmt",
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
    if dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2']:
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

# def create_user_payload_uki_es(dag_run):
def create_user_payload_uki_es(dag_run, config):
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
                    dag_run.conf['user_security_config']['auth_uri']
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
            "extensionFieldValues": _get_oef_payload(dag_run),
            "workCompliancePolicyAssignmentSchedule": []
        }
    }

    rail.set_result(val=exception_log, key="exception_log")

    return payload

# Export get_todays_date_in_json for compatibility
__all__ = [
    'get_update_user_payload_uki_es',
    'get_update_custom_fields_payload_uki_es',
    'get_notification_preference_to_assign_payload',
    'get_notification_preference_to_assign_payload_uki_es',
    'get_product_assignment_payload_uki_es',
    'get_user_end_date_update_payload_uki_es',
    'get_timeoff_to_assign_remove_payload_uki_es',
    'get_default_timeoff_policy_payload_uki_es',
    'get_put_user_timeoff_policy_set_payload_uki_es',
    'get_update_timesheet_template_update_payload_uki_es',
    'get_add_user_payload_uki_es',
    'get_todays_date_in_json',
    'get_todays_date_for_timezone_in_json'
]


def update_time_entry_path_payload_uki_es(dag_run):
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

def update_user_start_date_remove_end_date_uki_es(dag_run):
    return {
        "userUri": dag_run.conf['user_uri'],
        "dateRange": {
            "startDate": dag_run.conf['json_formatted_dates']['hire_date'],
            "endDate": null,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }

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


def get_json_date_from_date_str(date_str):
    if not date_str:
        return None
    
    # Parse the date string
    import pendulum
    try:
        dt = pendulum.parse(date_str)
        return {
            "year": dt.year,
            "month": dt.month,
            "day": dt.day
        }
    except:
        return None

def convert_date_to_string_date(_date, _format=INPUT_DATE_FORMAT):
    return _date.strftime(_format)

def convert_json_date_to_string_date(json_date, _format= INPUT_DATE_FORMAT):
    return date(day=json_date['day'], month=json_date['month'], year=json_date['year']).strftime(_format)

def date_to_use_for_disable(dag_run, return_as_json_date=True):
    if rail.result('prepare_update_payload', 'ia_updated') == "Yes":
        if dag_run.conf['file_data']['is_ia'] == "1":
            if return_as_json_date:
                return dag_run.conf['json_formatted_dates']['ia_start_date']
            return dag_run.conf['file_data']['ia_start_date']
        if dag_run.conf['is_ia'] == "0":
            if not return_as_json_date:
                return convert_date_to_string_date(convert_json_date_to_date(dag_run.conf['json_formatted_dates']['ia_end_date']) + timedelta(days=1))
            return get_json_date_from_date(convert_json_date_to_date(dag_run.conf['json_formatted_dates']['ia_end_date']) + timedelta(days=1))
    if return_as_json_date:
        return get_todays_date_in_json()   
    return convert_json_date_to_string_date(get_todays_date_in_json())

