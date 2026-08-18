from datetime import datetime, timedelta, date
import itertools
from dateutil.relativedelta import relativedelta
from functools import lru_cache
from json import dumps, loads
from os import path
import random
import rail

from dxctechnology.workday_user_import_v1.user_import_philippines.utils.request_payload import (
    get_json_date_from_date_str, convert_json_date_to_date, get_replicon_date, get_json_date_from_date,
    get_todays_date_in_json,
    LOCATION_DELIMITER,
    cached_write_json_artifact, get_required_formatted_date_from_json_date, _get_effective_date_based_on_work_week, INPUT_DATE_FORMAT
)

SPECIAL_SHIFTS = ['ES - BPS GSD Compressed Workweek (Philippines)'.lower(), 'ES - BPS GSD Shift (Philippines)'.lower(), 'ES – No Shift premium (Philippines)'.lower()]
SPECIAL_STATES = ['Quenzon', 'Taguig', 'Davao']

DATE_OF_JOINING_DATE_FORMAT = "%b %d %Y"

def convert_date_to_string_date(_date, _format=INPUT_DATE_FORMAT):
    return _date.strftime(_format)

def convert_json_date_to_string_date(json_date, _format= INPUT_DATE_FORMAT):
    return date(day=json_date['day'], month=json_date['month'], year=json_date['year']).strftime(_format)

def get_specified_json_date_minus_specified_days_months_years_date_in_json(_date_to_use: dict, days_in_number:int=0, months_in_number:int=0, years_in_number:int=0, return_type="json"):
    _date_to_use:date = convert_json_date_to_date(_date_to_use)
    _date_to_return = _date_to_use - relativedelta(
        days=days_in_number,
        months=months_in_number,
        years=years_in_number
    )
    if return_type == "date":
        return _date_to_return
    return {
        "day": _date_to_return.day,
        "month": _date_to_return.month,
        "year": _date_to_return.year
    }


def get_tenure_value(date_1, date_2):
    return (min(float(((date_1-date_2).days)/365), 0))*(-1)


def get_timesheet_period_effective_date_for_instance(instance):
    if instance == "trial":
        return "2025-01-09"
    if instance == "prod":
        return "2025-01-09"
    return "2025-01-05"



def get_run_conf():
    return rail.get_current_context()['dag_run'].conf 

@lru_cache(maxsize=32)
def get_holiday_calender_details():
    return rail.result('get_all_holiday_calendar')

@lru_cache(maxsize=32)
def get_policy_data():
    return rail.result('get_all_policy_sets')

@lru_cache(maxsize=64)
def get_groups_data():
    return (rail.result("get_all_employeegroup_data")['employee_data_for_assignment'], rail.result("get_all_locations"), rail.result("get_all_enabled_departments"),
            rail.result('get_all_companycode_data'), rail.result('get_all_cost_centers'))

@lru_cache(maxsize=32)
def get_replicon_udf_list():
    return rail.result("get_all_user_custom_fields")

def is_profile_enabled(dag_run):
    return dag_run.conf['user_security_config']['profile_status'].lower() == 'enabled'

def get_timezone_uri_from_master_mapper(value, master_mapper):
    if value is None:
        return None
    timezone_data = list(filter(
        lambda x: x['Type'] == "TimeZone" and x['Value'] == value
    , master_mapper))
    if timezone_data:
        return timezone_data[0]['URI']
    return None

"""
    - specific work_shift
        if users work_shift is a special shift, use work_shift + job_level to search in the mapper
        if not found, use Job_level + Others to search in the mapper
"""
"""
    - get all

"""



def get_data_from_mapper(item, instance, general_mapper_data, timeoff_mapper, ACTIVITY_MAPPER, holiday_calendar, schedules_mapper, master_mapper):  

    company_code = item['companycode']
    job_level = item['joblevel']
    country = item['country']
    work_shift = item['workshift'].lower()
    state = item['state']
    city = item.get('workcity', '')
    _state = state if state in SPECIAL_STATES else "Others" # Quezon is a typo in the mapper, so we are using Quenzon instead of Quezon

    "search based on the company code, job level, work shift"
    "if not found search with Others as work shift"
    "still not found, return empty mapper data and log as exception (skip processing)"

    def get_filter(record):
        if record['Location'] == country:
            if record['Company Code'] == company_code:
                if str(job_level) == str(record['Job level']): # making sure that type is same as '1' != 1
                    return True
        return False

    # filter the data based on the job-level only
    raw_mapper_data = list(filter(get_filter, general_mapper_data))
    filter_mapper_data_for_workshift = list(filter(lambda x: x['Work Shift'].lower() == work_shift, raw_mapper_data))

    if work_shift.lower() not in SPECIAL_SHIFTS: # check if work_shift is special
        if not filter_mapper_data_for_workshift:
            filter_mapper_data_for_workshift = list(filter(lambda x: x['Work Shift'].lower() == "Others".lower(), raw_mapper_data))

    if not filter_mapper_data_for_workshift:
        filter_mapper_data_for_workshift = [{}]

    mapper_data = filter_mapper_data_for_workshift[0]
    get_timeoff_data = get_mapper_timeoff_data(dag_run=None, timeoff_mapper=timeoff_mapper, caller=True, item=item)
    
    # Use new activity filtering with ACTIVITY_MAPPER
    get_activity_data = filter_activities_based_on_config(ACTIVITY_MAPPER, job_level, work_shift)
    
    get_holiday_calender = list(filter(lambda x: x['Country'] == country and x['state'] == state and x.get('city', '') == city, holiday_calendar))
    get_schedule_data = list(filter(lambda x: x['Workday Shift'].lower() == work_shift, schedules_mapper))

    def get_mapper_data_by_key(key):
        return mapper_data.get(key, '')
    _timezone = get_mapper_data_by_key('Time Zone')
    return {
        "mapper_values_found": bool(mapper_data),
        "mapper_activities": get_activity_data,
        "holiday_calendar": get_holiday_calender[0]['Holiday Calendar'] if get_holiday_calender else "",
        "holiday_calendar_exception": f"No holiday calendar found for Country: {country}, State: {state}, WorkCity: {city}" if not get_holiday_calender else None,
        "time_offs": get_timeoff_data,
        "schedule": get_schedule_data[0]['Replicon Schedule'] if get_schedule_data else "",
        "schedule_type": ("shift" if get_schedule_data[0]['Replicon Schedule'] == "Shift Schedule" else "office-schedule") if get_schedule_data else "na",
        "timesheet_period_effective_date": get_timesheet_period_effective_date_for_instance(instance), # this is the default date Sept 1, 2025
        **{
            "schedule_policy": get_mapper_data_by_key('Schedule Policy'),
            "overtime_requests": get_mapper_data_by_key('Overtime Requests'),
            "overtime_request_approval_paths": get_mapper_data_by_key('Overtime Request Approval Paths'),
            "timesheet_template": get_mapper_data_by_key('Timesheet Template'),
            "timesheet_approval": get_mapper_data_by_key('Timesheet Approval'),
            "timesheet_period": get_mapper_data_by_key('Timesheet Period'),
            "time_off_template": get_mapper_data_by_key('Time Off Template'),
            "time_off_approval": get_mapper_data_by_key('Time Off Approval'),
            "payrule": get_mapper_data_by_key('Payrule'),
            "time_off_types": get_timeoff_data,
            "timezone": _timezone,
            "timezone_uri": get_timezone_uri_from_master_mapper(_timezone, master_mapper) if get_timezone_uri_from_master_mapper(_timezone, master_mapper) else "",
            "work_week": get_mapper_data_by_key('Work Week'),
            "authentication_type": get_mapper_data_by_key('Authentication Type') if 'Authentication Type' in mapper_data else 'SSO',
            "time_entry_approval_path": get_mapper_data_by_key('Time Entry Approval Path'),
        }
    }

def get_default_values_from_master_mapper(item, master_mapper):
    # this will be used if necessary added it as a place holder
    return {
        "default_office_schedule" : list(filter(lambda x: x['Type'] == "Office Schedule", master_mapper))[0]['Value']
    }

def get_file_data_mapping(item):
    return {
        "emp_id": item["empid"],
        "perner_id": item["pernerid"],
        "email_id": item["email"],
        "first_name": item["firstname"],
        "last_name": item["lastname"],
        "country": item["country"],
        "state": item["state"],
        "workcity": item.get("workcity", ""),
        "exempt": item["exempt"],
        "exempt_effective_date": item["exempteffectivedate"],
        "employee_type": item["employeetype"],
        "hire_date": item["hiredate"],
        "gender": item["gender"],
        "service_date": item["servicedate"],
        "term_date": item["termdate"],
        "status": item["status"],
        "on_leave": item["onleave"],
        "parent_company": item['_parent_company_code'],
        "company_code": item["companycode"],
        "company_name": item["companyname"],
        "area_code": item["areacode"],
        "area_name": item["areaname"],
        "sub_area_code": item["subareacode"],
        "emp_group_code": item["empgroupcode"],
        "emp_group_name": item["empgroupname"],
        "emp_subgroup_code": item["empsubgroupcode"],
        "emp_subgroup_name": item["empsubgroupname"],
        "supervisor_id": item["supervisorid"],
        "supervisor_date": item["supervisordate"],
        "supervisor_f_name": item["supervisorfname"],
        "supervisor_l_name": item["supervisorlname"],
        "supervisor_email_id": item["supervisoremail"],
        "pay_group": item["paygroup"],
        "location_effective_date": item["locationeffectivedate"],
        "home_country": item["homecountry"],
        "cost_center": item["costcenter"],
        "cost_center_name": item["costcentername"],
        "cost_center_effective_date": item["costcentereffectivedate"],
        "org_code": item["orgcode"],
        "org_name": item["orgname"],
        "work_shift": item["workshift"],
        "work_shift_effective_date": item["workshifteffectivedate"],
        "job_level": item["joblevel"],
        "job_change_effective_date": item["jobchangeeffectivedate"],
        "fte": item["fte"],
        "fte_pct": item["ftepct"],
        "is_ia": item["isia"],
        "ia_start_date": item["iastartdate"],
        "ia_end_date": item["iaenddate"],
        "rut": item["rut"],
        "middle_name": item["middlename"],
        "time_type": item["timetype"],
        "dob": item["dob"],
        "management_lvl": item["managementlvl"],
        "ausjc": item["ausjc"],
        "terms_conditions": item["termsconditions"],
        "industrial_instrument_classification": item["industrialinstrumentclassification"],
        "additional_data_effective_date": item["additionaldataeffectivedate"],
        "termination_reason": item["terminationreason"],
        "scheduled_weekly_hours": item["scheduledweeklyhours"],
        "assignment_type": item["assignment_type"],
        "marital_status_ind": item.get("marital_status_ind", ""),
        "marital_status_efft_dt": item.get("marital_status_efft_dt", "")
    }

def _get_location_full_path(item):
    country = item['_actual_country']
    state = item.get('_actual_state', '')
    workcity = item.get('workcity', '')
    
    # Build location path: Country | State | WorkCity
    location_parts = [country]
    if state:
        location_parts.append(state)
    if workcity:
        location_parts.append(workcity)
    
    return LOCATION_DELIMITER.join(location_parts)

def _get_employee_type_full_path(item):
    return rail.smartjoin_by_delim(arr=[item['subareacode'], item['empgroupcode'], item['empsubgroupcode']], separator=" | ")

@lru_cache(maxsize=32)
def _get_user_permission_set_details(end_user_permission, supervisor_end_user_permission, supervisor_user_permission):
    permissions_details = rail.result("get_all_permission_sets")
    return {
        "end_user_permission": rail.find_first_by_attr_and_get_attr(permissions_details, 'displayText',
                                                                    end_user_permission, default={}),
        "supervisor_user_permission": rail.find_first_by_attr_and_get_attr(permissions_details, 'displayText',
                                                                    supervisor_user_permission, default={}),
        "supervisor_end_user_permission": rail.find_first_by_attr_and_get_attr(permissions_details, 'displayText',
                                                                    supervisor_end_user_permission, default={})
    }

def get_replicon_schedule_details():
    return rail.result("get_all_office_schedules")

def _get_user_udfs_details():
    replicon_udf_list = get_replicon_udf_list()

    return {
            "gender": replicon_udf_list.get("gender", {}),
            "service_date": replicon_udf_list.get("continuous_service_date", {}),
            "on_leave": replicon_udf_list.get("on_leave", {}),
            "personnel_area_name": replicon_udf_list.get("personnel_area_description", {}),
            "personnel_area_code": replicon_udf_list.get("personnel_area_code", {}),
            "job_level": replicon_udf_list.get("job_activity_type", {}),
            "fte": replicon_udf_list.get("fte", {}),
            "ftepct": replicon_udf_list.get("fte_%", {}),
            "international_assignee": replicon_udf_list.get("international_assignee", {}),
            "international_assignee_start_date": replicon_udf_list.get("international_assignee_start_date", {}),
            "international_assignee_end_date": replicon_udf_list.get("international_assignee_end_date", {}),
            "perner": replicon_udf_list.get("perner", {}),
            "rut": replicon_udf_list.get("rut", {}),
            "middle_name": replicon_udf_list.get("middle_name", {}),
            "time_type": replicon_udf_list.get("time_type", {}),
            "date_of_birth": replicon_udf_list.get("date_of_birth", {}),
            "employee_type_udf": replicon_udf_list.get("employee_group", {}),
            "work_shift": replicon_udf_list.get("work_shift", {}),
            "management_level": replicon_udf_list.get("management_level", {}),
            "ia_perner_id": replicon_udf_list.get("ia_perner_id", {}),
            "terms_and_conditions": replicon_udf_list.get("terms_and_conditions", {}),
            "termination_reason": replicon_udf_list.get("termination_reason", {}),
            "termination_reason_code": replicon_udf_list.get("termination_reason_code", {}),
            "employee_sub_group": replicon_udf_list.get("employee_sub_group", {}),
            "assignment_type": replicon_udf_list.get("assignment_type", {}),
            "annual_leave_anni_date": replicon_udf_list.get("annual_leave_anni_date", {}),
            "lsl_anniversary_date": replicon_udf_list.get("lsl_anniversary_date", {}),
            "personal_leave_anni_date": replicon_udf_list.get("personal_leave_anni_date", {}),
            "weekly_scheduled_hours": replicon_udf_list.get("weekly_scheduled_hours", {}),
            "ee_group": replicon_udf_list.get("ee_group", {}),
            "psa_user": replicon_udf_list.get("psa_user", {})
        }

def get_process_philippines_user_data_config(dag_run, item, config):

    full_path = list(filter(lambda mapper_item: mapper_item['Company_Code'] == item['companycode'] ,config.COMPANY_CODE_MAPPER))
    if not full_path:
        full_path = [{}]

    item['company_code_full_path'] = full_path[0].get('Full_Path')
    item['_parent_company_code'] = full_path[0].get('Parent')

    employee_data, location_data, department_data, division_data, cost_center_data = get_groups_data()

    policy_data = get_policy_data()
    _mapper_derived_data = {
        **get_data_from_mapper(item, config.instance, config.GENERAL_MAPPER, config.TIMEOFF_MAPPER, config.ACTIVITY_MAPPER, config.HOLIDAY_CALENDAR, config.SCHEDULES_MAPPER, config.MASTER_MAPPER),
        **get_default_values_from_master_mapper(item, config.MASTER_MAPPER)}

    company_code_uri = rail.find_first_by_attr_and_get_attr(
                            division_data,
                            "full_path",
                            item["company_code_full_path"],
                            default={}
                        )
    return {
        "user_record_index": int(item['user_record_index']), # will be used in processing batch wise
        "supervisor_user_log": rail.result("create_supervisor_log"),
        "file_name": path.split(rail.result("new_file_sensor"))[1],
        "starting_balance_set_to_uri": rail.result("get_starting_balance_script"),
        "prevent_balance_overdraw_uri": rail.result("get_prevent_balance_overdraw_script"),
        "employee_type_data": cached_write_json_artifact('get_all_employeegroup_data'),
        "division_data": cached_write_json_artifact('get_all_companycode_data'),
        "item": item, # for reference purpose only
        "file_data": get_file_data_mapping(item),
        "company_code_list": "",
        "employee_type_list": "",
        "mapper_data": _mapper_derived_data,
        "payrule": {
            "payrule": _mapper_derived_data['payrule'],
        },
        "user_security_config": {
            # philippines is one of the allowed country
            # hence hardcoded the below value
            "allowed_country": "enable",
            # this logic is based on if the company code is present in the mapper or not
            # however as this PHL workflow will support only 2 company code for now, this is hard coded
            # if any other company code is added that needs to be added in the config
            "profile_status": "enabled",
            "replicon_field": 'true' if item['status'] in [1,'1'] else 'false',
            "auth_uri": config.AUTHS.get((_mapper_derived_data['Authentication Type'] if "Authentication Type" in _mapper_derived_data else "SSO")),
            "products": [product['Value'] for product in config.PRODUCT],
            "product_uri": [product['URI'] for product in config.PRODUCT]
        },
        "udfs": _get_user_udfs_details(),
        "work_week":{
            "workweek_uri": f"urn:replicon:day-of-week:{_mapper_derived_data['work_week'].split(' ')[0].lower()}",
            "workweek_name": _mapper_derived_data['work_week'],
        },
        "holiday_calendar": {
            "holiday_calendar": _mapper_derived_data['holiday_calendar'],
            "holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                                        get_holiday_calender_details(),
                                        'name',
                                        _mapper_derived_data['holiday_calendar'],
                                        'uri')
        },
        "approval_path": {
            "timesheet_approval_path": {
                "timesheet_approval_path": _mapper_derived_data['timesheet_approval']
            },
            "timeoff_approval": {
                "time_off_approval_path": _mapper_derived_data['time_off_approval'],
            },
            "time_entry_approval_path": {
                "time_entry_approval_path": _mapper_derived_data['time_entry_approval_path'],
            }
        },
        "activities": {
            "activity_list" : _mapper_derived_data['mapper_activities']
        },
        "user_policies": {
            "punch_entry_policy": {},
            "timeoff_template": {
                **{"timeoff_template": _mapper_derived_data['time_off_template']},
                **rail.find_first_by_attr_and_get_attr(
                    policy_data,
                    'name',
                    _mapper_derived_data['time_off_template'],
                    default={}
                )
            },
            "timesheet_period": {
                **{"timesheet_period": _mapper_derived_data['timesheet_period']},
                **rail.find_first_by_attr_and_get_attr(
                    policy_data,
                    'name',
                    _mapper_derived_data['timesheet_period'],
                    default={}
                )
            },
            "timesheet_template": {
                **{"timesheet_template": _mapper_derived_data['timesheet_template']},
                **rail.find_first_by_attr_and_get_attr(
                    policy_data,
                    'name',
                    _mapper_derived_data['timesheet_template'],
                    default={}
                )
            },
            "schedule_policy": {
                "schedule_policy": _mapper_derived_data['schedule_policy'],
                **rail.find_first_by_attr_and_get_attr(
                    policy_data,
                    'name',
                    _mapper_derived_data['schedule_policy'],
                    default={}
                )
            },
            "overtime_requests": {
                "overtime_requests": _mapper_derived_data['overtime_requests'],
                **rail.find_first_by_attr_and_get_attr(
                    policy_data,
                    'name',
                    _mapper_derived_data['overtime_requests'],
                    default={}
                )
            },
            "overtime_request_approval_paths": {
                "overtime_request_approval_paths": _mapper_derived_data['overtime_request_approval_paths'],
                **rail.find_first_by_attr_and_get_attr(
                    policy_data,
                    'name',
                    _mapper_derived_data['overtime_request_approval_paths'],
                    default={}
                )
            }
        },
        "timezone": {
            "timezone": _mapper_derived_data["timezone"],
            "timezone_uri" : _mapper_derived_data["timezone_uri"]
        },
        "user_permissions" : _get_user_permission_set_details(
                config.end_user_permission,
                config.supervisor_end_user_permission,
                config.supervisor_end_user_supervision_permission
            ),
        "groups": {
            # Location
            "location_path": _get_location_full_path(item),
            "location": rail.find_first_by_attr_and_get_attr(
                location_data,
                'fullpath',
                _get_location_full_path(item),
                default={}
            ),
            "location_exception": f"Location '{_get_location_full_path(item)}' not found in Replicon" if not rail.find_first_by_attr_and_get_attr(
                location_data,
                'fullpath',
                _get_location_full_path(item),
                default={}
            ).get('uri') else None,
            # Employee Type
            "employee_type" : {
                "is_exempt" : item["exempt"]=="Yes",
                "exempt_salaried" : rail.find_first_by_attr_and_get_attr(employee_data,
                                'full_path',
                                "Exempt – Salaried",
                                default={}),
                "non_exempt_hourly" : rail.find_first_by_attr_and_get_attr(employee_data,
                                'full_path',
                                "Non Exempt - Hourly",
                                default={}),
                "other" : rail.find_first_by_attr_and_get_attr(employee_data,
                                'full_path',
                                _get_employee_type_full_path(item),
                                default={}),
                "employee_type_full_path": rail.find_first_by_attr_and_get_attr(employee_data, 'full_path', _get_employee_type_full_path(item), default={}),
                "uri": ((rail.find_first_by_attr_and_get_attr(employee_data,
                                'full_path',
                                "Exempt – Salaried",
                                default={})) if item["exempt"]=="Yes" else (rail.find_first_by_attr_and_get_attr(employee_data,
                                'full_path',
                                "Non Exempt - Hourly",
                                default={})))
            },
            # Organizational Unit
            "department": rail.find_first_by_attr_and_get_attr(
                department_data,
                "displayText",
                item["orgcode"],
                default={}
            ),
            # Cost Center
            "cost_center": rail.find_first_by_attr_and_get_attr(
                cost_center_data,
                "displayText",
                item["costcenter"],
                default={}
            ),
            # Company Code
            "division": company_code_uri if company_code_uri else rail.find_first_by_attr_and_get_attr(
                division_data,
                "name",
                item["companycode"],
                default={}
            ),
            # PayGroup
            "service_center":{} # We are making use of the name not the URI

        },
        "schedule": {
            "default_office_schedule": {
                "name" : _mapper_derived_data['default_office_schedule']
            },
            "schedule_name": _mapper_derived_data['schedule'],
            "schedule_type_uri": f"urn:replicon:schedule-type:{_mapper_derived_data['schedule_type']}",
            "schedule_type": _mapper_derived_data['schedule_type'],
            "office_schedule_details": {
                **rail.find_first_by_attr_and_get_attr(
                    get_replicon_schedule_details(),
                    "displayText",
                    _mapper_derived_data['schedule'],
                    default={}
                )
            } if _mapper_derived_data['schedule_type'] != "shift" else {}
        },
        "json_formatted_dates": {
            "hire_date": get_json_date_from_date_str(item['hiredate']),
            "service_date": get_json_date_from_date_str(item['servicedate']),
            "term_date": get_json_date_from_date_str(item['termdate']),
            "supervisor_date": get_json_date_from_date_str(item['supervisordate']),
            "employee_type_effective_date": get_json_date_from_date_str(item['exempteffectivedate']) if item['exempteffectivedate'] else get_todays_date_in_json(),
            "location_effective_date": get_json_date_from_date_str(item['locationeffectivedate']),
            "cost_center_effective_date": get_json_date_from_date_str(item['costcentereffectivedate']),
            "work_shift_effective_date": get_json_date_from_date_str(item['workshifteffectivedate']),
            "job_change_effective_date": get_json_date_from_date_str(item['jobchangeeffectivedate']),
            "ia_start_date": get_json_date_from_date_str(item['iastartdate']),
            "ia_end_date": get_json_date_from_date_str(item['iaenddate']),
            "exempt_eff_date": get_json_date_from_date_str(item['exempteffectivedate']),
            "date_of_birth": get_json_date_from_date_str(item['dob']),
            "additionaldata_effective_date": get_json_date_from_date_str(item['additionaldataeffectivedate']),
            "timesheet_period_effective_date": get_json_date_from_date_str(_mapper_derived_data['timesheet_period_effective_date']),
            "work_week": _get_effective_date_based_on_work_week(_mapper_derived_data['work_week'], [], return_as_dict=True)
        }
    }

def filter_timeoffs_based_on_gender(timeoff_records, gender):
    if timeoff_records['Gender'] == "All":
        return True
    elif timeoff_records['Gender'] == gender:
        return True
    else:
        return False

def filter_based_on_job_level(timeoff_record, job_level):
    if timeoff_record['Job Level'] == "All":
        return True
    elif str(timeoff_record['Job Level']) == str(job_level):
        return True
    else:
        return False

def filter_timeoffs_based_on_hire_date(timeoff_record, hire_date):
    if timeoff_record['Date of Joining'].lower() == "all":
        return True
    else:
        date_of_joining = datetime.strptime(timeoff_record['Date of Joining'], DATE_OF_JOINING_DATE_FORMAT).date()
        if timeoff_record['Date of joining compare action'] == ">":
            return hire_date > date_of_joining
        if timeoff_record['Date of joining compare action'] == "<":
            return hire_date < date_of_joining
        if timeoff_record['Date of joining compare action'] == "NA":
            return True
    return False

def filter_timeoffs_based_on_marital_status(timeoff_record, marital_status_ind):
    marital_status_required = timeoff_record.get('Marital Status Required', 'No')
    if marital_status_ind is None:
        marital_status_ind = "no"
    if marital_status_required.lower() == 'yes':
        return marital_status_ind.lower() == 'yes'
    return True

def filter_timeoffs_based_on_conditional_fields(timeoff_record, file_data):
    # For now, just check marital status
    marital_status_ind = file_data.get('marital_status_ind', '')
    if not filter_timeoffs_based_on_marital_status(timeoff_record, marital_status_ind):
        return False
    
    # Future: Add more conditional checks here
    # Example for future tenure-based leaves:
    # if timeoff_record.get('Minimum Tenure Days'):
    #     tenure_days = calculate_tenure_days(file_data.get('hire_date'))
    #     if tenure_days < int(timeoff_record.get('Minimum Tenure Days', 0)):
    #         return False
    
    return True

def evaluate_restriction(restriction, context):
    field = restriction['field']
    operator = restriction['operator']
    values = restriction['values']
    
    if field not in context:
        return True  # If field not provided, assume no restriction
    
    context_value = context[field]
    
    if operator == 'in':
        return context_value in values
    elif operator == 'not_in':
        return context_value not in values
    elif operator == 'equals':
        return context_value == values[0]
    elif operator == 'not_equals':
        return context_value != values[0]
    
    return True

def filter_activities_based_on_config(ACTIVITY_MAPPER, job_level, work_shift):
    context = {
        'job_level': job_level,
        'work_shift': work_shift
    }
    
    filtered_activities = []
    
    for activity in ACTIVITY_MAPPER:
        included = True
        
        for restriction in activity.get('restrictions', []):
            if not evaluate_restriction(restriction, context):
                included = False
                break
        
        if included:
            filtered_activities.append(activity['code'])
    
    return filtered_activities

def get_filter_response(timeoff_record, country, gender, job_level, hire_date, marital_status_ind=''):
    if (timeoff_record['Country']).lower() == country.lower():
        if (filter_timeoffs_based_on_gender(timeoff_record, gender) and 
            filter_based_on_job_level(timeoff_record, job_level) and 
            filter_timeoffs_based_on_hire_date(timeoff_record, hire_date) and
            filter_timeoffs_based_on_marital_status(timeoff_record, marital_status_ind)):
            return True
    return False

def get_user_base_data(dag_run, item=None):
    if item is not None:
        print(item)
        return {
            'country': item.get('country'),
            'parent_company': item.get('_parent_company_code'),
            'job_level': item.get('joblevel'),
            'gender': item.get('gender'),
            'hire_date': convert_json_date_to_date(get_json_date_from_date_str(item.get('hiredate'))),
            'marital_status_ind': item.get('marital_status_ind'),
            'marital_status_efft_dt': item.get('marital_status_efft_dt')
        }
    else:
        file_data = dag_run.conf.get('file_data', {})
        return {
            'country': file_data.get('country'),
            'parent_company': file_data.get('parent_company'),
            'job_level': file_data.get('job_level'),
            'gender': file_data.get('gender'),
            'hire_date': convert_json_date_to_date(dag_run.conf.get('json_formatted_dates', {}).get('hire_date')),
            'marital_status_ind': file_data.get('marital_status_ind'),
            'marital_status_efft_dt': file_data.get('marital_status_efft_dt')
        }

def get_mapper_timeoff_data(dag_run, timeoff_mapper, config= None, caller=None, item=None):
    # Use the central data extraction function
    user_data = get_user_base_data(dag_run, item if caller is not None else None)

    country = user_data['country']
    user_parent_company = user_data['parent_company']
    job_level = user_data['job_level']
    gender = user_data['gender']
    hire_date = user_data['hire_date']
    
    # Get marital status from file_data
    print(user_data)
    marital_status_ind = user_data['marital_status_ind']

    return list(map(lambda filtered_record: {
            **filtered_record,
            **{
                "name": filtered_record['Timeoff Type Name'].strip() # added this for the easy references in next tasks / steps
            }
        }, filter(lambda x : get_filter_response(x, country, gender, job_level, hire_date, marital_status_ind), timeoff_mapper)
        )
    )

@lru_cache(maxsize=32)
def get_cached_replicon_timeoff_data():
    return rail.result("get_all_timeoffs")

def timeoff_to_assign():
    mapper_timeoff_data = rail.result('get_mapper_timeoff_data')
    replicon_timeoff_data = get_cached_replicon_timeoff_data()

    # Create name-to-record mapping for faster lookups (O(1) vs O(n))
    timeoff_names_dict = {mapper_timeoff['name'].strip(): mapper_timeoff for mapper_timeoff in mapper_timeoff_data}

    # More efficient data processing
    timeoff_list = []
    not_found_names = set(timeoff_names_dict.keys())  # Use set for O(1) removal

    for replicon_timeoff in replicon_timeoff_data:
        if replicon_timeoff['name'] in timeoff_names_dict:
            timeoff_list.append({
                **replicon_timeoff,
                "mapper_data": timeoff_names_dict[replicon_timeoff['name']]
            })
            not_found_names.remove(replicon_timeoff['name'])

    return {
        "mapper_timeoff_data": mapper_timeoff_data,
        "timeoff_not_found_in_replicon": list(not_found_names),
        "timeoff_data_to_assign": [ {**{"to_index": timeoff_index}, **_timeoff} for timeoff_index, _timeoff in enumerate(timeoff_list)],
        "timeoff_data_to_assign_uri_list": [timeoff['uri'] for timeoff in timeoff_list],
        "timeoff_data_to_assign_uri_list_disabled_removed": [timeoff['uri'] for timeoff in timeoff_list if timeoff['mapper_data']['Should Disabled After Assignment'].lower() != "yes"]
    }

def get_marital_status_effective_date_if_applicable(timeoff_name, dag_run, mapper_timeoffs=None):
    # Use provided mapper data or fetch it
    if mapper_timeoffs is None:
        mapper_timeoffs = rail.result('get_mapper_timeoff_data', None)
    
    if mapper_timeoffs:
        timeoff_record = next((t for t in mapper_timeoffs if t.get('name') == timeoff_name), None)
        if timeoff_record and timeoff_record.get('Marital Status Required', 'No').lower() == 'yes':
            # Get marital status effective date
            file_data = dag_run.conf.get('file_data', {})
            marital_status_efft_dt = file_data.get('marital_status_efft_dt', '')
            if marital_status_efft_dt:
                # Use existing date parsing utility
                try:
                    # get_json_date_from_date_str expects MM/DD/YYYY format
                    return get_json_date_from_date_str(marital_status_efft_dt)
                except:
                    pass
    return None

def get_conditional_timeoff_effective_date(timeoff_name, dag_run, mapper_timeoffs=None):
    # For now, delegate to marital status function
    marital_date = get_marital_status_effective_date_if_applicable(timeoff_name, dag_run, mapper_timeoffs)
    if marital_date:
        return marital_date
    
    # Future: Add more conditional date logic here
    # Example:
    # if timeoff_requires_probation_completion(timeoff_name, mapper_timeoffs):
    #     return get_probation_end_date(dag_run)
    
    return None

def get_policy_set_to_assign():
    if rail.result("get_default_timeoff_policy"):
        return loads(dumps(rail.result("get_default_timeoff_policy")
                                ).replace("null", "\"effective\""
                                ).replace("\"script\"", "\"scriptTarget\""
                            )
                    )
    return []

def get_policy_set_to_assign_with_marital_status(dag_run):
    if not rail.result("get_default_timeoff_policy"):
        return []
    
    # Store original for debugging
    original_policy = rail.result("get_default_timeoff_policy")
    rail.set_result(key="default_policy", val = original_policy)
    
    # Check if this is a marital status dependent timeoff (like paternity leave)
    timeoff_name = dag_run.conf.get('timeoff_name', '')
    
    # For add user flow, check mapper_details from conf
    mapper_details = dag_run.conf.get('mapper_details', {})
    marital_status_date = None
    
    if mapper_details.get('Marital Status Required', 'No').lower() == 'yes':
        # Get marital status effective date
        file_data = dag_run.conf.get('file_data', {})
        marital_status_efft_dt = file_data.get('marital_status_efft_dt', '')
        if marital_status_efft_dt:
            try:
                marital_status_date = get_json_date_from_date_str(marital_status_efft_dt)
            except:
                pass
    
    # Log for debugging
    rail.set_result(key="marital_status_date_found", val = marital_status_date)
    rail.set_result(key="timeoff_name_being_processed", val = timeoff_name)
    rail.set_result(key="mapper_details", val = mapper_details)
    
    # If we have marital status date, update the policy dates
    if marital_status_date and original_policy and isinstance(original_policy, list):
        # Convert marital status date dict to datetime for comparison
        marital_date = datetime(marital_status_date['year'], marital_status_date['month'], marital_status_date['day'])
        
        # Calculate the offset between entries if there are multiple entries
        date_offsets = []
        if len(original_policy) > 1:
            for i in range(1, len(original_policy)):
                curr_date = original_policy[i].get('effectiveDate')
                prev_date = original_policy[i-1].get('effectiveDate')
                if isinstance(curr_date, dict) and isinstance(prev_date, dict):
                    curr_datetime = datetime(curr_date['year'], curr_date['month'], curr_date['day'])
                    prev_datetime = datetime(prev_date['year'], prev_date['month'], prev_date['day'])
                    offset = curr_datetime - prev_datetime
                    date_offsets.append(offset)
        
        # Create a deep copy to modify
        policy_set = loads(dumps(original_policy))
        
        # Update the first entry with marital status effective date
        if len(policy_set) > 0:
            policy_set[0]['effectiveDate'] = marital_status_date
            
            # Update subsequent entries maintaining the same offset
            cumulative_offset = timedelta(0)
            for i in range(1, len(policy_set)):
                if i-1 < len(date_offsets):
                    cumulative_offset += date_offsets[i-1]
                    new_date = marital_date + cumulative_offset
                    policy_set[i]['effectiveDate'] = {
                        'year': new_date.year,
                        'month': new_date.month,
                        'day': new_date.day
                    }
        
        # Store the updated policy for debugging
        rail.set_result(key="updated_policy_with_marital_date", val = policy_set)
        
        # Now do the script replacements
        final_policy = loads(dumps(policy_set).replace("\"script\"", "\"scriptTarget\""))
        rail.set_result(key="final_policy_after_replacements", val = final_policy)
        return final_policy
    else:
        # No marital status date, proceed with normal flow
        return loads(dumps(original_policy
                            ).replace("null", "\"effective\""
                            ).replace("\"script\"", "\"scriptTarget\""
                        )
                )

def process_marital_status_policy_for_type(dag_run):
    if not rail.result("get_default_timeoff_policy_for_type"):
        return []
    
    # Get the policy from timeoff type
    original_policy = rail.result("get_default_timeoff_policy_for_type")
    
    # Get marital status effective date
    file_data = dag_run.conf.get('file_data', {})
    marital_status_efft_dt = file_data.get('marital_status_efft_dt', '')
    
    if not marital_status_efft_dt:
        # If no marital status date, use today's date
        marital_status_date = get_todays_date_in_json()
    else:
        # Parse the marital status date - let it raise error if invalid format
        marital_status_date = get_json_date_from_date_str(marital_status_efft_dt)
    
    if original_policy and isinstance(original_policy, list):
        # Convert marital status date dict to datetime for comparison
        marital_date = datetime(marital_status_date['year'], marital_status_date['month'], marital_status_date['day'])
        
        # Calculate the offset between original entries if there are multiple entries
        date_offsets = []
        if len(original_policy) > 1:
            for i in range(1, len(original_policy)):
                curr_date = original_policy[i].get('effectiveDate')
                prev_date = original_policy[i-1].get('effectiveDate')
                if isinstance(curr_date, dict) and isinstance(prev_date, dict):
                    curr_datetime = datetime(curr_date['year'], curr_date['month'], curr_date['day'])
                    prev_datetime = datetime(prev_date['year'], prev_date['month'], prev_date['day'])
                    offset = curr_datetime - prev_datetime
                    date_offsets.append(offset)
        
        # Create a deep copy to modify
        policy_set = loads(dumps(original_policy))
        
        # Process each entry
        cumulative_offset = timedelta(0)
        for i, entry in enumerate(policy_set):
            # Ensure all entries have a description (required by API)
            if not entry.get('description'):
                entry['description'] = "Paternity Leave Policy"
            
            if i == 0:
                # First entry - set the marital status effective date
                entry['effectiveDate'] = marital_status_date
            else:
                # Calculate the effective date based on offset
                if i-1 < len(date_offsets):
                    cumulative_offset += date_offsets[i-1]
                else:
                    # Default to yearly offset if not found
                    cumulative_offset += timedelta(days=365)
                
                new_date = marital_date + cumulative_offset
                entry['effectiveDate'] = {
                    'year': new_date.year,
                    'month': new_date.month,
                    'day': new_date.day
                }
            
            # Remove startOffset if it exists since we're using effectiveDate
            if 'startOffset' in entry:
                del entry['startOffset']
        
        return policy_set
    
    return []

def is_user_disabled_for_non_go_live_country(dag_run, user_details_task_id):
    user_details = rail.result(user_details_task_id)
    return user_details['userDetails']['isEnabled'] is True \
        and dag_run.conf['user_security_config']['profile_status'] != "enabled"

def is_division_gsap_test():
    if rail.result("get_effective_group_membership")['parent_division']:
        return rail.result("get_effective_group_membership")['parent_division']['division']['displayText'] == "GSAP"
    return False

def get_trigger_process_timeoff_policies_items():
    current_timeoff_policies = rail.result("get_user_timeoff_policy_summary")['policiesByTimeOffType']

    return list(filter(lambda to_policy:  to_policy['isTimeOffAllowedAgainstThisTimeOffType'] is True
        and bool(to_policy['policySetSchedule'] and to_policy['policySetSchedule'][0]['effectiveDate']), current_timeoff_policies))

def can_update_user_end_date_test(dag_run):
    return (bool(dag_run.conf['file_data']['term_date'])
            and not bool(rail.result("get_user_details")['userDetails']['employmentDateRange'].get('endDate', False)))

def user_does_not_have_admin_and_payroll_permission_test():
    permissions = rail.result("get_assigned_permission_for_user")
    if not permissions:
        return True  # No permissions means no admin or payroll

    # Single pass through permissions checking both URIs
    admin_found = False
    payroll_found = False

    for permission in permissions:
        policy_uri = permission.get("policyUri")
        if policy_uri == "urn:replicon:policy:administration":
            admin_found = True
        elif policy_uri == "urn:replicon:policy:payroll-management":
            payroll_found = True

        # Early exit if both permissions found
        if admin_found and payroll_found:
            return False

    # Return True if neither permission was found
    return not (admin_found or payroll_found)

def is_user_already_disabled_test(dag_run):
    user_details = rail.result('get_user_details')
    return user_details['userDetails']['isEnabled'] is not True and dag_run.conf['user_security_config']['replicon_field'] in [False , 'false']


def is_user_rehire_test(dag_run):
    user_details = rail.result('get_user_details')
    rehire_value = user_details['userDetails']['isEnabled'] is False \
        and dag_run.conf['user_security_config']['replicon_field'] in ['true', True] \
            and dag_run.conf['user_security_config']['profile_status'] == "enabled"
    rail.set_result(key="rehire", val=("yes" if rehire_value else "no"))
    return rehire_value

def can_update_user_start_date_test(dag_run):
    user_start_date = rail.result("get_user_details")['userDetails']['employmentDateRange'].get('startDate', False)
    if not user_start_date:
        return True
    return dag_run.conf['file_data']['hire_date'] != f"{user_start_date['year']}-{user_start_date['day']}-{user_start_date['month']}"

def should_disabled_user_test(dag_run):
    user_details = rail.result('get_user_details')
    return user_details['userDetails']['isEnabled'] is True and\
        dag_run.conf['user_security_config']['replicon_field'] in [False , 'false']

def is_end_date_less_than_today_test(dag_run):
    return get_replicon_date(dag_run.conf['file_data']['term_date'], "date").date() < convert_json_date_to_date(get_todays_date_in_json())

def get_current_assigned_udf_values(custom_field_values):
    return {
        "perner_id": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "IA PERNER ID", "text"),
        "gender": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Gender", "text"),
        "service_date": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Continuous Service Date", "text"),
        "on_leave": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "On Leave", "text"),
        "personnal_area_code": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Personnel Area Code", "text"),
        "personnal_area_description": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Personnel Area Description", "text"),
        "job_activity_type": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Job Activity Type", "text"),
        "fte": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "FTE", "text"),
        "ftepct": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "FTE %", "text"),
        "is_ia": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "International Assignee", "text"),
        "ia_start_date": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "International assignee start date", "text"),
        "ia_end_date": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "International assignee end date", "text"),
        "rut": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "RUT", "text"),
        "middle_name": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Middle Name", "text"),
        "time_type": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Time Type", "text"),
        "dob": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Date of Birth", "text"),
        "employee_type_udf": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Employee Group", "text"),
        "mgmt_lvl": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "Management Level", "text"),
        "assignment_type": rail.find_first_by_attr_and_get_attr(custom_field_values,"customField.displayText", "assignment_type", "text"),
    }

def get_filtered_user_timeoff_policy(response):
    if not response:
        return None
    return list(filter(lambda x:x['enabled'] in ["true", True], map(lambda item: {
        "name": item['timeOffType']['displayText'],
        "enabled":item['isTimeOffAllowedAgainstThisTimeOffType'],
        "uri": item['timeOffType']['uri'],
        "policy": item['policySetSchedule'] if item['policySetSchedule'] else []
    },response['policiesByTimeOffType'])))

def get_date_to_use_for_no_accrual(dag_run, default_return="", return_type='json'):
    if rail.result('prepare_update_payload', 'ia_updated') in [True, 'true', 'True']:
        if dag_run.conf['file_data']['is_ia'] in [1,'1']:
            if return_type == "str":
                return f"{dag_run.conf['json_formatted_dates']['ia_start_date']['year']}-{dag_run.conf['json_formatted_dates']['ia_start_date']['day']}-{dag_run.conf['json_formatted_dates']['ia_start_date']['month']}"
            return dag_run.conf['json_formatted_dates']['ia_start_date']
        if dag_run.conf['file_data']['is_ia'] in [0,'0']:
            _end_date = convert_json_date_to_date(dag_run.conf['json_formatted_dates']['ia_end_date']) + timedelta(days=1)
            if return_type == "str":
                return f"{_end_date.year}-{_end_date.day}-{_end_date.month}"
            return get_json_date_from_date(_end_date)
    return dag_run.conf['json_formatted_dates']['hire_date'] if default_return == "" else default_return

def get_item_index(dag_run, DAG_BATCH_COUNT):
    item_idx_for_dag_run = dag_run.conf.get('user_record_index', None) if dag_run.conf else None
    if not item_idx_for_dag_run:
        return random.randint(1, DAG_BATCH_COUNT)
    return item_idx_for_dag_run

def get_trigger_dag_id(trigger_dag_id, max_dag_batch_count, item_index):
    batch_number = (item_index % max_dag_batch_count) + 1
    prefix = f"_{batch_number}"
    if batch_number == 1:
        prefix = ""
    if batch_number not in range(1, max_dag_batch_count+1):
        raise Exception("Batch number is outside of max batch count")
    return f"{trigger_dag_id}{prefix}"

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

def get_end_date_to_use(dag_run):
    timeoff_details = rail.result("get_timeoff_details")[0]
    location = rail.result("get_users_effective_group_membership")['locations'][0] \
        if rail.result("get_users_effective_group_membership")['locations'] else {}

    _date = datetime.strptime(dag_run.conf['end_date'], INPUT_DATE_FORMAT)

    if timeoff_details['name'].startswith("[AUS]"):
        _date += timedelta(days=1)
    elif not location.get('location', {}).get('parent', {}):
        return _date
    elif location.get('location', {}).get('parent', {}).get('location', {}).get('displayText', "") == "Australia":
        _date += timedelta(days=1)
    else:
        pass  # initialized the value

    return _date


def add_new_policy_line(dag_run):
    _date = get_end_date_to_use(dag_run)
    return {
        "effectiveDate": {
            "day": _date.day,
            "month": _date.month,
            "year": _date.year
        },
        "description": f"Added by Integration on {_date.strftime('%d-%m-%Y')}",
        "policySet": {
            "timeOffBalanceEventScripts": [{
                "additionalParameters": [{
                    "keyUri": "urn:replicon:script-key:parameter:amount",
                    "value": {
                        "number": 0
                    }
                }],
                "script": {
                    "description": "Set initial balance for the first day of a policy",
                    "name": "Starting Balance Set To",
                    "uri": dag_run.conf['starting_balance_set_to_uri']
                }
            }],
            "timeOffValidationScripts": [{
                "additionalParameters": [{
                    "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                    "value": {
                        "number": "0"
                    }
                }],
                "script": {
                    "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                    "name": "Prevent balance overdraw",
                    "uri": dag_run.conf['prevent_balance_overdraw_uri']
                }
            }]
        }
    }


def get_timeoff_polices_to_assign_callable(dag_run):
    policies = loads(dag_run.conf['policy_set'])
    user_end_date: datetime = convert_json_date_to_date(
        dag_run.conf['user_end_date_json'])
    existing_policy = list(filter(
        lambda policy: convert_json_date_to_date(
            policy['effectiveDate']) < user_end_date,
        policies
    ))

    existing_policy.append(add_new_policy_line(dag_run))

    return existing_policy

def format_timeoff_polices_to_assign_callable():
    return dumps(rail.result("get_timeoff_polices_to_assign")
                ).replace("/null/", "\"effective\""
                ).replace("\"script\"", "\"scriptTarget\""
                ).replace('":{"additionalParameters', '":[{"additionalParameters'
                ).replace(':{"keyUri"', ':[{"keyUri"'
                ).replace('}},"scriptTarget"', '}}],"scriptTarget"'
                ).replace('}},"timeOffValidationScripts', '}}],"timeOffValidationScripts'
                ).replace('}}},"description', '}}]},"description')

def get_all_run_ids_callable(trigger_id, parallel_count):
    results = []
    for x in range(parallel_count):
        result = rail.result(f'{trigger_id}_{x+1}')
        if result is not None:
            results.append(result)
    return list(itertools.chain(*results))

def get_user_uri(dag_run, task_id='create_user'):
    if dag_run.conf.get('user_uri'):
        return dag_run.conf.get('user_uri')
    return rail.result(task_id)['uri']
