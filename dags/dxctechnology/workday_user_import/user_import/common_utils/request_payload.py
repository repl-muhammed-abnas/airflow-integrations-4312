from datetime import date, datetime, timedelta, timezone
import re
from typing import Callable
from pendulum import now as pendulum_now
from functools import lru_cache
from json import loads
from uuid import uuid4
import rail

null = None

INPUT_DATE_FORMAT = "%Y-%d-%m"
PARENT_DEPARTMENT_NAME = "DXC"
LOCATION_DELIMITER = " | "
EMPLOYEE_TYPE_DELIMITER = " | "
EMP_SUB_GROUP_CODE_CODES = "R9,R4,RA,R8,TH,T4,TJ,TC,TI,T8,TK,TG,P0,P1,P5,P6,W0,W1,W5,W6"

USER_IMPORT_MANDATORY_FIELDS = {
            "empid": "Emp ID",
            "firstname": "First Name",
            "lastname": "Last Name",
            "email": "Email Address",
            "country": "Location Country",
            "hiredate": "Hire Date",
            "status": "Status",
            "companycode": "Company Code",
            "supervisorid": "Manager ID",
            "costcenter": "Cost Center Code",
            "workshift": "Work Shift"
        }

# Worakto step 24(recipeID = 1599853) value:=> California,Colorado,Nevada,Puerto Rico,Rhode Island
STATE_TO_GROUP = ['California', 'Colorado', 'Nevada', 'Puerto Rico', 'Rhode Island']
PERSONNEL_SUB_AREA_CODE_TO_GROUP = ['U04A', 'U02A', 'U05A', 'U06A']
TIMESHEET_PERIOD_EFFECTIVE_DATE = "%d/%m/%Y"

def get_required_formatted_date_from_json_date(json_date, _format=INPUT_DATE_FORMAT):
    _date = date(json_date['year'], json_date['month'], json_date['day'])

    if _format:
        _date_str = _date.strftime(_format)

    else:
        _date_str = _date.strftime(INPUT_DATE_FORMAT)

    return _date_str

def get_json_date_from_date(_date):
    return {
        "day": _date.day,
        "month": _date.month,
        "year": _date.year
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

def get_todays_minus_specified_days_date_in_json(days_in_number:int, return_type="json"):
    today = datetime.now() -timedelta(days=days_in_number)
    if return_type == "date":
        return today.date()
    return {
        "day": today.day,
        "month": today.month,
        "year": today.year
    }

def get_todays_date_for_timezone_in_json(timezone="America/Los_Angeles"):
    today = pendulum_now(timezone).date()
    return {
        "day": today.day,
        "month": today.month,
        "year": today.year
    }

def get_home_state_and_country_for_ia(dag_run, item):
    if not item:
        return []
    home_country_to_use = item['_country_to_use_for_query']
    home_state_to_use = item['home_state']
    if item['isia']:
        if item['isia'] in [1,'1']:
            if  "home pay" in item['assignment_type'].lower():
                home_country_to_use = item['country']
                home_state_to_use = item['state']

    return home_country_to_use, home_state_to_use


def get_user_timeoff_balance_summary_payload(dag_run):
    return {
        "account": {
            "userUri": dag_run.conf["user_uri"],
            "timeOffTypeUri": dag_run.conf["timeoff_type_uri"]
        },
        "asOfDate": dag_run.conf["user_end_date_json"]
    }


def get_update_policy_payload(dag_run):
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf["user_uri"],
            "timeOffTypeUri": dag_run.conf["timeoff_type_uri"]
        },
        "policySetScheduleEntries": loads(rail.result("format_timeoff_polices_to_assign"))
    }


def get_department_creation_payload(item):
    return {
        "departmentGroup": {
            "name": null,
            "uri": null,
            "parent": {
                "uri": null,
                "parent": null,
                "name": PARENT_DEPARTMENT_NAME,
                "parameterCorrelationId": null
            },
            "parameterCorrelationId": null
        },
        "modifications": {
            "name": item["orgcode"],
            "codeToApply": null,
            "descriptionToApply":{
                "value": item["orgname"]
            } if item["orgname"] else null,
            "isEnabled": "true"
        },
        "unitOfWorkId": str(uuid4())
    }

def get_costcenter_creation_payload(item):
    code_description = { "value": item["costcentername"] } if item["costcentername"] else null
    return {
        "costCenter": {
            "name": null,
            "uri": null,
            "parent": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "name": item["costcenter"],
            "codeToApply": code_description,
            "descriptionToApply": code_description,
            "isEnabled": "true"
        },
        "unitOfWorkId": str(uuid4())
    }

def get_create_locations_payload(dag_run):
    location_name = dag_run.conf["country"] if dag_run.conf["length"] == "1" else dag_run.conf["state"]
    return {
        "location": {
            "name": null,
            "uri": null,
            "parent": {
                "uri" : rail.result("get_parent_location_details")[0]["uri"]
            } if dag_run.conf["length"] == "2" else null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "name": location_name,
            "codeToApply": null,
            "descriptionToApply": null,
            "isEnabled": "true"
        },
        "unitOfWorkId": str(uuid4())
    }

def get_create_service_center_payload(item):
    return {
        "serviceCenter": {
            "name": null,
            "uri": null,
            "parent": null,
            "parameterCorrelationId": null
        },
        "modifications": {
            "name": item["paygroup"],
            "codeToApply": null,
            "descriptionToApply": null,
            "isEnabled": "true"
        },
        "unitOfWorkId": str(uuid4())
    }

def get_parent_location_payload(dag_run):
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:location-list-column:location",
            "urn:replicon:location-list-column:full-path"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon:location-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf["parent_location_name"]
                }
            }
        }
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


#! Below Functions are for getting trigger conf for Global Users processing

@lru_cache(maxsize=32)
def get_replicon_udf_list():
    return rail.result("get_all_user_custom_fields")

def _mapper_derived_values(item, config, activities_holiday_canada_holiday_calender_allowed_country): # pylint:disable=unused-argument
    return {
                "office_schedule": item['mapper_office_schedule'],
                "authentication": item['mapper_authentication'],
                "authentication_uri": item['mapper_authentication_uri'],
                "timesheet_approval_path": item['mapper_timesheet_approval_path'],
                "timesheet_period": item['mapper_timesheet_period'],
                "timesheet_template": item['mapper_timesheet_template'],
                "end_user_permission": item['mapper_end_user_permission'],
                "supervisor_user_permission": item['mapper_supervisor_user_permission'],
                "product": item['mapper_product'],
                "product_uri": item['mapper_product_uri'],
                "language": item['mapper_language'],
                "language_uri": item['mapper_language_uri'],
                "supervisor_end_user_permission": item['mapper_supervisor_end_user_permission'],
                "schedule_type": item['mapper_schedule_type'],
                "schedule_type_uri": item['mapper_schedule_type_uri'],
                "timeoff_template": item['mapper_timeoff_template'],
                "end_user_permission_connect_emp" : item["mapper_end_user_permission_connect_emp"],
                "timesheet_approval_path_canada" : item["mapper_timesheet_approval_path_canada"],
                "timeoff_approval_path_canada" : item["mapper_timeoff_approval_path_canada"],
                "supervisor_scheduler_permission" : item["mapper_supervisor_scheduler_permission"],
                "psg" : item["mapper_psg"],
                "timeoff_template" : item["mapper_timeoff_template_name"],
                "master_recipe_timeoff_template_2_from_service_call" : item["mapper_timeoff_template"],
                # This is for debugging purpose
                # Actual values will be derived at the assignment level, with other columns as well
                "timeoffs": item['mapper_timeoffs'],
                "timeoff_approval" : item["mapper_timeoff_approval"],
                "time_entry_approval_path_name" : item["mapper_time_entry_approval_path_name"],
                "profile_status" : item["mapper_profile_status"],
                "timesheet_period_canada" : item["mapper_timesheet_period_canada"],
                "canada_timezone" : item["mapper_canada_timezone"],
                "canada_timezone_uri" : item["mapper_canada_timezone_uri"],
                "canada_holiday_calendar" : activities_holiday_canada_holiday_calender_allowed_country["mapper_canada_holiday_calendar"],
                "canada_holiday_calendar_uri" : rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_holiday_calendar"),
                    "displayText",
                    activities_holiday_canada_holiday_calender_allowed_country["mapper_holiday_calendar"]
                ),
                "canada_timesheet_template" : item["mapper_canada_timesheet_template"],
                "canada_payrule" : item["mapper_canada_payrule"],
                "canada_timesheet_period_effective_date" : item["mapper_canada_timesheet_period_effective_date"],
                # GBL has normal Timezone
                "timezone" : item["mapper_timezone"],
                "timezone_uri" : item["mapper_timezone_uri"],
                "work_week" : item["mapper_work_week"],
                "work_week_uri" : item["mapper_work_week_uri"],
                "activities" : item["mapper_activities"],
                "holiday_calendar" : activities_holiday_canada_holiday_calender_allowed_country["mapper_holiday_calendar"],
                "holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_holiday_calendar"),
                    "displayText",
                    activities_holiday_canada_holiday_calender_allowed_country["mapper_holiday_calendar"]
                )
            }



def _get_employee_type_full_path(item):
    return rail.smartjoin_by_delim(arr=[item['subareacode'], item['empgroupcode'], item['empsubgroupcode']], separator=" | ")

def _get_location_full_path(item):
    if item['_actual_state']:
        return f"{item['_actual_country']}{LOCATION_DELIMITER}{item['_actual_state']}"
    return item['_actual_country']

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
            "ee_group": replicon_udf_list.get("ee_group", {})
        }

@lru_cache(maxsize=32)
def _get_user_permission_set_details(end_user_permission, supervisor_user_permission, supervisor_end_user_permission, aus_supervisor_end_user_permission, supervisor_scheduler_permission, end_user_connect_employee=None):
    permissions_details = rail.result("get_all_permission_sets")
    return {
            "end_user_permission": rail.find_first_by_attr_and_get_attr(permissions_details, 'displayText',
                                                                        end_user_permission, default={}),
            "supervisor_user_permission": rail.find_first_by_attr_and_get_attr(permissions_details, 'displayText',
                                                                        supervisor_user_permission, default={}),
            "supervisor_end_user_permission": rail.find_first_by_attr_and_get_attr(permissions_details, 'displayText',
                                                                        supervisor_end_user_permission, default={}),
            "aus_supervisor_end_user_permission": rail.find_first_by_attr_and_get_attr(permissions_details, 'displayText',
                                                                        aus_supervisor_end_user_permission, default={}),
            "supervisor_scheduler_permission": rail.find_first_by_attr_and_get_attr(permissions_details, 'displayText',
                                                                        supervisor_scheduler_permission, default={}),
            "connect_employee": rail.find_first_by_attr_and_get_attr(permissions_details, 'displayText',
                                                                     end_user_connect_employee, default={}) if end_user_connect_employee else {}
        }

def is_user_canada_c1(parent_company_code, user_country):
    if parent_company_code.lower() == "c1" and user_country.lower() == "canada":
            return True
    return False

def get_users_parent_company_code(user_parent_company_code):
    if user_parent_company_code.lower() == "ftp":
        return False, False, True, False, null, "ftp"
    
    if user_parent_company_code.lower() == "gsap":
        return False, False, False, True, null, "gsap"
    
    if user_parent_company_code.lower() == "c1":
        return True, False, False, False, null, "c1"
    
    if user_parent_company_code.lower() == "compass":
        return False, True, False, False, null, "compass"

    # This is for non go-live where parent is not found
    # C1, COMPASS, FTP, GSAP, NON_LIVE, 'parent_comply_code_value
    return False, False, False, False, True, null


@lru_cache(maxsize=16)
def get_groups_data():
    return rail.result("get_all_employeegroup_data")['employee_data_for_assignment'], rail.result("get_all_locations"), rail.result("get_all_enabled_departments"),\
            rail.result('get_all_companycode_data'), rail.result('get_all_cost_centers')


def user_is_compass_and_country_usa_and_is_international_assignment_home_country_is_not_ind_prt_cri(item, is_user_compass):
    if is_user_compass and item['_country_to_use_for_query'].lower() == "united states of america":
        if item['ia'] == [1, '1']:
            if item['_country_to_use_for_query'] not in ["india", "portugal", "costa rica"]:
                return True
    return False

def get_activities_holiday_canada_holiday_calender_allowed_country(holiday_calendar_data, item,is_user_c1, is_user_compass, is_user_ftp, is_user_gsap):
    if is_user_ftp:
        return {
            "dev-comment": "workato step 31",
            "mapper_activities": item["mapper_activities"],
            "mapper_holiday_calendar": item['mapper_holiday_calendar'],
            "mapper_holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(holiday_calendar_data, 'displayText', item['mapper_holiday_calendar'], 'uri'),
            "mapper_canada_activities": item['mapper_canada_activities'],
            "mapper_canada_holiday_calendar": item["mapper_canada_holiday_calendar"],
            "mapper_canada_holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(holiday_calendar_data, 'displayText', item['mapper_canada_holiday_calendar'], 'uri'),
            "allowed_country" : "Enable"
        }

    if not (is_user_gsap and item['_country_to_use_for_query'].lower() == "australia"):
        if not (item['_country_to_use_for_query'].lower() == "costa rica" and is_user_compass and item['isia'] in [1,'1']):
            # everything user below if
            if not (item['_country_to_use_for_query'].lower() == "australia" and is_user_compass and item['isia'] in [1,'1']):
                if item['_country_to_use_for_query'].lower() not in ["United States of America", "Puerto Rico", "India", "Portugal", "Costa Rica", "Australia"]:
                    return {
                        "dev-comment": "workato step 53",
                        "mapper_activities": item["mapper_non_usa_pri_ind_prt_cri_aus_countries_activities"],
                        "mapper_holiday_calendar": item['mapper_non_usa_pri_ind_prt_cri_aus_countries_holidaycalander'],
                        "mapper_holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                            holiday_calendar_data, 'displayText', item['mapper_non_usa_pri_ind_prt_cri_aus_countries_holidaycalander'], 'uri'),
                        "mapper_canada_activities": item['mapper_canada_activities'],
                        "mapper_canada_holiday_calendar": item["mapper_canada_holiday_calendar"],
                        "mapper_canada_holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                            holiday_calendar_data, 'displayText', item['mapper_canada_holiday_calendar'], 'uri'),
                        "allowed_country" : item["mapper_non_usa_pri_ind_prt_cri_aus_countries_allowed_country"]
                    }
                if not is_user_compass and item['_country_to_use_for_query'].lower() == "australia":
                    return {
                        "dev-comment": "workato step 55",
                        "mapper_activities": item["mapper_non_usa_pri_ind_prt_cri_aus_countries_activities"],
                        "mapper_holiday_calendar": item['mapper_non_usa_pri_ind_prt_cri_aus_countries_holidaycalander'],
                        "mapper_holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                            holiday_calendar_data, 'displayText', item['mapper_non_usa_pri_ind_prt_cri_aus_countries_holidaycalander'], 'uri'),
                        "mapper_canada_activities": item['mapper_canada_activities'],
                        "mapper_canada_holiday_calendar": item["mapper_canada_holiday_calendar"],
                        "mapper_canada_holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                            holiday_calendar_data, 'displayText', item['mapper_canada_holiday_calendar'], 'uri'),
                        "allowed_country" : item["mapper_non_usa_pri_ind_prt_cri_aus_countries_allowed_country"]
                    }
                if not is_user_compass and item['_country_to_use_for_query'].lower() == "portugal":
                    return {
                        "dev-comment": "workato step 61",
                        "mapper_activities": item["mapper_non_usa_pri_ind_prt_cri_aus_countries_activities"],
                        "mapper_holiday_calendar": item['mapper_prt_not_compass_holidaycalander_canada_holidaycalendar'],
                        "mapper_holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                            holiday_calendar_data, 'displayText', item['mapper_prt_not_compass_holidaycalander_canada_holidaycalendar'], 'uri'),
                        "mapper_canada_activities": item['mapper_canada_activities'],
                        "mapper_canada_holiday_calendar": item["mapper_prt_not_compass_holidaycalander_canada_holidaycalendar"],
                        "mapper_canada_holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                            holiday_calendar_data, 'displayText', item['mapper_prt_not_compass_holidaycalander_canada_holidaycalendar'], 'uri'),
                        "allowed_country" : item["mapper_non_usa_pri_ind_prt_cri_aus_countries_allowed_country"]
                    }
                if not is_user_compass and item['_country_to_use_for_query'].lower() == "costa rica":
                    return {
                        "dev-comment": "workato step 65",
                        "mapper_activities": item["mapper_non_usa_pri_ind_prt_cri_aus_countries_activities"],
                        "mapper_holiday_calendar": item['mapper_prt_not_compass_holidaycalander_canada_holidaycalendar'],
                        "mapper_holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                            holiday_calendar_data, 'displayText', item['mapper_prt_not_compass_holidaycalander_canada_holidaycalendar'], 'uri'),
                        "mapper_canada_activities": item['mapper_canada_activities'],
                        "mapper_canada_holiday_calendar": item["mapper_prt_not_compass_holidaycalander_canada_holidaycalendar"],
                        "mapper_canada_holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                            holiday_calendar_data, 'displayText', item['mapper_prt_not_compass_holidaycalander_canada_holidaycalendar'], 'uri'),
                        "allowed_country" : item["mapper_non_usa_pri_ind_prt_cri_aus_countries_allowed_country"]
                    }
                if not is_user_compass and item['_country_to_use_for_query'].lower() == "india":
                    return {
                        "dev-comment": "workato step 69",
                        "mapper_activities": item["mapper_non_usa_pri_ind_prt_cri_aus_countries_activities"],
                        "mapper_holiday_calendar": item['mapper_prt_not_compass_holidaycalander_canada_holidaycalendar'],
                        "mapper_holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                            holiday_calendar_data, 'displayText', item['mapper_prt_not_compass_holidaycalander_canada_holidaycalendar'], 'uri'),
                        "mapper_canada_activities": item['mapper_canada_activities'],
                        "mapper_canada_holiday_calendar": item["mapper_prt_not_compass_holidaycalander_canada_holidaycalendar"],
                        "mapper_canada_holiday_calendar_uri":rail.find_first_by_attr_and_get_attr(
                            holiday_calendar_data, 'displayText', item['mapper_prt_not_compass_holidaycalander_canada_holidaycalendar'], 'uri'),
                        "allowed_country" : item["mapper_non_usa_pri_ind_prt_cri_aus_countries_allowed_country"]
                    }
                if item['_country_to_use_for_query'].lower() == "puerto rico" and not is_user_c1:
                    return {
                        "dev-comment": "workato step 31",
                        "mapper_activities": item["mapper_activities"],
                        "mapper_holiday_calendar": item['mapper_holiday_calendar'],
                        "mapper_holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                            holiday_calendar_data, 'displayText', item['mapper_holiday_calendar'], 'uri'),
                        "mapper_canada_activities": item['mapper_canada_activities'],
                        "mapper_canada_holiday_calendar": item["mapper_canada_holiday_calendar"],
                        "mapper_canada_holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                            holiday_calendar_data, 'displayText', item['mapper_canada_holiday_calendar'], 'uri'),
                        "allowed_country" : "Enable"
                    }
                if user_is_compass_and_country_usa_and_is_international_assignment_home_country_is_not_ind_prt_cri(item, is_user_compass):
                    # this condition is taken care in different scenario 
                    # Failing this as this should not be executed
                    return {
                        "process_record": "No",
                        "dev-comment": "",
                        "mapper_activities": "",
                        "mapper_holiday_calendar": "",
                        "mapper_holiday_calendar_uri": "",
                        "mapper_canada_activities": "",
                        "mapper_canada_holiday_calendar": "",
                        "mapper_canada_holiday_calendar_uri": "",
                        "allowed_country" : ""
                    }
    # this needs to be check as gbl will be sent as default, but which one to be sent needs to be checked and 
    # updated here
    return {
            "process_record": "No",
            "dev-comment": "",
            "mapper_activities": "",
            "mapper_holiday_calendar": "",
            "mapper_holiday_calendar_uri": "",
            "mapper_canada_activities": "",
            "mapper_canada_holiday_calendar": "",
            "mapper_canada_holiday_calendar_uri": "",
            "allowed_country" : ""
        }

@lru_cache(maxsize=16)
def cached_write_json_artifact(data_task_id):
    return rail.write_json_artifact(rail.result(data_task_id))

@lru_cache(maxsize=8)
def get_holiday_calender_details():
    return rail.result('get_all_holiday_calendar')

def get_global_user_process_conf(item, dag_run, config):
    employee_data, location_data, department_data, division_data, cost_center_data = get_groups_data()
    # TODO: add it to documentation file
    # this will be used to determine which values that will be sent to child for processing 
    # from Process Users to Add/Update User (Workato)
    parent_company = item['parent_company']
    if parent_company is None:
        parent_company = ""
    is_user_c1_canada = is_user_canada_c1(parent_company, item['_country_to_use_for_query'])
    
    # this will be used to determine which values that will be sent to child for processing 
    # From batch_processor to process users (workato)
    is_user_c1, is_user_compass, is_user_ftp, is_user_gsap, non_live_user, parent_company_key = get_users_parent_company_code(parent_company)

    company_code_uri = rail.find_first_by_attr_and_get_attr(
                    division_data,
                    "full_path",
                    item["company_code_full_path"],
                    default={}
                )
    
    holiday_calendar_data = get_holiday_calender_details()
    activities_holiday_canada_holiday_calender_allowed_country = get_activities_holiday_canada_holiday_calender_allowed_country(holiday_calendar_data,
                                                                                            item,is_user_c1,is_user_compass,is_user_ftp,is_user_gsap)
    _country = item["_country_to_use_for_query"]
    _state = item["_state_to_use_for_query"]
    _home_country, _home_state = get_home_state_and_country_for_ia(dag_run, item)

    policy_data = rail.result('get_all_policy_sets')
    return {
            "item" : item, # this is added for ref only
            "starting_balance_set_to_uri": dag_run.conf.get("starting_balance_set_to_uri"),
            "prevent_balance_overdraw_uri": dag_run.conf.get("prevent_balance_overdraw_uri"),
            "supervisor_user_log": dag_run.conf.get("supervisor_user_log"),
            "employee_type_data": cached_write_json_artifact('get_all_employeegroup_data'),
            "division_data": cached_write_json_artifact('get_all_companycode_data'),
            "master_file_name": dag_run.conf["file_name"],
            "splitter_batch_name": item["splitter_batch_name"],
            "allowed_country": activities_holiday_canada_holiday_calender_allowed_country['allowed_country'],
            "replicon_field": 'true' if item['status'] in [1,'1'] else 'false',
            "file_data" : {
                "emp_id": item["empid"],
                "perner_id": item["pernerid"],
                "email_id": item["email"],
                "first_name": item["firstname"],
                "last_name": item["lastname"],
                "country": _country,
                "state": _state,
                "exempt": item["exempt"],
                "exempt_effective_date": item["exempteffectivedate"],
                "employee_type": item["employeetype"],
                "hire_date": item["hiredate"],
                "gender": item["gender"],
                "service_date": item["servicedate"],
                "term_date": item["termdate"],
                "status": item["status"],
                "on_leave": item["onleave"],
                "parent_company": parent_company,
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
                "home_country": _home_country,
                "home_state": _home_state,
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
                "assignment_type": item["assignment_type"]
            },
            "mapper_data": _mapper_derived_values(item, config, activities_holiday_canada_holiday_calender_allowed_country),
            "payrule": {
                "payrule": item['mapper_canada_payrule'] if is_user_c1_canada else null,
            },
            "activities": {
                "activity" : activities_holiday_canada_holiday_calender_allowed_country['mapper_activities']
            },
            "user_permission_sets" : _get_user_permission_set_details(                
                item['mapper_end_user_permission'],
                item['mapper_supervisor_user_permission'],
                item['mapper_supervisor_end_user_permission'],
                item['mapper_aus_supervisor_end_user_permission'],
                item['mapper_supervisor_scheduler_permission']
            ),
            "schedule_data": {
                "work_schedule" : item['workshift'],
                "work_schedule_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_office_schedules"),
                                                                          "displayText",item['workshift'], 'uri'),
                "office_schedule" : item['mapper_office_schedule']
            },
            "policy_sets": {
                "timeoff_template": rail.find_first_by_attr_and_get_attr(policy_data, 'name', item['mapper_timeoff_template_name']),
                "timesheet_period": rail.find_first_by_attr_and_get_attr(policy_data, "name", item['mapper_timesheet_period']),
                "timesheet_approval_path": rail.find_first_by_attr_and_get_attr(policy_data, "name", item['mapper_timesheet_approval_path']),
                "timesheet_template": rail.find_first_by_attr_and_get_attr(policy_data, "name", item['mapper_timesheet_template'])

            },
            "timezone": {
                "timezone": item["mapper_timezone"],
                "timezone_uri" : item["mapper_timezone_uri"]
            },
            "udfs": _get_user_udfs_details(),
            "groups": {
                # Location
                "location": rail.find_first_by_attr_and_get_attr(
                    location_data,
                    'fullpath',
                    _get_location_full_path(item),
                    default={}
                ),
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
                                    default={})
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
                #TimeSheetPeriodEffectiveDate #! mapper value
                "timesheet_period_effective_date": get_json_date_from_date_str(item['timesheet_period_effective_date'], TIMESHEET_PERIOD_EFFECTIVE_DATE),
            }
        }

def _mapper_derived_values_gsap(item, config, diff_caller_data): # pylint:disable=unused-argument
    return {
                "timesheet_template": diff_caller_data['timesheet_template'],
                "office_schedule": item['mapper_office_schedule'],
                "authentication": item['mapper_authentication'],
                "authentication_uri": item['mapper_authentication_uri'],
                "timesheet_approval_path": diff_caller_data['timesheet_apprval_path'],
                "timesheet_period": diff_caller_data['timesheet_period'],
                "end_user_permission": item['mapper_end_user_permission'],
                "supervisor_user_permission": item['mapper_supervisor_user_permission'],
                "product": item['mapper_product'],
                "product_uri": item['mapper_product_uri'],
                "language": item['mapper_language'],
                "language_uri": item['mapper_language_uri'],
                "supervisor_end_user_permission": item['mapper_supervisor_end_user_permission'],
                "schedule_name": diff_caller_data['schedule_name'],
                "schedule_type_uri": item['mapper_scheduletype_uri'],
                "timeoff_template": item['mapper_timeoff_template'],
                "end_user_permission_connect_emp" : item["mapper_end_user_permission_connect_emp"],
                "supervisor_scheduler_permission" : item["mapper_supervisor_scheduler_permission"],
                # "psg" : item["mapper_psg"],
                "timeoff_template" : item["mapper_timeoff_template"],
                "master_recipe_timeoff_template_2_from_service_call" : item["mapper_timeoff_template_master"],
                "timeoff_approval" : item["mapper_timeoff_approval"],
                "time_entry_approval_path_name" : item["mapper_timeentry_approval_path"],
                "profile_status" : item["mapper_profile_status"],
                # GBL has normal Timezone
                "timezone" : diff_caller_data["timezone"],
                "timezone_uri" : diff_caller_data["timezone_uri"],
                "work_week" : item["mapper_work_week"],
                "work_week_uri" : item["mapper_work_week_uri"],
                "activities" : diff_caller_data["activity_list"],
                "holiday_calendar" : diff_caller_data["holiday_calendar"],
                "holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_holiday_calendar"),
                    "displayText",
                    diff_caller_data["holiday_calendar"]
                ),
                "diff_caller_data": diff_caller_data,
                "termination_reason_code": item["mapper_termination_reason_code"]
            }

def get_diff_caller_data(config, item, is_gsap_user, is_compass_user, schedule_uri, office_schedule_data):
    employee_type = item['ausjc'] if item['ausjc'] else item['industrialinstrumentclassification']
    
    if item['empsubgroupcode']:
        empsubgroupcode_in_emp_group_code_list = item['empsubgroupcode'] in EMP_SUB_GROUP_CODE_CODES
    else:
        # "" in EMP_SUB_GROUP_CODE_CODES will return True which is not correct for us
        empsubgroupcode_in_emp_group_code_list = False
    if item['_country_to_use_for_query'] == "Australia" and is_gsap_user:

        if item['areacode'] != "AU36" and empsubgroupcode_in_emp_group_code_list and item['areacode'] != "AU33":
            return {
                "timesheet_template": item["mapper_timesheet_template"],
                "payrule": item["mapper_payrule"],
                "timesheet_apprval_path": item["mapper_timesheet_approval_path"],
                "employee_type_full_path": employee_type,
                "timeoff_template":item["mapper_timeoff_template"],
                "timeoff_approval":item["mapper_timeoff_approval"],
                "timesheet_period":item["mapper_timesheet_period"],
                "activity_list":item["mapper_activities"],
                "timezone":item["mapper_timezone"],
                "timezone_uri":item["mapper_timezone_uri"],
                "holiday_calendar":item["mapper_holiday_calendar"],
                "allowed_country":item["mapper_allowed_country"],
                "schedule_name": ("Shift" if item['workshift'].lower().startswith('r') else item['workshift']) if item['workshift'] else null,
                "schedule_uri": schedule_uri,
                "timesheet_period_effective_date": item['mapper_timesheet_period_effective_date']
            }

        if item['areacode'] == "AU33" and empsubgroupcode_in_emp_group_code_list:
            return {
                "timesheet_template": item["mapper_timesheet_template"],
                "payrule": item["mapper_payrule"],
                "timesheet_apprval_path": item["mapper_timesheet_approval_path"],
                "employee_type_full_path": employee_type,
                "timeoff_template":item["mapper_timeoff_template"],
                "timeoff_approval":item["mapper_timeoff_approval"],
                "timesheet_period":item["mapper_timesheet_period"],
                "activity_list":item["mapper_activities"],
                "timezone":item["mapper_timezone"],
                "timezone_uri":item["mapper_timezone_uri"],
                "holiday_calendar":item["mapper_holiday_calendar"],
                "allowed_country":item["mapper_allowed_country"],
                "schedule_name": ("Shift" if item['workshift'].lower().startswith('r') else item['workshift']) if item['workshift'] else null,
                "schedule_uri": schedule_uri,
                "timesheet_period_effective_date": item['mapper_timesheet_period_effective_date']
            }
        
        if item['areacode'] == "AU36" and not empsubgroupcode_in_emp_group_code_list and item['companycode'] == "3124":
            return {
                "timesheet_template": item["mapper_timesheet_template_au36_3124"],
                "payrule": item["mapper_payrule_au36_3124"],
                "timesheet_apprval_path": item["mapper_timesheet_approval_path"],
                "employee_type_full_path": employee_type,
                "timeoff_template":item["mapper_timeoff_template"],
                "timeoff_approval":item["mapper_timeoff_approval"],
                "timesheet_period":item["mapper_timesheet_period"],
                "activity_list":item["mapper_activities"],
                "timezone":item["mapper_timezone"],
                "timezone_uri":item["mapper_timezone_uri"],
                "holiday_calendar":item["mapper_holiday_calendar"],
                "allowed_country":item["mapper_allowed_country"],
                "schedule_name": ("Shift" if item['workshift'].lower().startswith('r') else item['workshift']) if item['workshift'] else null,
                "schedule_uri": schedule_uri,
                "timesheet_period_effective_date": item['mapper_timesheet_period_effective_date']
            }

        if item['areacode'] == "AU36" and empsubgroupcode_in_emp_group_code_list and item['companycode'] == "3124":
            return {
                "timesheet_template": item["mapper_timesheet_template_au36_3124_empsubgrp_has_r9"],
                "payrule": item["mapper_payrule_au36_3124_empsubgrp_has_r9"],
                "timesheet_apprval_path": item["mapper_timesheet_approval_path"],
                "employee_type_full_path": employee_type,
                "timeoff_template":item["mapper_timeoff_template"],
                "timeoff_approval":item["mapper_timeoff_approval"],
                "timesheet_period":item["mapper_timesheet_period"],
                "activity_list":item["mapper_activities"],
                "timezone":item["mapper_timezone"],
                "timezone_uri":item["mapper_timezone_uri"],
                "holiday_calendar":item["mapper_holiday_calendar"],
                "allowed_country":item["mapper_allowed_country"],
                "schedule_name": ("Shift" if item['workshift'].lower().startswith('r') else item['workshift']) if item['workshift'] else null,
                "schedule_uri": schedule_uri,
                "timesheet_period_effective_date": item['mapper_timesheet_period_effective_date']
            }

        if item['areacode'] != "AU36" and not empsubgroupcode_in_emp_group_code_list:
            return {
                "timesheet_template": item["mapper_timesheet_template_notau36_empsubgrp_notin_r9list"],
                "payrule": item["mapper_payrule_notau36_empsubgrp_notin_r9list"],
                "timesheet_apprval_path": item["mapper_timesheet_approval_path"],
                "employee_type_full_path": employee_type,
                "timeoff_template":item["mapper_timeoff_template"],
                "timeoff_approval":item["mapper_timeoff_approval"],
                "timesheet_period":item["mapper_timesheet_period"],
                "activity_list":item["mapper_activities"],
                "timezone":item["mapper_timezone"],
                "timezone_uri":item["mapper_timezone_uri"],
                "holiday_calendar":item["mapper_holiday_calendar"],
                "allowed_country":item["mapper_allowed_country"],
                "schedule_name": ("Shift" if item['workshift'].lower().startswith('r') else item['workshift']) if item['workshift'] else null,
                "schedule_uri": schedule_uri,
                "timesheet_period_effective_date": item['mapper_timesheet_period_effective_date']
            }
    else:
        if item['_country_to_use_for_query'] == "Australia" and is_compass_user:
            return {
                "timesheet_template": item["mapper_timesheet_template_aus_compass"],
                "payrule": item["mapper_payrule_ia1_compass_aus"],
                "timesheet_apprval_path": item["mapper_timesheet_approval_path_ia1_compass_aus"],
                "employee_type_full_path": employee_type,
                "timeoff_template":item["mapper_timeoff_template_ia1_compass_aus"],
                "timeoff_approval":item["mapper_timeoff_approval_ia1_compass_aus"],
                "timesheet_period":item["timesheet_period_ia1_compass_aus"],
                "activity_list":item["mapper_activity_list_ia1_compass_aus"],
                "timezone":item["mapper_timezone_ia1_compass_aus"],
                "timezone_uri":item["mapper_timezone_uri_ia1_compass_aus"],
                "holiday_calendar":item["mapper_holiday_calendar_ia1_compass_aus"],
                "allowed_country":item["mapper_allowed_country_ia1_compass_aus"],
                "schedule_name":item['mapper_schedule_name_ia1_compass_aus'],
                "schecule_name_uri":item["mapper_schedule_name_uri_ia1_compass_aus"],
                "timesheet_period_effective_date": item['mapper_timesheet_period_effective_date_ia1_compass_aus'],
                "schedule_uri": get_schedule_uri(item, config, office_schedule_data, "country")
            }

    return {
                "process_record": "No", # Record skipped bcz combination is not available in mapper
                "timesheet_template": "",
                "payrule": "",
                "timesheet_apprval_path": "",
                "employee_type_full_path": "",
                "timeoff_template":"",
                "timeoff_approval": "",
                "timesheet_period": "",
                "activity_list": "",
                "timezone": "",
                "timezone_uri": "",
                "holiday_calendar": "",
                "allowed_country": "",
                "schedule_name": "",
                "schedule_uri": "",
                "timesheet_period_effective_date": ""
            }

def _is_user_connect_employee(item):
    if item['areacode'] == "AU36":
        if item['companycode'] == "3124":
            return "Yes"
    return "No"

def get_schedule_uri(item, config, office_schedule_data, country_home_country):
    mapper_shift_values = list(filter(lambda row: row['Source']==item['workshift'], config.MAPPER))
    if mapper_shift_values and mapper_shift_values[0]['Value'] == "Office Schedule":
        value_to_assign = []
        if item['fulltimeparttime'] == "Full Time":
            value_to_assign = list(filter(lambda m_row: m_row['Type'] == "Schedule" and
                                 m_row['Country']==item[country_home_country] and 
                                 m_row['URI'] == "Office Schedule" and
                                 m_row['personnelsubarea'] == item['scheduledweeklyhours'] and 
                                 m_row['employeegroup'] == "Full Time", config.MAPPER))
            if value_to_assign:
                value_to_assign == value_to_assign[0]['Value']
        else:
            value_to_assign == item['workshift']
        if value_to_assign:
            return rail.find_first_by_attr_and_get_attr(
                office_schedule_data,
                "displayText",
                value_to_assign
            )
    return null

def get_gsap_user_process_conf(item, dag_run, config):
    employee_data, location_data, department_data, division_data, cost_center_data = get_groups_data()
    # TODO: add it to documentation file
    # this will be used to determine which values that will be sent to child for processing 
    # from Process Users to Add/Update User (Workato)
    parent_company = item['parent_company']
    if parent_company is None:
        parent_company = ""
    
    # this will be used to determine which values that will be sent to child for processing 
    # From batch_processor to process users (workato)
    _, is_user_compass, _, is_user_gsap, _, _ = get_users_parent_company_code(parent_company)

    company_code_uri = rail.find_first_by_attr_and_get_attr(
                    division_data,
                    "full_path",
                    item["company_code_full_path"],
                    default={}
                )
    shift_data = rail.result("get_all_office_schedules")
    schedule_uri = (rail.find_first_by_attr_and_get_attr(
            shift_data,
            'displayText',
            item['workshift'],
            'uri'
    ) if (("Shift" if item['workshift'].lower().startswith('r') else "Office Schedule") == "Office Schedule") else null) if item['workshift'] else null

    mapper_schedule_type_uri_compass_list = list(filter(lambda row: row['Type'] == "Schedule Type" and row['Country'] == item['_country_to_use_for_query'] and row['Source'] == item['workshift'], config.MAPPER))

    if mapper_schedule_type_uri_compass_list:
        mapper_schedule_type_uri_compass = mapper_schedule_type_uri_compass_list[0]['URI']
    else:
        mapper_schedule_type_uri_compass = ""

    _country = item["_country_to_use_for_query"]
    _state = item["_state_to_use_for_query"]
    _home_country, _home_state = get_home_state_and_country_for_ia(dag_run, item)

    diff_caller_data = get_diff_caller_data(config, item, is_user_gsap, is_user_compass, schedule_uri, shift_data)
    rail.set_result(key="diff_caller_data", val=diff_caller_data)

    holiday_calendar_data = get_holiday_calender_details()
    policy_data = rail.result('get_all_policy_sets')
    aus_job_change_effective_date_sample_date = next(filter(lambda row: row['Type']=="Job Effective Date Sample Date" and row['Country']=="Australia", config.DXC_WORKDAY_USER_SYNC_USER_MAPPER), {}).get('Value', {})
    return {
            "item" : item, # this is added for ref only
            "connect_employee": _is_user_connect_employee(item),
            "starting_balance_set_to_uri": dag_run.conf.get("starting_balance_set_to_uri"),
            "prevent_balance_overdraw_uri": dag_run.conf.get("prevent_balance_overdraw_uri"),
            "supervisor_user_log": dag_run.conf.get("supervisor_user_log"),
            "employee_type_data": cached_write_json_artifact('get_all_employeegroup_data'),
            "division_data": cached_write_json_artifact('get_all_companycode_data'),
            "master_file_name": dag_run.conf["file_name"],
            "splitter_batch_name": item["splitter_batch_name"],
            "allowed_country": diff_caller_data['allowed_country'],
            "replicon_field": 'true' if item['status'] in [1,'1'] else 'false',
            "aus_job_change_effective_date_sample_date": aus_job_change_effective_date_sample_date,
            "file_data" : {
                "emp_id": item["empid"],
                "perner_id": item["pernerid"],
                "email_id": item["email"],
                "first_name": item["firstname"],
                "last_name": item["lastname"],
                "country": _country,
                "state": _state,
                "exempt": item["exempt"],
                "exempt_effective_date": item["exempteffectivedate"],
                "employee_type": item["employeetype"],
                "hire_date": item["hiredate"],
                "gender": item["gender"],
                "service_date": item["servicedate"],
                "term_date": item["termdate"],
                "status": item["status"],
                "on_leave": item["onleave"],
                "parent_company": parent_company,
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
                "home_country": _home_country,
                "home_state": _home_state,
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
                "assignment_type": item["assignment_type"]
            },
            "mapper_data": _mapper_derived_values_gsap(item, config, diff_caller_data),
            "payrule": {
                "payrule": diff_caller_data['payrule'],
            },
            "activities": {
                "activity" : diff_caller_data['activity_list']
            },
            "holiday_calendar": {
                "holiday_calendar": diff_caller_data['holiday_calendar'],
                "holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                    holiday_calendar_data,
                    "displayText",
                    diff_caller_data['holiday_calendar']
                )
            },
            "user_permission_sets" : _get_user_permission_set_details(                
                item['mapper_end_user_permission'],
                item['mapper_supervisor_user_permission'],
                item['mapper_supervisor_end_user_permission'],
                item['mapper_aus_supervisor_end_user_permission'],
                item['mapper_supervisor_scheduler_permission']
            ),
            "schedule_data": {
                "is_office_schedule": True if item['workshift'] and (not item['workshift'].lower().startswith('r')) else False,
                "schedule": rail.find_first_by_attr_and_get_attr(
                    shift_data,
                    "displayText",
                    item['ausjc'] if diff_caller_data["schedule_name"] == "Shift" else diff_caller_data['schedule_name']
                ),
                "schedule_name": diff_caller_data["schedule_name"],
                "work_schedule" : item['workshift'],
                "work_schedule_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_office_schedules"),
                                                                          "displayText",item['workshift'], 'uri'),
                "office_schedule" : item['mapper_office_schedule'],
                "schedule_type_uri": item['mapper_schedule_type_uri'] if not is_user_compass else mapper_schedule_type_uri_compass,
                "schedule_uri": schedule_uri
            },
            "policy_sets": {
                "punch_entry_policy": {},
                "timeoff_template": rail.find_first_by_attr_and_get_attr(policy_data, 'name', diff_caller_data['timeoff_template'], default={}),
                "timesheet_period": rail.find_first_by_attr_and_get_attr(policy_data, "name", diff_caller_data['timesheet_period'], default={}),
                "timesheet_approval_path": rail.find_first_by_attr_and_get_attr(policy_data, "name", diff_caller_data['timesheet_apprval_path'], default={}),
                "timesheet_template": rail.find_first_by_attr_and_get_attr(policy_data, "name", diff_caller_data['timesheet_template'], default={})
            },
            "timezone": {
                "timezone": item["mapper_timezone"],
                "timezone_uri" : item["mapper_timezone_uri"]
            },
            "udfs": _get_user_udfs_details(),
            "groups": {
                # Location
                "location": rail.find_first_by_attr_and_get_attr(
                    location_data,
                    'fullpath',
                    _get_location_full_path(item),
                    default={}
                ),
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
                    "ausjc": rail.find_first_by_attr_and_get_attr(employee_data,
                                    'full_path',
                                    item['ausjc'],
                                    default={}),
                    "industrial_instrument_classification": rail.find_first_by_attr_and_get_attr(employee_data,
                                    'full_path',
                                    item['industrialinstrumentclassification'],
                                    default={}),
                    'uri': rail.find_first_by_attr_and_get_attr(employee_data,
                                    'full_path',
                                    diff_caller_data['employee_type_full_path'],
                                    'uri',
                                    default="")
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
                #TimeSheetPeriodEffectiveDate#! mapper value
                "timesheet_period_effective_date": get_json_date_from_date_str(diff_caller_data['timesheet_period_effective_date'], TIMESHEET_PERIOD_EFFECTIVE_DATE),
                "aus_job_change_effective_date_sample_date": get_json_date_from_date_str(aus_job_change_effective_date_sample_date, TIMESHEET_PERIOD_EFFECTIVE_DATE)
            }
        }

def _mapper_derived_values_portugal(item):
    return {
        "authentication": item["mapper_authentication"],
        "office_schedule": item['mapper_office_schedule'],
        "authentication_uri": item["mapper_authentication_uri"],
        "end_user_permission": item["mapper_end_user_permission"],
        "supervisor_user_permission": item["mapper_supervisor_user_permission"],
        "product": item["mapper_product"],
        "product_uri": item["mapper_product_uri"],
        "language": item["mapper_language"],
        "language_uri": item["mapper_language_uri"],
        "supervisor_end_user_permission": item["mapper_supervisor_end_user_permission"],
        "end_user_permission_connect_emp": item["mapper_end_user_permission_connect_emp"],
        "work_week": item["mapper_work_week"],
        "work_week_uri": item["mapper_work_week_uri"],
        "timesheet_approval_path": item["mapper_timesheet_approval_path_portugal_compass"],
        "timesheet_template": item["mapper_timesheet_template_portugal_compass"],
        "timeoff_template": item["mapper_timeoff_template_portugal_compass"],
        "master_recipe_timeoff_template_2_from_service_call" : item["mapper_timeoff_template_master"],
        "timeoff_approval": item["mapper_timeoff_approval_portugal_compass"],
        "timesheet_period": item["mapper_timesheet_period_portugal_compass"],
        "time_entry_approval_path_name" : item["mapper_timeentry_approval_path"],
        "profile_status" : item["mapper_profile_status"],
        "activities": item["mapper_activity_list__portugal_compass"],
        "timezone": item["mapper_timezone_portugal_compass"],
        "timezone_uri": item["mapper_timezone_uri_portugal_compass"],
        "holiday_calendar": item["mapper_holiday_calendar_portugal_compass"],
        "holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_holiday_calendar"),
                    "displayText",
                    item["mapper_holiday_calendar_portugal_compass"]
                ),
        "country_to_enable": item["mapper_country_to_enable_portugal_compass"],
        "timesheet_period_effective_date": item["mapper_timesheet_period_effective_date_portugal_compass"],
        "payrule": item["mapper_payrule_portugal_compass"],
        "punch_entry_policy": item["mapper_punch_entry_policy_portugal_compass"],
        "schedule_name": item["mapper_schedule_name_portugal_compass"]
    }

def portugal_calculate_work_week_date(work_week):
    today = datetime.today()
    day_of_week = today.weekday()  # Monday is 0 and Sunday is 6

    # Determine the start day of the work week
    start_day = work_week.lower().split(" ")[0]
    
    if start_day == "saturday":
        days_to_subtract = (day_of_week + 1) % 7
    elif start_day == "sunday":
        days_to_subtract = day_of_week % 7
    else:
        days_to_subtract = (day_of_week + 6) % 7

    calculated_date = today - timedelta(days=days_to_subtract)
    return {
        "day": calculated_date.day,
        "month": calculated_date.month,
        "year": calculated_date.year
    }

def get_portugal_user_process_conf(item, dag_run, config):
    employee_data, location_data, department_data, division_data, cost_center_data = get_groups_data()
    # TODO: add it to documentation file
    # this will be used to determine which values that will be sent to child for processing 
    # from Process Users to Add/Update User (Workato)
    parent_company = item['parent_company_code']
    if parent_company is None:
        parent_company = ""
    is_user_c1_canada = is_user_canada_c1(parent_company, item['_country_to_use_for_query'])
    
    # this will be used to determine which values that will be sent to child for processing 
    # From batch_processor to process users (workato)
    is_user_c1, is_user_compass, is_user_ftp, is_user_gsap, non_live_user, parent_company_key = get_users_parent_company_code(parent_company)

    company_code_uri = rail.find_first_by_attr_and_get_attr(
                    division_data,
                    "full_path",
                    item["company_code_full_path"],
                    default={}
                )
    shift_data = rail.result("get_all_office_schedules")
    schedule_uri = rail.find_first_by_attr_and_get_attr(
            shift_data,
            'displayText',
            item['mapper_schedule_name_portugal_compass'],
            'uri'
    )

    holiday_calendar_data = get_holiday_calender_details()
    policy_data = rail.result('get_all_policy_sets')

    if item["exempt"]=="No":
        _employee_type_uri_to_use = rail.find_first_by_attr_and_get_attr(employee_data,
                                    'full_path',
                                    "Non Exempt - Hourly",
                                    'uri',
                                    default={})
    else:
        _employee_type_uri_to_use = rail.find_first_by_attr_and_get_attr(employee_data,
                                    'full_path',
                                    "Exempt – Salaried",
                                    'uri',
                                    default={})
    return {
            "item" : item, # this is added for ref only
            "connect_employee": _is_user_connect_employee(item),
            "starting_balance_set_to_uri": dag_run.conf.get("starting_balance_set_to_uri"),
            "prevent_balance_overdraw_uri": dag_run.conf.get("prevent_balance_overdraw_uri"),
            "supervisor_user_log": dag_run.conf.get("supervisor_user_log"),
            "employee_type_data": cached_write_json_artifact('get_all_employeegroup_data'),
            "division_data": cached_write_json_artifact('get_all_companycode_data'),
            "master_file_name": dag_run.conf["file_name"],
            "splitter_batch_name": item["splitter_batch_name"],
            "allowed_country": item['mapper_country_to_enable_portugal_compass'],
            "replicon_field": 'true' if item['status'] in [1,'1'] else 'false',
            "file_data" : {
                "emp_id": item["empid"],
                "perner_id": item["pernerid"],
                "email_id": item["email"],
                "first_name": item["firstname"],
                "last_name": item["lastname"],
                "country": item["country"],
                "state": item["state"],
                "exempt": item["exempt"],
                "exempt_effective_date": item["exempteffectivedate"],
                "employee_type": item["employeetype"],
                "hire_date": item["hiredate"],
                "gender": item["gender"],
                "service_date": item["servicedate"],
                "term_date": item["termdate"],
                "status": item["status"],
                "on_leave": item["onleave"],
                "parent_company": parent_company,
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
                "assignment_type": item["assignment_type"]
            },
            "mapper_data": _mapper_derived_values_portugal(item),
            "payrule": {
                "payrule": item['mapper_payrule_portugal_compass'],
                "payrule_name": item['mapper_payrule_portugal_compass']
            },
            "activities": {
                "activity" : item['mapper_activity_list__portugal_compass']
            },
            "holiday_calendar": {
                "holiday_calendar": item['mapper_holiday_calendar_portugal_compass'],
                "holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                    holiday_calendar_data,
                    "displayText",
                    item['mapper_holiday_calendar_portugal_compass']
                )
            },
            "user_permission_sets" : _get_user_permission_set_details(                
                item['mapper_end_user_permission'],
                item['mapper_supervisor_user_permission'],
                item['mapper_supervisor_end_user_permission'],
                item['mapper_aus_supervisor_end_user_permission'],
                item['mapper_supervisor_scheduler_permission']
            ),
            "schedule_data": {
                "is_office_schedule": True if item['workshift'] and (not item['workshift'].lower().startswith('r')) else False,
                "schedule_name": item["mapper_schedule_name_portugal_compass"],
                "work_schedule" : item['workshift'],
                "work_schedule_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_office_schedules"),
                                                "displayText",item['workshift'], 'uri'),
                "office_schedule" : item['mapper_office_schedule'],
                "schedule_type_uri": item['mapper_default_schedule_type_uri'],
                "schedule_uri": schedule_uri
            },
            "policy_sets": {
                "punch_entry_policy": {},
                "timeoff_template": rail.find_first_by_attr_and_get_attr(
                    policy_data, 'name', item['mapper_timeoff_template_portugal_compass'], default={}),
                "timesheet_period": rail.find_first_by_attr_and_get_attr(
                    policy_data, "name", item['mapper_timesheet_period_portugal_compass'], default={}),
                "timesheet_approval_path": rail.find_first_by_attr_and_get_attr(
                    policy_data, "name", item['mapper_timesheet_approval_path_portugal_compass'], default={}),
                "timesheet_template": rail.find_first_by_attr_and_get_attr(
                    policy_data, "name", item['mapper_timesheet_template_portugal_compass'], default={})
            },
            "timezone": {
                "timezone": item["mapper_timezone_portugal_compass"],
                "timezone_uri" : item["mapper_timezone_uri_portugal_compass"]
            },
            "udfs": _get_user_udfs_details(),
            "groups": {
                # Location
                "location": rail.find_first_by_attr_and_get_attr(
                    location_data,
                    'fullpath',
                    _get_location_full_path(item),
                    default={}
                ),
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
                    "uri": _employee_type_uri_to_use
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
                #TimeSheetPeriodEffectiveDate#! mapper value
                "timesheet_period_effective_date": get_json_date_from_date_str(item['mapper_timesheet_period_effective_date_portugal_compass'], TIMESHEET_PERIOD_EFFECTIVE_DATE),
                "work_week_date": portugal_calculate_work_week_date(item['mapper_work_week'])
            }
        }

def get_payrule_effective_date_canada(item):
    today = datetime.now(timezone.utc)
    # workato: Sunday = 0, Monday = 1
    # python is Monday = 0, Sunday = 6
    today_weekday = today.weekday() + 1 
    work_week_startswith_saturday = item["work_week"].lower().split(" ")[0] == "saturday"
    days_to_reduce_mapper = {
        7 : [1,7], # workato: 0 (Sunday)
        1 : [2,0], # workato: 1 (Monday)
        2 : [3,1], # workato: 2 (Tuesday)
        3 : [4,2], # workato: 3 (Wednesday)
        4 : [5,3], # workato: 4 (Thursday)
        5 : [6,4], # workato: 5 (Friday)
        6 : [0,5]  # workato: 0 (Saturday)
    }
    return get_json_date_from_date(today - timedelta(days=days_to_reduce_mapper[today_weekday][0 if work_week_startswith_saturday else 1]))


def get_canada_user_process_conf(item, dag_run, config):
    employee_data, location_data, department_data, division_data, cost_center_data = get_groups_data()
    # TODO: add it to documentation file
    # this will be used to determine which values that will be sent to child for processing 
    # from Process Users to Add/Update User (Workato)
    parent_company = item['parent_company']
    holiday_calendar_data = get_holiday_calender_details()
    activities_holiday_canada_holiday_calender_allowed_country = {
                        "mapper_activities": item["mapper_canada_activities"],
                        "mapper_holiday_calendar": item['mapper_canada_holiday_calendar'],
                        "mapper_holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                            holiday_calendar_data, 'displayText', item['mapper_canada_holiday_calendar'], 'uri'),
                        "mapper_canada_activities": item['mapper_canada_activities'],
                        "mapper_canada_holiday_calendar": item["mapper_canada_holiday_calendar"],
                        "mapper_canada_holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(
                            holiday_calendar_data, 'displayText', item['mapper_canada_holiday_calendar'], 'uri'),
                        "allowed_country" : item["mapper_non_usa_pri_ind_prt_cri_aus_countries_allowed_country"]
                    }
    _mapper_data = _mapper_derived_values(item, config, activities_holiday_canada_holiday_calender_allowed_country)

    _mapper_data['timesheet_template'] = _mapper_data['canada_timesheet_template']
    _mapper_data['timesheet_period'] = _mapper_data['timesheet_period_canada']
    if parent_company is None:
        parent_company = ""
    is_user_c1_canada = is_user_canada_c1(parent_company, item['_country_to_use_for_query'])

    company_code_uri = rail.find_first_by_attr_and_get_attr(
                    division_data,
                    "full_path",
                    item["company_code_full_path"],
                    default={}
                )
    _country = item["_country_to_use_for_query"]
    _state = item["_state_to_use_for_query"]
    _home_country, _home_state = get_home_state_and_country_for_ia(dag_run, item)

    policy_data = rail.result('get_all_policy_sets')
    return {
            "item" : item, # this is added for ref only
            "starting_balance_set_to_uri": dag_run.conf.get("starting_balance_set_to_uri"),
            "prevent_balance_overdraw_uri": dag_run.conf.get("prevent_balance_overdraw_uri"),
            "supervisor_user_log": dag_run.conf.get("supervisor_user_log"),
            "employee_type_data": cached_write_json_artifact('get_all_employeegroup_data'),
            "division_data": cached_write_json_artifact('get_all_companycode_data'),
            "master_file_name": dag_run.conf["file_name"],
            "splitter_batch_name": item["splitter_batch_name"],
            "allowed_country": activities_holiday_canada_holiday_calender_allowed_country['allowed_country'],
            "replicon_field": 'true' if item['status'] in [1,'1'] else 'false',
            "file_data" : {
                "emp_id": item["empid"],
                "perner_id": item["pernerid"],
                "email_id": item["email"],
                "first_name": item["firstname"],
                "last_name": item["lastname"],
                "country": _country,
                "state": _state,
                "exempt": item["exempt"],
                "exempt_effective_date": item["exempteffectivedate"],
                "employee_type": item["employeetype"],
                "hire_date": item["hiredate"],
                "gender": item["gender"],
                "service_date": item["servicedate"],
                "term_date": item["termdate"],
                "status": item["status"],
                "on_leave": item["onleave"],
                "parent_company": parent_company,
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
                "home_country": _home_country,
                "home_state": _home_state,
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
                "assignment_type": item["assignment_type"]
            },
            "mapper_data": _mapper_data,
            "payrule": {
                "payrule": item['mapper_canada_payrule']
            },
            "activities": {
                "activity" : item['mapper_canada_activities']
            },
            "user_permission_sets" : _get_user_permission_set_details(                
                item['mapper_end_user_permission'],
                item['mapper_supervisor_user_permission'],
                item['mapper_supervisor_end_user_permission'],
                item['mapper_aus_supervisor_end_user_permission'],
                item['mapper_supervisor_scheduler_permission']
            ),
            "schedule_data": {
                "work_schedule" : item['workshift'],
                "work_schedule_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_office_schedules"),
                                                                          "displayText",item['workshift'], 'uri'),
                "office_schedule" : item['mapper_office_schedule']
            },
            "policy_sets": {
                "timeoff_template": rail.find_first_by_attr_and_get_attr(policy_data, 'name', item['mapper_timeoff_template_name']),
                "timesheet_period": rail.find_first_by_attr_and_get_attr(policy_data, "name", item['mapper_timesheet_period_canada']),
                "timesheet_approval_path": rail.find_first_by_attr_and_get_attr(policy_data, "name", item['mapper_timesheet_approval_path_canada']),
                "timesheet_template": rail.find_first_by_attr_and_get_attr(policy_data, "name", item['mapper_canada_timesheet_template'])

            },
            "timezone": {
                "timezone": item['mapper_canada_timezone'] if item['mapper_canada_timezone'] else item["mapper_timezone"],
                "timezone_uri" : item['mapper_canada_timezone_uri'] if item['mapper_canada_timezone_uri'] else item["mapper_timezone_uri"]
            },
            "udfs": _get_user_udfs_details(),
            "groups": {
                # Location
                "location": rail.find_first_by_attr_and_get_attr(
                    location_data,
                    'fullpath',
                    _get_location_full_path(item),
                    default={}
                ),
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
                    "uri": rail.find_first_by_attr_and_get_attr(employee_data,
                                    'full_path',
                                    _get_employee_type_full_path(item),
                                    'uri',
                                    default="")
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
                #TimeSheetPeriodEffectiveDate #! mapper value
                "timesheet_period_effective_date": get_json_date_from_date_str(item['timesheet_period_effective_date'], TIMESHEET_PERIOD_EFFECTIVE_DATE),
                "payrule_effective_date": get_payrule_effective_date_canada(_mapper_data)
            }
        }
 

def get_mapper_derived_values_global_multiple_checks(mapper,check1,value1, check2, value2, result_key):
    res =  list(filter(lambda item: item[check1]==value1 and item[check2]== value2,mapper))
    if res:
        return res[0][result_key]
    return null

def get_permission_name_global(mapper):
    return {
        "end_user_permission_name" : rail.find_first_by_attr_and_get_attr(mapper,"Type","End User Permission", "Value"),
        "supervisor_user_permission_name" : rail.find_first_by_attr_and_get_attr(mapper,"Type","Supervisor User Permission", "Value"),
        "supervisor_end_user_permission_name" : rail.find_first_by_attr_and_get_attr(mapper,"Type","Supervisor End User Permission", "Value"),
    }


def get_product_data_global(mapper):
    all_products= list(filter(lambda item: item['Type']=="Product",mapper))
    return {
        "product_names": list(map(lambda x: x['Value'],all_products)),
        "product_uris": list(map(lambda x: x['URI'],all_products))
    }


def get_timeoff_template_uri_global(item, mapper):
    source = next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null)
    first_check = [res for res in mapper if res['Type']=="Timeoff Template" and res['Country']== item['_country_to_use_for_query'] and res['Source']==source]
    second_check = [res for res in mapper if res['Type']=="Timeoff Template" and res['Source']==source]

    if first_check:
        timeoff_template_name=next((res['Value'] for res in mapper if res['Type']=="Timeoff Template" and res['Country']== item['_country_to_use_for_query']
            and res['Source']==source), null)
        return rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets'),'name',timeoff_template_name, 'uri')

    if second_check:
        timeoff_template_name=next((res['Value'] for res in mapper if res['Type']=="Timeoff Template" and res['Source']==source), null)
        return rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets'),'name',timeoff_template_name, 'uri')

    return null

def get_connect_end_user_permission_name(mapper):
    return next((res['Value'] for res in mapper if res['Type']=="End User Permission Connect Employee" and res['Country']=="Australia"), null)

def get_timesheet_template_costa_rica(item, mapper):
    source = next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null)
    if item['_country_to_use_for_query'].lower() == "costa rica" and item['isia'] in [1,"1"]:
        return next((res['Value'] for res in mapper if res['Type']=="Timesheet Template" and res['Source']== ( source if source else "No Timesheet")
                and res['Country']==item['_country_to_use_for_query'] and res['employeegroup'] == ( "ES-CR-ROT(CR)" if item['workshift']== "ES-CR-ROT(CR)" else "Others") and
                res['personnelsubarea']==item['exempt']), null)

    return next((res['Value'] for res in mapper if res['Type']=="Timesheet Template" and res['Source']== ( source if source else "No Timesheet")
        and res['Country']==item['_country_to_use_for_query'] and res['employeegroup'] == ( "ES-CR-ROT(CR)" if item['workshift']== "ES-CR-ROT(CR)" else "Others") and
        res['personnelsubarea']==item['exempt']), null)

def get_holiday_calender_name_costa_rica(item, mapper):
    if item['_country_to_use_for_query'].lower() == "costa rica" and item['isia'] in [1,"1"]:
        return next((res['Value'] for res in mapper if res['Type']=="Holiday Calendar"
                and res['Source']==(next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null))
                and res['Country']==item['_country_to_use_for_query']), null)

    return next((res['Value'] for res in mapper if res['Type']=="Holiday Calendar"
        and res['Source']==(next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null))
        and res['Country']==item['_country_to_use_for_query']), null)

def _mapper_derived_values_costa_rica(item, mapper):
    timesheet_period_eff_date =  next((res['Value'] for res in mapper if res['Type']=="Timesheet Period Effective Date" and res['Country']==(item['_country_to_use_for_query'])), null)
    return {
        "office_schedule": rail.find_first_by_attr_and_get_attr(mapper,"Type","Office Schedule", "Value"),
        "authentication": rail.find_first_by_attr_and_get_attr(mapper,"Type","Authentication", "Value"),
        "timesheet_approval_path": get_mapper_derived_values_global_multiple_checks(mapper,"Type","Timesheet Approval","Source","All","Value"),
        "timesheet_period": rail.find_first_by_attr_and_get_attr(mapper,"Type","Timesheet Period", "Value"),
        "authentication_uri": rail.find_first_by_attr_and_get_attr(mapper,"Type","Authentication", "URI"),
        "end_user_permission_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),"name",
                get_permission_name_global(mapper)['end_user_permission_name'], "uri"
            ) if get_permission_name_global(mapper)['end_user_permission_name'] else null,
        "supervisor_user_permission_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),"name",
                get_permission_name_global(mapper)['supervisor_user_permission_name'], "uri"
            ) if get_permission_name_global(mapper)['supervisor_user_permission_name'] else null,
        "product": get_product_data_global(mapper)['product_names'],
        "product_uri": get_product_data_global(mapper)['product_uris'],
        "language": rail.find_first_by_attr_and_get_attr(mapper,"Type","Language", "Value"),
        "language_uri": rail.find_first_by_attr_and_get_attr(mapper,"Type","Language", "URI"),
        "supervisor_end_user_permission_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),"name",
                get_permission_name_global(mapper)['supervisor_end_user_permission_name'], "uri"
            ) if get_permission_name_global(mapper)['supervisor_end_user_permission_name'] else null,
        "supervisor_user_permission_name":rail.find_first_by_attr_and_get_attr(mapper,"Type","Supervisor User Permission", "Value"),
        "supervisor_end_user_permission_name": rail.find_first_by_attr_and_get_attr(mapper,"Type","Supervisor End User Permission", "Value"),
        "end_user_permission_name": rail.find_first_by_attr_and_get_attr(mapper,"Type","End User Permission", "Value"),
        "schedule_type_uri": get_mapper_derived_values_global_multiple_checks(mapper,"Type","Schedule Type","Country","Default","URI"),
        "timeoff_template_uri": get_timeoff_template_uri_global(item,mapper),
        "timesheet_approval_canada": get_mapper_derived_values_global_multiple_checks(mapper,"Type","Timesheet Approval","Source","C1","Value"),
        "timeoff_approval_canada": get_mapper_derived_values_global_multiple_checks(mapper,"Type","Timeoff Approval","Source","C1","Value"),
        "supervisor_schedule_manager_name": rail.find_first_by_attr_and_get_attr(mapper,"Type","Supervisor Scheduler Permission", "Value"),
        "psg": next((res['Value'] for res in mapper if res['Type']=="PSG" and res['Source']=='C1' and res['personnelsubarea']==item['areacode']
            and res['employeegroup']==item['subareacode'] and res['status']==item['companycode']
            ), null),
        "termination_reason_code": next((res['Value'] for res in mapper if res['Type']=="Termination Reason" and res['Source']==item['terminationreason']
                and res['URI']==item['_state_to_use_for_query']), null) if item['terminationreason'] else null,
        "connect_end_user_permission_name": get_connect_end_user_permission_name(mapper),
        "connect_end_user_permission_uri":rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_sets"), "name", get_connect_end_user_permission_name(mapper)
            ) if get_connect_end_user_permission_name(mapper) else null,

        #costa_rica child_call_extra_data
        "parent_company": next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']==item['companycode']
            ), null),

        "timesheet_template_name": get_timesheet_template_costa_rica(item, mapper),
        "timesheet_template_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets'),'name', get_timesheet_template_costa_rica(item, mapper), 'uri')
            if get_timesheet_template_costa_rica(item, mapper) else null,

        "timesheet_approval_path": next((res['Value'] for res in mapper if res['Type']=="Timesheet Approval" and res['Source']==(
            next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null))
                and res['Country']==(item['_country_to_use_for_query'])), null),
        "timeoff_template":next((res['Value'] for res in mapper if res['Type']=="Timeoff Template" and res['Source']==(
            next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null))
                and res['Country']==(item['_country_to_use_for_query'])), null),
        "timeoff_approval": next((res['Value'] for res in mapper if res['Type']=="Timeoff Approval" and res['Source']==(
            next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null))
                and res['Country']==(item['_country_to_use_for_query'])), null),
        "timesheet_period": next((res['Value'] for res in mapper if res['Type']=="Timesheet Period" and res['Source']==(
            next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null))
                and res['Country']==(item['_country_to_use_for_query'])), null),
        "workweek": next((res['Value'] for res in mapper if res['Type']=="WorkWeek" and res['Source']==
            (next(res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']))),null)
            if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null)
            else null,
        "workweek_uri": next((res['URI'] for res in mapper if res['Type']=="WorkWeek" and res['Source']==
            (next(res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']))),null)
            if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null)
            else null,
        "activity_list": (
                [res['Value'] for res in mapper if res['Type']=="Activities" and res['Country']== (item['_country_to_use_for_query']) and res['Source']=="COMPASS"]
            if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) =="COMPASS" else null
            ) if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) else null,
        "employee_type_full_path":
            ( _get_employee_type_full_path(item)
                if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) =="C1" else
                ("Exempt – Salaried" if item['exempt']=="Yes" else "Non Exempt - Hourly")
            ) if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) else
             ("Exempt – Salaried" if item['exempt']=="Yes" else "Non Exempt - Hourly"),
        "timezone_name": next((res['Value'] for res in mapper if res['Type']=="TimeZone" and res['Country']==(item['_country_to_use_for_query'])), null),
        "timezone_uri": next((res['URI'] for res in mapper if res['Type']=="TimeZone" and res['Country']==(item['_country_to_use_for_query'])), null),
        "holiday_calender_name": get_holiday_calender_name_costa_rica(item, mapper),
        "holiday_calender_uri":rail.find_first_by_attr_and_get_attr(rail.result('get_all_holiday_calendar'),"name",get_holiday_calender_name_costa_rica(item, mapper), "uri")
            if (get_holiday_calender_name_costa_rica(item, mapper) if next((res['Source'] for res in mapper if res['Type']=="Company Code"
                and res['URI']== item['companycode']), null) else null) else null,
        "allowed_country": ("Enable" if next((res['Value'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) else null)
        if next((res['Value'] for res in mapper if res['Type']=="Country to enable" and res['Country']==(item['_country_to_use_for_query'])), null) else null,
        "timesheet_period_effective_date":timesheet_period_eff_date,
        "timesheet_period_effective_date_json_format": get_json_date_from_date_str(timesheet_period_eff_date, TIMESHEET_PERIOD_EFFECTIVE_DATE)if timesheet_period_eff_date else null,
        "profile_status":next((res['status'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null)
            if next((res['status'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) else "disabled",
        "timeentry_approval_path_name": next((res['Value'] for res in mapper if res['Type']=="Time Entry Approval Path" and res['Source']==
                (next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null))), null)
            if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) else null,
        "payrule": next((res['Value'] for res in mapper if res['Type']=="Payrule" and res['Source']==(
                next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null)
            if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) else "No Payrule")
            and res['Country']==(item['_country_to_use_for_query'])
            and res['employeegroup']==( "ES-CR-ROT(CR)" if item['workshift']=="ES-CR-ROT(CR)" else "Others")
            and res['personnelsubarea']==item['exempt']
            and res['employeesubgroup']==( "FT" if int(item['ftepct'])==100 else "PT")
            ), null),
    }

@lru_cache(maxsize=16)
def get_groups_data_global():
    return rail.result("get_all_employeegroup_data")['employee_data'], rail.result("get_all_locations"), rail.result("get_all_enabled_departments"),\
            rail.result('get_all_companycode_data'), rail.result('get_all_cost_centers')

def get_costa_rica_user_process_conf(item, dag_run, config):
    _country = item["_country_to_use_for_query"]
    _state = item["_state_to_use_for_query"]
    _home_country, _home_state = get_home_state_and_country_for_ia(dag_run, item)
    employee_data, location_data, department_data, division_data, cost_center_data = get_groups_data_global()
    schedule_manager =  rail.find_first_by_attr_and_get_attr(config.DXC_WORKDAY_USER_SYNC_USER_MAPPER,"Type","Supervisor Scheduler Permission", "Value")

    return {
            "file_data" : {
                "emp_id": item["empid"],
                "perner_id": item["pernerid"],
                "email_id": item["email"],
                "first_name": item["firstname"],
                "last_name": item["lastname"],
                "country": _country,
                "state": _state,
                "exempt": item["exempt"],
                "employee_type": item["employeetype"],
                "hire_date": item["hiredate"],
                "gender": item["gender"],
                "service_date": item["servicedate"],
                "term_date": item["termdate"],
                "status": item["status"],
                "on_leave": item["onleave"],
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
                "home_country": _home_country,
                "home_state": _home_state,
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
                "ia_start_date": item["iastartdate"],
                "ia_end_date": item["iaenddate"],
                "exempt_effective_date": item["exempteffectivedate"],
                "is_ia": item["isia"],
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
            },
            "mapper_data": _mapper_derived_values_costa_rica(item, config.DXC_WORKDAY_USER_SYNC_USER_MAPPER),
            "udfs": _get_user_udfs_details(),
            #"item" : item, # this is added for ref only
            "fulltime_parttime": ("Part Time" if float(item["ftepct"]) < 100 else "Full Time") if item["ftepct"] else "Full Time",
            "starting_balance_set_to_uri": dag_run.conf.get("starting_balance_set_to_uri"),
            "prevent_balance_overdraw_uri": dag_run.conf.get("prevent_balance_overdraw_uri"),
            "job_level_set": ("8-" if int(item["joblevel"]) < 9 else "8+") if item["joblevel"] else "No",
            "supervisor_user_log": dag_run.conf.get("supervisor_user_log"),

            "schedule_uri":rail.find_first_by_attr_and_get_attr(rail.result("get_all_office_schedules"),'displayText',item['workshift'],'uri')
                if item['workshift'] else null,
            "schedule_manager_permission_uri_for_supervisor": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),
                "name",schedule_manager, "uri") if schedule_manager else null,
            "schedule_name": item['workshift'],

            "master_file_name": dag_run.conf["file_name"],
            "splitter_batch_name": "COMPASS",

            "punch_entry_policy_name": null,
            "punch_entry_policy_uri": null,

            "location_uri": rail.find_first_by_attr_and_get_attr(
                location_data,
                'fullpath',
                _get_location_full_path(item),
                'uri'
            ),
            "employee_type_list": rail.write_json_artifact(employee_data),
            "employee_tye_uri": rail.find_first_by_attr_and_get_attr(employee_data,'full_path',
                ("Exempt – Salaried" if item["exempt"]=="Yes" else "Non Exempt - Hourly"),"uri",default={}),
            "employee_type_uri_for_all": rail.find_first_by_attr_and_get_attr(
                employee_data,'full_path',_get_employee_type_full_path(item),"uri",default={}),
            "cost_center_uri": rail.find_first_by_attr_and_get_attr(
                cost_center_data,
                "displayText",
                item["costcenter"],
                'uri'
            ) if item["costcenter"] else null,
            "company_code_list": rail.write_json_artifact(division_data),
            "company_code_uri": (rail.find_first_by_attr_and_get_attr(
                division_data,
                "full_path",
                next((res['Value'] for res in config.DXC_WORKDAY_USER_SYNC_USER_MAPPER if res['Type']=="Company Code" and res['URI']== item['companycode']), null),
                "uri"
            )
            if rail.find_first_by_attr_and_get_attr(
                division_data,
                "full_path",
                next((res['Value'] for res in config.DXC_WORKDAY_USER_SYNC_USER_MAPPER if res['Type']=="Company Code" and res['URI']== item['companycode']), null),
                "uri"
            )
            else rail.find_first_by_attr_and_get_attr(division_data, "name", item['companycode'], "uri")),
            "organizational_unit_uri": rail.find_first_by_attr_and_get_attr(department_data, "displayText", item['orgcode'], "uri")
                if item['orgcode'] else null,

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
            }
        }


def _mapper_derived_values_usa_les(item, config, parent_company):
    mapper = config.DXC_WORKDAY_USER_SYNC_USER_MAPPER
    _emp_type = "Exempt – Salaried" if item['exempt'] == "Yes" else "Non Exempt - Hourly"
    _psg = next((res['Value'] for res in mapper if res['Type']=="PSG" and res['Source']=='C1' and res['personnelsubarea']==item['areacode']
            and res['employeegroup']==item['subareacode'] and res['status']==item['companycode']
            ), "")
    return {
        "office_schedule": rail.find_first_by_attr_and_get_attr(mapper,"Type","Office Schedule", "Value"),
        "authentication": rail.find_first_by_attr_and_get_attr(mapper,"Type","Authentication", "Value"),
        "timesheet_approval_path": next(filter(lambda row: row['Type'] == "Timesheet Approval" and  row['Source'] == parent_company and row['Country'] == item['_country_to_use_for_query'],  mapper), {}).get('Value'),
        "timesheet_period_master": rail.find_first_by_attr_and_get_attr(mapper,"Type","Timesheet Period", "Value"),
        "authentication_uri": rail.find_first_by_attr_and_get_attr(mapper,"Type","Authentication", "URI"),
        "end_user_permission_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),"name",
                get_permission_name_global(mapper)['end_user_permission_name'], "uri"
            ) if get_permission_name_global(mapper)['end_user_permission_name'] else null,
        "supervisor_user_permission_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),"name",
                get_permission_name_global(mapper)['supervisor_user_permission_name'], "uri"
            ) if get_permission_name_global(mapper)['supervisor_user_permission_name'] else null,
        "product": get_product_data_global(mapper)['product_names'],
        "product_uri": get_product_data_global(mapper)['product_uris'],
        "language": rail.find_first_by_attr_and_get_attr(mapper,"Type","Language", "Value"),
        "language_uri": rail.find_first_by_attr_and_get_attr(mapper,"Type","Language", "URI"),
        "supervisor_end_user_permission_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),"name",
                get_permission_name_global(mapper)['supervisor_end_user_permission_name'], "uri"
            ) if get_permission_name_global(mapper)['supervisor_end_user_permission_name'] else null,
        "supervisor_user_permission_name":rail.find_first_by_attr_and_get_attr(mapper,"Type","Supervisor User Permission", "Value"),
        "supervisor_end_user_permission_name": rail.find_first_by_attr_and_get_attr(mapper,"Type","Supervisor End User Permission", "Value"),
        "end_user_permission_name": rail.find_first_by_attr_and_get_attr(mapper,"Type","End User Permission", "Value"),
        "schedule_type_uri": get_mapper_derived_values_global_multiple_checks(mapper,"Type","Schedule Type","Country","Default","URI"),
        "timesheet_approval_canada": get_mapper_derived_values_global_multiple_checks(mapper,"Type","Timesheet Approval","Source","C1","Value"),
        "timeoff_approval_canada": get_mapper_derived_values_global_multiple_checks(mapper,"Type","Timeoff Approval","Source","C1","Value"),
        "supervisor_schedule_manager_name": rail.find_first_by_attr_and_get_attr(mapper,"Type","Supervisor Scheduler Permission", "Value"),
        "psg": _psg,
        "termination_reason_code": next((res['Value'] for res in mapper if res['Type']=="Termination Reason" and res['Source']==item['terminationreason']
                and res['URI']==item['_state_to_use_for_query']), null) if item['terminationreason'] else null,
        "connect_end_user_permission_name": get_connect_end_user_permission_name(mapper),
        "connect_end_user_permission_uri":rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_sets"), "name", get_connect_end_user_permission_name(mapper)
            ) if get_connect_end_user_permission_name(mapper) else null,
        "employee_type": _emp_type,
        "employee_type_for_all": _get_employee_type_full_path(item),
        "timesheet_template": next(filter(lambda row: row['Type'] == "Timesheet Template" and row['Country'] == item['_country_to_use_for_query']\
                                                 and row['Source'] == parent_company  and row['personnelsubarea'] == _emp_type\
                                                 and row['employeegroup'] == ("All Others" if _emp_type == "Exempt – Salaried" else (
                                                     item['_state_to_use_for_query'] if item['_state_to_use_for_query'] in STATE_TO_GROUP else "All Others")) and row['status'] == item['companycode'], mapper), {}).get('Value'),
        # "Timesheeturi" will be calculated later
        "timeoff_template": next((res['Value'] for res in mapper if res['Type']=="Timeoff Template" and res['Country'] == item['_country_to_use_for_query'] and res['Source']==(
            next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null)))),
        "timeoff_approval": next(filter(lambda row: row['Type'] == "Timeoff Approval" and  row['Source'] == parent_company and row['Country'] == item['_country_to_use_for_query'] and row['status'] == item['companycode'],  mapper), {}).get('Value'),
        "timesheet_period": next(filter(lambda row: row['Type'] == "Timesheet Period" and  row['Source'] == parent_company and row['Country'] == item['_country_to_use_for_query'] and row['personnelsubarea'] == _emp_type,  mapper), {}).get('Value'),
        "work_week": get_work_week_usa_les(parent_company, _emp_type, item, mapper),
        "work_week_uri": get_work_week_usa_les(parent_company, _emp_type, item, mapper, 'URI'),
        "activity": get_activity_list_usa_les(parent_company, item, mapper),
        "employee_type_full_path": get_employee_type_full_path(parent_company, item, _emp_type),
        "timezone": next(filter(lambda row: row['Type'] == "TimeZone" and  row['Country'] == item['_country_to_use_for_query'] and row['Source'] == item['_state_to_use_for_query'],  mapper), {}).get('Value'),
        "timezone_uri": next(filter(lambda row: row['Type'] == "TimeZone" and row['Country'] == item['_country_to_use_for_query'] and row['Source'] == item['_state_to_use_for_query'],  mapper), {}).get('URI'),
        "holiday_calendar": get_holiday_calender_name_usa_les(item, parent_company, mapper),
        # "holiday_calendar_uri" will be derivied later
        "allowed_country": get_allowed_country(item, mapper, parent_company),
        "timesheet_period_effective_date": next(filter(lambda row: row['Type'] == "Timesheet Period Effective Date" and row['Country'] == item['_country_to_use_for_query'], mapper), {}).get('Value'),
        "c1payrule": next(filter(lambda row: row['Type'] == "Payrule" and\
                                        row['Country'] == item['_country_to_use_for_query'] and\
                                        row['Source'] == parent_company and\
                                        row['URI'] == (item['subareacode'] if item['subareacode'] in PERSONNEL_SUB_AREA_CODE_TO_GROUP else "All - Others") and\
                                        row['personnelsubarea'] == _psg and\
                                        row['employeegroup'] == item['empgroupcode'] and\
                                        row['employeesubgroup'] == item['empsubgroupcode'] and\
                                        row['status'] == (((item['_state_to_use_for_query'] if item['_state_to_use_for_query'] in STATE_TO_GROUP else "All - Others") if item['_country_to_use_for_query'] == "United States of America" else item['_country_to_use_for_query'])), mapper), {}).get('Value'),
        "c1_timesheet_template": next(filter(lambda row:  row['Type'] == "Timesheet Template" and\
                                                        row['Country'] == item['_country_to_use_for_query'] and\
                                                        row['Source'] == parent_company and\
                                                        row['URI'] == (item['subareacode'] if item['subareacode'] in PERSONNEL_SUB_AREA_CODE_TO_GROUP else "All - Others") and\
                                                        row['personnelsubarea'] == _psg and\
                                                        row['employeegroup'] == item['empgroupcode'] and\
                                                        row['employeesubgroup'] == item['empsubgroupcode'] and\
                                                        row['status'] == (((item['_state_to_use_for_query'] if item['_state_to_use_for_query'] in STATE_TO_GROUP else "All - Others") if item['_country_to_use_for_query'] == "United States of America" else item['_country_to_use_for_query'])), mapper), {}).get('Value'),
        "timesheet_approval+c1": next(filter(lambda row: row['Type'] == "Timesheet Approval" and\
                                        row['Country'] == item["_country_to_use_for_query"] and\
                                        row['Source'] == parent_company, mapper), {}).get('Value'),
        "timeoff_approval_c1": next(filter(lambda row: row['Type'] == "Timeoff Approval" and\
                                        row['Country'] == item["_country_to_use_for_query"] and\
                                        row['Source'] == parent_company, mapper), {}).get('Value'),
        "c1_activities": rail.smartjoin_by_delim(list(filter(lambda row: row['Type'] == "Activities" and\
                                                 row['Country'] == item["_country_to_use_for_query"] and\
                                                 row['Source'] == parent_company and\
                                                 row['URI'] == (item['subareacode'] if item['subareacode'] in PERSONNEL_SUB_AREA_CODE_TO_GROUP else "All - Others") and\
                                                 row['personnelsubarea'] == _psg and\
                                                 row['employeegroup'] == item['empgroupcode'] and\
                                                 row['employeesubgroup'] == item['empsubgroupcode'] and\
                                                 row['status'] == (((item['_state_to_use_for_query'] if item['_state_to_use_for_query'] in STATE_TO_GROUP else "All - Others") if item['_country_to_use_for_query'] == "United States of America" else item['_country_to_use_for_query'])), mapper)), separator="|"),

        "timesheet_period_c1": next(filter(lambda row: row['Type'] == "Timesheet Period" and\
                                        row['Country'] == item["_country_to_use_for_query"] and\
                                        row['Source'] == parent_company, mapper), {}).get('Value'),
        "profile_status": next(filter(lambda row: row['Type'] == "Company Code" and\
                                        row['URI'] == item["companycode"], mapper), {}).get('status', 'disabled'),
        "timeentry_approval_path_name": next(filter(lambda row: row['Type'] == "Time Entry Approval Path" and\
                                        row['Source'] == parent_company, mapper), {}).get('Value') if parent_company else null,
        "payrule": next(filter(lambda row: row['Type'] == "Payrule" and\
                                        row['Country'] == item["_country_to_use_for_query"] and\
                                        row['Source'] == parent_company and\
                                        row['personnelsubarea'] == _emp_type and\
                                        row['employeegroup'] == ("All Others" if _emp_type == "Exempt – Salaried" else (item['_state_to_use_for_query'] if item['_state_to_use_for_query'] in STATE_TO_GROUP else "All Others")) and\
                                        row['status'] == item['companycode'], mapper), {}).get('Value'),
        "schedule_hours": next(filter(lambda row: row['Type'] == "Schedule Hours" and\
                                        row['Source'] == item['workshift'], mapper), {}).get('Value'),
        "punch_entry_policy": next(filter(lambda row: row['Type'] == "Punch Entry Policy" and\
                                                        row['Source'] == parent_company and\
                                                        row['personnelsubarea'] == _psg and\
                                                        row['employeegroup'] == item['empgroupcode'] and\
                                                        row['employeesubgroup'] == item['empsubgroupcode'] and\
                                                        row['status'] == item['_state_to_use_for_query'], mapper), {}).get('Value')
    }

def get_allowed_country(item, mapper, parent_company):
    if next(filter(lambda row: row['Type'] == "Country to enable" and row['Country'] == item['_country_to_use_for_query'], mapper), {}).get('Value'):
        if parent_company:
            return "Enable"
    return null

def get_company_code_value_for_uri(item, mapper, company_code_uri_list):
    mapper_value = next(filter(lambda row: row['Type'] == "Company Code" and row['URI'] == item['companycode'], mapper), {}).get('Value')
    return rail.find_first_by_attr_and_get_attr(company_code_uri_list, 'name', mapper_value, default={})

def get_puch_entry_policy_usa_les(item, parent_company, mapper):
    return null

def get_holiday_calender_name_usa_les(item, parent_company, mapper):
    if parent_company == "C1":
        if item['_state_to_use_for_query'] == "Puerto Rico":
            if parent_company:
                return next(filter(lambda row: row['Type']=="Holiday Calendar" and row['Country'] == "Puerto Rico" and row['Source'] == parent_company, mapper), {}).get('Value')
            return null
        if item['_state_to_use_for_query'] == "Rhode Island":
            if parent_company:
                return next(filter(lambda row: row['Type']=="Holiday Calendar" and row['Country'] == item['_country_to_use_for_query'] and row['Source'] == parent_company and row['status'] == "Rhode Island", mapper), {}).get('Value')
            return null
        if parent_company:
            return next(filter(lambda row: row['Type']=="Holiday Calendar" and row['Country'] == item['_country_to_use_for_query'] and row['Source'] == parent_company, mapper), {}).get('Value')
    return null

def get_employee_type_full_path(parent_company, item, _emp_type):
    if parent_company:
        if parent_company == "C1":
            return rail.smartjoin_by_delim(arr=[item['empgroupcode'], item['empsubgroupcode']], separator=" | ")
    return _emp_type

def get_activity_list_usa_les(parent_company, item, mapper):
    if parent_company:
        if parent_company == "COMPASS":
            return "|".join([activity['Value'] for activity in list(filter(lambda row: row['Type'] == "Activities" and row['Source'] == "COMPASS" and row['Country'] == item['_country_to_use_for_query'],  mapper))])
        if parent_company == "FTP":
            return "|".join([activity['Value']for activity in list(filter(lambda row: row['Type'] == "Activities" and row['Source'] == "COMPASS",  mapper))])
    return null

def get_work_week_usa_les(parent_company, _emp_type, item, mapper, pluck_key='Value'):
    return next(filter(lambda row: row['Type'] == "WorkWeek" and row['Source'] == parent_company and row['Country'] == item['_country_to_use_for_query'], mapper), {}).get(pluck_key)

def _get_user_permission_set_details_usa_les(mapper_data):
    return {
            "end_user_permission": mapper_data['end_user_permission_uri'],
            "supervisor_user_permission": mapper_data['supervisor_user_permission_uri'],
            "supervisor_end_user_permission": mapper_data['supervisor_end_user_permission_uri'],
        }

def get_usa_les_user_process_conf(item, dag_run, config):
    employee_data, location_data, department_data, division_data, cost_center_data = get_groups_data()
    # TODO: add it to documentation file
    # this will be used to determine which values that will be sent to child for processing 
    # from Process Users to Add/Update User (Workato)
    parent_company = list(filter(lambda row: row['Type'] == "Company Code" and row['URI'] == item['companycode'], config.DXC_WORKDAY_USER_SYNC_USER_MAPPER))
    if parent_company is None:
        parent_company = ""
    else:
        parent_company = parent_company[0]['Source']
    # this will be used to determine which values that will be sent to child for processing 
    # From batch_processor to process users (workato)
    company_code_uri = get_company_code_value_for_uri(item, config.DXC_WORKDAY_USER_SYNC_USER_MAPPER, division_data)
    mapper_derived_data = _mapper_derived_values_usa_les(item, config, parent_company)
    policy_data = rail.result('get_all_policy_sets')
    return {
            "item" : item, # this is added for ref only
            "starting_balance_set_to_uri": dag_run.conf.get("starting_balance_set_to_uri"),
            "prevent_balance_overdraw_uri": dag_run.conf.get("prevent_balance_overdraw_uri"),
            "supervisor_user_log": dag_run.conf.get("supervisor_user_log"),
            "employee_type_data": cached_write_json_artifact('get_all_employeegroup_data'),
            "division_data": cached_write_json_artifact('get_all_companycode_data'),
            "master_file_name": dag_run.conf["file_name"],
            "splitter_batch_name": "Compass USA LES",
            "allowed_country": get_allowed_country(item, config.DXC_WORKDAY_USER_SYNC_USER_MAPPER, parent_company),
            "replicon_field": 'true' if item['status'] in [1,'1'] else 'false',
            "file_data" : {
                "emp_id": item["empid"],
                "perner_id": item["pernerid"],
                "email_id": item["email"],
                "first_name": item["firstname"],
                "last_name": item["lastname"],
                "country": item["country"],
                "state": item["state"],
                "exempt": item["exempt"],
                "exempt_effective_date": item["exempteffectivedate"],
                "employee_type": item["employeetype"],
                "hire_date": item["hiredate"],
                "gender": item["gender"],
                "service_date": item["servicedate"],
                "term_date": item["termdate"],
                "status": item["status"],
                "on_leave": item["onleave"],
                "parent_company": parent_company,
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
                "assignment_type": item["assignment_type"]
            },
            "mapper_data":mapper_derived_data,
            "payrule": {
                "payrule": ("US LCSC MDA Union - 36 hours" if mapper_derived_data['schedule_hours'] == "36" else "US LCSC MDA Union") if mapper_derived_data['payrule'] == "US LCSC MDA Union" else mapper_derived_data['payrule'],
            },
            "activities": {
                "activity" : mapper_derived_data['activity'],
                "c1_activity": mapper_derived_data['c1_activities']
            },
            "user_permission_sets" : _get_user_permission_set_details_usa_les(                
                mapper_derived_data
            ),
            "holiday_calendar": {
                "holiday_calendar": mapper_derived_data['holiday_calendar'],
                "holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(get_holiday_calender_details(), 'name', mapper_derived_data['holiday_calendar'], 'uri')
            },
            "schedule_data": {
                "work_schedule" : item['workshift'],
                "schedule_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_office_schedules"),
                                                                          "displayText",item['workshift'], 'uri'),
                "office_schedule" : mapper_derived_data['office_schedule']
            },
            "policy_sets": {
                "punch_entry_policy": {
                    **{"punch_entry_policy": mapper_derived_data['punch_entry_policy']},
                    **rail.find_first_by_attr_and_get_attr(policy_data, 'name', mapper_derived_data['punch_entry_policy'], default={})
                },
                "timeoff_template": {
                    **{"timeoff_template": mapper_derived_data['timeoff_template']},
                    **rail.find_first_by_attr_and_get_attr(policy_data, 'name', mapper_derived_data['timeoff_template'], default={})
                },
                "timesheet_period": {
                    **{"timesheet_period": mapper_derived_data['timesheet_period']},
                    **rail.find_first_by_attr_and_get_attr(policy_data, 'name', mapper_derived_data['timesheet_period'], default={})
                },
                "c1_timesheet_period": {
                    **{"c1_timesheet_period": mapper_derived_data['timesheet_period_c1']},
                    **rail.find_first_by_attr_and_get_attr(policy_data, 'name', mapper_derived_data['timesheet_period_c1'], default={})
                },
                "timesheet_approval_path": {
                    **{"timesheet_approval_path": mapper_derived_data['timeentry_approval_path_name']},
                    **rail.find_first_by_attr_and_get_attr(policy_data, 'name', mapper_derived_data['timeentry_approval_path_name'], default={})
                },
                "timesheet_template": {
                    **{"timesheet_template": mapper_derived_data['timesheet_template']},
                    **rail.find_first_by_attr_and_get_attr(policy_data, 'name', mapper_derived_data['timesheet_template'], default={})
                },
                "c1_timesheet_template": {
                    **{"c1_timesheet_template": mapper_derived_data['c1_timesheet_template']},
                    **rail.find_first_by_attr_and_get_attr(policy_data, 'name', mapper_derived_data['c1_timesheet_template'], default={})
                }
            },
            "timezone": {
                "timezone": mapper_derived_data["timezone"],
                "timezone_uri" : mapper_derived_data["timezone_uri"]
            },
            "udfs": _get_user_udfs_details(),
            "groups": {
                # Location
                "location": rail.find_first_by_attr_and_get_attr(
                    location_data,
                    'fullpath',
                    _get_location_full_path(item),
                    default={}
                ),
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
                    "employee_type_full_path": rail.find_first_by_attr_and_get_attr(employee_data, 'full_path', mapper_derived_data['employee_type_full_path'], default={}),
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
                "date_of_birth": get_json_date_from_date_str(item['dob']),
                "exempt_eff_date": get_json_date_from_date_str(item['exempteffectivedate']),
                #TimeSheetPeriodEffectiveDate #! mapper value
                "timesheet_period_effective_date": get_json_date_from_date_str(mapper_derived_data['timesheet_period_effective_date'], TIMESHEET_PERIOD_EFFECTIVE_DATE),
            }
        }


def get_timesheet_template_india(item, mapper, country):
    job_level_set = ("8-" if int(item["joblevel"]) < 9 else "8+") if item["joblevel"] else "No"
    source = next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null)
    return next((res['Value'] for res in mapper if res['Type']=="Timesheet Template" and res['Source']== ( source if source else "No Timesheet")
            and res['Country']==country and res['personnelsubarea']==job_level_set), null)

def get_holiday_calender_name_india(item, mapper, country):
    return next((res['Value'] for res in mapper if res['Type']=="Holiday Calendar"
        and res['Source']==(next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null))
        and res['Country']==country and res['personnelsubarea']==item['_state_to_use_for_query']), null)

def get_punch_entry_policy_name_india(item, mapper, country):
    source = next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null)
    return next((res['Value'] for res in mapper if res['Type']=="Punch Entry Policy"
        and res['Source'] == ( source if source else "No Punch Entry")
        and res['Country'] == country and res['employeegroup']==item['exempt']
        and res['status'] == item['companycode']
        ), null)

def _mapper_derived_values_india(item, mapper, country):
    job_level_set = ("8-" if int(float(item["joblevel"])) < 9 else "8+") if item["joblevel"] else "No"
    timesheet_period_eff_date =  next((res['Value'] for res in mapper if res['Type']=="Timesheet Period Effective Date" and res['Country']==country), null)

    return {
        ## Globlal-values ##
        "office_schedule": rail.find_first_by_attr_and_get_attr(mapper,"Type","Office Schedule", "Value"),
        "authentication": rail.find_first_by_attr_and_get_attr(mapper,"Type","Authentication", "Value"),
        "timesheet_approval_path": get_mapper_derived_values_global_multiple_checks(mapper,"Type","Timesheet Approval","Source","All","Value"),
        "timesheet_period": rail.find_first_by_attr_and_get_attr(mapper,"Type","Timesheet Period", "Value"),
        "authentication_uri": rail.find_first_by_attr_and_get_attr(mapper,"Type","Authentication", "URI"),
        "end_user_permission_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),"name",
                get_permission_name_global(mapper)['end_user_permission_name'], "uri"
            ) if get_permission_name_global(mapper)['end_user_permission_name'] else null,
        "supervisor_user_permission_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),"name",
                get_permission_name_global(mapper)['supervisor_user_permission_name'], "uri"
            ) if get_permission_name_global(mapper)['supervisor_user_permission_name'] else null,
        "product": get_product_data_global(mapper)['product_names'],
        "product_uri": get_product_data_global(mapper)['product_uris'],
        "language": rail.find_first_by_attr_and_get_attr(mapper,"Type","Language", "Value"),
        "language_uri": rail.find_first_by_attr_and_get_attr(mapper,"Type","Language", "URI"),
        "supervisor_end_user_permission_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),"name",
                get_permission_name_global(mapper)['supervisor_end_user_permission_name'], "uri"
            ) if get_permission_name_global(mapper)['supervisor_end_user_permission_name'] else null,
        "supervisor_user_permission_name":rail.find_first_by_attr_and_get_attr(mapper,"Type","Supervisor User Permission", "Value"),
        "supervisor_end_user_permission_name": rail.find_first_by_attr_and_get_attr(mapper,"Type","Supervisor End User Permission", "Value"),
        "end_user_permission_name": rail.find_first_by_attr_and_get_attr(mapper,"Type","End User Permission", "Value"),
        "schedule_type_uri": get_mapper_derived_values_global_multiple_checks(mapper,"Type","Schedule Type","Country","Default","URI"),
        "timeoff_template_uri": get_timeoff_template_uri_global(item,mapper),
        "timesheet_approval_canada": get_mapper_derived_values_global_multiple_checks(mapper,"Type","Timesheet Approval","Source","C1","Value"),
        "timeoff_approval_canada": get_mapper_derived_values_global_multiple_checks(mapper,"Type","Timeoff Approval","Source","C1","Value"),
        "supervisor_schedule_manager_name": rail.find_first_by_attr_and_get_attr(mapper,"Type","Supervisor Scheduler Permission", "Value"),
        "psg": next((res['Value'] for res in mapper if res['Type']=="PSG" and res['Source']=='C1' and res['personnelsubarea']==item['areacode']
            and res['employeegroup']==item['subareacode'] and res['status']==item['companycode']
            ), null),
        "termination_reason_code": next((res['Value'] for res in mapper if res['Type']=="Termination Reason" and res['Source']==item['terminationreason']
                and res['URI']==item['_state_to_use_for_query']), null) if item['terminationreason'] else null,
        "connect_end_user_permission_name": get_connect_end_user_permission_name(mapper),
        "connect_end_user_permission_uri":rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_sets"), "name", get_connect_end_user_permission_name(mapper)
            ) if get_connect_end_user_permission_name(mapper) else null,

        ## INDIA specific values ##
        "parent_company": next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']==item['companycode']
            ), null),

        "timesheet_template_name": get_timesheet_template_india(item, mapper, country),
        "timesheet_template_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets'),'name', get_timesheet_template_india(item, mapper, country), 'uri')
            if get_timesheet_template_india(item, mapper, country) else null,

        "timesheet_approval_path": next((res['Value'] for res in mapper if res['Type']=="Timesheet Approval" and res['Source']==(
            next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null))
                and res['Country']==country), null),

        "timeoff_template":next((res['Value'] for res in mapper if res['Type']=="Timeoff Template" and res['Source']==(
            next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null))
                and res['Country']==country), null),

        "timeoff_approval": next((res['Value'] for res in mapper if res['Type']=="Timeoff Approval" and res['Source']==(
            next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null))
                and res['Country']==country), null),

        "timesheet_period": next((res['Value'] for res in mapper if res['Type']=="Timesheet Period" and res['Source']==(
            next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null))
                and res['Country']==country), null),

        "workweek": next((res['Value'] for res in mapper if res['Type']=="WorkWeek" and res['Source']==
            (next(res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']))),null)
            if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null)
            else null,

        "workweek_uri": next((res['URI'] for res in mapper if res['Type']=="WorkWeek" and res['Source']==
            (next(res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']))),null)
            if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null)
            else null,

        "activity_list": (
                [res['Value'] for res in mapper if res['Type']=="Activities" and res['Country']== country and res['Source']=="COMPASS" and res['personnelsubarea']==job_level_set]
            if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) =="COMPASS" else null
            ) if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) else null,
    
        "employee_type_full_path":
            ( _get_employee_type_full_path(item)
                if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) =="C1" else
                ("Exempt – Salaried" if item['exempt']=="Yes" else "Non Exempt - Hourly")
            ) if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) else
             ("Exempt – Salaried" if item['exempt']=="Yes" else "Non Exempt - Hourly"),

        "timezone_name": next((res['Value'] for res in mapper if res['Type']=="TimeZone" and res['Country']==country), null),
        "timezone_uri": next((res['URI'] for res in mapper if res['Type']=="TimeZone" and res['Country']==country), null),

        "holiday_calender_name": get_holiday_calender_name_india(item, mapper, country),
        "holiday_calender_uri":rail.find_first_by_attr_and_get_attr(rail.result('get_all_holiday_calendar'),"name",get_holiday_calender_name_india(item, mapper, country), "uri")
            if (get_holiday_calender_name_india(item, mapper, country) if next((res['Source'] for res in mapper if res['Type']=="Company Code"
                and res['URI']== item['companycode']), null) else null) else null,

        "allowed_country": ("Enable" if next((res['Value'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) else null)
        if next((res['Value'] for res in mapper if res['Type']=="Country to enable" and res['Country']==country), null) else null,

        "timesheet_period_effective_date":timesheet_period_eff_date,
        "timesheet_period_effective_date_json_format": get_json_date_from_date_str(timesheet_period_eff_date, TIMESHEET_PERIOD_EFFECTIVE_DATE)if timesheet_period_eff_date else null,

        "profile_status":next((res['status'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null)
            if next((res['status'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) else "disabled",

        "timeentry_approval_path_name": next((res['Value'] for res in mapper if res['Type']=="Time Entry Approval Path" and res['Source']==
                (next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null))), null)
            if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) else null,

        "payrule": next((res['Value'] for res in mapper if res['Type']=="Payrule" and res['Source']==(
                next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null)
            if next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null) else "No Payrule")
            and res['Country']==country
            and res['personnelsubarea']== job_level_set
            ), null),

        "punch_entry_policy_name": get_punch_entry_policy_name_india(item, mapper, country),
        "punch_entry_policy_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets'),'name', get_punch_entry_policy_name_india(item, mapper, country), 'uri')
            if get_punch_entry_policy_name_india(item, mapper, country) else null,
    }

def get_india_user_process_conf(item, dag_run, config):
    employee_data, location_data, department_data, division_data, cost_center_data = get_groups_data_global()
    schedule_manager =  rail.find_first_by_attr_and_get_attr(config.DXC_WORKDAY_USER_SYNC_USER_MAPPER,"Type","Supervisor Scheduler Permission", "Value")

    _country = item["_country_to_use_for_query"]
    _state = item["_state_to_use_for_query"]
    _home_country, _home_state = get_home_state_and_country_for_ia(dag_run, item)
    mapper_derived_data=_mapper_derived_values_india(item, config.DXC_WORKDAY_USER_SYNC_USER_MAPPER, _country)
    return {
            "file_data" : {
                "emp_id": item["empid"],
                "perner_id": item["pernerid"],
                "email_id": item["email"],
                "first_name": item["firstname"],
                "last_name": item["lastname"],
                "country": _country,
                "state": _state,
                "exempt": item["exempt"],
                "employee_type": item["employeetype"],
                "hire_date": item["hiredate"],
                "gender": item["gender"],
                "service_date": item["servicedate"],
                "term_date": item["termdate"],
                "status": item["status"],
                "on_leave": item["onleave"],
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
                "home_country": _home_country,
                "home_state": _home_state,
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
                "ia_start_date": item["iastartdate"],
                "ia_end_date": item["iaenddate"],
                "exempt_effective_date": item["exempteffectivedate"],
                "is_ia": item["isia"],
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
            },
            "mapper_data": _mapper_derived_values_india(item, config.DXC_WORKDAY_USER_SYNC_USER_MAPPER, _country),
            "udfs": _get_user_udfs_details(),
            #"item" : item, # this is added for ref only
            "fulltime_parttime": ("Part Time" if float(item["ftepct"]) < 100 else "Full Time") if item["ftepct"] else "Full Time",
            "starting_balance_set_to_uri": dag_run.conf.get("starting_balance_set_to_uri"),
            "prevent_balance_overdraw_uri": dag_run.conf.get("prevent_balance_overdraw_uri"),
            "job_level_set": ("8-" if int(item["joblevel"]) < 9 else "8+") if item["joblevel"] else "No",
            "supervisor_user_log": dag_run.conf.get("supervisor_user_log"),

            "schedule_uri":rail.find_first_by_attr_and_get_attr(rail.result("get_all_office_schedules"),'displayText',item['workshift'],'uri')
                if item['workshift'] else null,
            "schedule_manager_permission_uri_for_supervisor": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),
                "name",schedule_manager, "uri") if schedule_manager else null,

            "master_file_name": dag_run.conf["file_name"],
            "splitter_batch_name": "COMPASS",

            "location_uri": rail.find_first_by_attr_and_get_attr(
                location_data,
                'fullpath',
                _get_location_full_path(item),
                'uri'
            ),
            "employee_type_list": rail.write_json_artifact(employee_data),
            "employee_tye_uri": rail.find_first_by_attr_and_get_attr(employee_data,'full_path',
                ("Exempt – Salaried" if item["exempt"]=="Yes" else "Non Exempt - Hourly"),"uri"),
            "employee_type_uri_for_all": rail.find_first_by_attr_and_get_attr(
                employee_data,'full_path',_get_employee_type_full_path(item),"uri"),
            "cost_center_uri": rail.find_first_by_attr_and_get_attr(
                cost_center_data,
                "displayText",
                item["costcenter"],
                'uri'
            ) if item["costcenter"] else null,
            "company_code_list": rail.write_json_artifact(division_data),
            "company_code_uri": (rail.find_first_by_attr_and_get_attr(
                division_data,
                "full_path",
                next((res['Value'] for res in config.DXC_WORKDAY_USER_SYNC_USER_MAPPER if res['Type']=="Company Code" and res['URI']== item['companycode']), null),
                "uri"
            )
            if rail.find_first_by_attr_and_get_attr(
                division_data,
                "full_path",
                next((res['Value'] for res in config.DXC_WORKDAY_USER_SYNC_USER_MAPPER if res['Type']=="Company Code" and res['URI']== item['companycode']), null),
                "uri"
            )
            else rail.find_first_by_attr_and_get_attr(division_data, "name", item['companycode'], "uri")),
            "organizational_unit_uri": rail.find_first_by_attr_and_get_attr(department_data, "displayText", item['orgcode'], "uri")
                if item['orgcode'] else null,

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
                "timesheet_period_effective_date": get_json_date_from_date_str(mapper_derived_data['timesheet_period_effective_date'], TIMESHEET_PERIOD_EFFECTIVE_DATE),
            }
        }

def get_activity_list_usa_csc(parent_company, item, mapper):
    if parent_company:
        if parent_company == "COMPASS":
            return "|".join([activity['Value'] for activity in list(filter(lambda row: row['Type'] == "Activities" and row['Source'] == "COMPASS" and row['Country'] == item['_country_to_use_for_query'],  mapper))])
        if parent_company == "FTP":
            return "|".join([activity['Value']for activity in list(filter(lambda row: row['Type'] == "Activities" and row['Source'] == "COMPASS",  mapper))])
    return null

def get_c1_activity_list_us_csc(parent_company, item, mapper, _psg):
    res = list(filter(lambda row: row['Type'] == "Activities" and\
        row['Country'] == item["_country_to_use_for_query"] and\
        row['Source'] == parent_company and\
        row['URI'] == (item['subareacode'] if item['subareacode'] in PERSONNEL_SUB_AREA_CODE_TO_GROUP else "All - Others") and\
        row['personnelsubarea'] == _psg and\
        row['employeegroup'] == item['empgroupcode'] and\
        row['employeesubgroup'] == item['empsubgroupcode'] and\
        row['status'] == (((item['_state_to_use_for_query'] if item['_state_to_use_for_query'] in STATE_TO_GROUP else "All - Others") 
        if item['country'] == "United States of America" else item['_country_to_use_for_query'])), mapper))
    
    if res:
        return list(map(lambda activity_value:activity_value['Value'], res))

def get_holiday_calender_name_usa_csc_updated(item, parent_company, mapper):
    step_number = get_mapper_values_for_usa_csc_step_number_for_diff_data(item, mapper, parent_company)
    
    # Get company source first
    company_code_row = next(filter(lambda row: 
        row['Type'] == "Company Code" and 
        row['URI'] == parent_company, mapper), {})
    
    company_source = company_code_row.get('Source')
    if not company_source:
        return None
    
    # Step 72: No C1 check, default case uses col10
    if step_number == 72:
        if item['state'] == "Puerto Rico":
            return next(filter(lambda row: 
                row['Type'] == "Holiday Calendar" and 
                row['Country'] == "Puerto Rico" and 
                row['Source'] == company_source, mapper), {}).get('Value')
        
        elif item['state'] == "Rhode Island":
            return next(filter(lambda row: 
                row['Type'] == "Holiday Calendar" and 
                row['Country'] == item['country'] and 
                row['Source'] == company_source and
                row['status'] == "Rhode Island", mapper), {}).get('Value')
        
        else:  # Default case WITH status filter
            return next(filter(lambda row: 
                row['Type'] == "Holiday Calendar" and 
                row['Country'] == item['country'] and 
                row['Source'] == company_source and
                row['status'] == item['state'], mapper), {}).get('Value')
    
    # Step 75: C1 check, simple logic, no special cases
    elif step_number == 75:
        if parent_company == "C1":
            return next(filter(lambda row: 
                row['Type'] == "Holiday Calendar" and 
                row['Country'] == item['country'] and 
                row['Source'] == company_source, mapper), {}).get('Value')
        return None
    
    # Step 81: C1 check, special cases, default NO col10
    elif step_number == 81:
        if parent_company == "C1":
            if item['state'] == "Puerto Rico":
                return next(filter(lambda row: 
                    row['Type'] == "Holiday Calendar" and 
                    row['Country'] == "Puerto Rico" and 
                    row['Source'] == company_source, mapper), {}).get('Value')
            
            elif item['state'] == "Rhode Island":
                return next(filter(lambda row: 
                    row['Type'] == "Holiday Calendar" and 
                    row['Country'] == item['country'] and 
                    row['Source'] == company_source and
                    row['status'] == "Rhode Island", mapper), {}).get('Value')
            
            else:  # Default case WITHOUT status filter
                return next(filter(lambda row: 
                    row['Type'] == "Holiday Calendar" and 
                    row['Country'] == item['country'] and 
                    row['Source'] == company_source, mapper), {}).get('Value')
        return None
    return None

def _mapper_derived_values_usa_csc(item, config, parent_company):
    mapper = config.DXC_WORKDAY_USER_SYNC_USER_MAPPER
    _emp_type = "Exempt – Salaried" if item['exempt'] == "Yes" else "Non Exempt - Hourly"
    _psg = next((res['Value'] for res in mapper if res['Type']=="PSG" and res['Source']=='C1' and res['personnelsubarea']==item['areacode']
            and res['employeegroup']==item['subareacode'] and res['status']==item['companycode']
            ), "")
    return {
        "office_schedule": rail.find_first_by_attr_and_get_attr(mapper,"Type","Office Schedule", "Value"),
        "authentication": rail.find_first_by_attr_and_get_attr(mapper,"Type","Authentication", "Value"),
        "timesheet_approval_path": next(filter(lambda row: row['Type'] == "Timesheet Approval" and  row['Source'] == parent_company and row['Country'] == item['_country_to_use_for_query'],  mapper), {}).get('Value'),
        "timesheet_period": rail.find_first_by_attr_and_get_attr(mapper,"Type","Timesheet Period", "Value"),
        "authentication_uri": rail.find_first_by_attr_and_get_attr(mapper,"Type","Authentication", "URI"),
        "end_user_permission_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),"name",
                get_permission_name_global(mapper)['end_user_permission_name'], "uri"
            ) if get_permission_name_global(mapper)['end_user_permission_name'] else null,
        "supervisor_user_permission_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),"name",
                get_permission_name_global(mapper)['supervisor_user_permission_name'], "uri"
            ) if get_permission_name_global(mapper)['supervisor_user_permission_name'] else null,
        "product": get_product_data_global(mapper)['product_names'],
        "product_uri": get_product_data_global(mapper)['product_uris'],
        "language": rail.find_first_by_attr_and_get_attr(mapper,"Type","Language", "Value"),
        "language_uri": rail.find_first_by_attr_and_get_attr(mapper,"Type","Language", "URI"),
        "supervisor_end_user_permission_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'),"name",
                get_permission_name_global(mapper)['supervisor_end_user_permission_name'], "uri"
            ) if get_permission_name_global(mapper)['supervisor_end_user_permission_name'] else null,
        "supervisor_user_permission_name":rail.find_first_by_attr_and_get_attr(mapper,"Type","Supervisor User Permission", "Value"),
        "supervisor_end_user_permission_name": rail.find_first_by_attr_and_get_attr(mapper,"Type","Supervisor End User Permission", "Value"),
        "end_user_permission_name": rail.find_first_by_attr_and_get_attr(mapper,"Type","End User Permission", "Value"),
        "schedule_type_uri": get_mapper_derived_values_global_multiple_checks(mapper,"Type","Schedule Type","Country","Default","URI"),
        "timesheet_approval_canada": get_mapper_derived_values_global_multiple_checks(mapper,"Type","Timesheet Approval","Source","C1","Value"),
        "timeoff_approval_canada": get_mapper_derived_values_global_multiple_checks(mapper,"Type","Timeoff Approval","Source","C1","Value"),
        "supervisor_schedule_manager_name": rail.find_first_by_attr_and_get_attr(mapper,"Type","Supervisor Scheduler Permission", "Value"),
        "psg": _psg,
        "termination_reason_code": next((res['Value'] for res in mapper if res['Type']=="Termination Reason" and res['Source']==item['terminationreason']
                and res['URI']==item['_state_to_use_for_query']), null) if item['terminationreason'] else null,
        "connect_end_user_permission_name": get_connect_end_user_permission_name(mapper),
        "connect_end_user_permission_uri":rail.find_first_by_attr_and_get_attr(rail.result("get_all_permission_sets"), "name", get_connect_end_user_permission_name(mapper)
            ) if get_connect_end_user_permission_name(mapper) else null,
        "employee_type": _emp_type,
        "employee_type_for_all": _get_employee_type_full_path(item),
        "timesheet_template": next(filter(lambda row: row['Type'] == "Timesheet Template" and row['Country'] == item['_country_to_use_for_query']\
                                                 and row['Source'] == parent_company  and row['personnelsubarea'] == _emp_type\
                                                 and row['employeegroup'] == ("All Others" if _emp_type == "Exempt – Salaried" else (
                                                     item['_state_to_use_for_query'] if item['_state_to_use_for_query'] in STATE_TO_GROUP else "All Others")) and row['status'] == item['companycode'], mapper), {}).get('Value'),
        # "Timesheeturi" will be calculated later
        "timeoff_template": next((res['Value'] for res in mapper if res['Type']=="Timeoff Template" and res['Source']==(
                next((res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']), null))
                and res['Country']==item['_country_to_use_for_query']), null),
        "timeoff_approval": next(filter(lambda row: row['Type'] == "Timeoff Approval" and  row['Source'] == parent_company and row['Country'] == item['_country_to_use_for_query'] and row['status'] == item['companycode'],  mapper), {}).get('Value'),
        "timesheet_period": next(filter(lambda row: row['Type'] == "Timesheet Period" and  row['Source'] == parent_company and row['Country'] == item['_country_to_use_for_query'] and row['personnelsubarea'] == _emp_type,  mapper), {}).get('Value'),
        "work_week": (next((res['Value'] for res in mapper if res['Type']=="WorkWeek" and res['Source']==
                    (next(res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']))),null) if ("Exempt – Salaried" if item['file_data']['exempt']=="Yes" else
                     "Non Exempt - Hourly") == "Exempt – Salaried" else next((res['Value'] for res in mapper if res['Type']=="WorkWeek" and res['Source']==
                    (next(res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']))
                     and res['Country'] == item['_country_to_use_for_query'] and res['personnelsubarea'] ==("Exempt – Salaried" if item['file_data']['exempt']=="Yes" else "Non Exempt - Hourly") ),null))
                    if parent_company =="COMPASS" else next((res['Value'] for res in mapper if res['Type']=="WorkWeek" and res['Source']==
                    (next(res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']))),null),
        "work_week_uri": (next((res['URI'] for res in mapper if res['Type']=="WorkWeek" and res['Source']==
                    (next(res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']))),null) if ("Exempt – Salaried" if item['file_data']['exempt']=="Yes" else
                     "Non Exempt - Hourly") == "Exempt – Salaried" else next((res['URI'] for res in mapper if res['Type']=="WorkWeek" and res['Source']==
                    (next(res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']))
                     and res['Country'] == item['_country_to_use_for_query'] and res['personnelsubarea'] ==("Exempt – Salaried" if item['file_data']['exempt']=="Yes" else "Non Exempt - Hourly") ),null))
                    if parent_company =="COMPASS" else next((res['URI'] for res in mapper if res['Type']=="WorkWeek" and res['Source']==
                    (next(res['Source'] for res in mapper if res['Type']=="Company Code" and res['URI']== item['companycode']))),null),
        "activity": get_activity_list_usa_csc(parent_company, item, mapper),
        "employee_type_full_path": get_employee_type_full_path(parent_company, item, _emp_type),
        "timezone": next(filter(lambda row: row['Type'] == "TimeZone" and  row['Country'] == item['_country_to_use_for_query'] and row['Source'] == item['_state_to_use_for_query'],  mapper), {}).get('Value'),
        "timezone_uri": next(filter(lambda row: row['Type'] == "TimeZone" and row['Country'] == item['_country_to_use_for_query'] and row['Source'] == item['_state_to_use_for_query'],  mapper), {}).get('URI'),
        "holiday_calendar": get_holiday_calender_name_usa_csc_updated(item, parent_company, mapper),
        # "holiday_calendar_uri" will be derivied later
        "allowed_country": get_allowed_country(item, mapper, parent_company),
        "timesheet_period_effective_date": next(filter(lambda row: row['Type'] == "Timesheet Period Effective Date" and row['Country'] == item['_country_to_use_for_query'], mapper), {}).get('Value'),
        "c1payrule": next(filter(lambda row: row['Type'] == "Payrule" and\
                                        row['Country'] == item['_country_to_use_for_query'] and\
                                        row['Source'] == parent_company and\
                                        row['URI'] == (item['subareacode'] if item['subareacode'] in PERSONNEL_SUB_AREA_CODE_TO_GROUP else "All - Others") and\
                                        row['personnelsubarea'] == _psg and\
                                        row['employeegroup'] == item['empgroupcode'] and\
                                        row['employeesubgroup'] == item['empsubgroupcode'] and\
                                        row['status'] == (((item['_state_to_use_for_query'] if item['_state_to_use_for_query'] in STATE_TO_GROUP else "All - Others") if item['_country_to_use_for_query'] == "United States of America" else item['_country_to_use_for_query'])), mapper), {}).get('Value'),
        "c1_timesheet_template": next(filter(lambda row:  row['Type'] == "Timesheet Template" and\
                                                        row['Country'] == item['_country_to_use_for_query'] and\
                                                        row['Source'] == parent_company and\
                                                        row['URI'] == (item['subareacode'] if item['subareacode'] in PERSONNEL_SUB_AREA_CODE_TO_GROUP else "All - Others") and\
                                                        row['personnelsubarea'] == _psg and\
                                                        row['employeegroup'] == item['empgroupcode'] and\
                                                        row['employeesubgroup'] == item['empsubgroupcode'] and\
                                                        row['status'] == (((item['_state_to_use_for_query'] if item['_state_to_use_for_query'] in STATE_TO_GROUP else "All - Others") if item['_country_to_use_for_query'] == "United States of America" else item['_country_to_use_for_query'])), mapper), {}).get('Value'),
        "timesheet_approval_c1": next(filter(lambda row: row['Type'] == "Timesheet Approval" and\
                                        row['Country'] == item["_country_to_use_for_query"] and\
                                        row['Source'] == parent_company, mapper), {}).get('Value'),
        "timeoff_approval_c1": next(filter(lambda row: row['Type'] == "Timeoff Approval" and\
                                        row['Country'] == item["_country_to_use_for_query"] and\
                                        row['Source'] == parent_company, mapper), {}).get('Value'),

        "c1_activities": rail.smartjoin_by_delim(get_c1_activity_list_us_csc(parent_company, item, mapper, _psg), separator="|") 
                if get_c1_activity_list_us_csc(parent_company, item, mapper, _psg) else null,

        "timesheet_period_c1": next(filter(lambda row: row['Type'] == "Timesheet Period" and\
                                        row['Country'] == item["_country_to_use_for_query"] and\
                                        row['Source'] == parent_company, mapper), {}).get('Value'),
        "profile_status": next(filter(lambda row: row['Type'] == "Company Code" and\
                                        row['URI'] == item["companycode"], mapper), {}).get('status', 'disabled'),
        "timeentry_approval_path_name": next(filter(lambda row: row['Type'] == "Time Entry Approval Path" and\
                                        row['Source'] == parent_company, mapper), {}).get('Value') if parent_company else null,
        "payrule": next(filter(lambda row: row['Type'] == "Payrule" and\
                                        row['Country'] == item["_country_to_use_for_query"] and\
                                        row['Source'] == parent_company and\
                                        row['personnelsubarea'] == _emp_type and\
                                        row['employeegroup'] == ("All Others" if _emp_type == "Exempt – Salaried" else (item['_state_to_use_for_query'] if item['_state_to_use_for_query'] in STATE_TO_GROUP else "All Others")) and\
                                        row['status'] == item['companycode'], mapper), {}).get('Value'),
        "schedule_hours": next(filter(lambda row: row['Type'] == "Schedule Hours" and\
                                        row['Source'] == item['workshift'], mapper), {}).get('Value'),
        "punch_entry_policy": next(filter(lambda row: row['Type'] == "Punch Entry Policy" and\
                                                        row['Source'] == parent_company and\
                                                        row['personnelsubarea'] == _psg and\
                                                        row['employeegroup'] == item['empgroupcode'] and\
                                                        row['employeesubgroup'] == item['empsubgroupcode'] and\
                                                        row['status'] == item['_state_to_use_for_query'], mapper), {}).get('Value')
    }

def get_puch_entry_policy_usa_csc(item, parent_company, mapper):
    step_number = get_mapper_values_for_usa_csc_step_number_for_diff_data(item, mapper, parent_company)
    if step_number == 72:
        return next(filter(lambda row: row['Type'] == "Punch Entry Policy" and\
                                        row['Source'] == parent_company and\
                                        row['personnelsubarea'] == item['psg'] and\
                                        row['employeegroup'] == item['empgroupcode'] and\
                                        row['employeesubgroup'] == item['empsubgroupcode'] and\
                                        row['status'] == item['_state_to_use_for_query'], mapper), {}).get('Value')
    if step_number == 75:
        return next(filter(lambda row: row['Type'] == "Punch Entry Policy" and\
                                        row['Source'] == parent_company and\
                                        row['personnelsubarea'] == item['psg'] and\
                                        row['employeegroup'] == item['empgroupcode'] and\
                                        row['employeesubgroup'] == item['empsubgroupcode'], mapper), {}).get('Value')
    if step_number == 81:
        return null
    if step_number == 92:
        return null
    return null

def get_activity_list_usa_csc(parent_company, item, mapper):
    if parent_company:
        if parent_company == "COMPASS":
            return "|".join([activity['Value'] for activity in list(filter(lambda row: row['Type'] == "Activities" and row['Source'] == "COMPASS" and row['Country'] == item['_country_to_use_for_query'],  mapper))])
        if parent_company == "FTP":
            return "|".join([activity['Value']for activity in list(filter(lambda row: row['Type'] == "Activities" and row['Source'] == "COMPASS",  mapper))])
    return null

def get_work_week_usa_csc(parent_company, _emp_type, item, mapper, pluck_key='Value'):
    step_number = get_mapper_values_for_usa_csc_step_number_for_diff_data(item, mapper, parent_company)
    if step_number == 72:
        if parent_company == "COMPASS":
            if _emp_type == "Exempt – Salaried":
                return next(filter(lambda row: row['Type'] == "WorkWeek" and row['Source'] == parent_company, mapper), {}).get(pluck_key)
            return next(filter(lambda row: row['Type'] == "WorkWeek" and row['Source'] == parent_company and row['Country'] == item['_country_to_use_for_query'] and row['personnelsubarea'] == _emp_type, mapper), {}).get(pluck_key)
        return next(filter(lambda row: row['Type'] == "WorkWeek" and row['Source'] == parent_company, mapper), {}).get(pluck_key)
    if step_number == 75:
        return next(filter(lambda row: row['Type'] == "WorkWeek" and row['Source'] == parent_company and row['Country'] == item['_country_to_use_for_query'] and row['personnelsubarea'] == _emp_type,mapper), {}).get(pluck_key)
    if step_number == 81:
        return next(filter(lambda row: row['Type'] == "WorkWeek" and row['Source'] == parent_company, mapper), {}).get(pluck_key)
    if step_number == 92:
        return next(filter(lambda row: row['Type'] == "WorkWeek" and row['Source'] == parent_company, mapper), {}).get(pluck_key)
    return null

def _get_user_permission_set_details_usa_csc(mapper_data):
    return {
            "end_user_permission": mapper_data['end_user_permission_uri'],
            "supervisor_user_permission": mapper_data['supervisor_user_permission_uri'],
            "supervisor_end_user_permission": mapper_data['supervisor_end_user_permission_uri'],
        }

def get_mapper_values_for_usa_csc_step_number_for_diff_data(item, mapper, parent_company):
    if item['_country_to_use_for_query'].lower() == "united states of america" and parent_company.lower() != "compass":
        return 72
    if item['_country_to_use_for_query'].lower() == "puerto rico" and parent_company.lower() == "c1":
        return 75
    if item['_country_to_use_for_query'].lower() == "united states of america" and parent_company.lower() == "compass":
        return 81
    # combnination didnt match
    return 0


def get_usa_csc_user_process_conf(item, dag_run, config):
    employee_data, location_data, department_data, division_data, cost_center_data = get_groups_data()
    # TODO: add it to documentation file
    # this will be used to determine which values that will be sent to child for processing 
    # from Process Users to Add/Update User (Workato)
    parent_company = list(filter(lambda row: row['Type'] == "Company Code" and row['URI'] == item['companycode'], config.DXC_WORKDAY_USER_SYNC_USER_MAPPER))
    if parent_company is None:
        parent_company = ""
    else:
        parent_company = parent_company[0]['Source']   
    # this will be used to determine which values that will be sent to child for processing 
    # From batch_processor to process users (workato)
    company_code_uri = get_company_code_value_for_uri(item, config.DXC_WORKDAY_USER_SYNC_USER_MAPPER, division_data)
    policy_data = rail.result('get_all_policy_sets')
    _country = item["_country_to_use_for_query"]
    _state = item["_state_to_use_for_query"]
    _home_country, _home_state = get_home_state_and_country_for_ia(dag_run, item)
    mapper_derived_data = _mapper_derived_values_usa_csc(item, config, parent_company)

    return {
            "item" : item, # this is added for ref only
            "starting_balance_set_to_uri": dag_run.conf.get("starting_balance_set_to_uri"),
            "prevent_balance_overdraw_uri": dag_run.conf.get("prevent_balance_overdraw_uri"),
            "supervisor_user_log": dag_run.conf.get("supervisor_user_log"),
            "employee_type_data": cached_write_json_artifact('get_all_employeegroup_data'),
            "division_data": cached_write_json_artifact('get_all_companycode_data'),
            "master_file_name": dag_run.conf["file_name"],
            "splitter_batch_name": "C1 USA CSC",
            "allowed_country": get_allowed_country(item, config.DXC_WORKDAY_USER_SYNC_USER_MAPPER, parent_company),
            "replicon_field": 'true' if item['status'] in [1,'1'] else 'false',
            "file_data" : {
                "emp_id": item["empid"],
                "perner_id": item["pernerid"],
                "email_id": item["email"],
                "first_name": item["firstname"],
                "last_name": item["lastname"],
                "country": _country,
                "state": _state,
                "exempt": item["exempt"],
                "exempt_effective_date": item["exempteffectivedate"],
                "employee_type": item["employeetype"],
                "hire_date": item["hiredate"],
                "gender": item["gender"],
                "service_date": item["servicedate"],
                "term_date": item["termdate"],
                "status": item["status"],
                "on_leave": item["onleave"],
                "parent_company": parent_company,
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
                "home_country": _home_country,
                "home_state": _home_state,
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
                "assignment_type": item["assignment_type"]
            },
            "mapper_data":mapper_derived_data,
            "payrule":  {
                "payrule": ("US LCSC MDA Union - 36 hours" if item["scheduledweeklyhours"] == "36" else "US LCSC MDA Union") if mapper_derived_data['c1payrule']=="US LCSC MDA Union" else  mapper_derived_data['c1payrule'],
            },
            "activities": {
                "activity" : mapper_derived_data['activity'],
                "c1_activity": mapper_derived_data['c1_activities']
            },
            "user_permission_sets" : _get_user_permission_set_details_usa_csc(                
                mapper_derived_data
            ),
            "holiday_calendar": {
                "holiday_calendar": mapper_derived_data['holiday_calendar'],
                "holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(get_holiday_calender_details(), 'name', mapper_derived_data['holiday_calendar'], 'uri')
            },
            "schedule_data": {
                "work_schedule" : item['workshift'],
                "schedule_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_office_schedules"),
                                                                          "displayText",item['workshift'], 'uri'),
                "office_schedule" : mapper_derived_data['office_schedule'],
                "schedule_hours": mapper_derived_data['schedule_hours'],
                "work_week": mapper_derived_data['work_week'],
                "schedule_type_uri": mapper_derived_data['schedule_type_uri'],
            },
            "policy_sets": {
                "punch_entry_policy": {
                    **{"punch_entry_policy": mapper_derived_data['punch_entry_policy']},
                    **rail.find_first_by_attr_and_get_attr(policy_data, 'name', mapper_derived_data['punch_entry_policy'], default={})
                },
                "timeoff_template": {
                    **{"timeoff_template": mapper_derived_data['timeoff_template']},
                    **rail.find_first_by_attr_and_get_attr(policy_data, 'name', mapper_derived_data['timeoff_template'], default={})
                },
                "timesheet_period": {
                    **{"timesheet_period": mapper_derived_data['timesheet_period']},
                    **rail.find_first_by_attr_and_get_attr(policy_data, 'name', mapper_derived_data['timesheet_period'], default={})
                },
                "c1_timesheet_period": {
                    **{"c1_timesheet_period": mapper_derived_data['timesheet_period_c1']},
                    **rail.find_first_by_attr_and_get_attr(policy_data, 'name', mapper_derived_data['timesheet_period_c1'], default={})
                },
                "timesheet_approval_path": {
                    **{"timesheet_approval_path": mapper_derived_data['timeentry_approval_path_name']},
                    **rail.find_first_by_attr_and_get_attr(policy_data, 'name', mapper_derived_data['timeentry_approval_path_name'], default={})
                },
                "timesheet_template": {
                    **{"timesheet_template": mapper_derived_data['timesheet_template']},
                    **rail.find_first_by_attr_and_get_attr(policy_data, 'name', mapper_derived_data['timesheet_template'], default={})
                },
                "c1_timesheet_template": {
                    **{"c1_timesheet_template": mapper_derived_data['c1_timesheet_template']},
                    **rail.find_first_by_attr_and_get_attr(policy_data, 'name', mapper_derived_data['c1_timesheet_template'], default={})
                }
            },
            "timezone": {
                "timezone": mapper_derived_data["timezone"],
                "timezone_uri" : mapper_derived_data["timezone_uri"]
            },
            "udfs": _get_user_udfs_details(),
            "groups": {
                # Location
                "location": rail.find_first_by_attr_and_get_attr(
                    location_data,
                    'fullpath',
                    _get_location_full_path(item),
                    default={}
                ),
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
                    "employee_type_full_path": rail.find_first_by_attr_and_get_attr(employee_data, 'full_path', mapper_derived_data['employee_type_full_path'], default={}),
                    "uri": ((rail.find_first_by_attr_and_get_attr(employee_data,
                                    'full_path',
                                    "Exempt – Salaried",
                                    default={})) if item["exempt"]=="Yes" else (rail.find_first_by_attr_and_get_attr(employee_data,
                                    'full_path',
                                    "Non Exempt - Hourly",
                                    default={}))),
                    "employee_type_uri_for_all": rail.find_first_by_attr_and_get_attr(
                            employee_data,'full_path',mapper_derived_data['employee_type_full_path'],"uri")
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
                "date_of_birth": get_json_date_from_date_str(item['dob']),
                "exempt_eff_date": get_json_date_from_date_str(item['exempteffectivedate']),
                #TimeSheetPeriodEffectiveDate #! mapper value
                "timesheet_period_effective_date": get_json_date_from_date_str(mapper_derived_data['timesheet_period_effective_date'], TIMESHEET_PERIOD_EFFECTIVE_DATE),
            }
        }

def cost_center_updated(dag_run, current_user_groups, effective_date, _get_cost_center_update_payload:Callable):
    return bool(_get_cost_center_update_payload(dag_run, current_user_groups, effective_date))

def department_updated(dag_run, current_user_groups, effective_date, _get_department_update_payload:Callable):
    return bool(_get_department_update_payload(dag_run, current_user_groups, effective_date))

def get_psa_user_udf_add_update_payload(dag_run, current_udf_value:str, caller:str, current_user_groups, effective_date, _get_cost_center_update_payload:Callable, _get_department_update_payload:Callable):
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
        if cost_center_updated(dag_run, current_user_groups, effective_date, _get_cost_center_update_payload
            ) or department_updated(dag_run, current_user_groups, effective_date, _get_department_update_payload
                ):
            if current_udf_value.lower() != psa_user_value.lower():
                return psa_user_value
        return None
    else:
        raise

def get_psa_udf_value(dag_run, current_custom_fields_values, current_user_groups, custom_fields_payload:list, _get_custom_fields_payload:Callable):
    psa_flag = get_psa_user_udf_add_update_payload(dag_run, rail.find_first_by_attr_and_get_attr(
        current_custom_fields_values, "customField.displayText", "PSA User", 'text', default=""), "update",
        current_user_groups, null)
    if psa_flag is not None:
        custom_fields_payload.append(
                    _get_custom_fields_payload(uri=dag_run.conf['udfs']['psa_user'].get('uri'), drop_down_value_name=psa_flag))

