from datetime import datetime, timedelta
import functools
from pendulum import now
from airflow.models import DagRun
from wipro.user_import_germany_v1.utils import request_payload

import rail
INVALID_DATES = ["9999-12-31", "0000-00-00"]
null = None


def get_error_message():
    context = rail.get_current_context()
    failed_task_ids = rail.lib.errors.get_failed_task_ids(context)
    error_message = ''
    if failed_task_ids:
        error_key = (context['ti'].xcom_pull(
            failed_task_ids[0], key='error') or 'Unknown error occurred')
        error_message = (error_key.get("response").get("body") if error_key.get("response")
                         else error_key.get('exc_message')) if isinstance(error_key, dict) else error_key

    return error_message


def get_today_date():
    now_date = datetime.utcnow()
    return {
        'year': now_date.year,
        'month': now_date.month,
        'day': now_date.day
    }


def get_invalid_user_log_details(item):
    message = "User not processed due to following reason/s "
    mandatory_fields = ["employee_id", "employee_first_name",
                        "country", "location",
                        "employment_status", "company_code", "personnel_area_text",
                        "adid", "date_of_joining","work_council_tagging"]
    for i in item:
        if not item[i] and i in mandatory_fields:
            message += i + ";"
    message += " are not present"
    return message


def get_all_custom_fields_data(response):
    return list(map(lambda i: {
        "displayText": i["displayText"],
        "uri": i["uri"]
    }, response))


def get_all_object_extension_fields_data(response):
    return list(map(lambda i: {
        "name": i["name"],
        "uri": i["uri"]
    }, response))


def get_all_legal_entities_data(response):
    return list(map(lambda i: {
        "name": i["cells"][0]["textValue"],
        "code": i["cells"][1].get("textValue", ""),
        "uri": i["cells"][0]["uri"],
    }, response["rows"]))


def get_location_hierarchy_data(response):
    response = response["rows"]
    parent_uri=rail.result("get_germany_parent_location_details")
    location_data = list(map(lambda i: {
        "location": i["cells"][0]["textValue"],
        "locationuri": i["cells"][0]["uri"],
        "parenturi": parent_uri
    }, list(filter(lambda i: i["hierarchyLevel"] == 1, response))))
    return location_data or [{
        "location": None,
        "locationuri": None,
        "parenturi": parent_uri
    }]



def get_payrule_data(response):
    return list(map(lambda i: {
        "name": i["cells"][0]["textValue"],
        "uri": i["cells"][1]["uri"]
    }, response["rows"]))


def get_timesheet_period_data(response):
    return list(map(lambda i: {
        "name": i["cells"][0]["textValue"],
        "uri": i["cells"][0]["uri"]
    }, response["rows"]))


@functools.lru_cache(maxsize=128)
def get_germany_object_extension_fields():
    result = rail.result("get_all_object_extension_fields")
    return {
        "project_supervisor_iduri": rail.find_first_by_attr_and_get_attr(result, "name", "Project Supervisor ID", "uri"),
        "project_supervisor_mailiduri": rail.find_first_by_attr_and_get_attr(result, "name", "Project Supervisor Email", "uri"),
        "hr_manager_iduri": rail.find_first_by_attr_and_get_attr(result, "name", "HR Manager ID", "uri"),
        "hr_manager_mailiduri": rail.find_first_by_attr_and_get_attr(result, "name", "HR Manager Email", "uri"),
        "genderuri": rail.find_first_by_attr_and_get_attr(result, "name", "Gender", "uri"),
        "acquireduri": rail.find_first_by_attr_and_get_attr(result, "name", "Acquired", "uri"),
        "acquired_companyuri": rail.find_first_by_attr_and_get_attr(result, "name", "Acquired Company", "uri"),
        
        "marital_statusuri": rail.find_first_by_attr_and_get_attr(result, "name", "Marital Status", "uri"),
        "onsite_direct_recruituri": rail.find_first_by_attr_and_get_attr(result, "name", "Onsite Direct Recruit", "uri"),
        "sales_identifieruri": rail.find_first_by_attr_and_get_attr(result, "name", "Sales Identifier", "uri"),
        "no_of_childrenuri": rail.find_first_by_attr_and_get_attr(result, "name", "Children", "uri"),
        "gpo_iduri": rail.find_first_by_attr_and_get_attr(result, "name", "HRIS ID", "uri"),
        "gpo_email_iduri": rail.find_first_by_attr_and_get_attr(result, "name", "HRSS Email", "uri"),
        "employee_banduri": rail.find_first_by_attr_and_get_attr(result, "name", "Employee Band", "uri"),
        "billability_statusuri": rail.find_first_by_attr_and_get_attr(result, "name", "Billability Status", "uri"),
        "forfait_emp_identifieruri": rail.find_first_by_attr_and_get_attr(result, "name", "Forfait jour employment identifier", "uri"),
        "employment_statusuri": rail.find_first_by_attr_and_get_attr(result, "name", "Employment Status", "uri"),
        "employment_percentageuri": rail.find_first_by_attr_and_get_attr(result, "name", "FTE", "uri"),
        "personnel_area_texturi": rail.find_first_by_attr_and_get_attr(result, "name", "Personnel area Text", "uri"),
        "personnel_subarea_texturi": rail.find_first_by_attr_and_get_attr(result, "name", "Personnel Subarea Text", "uri"),
        "religionuri": rail.find_first_by_attr_and_get_attr(result, "name", "Religion", "uri"),
        "project_supervisor_adiduri": rail.find_first_by_attr_and_get_attr(result, "name", "Project Supervisor Login Name", "uri")
    }


@functools.lru_cache(maxsize=128)
def get_germany_custom_fields():
    result = rail.result("get_all_custom_fields")
    return {
        "acquired_dojuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Acquired DOJ", "uri"),
        "travel_start_dateuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Travel Start Date", "uri"),
        "onsite_start_dateuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Onsite Start Date", "uri"),
        "onsite_end_dateuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Onsite End Date", "uri"),
        "reversal_dateuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Reversal Date", "uri"),
        "date_of_birthuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Date of Birth", "uri"),
        "hr_adiduri": rail.find_first_by_attr_and_get_attr(result, "displayText", "ID", "uri"),
        "gpo_adiduri": rail.find_first_by_attr_and_get_attr(result, "displayText", "HRSS ID", "uri")
    }

def get_germany_time_off_types(item,config):
    get_all_timeoff_types = []
    if item["gender"].lower() in ["female", "male"]:
        get_all_timeoff_types.extend(
            config.TIME_OFF_TYPES_MAPPER["all_wc"]["all"])
        get_all_timeoff_types.extend(
            config.TIME_OFF_TYPES_MAPPER["all_wc"][item["gender"].lower()])

        work_council_tagging = item["work_council_tagging"].lower().replace(" ","_")
        if work_council_tagging in config.TIME_OFF_TYPES_MAPPER.keys():
            get_all_timeoff_types.extend(config.TIME_OFF_TYPES_MAPPER[work_council_tagging]["all"])
            get_all_timeoff_types.extend(config.TIME_OFF_TYPES_MAPPER[work_council_tagging][item["gender"].lower()])

    get_all_timeoff_types = list(filter(lambda i:i['displayText'] in get_all_timeoff_types, rail.result("get_all_time_off_types")))
    return list(map(lambda i:i["uri"] , get_all_timeoff_types))

@functools.lru_cache
def get_approval_path_uri(country, location, primary_manager_flg, config, new_entity_flag):
    holiday_calendar = config.HOLIDAY_CALENDAR_MAPPER.get(location)
    if new_entity_flag:
        holiday_calendar = config.NEW_ENTITY_MAPPER["holiday_calendar"]
    return {
        "lookuptable": rail.result("create_log_for_user_import_global"),
        "legalentities": rail.result("get_all_legal_entities"),
        "countryuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_countries"), "displayText", country, "uri"),
        "timezoneuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_time_zones"), "displayText", config.GENERAL_MAPPER["time_zone"], "uri"),
        "holidaycalendaruri":  rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_holiday_calendars"), "displayText", holiday_calendar, "uri"),
        "schedule_typeuri": config.GENERAL_MAPPER["schedule_type"],
        "ot_request_template_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["ot_request_template"], "uri"),
        "ot_request_approval_path_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_overtime_approval_paths"), "displayText", config.GENERAL_MAPPER["ot_request_approval_path"], "uri"),
        "payrule_for_non_wbs_eonuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_payrules"), "name", config.GENERAL_MAPPER["payrule_for_non_wbs_eon"], "uri"),
        "payrule_for_non_wbs_rweuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_payrules"), "name", config.GENERAL_MAPPER["payrule_for_non_wbs_rwe"], "uri"),
        "payrule_for_wbsuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_payrules"), "name", config.GENERAL_MAPPER["payrule_fo_wbs"], "uri"),
        "payrule_for_non_wbsuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_payrules"), "name", config.GENERAL_MAPPER["payrule_for_non_wbs"], "uri"),
        "timesheet_for_non_wbs_eonuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["timesheet_for_non_wbs_eon"], "uri"),
        "timesheet_for_non_wbs_rweuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["timesheet_for_non_wbs_rwe"], "uri"),
        "timesheet_for_wbsuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["timesheet_for_wbs"], "uri"),
        "timesheet_for_non_wbsuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["timesheet_for_non_wbs"], "uri"),
        "timesheet_approval_pathuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timesheet_approval_path"), 'displayText', config.GENERAL_MAPPER["timesheet_approval_path"], "uri"),
        "timesheet_system_approval_path_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timesheet_approval_path"), 'displayText', config.GENERAL_MAPPER["timesheet_system_approval_path"], "uri"),
        "timesheet_perioduri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timesheet_periods"), "name", config.GENERAL_MAPPER["timesheet_period"], "uri"),
        "timeoff_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", "Time Off", "uri"),
        "timeoff_system_approval_path_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_approval_path"), "displayText", config.GENERAL_MAPPER["timesheet_system_approval_path"], "uri"),
        "l1_manager_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["l1_manager_wbs"], "uri"),
        "end_user_manager_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["end_user_manager"], "uri"),
        "project_manager_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["project_manager"], "uri"),
        "hr_manager_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["hr_manager"], "uri"),
        "default_user_permission_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["default_user_permission"], "uri"),
        "foreign_manager_emp_type_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_employee_types"), "displayText", config.GENERAL_MAPPER["foreign_manager_emp_type"], "uri"),
        "default_timeoff_annual_leave_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_time_off_types"),
            "displayText",
            config.DEFAULT_SETTINGS_MAPPER[str(country).lower(
            )]["default_time_off_type_for_bookings"],
            "uri"
        ),
        "employeetypeuris" : rail.result("get_all_employee_types"),
        "schedule_policy_uri": rail.result("get_germany_schedule_policy")
    }


def get_germany_supervisor_conf(item,config):
    return {
        **item,
        "l1_manager_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["l1_manager_wbs"], "uri"),

        "lookuptable": rail.result("create_log_for_user_import_global"),
        "end_user_manager_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["l1_manager_non_wbs"], "uri"),
        "foreign_manager_emp_type_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_employee_types"), "displayText", "Foreign Managers", "uri"),
        "country": item["country"]
    }

def get_timeoff_approval_path_name(item, config):
    if config.instance != "prod":
        if item['employee_band'] in config.EMPLOYEE_BAND_FOR_TIMEOFF_APPROVAL_PATH:
            return config.TIMEOFF_APPROVAL_PATH_BASED_ON_EMPLOYEE_BAND
    return  config.GENERAL_MAPPER["timeoff_approval_path"]

def get_germany_user_conf(item,config):
    item["employee_last_name"] = item["employee_last_name"] or "."
    item["hr_manager_id"] = "389668"
    item["hr_manager_mailid"] = "HANSHERMANN.BALON@WIPRO.COM"
    item["hr_adid"] = "HA389668@wipro.com"
    item["acquired"] = "Y" if item.get("acquired_company") else "N"
    if item["project_supervisor_adid"]:
        item["project_supervisor_adid"] = item["project_supervisor_adid"].strip() + "@wipro.com"
    return {
        **item,
        "gpo_adid": "G111999@wipro.com",
        "gpo_email_id": "germany.hrss@wipro.com",
        "gpo_id": "G111999",
        **get_germany_object_extension_fields(),
        **get_germany_custom_fields(),
        **get_approval_path_uri(item["country"],item["location"],item['primary_manager_flg'],config,item["new_entity_flag"]),
        "timeoff_approval_path_name": get_timeoff_approval_path_name(item, config),
        "timeoff_approval_path_uri":rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_approval_path"), "displayText", get_timeoff_approval_path_name(item, config), "uri"),
        "wctaguri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_cost_center"), "displayText", item["work_council_tagging"], "uri", ''),
        "departmenturi": rail.find_first_by_attr_and_get_attr(
            rail.result('get_all_enabled_dept_list'), 'displayText', item['department'], 'uri', ''),
        "timeoff_type_uris": get_germany_time_off_types(item,config),
        "legalentityuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_legal_entities"), "code", item["company_code"], "uri"),
        "monthly_accrual_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_event_scripts"),
            "displayText",
            "Monthly Accrual",
            "uri"
        ),
        "starting_balance_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_event_scripts"),
            "displayText",
            "Starting Balance Set To",
            "uri"
        ),
        "annual_acquisition_validation_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_validation_scripts"),
            "displayText",
            "Require other time off balance to be used first",
            "uri"
        ),
        "annual_accrual_timeoff_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_time_off_types"),
            "displayText",
            "DE - Erholungsurlaub (Jahresurlaub) | Annual leave",
            "uri"
        ),
        "new_entity_flag":int(item["new_entity_flag"]),
        "locationcountryuri": rail.result("get_germany_parent_location_details"),
        "locationuri":rail.find_first_by_attr_and_get_attr(
            list(filter (lambda i: i["parenturi"] == rail.result("get_germany_parent_location_details"),
            rail.result("get_all_location_hierarchy"))),"location",item["location"],"locationuri"),
    }


def map_impersonate_and_create_interactive_session(response):
    response = response.json()['d']
    auth_token = list(filter(
        lambda x: x["name"] == "AUTHTOKEN", response["sessionCookies"]))[0]["value"]
    tenant = list(
        filter(lambda x: x["name"] == "TENANT", response["sessionCookies"]))[0]["value"]
    return {'cookie': f'AUTHTOKEN={auth_token};TENANT={tenant}', 'Path': '/'}


def check_if_germany_user_location_update(dag_run):
    existing_location_uri = rail.result(
        'get_current_location_for_the_user').get("existinglocationuri", "")
    if dag_run.conf["locationuri"] != existing_location_uri:
        return [
        {
            "dateRange": {
            "startDate": get_today_date(),
            "endDate": null,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
            },
            "item": {
            "uri": dag_run.conf["locationuri"],
            "parentUri": null,
            "name": null
            }
        }], "Location Updated;"
    return [], ""


def check_if_germany_user_department_update(dag_run):
    existing_department_uri = rail.result(
        'get_current_location_for_the_user').get("existingdepartmenturi", "")
    if dag_run.conf["departmenturi"] and dag_run.conf["departmenturi"] != existing_department_uri:
        return dag_run.conf["departmenturi"]
    return null


def get_germany_user_create_permissions(dag_run): # TODO remove condition
    permission_sets = []
    if dag_run.conf["primary_manager_flg"] == "Y" or dag_run.conf["project_manager_flg"] == "Y":
        if dag_run.conf["l1_manager_uri"]:
            permission_sets.append(
                {
                    "uri": dag_run.conf["l1_manager_uri"],
                    "name": null
                }
            )
        if dag_run.conf["project_manager_uri"]:
            permission_sets.append(
                {
                    "uri": dag_run.conf["project_manager_uri"],
                    "name": null
                }
            )
        if dag_run.conf["end_user_manager_uri"]:
            permission_sets.append(
                {
                    "uri": dag_run.conf["end_user_manager_uri"],
                    "name": null
                }
            )
    if dag_run.conf["hr_manager_flg"] == "Y" and dag_run.conf["hr_manager_uri"]:
        permission_sets.append(
            {
                "uri": dag_run.conf["hr_manager_uri"],
                "name": null
            }
        )
    if (dag_run.conf["primary_manager_flg"] != "Y" or dag_run.conf["primary_manager_flg"] == "") and \
        (dag_run.conf["project_manager_flg"] != "Y" or dag_run.conf["project_manager_flg"] == ""):
        permission_sets.append(
            {
                "uri": dag_run.conf["default_user_permission_uri"],
                "name": null
            }
        )
    return permission_sets


def get_germany_user_create_policy_data_access(dag_run):
    policy_data_access = []
    if dag_run.conf["hr_manager_flg"] == "Y":
        policy_data_access.append({
            "policyUri": "urn:replicon:policy:payroll-management",
            "location": null,
            "division": {
                "uri": dag_run.conf["legalentityuri"],
                "parentUri": null,
                "name": null
            },
            "serviceCenter": null,
            "costCenter": null,
            "departmentGroup": null,
            "employeeTypeGroup": null
        }
        )
    return policy_data_access


def get_germany_user_create_policy_sets(dag_run, config):
    policy_sets = []
    policy_sets.append(
        {
            "uri": dag_run.conf["timeoff_uri"],
            "name": null
        })
    if dag_run.conf["company_code"] == config.GENERAL_MAPPER['legal_entity_non_wbs']:
        if dag_run.conf["work_council_tagging"] == config.GENERAL_MAPPER['wc_tag_eon']:
            policy_sets.append({
                "uri": dag_run.conf["timesheet_for_non_wbs_eonuri"],
                "name": null
            })
        elif dag_run.conf["work_council_tagging"] == config.GENERAL_MAPPER['wc_tag_rwe']:
            policy_sets.append({
                "uri": dag_run.conf["timesheet_for_non_wbs_rweuri"],
                "name": null
            })
        else:
            policy_sets.append({
                "uri": dag_run.conf["timesheet_for_non_wbsuri"],
                "name": null
            })
    if dag_run.conf["company_code"] == config.GENERAL_MAPPER['legal_entity_wbs']:
        policy_sets.append({
            "uri": dag_run.conf["timesheet_for_wbsuri"],
            "name": null
        })
    if dag_run.conf["ot_request_template_uri"]: # TODO remove check
        policy_sets.append({
            "uri": dag_run.conf["ot_request_template_uri"],
            "name": null
        })
    if dag_run.conf["schedule_policy_uri"]:
        policy_sets.append({
            "uri": dag_run.conf["schedule_policy_uri"],
            "name": null
        })
    return policy_sets

def get_germany_user_create_custom_fields(dag_run):
    custom_field_values = []
    custom_fields = ["acquired_doj", "onsite_end_date", "travel_end_date",
                     "onsite_start_date", "date_of_birth", "travel_start_date", "reversal_date"]
    custom_text_fields = ["hr_adid", "gpo_adid"]

    for i in custom_fields:
        if dag_run.conf[i] and dag_run.conf[i] not in INVALID_DATES:
            custom_field_values.append({
                "customField": {
                    "uri": dag_run.conf[i+"uri"],
                    "name": null,
                    "groupUri": null
                },
                "text": null,
                "date": rail.parse_date(dag_run.conf[i], "%Y-%m-%d"),
                "dropDownOption": null,
                "number": null
            })

    for i in custom_text_fields:
        if dag_run.conf.get(i,""):
            custom_field_values.append({
                "customField": {
                    "uri": dag_run.conf[i+"uri"],
                    "name": null,
                    "groupUri": null
                },
                "text": dag_run.conf[i],
                "date": null,
                "dropDownOption": null,
                "number": null
            })

    return custom_field_values


def get_germany_user_create_location(dag_run):
    location = []
    if dag_run.conf["location"] and dag_run.conf["locationuri"]:
        location.append({
            "location": {
                "uri": dag_run.conf["locationuri"],
                "parentUri": dag_run.conf["locationcountryuri"],
                "name": null
            },
            "effectiveDate": null
        })
    return location


def get_germany_user_payrule_script(dag_run, config): #Todo remove check
    payrules = []
    if dag_run.conf["company_code"] == config.GENERAL_MAPPER['legal_entity_non_wbs']:
        if dag_run.conf["work_council_tagging"] == config.GENERAL_MAPPER['wc_tag_eon'] and dag_run.conf["payrule_for_non_wbs_eonuri"]:
            payrules.append({
            "payRuleScript": {
                "uri": dag_run.conf["payrule_for_non_wbs_eonuri"],
                "name": null
            },
            "effectiveDate": null
        })
        elif dag_run.conf["work_council_tagging"] == config.GENERAL_MAPPER['wc_tag_rwe'] and dag_run.conf["payrule_for_non_wbsuri"]:
            payrules.append({
            "payRuleScript": {
                "uri": dag_run.conf["payrule_for_non_wbsuri"],
                "name": null
            },
            "effectiveDate": null
        })
        elif dag_run.conf["payrule_for_non_wbsuri"]:
            payrules.append({
            "payRuleScript": {
                "uri": dag_run.conf["payrule_for_non_wbsuri"],
                "name": null
            },
            "effectiveDate": null
        })
    if dag_run.conf["company_code"] == config.GENERAL_MAPPER['legal_entity_wbs'] and dag_run.conf["payrule_for_wbsuri"]:
        payrules.append({
            "payRuleScript": {
                "uri": dag_run.conf["payrule_for_wbsuri"],
                "name": null
            },
            "effectiveDate": null
        })

    return payrules


def get_germany_user_create_oefs(dag_run):
    extension_field_values = []
    extension_fields = ["project_supervisor_id", "project_supervisor_mailid",
                        "hr_manager_id", "hr_manager_mailid",
                        "gender", "acquired", "acquired_company", "billability_status",
                        "marital_status", "onsite_direct_recruit",
                        "sales_identifier", "employment_status", "no_of_children",
                        "insurance_type", "gpo_id", "gpo_email_id", "employee_band",
                        "forfait_emp_identifier", "employment_percentage", "personnel_area_text",
                        "personnel_subarea_text", "religion", "project_supervisor_adid"]

    for i in extension_fields:
        if dag_run.conf.get(i,""):
            print(">>>>>>>>>>>>>>>>>>>>>>>")
            print(i)
            extension_field_values.append({
                "definition": {
                    "uri": dag_run.conf[i+"uri"],
                    "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue": dag_run.conf[i],
                "fileValue": null,
                "jsonValue": null
            })
    print(extension_field_values)
    return extension_field_values

def get_updated_logs(dag_run):
    logs = []
    oef_logs = get_extension_field_values_updates(dag_run)[1]
    custom_field_logs = get_germany_user_update_custom_fields(dag_run)[1]
    basic_details = request_payload.get_basic_user_details_update(dag_run)[1]
    location_logs = check_if_germany_user_location_update(dag_run)[1]
    if location_logs:
        logs.append(location_logs + " ")
    if oef_logs:
        logs.append(oef_logs+ " ")
    if custom_field_logs:
        logs.append(custom_field_logs + " ")
    if basic_details:
        logs.append(basic_details + " ")
    update_tasks = {
        "update_the_reversal_date": "Reversal Date updated",
        "assign_hr_manager_permission": "HR manager permission updated",
        "assign_primary_manager_permission": "Primary manager permission updated",
        "assign_project_manager_permission": "Project manager permission updated",
        "update_location":"Location updated",
        "update_end_date":"End date updated"
    }
    success_tasks = list(map(lambda x: x.task_id, filter(lambda x: x.state == "success",
                                                         rail.get_current_context()["dag_run"].get_task_instances())))
    for i in success_tasks:
        if i in update_tasks:
            logs.append(update_tasks[i]+" ")


    return ";".join(logs)

def check_if_oef_update(feed_oef_val, oef_text):
    if feed_oef_val and\
            feed_oef_val != rail.find_first_by_attr_and_get_attr(
                rail.result("get_extension_field_values"),
                "displayText",
                oef_text,
                "textValue"
            ):
        return True
    return False


def check_if_custom_field_date_udapte(feed_custom_val, custom_text):
    if feed_custom_val and feed_custom_val not in INVALID_DATES:
        existing_custom_val = rail.find_first_by_attr_and_get_attr(
            rail.result("get_user_custom_field_values"),
            "displayText",
            custom_text,
            "textValue")
        if existing_custom_val and feed_custom_val != datetime.strftime(
                datetime.strptime(existing_custom_val, "%Y/%m/%d"), "%Y-%m-%d"):
            return True
        if not existing_custom_val:
            return True
    return False


def get_user_annual_leaves_taken(dag_run, response):
    if not response:
        return 0
    time_off_taken = list(filter(lambda i: i["timeOffStatus"]["displayText"] == "Approved" and
                                 i["timeOffType"]["uri"] == dag_run.conf["annual_accrual_timeoff_uri"], response))
    time_off_taken = list(
        map(lambda i: i["totalDuration"]["workdays"], time_off_taken))
    number_of_accrued_to_taken = sum(time_off_taken)
    return number_of_accrued_to_taken


def check_if_location_present(dag_run):
    return rail.find_first_by_attr_and_get_attr(
        list(dag_run.conf["location_details"]),
        "location",
        rail.result("for_each_location_add_hierarchy")["location"],
        "locationuri"
    )

def check_if_end_date(dag_run):

    end_date = None
    if dag_run.conf["ard_lrd"] and dag_run.conf["ard_lrd"] not in INVALID_DATES:
        end_date = dag_run.conf["ard_lrd"]
    if dag_run.conf["onsite_direct_recruit"].lower() == "assignee" and dag_run.conf["onsite_end_date"] not in INVALID_DATES:
        end_date = dag_run.conf["onsite_end_date"]
    if end_date:
        end_date =datetime.strptime(end_date, "%Y-%m-%d")
        start_date = rail.result('get_update_user_details')[
            "userDetails"]["employmentDateRange"]["startDate"]
        if start_date:
            start_date = datetime(**start_date)
        current_end_date = rail.result('get_update_user_details')[
            "userDetails"]["employmentDateRange"]["endDate"]

        if current_end_date:
            current_end_date = datetime(**current_end_date)

        if end_date > start_date and current_end_date != end_date:
            return {
                'year': end_date.year,
                'month': end_date.month,
                'day': end_date.day,
            }
    return False

def get_permission_details(dag_run):
    permission_set = []
    data = rail.result("get_manager_permission")["value"]
    for i in data:
        if i and i not in permission_set:
            if isinstance(i, list):
                permission_set.extend(i)
            else:
                permission_set.append(i)

    if not permission_set:
        permission_set.append(
            dag_run.conf["default_user_permission_uri"])

    return permission_set

def check_supervisor_update(dag_run):
    update_supervisor_uri =   rail.result("create_supervisor_in_replicon") or\
        rail.result("get_supervisor_user_details")[0]["userDetails"]["uri"]
    if dag_run.conf["primary_supervisor_id"] and update_supervisor_uri:
        if not dag_run.conf["supervisor_uri"]:
            return True
        if update_supervisor_uri != dag_run.conf["supervisor_uri"]:
            return True
    return False


def get_extension_field_values_updates(dag_run):
    oef_update_req=[]
    oef_update_logs = ""
    extension_fields = {"project_supervisor_id":"Project Supervisor ID",
                        "project_supervisor_mailid":"Project Supervisor Email",
                        "gender":"Gender", "acquired": "Acquired",
                        "acquired_company":"Acquired Company", "billability_status":"Billability Status",
                        "hiring_status":"Hiring Status", "marital_status":"Marital Status",
                        "onsite_direct_recruit":"Onsite Direct Recruit",
                        "sales_identifier":"Sales Identifier", "employment_status":"Employment Status", "no_of_children":"Children",
                        "employee_band":"Employee Band",
                        "employment_percentage":"FTE", "personnel_area_text":"Personnel area Text",
                        "personnel_subarea_text": "Personnel Subarea Text",  "forfait_emp_identifier":"Forfait jour employment identifier",
                        "religion": "Religion", "project_supervisor_adid":"Project Supervisor Login Name"}

    for i, v in extension_fields.items():
        if check_if_oef_update(
        dag_run.conf[i], v):
            oef_update_req.append(request_payload.get_oef_text_field_update_payload(
            dag_run.conf[i+"uri"],
            dag_run.conf[i]))
            oef_update_logs += v +" updated;"

    return oef_update_req, oef_update_logs

def get_germany_user_update_custom_fields(dag_run):
    custom_field_values = []
    custom_fields = {"acquired_doj":"Acquired DOJ",
                    "onsite_end_date":"Onsite End Date",
                    "onsite_start_date":"Onsite Start Date",
                    "date_of_birth":"Date of Birth",
                    "travel_start_date":"Travel Start Date"
                    }
    custom_field_logs = ""
    for i, v in custom_fields.items():
        if check_if_custom_field_date_udapte(dag_run.conf[i], v):
            custom_field_values.append({
            "value": {
                "customField": {
                    "uri": dag_run.conf[i+"uri"],
                    "name": null,
                    "groupUri": null
                },
                "text": null,
                "date": rail.parse_date(dag_run.conf[i], "%Y-%m-%d"),
                "dropDownOption": null,
                "number": null
            }})
            custom_field_logs += v + "updated;"

    return custom_field_values, custom_field_logs

def get_assignee_policies(dag_run):
    result = rail.result("get_default_time_off_type_policy_schedule_for_user")
    effective_date = datetime.strptime(dag_run.conf["onsite_start_date"], "%Y-%m-%d") if dag_run.conf["onsite_start_date"] and \
        dag_run.conf["onsite_start_date"] not in INVALID_DATES else datetime.utcnow()
    for i in result:
        if i :
            i[0].update({"effectiveDate":rail.get_replicon_date(effective_date)})
    return result


def get_timesheet_approval_path(dag_run):
    timesheet_approval_path_value = null
    timesheet_path_approval_log = ""
    timesheet_approval_path_to_assign_uri = dag_run.conf["timesheet_system_approval_path_uri"] if dag_run.conf['employee_band'] in [
        'GROUP D1', 'GROUP D2', 'GROUP E'] else dag_run.conf["timesheet_approval_pathuri"]
    if timesheet_approval_path_to_assign_uri != rail.result('get_update_user_details').get('timesheetApprovalPath').get('uri'):
        timesheet_approval_path_value = {
            "value": {
                "uri": timesheet_approval_path_to_assign_uri,
                "name": null
            }
        }
        timesheet_path_approval_log = "Timesheet Approval Path Updated"
    return timesheet_approval_path_value, timesheet_path_approval_log
