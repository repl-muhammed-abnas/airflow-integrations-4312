from datetime import datetime
import functools
from wipro.user_import_switzerland.utils import request_payload

import rail
INVALID_DATES = ["9999-12-31", "0000-00-00"]
null = None
update_logs = []


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
                        "employment_status", "company_code",
                        "adid", "date_of_joining"]
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

    parent_uri = rail.result("get_switzerland_parent_location_details")
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
def get_switzerland_object_extension_fields():
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
def get_switzerland_custom_fields():
    result = rail.result("get_all_custom_fields")
    return {
        "acquired_dojuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Acquired DOJ", "uri"),
        "travel_start_dateuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Travel Start Date", "uri"),
        
        "onsite_start_dateuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Onsite Start Date", "uri"),
        "onsite_end_dateuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Onsite End Date", "uri"),
        
        "reversal_dateuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Reversal Date", "uri"),
        "date_of_birthuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Date of Birth", "uri"),

        "hr_adiduri": rail.find_first_by_attr_and_get_attr(result, "displayText", "ID", "uri"),
        "gpo_adiduri": rail.find_first_by_attr_and_get_attr(result, "displayText", "HRSS ID", "uri"),
    }

def get_department_update(dag_run):
    if dag_run.conf["department"] and dag_run.conf["department"] != dag_run.conf["existingdepartment"] and dag_run.conf["department_flag"]:
        _date ={
          "startDate": get_today_date(),
          "endDate": null,
          "relativeDateRangeUri": null,
          "relativeDateRangeAsOfDate": null
        }  if dag_run.conf["existingdepartment"] else null
        update_logs.append("Department updated;")
        return [
                {
                    "dateRange": _date,
                    "item": {
                    "uri": null,
                    "parent": {
                        "uri": null,
                        "parent": null,
                        "name": "Wipro",
                        "parameterCorrelationId": null
                    },
                    "name": dag_run.conf["department"],
                    "parameterCorrelationId": null
                    }
                }
            ]
    return []

def get_switzerland_time_off_types(item, TIME_OFF_TYPES_MAPPER):
    onsite_recruit = item["onsite_direct_recruit"].lower()
    gender = item["gender"].lower()
    band = item["employee_band"].lower()
    fte = float(item["employment_percentage"])
    
    if gender not in ["female", "male"] or not TIME_OFF_TYPES_MAPPER:
        return {"enabled": [], "disabled": []}
    
    def matches_criteria(time_off):
        # Gender check
        if time_off["gender"] not in ["all", gender]:
            return False
        
        # Onsite check
        if time_off["onsite"] not in ["all", onsite_recruit]:
            return False
        
        # FTE check
        fte_type = time_off["fte"]
        if fte_type != "all":
            if fte_type == "100" and fte != 100:
                return False
            if fte_type == "<100" and fte >= 100:
                return False
        
        # Band check
        band_type = time_off["band"]
        if band_type != "all":
            if band_type == "d1_d2" and band not in ["d1", "d2"]:
                return False
            if band_type == "except_d1_d2" and band in ["d1", "d2"]:
                return False
        
        return True
    
    # Filter and categorize time off types
    enabled_names = []
    disabled_names = []
    
    for time_off in TIME_OFF_TYPES_MAPPER:
        if matches_criteria(time_off):
            target_list = enabled_names if time_off["status"] == "enabled" else disabled_names
            target_list.append(time_off["name"])
    
    # Remove duplicates while preserving order
    enabled_names = list(dict.fromkeys(enabled_names))
    disabled_names = list(dict.fromkeys(disabled_names))
    
    # Convert names to URIs
    all_time_off_types = rail.result("get_all_time_off_types")
    
    def get_uris(names):
        return [
            item["uri"] for item in all_time_off_types 
            if item["displayText"] in names
        ]
    
    return {
        "enabled": get_uris(enabled_names),
        "disabled": get_uris(disabled_names)
    }

def get_uri_based_on_onsite_direct_recruit(onsite_direct_recruit, data, data_attr, mapper_attr):
    onsite_recruit = onsite_direct_recruit.lower()
    if onsite_recruit == "assignee":
        return rail.find_first_by_attr_and_get_attr(data, data_attr, mapper_attr["assignee"], "uri")
    elif onsite_recruit == "local_hire":
        return rail.find_first_by_attr_and_get_attr(data, data_attr, mapper_attr["local_hire"], "uri")
    else:
        return rail.find_first_by_attr_and_get_attr(data, data_attr, mapper_attr["default"], "uri")

@functools.lru_cache(maxsize=128)
def get_approval_path_uri(country, onsite_direct_recruit, config):
    return {
        "lookuptable": rail.result("create_log_for_user_import_global"),
        "legalentities": rail.result("get_all_legal_entities"),
        "countryuri": rail.result("get_switzerland_country_uri"),
        "timezoneuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_time_zones"), "displayText", config.GENERAL_MAPPER["time_zone"], "uri"),
        "timesheet_approval_pathuri": get_uri_based_on_onsite_direct_recruit(onsite_direct_recruit, rail.result("get_all_timesheet_approval_path"), "displayText",
            config.GENERAL_MAPPER["timesheet_approval_path"]),
        "timesheet_system_approval_path_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timesheet_approval_path"), 'displayText', config.GENERAL_MAPPER["timesheet_system_approval_path"], "uri"),
        "timesheet_perioduri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timesheet_periods"), "name", config.GENERAL_MAPPER["timesheet_period"], "uri"),
        "timeoff_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", "Time Off", "uri"),
        "timeoff_approval_path_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_approval_path"), "displayText", config.GENERAL_MAPPER["timeoff_approval_path"], "uri"),
        "timeoff_system_approval_path_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_approval_path"), "displayText", config.GENERAL_MAPPER["timesheet_system_approval_path"], "uri"),
        "l1_manager_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["l1_manager"], "uri"),
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
        "timesheet_templateuri": get_uri_based_on_onsite_direct_recruit(onsite_direct_recruit, rail.result("get_all_policy_sets"), "displayText",
            config.GENERAL_MAPPER["timesheet_template"]),
        "punch_policyuri" :rail.find_first_by_attr_and_get_attr(
            rail.result("get_switzerland_punch_policy"),
            "name",
            config.GENERAL_MAPPER["punch_policy"],
            "uri"
        ),
        "payrule_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_payrules"),
            "name",
            config.GENERAL_MAPPER["payrule"],
            "uri"
        ),
        "shift":config.GENERAL_MAPPER["shift"][onsite_direct_recruit.lower()] if onsite_direct_recruit else config.GENERAL_MAPPER["shift"]["default"],
        "ot_request_template_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["ot_request_template"], "uri"),
        "ot_request_approval_path_uri":rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_overtime_approval_paths"), "displayText", config.GENERAL_MAPPER["ot_request_approval_path"], "uri")
    }


def get_switzerland_supervisor_conf(item, config):
    return {
        **item,

        "lookuptable": rail.result("create_log_for_user_import_global"),
        "l1_manager_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["l1_manager"], "uri"),
        "project_manager_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["project_manager"], "uri"),
        "foreign_manager_emp_type_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_employee_types"), "displayText", "Foreign Managers", "uri"),
        "country": item["country"]
    }

def get_switzerland_user_conf(item, config):
    item["employee_last_name"] = item["employee_last_name"] or "."
    item["hr_manager_id"] = "290901"
    item["hr_manager_mailid"] = "JUKAREDDY.AKHILESH@WIPRO.COM"
    item["hr_adid"] = "AY290901@wipro.com"
    item["acquired"] = "Y" if item.get("acquired_company") else "N"
    if item["project_supervisor_adid"]:
        item["project_supervisor_adid"] = item["project_supervisor_adid"].strip() + "@wipro.com"
    return {
        **item,
        "gpo_adid": "G112561@wipro.com",
        "gpo_email_id": "switzerland.hrss@wipro.com",
        "gpo_id": "G112561",
        **get_switzerland_object_extension_fields(),
        **get_switzerland_custom_fields(),
        "locationcountryuri": rail.result("get_switzerland_parent_location_details"),
        "locationuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_location_hierarchy"),
            "location",
            item["location"],
            "locationuri"
        ),
        **get_approval_path_uri(item["country"], item["onsite_direct_recruit"], config),
        "timeoff_type_uris": get_switzerland_time_off_types(item, config.TIME_OFF_TYPES_MAPPER),
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
        "employee_type_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_employee_types"), "displayText", item["exempt_non_exempt"], "uri"),
        "schedule_policyuri": rail.result("get_switzerland_schedule_policy"),
        "holidaycalendaruri":rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_holiday_calendars"), "displayText", config.HOLIDAY_CALENDAR_MAPPER[item["location"]], "uri")\
                  if item["location"] in config.HOLIDAY_CALENDAR_MAPPER else null,
        "default_timeoff_annual_leave_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_time_off_types"),
            "displayText",
            config.DEFAULT_SETTINGS_MAPPER[str(item["country"]).lower(
            )]["default_time_off_type_for_bookings_assignee"],
            "uri"
        ) if item["onsite_direct_recruit"].lower() == "assignee" else rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_time_off_types"),
            "displayText",
            config.DEFAULT_SETTINGS_MAPPER[str(item["country"]).lower(
            )]["default_time_off_type_for_bookings_local_hire"],
            "uri"
        ),
        "department_flag": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_departments"), "name", item["department"], "uri"),
    }


def map_impersonate_and_create_interactive_session(response):
    response = response.json()['d']
    auth_token = list(filter(
        lambda x: x["name"] == "AUTHTOKEN", response["sessionCookies"]))[0]["value"]
    tenant = list(
        filter(lambda x: x["name"] == "TENANT", response["sessionCookies"]))[0]["value"]
    return {'cookie': f'AUTHTOKEN={auth_token};TENANT={tenant}', 'Path': '/'}


def check_if_switzerland_user_location_update(dag_run, holiday_calendar = False):
    existing_location_uri = rail.result(
        'get_current_location_for_the_user').get("existinglocationuri", "")
    
    if dag_run.conf["locationuri"] != existing_location_uri:
        if holiday_calendar == True and dag_run.conf["holidaycalendaruri"]:
            update_logs.append(" Holiday Calendar Updated;")
            return {
                "value": {
                    "uri": dag_run.conf["holidaycalendaruri"],
                    "name": null
                }
                } 
        update_logs.append("Location Updated;")
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
            }]
    return [], ""


def get_switzerland_user_create_permissions(dag_run):
    conf = dag_run.conf
    permission_sets = []
    
    def add_permission(uri_key):
        permission_sets.append({"uri": conf[uri_key], "name": None})
    
    # HR Manager permissions
    if conf["hr_manager_flg"] == "Y":
        add_permission("hr_manager_uri")
    
    # Project Manager permissions (includes L1 manager and end user manager)
    if conf["project_manager_flg"] == "Y":
        add_permission("l1_manager_uri")
        add_permission("project_manager_uri")
        add_permission("end_user_manager_uri")
    
    # Primary Manager permissions (only L1 manager, and only if not project manager)
    elif conf["primary_manager_flg"] == "Y":
        add_permission("l1_manager_uri")
    
    # Default user permissions (only if not project manager)
    if conf.get("project_manager_flg") != "Y":
        add_permission("default_user_permission_uri")
    
    return permission_sets


def get_switzerland_user_create_policy_data_access(dag_run):
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


def get_switzerland_user_create_policy_sets(dag_run):
    policy_sets = []
    policy_sets.append(
        {
            "uri": dag_run.conf["timeoff_uri"],
            "name": null
        })
    if dag_run.conf["timesheet_templateuri"]:
        policy_sets.append({
            "uri": dag_run.conf["timesheet_templateuri"],
            "name": null
        })

    if dag_run.conf["schedule_policyuri"]:
        policy_sets.append(
            {
                "uri": dag_run.conf["schedule_policyuri"],
                "name": null
            })
    if dag_run.conf["punch_policyuri"]:
        policy_sets.append(
            {
                "uri": dag_run.conf["punch_policyuri"],
                "name": null
            })
    if dag_run.conf["ot_request_template_uri"]:
        policy_sets.append(
            {
                "uri": dag_run.conf["ot_request_template_uri"],
                "name": null
            })

    return policy_sets


def get_switzerland_user_create_custom_fields(dag_run):
    custom_field_values = []
    custom_fields = ["acquired_doj", "onsite_end_date", "travel_end_date",
                     "onsite_start_date", "date_of_birth", "travel_start_date", "reversal_date"]
    custom_fields_text = ["gpo_adid", "hr_adid"]
    for i in custom_fields_text:
        if dag_run.conf[i]:
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

    return custom_field_values


def get_switzerland_user_create_location(dag_run):
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


def get_switzerland_user_create_oefs(dag_run):
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
        if dag_run.conf.get(i, ""):
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
    return extension_field_values

def get_basic_user_details_update(dag_run):
    first_name = null
    last_name = null
    basic_details_logs = ""
    display_name = null
    email_id = null
    if dag_run.conf["employee_first_name"] != rail.result("get_update_user_details")["userDetails"]["firstName"]:
        first_name = dag_run.conf["employee_first_name"]
        basic_details_logs = "First name updated"
    if dag_run.conf["employee_last_name"] != rail.result("get_update_user_details")["userDetails"]["lastName"]:
        last_name = dag_run.conf["employee_last_name"]
        basic_details_logs += "Last name updated"
    if first_name or last_name:
        display_name = dag_run.conf["employee_first_name"] + " " + \
            dag_run.conf["employee_last_name"] + \
            " " + dag_run.conf["employee_id"]
        basic_details_logs += "Display Name updated"
    if dag_run.conf["employee_email_id"] != rail.result("get_update_user_details")["userDetails"]["emailAddress"]:
        email_id = dag_run.conf["employee_email_id"]
        basic_details_logs += "Email updated"
    basic_details = {
        "firstName": {
            "value": first_name
        } if first_name else null,
        "lastName": {
            "value": last_name
        }if last_name else null,
        "loginName": null,
        "displayName": {
            "value": display_name
        }if display_name else null,
        "emailAddress": {
            "value": email_id
        }if email_id else null
    }
    update_logs.append(basic_details_logs)
    return basic_details

def get_updated_logs():
    logs = update_logs
    update_tasks = {
        "update_the_reversal_date": "Reversal Date updated",
        "assign_hr_manager_permission": "HR manager permission updated",
        "assign_primary_manager_permission": "Primary manager permission updated",
        "assign_project_manager_permission": "Project manager permission updated",
        "update_end_date": "End date updated",
        "update_supervisor": "Supervisor updated"
    }
    success_tasks = list(map(lambda x: x.task_id, filter(lambda x: x.state == "success",
                                                         rail.get_current_context()["dag_run"].get_task_instances())))
    for i in success_tasks:
        if i in update_tasks:
            logs.append(update_tasks[i]+";")

    return "".join(logs)


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
    update_supervisor_uri = rail.result("create_supervisor_in_replicon") or \
        rail.result("get_supervisor_user_details")[0]["userDetails"]["uri"]
    if dag_run.conf["primary_supervisor_id"] and update_supervisor_uri:
        if update_supervisor_uri != dag_run.conf["supervisor_uri"]:
            return True
    return False


def get_extension_field_values_updates(dag_run):
    oef_update_req = []
    oef_update_logs = ""
    extension_fields = {"project_supervisor_id": "Project Supervisor ID",
                        "project_supervisor_mailid": "Project Supervisor Email",
                        "gender": "Gender", "acquired": "Acquired",
                        "acquired_company": "Acquired Company", "billability_status": "Billability Status",
                        "marital_status": "Marital Status",
                        "onsite_direct_recruit": "Onsite Direct Recruit",
                        "sales_identifier": "Sales Identifier", "employment_status": "Employment Status", "no_of_children": "Children",
                        "gpo_id": "HRIS ID", "gpo_email_id": "HRSS Email", "employee_band": "Employee Band",
                        "employment_percentage": "FTE", "personnel_area_text": "Personnel area Text",
                        "personnel_subarea_text": "Personnel Subarea Text",
                        "forfait_emp_identifier": "Forfait jour employment identifier",
                        "religion": "Religion",
                        "project_supervisor_adid":"Project Supervisor Login Name"
                        }
    for i, v in extension_fields.items():
        if check_if_oef_update(
                dag_run.conf[i], v):
            if dag_run.conf[i+"uri"]:
                oef_update_req.append(request_payload.get_oef_text_field_update_payload(
                    dag_run.conf[i+"uri"],
                    dag_run.conf[i]))
                oef_update_logs += v + " updated;"

    update_logs.append(oef_update_logs)
    return oef_update_req


def get_switzerland_user_update_custom_fields(dag_run):
    custom_field_values = []
    custom_fields = {"acquired_doj": "Acquired DOJ",
                     "onsite_end_date": "Onsite End Date",
                     
                     "onsite_start_date": "Onsite Start Date",
                     "date_of_birth": "Date of Birth",
                     "travel_start_date": "Travel Start Date",
                     
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
            custom_field_logs += v + " updated;"
    update_logs.append(custom_field_logs)
    return custom_field_values


def get_assignee_policies(dag_run):
    result = rail.result("get_default_time_off_type_policy_schedule_for_user")
    effective_date = datetime.strptime(dag_run.conf["onsite_start_date"], "%Y-%m-%d") \
        if dag_run.conf["onsite_start_date"] and dag_run.conf["onsite_start_date"] not in INVALID_DATES\
        else datetime.now()
    effective_date = rail.get_replicon_date(effective_date)
    for i in result:
        if i:
            i[0].update({"effectiveDate": effective_date})
    return result
