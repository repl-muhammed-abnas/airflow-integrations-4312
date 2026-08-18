from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from functools import lru_cache
from wipro.user_import_saudi_arabia_v2.utils import request_payload

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
                        "employee_last_name", "country", "location",
                        "employment_status", "company_code", "personnel_area_text",
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

    parent_uri=rail.result("get_saudi_arabia_parent_location_details")
    location_data = list(map(lambda i: {
        "location": i["cells"][0]["textValue"],
        "locationuri": i["cells"][0]["uri"],
        "parenturi": parent_uri
    }, list(filter(lambda i: i["hierarchyLevel"] == 1, response))))

    return location_data or  [{
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


@lru_cache(maxsize=128)
def get_saudi_arabia_object_extension_fields():
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
        "gpo_iduri": rail.find_first_by_attr_and_get_attr(result, "name", "GPO ID", "uri"),
        "gpo_email_iduri": rail.find_first_by_attr_and_get_attr(result, "name", "GPO Email ID", "uri"),
        "hris_iduri": rail.find_first_by_attr_and_get_attr(result, "name", "HRIS ID", "uri"),
        "hrss_email_iduri": rail.find_first_by_attr_and_get_attr(result, "name", "HRSS Email", "uri"),
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


@lru_cache(maxsize=128)
def get_saudi_arabia_custom_fields():
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

def get_saudi_arabia_time_off_types(item,config):
    onsite_recruit = item["onsite_direct_recruit"]
    get_all_timeoff_types = []
    if item["gender"].lower() in ["female", "male"]:
        get_all_timeoff_types.extend(
            config.TIME_OFF_TYPES_MAPPER["local_hire_or_assignee"]["all"])
        get_all_timeoff_types.extend(
            config.TIME_OFF_TYPES_MAPPER["local_hire_or_assignee"][item["gender"].lower()])
        if onsite_recruit.lower() == "local_hire":
            get_all_timeoff_types.extend(
                config.TIME_OFF_TYPES_MAPPER["local_hire"]["all"])
            get_all_timeoff_types.extend(
                config.TIME_OFF_TYPES_MAPPER["local_hire"][item["gender"].lower()])
        elif onsite_recruit.lower() == "assignee":
            get_all_timeoff_types.extend(
                config.TIME_OFF_TYPES_MAPPER["assignee"]["all"])
            get_all_timeoff_types.extend(
                config.TIME_OFF_TYPES_MAPPER["assignee"][item["gender"].lower()])
    get_all_timeoff_types = list(filter(lambda i:i['displayText'] in get_all_timeoff_types, rail.result("get_all_time_off_types")))
    return list(map(lambda i:i["uri"] , get_all_timeoff_types))


@lru_cache
def get_approval_path_uri(country,config):
    return {
        "lookuptable": rail.result("create_log_for_user_import_global"),
        "legalentities": rail.result("get_all_legal_entities"),
        "countryuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_countries"), "displayText", country, "uri"),
        "timezoneuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_time_zones"), "displayText", config.GENERAL_MAPPER["time_zone"], "uri"),
        "holidaycalendaruri":  rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_holiday_calendars"), "displayText", config.GENERAL_MAPPER["default_holiday_calender"], "uri"),
        "payrule_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_payrules"), "name", config.GENERAL_MAPPER["payrule"], "uri"),
        "timesheet_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["timesheet_template"], "uri"),
        "timesheet_approval_pathuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timesheet_approval_path"), 'displayText', config.GENERAL_MAPPER["timesheet_approval_path"], "uri"),
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
        "default_timeoff_annual_leave_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_time_off_types"),
            "displayText",
            config.DEFAULT_SETTINGS_MAPPER[str(country).replace(" ", "_").lower()]["default_time_off_type_for_bookings"],
            "uri"
        ),
        "project_supervisor_permission_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["project_supervisor_permission"], "uri"),
        "supervisor_permission_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["supervisor_permission"], "uri"),
        "resource_supervisor_permission_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["resource_supervisor_permission"], "uri"),
        "schedule_policy_uri": rail.result("get_saudi_arabia_schedule_policy"),
        "ot_request_template_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["ot_request_template"], "uri"),
        "ot_request_approval_path_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_ot_approval_paths"), "displayText", config.GENERAL_MAPPER["ot_request_approval_path"],"uri"),
        "payrate": config.GENERAL_MAPPER["payrate"]
    }


def get_saudi_arabia_supervisor_conf(item,config):
    return {
        **item,

        "lookuptable": rail.result("create_log_for_user_import_global"),
        "l1_manager_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["l1_manager"], "uri"),
        "end_user_manager_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["end_user_manager"], "uri"),
        "foreign_manager_emp_type_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_employee_types"), "displayText", "Foreign Managers", "uri"),
        "project_manager_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["project_manager"], "uri"),
        "country": item["country"]
    }


def get_saudi_arabia_user_conf(item,config):
    item["employee_last_name"] = item["employee_last_name"] or "."
    item["acquired"] = "Y" if item.get("acquired_company") else "N"
    item["hr_manager_id"] = "20284508"
    item["hr_manager_mailid"] = "ramlakhan.annavarpu@wipro.com"
    item["hr_adid"] = "VR20284508@wipro.com"
    if item["project_supervisor_adid"]:
        item["project_supervisor_adid"] = item["project_supervisor_adid"].strip() + "@wipro.com"
    if item["onsite_start_date"] and item["onsite_start_date"] not in INVALID_DATES \
        and item["onsite_end_date"] == INVALID_DATES[0]:
        item["onsite_end_date"] = (datetime.strptime(
            item["onsite_start_date"], "%Y-%m-%d") + relativedelta(years=50)).strftime("%Y-%m-%d")
    return {
        **item,
        **get_saudi_arabia_object_extension_fields(),
        **get_saudi_arabia_custom_fields(),
        "locationcountryuri": rail.result("get_saudi_arabia_parent_location_details"),
        "locationuri":rail.find_first_by_attr_and_get_attr(
        rail.result("get_all_location_hierarchy"),
        "location",
        item["location"],
        "locationuri"
        ),
        **get_approval_path_uri(item["country"],config),
        "timeoff_type_uris": get_saudi_arabia_time_off_types(item,config),
        "legalentityuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_legal_entities"), "code", item["company_code"], "uri"),
        "employee_type_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_employee_types"), "displayText", item["exempt_non_exempt"], "uri"),
        "enabled_timeoff_types": rail.write_json_artifact(rail.result('get_all_time_off_types')),
        "gpo_adid": "G110693@Wipro.com",
        "hris_id": "G110693",
        "hrss_email_id": "middleeast.leaves@wipro.com",
        "gpo_id": "G110693",
        "gpo_email_id": "middleeast.leaves@wipro.com",
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


def check_if_saudi_arabia_user_location_update(dag_run):
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


def get_saudi_arabia_user_create_permissions(dag_run):
    permission_sets = []
    if dag_run.conf["primary_manager_flg"] == "Y":
        permission_sets.append(
            {
                "uri": dag_run.conf["l1_manager_uri"],
                "name": null
            }
        )
    if dag_run.conf["project_manager_flg"] == "Y":
        permission_sets.append(
            {
                "uri": dag_run.conf["project_manager_uri"],
                "name": null
            }
        )
    if dag_run.conf["hr_manager_flg"] == "Y":
        permission_sets.append(
            {
                "uri": dag_run.conf["hr_manager_uri"],
                "name": null
            }
        )
    permission_sets.append(
        {
            "uri": dag_run.conf["default_user_permission_uri"],
            "name": null
        }
    )
    return permission_sets


def get_saudi_arabia_user_create_policy_data_access(dag_run):
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


def get_saudi_arabia_user_create_policy_sets(dag_run):
    policy_sets = []
    policy_sets.append(
        {
            "uri": dag_run.conf["timeoff_uri"],
            "name": null
        })
    policy_sets.append({
        "uri": dag_run.conf["timesheet_uri"],
        "name": null
    })
    if dag_run.conf["schedule_policy_uri"]:
        policy_sets.append(
            {
                "uri": dag_run.conf["schedule_policy_uri"],
                "name": null
            })
    if dag_run.conf["ot_request_template_uri"]:
        policy_sets.append({
            "uri": dag_run.conf["ot_request_template_uri"],
            "name": null
        })
    return policy_sets


def get_saudi_arabia_user_create_custom_fields(dag_run):
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


def get_saudi_arabia_user_create_location(dag_run):
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


def get_saudi_arabia_user_payrule_script(dag_run):
    return [
        {
            "payRuleScript": {
                "uri": dag_run.conf["payrule_uri"],
                "name": null
            },
            "effectiveDate": null
        }
    ]


def get_saudi_arabia_user_create_oefs(dag_run):
    extension_field_values = []
    extension_fields = ["project_supervisor_id", "project_supervisor_mailid",
                        "hr_manager_id", "hr_manager_mailid",
                        "gender", "acquired", "acquired_company", "billability_status",
                        "marital_status", "onsite_direct_recruit",
                        "sales_identifier", "employment_status", "no_of_children",
                        "insurance_type", "gpo_id", "gpo_email_id", "employee_band",
                        "forfait_emp_identifier", "employment_percentage",
                        "personnel_area_text", "personnel_subarea_text", "religion", "project_supervisor_adid",
                        "hris_id","hrss_email_id"]

    for i in extension_fields:
        if dag_run.conf.get(i,""):
            if i in ["project_supervisor_id","project_supervisor_mailid", "project_supervisor_adid"]:
                if dag_run.conf["process_project_supervisor_flag"] == "Y":
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
                continue
            if i == "employee_utilisation":
                extension_field_values.append({
                    "definition": {
                        "uri": dag_run.conf[i+"uri"],
                        "name": null
                    },
                    "tag": null,
                    "numericValue": dag_run.conf[i],
                    "textValue": null,
                    "fileValue": null,
                    "jsonValue": null
                })
                continue
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


def get_updated_logs(dag_run):
    logs = []
    oef_logs = get_extension_field_values_updates(dag_run)[1]
    custom_field_logs = get_saudi_arabia_user_update_custom_fields(dag_run)[1]
    basic_details = request_payload.get_basic_user_details_update(dag_run)[1]
    location_logs = check_if_saudi_arabia_user_location_update(dag_run)[1]
    department_logs = request_payload.get_department_update(dag_run)[1]
    if oef_logs:
        logs.append(oef_logs+ " ")
    if custom_field_logs:
        logs.append(custom_field_logs + " ")
    if basic_details:
        logs.append(basic_details + " ")
    if location_logs:
        logs.append(location_logs + " ")
    if department_logs:
        logs.append(department_logs)
    update_tasks = {
        "update_the_reversal_date": "Reversal Date updated",
        "assign_hr_manager_permission": "HR manager permission updated",
        "assign_primary_manager_permission": "Primary manager permission updated",
        "assign_project_manager_permission": "Project manager permission updated",
        "update_end_date":"End date updated",
        "update_supervisor":"Supervisor updated"
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
    if dag_run.conf["onsite_direct_recruit"].lower() == "assignee" and\
        dag_run.conf["onsite_end_date"] not in INVALID_DATES:
            end_date = dag_run.conf["onsite_end_date"]
    if end_date:
        end_date = datetime.strftime(datetime.strptime(
            end_date, "%Y-%m-%d"), "%Y-%m-%d")
        start_date = rail.result('get_update_user_details')[
            "userDetails"]["employmentDateRange"]["startDate"]
        if start_date:
            start_date = datetime.strftime(
                datetime(**start_date), "%Y-%m-%d")
        current_end_date = rail.result('get_update_user_details')[
            "userDetails"]["employmentDateRange"]["endDate"]

        if current_end_date:
            current_end_date = datetime.strftime(
                datetime(**current_end_date), "%Y-%m-%d")

        if end_date > start_date and current_end_date != end_date:
            return rail.parse_date(end_date, "%Y-%m-%d")
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
    if  dag_run.conf["supervisor_uri"] and (not rail.result('get_effective_supervisor_of_user') or \
        (rail.result('get_effective_supervisor_of_user') and\
        rail.result('get_effective_supervisor_of_user')['supervisor']["uri"] != dag_run.conf["supervisor_uri"])):
        return True
    return False

def get_extension_field_values_updates(dag_run):
    oef_update_req=[]
    oef_update_logs = ""
    extension_fields = {"project_supervisor_id":"Project Supervisor ID",
                        "project_supervisor_mailid":"Project Supervisor Email",
                        "hr_manager_id":"HR Manager ID", "hr_manager_mailid":"HR Manager Email",
                        "gender":"Gender", "acquired": "Acquired",
                        "acquired_company":"Acquired Company", "billability_status":"Billability Status",
                        "marital_status":"Marital Status",
                        "onsite_direct_recruit":"Onsite Direct Recruit",
                        "sales_identifier":"Sales Identifier", "employment_status":"Employment Status", 
                        "no_of_children":"Children",
                        "employee_band":"Employee Band",
                        "employment_percentage":"FTE", "personnel_area_text": "Personnel area Text",
                        "personnel_subarea_text": "Personnel Subarea Text",
                        "religion": "Religion",
                        "project_supervisor_adid":"Project Supervisor Login Name",
                        "gpo_email_id":"GPO Email ID", "gpo_id": "GPO ID",
                        "hris_id": "HRIS ID", "hrss_email_id":  "HRSS Email",
    }
    for i, v in extension_fields.items():
        if i == "employee_utilisation":
            existing_value = rail.find_first_by_attr_and_get_attr(
                rail.result("get_extension_field_values"),"displayText",v,'textValue')
            if dag_run.conf[i] and (not existing_value or float(dag_run.conf[i]) != float(existing_value)) :
                oef_update_req.append(
                    {
                        "value": {
                        "definition": {
                            "uri": dag_run.conf[i+"uri"],
                            "name": null
                        },
                        "tag": null,
                        "numericValue": dag_run.conf[i],
                        "textValue": null,
                        "fileValue": null,
                        "jsonValue": null
                        }
                    }
                )
                oef_update_logs += v +" updated;"
        elif check_if_oef_update(
            dag_run.conf[i], v):
            if i in ["project_supervisor_id","project_supervisor_mailid","project_supervisor_adid"]:
                if dag_run.conf["process_project_supervisor_flag"] == "Y":
                    oef_update_req.append(request_payload.get_oef_text_field_update_payload(
                            dag_run.conf[i+"uri"],
                            dag_run.conf[i]))
                    oef_update_logs += v +" updated;"
                continue

            oef_update_req.append(request_payload.get_oef_text_field_update_payload(
                dag_run.conf[i+"uri"],
                dag_run.conf[i]))
            oef_update_logs += v +" updated;"

    return oef_update_req, oef_update_logs

def get_saudi_arabia_user_update_custom_fields(dag_run):
    custom_field_values = []
    date_custom_fields = {"acquired_doj":"Acquired DOJ",
                    "onsite_end_date":"Onsite End Date",
                    
                    "onsite_start_date":"Onsite Start Date",
                    "date_of_birth":"Date of Birth",
                    "travel_start_date":"Travel Start Date"
                    }

    custom_field_logs = ""
    for i, v in date_custom_fields.items():
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

    return custom_field_values, custom_field_logs

@lru_cache(maxsize=8)
def get_all_enabled_timeoff(enabled_timeoff_types):
    return rail.load_all_records(enabled_timeoff_types)

def get_assignee_policies(dag_run):
    result = rail.result("get_default_time_off_type_policy_schedule_for_user")
    onsite_start_date = datetime.strptime(dag_run.conf["onsite_start_date"], "%Y-%m-%d") \
        if dag_run.conf["onsite_start_date"] not in INVALID_DATES and dag_run.conf["onsite_start_date"]\
            else datetime.now()
    effective_date = rail.get_replicon_date(onsite_start_date)
    for record in result:
        timeoff_name = rail.find_first_by_attr_and_get_attr(get_all_enabled_timeoff(dag_run.conf['enabled_timeoff_types']), 'uri', record['timeOffTypeUri'], 'displayText', '')
        if record['policySetScheduleEntries']:
            record['policySetScheduleEntries'][0].update({"effectiveDate": effective_date})
        if len(record['policySetScheduleEntries']) > 1 and timeoff_name in ["KSA - Annual Leave"]:
            onsite_start_date = rail.get_replicon_date(onsite_start_date + relativedelta(years=5))
            record['policySetScheduleEntries'][1].update({"effectiveDate": onsite_start_date})
    return result


def get_update_supervisor_permission_payload(dag_run):
    permission_set_uris = [
                            dag_run.conf["project_supervisor_permission_uri"],
                            dag_run.conf["supervisor_permission_uri"],
                            dag_run.conf["end_user_manager_uri"]]
    return {
        "user": {
            "uri": rail.result("get_project_supervisor_details")["userDetails"]["uri"]
        },
        "modifications": {
            "permissionSetsToApply": {
                "permissionSetUrisToAssign": permission_set_uris,
                "policyUrisToRemovePermissionSet": []
            },
            "workAuthorizationApprovalPathToApply": {
                "uri": dag_run.conf["ot_request_approval_path_uri"]
            } if dag_run.conf["ot_request_approval_path_uri"] else null,
            "policySetsToApply": {
            "policySetUrisToAssign": [dag_run.conf["ot_request_template_uri"]],
            "policyUrisToRemovePolicySet": [],
            "policySetUrisToRemove": []
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }
