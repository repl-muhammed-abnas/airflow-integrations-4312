from datetime import datetime, timedelta,date
from pendulum import now
import functools
import json
from wipro.user_import_spain_v3.utils import request_payload

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

def get_report_params():
    return {
                "reportParameters": [
                {
                "reportUri": rail.result("get_user_report_details")["uri"],
                "filterValues": [
                    {
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                        rail.result("get_user_report_details")["filterConfiguration"]["enabledFilters"],
                        "displayText","CurrentServiceCenterFilter","uri"),
                    "value": rail.result("get_country_uri").split(":")[-1]
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }
            ]
            }


def get_today_date():
    now_date = datetime.utcnow()
    return {
        'year': now_date.year,
        'month': now_date.month,
        'day': now_date.day
    }

def get_integration_run_date(config):
    today = now(config.time_zone)
    return {
        'day': int(today.day),
        'date': today.strftime(config.DATE_FORMAT),
        'datetime': today.isoformat()
    }

def normalize_date_format(date_value):
        """Convert YYYY/MM/DD to YYYY-MM-DD format"""
        if date_value and "/" in str(date_value):
            return date_value.replace("/", "-")
        return date_value


def get_all_users_with_enddate_data():
    users_with_enddate = rail.load_all_records(rail.result("query_all_users_with_enddate"))
    return list(
        map(lambda cell: {
            "useruri": cell["user_uri"],
            "enddate": normalize_date_format(cell["user_end_date"]),
            "onsite_end_date": normalize_date_format(cell["onsite_end_date"]),
            "employee_id": cell["employee_id"],
            "user_start_date": normalize_date_format(cell["user_start_date"]),
            "onsite_start_date": normalize_date_format(cell["onsite_start_date"]),
            "login_name": cell["login_name"],
            "onsite_direct_recruit": cell["onsite_direct_recruit"],
            "country": cell["country"],
            "first_name":cell["user_first_name"],
            "last_name":cell["user_last_name"],
            "company_code": cell["company_code"],
            "acquired_company": cell["acquired_company"],
        }, users_with_enddate))

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
    parent_uri=rail.result("get_spain_parent_location_details")
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


@functools.lru_cache(maxsize=128)
def get_spain_object_extension_fields():
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
def get_spain_custom_fields():
    result = rail.result("get_all_custom_fields")
    return {
        "acquired_dojuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Acquired DOJ", "uri"),
        "travel_start_dateuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Travel Start Date", "uri"),
        
        "onsite_start_dateuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Onsite Start Date", "uri"),
        "onsite_end_dateuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Onsite End Date", "uri"),
        
        "reversal_dateuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Reversal Date", "uri"),
        "date_of_birthuri": rail.find_first_by_attr_and_get_attr(result, "displayText", "Date of Birth", "uri"),

        "gpo_adiduri": rail.find_first_by_attr_and_get_attr(result, "displayText", "HRSS ID", "uri"),
        "hr_adiduri": rail.find_first_by_attr_and_get_attr(result, "displayText", "ID", "uri"),
    }

def get_spain_time_off_types(item,config):
    onsite_recruit = item["onsite_direct_recruit"]
    legal_entity_code = item['company_code']
    acquired_company = item['acquired_company']
    get_all_timeoff_types = []
    if item["gender"].lower() in ["female", "male"]:
        if legal_entity_code in ('W001', 'W096'):
            get_all_timeoff_types.extend(
                config.TIME_OFF_TYPES_MAPPER["local_hire_or_assignee"]["all"])
            get_all_timeoff_types.extend(
                config.TIME_OFF_TYPES_MAPPER["local_hire_or_assignee"][item["gender"].lower()])
            if acquired_company.lower() == "inetum" and legal_entity_code == "W001":
                get_all_timeoff_types.extend(
                config.TIME_OFF_TYPES_MAPPER["local_hire_or_assignee"]["inetum"])
            if legal_entity_code == "W096":
                get_all_timeoff_types.extend(
                config.TIME_OFF_TYPES_MAPPER["local_hire_or_assignee"]["designit"])
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
    get_all_timeoff_types = list(filter(
        lambda i: i['displayText'] in get_all_timeoff_types, rail.result("get_all_time_off_types")))
    return list(map(lambda i: i["uri"], get_all_timeoff_types))

def get_spain_disable_timeoff_uri(config):
    get_all_timeoff_types = []
    get_all_timeoff_types.extend(
                    config.TIME_OFF_TYPES_MAPPER["local_hire_or_assignee"]["disable_timeoff"])
    get_all_timeoff_types = list(filter(
        lambda i: i['displayText'] in get_all_timeoff_types, rail.result("get_all_time_off_types")))
    
    return list(map(lambda i: i["uri"], get_all_timeoff_types))

def get_spain_disable_timeoff_uri(config):
    get_all_timeoff_types = []
    get_all_timeoff_types.extend(
                    config.TIME_OFF_TYPES_MAPPER["local_hire_or_assignee"]["disable_timeoff"])
    get_all_timeoff_types = list(filter(
        lambda i: i['displayText'] in get_all_timeoff_types, rail.result("get_all_time_off_types")))

    return list(map(lambda i: i["uri"], get_all_timeoff_types))

def get_annualy_accural(item):
    default_yearly_entitlement = 0.0
    for value in item[0]['policySet']['timeOffBalanceEventScripts']:
        for key in value['additionalParameters']:
            if key['keyUri'] == "urn:replicon:script-key:parameter:accrual-annual-amount":
                default_yearly_entitlement = key['value']['number']
                break
    return default_yearly_entitlement

def get_spain_starting_balance(item,dag_run):
    start_date = None
    default_timeoff_policy  = rail.result('get_default_time_off_type_policy_schedule_for_user')
    if dag_run.conf["onsite_direct_recruit"] == "ASSIGNEE":
            default_timeoff_policy = rail.result("replace_effective_date")
    if item == dag_run.conf["annual_leave_uri"]:
        if dag_run.conf["onsite_direct_recruit"] == "ASSIGNEE":
            start_date = dag_run.conf["onsite_start_date"]
        else:
            start_date = dag_run.conf["date_of_joining"]
        
        start_dt= datetime.strptime(start_date, '%Y-%m-%d').date()
        is_jan_1st = (start_dt.month == 1 and start_dt.day == 1)
        
        if (dag_run.conf["company_code"] == "W001") and (dag_run.conf["acquired_company"].lower()  != "inetum"):
            annual_accural = 23
        else:
            annual_accural = 24

        daily_accural = annual_accural / 365

        no_of_days = abs((datetime.strptime(start_date, '%Y-%m-%d').date() - datetime(datetime.now().year, 12, 31).date()).days)

        if is_jan_1st:
            start_balance = 0
        else:
            start_balance = (round((no_of_days * daily_accural),0) + 0.5) if int(str(round((no_of_days * daily_accural),1)).split(".")[1]) < 5 else (round((no_of_days * daily_accural),0))

        originalbalance = '"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": 0.0}}'
        newbalance = '"keyUri": "urn:replicon:script-key:parameter:amount","value":{"number":' + str(start_balance) + '}}'

        default_yearly_entitlement = get_annualy_accural(default_timeoff_policy[list(dag_run.conf["timeoff_type_uris"]).index(item)])

        original_yearly_balance = '"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": '+ str(default_yearly_entitlement) +'}}'
        new_yearly_balance = '"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount","value":{"number":' + str(annual_accural) + '}}'


        return json.loads(json.dumps(default_timeoff_policy[list(dag_run.conf["timeoff_type_uris"]).index(item)])
                                   .replace('"script"', '"scriptTarget"')
                                   .replace('"description": null', '"description": "starting balance"')
                                   .replace(originalbalance, newbalance)
                                   .replace(original_yearly_balance, new_yearly_balance))
    else:

        return json.loads(json.dumps(default_timeoff_policy[list(dag_run.conf["timeoff_type_uris"]).index(item)])
                                                       .replace('"script"', '"scriptTarget"')
                                                       .replace('"description": null', '"description": "effective"'))

@functools.lru_cache
def get_approval_path_uri(country, config, new_entity_flag, personnel_area_text,employee_band):
    holiday_calendar = country
    if new_entity_flag:
        holiday_calendar = config.NEW_ENTITY_MAPPER["holiday_calendar"]
    return {
        "lookuptable": rail.result("create_log_for_user_import_global"),
        "legalentities": rail.result("get_all_legal_entities"),
        "countryuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_countries"), "displayText", country, "uri"),
        "timezoneuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_time_zones"), "displayText", config.GENERAL_MAPPER["time_zone"], "uri"),
        "default_holidaycalendaruri":  rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_holiday_calendars"), "displayText", holiday_calendar , "uri"),
        "holidaycalendaruri":  rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_holiday_calendars"), "displayText", personnel_area_text , "uri"),
        "schedule_typeuri": config.GENERAL_MAPPER["schedule_type"],
        "schedule_for_designituri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_office_schedules"), "displayText", "Spain DesignIT Default Schedule", "uri"),
        "schedule_for_rebadgeduri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_office_schedules"), "displayText", "Spain Inetum Default Schedule", "uri"),
        "ot_request_template_spainuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["ot_request_template_spain"], "uri"),
        "ot_request_template_rebadgeduri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["ot_request_template_rebadged"], "uri"),
        "ot_request_template_designituri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["ot_request_template_designit"], "uri"),
        "ot_request_approval_path_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_overtime_approval_paths"), "displayText", config.GENERAL_MAPPER["ot_request_approval_path"], "uri"),
        "payrule_for_fulltime_spainuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_payrules"), "name", config.GENERAL_MAPPER["payrule_for_fulltime_spain"], "uri"),
        "payrule_for_fulltime_rebadgeduri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_payrules"), "name", config.GENERAL_MAPPER["payrule_for_fulltime_rebadged"], "uri"),
        "payrule_for_fulltime_designituri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_payrules"), "name", config.GENERAL_MAPPER["payrule_for_fulltime_designit"], "uri"),
        "payrule_for_parttime_spainuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_payrules"), "name", config.GENERAL_MAPPER["payrule_for_parttime_spain"], "uri"),
        "payrule_for_parttime_rebadgeduri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_payrules"), "name", config.GENERAL_MAPPER["payrule_for_parttime_rebadged"], "uri"),
        "payrule_for_parttime_designituri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_payrules"), "name", config.GENERAL_MAPPER["payrule_for_parttime_designit"], "uri"),
        "timesheet_for_fulltime_spainuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["timesheet_for_fulltime_spain"], "uri"),
        "timesheet_for_parttime_spainuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["timesheet_for_parttime_spain"], "uri"),
        "timesheet_for_fulltime_rebadgeduri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["timesheet_for_fulltime_rebadged"], "uri"),
        "timesheet_for_parttime_rebadgeduri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["timesheet_for_parttime_rebadged"], "uri"),
        "timesheet_for_fulltime_designituri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["timesheet_for_fulltime_designit"], "uri"),
        "timesheet_for_parttime_designituri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", config.GENERAL_MAPPER["timesheet_for_parttime_designit"], "uri"),
        "timesheet_approval_pathuri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timesheet_approval_path"), 'displayText', config.GENERAL_MAPPER["timesheet_approval_path"], "uri"),
        "timesheet_system_approval_path_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timesheet_approval_path"), 'displayText', config.GENERAL_MAPPER["timesheet_system_approval_path"], "uri"),
        "timesheet_supervisor_approval_path_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timesheet_approval_path"), 'displayText', config.GENERAL_MAPPER["timesheet_supervisor_approval_path"], "uri"),
        "timesheet_perioduri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timesheet_periods"), "name", config.GENERAL_MAPPER["timesheet_period"], "uri"),
        "timeoff_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_policy_sets"), "displayText", "Time Off", "uri"),
        "timeoff_approval_path_uri":  rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_approval_path"), "displayText", "Spain D1 and above Time Off Approval Path", "uri") if (employee_band == "GROUP D1" or employee_band == "GROUP D2" or employee_band == "GROUP E") else rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_approval_path"), "displayText", "Spain Time off Approval path", "uri"),
        "timeoff_system_approval_path_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_approval_path"), "displayText", config.GENERAL_MAPPER["timeoff_system_approval_path"], "uri"),
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
            config.DEFAULT_SETTINGS_MAPPER[str(country).lower(
            )]["default_time_off_type_for_bookings"],
            "uri"
        )
    }

def get_spain_supervisor_conf(item,config):
    return {
        **item,

        "lookuptable": rail.result("create_log_for_user_import_global"),
        "l1_manager_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["l1_manager"], "uri"),
        "end_user_manager_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["end_user_manager"], "uri"),
        "project_manager_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_permission_sets"), "displayText", config.GENERAL_MAPPER["project_manager"], "uri"),
        "foreign_manager_emp_type_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_employee_types"), "displayText", "Foreign Managers", "uri"),
        "country": item["country"]
    }


def get_spain_user_conf(item,config):
    item["employee_last_name"] = item["employee_last_name"] or "."
    item["hr_manager_id"] = "20132497"
    item["hr_manager_mailid"] = "VANIA.SILVA@WIPRO.COM"
    item["hr_adid"] = "VN20132497@wipro.com"
    item["acquired"] = "Y" if item.get("acquired_company") else "N"
    if item["project_supervisor_adid"]:
        item["project_supervisor_adid"] = item["project_supervisor_adid"].strip() + "@wipro.com"
    return {
        **item,
        "gpo_adid": "G113481@wipro.com",
        "gpo_email_id": "spain.hrss@wipro.com",
        "gpo_id": "G113481",
        "new_entity_flag": int(item["new_entity_flag"]),
        **get_spain_object_extension_fields(),
        **get_spain_custom_fields(),
       "locationcountryuri": rail.result("get_spain_parent_location_details"),
        "locationuri":rail.find_first_by_attr_and_get_attr(
        rail.result("get_all_location_hierarchy"),
        "location",
        item["location"],
        "locationuri"
        ),
        **get_approval_path_uri(item["country"],config,item["new_entity_flag"], item['personnel_area_text'],item['employee_band']),
        "schedule_policyuri": rail.result("get_spain_schedule_policy"),
        "timeoff_type_uris": get_spain_time_off_types(item,config),
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
        "custom_accural_annual_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_event_scripts"),
            "displayText",
            "ESP - Custom annual accrual rule",
            "uri"
        ),
        "custom_prevent_balance_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_validation_scripts"),
            "displayText",
            "ESP - Prevent balance overdraw",
            "uri"
        ),
        "custom_required_balance_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_validation_scripts"),
            "displayText",
            "ESP - Require other time off balance to be used",
            "uri"
        ),
        "custom_probation_balance_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_validation_scripts"),
            "displayText",
            "ESP - Annual Leave Probation Validation",
            "uri"
        ),
        "employee_type_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_employee_types"), "displayText", item["exempt_non_exempt"], "uri"),
        "disable_timeoff_uri": [
                        x for x in get_spain_time_off_types(item,config) 
                        if x not in get_spain_disable_timeoff_uri(config)
                    ],
        "annual_leave_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_time_off_types"), "displayText", "ESP - Vacaciones anuales (Annual leave)", "uri"),
        "parent_ecid": rail.render_template('{{dag_run_ecid()}}'),
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


def check_if_spain_user_location_update(dag_run):
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



def get_spain_user_create_permissions(dag_run):
    permission_sets = []
    if dag_run.conf["primary_manager_flg"] == "Y" or dag_run.conf["hr_manager_flg"] == "Y":
        if dag_run.conf["l1_manager_uri"]:
            permission_sets.append(
                {
                    "uri": dag_run.conf["l1_manager_uri"],
                    "name": null
                }
            )
        permission_sets.append(
            {
                "uri": dag_run.conf["end_user_manager_uri"],
                "name": null
            }
        )
    if dag_run.conf["project_manager_flg"] == "Y" and dag_run.conf["project_manager_uri"]:
        permission_sets.append(
            {
                "uri": dag_run.conf["project_manager_uri"],
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
    if (not dag_run.conf["primary_manager_flg"]  or dag_run.conf["primary_manager_flg"] == "N") and \
        (not dag_run.conf["hr_manager_flg"]  or dag_run.conf["hr_manager_flg"] == "N"):
        permission_sets.append(
            {
                "uri": dag_run.conf["default_user_permission_uri"],
                "name": null
            }
        )
    return permission_sets


def get_spain_user_create_policy_sets(dag_run):
    policy_sets = []
    if not dag_run.conf["new_entity_flag"]:
        if dag_run.conf["company_code"] == "W001":
            policy_sets.append({
                "uri": dag_run.conf["timeoff_uri"],
                "name": null
            })
            if dag_run.conf["acquired_company"].lower() != "inetum":
                policy_sets.append({
                    "uri": dag_run.conf["ot_request_template_spainuri"],
                    "name": null
                })
                policy_sets.append(
                    {
                        "uri": dag_run.conf["schedule_policyuri"],
                        "name": null
                })
                if int(float(dag_run.conf["employment_percentage"])) == 100:
                    policy_sets.append({
                        "uri": dag_run.conf["timesheet_for_fulltime_spainuri"],
                        "name": null
                    })
                else:
                    policy_sets.append({
                        "uri": dag_run.conf["timesheet_for_parttime_spainuri"],
                        "name": null
                    })
            else:
                policy_sets.append({
                    "uri": dag_run.conf["ot_request_template_rebadgeduri"],
                    "name": null
                })
                if int(float(dag_run.conf["employment_percentage"])) == 100:
                    policy_sets.append({
                        "uri": dag_run.conf["timesheet_for_fulltime_rebadgeduri"],
                        "name": null
                    })
                else:
                    policy_sets.append({
                        "uri": dag_run.conf["timesheet_for_parttime_rebadgeduri"],
                        "name": null
                    })
        if dag_run.conf["company_code"] == "W096":
            policy_sets.append({
                "uri": dag_run.conf["timeoff_uri"],
                "name": null
            })
            policy_sets.append({
                    "uri": dag_run.conf["ot_request_template_designituri"],
                    "name": null
            })
            if int(float(dag_run.conf["employment_percentage"])) == 100:
                policy_sets.append({
                    "uri": dag_run.conf["timesheet_for_fulltime_designituri"],
                    "name": null
                })
            else:
                policy_sets.append({
                    "uri": dag_run.conf["timesheet_for_parttime_designituri"],
                    "name": null
                })
    return policy_sets


def get_spain_user_create_custom_fields(dag_run):
    custom_field_values = []
    custom_fields = ["acquired_doj", "onsite_end_date", "travel_end_date",
                     "onsite_start_date", "date_of_birth", "travel_start_date", "reversal_date"]
    custom_fields_text = [ "gpo_adid", "hr_adid"]
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


def get_spain_user_create_location(dag_run):
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


def get_spain_user_payrule_script(dag_run):
    payrules = []
    if dag_run.conf["company_code"] == "W001":
        if dag_run.conf["acquired_company"].lower() != "inetum":
            if int(float(dag_run.conf["employment_percentage"])) == 100:
                payrules.append({
                    "payRuleScript": {
                        "uri": dag_run.conf["payrule_for_fulltime_spainuri"],
                        "name": null
                    },
                    "effectiveDate": null
                })
            else:
                payrules.append({
                    "payRuleScript": {
                        "uri": dag_run.conf["payrule_for_parttime_spainuri"],
                        "name": null
                    },
                    "effectiveDate": null
                })
        else:
            if int(float(dag_run.conf["employment_percentage"])) == 100:
                payrules.append({
                    "payRuleScript": {
                        "uri": dag_run.conf["payrule_for_fulltime_rebadgeduri"],
                        "name": null
                    },
                    "effectiveDate": null
                })
            else:
                payrules.append({
                    "payRuleScript": {
                        "uri": dag_run.conf["payrule_for_parttime_rebadgeduri"],
                        "name": null
                    },
                    "effectiveDate": null
                })
    if dag_run.conf["company_code"] == "W096":
        if int(float(dag_run.conf["employment_percentage"])) == 100:
            payrules.append({
                "payRuleScript": {
                    "uri": dag_run.conf["payrule_for_fulltime_designituri"],
                    "name": null
                },
                "effectiveDate": null
            })
        else:
            payrules.append({
                "payRuleScript": {
                    "uri": dag_run.conf["payrule_for_parttime_designituri"],
                    "name": null
                },
                "effectiveDate": null
            })
    return payrules


def get_spain_user_create_oefs(dag_run):
    extension_field_values = []
    extension_fields = ["project_supervisor_id", "project_supervisor_mailid",
                        "hr_manager_id", "hr_manager_mailid",
                        "gender", "acquired", "acquired_company", "billability_status",
                        "marital_status", "onsite_direct_recruit",
                        "sales_identifier", "employment_status", "no_of_children",
                        "insurance_type", "gpo_id", "gpo_email_id", "employee_band",
                        "forfait_emp_identifier", "employment_percentage", "personnel_area_text",
                        "personnel_subarea_text","religion","project_supervisor_adid"]

    for i in extension_fields:
        if dag_run.conf.get(i,""):
            if i in ["project_supervisor_id", "project_supervisor_mailid"]:
                if dag_run.conf['personnel_subarea_text'] == 'D&OP':
                    extension_field_values.append({
                        "definition": {
                            "uri": dag_run.conf[i+"uri"],
                            "name": null
                        },
                        "tag": null,
                        "numericValue": null,
                        "textValue": dag_run.conf["primary_supervisor_id"] if i == 'project_supervisor_id' else dag_run.conf["primary_supervisor_mailid"],
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
    custom_field_logs = get_spain_user_update_custom_fields(dag_run)[1]
    basic_details = request_payload.get_basic_user_details_update(dag_run)[1]
    location_logs = check_if_spain_user_location_update(dag_run)[1]
    timesheet_template_logs = get_timesheet_template_for_update(dag_run)[1]
    payrule_schedule_logs = get_payrule_schdule_for_update(dag_run)[1]
    department_logs = request_payload.get_department_update(dag_run)[1]
    if oef_logs:
        logs.append(oef_logs+ " ")
    if custom_field_logs:
        logs.append(custom_field_logs + " ")
    if basic_details:
        logs.append(basic_details + " ")
    if location_logs:
        logs.append(location_logs + " ")
    if timesheet_template_logs:
        logs.append(timesheet_template_logs+" ")
    if payrule_schedule_logs:
        logs.append(payrule_schedule_logs+" ")
    if department_logs:
        logs.append(department_logs + " ")
    update_tasks = {
        "update_the_reversal_date": "Reversal Date updated",
        "assign_hr_manager_permission": "HR manager permission updated",
        "assign_primary_manager_permission": "Primary manager permission updated",
        "assign_project_manager_permission": "Project manager permission updated",
        "update_end_date":"End date updated",
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


def get_user_annual_leaves_taken(config, response):
    if not response:
        return 0
    time_off_taken = list(filter(lambda i: i["timeOffStatus"]["displayText"] == "Approved" and
                                 i["timeOffType"]["displayText"] in config.TIME_OFF_TYPES_MAPPER["local_hire_or_assignee"]["unpaid_timeoff_types"], response))
    time_off_taken = list(
        map(lambda i: i["totalDuration"]["decimalWorkdays"], time_off_taken))
    number_of_accrued_to_taken = sum(time_off_taken)

    annual_time_off_taken = list(filter(lambda i: i["timeOffStatus"]["displayText"] == "Approved" and
                                 i["timeOffType"]["displayText"] =="ESP - Vacaciones anuales (Annual leave)", response))
    annual_time_off_taken = list(
        map(lambda i: i["totalDuration"]["decimalWorkdays"], annual_time_off_taken))
    number_of_annual_to_taken = sum(annual_time_off_taken)
    return {"unpaid_leave":number_of_accrued_to_taken,"annual_leave":number_of_annual_to_taken}


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


def get_supervisor_or_pm_uri():
    if rail.result("create_supervisor_in_replicon"):
        return rail.result("create_supervisor_in_replicon")
    elif rail.result("get_supervisor_user_details"):
        return rail.result("get_supervisor_user_details")[0]["userDetails"]["uri"]

def check_supervisor_update(dag_run):
    update_supervisor_uri = get_supervisor_or_pm_uri()
    if  dag_run.conf["supervisor_uri"] and update_supervisor_uri != dag_run.conf["supervisor_uri"]:
            return True
    return False

def get_extension_field_values_updates(dag_run):
    oef_update_req=[]
    oef_update_logs = ""

    for i, v in {"project_supervisor_id": "Project Supervisor ID", "project_supervisor_mailid": "Project Supervisor Email"}.items():
        if dag_run.conf["personnel_subarea_text"] == 'D&OP':
            supervisor_detail_to_update_for_dop = dag_run.conf["primary_supervisor_id"] if i == 'project_supervisor_id' else\
                dag_run.conf["primary_supervisor_mailid"]
            if supervisor_detail_to_update_for_dop and supervisor_detail_to_update_for_dop != rail.find_first_by_attr_and_get_attr(rail.result(
                    "get_extension_field_values"), "displayText", v, "textValue"):
                oef_update_req.append(request_payload.get_oef_text_field_update_payload(
                    dag_run.conf[i+"uri"], supervisor_detail_to_update_for_dop))
                oef_update_logs += v + " updated;"
        elif check_if_oef_update(
                dag_run.conf[i], v):
            oef_update_req.append(request_payload.get_oef_text_field_update_payload(
                dag_run.conf[i+"uri"],
                dag_run.conf[i]))
            oef_update_logs += v + " updated;"

    extension_fields = {"hr_manager_id":"HR Manager ID", "hr_manager_mailid":"HR Manager Email",
                        "gender":"Gender", "acquired": "Acquired",
                        "acquired_company":"Acquired Company", "billability_status":"Billability Status",
                        "hiring_status":"Hiring Status", "marital_status":"Marital Status",
                        "onsite_direct_recruit":"Onsite Direct Recruit",
                        "sales_identifier":"Sales Identifier", "employment_status":"Employment Status", "no_of_children":"Children",
                        "employee_band":"Employee Band",
                        "employment_percentage":"FTE", "personnel_area_text": "Personnel area Text",
                        "personnel_subarea_text": "Personnel Subarea Text",
                        "religion": "Religion",
                        "project_supervisor_adid":"Project Supervisor Login Name"}
    for i, v in extension_fields.items():
        if check_if_oef_update(
        dag_run.conf[i], v):
            oef_update_req.append(request_payload.get_oef_text_field_update_payload(
            dag_run.conf[i+"uri"],
            dag_run.conf[i]))
            oef_update_logs += v +" updated;"

    return oef_update_req, oef_update_logs

def get_spain_user_update_custom_fields(dag_run):
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
            custom_field_logs += v + " updated;"

    return custom_field_values, custom_field_logs

def get_assignee_policies(dag_run):
    result = rail.result("get_default_time_off_type_policy_schedule_for_user")
    effective_date = datetime.strptime(dag_run.conf["onsite_start_date"], "%Y-%m-%d") \
        if dag_run.conf["onsite_start_date"] not in INVALID_DATES and dag_run.conf["onsite_start_date"]\
          else datetime.combine(datetime.now().date(), datetime.min.time())
    effective_date = rail.get_replicon_date(effective_date)
    for i in result:
        if i :
            i[0].update({"effectiveDate": effective_date})
    return result

def get_spain_user_create_schedule_policy(dag_run):
    schedule_policy = []
    if dag_run.conf["company_code"] == "W001":
        if dag_run.conf["acquired_company"] != "INETUM":
            schedule_policy.append({
            "schedulePolicy": {
                "officeScheduleUri": null,
                "name": null,
                "officeSchedule": null,
                "scheduleTypeUri": "urn:replicon:schedule-type:shift"
            },
            "effectiveDate": null
        })
        else:
            schedule_policy.append({
            "schedulePolicy": {
                "officeScheduleUri": dag_run.conf["schedule_for_rebadgeduri"],
                "name": null,
                "officeSchedule": null,
                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
            },
            "effectiveDate": null
        })

    if dag_run.conf["company_code"] == "W096":
        schedule_policy.append({
            "schedulePolicy": {
                "officeScheduleUri": dag_run.conf["schedule_for_designituri"],
                "name": null,
                "officeSchedule": null,
                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
            },
            "effectiveDate": null
        })
    return schedule_policy

def get_startday_of_nexttimesheet():
    if rail.result('get_timesheet_details') and 'day' in rail.result('get_timesheet_details')['dateRange']['endDate']:
        return (datetime.strptime(
            str(rail.result('get_timesheet_details')['dateRange']['endDate']['year']) + '-' + str(
                rail.result('get_timesheet_details')['dateRange']['endDate']['month']) + '-' + str(
                    rail.result('get_timesheet_details')['dateRange']['endDate']['day']), "%Y-%m-%d") + timedelta(days=1)).date()
    return datetime.now().date()

def get_payrule_schdule_for_update(dag_run):
    payrules = []
    payrules_log = ""
    payrule_uri_to_assign = null
    if dag_run.conf["company_code"] == 'W001':
        if dag_run.conf["acquired_company"].lower() != "inetum":
            if int(float(dag_run.conf["employment_percentage"])) == 100:
                payrule_uri_to_assign = dag_run.conf["payrule_for_fulltime_spainuri"]
            else:
                payrule_uri_to_assign = dag_run.conf["payrule_for_parttime_spainuri"]
        else:
            if int(float(dag_run.conf["employment_percentage"])) == 100:
                payrule_uri_to_assign = dag_run.conf["payrule_for_fulltime_rebadgeduri"]
            else:
                payrule_uri_to_assign = dag_run.conf["payrule_for_parttime_rebadgeduri"]
    elif dag_run.conf["company_code"] == 'W096':
        if int(float(dag_run.conf["employment_percentage"])) == 100:
            payrule_uri_to_assign = dag_run.conf["payrule_for_fulltime_designituri"]
        else:
            payrule_uri_to_assign = dag_run.conf["payrule_for_parttime_designituri"]
    else:
        payrule_uri_to_assign = null

    if rail.result('get_timesheet_for_date2') and rail.result('get_timesheet_for_date2').get('timesheet').get('uri'):
        if not rail.result('get_update_user_details')['payRuleScriptSchedule'] or \
            (rail.result('get_update_user_details')['payRuleScriptSchedule'] and \
             payrule_uri_to_assign != rail.result('get_update_user_details')['payRuleScriptSchedule'][0]['payRuleScript']['uri']):

            payrules.append({
                "dateRange": {
                    "startDate": rail.get_replicon_date(get_startday_of_nexttimesheet()),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "item": {
                    "uri": payrule_uri_to_assign,
                    "name": null
                }
            })
            payrules_log = "Payrule updated"
    else:
        payrules_log = "Payrule not updated since timesheet not found"
    return payrules, payrules_log

def get_timesheet_template_for_update(dag_run):
    timesheet_template_to_assign_uri = null
    timesheet_template_to_assign_value = null
    timesheet_template_to_assign_log = ""
    if dag_run.conf["company_code"] == 'W001':
        if dag_run.conf["acquired_company"].lower() != "inetum":
            if int(float(dag_run.conf["employment_percentage"])) == 100:
                timesheet_template_to_assign_uri = dag_run.conf["timesheet_for_fulltime_spainuri"]
            else:
                timesheet_template_to_assign_uri = dag_run.conf["timesheet_for_parttime_spainuri"]
        else:
            if int(float(dag_run.conf["employment_percentage"])) == 100:
                timesheet_template_to_assign_uri = dag_run.conf["timesheet_for_fulltime_rebadgeduri"]
            else:
                timesheet_template_to_assign_uri = dag_run.conf["timesheet_for_parttime_rebadgeduri"]
    elif dag_run.conf["company_code"] == 'W096':
        if int(float(dag_run.conf["employment_percentage"])) == 100:
            timesheet_template_to_assign_uri = dag_run.conf["timesheet_for_fulltime_designituri"]
        else:
            timesheet_template_to_assign_uri = dag_run.conf["timesheet_for_parttime_designituri"]
    else:
        timesheet_template_to_assign_uri = null

    if rail.result('get_update_user_details').get('timesheetTemplate') and \
        timesheet_template_to_assign_uri and \
        timesheet_template_to_assign_uri != rail.result('get_update_user_details').get('timesheetTemplate').get('uri'):
        timesheet_template_to_assign_value = {
            "value": {
                "uri": timesheet_template_to_assign_uri,
                "name": null
            }
        }
        timesheet_template_to_assign_log = "Timesheet template updated"
    if not rail.result('get_update_user_details').get('timesheetTemplate') and timesheet_template_to_assign_uri:
        timesheet_template_to_assign_value = {
            "value": {
                "uri": timesheet_template_to_assign_uri,
                "name": null
            }
        }
        timesheet_template_to_assign_log = "Timesheet template updated"
    return timesheet_template_to_assign_value, timesheet_template_to_assign_log
