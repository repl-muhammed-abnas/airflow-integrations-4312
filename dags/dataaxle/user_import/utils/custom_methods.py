import hashlib
import pendulum
from datetime import datetime, date
import rail


null = None


def create_hash(config, item):
    fields = [str(item[col]) for col in config.INPUT_FILE_HEADERS]
    concatenated = "_".join(fields)
    return hashlib.md5(concatenated.encode()).hexdigest()


def build_ignore_list_logs(item, action, status, details):
    return {
        "username": item["first_name"] + " " + item["last_name"],
        "employeeid": item["empl_id"],
        "action": action,
        "status": status,
        "details": details,
        "jobid": rail.render_template('{{ dag_run_ecid() }}')
    }

def build_user_import_log(dag_run, action, status, details, parent_job_id, child_job_id):
    return {
        "employeeid": dag_run.conf.get("empl_id"),
        "username": dag_run.conf.get("first_name") + " " + dag_run.conf.get("last_name"),
        "action": action,
        "status": status,
        "details": details,
        "jobid": parent_job_id + "|" + child_job_id,
    }


def build_mandatory_fields_query(config, operator="=", joiner="OR"):
    """
    Build a query to filter records based on mandatory field values.

    operator: "=" for empty fields, "!=" for non-empty fields
    joiner:   "OR" to match any condition, "AND" to match all conditions
    """
    conditions = f" {joiner} ".join(f"{field} {operator} ''" for field in config.MANDATORY_FIELDS)
    return f"SELECT * FROM changed_records WHERE ({conditions})"


def get_new_job_titles_and_codes():
    existing_job_titles = {job_title["displayText"] for job_title in rail.result("get_enabled_service_centers")}
    input_job_titles_and_codes = rail.result("load_job_title")
    seen = set()
    new_job_titles_and_codes = []

    for record in input_job_titles_and_codes:
        title = record["job_title"]
        if title not in existing_job_titles and title not in seen:
            new_job_titles_and_codes.append({
                "job_title": title,
                "job_code": record["job_code"]
            })
            seen.add(title)
    return new_job_titles_and_codes


def get_new_custom_fields(input_file_custom_field_values, column_name):
    existing_custom_fields_values = {record["displayText"] for record in rail.result("get_all_custom_fields_drop_down_options")}
    seen = set()
    new_custom_field_values = []
    
    for record in input_file_custom_field_values:
        custom_field_value = record[column_name]
        if custom_field_value not in existing_custom_fields_values and custom_field_value not in seen:
            new_custom_field_values.append(custom_field_value)
            seen.add(custom_field_value)
    return new_custom_field_values


def get_old_and_new_custom_fields():
    old_and_new_fields = []
    for row in rail.result("get_all_custom_fields_drop_down_options"):
        old_and_new_fields.append(
            {
                "target": {
                    "uri": null,
                    "name": null
                },
                "name": row["displayText"],
                "isEnabled": "true"
            }
        )
    for row in rail.result("get_new_custom_fields"):
        old_and_new_fields.append(
            {
                "target": {
                    "uri": null,
                    "name": null
                },
                "name": row,
                "isEnabled": "true"
            }
        )
    return old_and_new_fields


def build_replicon_user_lookup():
    """
    Workato equivalent: "user check" ruby step.
    Builds a dict keyed by Employee ID -> UserUri from the Replicon user list report.
    (column_2 = Employee ID, column_3 = UserUri in the report CSV)
    """
    records = rail.load_all_records(rail.result("load_replicon_user_list_csv"))
    return {row["employee_id"]: row["user_uri"] for row in records}


def get_new_office_schedules():
    existing_schedules = {schedule["displayText"] for schedule in rail.result("get_all_office_schedules")}
    input_schedules = rail.load_all_records(rail.result("query_standard_hours"))
    seen = set()
    new_schedules = []

    for record in input_schedules:
        schedule_name = record["standard_hours"]
        if schedule_name not in existing_schedules and schedule_name not in seen:
            new_schedules.append({"standard_hours": schedule_name})
            seen.add(schedule_name)
    return new_schedules


def get_supervisor_details(config, report_to_manager_id):
    supervisor_details = rail.find_first_by_attr_and_get_attr(
        rail.result("load_all_supervisors_from_input"),
        "empl_id",
        report_to_manager_id,
    )
    if not supervisor_details:
        return {
            "empl_id": None, "email_id": None, "last_name": None, "first_name": None,
            "hire_or_rehire": None, "term_date": None, "location_description": None,
            "company_name": None, "location_state": None, "country": None,
            "payroll_dept_no": None, "payroll_dept_name": None, "rpc": None,
            "job_code": None, "job_title": None, "standard_hours": None,
            "hrly_or_salary": None, "reports_to_manager_id": None,
            "executive_level": None, "report_to_name": None, "empl_status": None,
            "md5": None, "hourly_billing_currency": None, "hourly_cost": None,
            "hourly_payroll_currency": None, "holiday_calendar": None,
            "timezone": None, "division": None, "department": None,
            "location_to_assign": None, "payroll_department_number_uri": None,
            "payroll_department_name_uri": None, "executive_level_uri": None,
            "user_supervisor_name_uri": None, "payroll_department_no_drop_down_uri": None,
            "payroll_department_name_dropdown_uri": None, "executive_level_dropdown_uri": None,
            "user_supervisor_name_dropdown_uri": None, "currency_uri": None,
            "employee_type_group": None,
        }
    
    hire_or_rehire_date = datetime.strptime(supervisor_details.get("hire_or_rehire"), "%m/%d/%Y")
    return {
        "empl_id": supervisor_details.get("empl_id"),
        "email_id": supervisor_details.get("email_id"),
        "last_name": supervisor_details.get("last_name"),
        "first_name": supervisor_details.get("first_name"),
        "hire_or_rehire": {
            "year": hire_or_rehire_date.year,
            "month": hire_or_rehire_date.month,
            "day": hire_or_rehire_date.day,
        },
        "term_date": supervisor_details.get("term_date"),
        "location_description": supervisor_details.get("location_description"),
        "company_name": supervisor_details.get("company_name"),
        "location_state": supervisor_details.get("location_state"),
        "country": supervisor_details.get("country"),
        "payroll_dept_no": supervisor_details.get("payroll_dept_no"),
        "payroll_dept_name": supervisor_details.get("payroll_dept_name"),
        "rpc": supervisor_details.get("rpc"),
        "job_code": supervisor_details.get("job_code"),
        "job_title": supervisor_details.get("job_title"),
        "standard_hours": supervisor_details.get("standard_hours"),
        "hrly_or_salary": supervisor_details.get("hrly_or_salary"),
        "reports_to_manager_id": supervisor_details.get("reports_to_manager_id"),
        "executive_level": supervisor_details.get("executive_level"),
        "report_to_name": supervisor_details.get("report_to_name"),
        "empl_status": supervisor_details.get("empl_status"),
        "md5": "NA",
        "hourly_billing_currency": find_value_from_list_of_dicts(
            config.COUNTRY_MAPPER, return_column="Hourly Billing Currency", 
            **{"country": supervisor_details.get("country")}
        ),
        "hourly_cost": find_value_from_list_of_dicts(
            config.COUNTRY_MAPPER, return_column="Hourly Cost Currency", 
            **{"country": supervisor_details.get("country")}
        ),
        "hourly_payroll_currency": find_value_from_list_of_dicts(
            config.COUNTRY_MAPPER, return_column="Hourly Payroll Currency", 
            **{"country": supervisor_details.get("country")}
        ),
        "holiday_calendar": find_value_from_list_of_dicts(
            config.COUNTRY_MAPPER, return_column="Holiday Calendar", 
            **{"country": supervisor_details.get("country")}
        ),
        "timezone": find_value_from_list_of_dicts(
            config.LOCATION_AND_TIMEZONE_MAPPER, return_column="Replicon timezone",
            **{"location - office": supervisor_details.get("location_description"),
            "Location - state": supervisor_details.get("location_state")
            }
        ),
        "division": find_value_from_list_of_dicts(
            config.DIVISION_MAPPER, return_column="2020 business unit - headcount division",
            **{
                "ADP payroll DEPT no": supervisor_details.get("payroll_dept_no"),
                "Payroll department name": supervisor_details.get("payroll_dept_name")
            }
        ),
        "department": get_children_department_uri(supervisor_details.get("company_name")),
        "location_to_assign": find_value_from_list_of_dicts(
            config.LOCATION_AND_TIMEZONE_MAPPER, return_column="Location",
            **{
                "location - office": supervisor_details.get("location_description"),
                "Location - state": supervisor_details.get("location_state")
            }
        ),
        "payroll_department_number_uri": rail.result("get_custom_fields").get("payroll_department_number_uri"),
        "payroll_department_name_uri": rail.result("get_custom_fields").get("payroll_department_uri"),
        "executive_level_uri": rail.result("get_custom_fields").get("executive_level_uri"),
        "user_supervisor_name_uri": rail.result("get_custom_fields").get("user_supervisor_name_uri"),
        "payroll_department_no_drop_down_uri": find_value_from_list_of_dicts(
            rail.result("get_all_custom_fields_drop_down_options_payroll_dept_no"), return_column="uri",
            **{
                "displayText": supervisor_details.get("payroll_dept_no")
            }
        ),
        "payroll_department_name_dropdown_uri": find_value_from_list_of_dicts(
            rail.result("get_all_custom_fields_drop_down_options_payroll_dept_name"), return_column="uri",
            **{
                "displayText": supervisor_details.get("payroll_dept_name")
            }
        ),
        "executive_level_dropdown_uri": find_value_from_list_of_dicts(
            rail.result("get_all_custom_fields_drop_down_options_executive_level"), return_column="uri",
            **{
                "displayText": supervisor_details.get("executive_level")
            }
        ),
        "user_supervisor_name_dropdown_uri": find_value_from_list_of_dicts(
            rail.result("get_all_custom_fields_drop_down_options_user_supervisor_name"), return_column="uri",
            **{
                "displayText": supervisor_details.get("report_to_name")
            }
        ),
        "currency_uri": find_value_from_list_of_dicts(rail.result("get_enabled_currencies"), return_column="uri",**{"symbol": find_value_from_list_of_dicts(
            config.COUNTRY_MAPPER, return_column="Hourly Cost Currency",
            **{
                "country": supervisor_details.get("country")
            }
        )}),
        "employee_type_group": find_value_from_list_of_dicts(rail.result("get_employee_type_group"), return_column="text_value",
            **{
                "code": supervisor_details.get("hrly_or_salary")
            }
        ),
    }


def find_value_from_list_of_dicts(mapper, return_column, **match_criteria):
    for record in mapper:
        if all(record.get(key) == value for key, value in match_criteria.items()):
            return record.get(return_column)
    return None


def build_process_user_conf(config, item):
    """Builds the conf dict for the process_users child DAG."""
    country = item.get("country")
    hire_or_rehire_date = datetime.strptime(item.get("hire_or_rehire"), "%m/%d/%Y")
    custom_fields = rail.result("get_custom_fields")
    today = pendulum.now(config.timezone)

    return {
        "empl_id": item.get("empl_id"),
        "email_id": item.get("email_id"),
        "last_name": item.get("last_name"),
        "first_name": item.get("first_name"),
        "term_date": item.get("term_date"),
        "location_description": item.get("location_description"),
        "company_name": item.get("company_name"),
        "location_state": item.get("location_state"),
        "country": country,
        "payroll_dept_no": item.get("payroll_dept_no"),
        "payroll_dept_name": item.get("payroll_dept_name"),
        "rpc": item.get("rpc"),
        "job_code": item.get("job_code"),
        "job_title": item.get("job_title"),
        "standard_hours": item.get("standard_hours"),
        "hrly_or_salary": item.get("hrly_or_salary"),
        "reports_to_manager_id": item.get("reports_to_manager_id"),
        "executive_level": item.get("executive_level"),
        "report_to_name": item.get("report_to_name"),
        "empl_status": item.get("empl_status"),
        "md5": item.get("md5"),
        "hourly_billing_currency": find_value_from_list_of_dicts(
            config.COUNTRY_MAPPER, return_column="Hourly Billing Currency",
            **{"country": country},
        ),
        "hourly_cost": find_value_from_list_of_dicts(
            config.COUNTRY_MAPPER, return_column="Hourly Cost Currency",
            **{"country": country},
        ),
        "hourly_payroll_currency": find_value_from_list_of_dicts(
            config.COUNTRY_MAPPER, return_column="Hourly Payroll Currency",
            **{"country": country},
        ),
        "holiday_calendar": find_value_from_list_of_dicts(
            config.COUNTRY_MAPPER, return_column="Holiday Calendar",
            **{"country": country},
        ),
        "timezone": find_value_from_list_of_dicts(
            config.LOCATION_AND_TIMEZONE_MAPPER, return_column="Replicon timezone",
            **{
                "location - office": item.get("location_description"),
                "Location - state": item.get("location_state"),
            },
        ),
        "division": find_value_from_list_of_dicts(
            config.DIVISION_MAPPER, return_column="2020 business unit - headcount division",
            **{
                "ADP payroll DEPT no": item.get("payroll_dept_no"),
                "Payroll department name": item.get("payroll_dept_name"),
            },
        ),
        "department": get_children_department_uri(item.get("company_name")),
        "location_to_assign": find_value_from_list_of_dicts(
            config.LOCATION_AND_TIMEZONE_MAPPER, return_column="Location",
            **{
                "location - office": item.get("location_description"),
                "Location - state": item.get("location_state"),
            },
        ),
        "payroll_department_number_uri": custom_fields.get("payroll_department_number_uri"),
        "payroll_department_name_uri": custom_fields.get("payroll_department_uri"),
        "executive_level_uri": custom_fields.get("executive_level_uri"),
        "user_supervisor_name_uri": custom_fields.get("user_supervisor_name_uri"),
        "payroll_department_no_drop_down_uri": find_value_from_list_of_dicts(
            rail.result("get_all_custom_fields_drop_down_options_payroll_dept_no"), return_column="uri",
            displayText=item.get("payroll_dept_no"),
        ),
        "payroll_department_name_dropdown_uri": find_value_from_list_of_dicts(
            rail.result("get_all_custom_fields_drop_down_options_payroll_dept_name"), return_column="uri",
            displayText=item.get("payroll_dept_name"),
        ),
        "executive_level_dropdown_uri": find_value_from_list_of_dicts(
            rail.result("get_all_custom_fields_drop_down_options_executive_level"), return_column="uri",
            displayText=item.get("executive_level"),
        ),
        "user_supervisor_name_dropdown_uri": find_value_from_list_of_dicts(
            rail.result("get_all_custom_fields_drop_down_options_user_supervisor_name"), return_column="uri",
            displayText=item.get("report_to_name"),
        ),
        "currency_uri": find_value_from_list_of_dicts(
            rail.result("get_enabled_currencies"), return_column="uri",
            symbol=find_value_from_list_of_dicts(
                config.COUNTRY_MAPPER, return_column="Hourly Cost Currency", country=country,
            ),
        ),
        "employee_type_group": find_value_from_list_of_dicts(
            rail.result("get_employee_type_group"), return_column="text_value",
            code=item.get("hrly_or_salary"),
        ),
        "manager_details": get_supervisor_details(config, item.get("reports_to_manager_id")),
        "hire_or_rehire": {
            "year": hire_or_rehire_date.year,
            "month": hire_or_rehire_date.month,
            "day": hire_or_rehire_date.day,
        },
        "today_date": {
            "year": today.year,
            "month": today.month,
            "day": today.day,
        },
        "user_uri": (rail.result("build_replicon_user_lookup") or {}).get(item.get("empl_id")),
        "user_import_log": rail.result("user_import_log"),
        "parent_job_id": rail.render_template("{{ dag_run_ecid() }}"),
    }


def extract_existing_user_data():
    """Return first user from BulkGetUsers3 emplid search result, or None."""
    result = rail.result("search_user_by_emplid") or []
    if result:
        user = result[0]
        user_details = user.get("userDetails") or {}
        sec_config = user.get("securityConfiguration") or {}
        return {
            "uri": user_details.get("uri"),
            "isEnabled": sec_config.get("isLoginEnabled"),
        }
    return None


def extract_supervisor_uri_after_creation():
    """Return URI of the supervisor found after triggering create_user_supervisor_child."""
    result = rail.result("search_supervisor_after_creation") or []
    if result:
        user_details = result[0].get("userDetails") or {}
        return user_details.get("uri")
    return None


def check_login_name_duplicate():
    """Return True if a user with the exact login name already exists."""
    result = rail.result("search_user_by_login") or []
    if not result:
        return False
    login_name = rail.result("extract_login_name")
    for user in result:
        sec_config = user.get("securityConfiguration") or {}
        if sec_config.get("loginName") and sec_config.get("loginName").lower() == login_name.lower():
            return True
    return False


def extract_login_name(email_id):
    return email_id.split("@")[0]


def get_children_department_uri(department_from_input_file):
    children_departments = rail.result("get_children_department_details")
    for child_department in children_departments:
        if child_department["displayText"] == department_from_input_file:
            return child_department["uri"]
    return None


def extract_supervisor_uri_from_search():
    """Return URI of the supervisor found by emplid search, or None."""
    result = rail.result("search_supervisor_by_emplid") or []
    if result:
        user_details = result[0].get("userDetails") or {}
        return user_details.get("uri")
    return None


def get_supervisor_permission_uri_from_assigned(task_result_name):
    """Return URI of the 'Supervisor' permission set if already assigned, else None."""
    perm_sets = rail.result(task_result_name) or []
    for ps in perm_sets:
        ps_info = ps.get("permissionSet") or {}
        if ps_info.get("name") == "Supervisor":
            return ps_info.get("uri")
    return None


def get_supervisor_permission_uri_from_all():
    """Return URI of the 'Supervisor' permission set from all available sets."""
    all_perms = rail.result("get_all_permission_sets") or []
    for perm in all_perms:
        if perm.get("name") == "Supervisor":
            return perm.get("uri")
    return None


def get_effective_schedule_policy_name():
    """Return displayText of the most recently effective schedule policy for this user."""
    user_data = get_user_data()
    if not user_data:
        return None
    schedule_policies = user_data[0].get("schedulePolicies", [])
    if not schedule_policies:
        return None

    today = date.today()
    best = None
    min_diff = float("inf")

    for policy in schedule_policies:
        eff_date_data = policy.get("effectiveDate") or {}

        if eff_date_data:
            eff_date = date(
                int(eff_date_data["year"]),
                int(eff_date_data["month"]),
                int(eff_date_data["day"]),
            )
            diff = (today - eff_date).days
            if 0 <= diff < min_diff:
                min_diff = diff
                best = policy

    if best is None and schedule_policies:
        best = schedule_policies[0]

    if best:
        office_schedule = best.get("officeSchedule") or {}
        return office_schedule.get("displayText")
    return None


def get_user_data():
    return rail.result("get_user_data")


def is_change_in_custom_field_value(custom_field, input_custom_field_value):
    user_data = get_user_data()
    if not user_data:
        return False
    if not user_data[0]["userDetails"]["customFieldValues"]:
        return False
    list_of_custom_fields = user_data[0]["userDetails"]["customFieldValues"]
    for field in list_of_custom_fields:
        if field["customField"].get("name") == custom_field and field.get("text") != input_custom_field_value:
            return True
    return False


def extract_current_supervisor_uri(response):
    """Extract the current supervisor's URI from UserListService GetData response."""
    if not response:
        return None
    rows = response.get("rows") or []
    if not rows:
        return None
    cells = rows[0].get("cells") or []
    if not cells:
        return None
    return cells[0].get("uri")