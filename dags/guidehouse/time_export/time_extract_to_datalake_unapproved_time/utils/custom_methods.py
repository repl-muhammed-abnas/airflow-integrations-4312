import rail

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from guidehouse.time_export.time_export_master.utils.custom_methods import sanitize_free_text

def get_current_week_date_range(todays_date):
    today = datetime.strptime(todays_date, "%Y-%m-%d")
    weekday = today.weekday()  # Monday=0, Sunday=6
    if weekday == 0:  # Monday
        start_date = today
        end_date = today + timedelta(days=6)
    elif weekday == 6:  # Sunday
        start_date = today - timedelta(days=6)
        end_date = today
    else:
        start_date = today - timedelta(days=weekday)
        end_date = today + timedelta(days=(6 - weekday))
    return {
        "entry_start_date": start_date.strftime("%m/%d/%Y"),
        "entry_end_date": end_date.strftime("%m/%d/%Y"),
    }

def get_previous_week_date_range(current_week_range):
    monday = datetime.strptime(current_week_range['entry_start_date'], "%m/%d/%Y")
    previous_monday = monday - timedelta(days=7)
    previous_sunday = previous_monday + timedelta(days=6)
    return {
        "entry_start_date": previous_monday.strftime("%m/%d/%Y"),
        "entry_end_date": previous_sunday.strftime("%m/%d/%Y"),
    }

def get_historic_week_date_range(previous_week_date_range):
    previous_monday = datetime.strptime(previous_week_date_range['entry_start_date'], "%m/%d/%Y")
    return {
        "entry_start_date": (previous_monday - relativedelta(years=3)).strftime("%m/%d/%Y"),
        "entry_end_date": (previous_monday - timedelta(days=1)).strftime("%m/%d/%Y"),
    }


def get_current_prior_and_history_week_date_range(todays_date):
    current_week_date_range = get_current_week_date_range(todays_date)
    previous_week_date_range = get_previous_week_date_range(current_week_date_range)
    historic_week_date_range = get_historic_week_date_range(previous_week_date_range)
    return {
        "current_week_entry_start_date": current_week_date_range['entry_start_date'],
        "current_week_entry_end_date": current_week_date_range['entry_end_date'],
        "previous_week_entry_start_date": previous_week_date_range['entry_start_date'],
        "previous_week_entry_end_date": previous_week_date_range['entry_end_date'],
        "historic_week_entry_start_date": historic_week_date_range['entry_start_date'],
        "historic_week_entry_end_date": historic_week_date_range['entry_end_date'],
    }


def get_company_code_date_range_pairs(date_ranges, company_code_sets):
    date_windows = [
        ("CurrentWeek", date_ranges["current_week_entry_start_date"], date_ranges["current_week_entry_end_date"]),
        ("PriorWeek", date_ranges["previous_week_entry_start_date"], date_ranges["previous_week_entry_end_date"]),
        ("PT", date_ranges["historic_week_entry_start_date"], date_ranges["historic_week_entry_end_date"]),
    ]
    pairs = []
    for company_code_set in company_code_sets:
        for week_type, start_date, end_date in date_windows:
            pairs.append({
                "company_code": list(company_code_set["codes"]),
                "file_name_prefix": company_code_set["file_name_prefix"],
                "week_type": week_type,
                "entry_start_date": start_date,
                "entry_end_date": end_date,
            })
    return pairs


def get_unapproved_time_extract_report_filter_uri(report_details):
    enabled_filters = report_details['filterConfiguration']['enabledFilters']
    return {
        "entry_date_filter_uri": rail.find_first_by_attr_and_get_attr(enabled_filters, 'displayText', 'EntryDateFilter', 'uri'),
        "current_company_code_filter_uri": rail.find_first_by_attr_and_get_attr(enabled_filters, 'displayText', 'CurrentCostCenterFilter', 'uri')
    }


def make_report_filter(dag_run):
    report_filters = []
    for company_code in dag_run.conf["company_code"]:
        company_code_uri = get_last_element_of_company_code_uri(dag_run.conf["level_1_cost_center_name_and_uri_pairs"], company_code)
        if company_code_uri:
            report_filters.append({
                "reportFilterUri": dag_run.conf["report_filter_uri"]["current_company_code_filter_uri"],
                "value": company_code_uri,
            })
    report_filters.append({
        "reportFilterUri": dag_run.conf["report_filter_uri"]["entry_date_filter_uri"],
        "value": None
    })
    report_filters.append({
        "reportFilterUri": dag_run.conf["report_filter_uri"]["entry_date_filter_uri"],
        "value": dag_run.conf["entry_start_date"]
    })
    report_filters.append({
        "reportFilterUri": dag_run.conf["report_filter_uri"]["entry_date_filter_uri"],
        "value": dag_run.conf["entry_end_date"]
    })
    return report_filters


def get_last_element_of_company_code_uri(cost_center_name_uri_dict, company_code):
    company_code_uri = cost_center_name_uri_dict.get(company_code, "")
    if company_code_uri:
        return company_code_uri.split(":")[-1]
    return ""


def page_handler(request, result):
    if len(result["rows"]) > 0:
        request["page"] += 1
        return request
    return None


def get_level_1_cost_center_name_and_uri_pairs(cost_centers, hierarchy_level):
    cost_center_name_and_uri_pairs = {}
    for cost_center in cost_centers:
        if cost_center.get("hierarchy_level") == hierarchy_level:
            cost_center_name = cost_center.get("cost_center_name")
            cost_center_uri = cost_center.get("cost_center_uri")
            cost_center_name_and_uri_pairs[cost_center_name] = cost_center_uri
    return cost_center_name_and_uri_pairs


def format_date(date_str, format_in="%Y/%m/%d", format_out="%m%d%Y"):
    if not date_str:
        return ""
    date_obj = datetime.strptime(date_str, format_in)
    return date_obj.strftime(format_out)


def get_unique_id(item):
    employee_id = item.get("employee_id", "")
    start_date = format_date(item.get("timesheet_start_date"))
    if employee_id:
        employee_id = str(employee_id)
    return employee_id + start_date


def get_task_code_or_task_name(item):
    source_system = (item.get("source_system") or "").lower()
    if source_system == "costpoint":
        return item.get("task_code", "")
    if source_system == "peoplesoft":
        return item.get("task_name", "")
    return ""


def format_timesheet_period(timesheet_period_str):
    if not timesheet_period_str:
        return ""
    timesheet_period_start_date, timesheet_period_end_date = timesheet_period_str.split("-")
    timesheet_period_start_date = format_date(timesheet_period_start_date.strip())
    timesheet_period_end_date = format_date(timesheet_period_end_date.strip())
    return timesheet_period_start_date + "-" + timesheet_period_end_date


def transform_peoplesoft_india_time_entry_records(item, dag_run, time_off_project_task_mapper):
    if not item:
        return []

    time_off_type = item.get("time_off_type", "")

    result = {}

    result["unique_id"] = get_unique_id(item)
    result["employee_id"] = item.get("employee_id", "")
    result["user_name"] = item.get("user_name")
    result["entry_date"] = format_date(item.get("entry_date"))

    if time_off_type:
        result["project_code"] = pluck_value_from_time_off_mapper(
            financial_system=item.get("financial_system", ""),
            time_off_type=time_off_type,
            time_off_type_mapper=time_off_project_task_mapper,
            fmla=item.get("fmla", ""),
            project_or_task="project"
        )
        result["task_code"] = pluck_value_from_time_off_mapper(
            financial_system=item.get("financial_system", ""),
            time_off_type=time_off_type,
            time_off_type_mapper=time_off_project_task_mapper,
            fmla=item.get("fmla", ""),
            project_or_task="task"
        )
        result["work_location_code"] = get_work_location_code(
            current_location_full_path=item.get("location_current_full_path", ""),
            dag_run=dag_run
        )

    else:
        result["project_code"] = item.get("project_code", "")
        result["task_code"] = get_task_code_or_task_name(item)
        result["work_location_code"] = item.get("work_location_code", "")

    result["hours"] = item.get("hours", "")
    result["company_code"] = item.get("company_code_current", "")
    result["timesheet_approval_status"] = item.get("timesheet_approval_status", "")
    result["time_off_type"] = time_off_type
    result["time_off_hours"] = item.get("time_off_hours", "")
    result["plc_name"] = ""
    result["plc"] = ""
    result["timesheet_period"] = format_timesheet_period(item.get("timesheet_period", ""))
    result["financial_system"] = item.get("financial_system", "")
    result["short_entry_id"] = item.get("short_entry_id", "")
    result["comments"] = sanitize_free_text(item.get("comments"))

    return result


def transform_costpoint_time_entry_records(item, dag_run, time_off_project_task_mapper):
    if not item:
        return []

    time_off_type = item.get("time_off_type", "")

    result = {}

    result["unique_id"] = get_unique_id(item)
    result["employee_id"] = item.get("employee_id", "")
    result["user_name"] = item.get("user_name")
    result["entry_date"] = format_date(item.get("entry_date"))

    if time_off_type:
        result["project_code"] = pluck_value_from_time_off_mapper(
            financial_system=item.get("financial_system", ""),
            time_off_type=time_off_type,
            time_off_type_mapper=time_off_project_task_mapper,
            fmla=item.get("fmla", ""),
            project_or_task="project"
        )

        result["task_code"] = pluck_value_from_time_off_mapper(
            financial_system=item.get("financial_system", ""),
            time_off_type=time_off_type,
            time_off_type_mapper=time_off_project_task_mapper,
            fmla=item.get("fmla", ""),
            project_or_task="task"
        )

        result["work_location_code"] = get_work_location_code(
            current_location_full_path=item.get("location_current_full_path", ""),
            dag_run=dag_run
        )

        result["plc_name"] = "GENRL"
        result["plc"] = "GENRL"

    else:
        result["project_code"] = item.get("project_code", "")
        result["task_code"] = get_task_code_or_task_name(item)
        result["work_location_code"] = item.get("work_location_code", "")
        role = (rail.result("login_name_task_uri_to_task_role_mapping") or {}).get(
            f"{item.get('login_name', '')}||{item.get('task_uri', '')}") or {}
        result["plc_name"] = role.get("plc_name") or "GENRL"
        result["plc"] = role.get("plc") or "GENRL"


    result["hours"] = item.get("hours", "")
    result["company_code"] = item.get("company_code_current", "")
    result["timesheet_approval_status"] = item.get("timesheet_approval_status", "")
    result["time_off_type"] = time_off_type
    result["time_off_hours"] = item.get("time_off_hours", "")
    result["timesheet_period"] = format_timesheet_period(item.get("timesheet_period", ""))
    result["financial_system"] = item.get("financial_system", "")
    result["short_entry_id"] = item.get("short_entry_id", "")
    result["comments"] = sanitize_free_text(item.get("comments"))

    return result


def pluck_value_from_time_off_mapper(financial_system, time_off_type, time_off_type_mapper, fmla, project_or_task):
    if not time_off_type:
        return ""

    if financial_system in ("PeopleSoft", "India"):
        system_prefix = "ps"
    elif financial_system == "CostPoint":
        system_prefix = "cp"
    else:
        return ""

    fmla_prefix = "fmla_" if (fmla or "").lower() == "yes" else ""
    key = f"{fmla_prefix}{system_prefix}_{project_or_task}_code"
    return time_off_type_mapper.get(time_off_type, {}).get(key, "")


def get_work_location_code(current_location_full_path, dag_run):
    if not current_location_full_path:
        return ""
    current_location_full_path = [ location.strip() for location in current_location_full_path.split("/")]
    country = current_location_full_path[0]
    if country == "United States of America" and len(current_location_full_path) > 1:
        return dag_run.conf.get("usa_level_1_location_code_pair", {}).get(current_location_full_path[1], "")
    return dag_run.conf.get("level_1_location_code_pairs", {}).get(country, "")


def build_project_role_code_map(response):
    role_uri_map = {}
    for page in response:
        for row in page.get("rows", []):
            cells = row.get("cells", [])
            if len(cells) < 2:
                continue
            role_uri_map[cells[1].get("uri")] = {
                "code": cells[0].get("textValue"),
                "name": cells[1].get("textValue"),
            }
    return role_uri_map


def build_login_task_role_mapping(results, default_role="GENRL"):
    role_uri_map = rail.result("get_all_project_roles") or {}
    mapping = {}
    for items, data in results:
        for item, detail in zip(items, data):
            detail = detail or {}
            if detail.get("error") or not detail.get("estimateDetails"):
                continue
            key = f"{item.get('login_name', '')}||{item.get('task_uri', '')}"
            project_role = detail["estimateDetails"].get("projectRole") or {}
            role = role_uri_map.get(project_role.get("uri")) or {}
            mapping[key] = {
                "plc": role.get("code") or default_role,
                "plc_name": role.get("name") or default_role,
            }
    return mapping
