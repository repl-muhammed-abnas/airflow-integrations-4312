import itertools
import pendulum
import rail
null = None

def logging_details(time_zone):
    today = pendulum.now(time_zone)
    return {
        "current_time_json": {
            "year": today.year,
            "month": today.month,
            "day": today.day
        }
    }

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

def get_invalid_user_log_details(item):
    message = "User not processed due to following reason/s "
    mandatory_fields = {
    	"employee_id": "EmployeeID",
    	"workforce_id": "WorkforceID",
    	"first_name": "FirstName",
    	"last_name": "LastName",
    	"employee_email": "EmployeeEmail",
    	"start_date": "StartDate",
    	"employee_status": "EmployeeStatus",
    	"employee_type_code": "EmployeeTypeCode",
    	"employee_type_name": "EmployeeTypeName",
    	"location_code": "LocationCode",
    	"location_name": "LocationName",
    	"department_code": "DepartmentCode",
    	"department_name": "DepartmentName",
    	"company_code": "CompanyCode",
    	"company_code_name": "CompanyCodeName",
    	"costcenter_code": "CostCenterCode",
    	"costcenter_name": "CostCenterName",
    	"supervisor": "Supervisor",
    	"work_schedule": "WorkSchedule",
    	"holiday_calendar": "HolidayCalendar"
    }

    blank_fields = []
    for i in item:
        if not item[i] and i in mandatory_fields:
            blank_fields.append(mandatory_fields[i])
    return message + "; ".join(blank_fields) + " not present in the payload"

def get_required_timeoffs_data(response, config):
    return [{
        location_code: list(map(lambda timeoff_data: {
            "timeoff_type_name": timeoff_data["displayText"],
            "uri": timeoff_data["uri"]
        }, filter(lambda timeoff_data: timeoff_data["displayText"] in config.location_wise_data_mapper[location_code]["timeoff_types"], response)))
    } for location_code in config.location_wise_data_mapper.keys()]

def get_user_current_holiday_calendar(response):
    if not response:
        return {}
    return {
        "holiday_calendar_name": response[0]["holidayCalendar"]["displayText"],
        "uri": response[0]["holidayCalendar"]["uri"]
    }

def get_required_timesheet_templates(response, config):
    # Collect all template names (both internal and external for v1.6)
    template_names = []
    for data in config.location_wise_data_mapper.values():
        # Support both old structure (single template) and new structure (internal/external)
        if "timesheet_template_internal" in data and "timesheet_template_external" in data:
            template_names.append(data["timesheet_template_internal"])
            template_names.append(data["timesheet_template_external"])
        elif "timesheet_template" in data:
            template_names.append(data["timesheet_template"])

    return list(map(lambda timeoff_data: {
        "timesheet_template_name": timeoff_data["displayText"],
        "uri": timeoff_data["uri"]
    }, filter(lambda timeoff_data: timeoff_data["displayText"] in template_names, response)))

def get_required_permission_sets(response):
    return {
        "project_res_with_reports": rail.find_first_by_attr_and_get_attr(response, "displayText", "Project Resource with Reports", "uri")
    }

def get_required_timesheet_approval_path(response):
    return rail.find_first_by_attr_and_get_attr(response, "displayText", "Client Representative", "uri")

def get_required_timeentry_approval_path(response):
    return rail.find_first_by_attr_and_get_attr(response, "displayText", "Project Manager", "uri")

def get_required_object_extension_fields_data(response):
    return list(map(lambda oef: {
        "name": oef["name"],
        "uri": oef["uri"]
    }, filter(lambda oef: oef["name"] == "Workforce ID", response)))

def get_required_timesheet_periods(response):
    return list(map(lambda record: {
        "timesheet_period_name": record["cells"][0]["textValue"],
        "uri": record["cells"][0]["uri"]
    }, filter(lambda record: record["cells"][0]["textValue"] == "Weekly without crossing months", response["rows"])))

def get_all_locations(response):
    return [{
        "location_name": location_data["displayText"],
        "uri": location_data["uri"]
    } for location_data in response]

def get_required_location(response, dag_run):
    return [{
        "location_name": location_data["displayText"],
        "uri": location_data["uri"]
    } for location_data in response if location_data["displayText"] == dag_run.conf["location_name"]]

def get_all_costcenters(response):
    return [{
        "costcenter_name": costcenter_data["displayText"],
        "uri": costcenter_data["uri"]
    } for costcenter_data in response]

def get_required_costcenter(response, dag_run):
    return [{
        "costcenter_name": costcenter_data["displayText"],
        "uri": costcenter_data["uri"]
    } for costcenter_data in response if costcenter_data["displayText"] == dag_run.conf["costcenter_name"]]

def get_all_departments(response):
    return [{
        "department_name": department_data["displayText"],
        "uri": department_data["uri"]
    } for department_data in response]

def get_required_department(response, dag_run):
    return [{
        "department_name": department_data["displayText"],
        "uri": department_data["uri"]
    } for department_data in response if department_data["displayText"] == f'{dag_run.conf["department_name"]}-{dag_run.conf["department_code"]}']

def get_all_employeetypes(response):
    return [{
        "employee_type_name": employee_types_data["displayText"],
        "uri": employee_types_data["uri"]
    } for employee_types_data in response]

def get_required_employeetype(response, dag_run):
    return [{
        "employee_type_name": employee_types_data["displayText"],
        "uri": employee_types_data["uri"]
    } for employee_types_data in response if employee_types_data["displayText"] == dag_run.conf["employee_type_name"]]

def get_all_servicecenters(response):
    return [{
        "company_code_name": service_center_data["displayText"],
        "uri": service_center_data["uri"]
    } for service_center_data in response]

def get_required_servicecenter(response, dag_run):
    return [{
        "company_code_name": service_center_data["displayText"],
        "uri": service_center_data["uri"]
    } for service_center_data in response if service_center_data["displayText"] == dag_run.conf["company_code_name"]]

def get_prereq_data_conf(item):
    return {
        **item,
        "process_start_time": rail.result("logging_details")["current_time_json"],
        "workforceid_oef_uri": rail.result("get_required_object_extension_fields")[0]["uri"]
            if rail.result("get_required_object_extension_fields") else null,
        "timeoff_types": list(itertools.chain.from_iterable(timeoffs_record.get(item["location_code"], [])
            for timeoffs_record in rail.result("get_required_time_off_types")
                if item["location_code"] in timeoffs_record.keys())),
        "holiday_calendar_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_holiday_calendars"),
            "displayText", item["holiday_calendar"], "uri"),
        "permission_sets": rail.result("get_required_permission_sets"),
        "timesheet_approval_path": rail.result("get_required_timesheet_approval_path"),
        "time_entry_approval_path": rail.result("get_required_timeentry_approval_path"),
        "timesheet_period": rail.result("get_required_timesheet_periods")[0]
            if rail.result("get_required_timesheet_periods") else null,
        "timesheet_template": rail.result("get_required_timesheet_templates"),
        "work_schedule_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_scheduletypes"),
            "displayText", item["work_schedule"], "uri"),
    }

def do_format_logs():
    log_artifacts = []
    log_records = []

    logs = (rail.result("gather_user_logs") if rail.result("gather_user_logs") else []) + [rail.result("create_groups_log")]

    if logs:
        if isinstance(logs, list):
            log_artifacts.extend(logs)
        else:
            log_artifacts.append(logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **log['properties'],
        "runid": log['ecid']
        }, log_records))

    rail.set_result(key="get_logged_success", val=len(list(filter(lambda item: item['status']=="Success", final_log_records))))
    rail.set_result(key="get_logged_errors", val=len(list(filter(lambda item: item['status']=="Error", final_log_records))))
    rail.set_result(key="get_logged_exceptions", val=len(list(filter(lambda item: item['status']=="Exception", final_log_records))))

    return final_log_records
