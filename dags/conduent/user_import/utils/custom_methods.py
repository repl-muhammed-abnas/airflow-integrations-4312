from datetime import datetime
from pendulum import now
import rail
from conduent.user_import.utils import request_payload
null = None


def get_effective_date(config, dag_run, date_type="effective_date"):
    _today = now(tz=config.time_zone).strftime("%m/%d/%Y")
    if date_type == "end_date" and dag_run.conf.get("date_termed"):
        _today = dag_run.conf["date_termed"]
    elif date_type == "effective_date" and dag_run.conf.get("effective_date"):
        _today = dag_run.conf.get("effective_date")
    return rail.parse_date(_today, "%m/%d/%Y")


def get_exception_logs(item):
    msg = "Mandatory field/s"
    if not item.get("win_id"):
        msg += "WIN ID | "
    if not item.get("first_name"):
        msg += "First Name | "
    if not item.get("last_name"):
        msg += "Last Name | "
    if not item.get("email"):
        msg += "Email | "
    if not item.get("assignment_status"):
        msg += "Assignment status | "
    if not item.get("date_active"):
        msg += "Date Active | "
    msg += "is/are missing."
    return msg


def get_all_custom_fields_data(response):
    custom_fields = list(map(lambda i: {
        "displayText": i["displayText"],
        "uri": i["uri"]
    }, response))
    return {
        "job_title_uri": rail.find_first_by_attr_and_get_attr(
            custom_fields,
            "displayText",
            "Job Title",
            "uri"
        ),
        "assignment_status_uri": rail.find_first_by_attr_and_get_attr(
            custom_fields,
            "displayText",
            "Assignment Status",
            "uri"
        )
    }


def get_user_groups_data(item, config):
    holiday_calendar_uri = ""
    if item["location_code"] and config.HOLIDAY_CALENDAR_MAPPER.get(item["location_code"]):
        holiday_calendar_uri = rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_holiday_calendars"),
            "displayText",
            config.HOLIDAY_CALENDAR_MAPPER[item["location_code"]],
            "uri"
        )
    return {
        "location_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_locations"),
            "displayText",
            item["location_code"],
            "uri"
        ),
        "business_group_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_business_groups"),
            "displayText",
            item["business_group"],
            "uri"
        ),
        "cost_center_schedule_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_cost_centers"),
            "displayText",
            item["cost_center"],
            "uri"
        ),
        "holiday_calendar_uri": holiday_calendar_uri,
        "timesheet_period": config.GENERAL_MAPPER["timesheet_period"],
        "timesheet_approval_path": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timesheet_approval_path"),
            "displayText",
            config.GENERAL_MAPPER["timesheet_approval_path"],
            "uri"
        ),
        "timeoff_approval_path": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_timeoff_approval_path"),
            "displayText",
            config.GENERAL_MAPPER["timeoff_approval_path"],
            "uri"
        ),
        "time_zone_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_time_zones"),
            "displayText",
            config.GENERAL_MAPPER["time_zone"],
            "uri"
        ),
        "work_week_uri": "urn:replicon:day-of-week:sunday",
        "work_schedule_name_uri": rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_office_schedules"),
            "displayText",
            item["work_schedule_name"],
            "uri"
        ) or rail.find_first_by_attr_and_get_attr(
            rail.result("get_all_office_schedules"),
            "displayText",
            config.GENERAL_MAPPER["default_schedule"],
            "uri"
        )

    }


def get_user_config(item, config):
    return {
        **item,
        **rail.result("get_all_custom_fields"),
        **get_user_groups_data(item, config)
    }


def get_user_exception_logs(dag_run):
    msg = ""
    if not dag_run.conf["business_group_uri"] and dag_run.conf["business_group"]:
        msg = "New Business Group/Missing Business Group;"
    if not dag_run.conf["location_uri"] and dag_run.conf["location_code"]:
        msg += "New Location/Missing Location;"
        msg += "Holiday Calendar Not assigned;"
    elif dag_run.conf["location_uri"] and dag_run.conf["location_code"] and not dag_run.conf["holiday_calendar_uri"]:
        msg += "Holiday Calendar Not assigned;"
    if not dag_run.conf["cost_center_schedule_uri"] and dag_run.conf["cost_center"]:
        msg += "New cost center/Missing Cost center;"
    if not rail.result("get_supervisor_details") and dag_run.conf["manager_win"]:
        msg += "Supervisor not assigned ;Active Supervisor not found with " + \
            dag_run.conf["manager_win"] + " id " + \
            "or Mutiple profiles with same id;"
    return msg


def get_update_user_logs(config, dag_run):
    logs = []
    exceptions = get_user_exception_logs(dag_run)
    basic_attribute_logs = request_payload.get_update_user_payload(
        dag_run, config, "logs")
    if exceptions:
        logs.append(exceptions)
    update_tasks = {
        "update_user": basic_attribute_logs,
        "update_supervisor": "Supervisor updated;"
    }
    success_tasks = list(map(lambda x: x.task_id, filter(lambda x: x.state == "success",
                                                         rail.get_current_context()["dag_run"].get_task_instances())))
    for i in success_tasks:
        if i in update_tasks and update_tasks[i]:
            logs.append(update_tasks[i]+" ")
    if not logs:
        logs.append("No user attribute updated")
    return "".join(logs)


def format_logs(dag_run):
    master_log = []
    user_import_logs = []
    if not dag_run.conf.get("disable_user", ""):
        user_import_logs = dag_run.conf["log_artifacts"]

    if rail.result('get_disable_user_import_logs'):
        user_import_logs += rail.result('get_disable_user_import_logs')

    for log in user_import_logs:
        log_records = rail.load_all_records(log)
        if log_records:
            master_log.extend(log_records)

    users ={}

    for user in master_log:
        win_id = user['properties'].get('win_id', '')
        details = user['properties'].get('details', '')
        if details:
            if win_id not in users:
                users[win_id] = []
            users[win_id].append(user)

    logs = []
    for user_logs  in users.values():
       
        has_errors = False
        err_rec = None
        for log in user_logs:
            if log['properties'].get('status') == 'Error':
                has_errors= True
                err_rec = log
                break

        status = ""
        first = user_logs[0]
        if has_errors:
            status = "Error"
        else:
            status = first['properties'].get('status', 'Error')

        # choose ecid from the actual error record when applicable
        ecid_out = first.get("ecid")
        if status == "Error":
            if err_rec:
                ecid_out = err_rec.get("ecid") or ecid_out

        logs.append({
            "timestamp": first.get("timestamp"),
            "win_id": first['properties'].get("win_id", ""),
            "login_name": first['properties'].get("email", ""),
            "employee_first_name": first['properties'].get("first_name", ""),
            "employee_last_name": first['properties'].get("last_name", ""),
            "status": status,
            "action": first['properties'].get("action", ""),
            "details": ', '.join(list(map(lambda x: x['properties'].get('details', ""), user_logs))),
            "ecid": ecid_out
        })
    if users:
        rail.set_result(key="error_record_count", val=len(list(filter(lambda x: x['status'] == 'Error', logs))))
        rail.set_result(key="total_processed_count", val=len(logs))
        rail.set_result(key="total_count", val=dag_run.conf.get("total_import_records", 0))
        rail.set_result(key="success_count", val=len(list(filter(lambda x: x['status'] == 'Success', logs))))
        rail.set_result(key="user_add_count", val=len(list(filter(lambda x: x["action"] == "Add" or x["action"] == "Rehire", logs))))
        rail.set_result(key="user_update_count", val=len(list(filter(lambda x: x["action"] == "Update", logs))))

    return logs



def check_if_schedule_update(dag_run, config):
    data = rail.result("get_user_details")["schedulePolicies"]
    if dag_run.conf["work_schedule_name_uri"]:
        if not data:
            return True
        current_schedule = list(filter(lambda x: datetime(
            **x['effectiveDate']) if x['effectiveDate'] else datetime.min <= datetime(**get_effective_date(config, dag_run)), data))
        if len(current_schedule) == 0 or current_schedule[-1]['officeSchedule']["uri"] != dag_run.conf["work_schedule_name_uri"]:
            return True
    return False


def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.min
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])


def get_udf_req(customfield_uri=null, text=null, date=null, dropdown=null, number=null):
    return {
        "customField": {
            "uri": customfield_uri,
            "name": null,
            "groupUri": null
        },
        "text": text,
        "date": date,
        "dropDownOption": dropdown,
        "number": number
    }

def get_custom_fields_update(dag_run):
    custom_fields = []
    status = ""
    user_details = {
        "assignment_status": rail.result("get_user_details")["userDetails"]["customFieldValues"][0]["text"],
        "job_title": rail.result("get_user_details")["userDetails"]["customFieldValues"][1]["text"],
        "date_termed": rail.result("get_user_details")["userDetails"]["employmentDateRange"]["endDate"],
        "date_active": rail.result("get_user_details")["userDetails"]["employmentDateRange"]["startDate"],
        "enabled": rail.result("get_user_details")["securityConfiguration"]["isLoginEnabled"]
    }
    if user_details["assignment_status"] != dag_run.conf["assignment_status"]\
            and dag_run.conf["assignment_status"].lower() == "active":
        custom_fields.append(get_udf_req(
            dag_run.conf["assignment_status_uri"], text=dag_run.conf["assignment_status"]))
        status += "Assigment status updated;"
    elif user_details["assignment_status"] != dag_run.conf["assignment_status"] and \
        dag_run.conf["assignment_status"].lower() in ["inactive", "suspended"] and user_details["enabled"] and \
        not rail.find_first_by_attr_and_get_attr(
            rail.result("get_user_details")["permissionSets"],
            "displayText",
            "Project Manager",
            "uri"):
        custom_fields.append(get_udf_req(
            dag_run.conf["assignment_status_uri"], text=dag_run.conf["assignment_status"]))
        status += "Assigment status updated;"
    if user_details["job_title"] != dag_run.conf["job_title"]:
        custom_fields.append(get_udf_req(
            dag_run.conf["job_title_uri"], text=dag_run.conf["job_title"]))
        status += "Job title updated;"
    return custom_fields, status

def check_if_user_attribute_update(config,dag_run):
    user_details = {
        "first_name": rail.result("get_user_details")["userDetails"]["firstName"],
        "last_name": rail.result("get_user_details")["userDetails"]["lastName"],
        "preferred_name": rail.result("get_user_details")["securityConfiguration"]["user"]["displayText"],
        "date_termed": rail.result("get_user_details")["userDetails"]["employmentDateRange"]["endDate"],
        "date_active": rail.result("get_user_details")["userDetails"]["employmentDateRange"]["startDate"],
        "enabled": rail.result("get_user_details")["securityConfiguration"]["isLoginEnabled"],
        "assignment_status": rail.result("get_user_details")["userDetails"]["customFieldValues"][0]["text"],
    }
    start_date = get_date_from_replicon_date(user_details["date_active"])
    updated_end_date = null
    if dag_run.conf["date_termed"]:
        updated_end_date = get_date_from_replicon_date(
            rail.parse_date(dag_run.conf["date_termed"], "%m/%d/%Y"))
    updated_start_date = get_date_from_replicon_date(
        rail.parse_date(dag_run.conf["date_active"], "%m/%d/%Y"))
    status = ""
    first_name = last_name = preferred_name = date_active = null
    date_termed = user_details["date_termed"] or null
    if user_details["first_name"] != dag_run.conf["first_name"]:
        first_name = dag_run.conf["first_name"]
        status = "First name updated;"
    if user_details["last_name"] != dag_run.conf["last_name"]:
        last_name = dag_run.conf["last_name"]
        status += "Last name updated;"
    if user_details["preferred_name"] != dag_run.conf["preferred_name"]:
        preferred_name = dag_run.conf["preferred_name"]
        status += "Preferred name updated;"
    if start_date != updated_start_date:
        date_active = rail.parse_date(dag_run.conf["date_active"], "%m/%d/%Y")
        start_date = updated_start_date
        status += "Start date updated;"
    if not updated_end_date and user_details["date_termed"] and dag_run.conf["assignment_status"].lower() == "active":
        date_termed = null
        status += "End date updated;"
    elif updated_end_date and get_date_from_replicon_date(user_details["date_termed"]) != updated_end_date\
            and start_date < updated_end_date:
        date_termed = rail.parse_date(dag_run.conf["date_termed"], "%m/%d/%Y")
        status += "End date updated;"
    elif user_details["assignment_status"] != dag_run.conf["assignment_status"] and \
        dag_run.conf["assignment_status"].lower() in ["inactive", "suspended"] and user_details["enabled"] and \
        not rail.find_first_by_attr_and_get_attr(
            rail.result("get_user_details")["permissionSets"],
            "displayText",
            "Project Manager",
            "uri"):
        date_termed = get_effective_date(config, dag_run, date_type="end_date")
    return {
        "firstName": first_name,
        "lastName": last_name,
        "emailAddress": null,
        "language": null,
        "employmentDateRange": {
            "startDate": date_active or user_details["date_active"],
            "endDate": date_termed,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        },
        "employmentStartDate": null,
        "employmentEndDate": null,
        "employeeId": null,
        "displayNameParameter": {
            "displayName": preferred_name
        } if preferred_name else null
    }, status


def get_supervisor_uri(response, dag_run):
    if not response["rows"]: 
        return False
    project_manager = list(filter(lambda i:dag_run.conf["manager_win"] == i["cells"][3]["textValue"], response["rows"]))
    if len(project_manager) == 1:
        return project_manager[0]["cells"][0]["uri"]
    return False

def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

def if_loginname_already_exists():
    if get_task_state('update_user_loginname') == 'failed':
        reason  = rail.result('update_user_loginname', key="error").get('response').get('json').get('error').get('reason')
        if reason == 'The specified user already exists.':
            return True
        return False
    return False