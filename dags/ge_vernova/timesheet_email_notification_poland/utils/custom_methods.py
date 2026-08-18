import json
import pendulum
import rail

null = None

def logging_details():
    return {
        "dag_run_start_time": pendulum.now().strftime("%m%d%Y%H%M")
    }

def get_data(attribute, supervisor_name, timesheets_data):
    return list(map(lambda row: row[attribute],
                    filter(lambda row: row["supervisor_name"] == supervisor_name, timesheets_data)))

def get_supervisor_email(supervisor_name, timesheets_data):
    return rail.find_first_by_attr_and_get_attr(timesheets_data, "supervisor_name", supervisor_name, "supervisor_email")

def get_usernames(supervisor_name, timesheets_data):
    return list(map(lambda row: (" ".join(row["username"].split(",")[::-1])).strip(),
                    filter(lambda row: row["supervisor_name"] == supervisor_name, timesheets_data)))

def get_supervisors_timesheet_data(item):
    timesheets_data = rail.result("load_timesheets_data")
    if not item:
        return []

    return {
        "supervisor_name": item["supervisor_name"],
        "date_range_value": get_data("date_range_value", item["supervisor_name"], timesheets_data),
        "supervisor_email": get_supervisor_email(item["supervisor_name"], timesheets_data),
        "users": get_usernames(item["supervisor_name"], timesheets_data),
        "locations": get_data("location", item["supervisor_name"], timesheets_data)
    }

def get_users_timesheets_data(dag_run):
    users_data = zip(json.loads(dag_run.conf["supervisor_data"]["users"]), json.loads(dag_run.conf["supervisor_data"]["date_range_value"]))
    return list(map(lambda data:
        {
            "user": data[0],
            "timesheetperiod": data[1]
        }, users_data ))

def get_users_locations(dag_run):
    users_data = json.loads(dag_run.conf["supervisor_data"]["locations"])
    return list(map(lambda location:
        {
            "location": location,
        }, users_data ))

def get_poland_locations():
    return list(map(lambda data:
        {
            "location": data["location"],
        }, filter(lambda data: data["location"] == "Poland", rail.result("users_locations") )))

def get_email_template(suffix):
    if suffix == 'poland':
        return 'poland_format.html'
    if suffix == 'all_locations':
        return 'other_locations_format.html'
    return 'not_poland_format.html'
