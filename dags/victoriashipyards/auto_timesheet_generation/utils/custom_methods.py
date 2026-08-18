import pendulum

null = None


def get_log_params(time_zone):
    return {
        "dag_run_start_time": pendulum.now(time_zone).isoformat(),
        "log_filename": 'victoriashipyards_timesheetgeneration_logs_' + str((pendulum.now("Europe/Paris")).strftime("%Y_%m_%d")) + '.csv',
        "time_zone": time_zone
    }

exceptions_list =[
    'No timesheet period settings have been configured',
    "Cannot enter a timesheet outside of the user's start & end date ranges",
    "The user does not have a timesheet template or timesheet period assigned",
    "Timesheets cannot be created more than 0 month(s) in the future"
    ]

def get_log_severity_or_status(item):
    return "Error" if item["timesheetGetError"] is not null and item["timesheetGetError"]["notifications"] is not null and \
        " | ".join(list(map(lambda errors: errors["displayText"], item["timesheetGetError"]["notifications"]))) \
        is not null and " | ".join(list(map(lambda errors: errors["displayText"], item["timesheetGetError"]["notifications"]))) != '' and \
        all(text not in " | ".join(list(map(lambda errors: errors["displayText"], item["timesheetGetError"]["notifications"]))) for text in exceptions_list) \
        else ("Success" if item["timesheetForDate"] is not null and item["timesheetForDate"]["timesheet"]["uri"] is not null
        and item["timesheetForDate"]["timesheet"]["uri"] != '' else "Exception")


def get_log_details(item):
    return " | ".join(list(map(lambda errors: errors["displayText"], item["timesheetGetError"]["notifications"]))) if item["timesheetGetError"] \
        is not null and item["timesheetGetError"]["notifications"] is not null \
        and " | ".join(list(map(lambda errors: errors["displayText"], item["timesheetGetError"]["notifications"]))) is not null \
        and " | ".join(list(map(lambda errors: errors["displayText"], item["timesheetGetError"]["notifications"]))) != '' \
        else ("Created Successfully" if item["timesheetForDate"] is not null and item["timesheetForDate"]["timesheet"]["uri"] is not null
              and item["timesheetForDate"]["timesheet"]["uri"] != '' else "Timesheet template not assigned to the user")
