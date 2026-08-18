import rail

def check_decimal_workdays_equals_duration(dag_run, user_timeoff_details, duration):
    return bool(rail.find_first_by_attr_and_get_attr(rail.result(user_timeoff_details),
        "timeOffType.name", dag_run.conf["timeoff_type"], "totalDuration.decimalWorkdays") == duration)

def check_cal_day_duration_equals_timeoffhrs(dag_run, user_timeoff_details, hours, minutes):
    return bool(rail.find_first_by_attr_and_get_attr(rail.result(user_timeoff_details),
                "timeOffType.name", dag_run.conf["timeoff_type"], hours) == int(dag_run.conf["timeoff_hrs"]["hours"]) and
                    rail.find_first_by_attr_and_get_attr(rail.result(user_timeoff_details),
                        "timeOffType.name", dag_run.conf["timeoff_type"], minutes) == int(dag_run.conf["timeoff_hrs"]["minutes"]))

def check_if_timeoff_present(dag_run):
    if dag_run.conf["type"] == "F":
        return bool(check_decimal_workdays_equals_duration(dag_run, "get_time_off_details_for_user_and_date_range_1",
            int(float(dag_run.conf["duration"]))))

    if dag_run.conf["type"] == "P":
        return bool(check_decimal_workdays_equals_duration(dag_run, "get_time_off_details_for_user_and_date_range_1",
            float(dag_run.conf["duration"])))

    if dag_run.conf["type"] == "N":
        return check_cal_day_duration_equals_timeoffhrs(dag_run, "get_time_off_details_for_user_and_date_range_1",
            "totalDuration.hours", "totalDuration.minutes")

    return False

def check_if_timeoff_booking_with_hours_mins_present_to_delete(dag_run):
    return check_cal_day_duration_equals_timeoffhrs(dag_run, "get_time_off_details_for_user_and_date_range_2",
            "totalDuration.calendarDayDuration.hours", "totalDuration.calendarDayDuration.minutes")

def check_if_timeoff_booking_with_decimal_workdays_present_to_delete(dag_run):
    return bool(check_decimal_workdays_equals_duration(dag_run, "get_time_off_details_for_user_and_date_range_2",
        int(float(dag_run.conf["duration"])))
            or check_decimal_workdays_equals_duration(dag_run, "get_time_off_details_for_user_and_date_range_2",
                float(dag_run.conf["duration"])))
