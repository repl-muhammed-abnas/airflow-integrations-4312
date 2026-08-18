from datetime import datetime, timedelta
import pendulum
import rail
from airflow.models import Variable, DagRun

null = None

# pylint:disable = too-many-arguments


def check_start_date_in_daterange(schedule, time_zone, start_month, start_day, end_month, end_day):
    start_date_json = rail.result("get_user_info")[
        "userDetails"]["employmentDateRange"]["startDate"]
    user_start_date = datetime(
        start_date_json["year"], start_date_json["month"], start_date_json["day"])
    current_year = pendulum.now(time_zone).year
    schedule_start_date = datetime(current_year, start_month, start_day)
    schedule_end_date = datetime(current_year, end_month, end_day)
    return {
        "schedule": schedule if schedule_start_date <= user_start_date <= schedule_end_date else null,
        "schedule_start_date": schedule_start_date.strftime("%d-%m-%Y"),
        "schedule_end_date": schedule_end_date.strftime("%d-%m-%Y"),
        "user_start_date": user_start_date.strftime("%d-%m-%Y")
    }


def is_booking_allowed():
    states_holiday_calendar_mapper = rail.result(
        "logging_details")["states_optional_holiday_calendars"]
    for data in states_holiday_calendar_mapper:
        if data["allowed"].lower() == "yes":
            if data["optional_holiday_calendar"] == rail.result("get_all_holiday_calendars")[0]["optional_holiday_calendar_name"]:
                return True
    return False


def get_schedule_on_daterange(config):
    start_date_json = rail.result("get_user_info")[
        "userDetails"]["employmentDateRange"]["startDate"]
    schedule_feb_jun_details = check_start_date_in_daterange("E1", config.time_zone, config.e1_schedule_daterange["start_month"],
            config.e1_schedule_daterange["start_day"], config.e1_schedule_daterange["end_month"], config.e1_schedule_daterange["end_day"])
    schedule_aug_dec_details = check_start_date_in_daterange("E2", config.time_zone, config.e2_schedule_daterange["start_month"],
            config.e2_schedule_daterange["start_day"], config.e2_schedule_daterange["end_month"], config.e2_schedule_daterange["end_day"])
    return schedule_feb_jun_details if schedule_feb_jun_details["schedule"] == "E1" \
        else (schedule_aug_dec_details if schedule_aug_dec_details["schedule"] == "E2" else {
            "schedule": null,
            "user_start_date": f'{start_date_json["day"]}-{start_date_json["month"]}-{start_date_json["year"]}'
        })


def check_if_bookable_date_is_less_than_start_date():
    return datetime.strptime(rail.result("get_bookable_holidays_in_date_range")[0]["holiday_date"], "%d-%m-%Y") \
        < datetime.strptime(rail.result("get_schedule_based_on_daterange")["user_start_date"], "%d-%m-%Y")


def check_if_bookable_date_is_start_date():
    return rail.result("get_bookable_holidays_in_date_range")[0]["holiday_date"] in rail.result("get_schedule_based_on_daterange")["user_start_date"]


def get_dagruns_to_process(time_zone, lookup_log_timestamp_var, lookup_log_timestamp_hours, dag_id):
    current_time = pendulum.now(time_zone)
    lookup_timestamp_value = Variable.get(
        lookup_log_timestamp_var, default_var=None)

    query_execution_start_date = datetime.fromisoformat(lookup_timestamp_value) if lookup_timestamp_value else (
        current_time - timedelta(hours=lookup_log_timestamp_hours))

    dag_runs = []
    execution_dates = []
    for run in DagRun.find(dag_id=dag_id, state='success', execution_start_date=query_execution_start_date):
        execution_dates.append(run.execution_date)
        dag_runs.append(run.id)
    if execution_dates:
        max_execution_date = max(execution_dates)
        Variable.set(lookup_log_timestamp_var,
                     (max_execution_date + timedelta(seconds=1)).isoformat())
    return dag_runs


def get_log_message(log_name):
    if log_name == "no_bookable_holiday":
        return 'No System booked timeoff found in Replicon for date range "' \
            + rail.result("get_schedule_based_on_daterange")["schedule_start_date"] \
            + " - " + rail.result("get_schedule_based_on_daterange")["schedule_end_date"] \
            + '" in holiday calendar "' + \
            rail.result("get_all_holiday_calendars")[
                0]["optional_holiday_calendar_name"] + '"'
    if log_name == "multiple_bookable_holiday":
        return 'Multiple System booked timeoff\'s found in Replicon for date range "' \
            + rail.result("get_schedule_based_on_daterange")["schedule_start_date"] \
            + " - " + rail.result("get_schedule_based_on_daterange")["schedule_end_date"] \
            + '" in holiday calendar "' + \
            rail.result("get_all_holiday_calendars")[
                0]["optional_holiday_calendar_name"] + '"'
    if log_name == "start_date_not_in_range":
        return 'User\'s start date is "' + rail.result("get_schedule_based_on_daterange")["user_start_date"] \
            + '" and not in the scheduled date range'

    return "No details"
