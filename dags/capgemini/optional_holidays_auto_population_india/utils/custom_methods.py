import json
import pendulum
import rail
from airflow.models import Variable
from rail import get_current_context

null = None


def get_task_state(task_id):
    task_instance = get_current_context()['dag_run'].get_task_instance(task_id)
    return task_instance.current_state() if task_instance else null


def get_logging_details(config, dag_type):
    today = pendulum.now(config.time_zone)
    return {
        "time_zone": config.time_zone,
        "process_start_time": today.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
        "states_optional_holiday_calendars": json.loads(Variable.get(config.states_optional_holiday_calendars)) if dag_type != "schedule_logs" else null
    }


def get_unavailable_states():
    location_details = rail.result("get_allowed_location_uris")
    return list(map(lambda data: data["state_name"], filter(lambda data: data["state_uri"] is null, location_details)))


def get_unavailable_holiday_calendars():
    holiday_cal_details = rail.result("get_allowed_location_uris")
    return list(map(lambda data: data["optional_holiday_cal_name"], filter(lambda data: data["optional_holiday_cal_uri"] is null, holiday_cal_details)))


def check_status():
    return bool(get_task_state("put_and_submit_timeoff_booking_for_user").lower() == "failed"
                or get_task_state("put_and_submit_timeoff_booking_for_user").lower() == "success")


def get_failure_reason():
    notifications = rail.result("put_and_submit_timeoff_booking_for_user", key="error")[
        "response"]["json"]["error"]["details"]["notifications"]
    return ",".join(list(map(lambda data: data["displayText"], notifications)))


def check_approval_error():
    error_message = rail.result(
        "approve_timeoff_booking_for_user", key='error')
    return bool((error_message["response"]["json"]["error"]["details"]
                 ["notifications"][0]["displayText"]).lower() == "time off booking has already been approved.") \
        if error_message and error_message["response"] and error_message["response"]["json"]["error"]["details"] else False
