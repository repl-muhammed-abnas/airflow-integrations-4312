import json
import pendulum
import itertools
import rail
from airflow.models import Variable
from rail import get_current_context

null = None


def get_task_state(task_id):
    task_instance = get_current_context()['dag_run'].get_task_instance(task_id)
    return task_instance.current_state() if task_instance else null


def get_logging_details(time_zone, states_optional_holiday_calendars, dag_type):
    today = pendulum.now(time_zone)
    return {
        "time_zone": time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "states_optional_holiday_calendars": json.loads(Variable.get(states_optional_holiday_calendars)) if dag_type != "schedule_logs" else null
    }

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

def get_new_users_artifact_data():
    return list(map(lambda user_data: user_data["properties"],
        list(itertools.chain.from_iterable(list(map(rail.load_all_records,
            rail.result("get_new_users_artifacts")["value"]))))))
