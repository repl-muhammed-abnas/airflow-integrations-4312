import functools
import pendulum
import rail
from dateutil.relativedelta import relativedelta

null = None

def get_logging_details(config):
    today = pendulum.now(config.time_zone)
    prev_2_hours_datetime = (today - relativedelta(hours=2, minute=0, second=0, microsecond=0)).isoformat()
    return {
        "time_zone": config.time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "log_filename": 'Timeoff_Sync_HIBOB_to_Replicon_Logs_' + today.strftime("%Y%m%d_%H%M%S") + '.csv',
        "prev_2_hours_encoded": prev_2_hours_datetime.replace(":", "%3A").replace("+", "%2B")
    }

def check_timeoff_type_assigned_to_user(dag_run):
    user_timeoff_policy_data = rail.result("get_user_info")["timeOffTypePolicySummary"]["policiesByTimeOffType"]
    return rail.find_first_by_attr_and_get_attr(user_timeoff_policy_data, "timeOffType.name", dag_run.conf["booking_data"]["policyTypeDisplayName"])

@functools.lru_cache(maxsize=128)
def get_all_user_data():
    return rail.result("get_all_users_from_hibob")

def get_actual_employee_id(unique_id):
    return rail.find_first_by_attr_and_get_attr(get_all_user_data(), "unique_id", unique_id, "employee_id")
