from datetime import timedelta
import pendulum
import rail
from dateutil.relativedelta import relativedelta
from airflow.models import Variable

null = None

def get_logging_details(config):
    today = pendulum.now(config.time_zone)
    lookup_timestamp_value = Variable.get(
        config.lookup_log_timestamp_var, default_var=None)
    prev_3_hours_datetime = (today - relativedelta(hours=3, minute=0, second=0, microsecond=0)).isoformat()

    Variable.set(config.lookup_log_timestamp_var, 
                    (today + timedelta(seconds=1)).isoformat())
    
    look_back_timestamp = lookup_timestamp_value if lookup_timestamp_value else prev_3_hours_datetime

    return {
        "time_zone": config.time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "log_filename": 'Logs_Youviewtv_Timeoff_Sync_' + today.strftime("%Y%m%d_%H%M%S") + '.csv',
        "prev_3_hours_encoded": look_back_timestamp.replace(":", "%3A").replace("+", "%2B")
    }

def check_timeoff_type_assigned_to_user(dag_run):
    user_timeoff_policy_data = rail.result("get_user_info")["timeOffTypePolicySummary"]["policiesByTimeOffType"]
    return rail.find_first_by_attr_and_get_attr(user_timeoff_policy_data, "timeOffType.name","Absence")
