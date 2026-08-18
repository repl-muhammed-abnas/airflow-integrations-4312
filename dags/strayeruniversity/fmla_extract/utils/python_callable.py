from datetime import datetime
import pytz
import pendulum

def logging_details(config):
    return {
        "dag_run_start_time": str(pendulum.now(config.time_zone).strftime("%Y-%m-%d %H:%M:%S %z")),
    }

def get_file_last_modified_time(dag_run, time_zone):
    utc_tz = pytz.timezone('UTC')
    localized_timestamp = utc_tz.localize(datetime.strptime(dag_run.conf['item']['modified_time'], "%Y%m%d%H%M%S"))
    return (localized_timestamp.astimezone(pytz.timezone(time_zone))).strftime("%d%m%Y%H%M%S")
