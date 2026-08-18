from datetime import datetime
from pendulum import now
import rail


def get_email_details_callable(time_zone):
    _now = now(time_zone)
    return {
        "job_end_time": _now.isoformat(),
        "job_duration": (((_now - datetime.strptime(rail.result('log_job_start_time'), "%Y-%m-%dT%H:%M:%S%z")).seconds)//60),
        "log_timestamp": _now.strftime("%Y%m%dT%H%M%S"),
        "email_timestamp": _now.isoformat(),
        "log_file_name": f"Log_{_now.strftime('%Y%m%dT%H%M%S')}.csv"
    }
