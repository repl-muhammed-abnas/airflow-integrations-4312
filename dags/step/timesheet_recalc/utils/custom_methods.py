from datetime import timedelta
import pendulum

def logging_details(time_zone):
    current_time = pendulum.now(time_zone)
    return {
        "timerange_start_time": (current_time - timedelta(days=24)).strftime("%m/%d/%Y"),
        "timerange_end_time": (current_time - timedelta(days=4)).strftime("%m/%d/%Y")
    }
    