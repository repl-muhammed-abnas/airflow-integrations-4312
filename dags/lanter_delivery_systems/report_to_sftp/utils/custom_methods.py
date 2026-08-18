from pendulum import now


def logging_details(time_zone):
    current_time = now(time_zone)
    return {
        "dag_start_time": current_time.strftime("%m/%d/%YT%H:%M:%S"),
        "file_date_time": current_time.strftime("%m%d%Y%H%M%S")
    }
