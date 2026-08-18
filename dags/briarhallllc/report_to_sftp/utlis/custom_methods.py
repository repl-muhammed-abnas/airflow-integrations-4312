import pendulum

def logging_details(time_zone):
    current_time = pendulum.now(time_zone)
    return {
        "dag_start_time": current_time.strftime("%d%m%Y%H%M%S"),
        "dag_start_time_file_name": current_time.strftime("%m_%d_%Y")
    }
    