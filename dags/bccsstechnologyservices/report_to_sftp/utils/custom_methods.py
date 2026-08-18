import pendulum

def logging_details(time_zone):
    current_time = pendulum.now(time_zone)
    return {
        "dag_run_start_time": str(current_time),
        "dag_start_time": current_time.subtract(days=1).strftime("%m_%d_%YT%H_%M")
    }
    