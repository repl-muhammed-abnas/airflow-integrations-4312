import pendulum

def logging_details(time_zone):
    current_time = pendulum.now(time_zone)
    return {
        "dag_run_start_time": str(current_time),
        "jobdateformatted": current_time.strftime("%m_%d_%Y"),
        "time_zone": time_zone,
        "dag_start_time": current_time.strftime("%Y%m%d_%H%M%S")
    }
    