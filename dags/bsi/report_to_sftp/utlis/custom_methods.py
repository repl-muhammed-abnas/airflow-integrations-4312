import pendulum

def logging_details(time_zone):
    current_time = pendulum.now(time_zone)
    return {
        "dag_start_time": current_time.strftime("%Y-%m-%d"),
        "dag_start_time_weekly_monthly": current_time.strftime("%m%d%Y")
    }
    