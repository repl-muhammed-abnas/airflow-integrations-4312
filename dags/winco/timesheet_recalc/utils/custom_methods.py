import pendulum

def logging_details(time_zone):
    current_time = pendulum.now(time_zone)
    return {
        "timerange_start_time": (current_time).strftime("%Y-%m-%dT%H:%M:%S")
    }
    