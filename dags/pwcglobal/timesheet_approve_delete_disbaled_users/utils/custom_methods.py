import pendulum

def logging_details(time_zone):
    current_time = pendulum.now(time_zone)
    return {
        "current_date": (current_time).strftime("%Y-%m-%d"),
        "logfilename_date": (current_time).strftime("%Y%m%d%M%S")
    }
    