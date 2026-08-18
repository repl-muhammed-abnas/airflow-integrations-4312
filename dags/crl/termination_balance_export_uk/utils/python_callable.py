import pendulum

def get_time_in_formats(time_zone):
    current_time = pendulum.now(time_zone)
    return {
        "start_time": str(current_time),
        "ymd_format": current_time.strftime("%Y%m%d"),
        "hms_format": current_time.strftime("%H%M%S")
    }