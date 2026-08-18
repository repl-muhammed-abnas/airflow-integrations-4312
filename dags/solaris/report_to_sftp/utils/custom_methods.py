import pendulum

def logging_details(time_zone):
    current_time = pendulum.now(time_zone)
    return {
        "dag_start_time": current_time.strftime("%m_%d_%Y_T%H_%M")
    }
    