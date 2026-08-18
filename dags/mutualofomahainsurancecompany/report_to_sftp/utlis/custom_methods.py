import pendulum

def logging_details(time_zone, report_name):
    current_time = pendulum.now(time_zone)
    return {
        "dag_start_time_file_name": report_name + " " + current_time.strftime("%Y-%m-%d") + ".csv"
    }
