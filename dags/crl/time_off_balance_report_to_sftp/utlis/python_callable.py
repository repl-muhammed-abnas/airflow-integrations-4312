import pendulum

def get_csv_filename(time_zone, report_name):
    current_time = pendulum.now(time_zone)
    filename = f"{report_name}_{current_time.strftime('%Y%m%d')}.csv"
    return filename
