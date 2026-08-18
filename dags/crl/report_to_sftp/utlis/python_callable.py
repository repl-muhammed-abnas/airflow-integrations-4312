import pendulum

def get_csv_filename(company_key, time_zone, report_name, report_type):
    current_time = pendulum.now(time_zone)
    code = report_name.split('-')[-1].strip().replace(" ", "_")
    filename = f"{company_key}_{report_type}_{code}_{current_time.strftime('%d%m%Y_%H%M%S')}.csv"
    return filename
