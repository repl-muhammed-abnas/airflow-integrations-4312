from datetime import datetime
import pendulum
from dateutil.relativedelta import relativedelta
import rail

null = None
REPORT_DATE_FORMAT = "%d/%m/%Y"
EXPORT_DATE_FORMAT = "%Y-%m-%d"
REPORT_TIME_FORMAT = "%H:%M:%S"
EXPORT_TIME_FORMAT = "%H:%M"
REPORT_FILTER_DATE_FORMAT = "%m/%d/%Y"
FILENAME_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"

def get_generate_calendar_dates(dag_run):
    current_date = datetime.strptime(dag_run.conf["export_start_date"], REPORT_FILTER_DATE_FORMAT)
    end_dated = datetime.strptime(dag_run.conf["export_end_date"], REPORT_FILTER_DATE_FORMAT)
    dates_list = []
    while current_date <= end_dated:
        dates_list.append({
            "day_num": str(current_date.day),
            "date": current_date.strftime(REPORT_DATE_FORMAT),
            "week_day": current_date.strftime("%A"),
            "week_number": str(current_date.isocalendar().week)
        })
        current_date += relativedelta(days=1)
    return dates_list

def get_date_obj(date_string, date_format):
    return datetime.strptime(date_string, date_format)

def get_shift_data_csv_rows(item):
    if not item:
        return []
    hours_mins = str(item['no_of_hours']).split('.')
    return [
        item['local_id'],
        item['card_number'],
        get_date_obj(item['shift_date'], REPORT_DATE_FORMAT).strftime(EXPORT_DATE_FORMAT),
        get_date_obj(item['shift_start_time'], REPORT_TIME_FORMAT).strftime(EXPORT_TIME_FORMAT),
        get_date_obj(item['shift_end_time'], REPORT_TIME_FORMAT).strftime(EXPORT_TIME_FORMAT),
        f"{hours_mins[0].zfill(2)}.{hours_mins[-1]}",
        item['type_of_shift']
    ]

def get_holiday_uri(response):
    if response:
        holiday_uri = rail.find_first_by_attr_and_get_attr(response, "displayText", "Poland Holiday Calendar", "uri")
        if holiday_uri:
            return holiday_uri
    raise Exception('Holiday Calendar "Poland Holiday Calendar" not available in Replicon')

def get_holidays_list(response):
    return '"' + '","'.join(list(map(lambda holiday_list: datetime(holiday_list["date"]["year"],
        holiday_list["date"]["month"], holiday_list["date"]["day"]).strftime(REPORT_DATE_FORMAT), response))) + '"'

def get_shifts_date_range_json(time_zone, no_of_months_shift_data_to_export, current_month_filename_prefix, future_months_filename_prefix):
    today = pendulum.now(time_zone)
    current_time = today.strftime(FILENAME_TIMESTAMP_FORMAT)
    no_of_months_to_export = no_of_months_shift_data_to_export
    months_date_range_list = list(map(lambda month_num: {
        "start_date": (today+relativedelta(months=month_num, day=1)).strftime(REPORT_FILTER_DATE_FORMAT),
        "end_date": (today+relativedelta(months=month_num, day=31)).strftime(REPORT_FILTER_DATE_FORMAT)
    }, range(0, no_of_months_to_export)))

    return list(map(lambda daterange: {
        "export_filename": f'{current_month_filename_prefix}_{current_time}.csv'
            if today.month == datetime.strptime(daterange['start_date'], REPORT_FILTER_DATE_FORMAT).month
                else (future_months_filename_prefix + "_" + datetime.strptime(daterange["start_date"],
                    REPORT_FILTER_DATE_FORMAT).strftime("%b").upper() + "_" + current_time + ".csv"),
        "export_start_date": daterange["start_date"],
        "export_end_date": daterange["end_date"],
        "export_month": datetime.strptime(daterange["start_date"], REPORT_FILTER_DATE_FORMAT).strftime("%B")
    }, months_date_range_list))
