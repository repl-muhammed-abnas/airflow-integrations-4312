import datetime as dt
import pytz


def get_europe_paris_time_now():
    return str((dt.datetime.now(pytz.timezone("Europe/Paris"))).strftime("%Y-%m-%dT%H:%M:%S"))


def get_report_start_end_date(time_zone):
    return str((dt.datetime.now(pytz.timezone(time_zone)) - dt.timedelta(1)).strftime("%m/%d/%Y"))
