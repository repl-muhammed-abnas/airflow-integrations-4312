from datetime import datetime, timedelta
import calendar
import pytz
from dateutil.relativedelta import relativedelta
null = None


def schedule_interval(timezone):
    timezone_str = pytz.timezone(timezone)
    today = datetime.now(timezone_str)
    last_day = (today + relativedelta(months=1)).replace(day=1) - timedelta(days=1)

    if today.day == (last_day - timedelta(days=1)).day:
        return True

    if calendar.weekday(today.year, today.month, today.day) == 6:
        return True

    return False


def schedule_interval_sandbox(timezone):
    timezone_str = pytz.timezone(timezone)
    today = datetime.now(timezone_str)
    last_day = (today + relativedelta(months=1)).replace(day=1) - timedelta(days=1)

    if today.day == (last_day - timedelta(days=1)).day:
        return True

    if calendar.weekday(today.year, today.month, today.day) == 6:
        return True

    if today.day == last_day.day:
        return True

    return False


def get_dates(timezone):
    timezone_str = pytz.timezone(timezone)
    today = datetime.now(timezone_str)
    return f"{(today + relativedelta(months=-1)).strftime('%m/%d/%Y')}" +" - "+ f"{today.strftime('%m/%d/%Y')}"
