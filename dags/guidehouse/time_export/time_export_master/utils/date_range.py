from datetime import timedelta
from pendulum import parse as pendulum_parse
from pendulum import timezone as pendulum_timezone


def get_monday_start(date, tz_name="America/New_York"):
    """Get the most recent Monday on or before the given date."""
    tz = pendulum_timezone(tz_name)
    dt = pendulum_parse(str(date)).in_timezone(tz)
    days_since_monday = dt.weekday()
    monday = dt.subtract(days=days_since_monday)
    return monday.date()


def get_saturday_end(date, tz_name="America/New_York"):
    """Get the Saturday on or after the given date (end of week)."""
    tz = pendulum_timezone(tz_name)
    dt = pendulum_parse(str(date)).in_timezone(tz)
    days_until_saturday = (5 - dt.weekday()) % 7
    if days_until_saturday == 0 and dt.weekday() != 5:
        days_until_saturday = 7
    saturday = dt.add(days=days_until_saturday)
    return saturday.date()


def get_hourly_date_window(run_date, tz_name="America/New_York"):
    """
    Return (start_date, end_date) for hourly export.

    Date window: rolling 1 year back from run date.
    Both dates align to Monday start / Saturday end.

    Args:
        run_date: The run date (datetime or date)
        lookback_days: Number of days to look back (default 365)
        tz_name: Timezone name

    Returns:
        Tuple of (start_date, end_date) as date objects
    """
    tz = pendulum_timezone(tz_name)
    if isinstance(run_date, str):
        run_date = pendulum_parse(run_date).in_timezone(tz)
    elif hasattr(run_date, "tzinfo") and run_date.tzinfo is None:
        run_date = tz.convert(run_date)

    lookback_start = run_date.subtract(years=1)
    window_start = get_monday_start(lookback_start, tz_name)
    window_end = get_saturday_end(run_date.subtract(days=1), tz_name)

    return window_start, window_end


def get_daily_date_window(run_date, hourly_start_date, tz_name="America/New_York"):

    tz = pendulum_timezone(tz_name)
    if isinstance(run_date, str):
        run_date = pendulum_parse(run_date).in_timezone(tz)
    elif hasattr(run_date, "tzinfo") and run_date.tzinfo is None:
        run_date = tz.convert(run_date)

    three_years_back = run_date.subtract(years=3)
    window_start = get_monday_start(three_years_back, tz_name)

    if isinstance(hourly_start_date, str):
        hourly_start_date = pendulum_parse(hourly_start_date).date()

    window_end = get_saturday_end(hourly_start_date - timedelta(days=1), tz_name)

    return window_start, window_end


def format_date_for_export(date_obj, format_str="%Y%m%d"):
    """Format date object for export file naming."""
    if isinstance(date_obj, str):
        date_obj = pendulum_parse(date_obj).date()
    return date_obj.strftime(format_str)
