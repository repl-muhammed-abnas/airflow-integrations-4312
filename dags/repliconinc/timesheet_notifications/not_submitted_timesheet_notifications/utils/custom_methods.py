from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def get_last_saturday():
    """Get last Saturday's date for repliconpinc timesheet period."""
    today = datetime.today()
    last_saturday = today - timedelta(days=(today.weekday() + 2) % 7)
    return last_saturday.strftime("%m/%d/%Y")


def get_deltekps_period_end_date():
    """
    Get timesheet period end date for deltekps.
    If today > 15: end date = 15th of current month
    If today <= 15: end date = last day of previous month
    """
    today = datetime.today()
    if today.day > 15:
        end_date = today.replace(day=15)
    else:
        end_date = today + relativedelta(months=-1, day=31)
    return end_date.strftime("%m/%d/%Y")


def get_timesheet_period_end_date(company_key):
    """Get timesheet period end date based on company_key."""
    if company_key == "deltekps":
        return get_deltekps_period_end_date()
    else:
        # Default to last Saturday for repliconpinc and others
        return get_last_saturday()


def get_timesheet_report_payload(report_uri, timesheet_period_filter_uri, company_key, period_start_date):
    """Generate payload for timesheet report details API call."""
    period_end_date = get_timesheet_period_end_date(company_key)
    return {
        "reportUri": report_uri,
        "filterValues": [
            {
                "reportFilterUri": timesheet_period_filter_uri,
                "value": None
            },
            {
                "reportFilterUri": timesheet_period_filter_uri,
                "value": period_start_date
            },
            {
                "reportFilterUri": timesheet_period_filter_uri,
                "value": period_end_date
            }
        ],
        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
    }
