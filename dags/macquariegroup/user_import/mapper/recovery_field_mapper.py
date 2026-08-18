from datetime import datetime
from dateutil.relativedelta import relativedelta


def get_json_date(date: datetime):
    return {
        "day": date.day,
        "month": date.month,
        "year": date.year
    }


recovery_field_mapper = [
    {
        'group': 'Financial Management Group',
        'employee_type': 'FMG/MOD/BIS/CCIR',
        'ts_period': 'Monthly - FMG GFS (24 to 23)',
        'integration_date': get_json_date(datetime.now()),
        'timesheet_period_assignment': get_json_date(datetime.now().replace(day=24) - relativedelta(month=1))
    },
    {
        'group': 'Financial Management Group',
        'employee_type': 'FMG (Non-GFS)',
        'ts_period': 'Monthly - FMG (Non-GFS)',
        'integration_date': get_json_date(datetime.now()),
        'timesheet_period_assignment': get_json_date(datetime.now().replace(day=1))
    },
    {
        'group': 'Risk Management Group',
        'employee_type': 'RMG',
        'ts_period': 'Monthly - RMG',
        'integration_date': get_json_date(datetime.now()),
        'timesheet_period_assignment': get_json_date(datetime.now().replace(day=1))
    }
]
