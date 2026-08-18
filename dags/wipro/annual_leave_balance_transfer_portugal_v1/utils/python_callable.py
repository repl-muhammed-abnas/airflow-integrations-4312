from datetime import datetime
from wipro.annual_leave_balance_transfer_portugal_v1 import config

null = None


def get_split_date(date_value, split_type='str'):
    if str(type(date_value)) == "<class 'str'>":
        date_value = datetime.strptime(date_value, config.DATE_DEFAULT_FORMAT)
    if split_type == 'int':
        return {
            'day': int(date_value.strftime("%d")),
            'month': int(date_value.strftime("%m")),
            'year': int(date_value.strftime("%Y"))
        }
    return {
        'day': date_value.strftime("%d"),
        'month': date_value.strftime("%m"),
        'year': date_value.strftime("%Y")
    }
