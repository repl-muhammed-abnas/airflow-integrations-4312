from datetime import datetime
from pendulum import now
from wipro.annual_leave_balance_transfer_spain import config
import rail

null = None


def get_split_date(date_value, split_type='str'):
    if date_value and isinstance(date_value, str):
        date_value = datetime.strptime(date_value, config.DATE_DEFAULT_FORMAT)
    if split_type == 'int':
        return {
            'day': date_value.day,
            'month': date_value.month,
            'year': date_value.year
        }
    return {
        'day': date_value.strftime("%d"),
        'month': date_value.strftime("%m"),
        'year': date_value.strftime("%Y")
    }

def get_expire_on_date():
    date_now = now(tz=config.time_zone)
    return {
        'day': 1,
        'month': 4,
        'year': date_now.year
    }

def timeoff_type_disabled_or_not_assigned_check(dag_run):
    find_timeoff_type_in_user_details = rail.find_first_by_attr_and_get_attr(rail.result(
        "get_user_details")["timeoffpolicies"], 'timeOffType.uri', dag_run.conf['timeoff_type_uris']['timeoff_uri_to_transfer_balance_into'], 'isTimeOffAllowedAgainstThisTimeOffType', null)
    if find_timeoff_type_in_user_details == null:
        return {
            'check': True,
            'details': f"The required time off type {dag_run.conf['timeoff_type_name_for_transferring_balance']} is not assigned to the user"
        }
    elif find_timeoff_type_in_user_details != null:
        if find_timeoff_type_in_user_details == False:
            return {
                'check': True,
                'details': f"Time off bookings for the required time off type {dag_run.conf['timeoff_type_name_for_transferring_balance']} are disabled for user"
            }
    return {
        'check': False,
        'details': ""
    }
