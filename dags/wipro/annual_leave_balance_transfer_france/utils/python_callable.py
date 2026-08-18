from datetime import datetime
from pendulum import now
from functools import lru_cache
from wipro.annual_leave_balance_transfer_france import config
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
    message = []
    invalid_timeoff_types = {}
    msg = rail.smartjoin_by_delim(dag_run.conf['timeoff_type_uris']['missing_types'], ";")
    if msg:
        msg = f"The required time off type(s) {msg} is not configured or not enabled."
        message = [msg]
    current_timeoff_policies = {item['timeOffType']['uri']: item for item in rail.result("get_user_details")["timeoffpolicies"]}
    for _, uri in dag_run.conf['timeoff_type_uris']['into'].items():
        if uri not in current_timeoff_policies:
            message.append(
                f"The required time off type {dag_run.conf['timeoff_type_uris']['uri_to_name'][uri]} is not assigned to the user"
            )
            invalid_timeoff_types[uri] = dag_run.conf['timeoff_type_uris']['uri_to_name'][uri]
        elif uri in current_timeoff_policies and not current_timeoff_policies[uri]['isTimeOffAllowedAgainstThisTimeOffType']:
            message.append(
                f"Time off bookings for the required time off type {dag_run.conf['timeoff_type_uris']['uri_to_name'][uri]} are disabled for user"
            )
            invalid_timeoff_types[uri] = dag_run.conf['timeoff_type_uris']['uri_to_name'][uri]
    return {
        'is_invalid': False if not message else True,
        'details': "" if not message else rail.smartjoin_by_delim(message, ";"),
        'invalid_timeoff_types': invalid_timeoff_types
    }


@lru_cache(maxsize=8)
def get_nonzero_data():
    return rail.load_all_records(rail.result('query_nonzero_balance_records'))

def get_balance_to_transfer():
    resp = {}
    for item in get_nonzero_data():
        resp[item['timeoff_type']] = item['timeoff_balance']
    return resp