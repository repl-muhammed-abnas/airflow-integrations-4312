from datetime import datetime
from functools import lru_cache
from collections import defaultdict
from wipro.annual_leave_balance_transfer_switzerland import config
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

def can_transfer_timeoff_balance(dag_run):
    is_timeoff_allowed = rail.find_first_by_attr_and_get_attr(rail.result(
        "get_user_details")["timeoffpolicies"], 'timeOffType.uri',
        dag_run.conf['timeoff_type_uri_for_transferring_balance_into'],
        'isTimeOffAllowedAgainstThisTimeOffType', null)
    if is_timeoff_allowed == null:
        return {
            'check': False,
            'details': "Time off not assigned"
        }
    elif is_timeoff_allowed != null:
        if is_timeoff_allowed == False:
            return {
                'check': True,
                'details': f"Time off bookings are disabled for the required time off type {dag_run.conf['timeoff_type_name_for_transferring_balance_into']} for user"
            }
    return {
        'check': False,
        'details': ""
    }

def get_all_time_off_type(dag_run, response):
    assigned_timeoff = list(map(lambda x: x['uri'], response))
    assigned_timeoff.append(dag_run.conf['timeoff_type_uri_for_transferring_balance_into'])
    return assigned_timeoff

def get_required_timeoff_type_uris(config, response):
    required_timeoff_types = defaultdict(dict)
    required_timeoff_types = {item['displayText']: item for item in response if item['displayText'] in config.REQUIRED_TIMEOFF_TYPES}
    uri_to_name = {item['uri']: item['displayText'] for item in response if item['displayText'] in config.REQUIRED_TIMEOFF_TYPES}
    missing_types = [name for name in config.REQUIRED_TIMEOFF_TYPES if name not in required_timeoff_types]
    return {
        'from': {
            'timeoff_annual_leave_uri': required_timeoff_types[config.ANNUAL_LEAVE].get('uri'),
            'timeoff_annual_leave_parttime_uri': required_timeoff_types[config.ANNUAL_LEAVE_PARTTIME].get('uri'),
            'timeoff_annual_leaves_assignees_uri': required_timeoff_types[config.ANNUAL_LEAVES_ASSIGNEES].get('uri'),
        },
        'into': {
            'timeoff_annual_leave_additional_uri': required_timeoff_types[config.ANNUAL_LEAVE_ADDITIONAL].get('uri')
        },
        'uri_to_name': uri_to_name,
        'missing_types': missing_types
    }

def get_report_parameters():
    enabled_filters = rail.result('get_report_details')['filterConfiguration']['enabledFilters']
    report_run_date = rail.result('dag_run_log_time_info')['report_run_date']
    timeoff_uris = rail.result("log_get_required_timeoff_type_uris")
    service_center_uri = rail.result("get_required_country_service_center_uri").split(":")[-1]

    # Pre-fetch filter URIs to avoid repeated lookups
    as_of_date_filter_uri = rail.find_first_by_attr_and_get_attr(enabled_filters, 'displayText', 'AsOfDateFilter', 'uri')
    timeoff_type_filter_uri = rail.find_first_by_attr_and_get_attr(enabled_filters, 'displayText', 'TimeOffTypeFilter', 'uri')
    service_center_filter_uri = rail.find_first_by_attr_and_get_attr(enabled_filters, 'displayText', 'CurrentServiceCenterFilter', 'uri')

    # Prepare all timeoff type values
    timeoff_values = [
        timeoff_uris['from']['timeoff_annual_leave_uri'],
        timeoff_uris['from']['timeoff_annual_leave_parttime_uri'],
        timeoff_uris['from']['timeoff_annual_leaves_assignees_uri'],
        timeoff_uris['into']['timeoff_annual_leave_additional_uri'],
    ]
    timeoff_values = [uri.split(":")[-1] for uri in timeoff_values]

    filter_values = [
        {"reportFilterUri": as_of_date_filter_uri, "value": "DateRange"},
        {"reportFilterUri": as_of_date_filter_uri, "value": report_run_date},
        {"reportFilterUri": as_of_date_filter_uri, "value": report_run_date},
        *[
            {"reportFilterUri": timeoff_type_filter_uri, "value": value}
            for value in timeoff_values
        ],
        {"reportFilterUri": service_center_filter_uri, "value": service_center_uri},
    ]

    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')['uri'],
                "filterValues": filter_values,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

@lru_cache(maxsize=8)
def get_nonzero_data():
    return rail.load_all_records(rail.result('query_nonzero_balance_records'))

def get_balance_to_transfer_with_limits(config):
    nonzero_data = get_nonzero_data()

    resp = {item['timeoff_type']: item['timeoff_balance'] for item in nonzero_data}
    key = next((k for k in resp if k != config.ANNUAL_LEAVE_ADDITIONAL), None)

    if key is None:
        return {
            "from": False,
            "balance": 0,
            "resp": resp
        }
    balance = float(resp[key])
    original_balance = resp[key]

    return {
        "from": key,
        "balance": str(balance),
        "original_balance": original_balance,
        "resp": resp
    }

def get_balance_to_transfer(config):
    return get_balance_to_transfer_with_limits(config)


def get_all_from_timeoff_types():
    return {
        "from":"'"+"','".join([config.ANNUAL_LEAVE, config.ANNUAL_LEAVE_PARTTIME, config.ANNUAL_LEAVES_ASSIGNEES])+"'",
        "into": f"'{config.ANNUAL_LEAVE_ADDITIONAL}'"
        }

def get_transfer_success_message(dag_conf, transfer_data):
    balance_transferred = transfer_data['balance']
    from_type = transfer_data['from']
    to_type = dag_conf['timeoff_type_name_for_transferring_balance_into']
    
    message = f"Balance of {balance_transferred} transferred from {from_type} to {to_type} is successful."
    return message
