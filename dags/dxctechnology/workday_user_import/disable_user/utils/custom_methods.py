from datetime import datetime, timedelta, date
from decimal import Decimal
from json import dumps, loads
from dateutil.parser import parse as date_parser
from rail import result


def exp_to_decimal_best(exp_str):
    try:
        decimal_num = Decimal(str(exp_str))
        result = format(decimal_num.quantize(Decimal('1.00')), '.2f')
        return result
    except Exception as e:
        return f"Error: {str(e)}"


def get_end_date_to_use(dag_run):
    timeoff_details = result("get_timeoff_details")[0]
    location = result("get_users_effective_group_membership")['locations'][0] \
        if result("get_users_effective_group_membership")['locations'] else {}

    _date = date_parser(dag_run.conf['end_date'])

    if timeoff_details['name'].startswith("[AUS]"):
        _date += timedelta(days=1)
    elif not location.get('location', {}).get('parent', {}):
        return _date
    elif location.get('location', {}).get('parent', {}).get('location', {}).get('displayText', "") == "Australia":
        _date += timedelta(days=1)
    else:
        pass  # initialized the value

    return _date

def convert_json_date_to_date(json_date):
    return date(day=json_date['day'], month=json_date['month'], year=json_date['year'])

def add_new_policy_line(dag_run):
    _date = get_end_date_to_use(dag_run)
    return {
        "effectiveDate": {
            "day": _date.day,
            "month": _date.month,
            "year": _date.year
        },
        "description": f"Added by Integration on {_date.strftime('%d-%m-%Y')}",
        "policySet": {
            "timeOffBalanceEventScripts": [{
                "additionalParameters": [{
                    "keyUri": "urn:replicon:script-key:parameter:amount",
                    "value": {
                        "number": exp_to_decimal_best(str(result('get_user_timeoff_balance_summary')["timeRemaining"]))
                    }
                }],
                "script": {
                    "description": "Set initial balance for the first day of a policy",
                    "name": "Starting Balance Set To",
                    "uri": dag_run.conf['starting_balance_set_to_uri']
                }
            }],
            "timeOffValidationScripts": [{
                "additionalParameters": [{
                    "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                    "value": {
                        "number": "0"
                    }
                }],
                "script": {
                    "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                    "name": "Prevent balance overdraw",
                    "uri": dag_run.conf['prevent_balance_overdraw_uri']
                }
            }]
        }
    }


def get_timeoff_polices_to_assign_callable(dag_run):
    policies = loads(dag_run.conf['policy_set'])
    user_end_date: datetime = convert_json_date_to_date(
        dag_run.conf['user_end_date_json'])
    existing_policy = list(filter(
        lambda policy: convert_json_date_to_date(
            policy['effectiveDate']) < user_end_date,
        policies
    ))

    existing_policy.append(add_new_policy_line(dag_run))

    return existing_policy


def format_timeoff_polices_to_assign_callable():
    return dumps(result("get_timeoff_polices_to_assign")
                ).replace("/null/", "\"effective\""
                ).replace("\"script\"", "\"scriptTarget\""
                ).replace('":{"additionalParameters', '":[{"additionalParameters'
                ).replace(':{"keyUri"', ':[{"keyUri"'
                ).replace('}},"scriptTarget"', '}}],"scriptTarget"'
                ).replace('}},"timeOffValidationScripts', '}}],"timeOffValidationScripts'
                ).replace('}}},"description', '}}]},"description')
