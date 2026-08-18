from datetime import datetime, timedelta, date
from decimal import Decimal
from json import dumps, loads
from dateutil.parser import parse as date_parser
from rail import result
import rail

INPUT_DATE_FORMAT = "%Y-%d-%m"
TIMEOFF_CONFIG = {
    'parttime': {
        'UK': {
            'annual': '[UK] P/T Annual Leave Hrs',
            'bought': '[UK] P/T Bought A/L Hrs',
            'sold': '[UK] P/T Sold A/L Hrs',
            'holiday': '[UK] P/T Public Holiday Hrs'
        },
        'IRL': {
            'annual': '[IRL] P/T Annual Leave Hrs',
            'bought': '[IRL] P/T Bought A/L Hrs',
            'sold': '[IRL] P/T Sold A/L Hrs',
            'holiday': '[IRL] P/T Public Holiday Hrs'
        },
    },
    'fulltime': {
        'UK': {
            'annual': '[UK] Annual Leave',
            'bought': '[UK] Bought A/L',
            'sold': '[UK] Sold A/L',
        },
        'IRL': {
            'annual': '[IRL] Annual Leave',
            'bought': '[IRL] Bought A/L',
            'sold': '[IRL] Sold A/L',
        },
    },
}

def get_report_filter():
    filters = []
    entry_date_filter_uri = rail.find_first_by_attr_and_get_attr(rail.result(
        'get_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'TimesheetPeriodFilter', 'uri')
    filters.append({
        'reportFilterUri': entry_date_filter_uri,
        'value': None
    })
    filters.append({
        'reportFilterUri': entry_date_filter_uri,
        'value': datetime.now().strftime('%m/%d/%Y')
    })
    filters.append({
        'reportFilterUri': entry_date_filter_uri,
        'value': None
    })
    return filters

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

def exp_to_decimal_best(exp_str):
    try:
        decimal_num = Decimal(str(exp_str))
        # Format as fixed-point with 2 decimal places
        result = format(decimal_num.quantize(Decimal('1.00')), '.2f')
        return result
    except Exception as e:
        return f"Error: {str(e)}"


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

def prepare_timeoff_uris_func(timeoffs, end_date_parts):
    # Reconstruct end_date from parts
    end_date_str = f"{end_date_parts['day']}-{end_date_parts['month']}-{end_date_parts['year']}"
    end_date = datetime.strptime(end_date_str, "%d-%m-%Y")
    
    update_timeoffs = []
    delete_timeoffs = []
    
    for timeoff in timeoffs:
        try:
            start_date = datetime.strptime(timeoff['startdate'], "%d %B %Y")
            timeoff_end_date = datetime.strptime(timeoff['enddate'], "%d %B %Y")
            
            if timeoff_end_date >= end_date and start_date < end_date:
                # Case 1: Timeoff extends beyond end_date but starts before/on end_date -> UPDATE
                update_timeoffs.append({
                    "uri": timeoff['timeoffuris'],
                    "start_date": timeoff['startdate'],
                    "end_date": (end_date - timedelta(days=1)).strftime("%d-%m-%Y"),
                    "timeofftype_uri": timeoff['timeofftype']
                })
            elif start_date >= end_date:
                # Case 2: Timeoff starts after end_date -> DELETE
                delete_timeoffs.append({
                    "uri": timeoff['timeoffuris']
                })
            # Case 3: Timeoff fully within allowed range -> IGNORE
            
        except (ValueError, KeyError):
            # If date parsing fails, default to delete for safety
            delete_timeoffs.append({
                "uri": timeoff['timeoffuris']
            })
    
    return {
        "update_timeoffs": update_timeoffs,
        "delete_timeoffs": delete_timeoffs
    }

def get_annual_brought_sold_holiday_leave_list(return_for, country, part_time_full_time, return_type="list"):
    from airflow.exceptions import AirflowException
    if return_for == "country":
        ft = TIMEOFF_CONFIG['fulltime'][country]
        pt = TIMEOFF_CONFIG['parttime'][country]
        if part_time_full_time == 'both':
            if return_type == "json":
                return {
                    "part_time": pt,
                    "full_time": ft
                }
            return [
                ft['annual'], pt['annual'],
                ft['bought'], pt['bought'],
                ft['sold'], pt['sold'],
                pt['holiday']
            ]
        if part_time_full_time == 'parttime':
            if return_type == "json":
                return pt
            return [pt['annual'], pt['bought'], pt['sold'], pt['holiday']]
        if part_time_full_time == 'fulltime':
            if return_type == "json":
                return ft
            return [ft['annual'], ft['bought'], ft['sold']]
        raise ValueError("Invalid part_time_full_time value. Use 'parttime', 'fulltime', or 'both'.")

    if part_time_full_time == "parttime":
        res_data = TIMEOFF_CONFIG[part_time_full_time]
        return (
            [res_data['UK']['annual'], res_data['IRL']['annual']],
            [res_data['UK']['bought'], res_data['IRL']['bought']],
            [res_data['UK']['sold'], res_data['IRL']['sold']],
            [res_data['UK']['holiday'], res_data['IRL']['holiday']]
        )
    if part_time_full_time == "fulltime":
        res_data = TIMEOFF_CONFIG[part_time_full_time]
        return (
            [res_data['UK']['annual'], res_data['IRL']['annual']],
            [res_data['UK']['bought'], res_data['IRL']['bought']],
            [res_data['UK']['sold'], res_data['IRL']['sold']],
        )
    raise AirflowException("Invalid part_time_full_time value. Use 'parttime' or 'fulltime'.")


