from rail import find_first_by_attr_and_get_attr, set_result
from airflow.exceptions import AirflowException


def get_starting_balance_script_data_handler(response):
    script_uri = find_first_by_attr_and_get_attr(
        response, 'displayText', 'Starting Balance Set To', 'uri', None)
    if script_uri:
        return script_uri
    raise AirflowException("Script `Starting Balance Set To` is not found")


def get_prevent_balance_overdraw_script_data_handler(response):
    script_uri = find_first_by_attr_and_get_attr(
        response, 'displayText', 'Prevent balance overdraw', 'uri', None)
    if script_uri:
        return script_uri
    raise AirflowException("Script `Prevent balance overdraw` is not found")


def get_user_timeoff_type_policy_summary_data_handler(response):
    set_result(key="response", val=response)
    return list(filter(lambda timeoff_policy: timeoff_policy['isTimeOffAllowedAgainstThisTimeOffType'] is True
                       and bool(False if not timeoff_policy[
                           'policySetSchedule'] else timeoff_policy['policySetSchedule'][0].get('effectiveDate', {}
                                                        ).get('day', False)), response['policiesByTimeOffType']))
