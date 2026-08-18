import json
from math import ceil
import rail
from wipro.annual_leave_balance_transfer_france.utils import python_callable
from wipro.annual_leave_balance_transfer_france.config import ANNUAL_LEAVE, ANNUAL_LEAVE_ACCRUED

null = None

def round_up_to_next(number):
    return ceil(float(number))

def get_final_policyset(dag_run, timeoff_uri, default_policy_task_id, timeoff_type_from):
        get_user_timeoff_policysetschedule = rail.find_first_by_attr_and_get_attr(rail.result(
            "get_user_details")["timeoffpolicies"], 'timeOffType.uri', timeoff_uri, 'policySetSchedule', [])

        user_timeoff_policysetschedule = json.loads(json.dumps(get_user_timeoff_policysetschedule, ensure_ascii=False).replace('"null"', '"effective"').replace(
            '"script"', '"scriptTarget"'))
        default_policyset_for_0_offset = rail.find_first_by_attr_and_get_attr(rail.load_json_artifact(
            dag_run.conf[default_policy_task_id]), 'startOffset.offsetValue', 0, 'policySet')
        
        timeoff_type_data = rail.result('get_timeoff_type_and_balance_to_transfer')
        balance_to_transfer = timeoff_type_data[timeoff_type_from]
        if timeoff_type_from == ANNUAL_LEAVE_ACCRUED and float(timeoff_type_data[ANNUAL_LEAVE]) < 0:
            balance_to_transfer = float(balance_to_transfer) + float(timeoff_type_data[ANNUAL_LEAVE])
            balance_to_transfer = min(balance_to_transfer, 25)

        starting_balance_script_with_0_balance = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": 0.0}})
        modified_script_with_required_starting_balance = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": round_up_to_next(balance_to_transfer)}})

        policyset_json = json.dumps(
            default_policyset_for_0_offset, ensure_ascii=False)

        policyset_to_add = json.loads(policyset_json.replace(
            starting_balance_script_with_0_balance, modified_script_with_required_starting_balance).replace('"null"', '"effective"').replace(
            '"script"', '"scriptTarget"'))

        user_timeoff_policysetschedule.append({
            "description": "Effective on - " + dag_run.conf['new_policy_effective_date'],
            "effectiveDate": python_callable.get_split_date(dag_run.conf['new_policy_effective_date'], 'int'),
            "policySet": policyset_to_add
        })
        return user_timeoff_policysetschedule


def get_report_parameters():
    # Cache all required rail.result calls
    required_filters = rail.result('get_required_filters')
    timeoff_type_uris = rail.result("get_required_timeoff_type_uris")['from']
    log_dag_run_details = rail.result('dag_run_log_time_info')
    country_service_center_uri = rail.result("get_country_servicecenter_uri").split(":")[-1]
    report_uri = rail.result('get_report_details')['uri']

    as_of_date_filter_uri = required_filters['as_of_date_filter_uri']
    timeoff_type_filter_uri = required_filters['timeoff_type_filter_uri']
    country_service_centre_filter_uri = required_filters['country_service_centre_filter_uri']
    report_run_date = log_dag_run_details['report_run_date']

    # List of time off type keys in the order required by the business logic
    timeoff_type_keys = [
        'timeoff_annual_leave_uri',
        'timeoff_annual_leave_accrued_uri',
        'timeoff_annual_leave_seniority_days_uri',
        'timeoff_annual_leave_rtt_uri',
        'timeoff_annual_leave_rtt_for_forfait_jours_uri'
    ]

    filter_values = [
        {"reportFilterUri": as_of_date_filter_uri, "value": "DateRange"},
        {"reportFilterUri": as_of_date_filter_uri, "value": report_run_date},
        {"reportFilterUri": as_of_date_filter_uri, "value": report_run_date},
    ]

    # Add time off type filters in required sequence
    for key in timeoff_type_keys:
        filter_values.append({
            "reportFilterUri": timeoff_type_filter_uri,
            "value": timeoff_type_uris[key].split(":")[-1]
        })

    filter_values.append({
        "reportFilterUri": country_service_centre_filter_uri,
        "value": country_service_center_uri
    })

    return {
        "reportParameters": [
            {
                "reportUri": report_uri,
                "filterValues": filter_values,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }
