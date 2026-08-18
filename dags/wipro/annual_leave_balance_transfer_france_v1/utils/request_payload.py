import json
from math import ceil
import rail
import uuid
from wipro.annual_leave_balance_transfer_france_v1.utils import python_callable
from wipro.annual_leave_balance_transfer_france_v1.config import (
    ANNUAL_LEAVE, ANNUAL_LEAVE_ACCRUED,
    ANNUAL_LEAVE_RTT, ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS
)

null = None

def round_up_to_next(number):
    return ceil(float(number))

def round_up_to_half(number):
    return ceil(float(number) * 2) / 2

_RTT_TYPES = {ANNUAL_LEAVE_RTT, ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS}

def build_transaction_payload(dag_run, timeoff_uri, timeoff_type_from, timeoff_type_to, add):
    timeoff_type_data = rail.result('get_timeoff_type_and_balance_to_transfer')
    balance_to_transfer = timeoff_type_data[timeoff_type_from] 
    
    if add:
        if timeoff_type_from in _RTT_TYPES:
            final_balance = round_up_to_half(balance_to_transfer)
        else:
            final_balance = round_up_to_next(balance_to_transfer)
    else:
        final_balance = float(balance_to_transfer)
    user_uri = rail.result("get_user_details")["useruri"]
    effective_date = python_callable.get_split_date(
        dag_run.conf['effective_date'], 'int'
    )

    return {
        "transaction": {
            "target": {
                "uri": None
            },
            "account": {
                "userUri": user_uri,
                "timeOffTypeUri": timeoff_uri
            },
            "date": {
                "year": effective_date["year"],
                "month": effective_date["month"],
                "day": effective_date["day"]
            },
            "precedenceIndex": None,
            "amount": str(final_balance) if add else str(-final_balance),
            "metadata": [
                {
                    "keyUri": "urn:replicon:time-off-transaction-key-value-key:transaction-source",
                    "value": {
                        "uri": "urn:replicon:time-off-transaction-source:adjust-balance",
                        "slug": None,
                        "bool": None,
                        "date": None,
                        "number": None,
                        "text": None,
                        "time": None,
                        "calendarDayDurationValue": None,
                        "workdayDurationValue": None,
                        "dateRange": None,
                        "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:time-off-transaction-key-value-key:transaction-description",
                    "value": {
                        "uri": None,
                        "slug": None,
                        "bool": None,
                        "date": None,
                        "number": None,
                        "text": f"Leave balance transferred from {timeoff_type_from} to {timeoff_type_to}" if add else f"Leave balance transferred to {timeoff_type_to} from {timeoff_type_from}",
                        "time": None,
                        "calendarDayDurationValue": None,
                        "workdayDurationValue": None,
                        "dateRange": None,
                        "collection": []
                    }
                }
            ]
        },
        "unitOfWorkId": "_" + str(uuid.uuid4())[:6]
    } 
    
def get_report_parameters():
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