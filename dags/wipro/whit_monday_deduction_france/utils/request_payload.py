import uuid
import rail
from wipro.whit_monday_deduction_france import config

null = None


def get_june_holidays_payload():
    current_year = int(rail.result('dag_run_log_time_info')['dag_run_date'].split('/')[0])
    return {
        "holidayCalendarUri": rail.result('get_france_holiday_calendar').get('uri'),
        "dateRange": {
            "startDate": {"day": 1, "month": 6, "year": current_year},
            "endDate": {"day": 30, "month": 6, "year": current_year}
        }
    }


def get_report_parameters():
    required_filters = rail.result('get_required_filters')
    timeoff_type_uris = rail.result('get_required_timeoff_type_uris')
    log_dag_run_details = rail.result('dag_run_log_time_info')
    country_service_center_uri = rail.result('get_country_servicecenter_uri').split(":")[-1]
    report_uri = rail.result('get_report_details')['uri']

    as_of_date_filter_uri = required_filters['as_of_date_filter_uri']
    timeoff_type_filter_uri = required_filters['timeoff_type_filter_uri']
    country_service_centre_filter_uri = required_filters['country_service_centre_filter_uri']
    report_run_date = log_dag_run_details['report_run_date']

    timeoff_type_keys = [
        'timeoff_annual_leave_rtt_carried_over_uri',
        'timeoff_annual_leave_rtt_for_forfait_jours_carried_over_uri'
    ]

    filter_values = [
        {"reportFilterUri": as_of_date_filter_uri, "value": "DateRange"},
        {"reportFilterUri": as_of_date_filter_uri, "value": report_run_date},
        {"reportFilterUri": as_of_date_filter_uri, "value": report_run_date},
    ]

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


def build_whit_monday_deduction_payload(dag_run, timeoff_uri):
    user_uri = rail.result("get_user_details")["useruri"]
    effective_date_str = dag_run.conf['effective_date']
    from datetime import datetime
    d = datetime.strptime(effective_date_str, config.DATE_DEFAULT_FORMAT)
    return {
        "transaction": {
            "target": {"uri": None},
            "account": {
                "userUri": user_uri,
                "timeOffTypeUri": timeoff_uri
            },
            "date": {
                "year": d.year,
                "month": d.month,
                "day": d.day
            },
            "precedenceIndex": None,
            "amount": "-1",
            "metadata": [
                {
                    "keyUri": "urn:replicon:time-off-transaction-key-value-key:transaction-source",
                    "value": {
                        "uri": "urn:replicon:time-off-transaction-source:adjust-balance",
                        "slug": None, "bool": None, "date": None, "number": None,
                        "text": None, "time": None, "calendarDayDurationValue": None,
                        "workdayDurationValue": None, "dateRange": None, "collection": []
                    }
                },
                {
                    "keyUri": "urn:replicon:time-off-transaction-key-value-key:transaction-description",
                    "value": {
                        "uri": None, "slug": None, "bool": None, "date": None, "number": None,
                        "text": "Whit Monday deduction",
                        "time": None, "calendarDayDurationValue": None,
                        "workdayDurationValue": None, "dateRange": None, "collection": []
                    }
                }
            ]
        },
        "unitOfWorkId": "_" + str(uuid.uuid4())[:6]
    }
