"""
Data processing utilities for Unisys Fieldglass time export integration
"""
from functools import lru_cache
import rail
import pendulum

null = None

def get_yesterdays_date():
    return pendulum.now().subtract(days=1).strftime("%m/%d/%Y")

@lru_cache(maxsize=8)
def get_dagrun_conf():
    return rail.get_dag_run_conf()

def get_start_date():
    if get_dagrun_conf():
        return get_dagrun_conf().get('start_date', get_yesterdays_date())
    return get_yesterdays_date()

def get_end_date():
    if get_dagrun_conf():
        return get_dagrun_conf().get('end_date', get_yesterdays_date())
    return get_yesterdays_date()

def get_report_payload(report_details_task_id):
    """
    Generate the report payload for fetching timesheet data.
    Based on design requirements:
    - Filter by approval date (previous day)
    - Only approved timecards
    - Only contractor employees
    """
    enabled_filters = rail.result(report_details_task_id)["filterConfiguration"]["enabledFilters"]

    report_filter = rail.find_first_by_attr_and_get_attr(enabled_filters, "displayText", "ApprovalDateFilter", "uri")

    filter_list = [
        {
            "reportFilterUri": report_filter,
            "value": null
        },
        {
            "reportFilterUri": report_filter,
            "value": get_start_date()
        },
        {
            "reportFilterUri": report_filter,
            "value": get_end_date()
        },
    ]
    return {
        "reportParameters": [
            {
                "reportUri": rail.result(report_details_task_id)["uri"],
                "filterValues": filter_list,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }