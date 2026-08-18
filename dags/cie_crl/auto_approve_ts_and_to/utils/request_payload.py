from datetime import datetime, timedelta
import json
import rail
from cie_crl.auto_approve_ts_and_to import config

null = None


def get_submit_timesheet_payload_uris(dag_run):
    if dag_run:
        return {
            "timesheetUris": dag_run.conf["item"],
            "comments": "Timesheet Submitted by CIE Approval Utility."
        }
    return None


def execute_submit_timesheet_payload(dag_run):
    if dag_run:
        return {
                    "timesheetApprovalBatchUri": rail.result(create_submit_batch) 
                }
    return None

def get_user_supervisor(dag_run):
    if dag_run:
        print("dag_run.conf['item']", dag_run.conf["item"])
        return dag_run.conf["item"]
    return None


def get_timehseet_approve_batch(dag_run):

    if dag_run:
        return {
            "timesheetUris": rail.result('filter_validation_uris'),
            "comments": config.timesheet_approve_remarks
        }
    return None

def get_bulk_validation_payload(dag_run):

    if dag_run:
        return {
            "timesheetUris": dag_run.conf["item"]
        }
    return None

def execute_batch_timesheet_data(item):
    if item:
        return {
            "timesheetApprovalBatchUri": item,
        }
    return None

def get_report_filter_uris(config):
    run_datetime = datetime.strptime(rail.result('get_current_date'), config.date_format)
    end_date = run_datetime - timedelta(days=1)
    start_date = end_date - timedelta(days = config.days)
    data = rail.result("get_ts_report_details")['filterConfiguration']['enabledFilters']
    report_uri = rail.result("get_ts_report_details")['uri']
    ts_period_filter = rail.find_first_by_attr_and_get_attr(data, 'displayText', "TimesheetPeriodFilter", 'uri')
    approval_status_filter = rail.find_first_by_attr_and_get_attr(data, 'displayText', "ApprovalStatusFilter", 'uri')


    filters = [
        {
            "reportFilterUri": ts_period_filter,
            "value": null
        },
        {
            "reportFilterUri": ts_period_filter,
            "value": f'{ (start_date).strftime(config.date_format) }'
        },
        {
            "reportFilterUri": ts_period_filter,
            "value": f'{ (end_date).strftime(config.date_format) }'
        },
        {
            "reportFilterUri": approval_status_filter,
            "value": config.waiting_for_approval_filter_value
        },
        {
            "reportFilterUri": approval_status_filter,
            "value": config.not_submitted_filter_value
        }
    ]


    report_input = {
                    "reportParameters": [
                    {
                        "reportUri": report_uri,
                        "filterValues": filters,
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }]
                }

    return json.dumps(report_input)


def get_to_report_filter_uris(config):
    run_datetime = datetime.strptime(rail.result('get_current_date'), config.date_format)
    end_date = run_datetime - timedelta(days=1)
    start_date = end_date - timedelta(days = config.days)
    data = rail.result("get_to_report_details")['filterConfiguration']['enabledFilters']
    report_uri = rail.result("get_to_report_details")['uri']
    ts_period_filter = rail.find_first_by_attr_and_get_attr(data, 'displayText', "DateRangeFilter", 'uri')
    approval_status_filter = rail.find_first_by_attr_and_get_attr(data, 'displayText', "ApprovalStatusFilter", 'uri')

    filters = [
        {
            "reportFilterUri": ts_period_filter,
            "value": null
        },
        {
            "reportFilterUri": ts_period_filter,
            "value": f'{ (start_date).strftime(config.date_format) }'
        },
        {
            "reportFilterUri": ts_period_filter,
            "value": f'{ (end_date).strftime(config.date_format) }'
        },
        {
            "reportFilterUri": approval_status_filter,
            "value": "3"
        }
    ]


    report_input = {
                    "reportParameters": [
                    {
                        "reportUri": report_uri,
                        "filterValues": filters,
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }]
                }


    return json.dumps(report_input)

def get_holiday_payload(config):
    run_datetime = datetime.strptime(rail.result('get_current_date'), config.date_format)
    return {
                "holidayCalendarUri": rail.result('holiday_calender').get('uri'),
                "dateRange": {
                    "startDate": {
                    "day": run_datetime.day,
                    "month": run_datetime.month,
                    "year": run_datetime.year
                },
                    "endDate": {
                    "day": run_datetime.day,
                    "month": run_datetime.month,
                    "year": run_datetime.year
                }
                }
            }