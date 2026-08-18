from datetime import datetime as dt
import pendulum
from dateutil.relativedelta import relativedelta
import rail
from rail import get_current_context

null = None

REPORT_FILTER_DATE_FORMAT = "%m/%d/%Y"
FILENAME_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
FILENAME_DATE_FORMAT = "%Y%m%d"

def get_dag_run_conf():
    return get_current_context()['dag_run'].conf

def get_logging_detail(config):
    today = pendulum.now(config.time_zone)
    current_time = today.strftime(FILENAME_TIMESTAMP_FORMAT)
    start_date = (today-relativedelta(months=4)).strftime(REPORT_FILTER_DATE_FORMAT)
    end_date = (today).strftime(REPORT_FILTER_DATE_FORMAT)
    filename_startdate = (today-relativedelta(months=4)).strftime(FILENAME_DATE_FORMAT)
    filename_enddate =  (today).strftime(FILENAME_DATE_FORMAT)
    return {
        "processing_start_time": pendulum.now(config.time_zone).strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "report_start_date": get_dag_run_conf()['start_date'] if get_dag_run_conf() else start_date,
        "report_end_date": get_dag_run_conf()['end_date'] if get_dag_run_conf() else end_date,
        "filename": f"{config.FILENAME_PREFIX}_{filename_startdate}_{filename_enddate}_{current_time}.csv"
    }

def get_leave_request_report_filters(dag_run):
    unscheduled_hours_datefilter = rail.find_first_by_attr_and_get_attr(rail.result('get_report_details')['filterConfiguration']['enabledFilters'],
                    'displayText', "WorkAuthorizationDateFilter", 'uri')
    return [
        {
            "reportFilterUri": unscheduled_hours_datefilter,
            "value": null
        },
        {
            "reportFilterUri": unscheduled_hours_datefilter,
            "value": rail.result("get_logging_details")["report_start_date"]
        },
        {
            "reportFilterUri": unscheduled_hours_datefilter,
            "value": rail.result("get_logging_details")["report_end_date"]
        }
    ]

def get_report_parameters(dag_run):
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')["uri"],
                "filterValues": get_leave_request_report_filters(dag_run),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

REPORT_DATE_FORMAT = '%b %d, %Y'
REPORT_DATE_TIME_FORMAT = '%b %d, %Y %I:%M:%S %p'

def get_formatted_data(item):
    if not item:
        return None
    return [
        item['Employee Name'],
        item['GGID'],
        item['Emp Local ID'],
        item['SBU Code'],
        item['Employee Grade'],
        item['PU'],
        item['Project Code'],
        item['Project Type'],
        dt.strptime(item['Request Start_Date'],REPORT_DATE_FORMAT).strftime('%d%m%Y') if item['Request Start_Date'] else item['Request Start_Date'],
        dt.strptime(item['Request End_Date'],REPORT_DATE_FORMAT).strftime('%d%m%Y') if item['Request End_Date'] else item['Request End_Date'],
        item['Request Duration'],
        item['Project Manager'],
        dt.strptime(item['Submitted On'],REPORT_DATE_TIME_FORMAT).strftime('%Y%m%d_%H%M%S') if item['Submitted On'] else item['Submitted On'],
        dt.strptime(item['Modified On'],REPORT_DATE_TIME_FORMAT).strftime('%Y%m%d_%H%M%S') if item['Modified On'] else item['Modified On'],
        item['Approver Name'],
        item['Approver Employee_Id'],
        dt.strptime(item['Approval Date'],REPORT_DATE_TIME_FORMAT).strftime('%Y%m%d_%H%M%S') if item['Approval Date'] else item['Approval Date'],
        item['Location Current']
    ]
