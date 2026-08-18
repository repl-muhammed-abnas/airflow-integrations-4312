from datetime import timedelta
import pendulum
from rail import get_current_context
import rail

null = None

def get_dag_run_conf():
    return get_current_context()['dag_run'].conf

def get_report_params(date_filter, time_zone, report_uri):
    previous_date = (pendulum.now(tz=time_zone) - timedelta(days=1)).strftime("%m/%d/%Y")
    return {
        "reportParameters": [
            {
                "reportUri": report_uri,
                "filterValues": [
                    {
                        "reportFilterUri": date_filter,
                        "value": "null"
                    },
                    {
                        "reportFilterUri": date_filter,
                        "value": get_dag_run_conf()["start_date"] if get_dag_run_conf() and get_dag_run_conf()["start_date"] else previous_date
                    },
                    {
                        "reportFilterUri": date_filter,
                        "value": get_dag_run_conf()["end_date"] if get_dag_run_conf() and get_dag_run_conf()["end_date"] else previous_date
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_approved_timeoffs_report_batch_payload(time_zone):
    approval_date_filter_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_approved_timeoffs_report_details')[
            'filterConfiguration']
        ['enabledFilters'], 'displayText', "ApprovalDateFilter", 'uri')
    return get_report_params(approval_date_filter_uri, time_zone, rail.result("get_approved_timeoffs_report_details")["uri"])

def get_deleted_timeoffs_report_batch_payload(time_zone):
    modified_on_date_filter_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_deleted_timeoffs_report_details')[
            'filterConfiguration']
        ['enabledFilters'], 'displayText', "ModifiedOnUtcDateRangeFilter", 'uri')
    return get_report_params(modified_on_date_filter_uri, time_zone, rail.result("get_deleted_timeoffs_report_details")["uri"])
