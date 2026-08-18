from datetime import timedelta
import pendulum
import rail

null = None

def get_report_params(date_filter, start_date, end_date, report_uri):
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
                        "value": start_date
                    },
                    {
                        "reportFilterUri": date_filter,
                        "value": end_date
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
    previous_date = (pendulum.now(tz=time_zone) - timedelta(days=1)).strftime("%m/%d/%Y")
    return get_report_params(approval_date_filter_uri, previous_date, previous_date, rail.result("get_approved_timeoffs_report_details")["uri"])

def get_deleted_timeoffs_report_batch_payload(time_zone):
    modified_on_date_filter_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_deleted_timeoffs_report_details')[
            'filterConfiguration']
        ['enabledFilters'], 'displayText', "ModifiedOnUtcDateRangeFilter", 'uri')
    start_date = (pendulum.now(tz=time_zone) - timedelta(days=7)).strftime("%m/%d/%Y")
    end_date = (pendulum.now(tz=time_zone) - timedelta(days=1)).strftime("%m/%d/%Y")
    return get_report_params(modified_on_date_filter_uri, start_date, end_date, rail.result("get_deleted_timeoffs_report_details")["uri"])

def get_modified_on_report_batch_payload(modified_on_date_filter_uri, start_date, end_date, report_uri):
    return {
        "reportParameters": [
            {
                "reportUri": report_uri,
                "filterValues": [
                    {
                        "reportFilterUri": modified_on_date_filter_uri,
                        "value": "null"
                    },
                    {
                        "reportFilterUri": modified_on_date_filter_uri,
                        "value": start_date
                    },
                    {
                        "reportFilterUri": modified_on_date_filter_uri,
                        "value": end_date
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_modified_timeoffs_report_batch_payload(time_zone):
    modified_on_date_filter_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_modified_timeoffs_report_details')[
            'filterConfiguration']
        ['enabledFilters'], 'displayText', "ModifiedOnUtcDateRangeFilter", 'uri')
    start_date = (pendulum.now(tz=time_zone) - timedelta(days=30)).strftime("%m/%d/%Y")
    end_date = (pendulum.now(tz=time_zone) - timedelta(days=1)).strftime("%m/%d/%Y")
    return get_modified_on_report_batch_payload(modified_on_date_filter_uri, start_date,
        end_date, rail.result("get_modified_timeoffs_report_details")["uri"],)

def get_added_timeoffs_report_batch_payload(time_zone):
    modified_on_date_filter_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_added_timeoffs_report_details')[
            'filterConfiguration']
        ['enabledFilters'], 'displayText', "ModifiedOnUtcDateRangeFilter", 'uri')
    start_date = (pendulum.now(tz=time_zone) - timedelta(days=30)).strftime("%m/%d/%Y")
    end_date = (pendulum.now(tz=time_zone) - timedelta(days=1)).strftime("%m/%d/%Y")
    return get_modified_on_report_batch_payload(modified_on_date_filter_uri, start_date,
        end_date, rail.result("get_added_timeoffs_report_details")["uri"],)

def get_approvedlast30days_timeoffs_report_batch_payload(time_zone):
    modified_on_date_filter_uri = rail.find_first_by_attr_and_get_attr(
        rail.result('get_approvedlast30days_timeoffs_report_details')[
            'filterConfiguration']
        ['enabledFilters'], 'displayText', "ModifiedOnUtcDateRangeFilter", 'uri')
    start_date = (pendulum.now(tz=time_zone) - timedelta(days=30)).strftime("%m/%d/%Y")
    end_date = (pendulum.now(tz=time_zone) - timedelta(days=2)).strftime("%m/%d/%Y")
    return get_modified_on_report_batch_payload(modified_on_date_filter_uri, start_date,
        end_date, rail.result("get_approvedlast30days_timeoffs_report_details")["uri"],)
