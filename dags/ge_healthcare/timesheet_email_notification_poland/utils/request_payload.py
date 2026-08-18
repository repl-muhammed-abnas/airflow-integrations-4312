import rail

null = None

def get_approval_status_filter_uri():
    return rail.find_first_by_attr_and_get_attr(rail.result('get_report_details')['filterConfiguration']['enabledFilters'],
            'displayText', 'ApprovalStatusFilter', 'uri')

def get_report_generate_payload():
    approval_status_filter_uri = get_approval_status_filter_uri()
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')["uri"],
                "filterValues": [
                    {
                        "reportFilterUri": approval_status_filter_uri,
                        "value": "1"
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }
