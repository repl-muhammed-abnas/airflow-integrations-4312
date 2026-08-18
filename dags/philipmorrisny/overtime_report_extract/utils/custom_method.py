from datetime import timedelta
import pendulum
import rail

today=pendulum.now(tz='America/Denver')

startdate = (today - timedelta(days=14)).strftime("%m/%d/%Y")
enddate = (today - timedelta(days=1)).strftime("%m/%d/%Y")


def get_report_params(report_details_task_id):
    approvaldatefilter = rail.find_first_by_attr_and_get_attr(
        rail.result(report_details_task_id)[
            'filterConfiguration']
        ['enabledFilters'], 'displayText', 'ApprovalDateFilter', 'uri')
    return {
        "reportParameters": [
            {
                "reportUri": rail.result(report_details_task_id)['uri'],
                "filterValues": [
                    {
                        "reportFilterUri": approvaldatefilter,
                        "value": None
                    },
                    {
                        "reportFilterUri": approvaldatefilter,
                        "value": startdate
                    },
                    {
                        "reportFilterUri": approvaldatefilter,
                        "value": enddate
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }
