from datetime import datetime
from capgemini.shift_schedule_extract_to_TETA.utils.custom_methods import REPORT_FILTER_DATE_FORMAT
import rail

null = None

def get_shift_assignment_report_batch_payload(dag_run):
    shiftdatefilter = rail.find_first_by_attr_and_get_attr(
        rail.result('get_shift_assignment_report_details')['filterConfiguration']['enabledFilters'], 'displayText', "ShiftDateFilter", 'uri')
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_shift_assignment_report_details')['uri'],
                "filterValues": [

                    {
                        "reportFilterUri": shiftdatefilter,
                        "value": null,
                    },
                    {
                        "reportFilterUri": shiftdatefilter,
                        "value": dag_run.conf["export_start_date"],
                    },
                    {
                        "reportFilterUri": shiftdatefilter,
                        "value": dag_run.conf["export_end_date"],
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def public_holidays_in_daterange_payload(dag_run):
    start_date = datetime.strptime(dag_run.conf["export_start_date"], REPORT_FILTER_DATE_FORMAT)
    end_date = datetime.strptime(dag_run.conf["export_end_date"], REPORT_FILTER_DATE_FORMAT)
    return {
        "holidayCalendarUri": rail.result("get_required_holiday_calendar_uri"),
        "dateRange": {
            "startDate": {
                "year": start_date.year,
                "month": start_date.month,
                "day": start_date.day
            },
            "endDate": {
                "year": end_date.year,
                "month": end_date.month,
                "day": end_date.day
            },
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }
