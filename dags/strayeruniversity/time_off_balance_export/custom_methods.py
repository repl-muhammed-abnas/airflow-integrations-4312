from datetime import datetime
from pendulum import now
from dateutil.relativedelta import relativedelta
import rail
null=None

timeoffbalance_extract_mapper={
        "Sick TO": "Sick",
        "SEI PTO A": "SEI PTO A",
        "SEI PTO B": "SEI PTO B",
        "SEI PTO CA A": "SEI PTO CA A",
        "SEI PTO CA B": "SEI PTO CA B"
}

def get_timeoff_report_params(dag_run, config):
    daterangefilter_uri=rail.find_first_by_attr_and_get_attr(
                                    rail.result("get_time_off_report_details")["filterConfiguration"]["enabledFilters"],
                                    "displayText",
                                    "DateRangeFilter",
                                    "uri"
                            )
    today = now(tz=config.time_zone)
    if dag_run.conf.get("end_date"):
        today = datetime.strptime(dag_run.conf["end_date"], "%d/%m/%Y")
    start_date = datetime.strftime(today + relativedelta(days=-14), "%Y/%m/%d")
    end_date = datetime.strftime(today, "%Y/%m/%d")

    return {
        "reportParameters": [
            {
            "reportUri": rail.result("get_time_off_report_details")["uri"],
            "filterValues": [
                {
                "reportFilterUri": daterangefilter_uri,
                "value": null
                },
                {
                "reportFilterUri": daterangefilter_uri,
                "value": start_date
                },
                {
                "reportFilterUri": daterangefilter_uri,
                "value": end_date
                }
            ],
            "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_timeoff_data():
    response = rail.load_all_records(rail.result("parse_timeoff_csv"))
    return list(
        map(lambda item:{
                    "Employee ID": item["Employee ID"],
                    "Time Off Type": timeoffbalance_extract_mapper[item["Time Off Type"]]
                      if item["Time Off Type"] in timeoffbalance_extract_mapper else item["Time Off Type"],
                    "Balance (As of End Date)": item["Balance (As of End Date)"],
                    "Time Off Type DB ID": item["Time Off Type DB ID"],
                    "User DB ID": item["User DB ID"],
                }
        , response)
    )
