import pendulum
from datetime import datetime, timedelta
import rail

null = None

def get_logging_details(time_zone, filename_prefix):
    today = pendulum.now(time_zone)
    current_time = today.strftime('%Y%m%d%H%M%S')
    return {
        "time_zone": time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "sl_export_filename": f"{filename_prefix}_SL_FRA_{current_time}",
        "pm_export_filename": f"{filename_prefix}_PM_FRA_{current_time}"
    }

def get_sellback_leaves_report_filters(dag_run, time_zone):
    modified_on = (pendulum.now(time_zone) - timedelta(days=1)).strftime("%m/%d/%Y")
    daterangefilter = rail.find_first_by_attr_and_get_attr(rail.result('get_report_details')['filterConfiguration']['enabledFilters'],
                    'displayText', "DateRangeFilter", 'uri')
    return [
        {
            "reportFilterUri": daterangefilter,
            "value": null
        },
        {
            "reportFilterUri": daterangefilter,
            "value": dag_run.conf["start_date"] if dag_run.conf and dag_run.conf["start_date"] else modified_on
        },
        {
            "reportFilterUri": daterangefilter,
            "value": dag_run.conf["end_date"] if dag_run.conf and dag_run.conf["end_date"] else modified_on
        }
    ]

def get_report_parameters(dag_run, time_zone):
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')["uri"],
                "filterValues": get_sellback_leaves_report_filters(dag_run, time_zone),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_filtered_data(codes_to_export_mapper, identifier):
    return "(\"" + "\", \"".join(list(map(lambda timeoff_data: timeoff_data["timeoff_type_name"],
        filter(lambda timeoff_data: timeoff_data["ZYOQ_MOTIFA"] == identifier, codes_to_export_mapper)))) + "\")"

def get_sl_and_pm_timeoff_types(codes_to_export_mapper):
    return {
        "sl_timeoff_types": get_filtered_data(codes_to_export_mapper, "SL"),
        "pm_timeoff_types": get_filtered_data(codes_to_export_mapper, "PM"),
    }

def get_sellback_data_rows(item, config):
    current_date = pendulum.now(config.time_zone)
    return [
        "000000000"
        + f'CAP{item["employeeid"].zfill(8)}'
        + (' ' * 24)
        + "*F"
        + "ZY"
        + "OQ"
        + (' ' * 2)
        + (' ' * 20)
        + "3"
        + "0"
        + rail.find_first_by_attr_and_get_attr(config.codes_to_export_mapper, "timeoff_type_name",
            item["timeofftype"], "ZYOQ_CODCON")
        + str(current_date.year)
        + "0000"
        + datetime.strptime(item["date"], "%b %d, %Y").strftime("%Y-%m-%d")
        + rail.find_first_by_attr_and_get_attr(config.codes_to_export_mapper, "timeoff_type_name",
            item["timeofftype"], "ZYOQ_TYPAJU")
        + "000"
        + datetime.strptime(item["date"], "%b %d, %Y").strftime("%Y-%m-%d")
        + rail.find_first_by_attr_and_get_attr(config.codes_to_export_mapper, "timeoff_type_name",
            item["timeofftype"], "ZYOQ_MOTIFA")
        + f'-{int(abs(float(item["amount"])) * 100):05d}'
        + item["timeofftype"]
    ]
