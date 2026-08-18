from datetime import datetime
import pendulum
from dateutil.relativedelta import relativedelta
from capgemini.france_place_of_work_export_to_sopra_v1.mapper.pay_codes import pay_codes_list
import rail

null = None
EXPORT_DATE_FORMAT = "%Y-%m-%d"
REPORT_FILTER_DATE_FORMAT = "%m/%d/%Y"
FILENAME_TIMESTAMP_FORMAT = "%Y_%m%d%H%M%S"

def get_place_of_work_csv_data(item, dag_run):
    if not item:
        return []
    return [
        rail.find_first_by_attr_and_get_attr(pay_codes_list, "place_of_work", item["place_of_work_fra"], "codeEV", ""),
        item["employee_id"].zfill(8),
        "Q",
        item["bucket"],
        "",
        datetime.strptime(dag_run.conf["export_end_date"], REPORT_FILTER_DATE_FORMAT).strftime(EXPORT_DATE_FORMAT),
        ""
    ]

def get_place_of_work_csv_no_data():
    return [
        {
            "paycode": "",
            "employee_id": "",
            "format": "",
            "bucket": "",
            "monsal": "",
            "last_date_of_prev_month": "",
            "entitlement": ""
        }
    ]

def get_entry_date_range_list(time_zone, no_of_months_place_of_work_data_to_export, filename_prefix):
    today = pendulum.now(time_zone)
    current_time = today.strftime(FILENAME_TIMESTAMP_FORMAT)
    months_date_range_list = list(map(lambda month_num: {
        "start_date": (today-relativedelta(months=month_num, day=1)).strftime(REPORT_FILTER_DATE_FORMAT),
        "end_date": (today-relativedelta(months=month_num, day=31)).strftime(REPORT_FILTER_DATE_FORMAT)
    }, range(1, no_of_months_place_of_work_data_to_export + 1)))

    return list(map(lambda daterange: {
        "export_filename": (filename_prefix + "_" + datetime.strptime(daterange["start_date"],
            REPORT_FILTER_DATE_FORMAT).strftime("%y%m") + "_" + current_time + ".xml"),
        "export_start_date": daterange["start_date"],
        "export_end_date": daterange["end_date"],
        "export_month": datetime.strptime(daterange["start_date"], REPORT_FILTER_DATE_FORMAT).strftime("%B")
    }, months_date_range_list))
