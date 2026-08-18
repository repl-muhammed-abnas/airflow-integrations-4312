from datetime import datetime, timedelta
import functools
import json
import pendulum
from airflow.models import Variable
import rail

null = None
MODIFIED_ON_UTC_FORMAT = "%b %d, %Y %I:%M:%S %p"
DATE_FORMAT_FROM_REPORT = "%b %d, %Y"
DATE_FORMAT_TO_EXPORT = "%d-%m-%Y"
EXPORT_FILENAME_TIMESTAMP = "%Y%m%d_%H%M%S"
CURRENT_TIMESTAMP = "%Y-%m-%dT%H:%M:%S.%f%z"
YMD_FORMAT = "%Y-%m-%d"
TIME_FORMAT_WITH_TIMEZONE = "T%H:%M:%S.%f%z"

def get_logging_details(required_timeoffs, export_file_prefix, time_zone):
    today = pendulum.now(time_zone)
    timeoff_list_var = list(map(lambda data: data, filter(lambda data: data["allowed"].lower() == "yes",
        Variable.get(required_timeoffs, deserialize_json=True))))
    return {
        "process_start_time": today.strftime(CURRENT_TIMESTAMP),
        "current_date": today.strftime(DATE_FORMAT_TO_EXPORT),
        "previous_date": (today - timedelta(days=1)).strftime(DATE_FORMAT_TO_EXPORT),
        "payroll_export_filename": f"Replicon_{export_file_prefix}_LOP_LOPR_{today.strftime(EXPORT_FILENAME_TIMESTAMP)}",
        "time_zone": time_zone,
        "required_timeoffs": f"""('{"','".join([data['timeoff_type'] for data in timeoff_list_var])}')""",
        "required_timeoffs_mapper": timeoff_list_var
    }

@functools.lru_cache(maxsize=128)
def get_previous_date(dag_run):
    return dag_run.conf["logging_details"]["previous_date"]

def get_previous_datetime(dag_run):
    return (datetime.strptime(dag_run.conf["logging_details"]["previous_date"], DATE_FORMAT_TO_EXPORT).strftime(YMD_FORMAT)
        + pendulum.now(dag_run.conf["logging_details"]["time_zone"]).strftime(TIME_FORMAT_WITH_TIMEZONE))

def get_payroll_data_csv_rows(item):
    if not item:
        return []
    return [
        null,
        item['employeee_id'],
        item['ggid'],
        item['lwp_type'],
        item['lwp_start_date'],
        item['lwp_end_date'],
        item['lwp_code'],
        item['modified_dated'],
        item['remarks'],
        item['company_name']
    ]

def get_date_obj(date_str, date_format):
    return datetime.strptime(date_str, date_format)

def get_costcenter_name(dag_run):
    return dag_run.conf['timeoff_booking_details']['cost_center_full_path'].split('/')[1].strip().split('|')[0].strip() \
        if len(dag_run.conf['timeoff_booking_details']['cost_center_full_path'].split('/')) > 1 else null

def get_new_timeoff_record_to_add(dag_run):
    lwp_start_date = get_date_obj(dag_run.conf["timeoff_booking_details"]["booking_start_date"],
        DATE_FORMAT_FROM_REPORT).strftime(DATE_FORMAT_TO_EXPORT)
    lwp_end_date = get_date_obj(dag_run.conf["timeoff_booking_details"]["booking_end_date"],
        DATE_FORMAT_FROM_REPORT).strftime(DATE_FORMAT_TO_EXPORT)
    booking_details = dag_run.conf["timeoff_booking_details"]
    return json.dumps([{
        "timeoff_id": booking_details["leave_request_id"],
        "employeee_id": booking_details["local_employee_number"],
        "ggid": booking_details["employee_id"],
        "lwp_type": "L",
        "lwp_start_date": lwp_start_date if lwp_start_date else null,
        "lwp_end_date": lwp_end_date if lwp_end_date else null,
        "lwp_code": rail.find_first_by_attr_and_get_attr(dag_run.conf["logging_details"]["required_timeoffs_mapper"],
            'timeoff_type', booking_details["timeoff_type"], 'expected_timeoff_code'),
        "modified_dated": get_previous_datetime(dag_run),
        "company_name": get_costcenter_name(dag_run)
    }])

def get_new_and_existing_timeoff_record_to_add(dag_run):
    lwp_start_date = get_date_obj(dag_run.conf["timeoff_booking_details"]["booking_start_date"],
        DATE_FORMAT_FROM_REPORT).strftime(DATE_FORMAT_TO_EXPORT)
    lwp_end_date = get_date_obj(dag_run.conf["timeoff_booking_details"]["booking_end_date"],
        DATE_FORMAT_FROM_REPORT).strftime(DATE_FORMAT_TO_EXPORT)
    generic_keyvalue = rail.result("sort_and_parse_json_value")
    booking_details = dag_run.conf["timeoff_booking_details"]
    existing_records = [{
        "timeoff_id": record["timeoff_id"],
        "employeee_id": record["employeee_id"],
        "ggid": record["ggid"],
        "lwp_type": record["lwp_type"],
        "lwp_start_date": record["lwp_start_date"],
        "lwp_end_date": record["lwp_end_date"],
        "lwp_code": record["lwp_code"],
        "modified_dated": record["modified_dated"],
        "company_name": record["company_name"]
    } for record in generic_keyvalue]
    new_record = {
        "timeoff_id": booking_details["leave_request_id"],
        "employeee_id": booking_details["local_employee_number"],
        "ggid": booking_details["employee_id"],
        "lwp_type": "L",
        "lwp_start_date": lwp_start_date if lwp_start_date else null,
        "lwp_end_date": lwp_end_date if lwp_end_date else null,
        "lwp_code": rail.find_first_by_attr_and_get_attr(dag_run.conf["logging_details"]["required_timeoffs_mapper"],
            'timeoff_type', booking_details["timeoff_type"], 'expected_timeoff_code'),
        "modified_dated": get_previous_datetime(dag_run),
        "company_name": get_costcenter_name(dag_run)
    }
    existing_records.append(new_record)
    all_records = existing_records
    return json.dumps(all_records)

def get_date(item):
    return datetime.strptime(item['modified_dated'], CURRENT_TIMESTAMP)

def parse_and_sort_json_data():
    key_data_list = json.loads(rail.result("get_keyvalue_for_user_timeoff")["jsonValue"])
    return sorted(key_data_list, key=get_date, reverse=True)

def check_startdates_and_enddates_equal(dag_run):
    return get_date_obj(rail.result("sort_and_parse_json_value")[0]["lwp_start_date"], DATE_FORMAT_TO_EXPORT) == get_date_obj(
        dag_run.conf["timeoff_booking_details"]["booking_start_date"], DATE_FORMAT_FROM_REPORT) and \
            get_date_obj(rail.result("sort_and_parse_json_value")[0]["lwp_end_date"], DATE_FORMAT_TO_EXPORT) == get_date_obj(
                dag_run.conf["timeoff_booking_details"]["booking_end_date"], DATE_FORMAT_FROM_REPORT)

def log_L_timeoff_record_to_add(dag_run):
    lwp_start_date = get_date_obj(dag_run.conf["timeoff_booking_details"]["booking_start_date"],
        DATE_FORMAT_FROM_REPORT).strftime(DATE_FORMAT_TO_EXPORT)
    lwp_end_date = get_date_obj(dag_run.conf["timeoff_booking_details"]["booking_end_date"],
        DATE_FORMAT_FROM_REPORT).strftime(DATE_FORMAT_TO_EXPORT)
    booking_details = dag_run.conf["timeoff_booking_details"]
    return {
        "entity": null,
        "employeee_id": booking_details["local_employee_number"],
        "ggid": booking_details["employee_id"],
        "lwp_type": "L",
        "lwp_start_date": lwp_start_date,
        "lwp_end_date": lwp_end_date,
        "lwp_code": rail.find_first_by_attr_and_get_attr(dag_run.conf["logging_details"]["required_timeoffs_mapper"],
            'timeoff_type', booking_details["timeoff_type"], 'expected_timeoff_code'),
        "modified_dated": get_previous_date(dag_run),
        "remarks": "Approved",
        "company_name": get_costcenter_name(dag_run)
    }

def log_R_timeoff_record_to_reverse(dag_run):
    generic_keyvalue = rail.result("sort_and_parse_json_value")[0]
    return {
        "entity": null,
        "employeee_id": generic_keyvalue["employeee_id"],
        "ggid": generic_keyvalue["ggid"],
        "lwp_type": "R",
        "lwp_start_date": generic_keyvalue["lwp_start_date"],
        "lwp_end_date": generic_keyvalue["lwp_end_date"],
        "lwp_code": generic_keyvalue["lwp_code"],
        "modified_dated": get_previous_date(dag_run),
        "remarks": "Cancelled",
        "company_name": generic_keyvalue["company_name"]
    }

def log_R_timeoff_record_to_delete(dag_run):
    lwp_start_date = get_date_obj(dag_run.conf["timeoff_booking_details"]["booking_start_date"],
        DATE_FORMAT_FROM_REPORT).strftime(DATE_FORMAT_TO_EXPORT)
    lwp_end_date = get_date_obj(dag_run.conf["timeoff_booking_details"]["booking_end_date"],
        DATE_FORMAT_FROM_REPORT).strftime(DATE_FORMAT_TO_EXPORT)
    booking_details = dag_run.conf["timeoff_booking_details"]
    return {
        "entity": null,
        "employeee_id": booking_details["local_employee_number"],
        "ggid": booking_details["employee_id"],
        "lwp_type": "R",
        "lwp_start_date": lwp_start_date,
        "lwp_end_date": lwp_end_date,
        "lwp_code": rail.find_first_by_attr_and_get_attr(dag_run.conf["logging_details"]["required_timeoffs_mapper"],
            'timeoff_type', booking_details["timeoff_type"], 'expected_timeoff_code'),
        "modified_dated": get_previous_date(dag_run),
        "remarks": "Cancelled",
        "company_name": get_costcenter_name(dag_run)
    }

def do_format_logs():
    log_artifacts = []
    log_records = []

    logs = rail.result("gather_process_timeoffs_logs")

    if logs:
        if isinstance(logs, list):
            log_artifacts.extend(logs)
        else:
            log_artifacts.append(logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **log['properties'],
        }, log_records))

    return final_log_records
