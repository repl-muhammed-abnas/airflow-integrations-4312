from datetime import datetime, timedelta
import functools
import itertools
import json
import pendulum
from airflow.models import Variable
import rail

null = None
MODIFIED_ON_UTC_FORMAT = "%b %d, %Y %I:%M:%S %p"
DATE_FORMAT_FROM_REPORT = "%b %d, %Y"
DATE_FORMAT_TO_EXPORT = "%d-%m-%Y"
EXPORT_FILENAME_TIMESTAMP = "%Y%m%d_%H%M%S"

def get_logging_details(config):
    today = pendulum.now(config.time_zone)
    timeoff_list_var = list(map(lambda data: data, filter(lambda data: data["allowed"].lower() == "yes",
        Variable.get(config.required_timeoffs, deserialize_json=True))))
    return {
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "current_date": today.strftime(DATE_FORMAT_TO_EXPORT),
        "previous_date": (today - timedelta(days=1)).strftime(DATE_FORMAT_TO_EXPORT),
        "payroll_export_filename": f"Replicon_{config.export_file_prefix}_LOP_LOPR_{today.strftime(EXPORT_FILENAME_TIMESTAMP)}",
        "time_zone": config.time_zone,
        "required_timeoffs": f"""('{"','".join([data['timeoff_type'] for data in timeoff_list_var])}')""",
        "required_timeoffs_mapper": timeoff_list_var
    }

def get_filtered_leave_data(max_booking_modified_on_date, leave_request_id, modified_bookings):
    return list(map(lambda booking_data: booking_data,
        filter(lambda booking_data: booking_data["leave_request_id"] == leave_request_id
            and datetime.strptime(booking_data["modified_on"],
                MODIFIED_ON_UTC_FORMAT).strftime(DATE_FORMAT_FROM_REPORT) == max_booking_modified_on_date,
                    modified_bookings)))

def get_max_modified_on_date(leave_request_id, modified_bookings):
    return max(list(map(lambda booking_data: datetime.strptime(booking_data["modified_on"], MODIFIED_ON_UTC_FORMAT),
                filter(lambda booking_data: booking_data["leave_request_id"] == leave_request_id,
                       modified_bookings)))).strftime(DATE_FORMAT_FROM_REPORT)

def get_start_end_validation_data():
    modified_bookings = rail.load_all_records(rail.result("query_list_all_modified_bookings_to_be_considered"))
    distinct_bookings_list = rail.load_all_records(rail.result("query_list_distinct_bookings"))

    def get_booking_details(leave_request_id, get_attribute):
        return rail.find_first_by_attr_and_get_attr(modified_bookings, "leave_request_id", leave_request_id, get_attribute)

    validated_data = []
    for item in distinct_bookings_list:
        max_booking_modified_on_date = get_max_modified_on_date(item["leave_request_id"], modified_bookings)

        timeoff_type = list(map(lambda booking_data: booking_data["current_timeoff_type"],
                filter(lambda booking_data: booking_data["current_timeoff_type"],
                       get_filtered_leave_data(max_booking_modified_on_date, item["leave_request_id"], modified_bookings))))

        booking_start_date = list(map(lambda booking_data: booking_data["current_start_date"],
                filter(lambda booking_data: booking_data["current_start_date"],
                       get_filtered_leave_data(max_booking_modified_on_date, item["leave_request_id"], modified_bookings))))

        booking_end_date = list(map(lambda booking_data: booking_data["current_end_date"],
                filter(lambda booking_data: booking_data["current_end_date"],
                       get_filtered_leave_data(max_booking_modified_on_date, item["leave_request_id"], modified_bookings))))

        start_date_update = list(map(lambda booking_data: booking_data["field"],
                filter(lambda booking_data: booking_data["field"] and booking_data["field"] == "Start Date",
                       get_filtered_leave_data(max_booking_modified_on_date, item["leave_request_id"], modified_bookings))))

        end_date_update = list(map(lambda booking_data: booking_data["field"],
                filter(lambda booking_data: booking_data["field"] and booking_data["field"] == "End Date",
                       get_filtered_leave_data(max_booking_modified_on_date, item["leave_request_id"], modified_bookings))))

        original_start_date = list(map(lambda booking_data: datetime.strptime(booking_data["original_value"], DATE_FORMAT_FROM_REPORT),
                filter(lambda booking_data: booking_data["field"] and booking_data["field"] == "Start Date",
                       get_filtered_leave_data(max_booking_modified_on_date, item["leave_request_id"], modified_bookings))))

        original_end_date = list(map(lambda booking_data: datetime.strptime(booking_data["original_value"], DATE_FORMAT_FROM_REPORT),
                filter(lambda booking_data: booking_data["field"] and booking_data["field"] == "End Date",
                       get_filtered_leave_data(max_booking_modified_on_date, item["leave_request_id"], modified_bookings))))

        validated_data.append({
            "leave_request_id" : item["leave_request_id"],
            "local_employee_number" : get_booking_details(item["leave_request_id"], "local_employee_number"),
            "employee_id" : get_booking_details(item["leave_request_id"], "employee_id"),
            "timeoff_type" : timeoff_type[0] if timeoff_type else null,
            "booking_start_date" : booking_start_date[0] if booking_start_date else null,
            "booking_end_date" : booking_end_date[0] if booking_end_date else null,
            "start_date_update" : "Yes" if start_date_update and start_date_update[0] == "Start Date" else "No",
            "end_date_update" : "Yes" if end_date_update and end_date_update[0] == "End Date" else "No",
            "original_start_date" : min(original_start_date).strftime(DATE_FORMAT_FROM_REPORT) if original_start_date else null,
            "original_end_date" : min(original_end_date).strftime(DATE_FORMAT_FROM_REPORT) if original_end_date else null,
            "cost_center_full_path" : get_booking_details(item["leave_request_id"], "cost_center_full_path"),
            "final_modified_date" : max_booking_modified_on_date
        })
    return validated_data

@functools.lru_cache(maxsize=128)
def get_previous_date():
    return rail.result("logging_details")["previous_date"]

def get_bookings_csv(item, lwp_type, status):
    if not item:
        return []
    return [
        null,
        item['local_employee_number'],
        item['employee_id'],
        lwp_type,
        datetime.strptime(item['booking_start_date'], DATE_FORMAT_FROM_REPORT).strftime(DATE_FORMAT_TO_EXPORT),
        datetime.strptime(item['booking_end_date'], DATE_FORMAT_FROM_REPORT).strftime(DATE_FORMAT_TO_EXPORT),
        rail.find_first_by_attr_and_get_attr(rail.result("logging_details")["required_timeoffs_mapper"],
            'timeoff_type', item['timeoff_type'], 'expected_timeoff_code'),
        get_previous_date(),
        status,
        item['cost_center_full_path'].split('/')[1].strip().split('|')[0].strip() if len(item['cost_center_full_path'].split('/')) > 1 else null
    ]

def get_modified_bookings_csv(item, lwp_start_date, lwp_end_date):
    if not item:
        return []
    return [
        null,
        item['local_employee_number'],
        item['employee_id'],
        "R",
        datetime.strptime(lwp_start_date, DATE_FORMAT_FROM_REPORT).strftime(DATE_FORMAT_TO_EXPORT) if lwp_start_date else null,
        datetime.strptime(lwp_end_date, DATE_FORMAT_FROM_REPORT).strftime(DATE_FORMAT_TO_EXPORT) if lwp_end_date else null,
        rail.find_first_by_attr_and_get_attr(rail.result("logging_details")["required_timeoffs_mapper"],
            'timeoff_type', item['timeoff_type'], 'expected_timeoff_code'),
        get_previous_date(),
        "Cancelled",
        item['cost_center_full_path'].split('/')[1].strip().split('|')[0].strip() if len(item['cost_center_full_path'].split('/')) > 1 else null
    ]

def load_logs():
    artifact_list = rail.result("get_bookings_data_artifacts")["value"]
    return json.dumps(list(itertools.chain.from_iterable(list(map(rail.load_all_records, artifact_list)))))

def get_payroll_data_csv_rows(item):
    if not item:
        return []
    return [
        null,
        item['EMP_ID'],
        item['GGID'],
        item['LWP_TYPE'],
        item['LWP_START_DATE'],
        item['LWP_END_DATE'],
        item['LWP_CODE'],
        item['MODIFIED_DATED'],
        item['REMARKS'],
        item['COMPANYNAME']
    ]
