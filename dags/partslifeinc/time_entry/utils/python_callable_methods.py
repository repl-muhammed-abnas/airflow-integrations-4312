from datetime import datetime, timedelta
import rail

MANDATORY_FIELDS = {
    "timesheet_entry_date": "Month, Day, Year of Ticket Date",
    "employeename": "Employee Name",
    "punch_in_hr": "Hour of Attendance_Detail__Adjusted_Clock_In",
    "punch_in_min": "Minute of Attendance_Detail__Adjusted_Clock_In",
    "punch_out_hr": "Hour of Attendance_Detail__Adjusted_Clock_Out",
    "punch_out_min": "Minute of Attendance_Detail__Adjusted_Clock_Out",
}


def get_missing_field_message(item):
    missing_fields = []
    for key, log_value in MANDATORY_FIELDS.items():
        if not item[key]:
            missing_fields.append(f"{log_value} not present in the input")
    return rail.smartjoin_by_delim(missing_fields, ";")

def group_records_user_and_date(entries):
    grouped_data = {}
    for entry in entries:
        employee_name = entry['employeename']
        entry_date = entry['timesheet_entry_date']
        grouped_data.setdefault(employee_name,{})
        grouped_data[employee_name].setdefault(entry_date,[])
        grouped_data[employee_name][entry_date].append(entry)

    result = []
    for employee, dates in grouped_data.items():
        for date, data in dates.items():
            result.append({
                "employee": employee,
                "timesheet_entry_date": date,
                "data": data
            })
    return result

def validate_date_format(date_str):
    if not date_str:
        return None
    try:
        date = datetime.strptime(date_str, "%B %d, %Y")
        return {"year": date.year, "month": date.month, "day": date.day}
    except:  # pylint: disable=bare-except
        return None

def is_overlap(interval1, interval2):
    # Convert input intervals to datetime objects
    start1 = datetime(2024, 4, 28, int(interval1["punch_in_hr"]), int(interval1["punch_in_min"])) + timedelta(seconds=1)
    end1 = datetime(2024, 4, 28, int(interval1["punch_out_hr"]), int(interval1["punch_out_min"]))
    start2 = datetime(2024, 4, 28, int(interval2["punch_in_hr"]), int(interval2["punch_in_min"])) + timedelta(seconds=1)
    end2 = datetime(2024, 4, 28, int(interval2["punch_out_hr"]), int(interval2["punch_out_min"]))

    # Check for overlap
    return start1 <= end2 and start2 <= end1

def update_overlapping_entries(data):
    for i, interval in enumerate(data):
        for j, other_interval in enumerate(data):
            if i != j and is_overlap(interval, other_interval):
                data[i]["overlap"] = True
                break
        else:
            data[i]["overlap"] = False

def get_timeentry_oef_uri(resp):
    return {
        'attendance_code': rail.find_first_by_attr_and_get_attr(resp, 'displayText', 'Attendance Code', 'uri', ''),
        'end_item': rail.find_first_by_attr_and_get_attr(resp, 'displayText', 'End Item', 'uri', '')
    }

def do_format_logs():
    def load_records(log_artifact):
        try:
            logs = rail.load_all_records(log_artifact)
            return logs
        except:  # pylint: disable=bare-except
            return []

    log_artifacts = []
    if rail.result("create_time_entry_logs"):
        log_artifacts.append(rail.result("create_time_entry_logs"))

    log_records = []

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = load_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = list(
        map(
            lambda x: {
                **{k: v for k, v in x["properties"].items() if k != "email"},
                **{"jobid": x["ecid"]},
            },
            log_records,
        )
    )
    return final_log_records
