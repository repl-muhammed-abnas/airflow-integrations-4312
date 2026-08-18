from datetime import datetime
import rail

MANDATORY_FIELDS = {
    "employeeid": "employeeid",
    "timeoffaction": "timeoffaction",
    "timeofftype": "timeofftype",
    "timeoffdate": "timeoffdate",
    "amount": "amount",
    "unit": "unit",
}


def get_missing_field_message(item):
    missing_fields = []
    for key, log_value in MANDATORY_FIELDS.items():
        if not item[key]:
            missing_fields.append(f"{log_value} not present in the input")
    return rail.smartjoin_by_delim(missing_fields, ";")


def validate_date_format(date_str):
    if not date_str:
        return None
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d")
        return {"year": date.year, "month": date.month, "day": date.day}
    except:  # pylint: disable=bare-except
        return None


def do_format_logs():
    def load_records(log_artifact):
        try:
            logs = rail.load_all_records(log_artifact)
            return logs
        except:  # pylint: disable=bare-except
            return []

    log_artifacts = []
    if rail.result("create_timeoff_import_logs"):
        log_artifacts.append(rail.result("create_timeoff_import_logs"))

    if rail.result("gather_timeoff_import_child_logs"):
        log_artifacts.extend(rail.result("gather_timeoff_import_child_logs"))

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

    rail.set_result(key="error_record_count", val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    return final_log_records
