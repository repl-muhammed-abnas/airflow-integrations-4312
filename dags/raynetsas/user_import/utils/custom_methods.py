from rail import load_all_records, set_result, result, write_json_artifact

def do_format_logs():
    log_records = load_all_records(result("create_log"))

    set_result(key="get_successful_logs", val=len(list(filter(lambda item: item['properties']['status']=="Success", log_records))))
    set_result(key="get_errored_logs", val=len(list(filter(lambda item: item['properties']['status']=="Error", log_records))))
    set_result(key="get_exception_logs", val=len(list(filter(lambda item: item['properties']['status']=="Exception", log_records))))
    set_result(key="get_skipped_logs", val=len(list(filter(lambda item: item['properties']['status']=="Skipped", log_records))))

    return write_json_artifact(log_records)
