from datetime import datetime
import pendulum
import rail
null = None
EFFECTIVE_DATE_FORMAT_BAMBOOHR = '%Y-%m-%d'

def get_basic_details_value(bamboohr_field, replicon_field):
    return (bamboohr_field if bamboohr_field != replicon_field else null)

def get_updated_user_basic_details(dag_run):
    user_details_in_replicon = dag_run.conf["replicon_user_details"]["userDetails"]
    user_details_in_bamboohr = dag_run.conf["user_details"]
    start_date_in_replicon = user_details_in_replicon["employmentDateRange"]["startDate"]
    end_date_in_replicon = user_details_in_replicon["employmentDateRange"]["endDate"]
    return {
        "firstname": get_basic_details_value(user_details_in_bamboohr["firstname"], user_details_in_replicon["firstName"]),
        "lastname": get_basic_details_value(user_details_in_bamboohr["lastname"], user_details_in_replicon["lastName"]),
        "workemail": get_basic_details_value(user_details_in_bamboohr["workemail"], user_details_in_replicon["emailAddress"]),
        "startdate": get_basic_details_value(user_details_in_bamboohr["startdate"]
            if user_details_in_bamboohr["startdate"] else null, (datetime(start_date_in_replicon['year'], start_date_in_replicon['month'],
                start_date_in_replicon['day']).strftime(EFFECTIVE_DATE_FORMAT_BAMBOOHR)) if start_date_in_replicon else null),
        "enddate": get_basic_details_value(user_details_in_bamboohr["terminationdate"]
            if user_details_in_bamboohr["terminationdate"] else null, (datetime(end_date_in_replicon['year'], end_date_in_replicon['month'],
                end_date_in_replicon['day']).strftime(EFFECTIVE_DATE_FORMAT_BAMBOOHR)) if end_date_in_replicon else null)
    }

def get_today_json(time_zone):
    today = pendulum.now(time_zone)
    return {
        "year": today.year,
        "month": today.month,
        "day": today.day
    }

def get_email_log_details(STANDARD_EMAIL_DATE_FORMAT):
    current_time = pendulum.now()
    start_time_str = rail.result("get_lastsync_time_and_current_time")["process_start_time"]
    return {
        "job_start_time": start_time_str,
        "job_end_time": current_time.strftime(STANDARD_EMAIL_DATE_FORMAT),
        "job_duration_minutes": round((current_time - datetime.strptime(start_time_str, STANDARD_EMAIL_DATE_FORMAT)).total_seconds() / 60, 1),
        "log_file_name": rail.result("get_log_filename"),
        "log_file_link": rail.result("generate_log_file_link"),
        "total_record_count": len(rail.load_all_records(rail.result("bamboohr_updated_employees_data")))
    }

def do_format_logs():
    log_artifacts = []
    log_records = []

    logs = rail.result("gather_user_logs")

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
        "ecid": log['ecid']
        }, log_records))

    rail.set_result(key="get_logged_success", val=len(list(filter(lambda item: item['status']=="Success", final_log_records))))
    rail.set_result(key="get_logged_errors", val=len(list(filter(lambda item: item['status']=="Error", final_log_records))))
    rail.set_result(key="get_logged_exceptions", val=len(list(filter(lambda item: item['status']=="Exception", final_log_records))))

    return final_log_records

def check_any_modifications():
    modifications = rail.result("get_update_modifications_user_payload")["modifications"]
    return bool(modifications["employeeTypeGroupScheduleToApply"]
        or modifications["departmentGroupScheduleToApply"]
        or modifications["costRateScheduleModifications"]
        or modifications["objectExtensionFieldsToApply"])

def no_data_updated():
    return (not rail.result("update_user_loginname_and_licenses") and not rail.result("update_user_loginname_in_replicon") and
        not rail.result("update_user_basic_details_in_replicon") and not rail.result("update_supervisor_for_user") and
            not rail.result("apply_modifications_on_user_in_replicon"))
