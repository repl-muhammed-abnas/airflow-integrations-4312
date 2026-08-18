import rail
from pendulum import now
from datetime import datetime, timedelta
import json

_MODIFIEDON_FORMAT = "%m/%d/%Y %H:%M:%S"
_LAST_RUN_VAR_FORMAT = "%Y-%m-%d %H:%M:%S"


def capture_run_start_time():
    from airflow.models import Variable
    from repliconinc.timeoff_sync_to_polaris import config as _cfg
    _now = now(_cfg.timezone_utc)
    last_run_str = Variable.get(_cfg.last_run_var_name, default_var=None)
    if not last_run_str:
        yesterday_midnight = (_now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        last_run_str = yesterday_midnight.strftime(_LAST_RUN_VAR_FORMAT)
    return {
        "last_run_time": last_run_str,
        "run_start_time": _now.strftime(_LAST_RUN_VAR_FORMAT),
    }


def filter_records_by_modifiedon():
    records = rail.load_all_records(rail.result("data1")) or []
    context = rail.result("capture_run_start_time")
    last_run_dt = datetime.strptime(context["last_run_time"], _LAST_RUN_VAR_FORMAT)
    filtered = []
    for rec in records:
        modifiedon_str = (rec.get("modifiedon") or "").strip()
        if not modifiedon_str:
            continue
        try:
            rec_dt = datetime.strptime(modifiedon_str, _MODIFIEDON_FORMAT)
            if rec_dt > last_run_dt:
                filtered.append(rec)
        except ValueError:
            continue
    return json.dumps(filtered)


def update_last_run_var():
    from airflow.models import Variable
    from repliconinc.timeoff_sync_to_polaris import config as _cfg
    context = rail.result("capture_run_start_time")
    Variable.set(_cfg.last_run_var_name, context["run_start_time"])
def check_decimal_workdays_equals_duration(dag_run, user_timeoff_details, duration):
    return bool(rail.find_first_by_attr_and_get_attr(rail.result(user_timeoff_details),
        "timeOffType.name", dag_run.conf["timeoff_type"], "totalDuration.decimalWorkdays") == duration)

def check_cal_day_duration_equals_timeoffhrs(dag_run, user_timeoff_details, hours, minutes):
    return bool(rail.find_first_by_attr_and_get_attr(rail.result(user_timeoff_details),
                "timeOffType.name", dag_run.conf["timeoff_type"], hours) == int(dag_run.conf["timeoff_hrs"]["hours"]) and
                    rail.find_first_by_attr_and_get_attr(rail.result(user_timeoff_details),
                        "timeOffType.name", dag_run.conf["timeoff_type"], minutes) == int(dag_run.conf["timeoff_hrs"]["minutes"]))

def check_if_timeoff_present(dag_run):
    if dag_run.conf["type"] == "F":
        return bool(check_decimal_workdays_equals_duration(dag_run, "get_time_off_details_for_user_and_date_range_1",
            int(float(dag_run.conf["duration"]))))

    if dag_run.conf["type"] == "P":
        return bool(check_decimal_workdays_equals_duration(dag_run, "get_time_off_details_for_user_and_date_range_1",
            float(dag_run.conf["duration"])))

    if dag_run.conf["type"] == "N":
        return check_cal_day_duration_equals_timeoffhrs(dag_run, "get_time_off_details_for_user_and_date_range_1",
            "totalDuration.hours", "totalDuration.minutes")

    return False

def check_if_timeoff_booking_with_hours_mins_present_to_delete(dag_run):
    return check_cal_day_duration_equals_timeoffhrs(dag_run, "get_time_off_details_for_user_and_date_range_2",
            "totalDuration.calendarDayDuration.hours", "totalDuration.calendarDayDuration.minutes")

def check_if_timeoff_booking_with_decimal_workdays_present_to_delete(dag_run):
    return bool(check_decimal_workdays_equals_duration(dag_run, "get_time_off_details_for_user_and_date_range_2",
        int(float(dag_run.conf["duration"])))
            or check_decimal_workdays_equals_duration(dag_run, "get_time_off_details_for_user_and_date_range_2",
                float(dag_run.conf["duration"])))
    
def convert_decimal_to_seconds():
    conf = rail.get_dag_run_conf()
    try:
        hours = float(conf.get("timeoff_hrs", {}).get("hours", 0))
        minutes = float(conf.get("timeoff_hrs", {}).get("minutes", 0))
        decimal_hours = hours + (minutes / 60.0)
        seconds = int(decimal_hours * 3600)
    except (TypeError, ValueError, KeyError):
        seconds = 0
    return {"seconds": seconds}

def do_format_logs(dag_run):
    """Format logs from various sources into a consistent structure"""
    log_artifacts = []
    log_records = []

    userlogs = dag_run.conf.get('userlogs')
    otherlogs = dag_run.conf.get('otherlogs')

    if userlogs:
        if isinstance(userlogs, list):
            log_artifacts.extend(userlogs)
        else:
            log_artifacts.append(userlogs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        else:
            log_artifacts.append(otherlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []
    final_log_records = list(map(lambda log: {
        **{
            'jobid': log.get('ecid', '')
        },
        **log.get('properties', {}),
    }, log_records))

    rail.set_result(key="error_record_count", val=len(
        list(filter(lambda x: x.get('status') == 'Error', final_log_records))))
    rail.set_result(key="success_record_count", val=len(
        list(filter(lambda x: x.get('status') == 'Success', final_log_records))))
    rail.set_result(key="exception_record_count", val=len(
        list(filter(lambda x: x.get('status') == 'Exception', final_log_records))))
    rail.set_result(key="total_record_count", val=dag_run.conf.get('total_records', 0))

    return final_log_records


def get_email_details_callable(dag_run, time_zone):
    _now = now(time_zone)
    job_start_time_str = dag_run.conf.get('job_start_time', '')
    job_duration = 0
    if job_start_time_str:
        try:
            job_start = datetime.strptime(job_start_time_str.replace('+0000', '+00:00'), "%Y-%m-%dT%H:%M:%S%z")
            job_duration = ((_now - job_start).seconds) // 60
        except (ValueError, TypeError):
            job_duration = 0
    
    return {
        "job_end_time": _now.isoformat(),
        "job_duration": job_duration,
        "log_timestamp": _now.strftime("%Y%m%dT%H%M%S"),
        "email_timestamp": _now.isoformat(),
        "log_file_name": f"TimeoffSyncLog_{_now.strftime('%Y%m%dT%H%M%S')}.csv"
    }