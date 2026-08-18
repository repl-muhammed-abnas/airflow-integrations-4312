from json import dumps
from math import ceil
from pendulum import now
from rail import result, load_all_records, render_template
from mammoet.payroll_export_italy.utils.request_payload import get_time_export_date_range

EXPORT_DATE_FORMAT = "%Y%m%d%H%M%S"


def get_logging_details_callable(config):
    today = now(config.time_zone).strftime(EXPORT_DATE_FORMAT)
    return {
        "payroll_export_name": f"{config.PAYROLL_FILE_PREFIX}PayRoll_Export_{today}",
        "no_data_payroll_export_name": f"NO_DATA {config.PAYROLL_FILE_PREFIX}PayRoll_Export_{today}",
        "paycodes": "', '".join(map(lambda pay_code: str(pay_code['PayCode']), config.PAYCODES))
    }


def create_json_payload_callable(task_id):
    return dumps({
        "data": load_all_records(result(task_id))
    })


def get_log_to_sumo_extra_info(dag_run, config):
    posting_batch_count = ceil(
        (result('filter_payroll_data', 'length') or 0)/config.API_JSON_PAYLOAD_LIMIT)
    start_date, end_date = get_time_export_date_range(dag_run)
    payroll_export_name = result('get_logging_details')['payroll_export_name'] if (result(
        'filter_payroll_data', 'length') or -1) > 0 else result('get_logging_details')['no_data_payroll_export_name']
    return {
        "payroll_export_run_type": dag_run.conf["payroll_export_run_type"],
        "timezone": dag_run.conf['timezone'],
        "payroll_location_name": dag_run.conf['payroll_location_name'],
        "payroll_export_start_date": start_date.strftime("%Y-%m-%d"),
        "payroll_export_end_date": end_date.strftime("%Y-%m-%d"),
        "is_exported": "Yes" if render_template("{{get_task_state('send_success_email')}}").lower() == "success" else "No",
        "payroll_export_name": payroll_export_name,
        "total_valid_exported_records": result('filter_payroll_data', 'length'),
        "pwb_record_count": result('create_payroll_collection', 'length'),
        "posting_batch_count": posting_batch_count,
        "payroll_export_file_names": [f'{payroll_export_name}_{idx+1}' for idx in range(0, posting_batch_count)]
    }

def get_can_process_task(dag_run, config):
    if dag_run.conf["payroll_export_run_type"] == 'daily':
        return True
    if dag_run.conf["payroll_export_run_type"] == 'monthly':
        if now(tz=config.time_zone).day == 2:
            return True 
        return False
    raise Exception("Payroll export type is other than daily or monthly")
