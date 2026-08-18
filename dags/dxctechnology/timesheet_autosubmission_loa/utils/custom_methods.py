from datetime import datetime
from dateutil.relativedelta import relativedelta
import rail

def get_report_dates_per_erp_each_month():
    run_date = datetime.now()
    # This function is used to get the report dates for each month
    # It will return a list of dictionaries with the start and end dates for each month
    report_start_end_date = []
    for month_id in range(1, 13):
        month_start_date = run_date - relativedelta(months=month_id)
        month_end_date = run_date - relativedelta(months=month_id-1)
        report_start_end_date.append({
            'report_start_date': month_start_date.strftime("%Y-%m-%d"),
            'report_end_date': month_end_date.strftime("%Y-%m-%d")
        })
    return report_start_end_date

def do_format_logs(dag_run):
    log_artifacts = []
    log_records = []

    timesheet_logs = rail.result('gather_logs')

    if timesheet_logs:
        if isinstance(timesheet_logs, list):
            log_artifacts.extend(timesheet_logs)
        else:
            log_artifacts.append(timesheet_logs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **{
            'ecid': log['ecid'],
            'message': log['message']
        },
        **log['properties'],
        }, log_records))

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['status'] == 'Error', final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['status'] == 'Success', final_log_records ))))

    return final_log_records
