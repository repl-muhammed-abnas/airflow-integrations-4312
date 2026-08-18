from pendulum import now
from rail import find_first_by_attr_and_get_attr, load_all_records, result
from crl.timeoff_balance_export_us_v2.mapper.time_off_balance_mapper import Time_off_mappper, sicked_timeoff_names
# pylint: disable=cell-var-from-loop
def get_time_in_formats(time_zone):
    current_time = now(time_zone)
    return {
        "start_time": str(current_time),
        "ymd_format": current_time.strftime("%Y%m%d"),
        "hms_format": current_time.strftime("%H%M%S")
    }

def get_timeoff_values():
    data = load_all_records(result('query_report_data'))
    result_list = []
    for item in data:
        if item['timeoff_balance'] and float(item['timeoff_balance']) > 0:
            time_off_type = list(filter(lambda x:  x['timeoff_type'] == item['timeoff_type'], Time_off_mappper))
            result_list.append({
                'empid': item['empid'],
                'timeoff_balance': float(item['timeoff_balance']),
                'useruri':item['useruri'],
                'paycode': time_off_type[0]['paycode'],
                'user_start_date':item['user_start_date'],
                'user_end_date':item['user_end_date'],
                "update_spo_udf":"yes" if item['timeoff_type'] in sicked_timeoff_names else "no",
            })
    return result_list

def get_users_not_eligible_but_have_udf_yes():
    data = load_all_records(result('query_report_data'))
    result_list = []
    for item in data:
        if item['timeoff_balance'] and float(item['timeoff_balance']) <= 0:
            time_off_type = list(filter(lambda x:  x['timeoff_type'] == item['timeoff_type'], Time_off_mappper))
            result_list.append({
                'empid': item['empid'],
                'timeoff_balance': float(item['timeoff_balance']),
                'useruri':item['useruri'],
                'paycode': time_off_type[0]['paycode'],
                'user_start_date':item['user_start_date'],
                'user_end_date':item['user_end_date'],
                "update_spo_udf":"yes" if item['timeoff_type'] in sicked_timeoff_names else "no",
            })
    return result_list

def if_payroll_processing_date_is_today(config):
    current_date = now(config.time_zone).strftime("%d-%m-%Y")
    return bool(find_first_by_attr_and_get_attr(
        config.USA_PAYROLL_CALENDER_MAPPER_TO_USE, "payroll_processing_date", current_date))

def get_file_name(config):
    file_name = "P" + config.adp_gv_system + config.gv_system_number + "476" + \
            "_" + now(config.time_zone).strftime("%Y%m%d%H%M%S") + "_" + "USTIME_HRMD05_DUT8G2I"
    return file_name

def get_file_name_biweekly(config):
    file_name = "P" + config.adp_gv_system + config.gv_system_number + "476" + \
            "_" + now(config.time_zone).strftime("%Y%m%d%H%M%S") + "_" + "USTIME_HRMD07_DUT8G2I"
    return file_name
