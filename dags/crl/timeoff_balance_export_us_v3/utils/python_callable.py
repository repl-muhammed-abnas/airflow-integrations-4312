from datetime import date
from pendulum import now
from rail import find_first_by_attr_and_get_attr, load_all_records, result
from crl.timeoff_balance_export_us_v3.mapper.time_off_balance_mapper import Time_off_mappper, sicked_timeoff_names
CHICAGO_CARRYOVER_LOCATIONS = ["CHICAGOCR", "CHICAGOCSS", "CHICAGOIIT", "CHICAGOGEMS"]
CARRYOVER_HOURS = 80

# pylint: disable=cell-var-from-loop
def get_time_in_formats(time_zone):
    current_time = now(time_zone)
    return {
        "start_time": str(current_time),
        "ymd_format": current_time.strftime("%Y%m%d"),
        "hms_format": current_time.strftime("%H%M%S")
    }

def _is_chicago_location(location_full_path):
    return any(loc in (location_full_path or '') for loc in CHICAGO_CARRYOVER_LOCATIONS)

def get_timeoff_values():
    data = load_all_records(result('query_report_data'))
    result_list = []
    for item in data:
        if item['timeoff_balance'] and float(item['timeoff_balance']) > 0:
            balance = float(item['timeoff_balance'])
            location = item.get('location_full_path', '')
            if _is_chicago_location(location):
                if balance <= CARRYOVER_HOURS:
                    continue
                balance = balance - CARRYOVER_HOURS
            time_off_type = list(filter(lambda x:  x['timeoff_type'] == item['timeoff_type'], Time_off_mappper))
            result_list.append({
                'empid': item['empid'],
                'timeoff_balance': balance,
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

def is_daily_export_active(config):
    current = now(config.time_zone)
    blackout_start = (config.daily_export_blackout_start_month, config.daily_export_blackout_start_day)
    blackout_end = (config.daily_export_blackout_end_month, config.daily_export_blackout_end_day)
    if (current.month == blackout_start[0] and current.day >= blackout_start[1]) or \
       (current.month == blackout_end[0] and current.day <= blackout_end[1]):
        return False
    return True

def get_last_saturday_of_december():
    year = now().year
    # December 31st and walk back to Saturday (weekday 5)
    dec_31 = date(year, 12, 31)
    days_after_saturday = (dec_31.weekday() - 5) % 7
    last_saturday = dec_31.replace(day=31 - days_after_saturday)
    return last_saturday.strftime("%Y-%m-%d")

def get_file_name(config):
    file_name = "P" + config.adp_gv_system + config.gv_system_number + "476" + \
            "_" + now(config.time_zone).strftime("%Y%m%d%H%M%S") + "_" + "USTIME_HRMD05_DUT8G2I"
    return file_name

def get_file_name_biweekly(config):
    file_name = "P" + config.adp_gv_system + config.gv_system_number + "476" + \
            "_" + now(config.time_zone).strftime("%Y%m%d%H%M%S") + "_" + "USTIME_HRMD07_DUT8G2I"
    return file_name
