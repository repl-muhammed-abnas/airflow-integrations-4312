from datetime import timedelta
import pendulum
import rail
from assuredpartnersinc.timeoff_balance_export_v1.mapper.timeoff_export_mapper import timeoff_types_mapper

null = None


def logging_details(time_zone):
    return {
        "dag_run_start_time": pendulum.now(time_zone).isoformat(),
        "dateaccruedthru": str((pendulum.now(time_zone)-timedelta(days=14)).strftime("%m/%d/%Y")),
        "periodenddate": str((pendulum.now(time_zone)-timedelta(days=1)).strftime("%m/%d/%Y")),
        "jobdateformatted": str((pendulum.now(time_zone)).strftime("%m_%d_%Y")),
        "time_zone": time_zone
    }


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def calculate_timeoff(timeoff_data, daily_hours, units):
    return float(timeoff_data) * float(daily_hours) if "Workdays" in units and timeoff_data != '' and daily_hours != '' else timeoff_data


def get_filtered_users_timeoff_data(item):
    if not item:
        return []
    res = {
        "employeeid": item["employeeid"],
        "companycode": item["companycode"],
        "timeofftype": item["timeofftype"],
        "timeoffaccrued": calculate_timeoff(item["timeoffaccrued"], item["dailyhours"], item["units"]),
        "timeofftaken": calculate_timeoff(item["timeofftaken"], item["dailyhours"], item["units"]),

        "timeoffbalance": round(float(str(calculate_timeoff(item["timeoffbalance"], item["dailyhours"], item["units"])).replace(",", "")) +
                                float(str(calculate_timeoff(item["timeofftaken"], item["dailyhours"], item["units"])).replace(",", "")), 2),

        "headercode": " ".join(list(map(lambda timeoff_type: timeoff_type["Header  code"],
                                        filter(lambda timeoff_type: timeoff_type["replicon name"] == item["timeofftype"], timeoff_types_mapper)))),
        "ptocode": " ".join(list(map(lambda timeoff_type: timeoff_type["PT o  code on  export"],
                                     filter(lambda timeoff_type: timeoff_type["replicon name"] == item["timeofftype"], timeoff_types_mapper)))),
        "initialtimeoffbalance": item["timeoffbalance"],
        "timeoffbalanceupdated": calculate_timeoff(item["timeoffbalance"], item["dailyhours"], item["units"])
    }

    return {k: v if v is not null else '' for k, v in res.items()}


def get_date_accrued_thru(ptocode):
    dag_run_conf = get_dag_run_conf()
    return dag_run_conf['dateaccruedthru'] if ptocode is not null and ptocode != '' else null

def get_period_end_data(ptocode):
    dag_run_conf = get_dag_run_conf()
    return dag_run_conf["periodenddate"] if ptocode is not null and ptocode != '' else null

def get_timeoff_balance_data(timeoff_balance, ptocode):
    return (timeoff_balance if timeoff_balance is not null and timeoff_balance != '' else 0) if ptocode is not null and ptocode != '' else null


def get_timeoff_taken_data(timeoff_taken, ptocode):
    return (timeoff_taken if timeoff_taken is not null and timeoff_taken != '' else 0) if ptocode is not null and ptocode != '' else null


def get_users_timeoff_rows(item):
    row_data = [
        item['employeeid'],
        item['ptocode_ptocode'],
        get_date_accrued_thru(item['ptocode_ptocode']),
        get_period_end_data(item['ptocode_ptocode']),
        get_timeoff_balance_data(
            item['ptocode_timeoffbalance'], item['ptocode_ptocode']),
        get_timeoff_taken_data(
            item['ptocode_timeofftaken'], item['ptocode_ptocode']),
        item['holiday_ptocode'],
        get_date_accrued_thru(item['holiday_ptocode']),
        get_period_end_data(item['holiday_ptocode']),
        get_timeoff_balance_data(
            item['holiday_timeoffbalance'], item['holiday_ptocode']),
        get_timeoff_taken_data(
            item['holiday_timeofftaken'], item['holiday_ptocode']),
        item['sick_ptocode'],
        get_date_accrued_thru(item['sick_ptocode']),
        get_period_end_data(item['sick_ptocode']),
        get_timeoff_balance_data(
            item['sick_timeoffbalance'], item['sick_ptocode']),
        get_timeoff_taken_data(
            item['sick_timeofftaken'], item['sick_ptocode']),
        item['volunteer_ptocode'],
        get_date_accrued_thru(item['volunteer_ptocode']),
        get_period_end_data(item['volunteer_ptocode']),
        get_timeoff_balance_data(
            item['volunteer_timeoffbalance'], item['volunteer_ptocode']),
        get_timeoff_taken_data(
            item['volunteer_timeofftaken'], item['volunteer_ptocode']),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    ]
    return row_data


def get_merging_query(collection_code):
    if collection_code == "user_pto":
        return '''SELECT employeeid, companycode, timeofftype, timeoffaccrued, timeofftaken, timeoffbalance, headercode, ptocode
                FROM unique_users_timeoff_data
                LEFT JOIN pto_1_code_data
                ON employeeid = employeeid1'''
    if collection_code == "user_pto_holiday":
        return '''SELECT employeeid, companycode, ptocode_timeofftype, ptocode_timeoffaccrued, ptocode_timeofftaken, ptocode_timeoffbalance,
                    ptocode_headercode, ptocode_ptocode, timeofftype, timeoffaccrued, timeofftaken, timeoffbalance, headercode,
                    ptocode
                FROM user_pto_code_data
                LEFT JOIN holiday_code_data
                ON employeeid = employeeid1'''
    if collection_code == "user_pto_holiday_sick":
        return '''SELECT employeeid, companycode, ptocode_timeofftype, ptocode_timeoffaccrued, ptocode_timeofftaken, ptocode_timeoffbalance,
                    ptocode_headercode, ptocode_ptocode, holiday_timeoffaccrued, holiday_timeofftaken, holiday_timeoffbalance,
                    holiday_headercode, holiday_ptocode, timeofftype, timeoffaccrued, timeofftaken, timeoffbalance, headercode,
                    ptocode
                FROM user_pto_holiday_code_data
                LEFT JOIN sick_code_data
                ON employeeid = employeeid1'''
    if collection_code == "user_pto_hday_sick_vol":
        return '''SELECT employeeid, companycode, ptocode_timeofftype, ptocode_timeoffaccrued, ptocode_timeofftaken, ptocode_timeoffbalance,
                    ptocode_headercode, ptocode_ptocode, holiday_timeoffaccrued, holiday_timeofftaken, holiday_timeoffbalance,
                    holiday_headercode, holiday_ptocode,
                    sick_timeoffaccrued, sick_timeofftaken, sick_timeoffbalance, sick_headercode, sick_ptocode,
                    timeofftype, timeoffaccrued, timeofftaken, timeoffbalance, headercode, ptocode
                FROM user_pto_holiday_sick_code_data
                LEFT JOIN volunteer_code_data
                ON employeeid = employeeid1'''
    if collection_code == "user_pto_hday_sick_vol_emergsick":
        return '''SELECT employeeid, companycode, ptocode_timeofftype, ptocode_timeoffaccrued, ptocode_timeofftaken, ptocode_timeoffbalance,
                    ptocode_headercode, ptocode_ptocode, holiday_timeoffaccrued, holiday_timeofftaken, holiday_timeoffbalance,
                    holiday_headercode, holiday_ptocode,
                    sick_timeoffaccrued, sick_timeofftaken, sick_timeoffbalance, sick_headercode, sick_ptocode,
                    volunteer_timeoffaccrued, volunteer_timeofftaken, volunteer_timeoffbalance, volunteer_headercode, volunteer_ptocode,
                    timeofftype, timeoffaccrued, timeofftaken, timeoffbalance, headercode, ptocode
                FROM user_pto_hday_sick_vol_code_data
                LEFT JOIN emerg_sick_code_data
                ON employeeid = employeeid1'''
    return null


def get_timeoff_code_data_query(code):
    if code == "pto_1":
        return 'SELECT * FROM processed_timeoff_data WHERE headercode = "PTO-1 Code" AND headercode != ""'
    if code == "holiday":
        return 'SELECT * FROM processed_timeoff_data WHERE headercode = "Holiday Code" AND headercode != ""'
    if code == "sick":
        return 'SELECT * FROM processed_timeoff_data WHERE headercode = "Sick Code" AND headercode != ""'
    if code == "volunteer":
        return 'SELECT * FROM processed_timeoff_data WHERE headercode = "Volunteer Code" AND headercode != ""'
    if code == "emerg_sick":
        return 'SELECT * FROM processed_timeoff_data WHERE headercode = "Emerg Sick Code" AND headercode != ""'
    return null


def get_timeoff_data_columns(timeoff_data_columns):
    if timeoff_data_columns == "user_pto_data_columns":
        return {
            "employeeid": "employeeid",
            "companycode": "companycode",
            "timeofftype": "ptocode_timeofftype",
            "timeoffaccrued": "ptocode_timeoffaccrued",
            "timeofftaken": "ptocode_timeofftaken",
            "timeoffbalance": "ptocode_timeoffbalance",
            "headercode": "ptocode_headercode",
            "ptocode": "ptocode_ptocode"
        }
    if timeoff_data_columns == "user_pto_holiday_data_columns":
        return {
            "employeeid": "employeeid",
            "companycode": "companycode",
            "ptocode_timeofftype": "ptocode_timeofftype",
            "ptocode_timeoffaccrued": "ptocode_timeoffaccrued",
            "ptocode_timeofftaken": "ptocode_timeofftaken",
            "ptocode_timeoffbalance": "ptocode_timeoffbalance",
            "ptocode_headercode": "ptocode_headercode",
            "ptocode_ptocode": "ptocode_ptocode",
            "timeofftype": "holiday_timeofftype",
            "timeoffaccrued": "holiday_timeoffaccrued",
            "timeofftaken": "holiday_timeofftaken",
            "timeoffbalance": "holiday_timeoffbalance",
            "headercode": "holiday_headercode",
            "ptocode": "holiday_ptocode"
        }
    if timeoff_data_columns == "user_pto_holiday_sick_data_columns":
        return {
            "employeeid": "employeeid",
            "companycode": "companycode",
            "ptocode_timeofftype": "ptocode_timeofftype",
            "ptocode_timeoffaccrued": "ptocode_timeoffaccrued",
            "ptocode_timeofftaken": "ptocode_timeofftaken",
            "ptocode_timeoffbalance": "ptocode_timeoffbalance",
            "ptocode_headercode": "ptocode_headercode",
            "ptocode_ptocode": "ptocode_ptocode",
            "holiday_timeofftype": "holiday_timeofftype",
            "holiday_timeoffaccrued": "holiday_timeoffaccrued",
            "holiday_timeofftaken": "holiday_timeofftaken",
            "holiday_timeoffbalance": "holiday_timeoffbalance",
            "holiday_headercode": "holiday_headercode",
            "holiday_ptocode": "holiday_ptocode",
            "timeofftype": "sick_timeofftype",
            "timeoffaccrued": "sick_timeoffaccrued",
            "timeofftaken": "sick_timeofftaken",
            "timeoffbalance": "sick_timeoffbalance",
            "headercode": "sick_headercode",
            "ptocode": "sick_ptocode"
        }
    if timeoff_data_columns == "user_pto_hday_sick_vol_data_columns":
        return {
            "employeeid": "employeeid",
            "companycode": "companycode",
            "ptocode_timeofftype": "ptocode_timeofftype",
            "ptocode_timeoffaccrued": "ptocode_timeoffaccrued",
            "ptocode_timeofftaken": "ptocode_timeofftaken",
            "ptocode_timeoffbalance": "ptocode_timeoffbalance",
            "ptocode_headercode": "ptocode_headercode",
            "ptocode_ptocode": "ptocode_ptocode",
            "holiday_timeofftype": "holiday_timeofftype",
            "holiday_timeoffaccrued": "holiday_timeoffaccrued",
            "holiday_timeofftaken": "holiday_timeofftaken",
            "holiday_timeoffbalance": "holiday_timeoffbalance",
            "holiday_headercode": "holiday_headercode",
            "holiday_ptocode": "holiday_ptocode",
            "sick_timeofftype": "sick_timeofftype",
            "sick_timeoffaccrued": "sick_timeoffaccrued",
            "sick_timeofftaken": "sick_timeofftaken",
            "sick_timeoffbalance": "sick_timeoffbalance",
            "sick_headercode": "sick_headercode",
            "sick_ptocode": "sick_ptocode",
            "timeofftype": "volunteer_timeofftype",
            "timeoffaccrued": "volunteer_timeoffaccrued",
            "timeofftaken": "volunteer_timeofftaken",
            "timeoffbalance": "volunteer_timeoffbalance",
            "headercode": "volunteer_headercode",
            "ptocode": "volunteer_ptocode"
        }
    if timeoff_data_columns == "user_pto_hday_sick_vol_emergsick_data_columns":
        return {
            "employeeid": "employeeid",
            "companycode": "companycode",
            "ptocode_timeofftype": "ptocode_timeofftype",
            "ptocode_timeoffaccrued": "ptocode_timeoffaccrued",
            "ptocode_timeofftaken": "ptocode_timeofftaken",
            "ptocode_timeoffbalance": "ptocode_timeoffbalance",
            "ptocode_headercode": "ptocode_headercode",
            "ptocode_ptocode": "ptocode_ptocode",
            "holiday_timeofftype": "holiday_timeofftype",
            "holiday_timeoffaccrued": "holiday_timeoffaccrued",
            "holiday_timeofftaken": "holiday_timeofftaken",
            "holiday_timeoffbalance": "holiday_timeoffbalance",
            "holiday_headercode": "holiday_headercode",
            "holiday_ptocode": "holiday_ptocode",
            "sick_timeofftype": "sick_timeofftype",
            "sick_timeoffaccrued": "sick_timeoffaccrued",
            "sick_timeofftaken": "sick_timeofftaken",
            "sick_timeoffbalance": "sick_timeoffbalance",
            "sick_headercode": "sick_headercode",
            "sick_ptocode": "sick_ptocode",
            "volunteer_timeofftype": "volunteer_timeofftype",
            "volunteer_timeoffaccrued": "volunteer_timeoffaccrued",
            "volunteer_timeofftaken": "volunteer_timeofftaken",
            "volunteer_timeoffbalance": "volunteer_timeoffbalance",
            "volunteer_headercode": "volunteer_headercode",
            "volunteer_ptocode": "volunteer_ptocode",
            "timeofftype": "emergsick_timeofftype",
            "timeoffaccrued": "emergsick_timeoffaccrued",
            "timeofftaken": "emergsick_timeofftaken",
            "timeoffbalance": "emergsick_timeoffbalance",
            "headercode": "emergsick_headercode",
            "ptocode": "emergsick_ptocode"
        }
    return null


def get_query_merge_master():
    if rail.result("get_filtered_enabled_users_timeoff_data") != null and rail.result("get_filtered_disabled_users_timeoff_data") != null:
        return 'SELECT * FROM processed_enabled_users_timeoff_data UNION ALL SELECT * FROM processed_disabled_users_timeoff_data'

    if rail.result("get_filtered_enabled_users_timeoff_data") != null:
        return 'SELECT * FROM processed_enabled_users_timeoff_data'

    if rail.result("get_filtered_disabled_users_timeoff_data") != null:
        return 'SELECT * FROM processed_disabled_users_timeoff_data'

    return null
