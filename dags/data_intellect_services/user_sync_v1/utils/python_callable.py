from datetime import datetime
import pendulum
import rail

null = None

def split_startdate(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return {
        "year" : date_obj.year,
        "month" : date_obj.month,
        "day" : date_obj.day
    }

def get_logging_details(config):
    today = pendulum.now(config.time_zone)
    return {
        "time_zone": config.time_zone,
        "process_start_time": today.strftime("%Y-%m-%dT%H:%M:%S.%f%z"),
        "log_filename": f'Logs_DataIntellect_User_Sync_{today.strftime("%Y%m%dT%H%M%S")}.csv'
    }

def get_today_json(time_zone):
    today = pendulum.now(time_zone)
    return {
        "year": today.year,
        "month": today.month,
        "day": today.day
    }

def get_basic_updated_payload_for_collection():
    users_data_payload = rail.load_all_records(rail.result("create_user_data_collection"))
    users_basic_details_updated_data = list(filter(lambda user_details: user_details["action"] == 'Update' and
        user_details["type"] == 'Employee Update', users_data_payload))
    def merge_dicts(d1, d2):
        for k in d2:
            if d2[k] is not None:
                d1[k] = d2[k]
        return d1
    data_sorted = sorted(users_basic_details_updated_data, key=lambda x: datetime.strptime(x['timestamp'], "%Y-%m-%dT%H:%M:%S.%f"), reverse=True)
    latest_records = {}
    for record in data_sorted:
        if record['id'] in latest_records:
            latest_records[record['id']] = merge_dicts(latest_records[record['id']], record)
        else:
            latest_records[record['id']] = record
    latest_records_list = list(latest_records.values())

    return latest_records_list
