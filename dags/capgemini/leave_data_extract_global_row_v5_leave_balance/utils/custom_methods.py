import functools
import pendulum
import rail


def get_logging_details(time_zone):
    today = pendulum.now(time_zone)
    return {
        "time_zone": time_zone,
        "process_start_time": today.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
    }


def get_file_name(filename_format):
    now = pendulum.now()
    timestamp = now.strftime('%Y%m%d_%H%M%S')
    return f"{filename_format}_{timestamp}"


def create_batch_creation_datetime(response):
    creation_time = response["creationTime"]
    return pendulum.datetime(
        creation_time["year"], creation_time["month"], creation_time["day"],
        creation_time["hour"], creation_time["minute"], creation_time["second"]
    ).strftime("%d/%m/%Y %H:%M:%S")


@functools.lru_cache(maxsize=128)
def get_batch_creation_datetime():
    return rail.result("get_batch_creation_time")


def get_csv_row_data(item):
    return [
        item["Employee ID"],
        item["Local Employee Number"],
        item["Time Off Type"],
        item["Time Off Type Description"],
        item["Leave Carry Forward"],
        item["Leave Accrued"],
        item["Leave Availed"],
        item["Leave Reset"],
        item["Leave Balance"],
        item["Units"],
        get_batch_creation_datetime(),
        item["User End Date"]
    ]
