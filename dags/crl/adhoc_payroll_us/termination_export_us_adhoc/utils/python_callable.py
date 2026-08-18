from datetime import datetime as dt
import pendulum
import rail
from dateutil.relativedelta import relativedelta

def get_time_in_formats(time_zone):
    current_time = pendulum.now(time_zone)
    return {
        "start_time": str(current_time),
        "ymd_format": current_time.strftime("%Y%m%d"),
        "hms_format": current_time.strftime("%H%M%S")
    }


def get_all_required_employee_types(mapper):
    return [rail.find_first_by_attr_and_get_attr(rail.result('get_all_employee_type'),
                                                 "displaytext", data['employee_type_name'], "uri") for data in mapper if data["export"] == "yes"]


def get_filtered_allowed_location_uris(response):
    if not response['rows']:
        return []
    location_list = list(filter(lambda x: x['displaytext'] == "USA", list(map(lambda item: {
        "uri": item['cells'][0]['uri'],
        "displaytext": item['cells'][1]['cellCollection'][0]['textValue']
    }, response['rows']))))

    return [item['uri'] for item in location_list]


def getenabledemployee(response):
    response = response.json()['d']
    if not response:
        return []

    return list(set(map(lambda data: data['cells'][0]['uri'], response['rows'])))


def get_employeetype(response):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['uri'], list(map(lambda item: {
        "uri": item['uri'],
        "displaytext": item['displayText']
    }, response))))


def get_hourly_employee_types(mapper):
    return (data['employee_type_name'] for data in mapper if data["export"] == "yes")

def get_start_date_begin_of_week():
    start_date=dt.utcnow() + relativedelta(months=-3)
    return dt.strftime(start_date, "%Y-%m-%d")


def get_end_date_begin_of_week():
    return dt.utcnow().strftime("%Y-%m-%d")
