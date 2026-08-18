# pylint: disable=too-many-statements line-too-long
from datetime import datetime
import calendar
import pendulum
import rail


def check_for_trigger_day(time_zone):
    current_datetime = pendulum.now(time_zone)
    current_calendar = calendar.monthrange(
        current_datetime.year, current_datetime.month)
    last_date = current_calendar[1]
    date_today = datetime(current_datetime.year,
                          current_datetime.month, current_datetime.day)
    last_date_of_month = datetime(
        current_datetime.year, current_datetime.month, last_date)
    return last_date_of_month == date_today


def get_uri_list():
    new_list = []
    for entry in rail.result('get_enabled_employees')['rows']:
        textValue = rail.find_first_by_attr_and_get_attr(
            entry['cells'], 'dataType', 'urn:replicon:list-type:date', 'textValue')
        if textValue:
            new_list.append(
                {'uri': rail.find_first_by_attr_and_get_attr(entry['cells'], 'dataType', 'urn:replicon:list-type:object', 'uri'),
                 'enddate': textValue,
                 'day_diff': get_date_difference(textValue),
                 'username': rail.find_first_by_attr_and_get_attr(entry['cells'], 'dataType', 'urn:replicon:list-type:object', 'textValue'),
                 'OHRID': rail.find_first_by_attr_and_get_attr(entry['cells'], 'dataType', 'urn:replicon:list-type:string', 'textValue')
                 })
    return new_list


def get_all_foreign_uri_list():
    new_list = []
    for entry in rail.result('get_all_foreign_supervisors')['rows']:
        new_list.append(
            {'uri': rail.find_first_by_attr_and_get_attr(entry['cells'], 'dataType', 'urn:replicon:list-type:object', 'uri'),
             'username': rail.find_first_by_attr_and_get_attr(entry['cells'], 'dataType', 'urn:replicon:list-type:object', 'textValue'),
             'OHRID': rail.find_first_by_attr_and_get_attr(entry['cells'], 'dataType', 'urn:replicon:list-type:string', 'textValue')
             })
    return new_list


def get_date_difference(date):
    date_difference = (datetime.now() - datetime.strptime(
        date, "%d/%m/%Y")).days
    return date_difference


def get_date_format(time_zone):
    date_time = pendulum.now(time_zone)
    return datetime(date_time.year, date_time.month, date_time.day,
                    date_time.hour, date_time.second).strftime("%m_%d_%Y_%H_%s")
