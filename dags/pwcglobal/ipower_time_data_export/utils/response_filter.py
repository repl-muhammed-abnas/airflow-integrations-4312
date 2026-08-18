import itertools
from datetime import timedelta
import pendulum as pndlum
import rail


def filter_location_hierarchy_data(response):
    data = response.json()['d']["rows"]
    location_list_output = list(map(lambda row: {
                                    "parent": '/'.join(list(map(lambda x: x['textValue'], row['cells'][5]['cellCollection']))),
                                    "countryname": row['cells'][0]['textValue'],
                                    "countryuri": row['cells'][0]['uri'],
                                    "countryguid": row['cells'][0]['uri'].split(':')[-1]
                                    }, data))
    return {"locationlistoutput": location_list_output}


def create_timesheet_report_list(config):
    time_stamp = pndlum.now(config.time_zone)
    time_value = None
    timesheet_report_filter_list = []
    timesheet_report_data = (rail.result('get_report_details'))[
        'filterConfiguration']['enabledFilters']
    for i in range(0, 3):
        if i != 0:
            time_value = (time_stamp-timedelta(days=1)).strftime("%m/%d/%Y")
        else:
            time_value = "null"
        timesheet_report_filter_list.append(list(map(lambda enabled_filters: {
            "reportFilterUri": enabled_filters['uri'],
            "value": time_value
        }, filter(lambda enabled_filters: enabled_filters['displayText'] == "TimesheetSubmissionDateFilter", timesheet_report_data))))
    return list(itertools.chain.from_iterable(timesheet_report_filter_list))


def create_location_report_list():
    location_report_filter_list = []
    location_report_data = rail.result('get_countries_hierarchy_data')[
        'locationlistoutput']
    timesheet_report_data = rail.result('get_report_details')[
        'filterConfiguration']['enabledFilters']
    timesheet_report = {}
    for timesheet_report in timesheet_report_data:
        if timesheet_report['displayText'] == "CurrentLocationFilter":
            location_report_filter_list.append(list(map(lambda location_data, ts_report=timesheet_report: {
                "reportFilterUri": ts_report['uri'],
                "value": location_data['countryguid']
            }, location_report_data)))
            return list(itertools.chain.from_iterable(location_report_filter_list))
    return []
