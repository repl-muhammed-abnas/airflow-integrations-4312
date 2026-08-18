import json
from datetime import datetime
import calendar
import pytz
import dateutil.relativedelta
from dateutil.relativedelta import relativedelta
import rail
from airflow.models import Variable


def schedule_interval():
    last_day_of_month = str(datetime.now() + relativedelta(day=31))[8:10]
    today = get_dates()['today']

    if int(last_day_of_month)-1 == int(today.split('/')[1]):
        return True

    if calendar.weekday(int(today.split('/')[2]), int(today.split('/')[0]), int(today.split('/')[1])) == 6:
        return True

    return False


def get_location_details(country):
    location_details = json.loads(
        Variable.get("location_details", default_var=[]))

    locations_data = list(filter(lambda x: x['country'] == country, list(map(lambda item: {
        'country': item['Country'],
        'enabled': item['Enabled'],
        'location': item['Locations']
    }, location_details))))

    return [x['location'] for x in locations_data]


def get_dates():
    timezone = pytz.timezone("Australia/Melbourne")
    today = datetime.now(timezone)
    start_date = today + dateutil.relativedelta.relativedelta(months=-1)

    return {
        'start_date': start_date.strftime("%m/%d/%Y"),
        'today': today.strftime("%m/%d/%Y")
    }


def get_load_users_data_from_report():
    filter_values = []
    for item in rail.result('get_location_details'):
        filter_values.append({
            "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details'
                )['filterConfiguration']['enabledFilters'], 'displayText', 'CurrentLocationFilter', 'uri'),
            "value": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_locations'
                ), 'displayText', item, 'uri', default='').split(':')[-1]
        })

    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details'
                )['filterConfiguration']['enabledFilters'], 'displayText', 'TimesheetPeriodFilter', 'uri'),
        "value": None
    })

    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details'
                )['filterConfiguration']['enabledFilters'], 'displayText', 'TimesheetPeriodFilter', 'uri'),
        "value": get_dates()['start_date']
    })

    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details'
                )['filterConfiguration']['enabledFilters'], 'displayText', 'TimesheetPeriodFilter', 'uri'),
        "value": get_dates()['today']
    })

    filter_values.append({
        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details'
                )['filterConfiguration']['enabledFilters'], 'displayText', 'ApprovalStatusFilter', 'uri'),
        "value": 0
    })

    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')['uri'],
                "filterValues": filter_values,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }
