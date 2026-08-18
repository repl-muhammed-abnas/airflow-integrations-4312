from datetime import datetime
import uuid
import pendulum
from dateutil.relativedelta import relativedelta
import rail

null = None

def get_location_hierarchy_payload(location):
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:location-list-column:full-path",
            "urn:replicon:location-list-column:location"
        ],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:location-list-filter:text"
            },
            "operatorUri": "urn:replicon:filter-operator:text-search",
            "rightExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": {
                    "uri": null,
                    "uris": [],
                    "bool": null,
                    "date": null,
                    "money": null,
                    "number": null,
                    "text": location,
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null,
                    "numberRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        },
        "hierarchyListDataOptionUris": [
            "urn:replicon:hierarchy-list-data-option:include-descendant-rows"
        ]
    }

def get_timeoff_balance_payload(time_zone):
    today = pendulum.now(time_zone)
    end_of_year = today + relativedelta(month=12, day=31)
    return {
        "account": {
            "userUri": rail.result("get_user_info")["userDetails"]["uri"],
            "timeOffTypeUri": rail.result("get_specfic_time_off_type")
        },
        "asOfDate": {
            "year": end_of_year.year,
            "month": end_of_year.month,
            "day": end_of_year.day
        }
    }

def get_optional_holiday_booking_date():
    holiday_date = datetime.strptime(rail.result("get_bookable_holidays_in_date_range")[
                                     0]["holiday_date"], "%d-%m-%Y").date()
    return {
        "year": holiday_date.year,
        "month": holiday_date.month,
        "day": holiday_date.day
    }


def get_put_holiday_payload(dag_run):
    return {
        "timeOff": {
            "target": null,
            "owner": {
                "uri": dag_run.conf["user_data"]["user_uri"],
                "loginName": null,
                "parameterCorrelationId": null
            },
            "timeOffType": {
                "uri": dag_run.conf["properties"]["optional_holiday_timeoff_uri"],
                "name": null
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": dag_run.conf["optional_holiday_booking_date_json"],
                    "timeOfDay": null,
                    "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
                    "specificDuration": null
                },
                "timeOffEnd": null
            },
            "userExplicitEntries": [],
            "comments": null,
            "customFieldValues": [],
            "objectExtensionFieldValues": []
        },
        "comments": "Submitted by Optional Holiday Admin",
        "unitOfWorkId": str(uuid.uuid4())
    }


def get_approve_holiday_booking_payload():
    return {
        "timeOffUri": rail.result("get_timeoff_uri"),
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": "Approved by Optional Holiday Admin"
    }


def new_user_holiday_bookings_in_daterange_payload(config):
    current_year = pendulum.now(config.time_zone).year
    date_range = config.e1_schedule_daterange if rail.result("get_schedule_based_on_daterange")["schedule"] == "E1" \
        else (config.e2_schedule_daterange if rail.result("get_schedule_based_on_daterange")["schedule"] == "E2" else null)
    return {
        "holidayCalendarUri": rail.result("get_all_holiday_calendars")[0]["optional_holiday_calendar_uri"],
        "dateRange": {
            "startDate": {
                "year": current_year,
                "month": date_range["start_month"],
                "day": date_range["start_day"]
            },
            "endDate": {
                "year": current_year,
                "month": date_range["end_month"],
                "day": date_range["end_day"]
            },
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }


def get_timeoff_booking_payload(dag_run):
    booking_date = dag_run.conf["optional_holiday_booking_date_json"]
    return {
        "userUri": dag_run.conf["user_data"]["user_uri"],
        "dateRange": {
            "startDate": booking_date,
            "endDate": booking_date,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }

def user_details_report_payload():
    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_user_report_details')['uri'],
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }

def get_user_holiday_cal_payload(dag_run, config):
    current_date = pendulum.now(config.time_zone)
    effective_date = {
        "year": current_date.year,
        "month": current_date.month,
        "day": current_date.day
    }
    return {
        "target": {
            "uri": dag_run.conf["user_uri"],
            "loginName": null,
            "employeeId": null,
            "parameterCorrelationId": null
        },
        "dateRange": {
            "startDate": effective_date,
            "endDate": effective_date,
            "relativeDateRangeUri": null,
            "relativeDateRangeAsOfDate": null
        }
    }

