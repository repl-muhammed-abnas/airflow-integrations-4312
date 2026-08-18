
from datetime import datetime as dt, timedelta
from pendulum import now
import rail
from airflow.models import Variable
from functools import lru_cache
null = None


def get_last_run_date_time(config):
    # since the utc time zone is used in the instance for the service call same has been added here
    time_zone = "Etc/UTC"
    last_run_date_time = dt.strptime(Variable.get(config.shiftassignment_export_last_run_time), "%d:%m:%Y:%H:%M:%S") or\
        now(tz=time_zone) - timedelta(hours=24)
    start = last_run_date_time
    end = now(tz=time_zone)
    Variable.set(config.shiftassignment_export_last_run_time,
                 end.strftime("%d:%m:%Y:%H:%M:%S"))

    return {
        "startDateTime": {
            "year": start.year,
            "month": start.month,
            "day": start.day,
            "hour": start.hour,
            "minute": start.minute,
            "second": start.second,
            "millisecond": 0
        },
        "endDateTime": {
            "year": end.year,
            "month": end.month,
            "day": end.day,
            "hour": end.hour,
            "minute": end.minute,
            "second": end.second,
            "millisecond": 0
        }
    }


def get_filter_uris(enabled_filters):
    return {
        'entry_date_filter_uri': rail.find_first_by_attr_and_get_attr(enabled_filters, 'displayText', 'EntryDateFilter', 'uri'),
        'country_current_servicecenter_filter_uri': rail.find_first_by_attr_and_get_attr(
            enabled_filters, 'displayText', 'CurrentServiceCenterFilter', 'uri'),
    }


def get_report_parameters():
    filter_values = []

    filter_values.append({
        "reportFilterUri": rail.result('get_required_filters')['entry_date_filter_uri'],
        "value": null
    })
    filter_values.append({
        "reportFilterUri": rail.result('get_required_filters')['entry_date_filter_uri'],
        "value": rail.result('log_user_report_filter_dates')['min_start_date']
    })
    filter_values.append({
        "reportFilterUri": rail.result('get_required_filters')['entry_date_filter_uri'],
        "value": rail.result('log_user_report_filter_dates')['max_end_date']
    })

    for uri in rail.result("get_required_country_service_center_uris_as_per_mapper"):
        if uri:
            filter_values.append({
                "reportFilterUri": rail.result('get_required_filters')['country_current_servicecenter_filter_uri'],
                "value": uri.split(":")[-1]
            })

    return {
        "reportParameters": [
            {
                "reportUri": rail.result('get_active_user_report_details')['uri'],
                "filterValues": filter_values,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }


@lru_cache(maxsize=8)
def get_workweek_range(day, month, year):
    date_obj = dt.strptime(f"{day}.{month}.{year}", "%d.%m.%Y").date()

    # Get the day of the week (0 = Monday, 6 = Sunday)
    day_of_week = date_obj.weekday()
    start_date = date_obj - timedelta(days=day_of_week)
    end_date = date_obj + timedelta(days=6 - day_of_week)
    return {
        "work_week_start_date": start_date.strftime("%d.%m.%Y"),
        "work_week_end_date": end_date.strftime("%d.%m.%Y")
    }


def get_page_size(request, response):
    if len(response) < 500:
        return null
    request["page"] = request["page"] + 1
    return request


def get_shift_results(response):
    response = list(map(lambda i: {
        "date": dt(**i["date"]).strftime("%d.%m.%Y"),
        "end_time": dt(**{"year": 1, "day": 1, "month": 1,
                          "minute": i["endTime"]["minute"],
                          "hour": i["endTime"]["hour"]}).strftime("%H:%M:%S"),
        "action": i["modificationActionUri"].split(":")[-1],
        "shift": i["shift"]["displayText"],
        "shift_uri": i["shift"]["uri"],
        "start_time": dt(**{"year": 1, "day": 1, "month": 1,
                            "minute": i["startTime"]["minute"],
                            "hour": i["startTime"]["hour"]}).strftime("%H:%M:%S"),
        "user_loginname": i["user"]["loginName"],
        "user_uri": i["user"]["uri"],
        **get_workweek_range(i["date"]['day'], i["date"]['month'], i["date"]['year'])
    }, response))
    return rail.write_json_artifact(response)


def get_details(response):
    _type = _location = _dws = shift_name = ""
    result = []
    for i in response["rows"]:
        if i["cells"][1] and "textValue" in i["cells"][1]:
            shift_description = i["cells"][1]["textValue"]
            if shift_description and "|" in shift_description and len(shift_description.split("|")) == 3:
                _dws, _type, _location = shift_description.split("|")
        if i["cells"][2] and "textValue" in i["cells"][2]:
            shift_name = i["cells"][2]["textValue"]
        if i["cells"][0] and "uri" in i["cells"][0]:
            shift_uri = i["cells"][0]["uri"]
        result.append({
            "shift_name": shift_name,
            "shift_type": _type,
            "shift_location": _location,
            "shift_dws": _dws,
            "shift_uri": shift_uri
        })
    return result


def get_log_details(item):
    msg = ""
    log_desc = {"employee_id": "Employee Id",
                "country": "Country", "shift_uri": "Shift Uri"}
    for i in ["employee_id", "country", "shift_uri"]:
        if not item[i]:
            msg += log_desc[i] + "|"
    return msg + "was/were missing hence the data is not exported."
