from datetime import datetime
from uuid import uuid4
import rail

null = None

DATE_FORMAT = '%m/%d/%Y'

def get_replicon_date(date_str):
    if not date_str:
        return None
    date = datetime.strptime(date_str, DATE_FORMAT)
    return {
        'year': date.year,
        'month': date.month,
        'day': date.day
    }

def get_time_off_details_on_booking_id(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
                "urn:replicon:time-off-list-column:time-off",
                "urn:replicon:time-off-list-column:time-off-type",
                "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-column:" + dag_run.conf['booking_id_oef_uri'].split(":")[-1],
                "urn:replicon:time-off-list-column:start-date",
                "urn:replicon:time-off-list-column:end-date",
                "urn:replicon:time-off-list-column:start-day-start-time",
                "urn:replicon:time-off-list-column:end-day-end-time"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "filterDefinitionUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-filter:"+dag_run.conf[
                    'booking_id_oef_uri'].split(":")[-1]
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
                "value": {
                    "text": dag_run.conf['booking_id']
                }
            }
        }
    }

def get_submit_time_off_entry_payload():
    return {
        "timeOffUri": rail.result('create_timeoff_booking_for_user')['uri'],
        "unitOfWorkId": str(uuid4()),
        "comments": "Timeoff Booking Approved by Integration"
    }

MANDATORY_FIELDS = {
        "booking_id": "booking_id",
        "loginname": "loginname",
        "hours": "hours",
        "startdate": "startdate",
        "enddate": "enddate",
        "action":"action"
}

def get_mandatory_fields_exception_message(item):
    missing_fields = []
    for payload_key, log_value in MANDATORY_FIELDS.items():
        if not item[payload_key]:
            missing_fields.append(f"{log_value} is not present in payload")
    if item['action'] not in ("Add", "Delete"):
        missing_fields.append(f"status '{item['action']}' is not valid in the payload")
    return rail.smartjoin_by_delim(missing_fields, ";")

def create_timeoff_dates_payload(start_date_str, end_date_str, time_off_hours):
    start_date = datetime.strptime(start_date_str, DATE_FORMAT)
    end_date = datetime.strptime(end_date_str, DATE_FORMAT)

    def allocate_hours():

        num_days = (end_date - start_date).days + 1

        hours_per_day = 8
        hours = [hours_per_day] * num_days

        if total_hours < hours_per_day * num_days:
            full_days = int(total_hours // hours_per_day)
            remaining_hours = total_hours % hours_per_day

            for i in range(full_days):
                hours[i] = hours_per_day

            if remaining_hours > 0:
                hours[full_days] = remaining_hours
            else:
                hours[full_days] = 0

        return hours

    payload = {
        "timeOffStart": {
            "date": {
                "year": start_date.year,
                "month": start_date.month,
                "day": start_date.day
            },
            "relativeDuration": None,
            "specificDuration": None
        },
        "timeOffEnd": {
            "date": {
                "year": end_date.year,
                "month": end_date.month,
                "day": end_date.day
            },
            "relativeDuration": None,
            "specificDuration": None
        }
    }

    def get_seconds(hours):
        return str(int(hours*3600))

    total_hours = float(time_off_hours)
    days_off = (end_date - start_date).days + 1

    if days_off == 1:
        if total_hours == 8:
            payload['timeOffStart']['specificDuration'] = None
            payload['timeOffStart']['relativeDuration'] = None
        elif total_hours == 4:
            payload['timeOffStart']['relativeDuration'] = "urn:replicon:time-off-relative-duration:half-day"
        else:
            payload['timeOffStart']['specificDuration'] = {
                "hours": "0",
                "minutes": "0",
                "seconds": get_seconds(total_hours),
                "milliseconds": "0",
                "microseconds": "0"
            }
    else:
        payload['timeOffStart']['specificDuration'] = None
        payload['timeOffStart']['relativeDuration'] = None

        remaining_hours = allocate_hours()[-1]

        if remaining_hours > 0:
            if remaining_hours == 4:
                payload['timeOffEnd']['relativeDuration'] = "urn:replicon:time-off-relative-duration:half-day"
            elif remaining_hours == 8:
                payload['timeOffEnd']['specificDuration'] = None
                payload['timeOffEnd']['relativeDuration'] = None
            else:
                payload['timeOffEnd']['specificDuration'] = {
                    "hours": "0",
                    "minutes": "0",
                    "seconds": get_seconds(remaining_hours),
                    "milliseconds": "0",
                    "microseconds": "0"
                }
        else:
            payload['timeOffEnd']['specificDuration'] = {
                    "hours": "0",
                    "minutes": "0",
                    "seconds": "0",
                    "milliseconds": "0",
                    "microseconds": "0"
                }

    return payload

def get_create_timeoff_payload(dag_run):
    return {
        "timeOff": {
            "owner": {
                "uri": dag_run.conf['user_uri']
            },
            "timeOffType": {
                "uri": dag_run.conf['timeoff_uri'],
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": create_timeoff_dates_payload(dag_run.conf['startdate'],dag_run.conf['enddate'],dag_run.conf['hours']),
            "objectExtensionFieldValues": [
                {
                    "definition": {
                        "uri": dag_run.conf['booking_id_oef_uri']
                    },
                    "textValue": dag_run.conf['booking_id']
                }
            ],
        },
        "comments": "Timeoff Booking Added by Integration",
        "unitOfWorkId": str(uuid4())
    }
