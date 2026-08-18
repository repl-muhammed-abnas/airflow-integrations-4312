from datetime import datetime
import rail

null = None


def get_process_time_data_records_conf(item):

    return {
        **{k: v if v is not None else '' for k, v in item.items()}
    }


def mandatory_fields_check(dag_run):
    """
    Validates mandatory fields. Either Duration OR No_of_Days_Booked must be present (not both).
    - If both are present → validation fails
    - If neither is present → validation fails
    """
    duration = (dag_run.conf.get('Duration') or '').strip()
    no_of_days_booked = (dag_run.conf.get('No_of_Days_Booked') or '').strip()

    # Exactly one of Duration or No_of_Days_Booked must be present
    has_duration = bool(duration)
    has_days = bool(no_of_days_booked)
    valid_duration_days = (has_duration or has_days) and not (has_duration and has_days)

    return (dag_run.conf['Employee_ID'] and dag_run.conf['Source_Time_Off_Booking_ID']
            and dag_run.conf['Entry_date'] and dag_run.conf['End_Date']
            and dag_run.conf['Time_Type__externalcode_'] and dag_run.conf['Status']
            and valid_duration_days)


def get_hidden_oef_value_payload():
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
            "urn:replicon:object-extension-tag-definition-list-column:name",
            "urn:replicon:object-extension-tag-definition-list-column:object-extension-tag-definition"
        ],
        "sort": [],
        "filterExpression": null
    }


def get_time_off_details_on_entryid(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
                "urn:replicon:time-off-list-column:time-off",
                "urn:replicon:time-off-list-column:time-off-type",
                "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-column:" +
            rail.result('get_hidden_oef_value')[0]['hiddenoefvalue'],
                "urn:replicon:time-off-list-column:start-date",
                "urn:replicon:time-off-list-column:end-date"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-filter:" +
                rail.result('get_hidden_oef_value')[0]['hiddenoefvalue']
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
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
                    "text": dag_run.conf['Source_Time_Off_Booking_ID'],
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
        }
    }


def get_create_time_off_draft_payload():
    return {
        "ownerUri": rail.result('search_user')[0]['uri']
    }

def get_search_user_payload(dag_run):
    return {
        "users": [
            {
                "employeeId": dag_run.conf['Employee_ID']
            }
        ]
    }

def get_replicon_date(date_str):
    if not date_str:
        return None
    try:
        date = datetime.strptime(date_str, '%d/%m/%Y')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None

full_day_uri = "urn:replicon:time-off-relative-duration:full-day"
half_day_uri = "urn:replicon:time-off-relative-duration:half-day"

def get_put_timeoff_entry_payload(dag_run):
    """
    Build the time off entry payload for Replicon API.
    - If Duration has value: Use specificDuration (hourly booking) - Duration takes priority
    - If Duration is empty: Use relativeDuration (full-day/half-day based on Booking_Type or No_of_Days_Booked)
    """
    no_of_days_booked = dag_run.conf.get('No_of_Days_Booked', '').strip() if dag_run.conf.get('No_of_Days_Booked') else ''
    duration = dag_run.conf.get('Duration', '').strip() if dag_run.conf.get('Duration') else ''
    booking_type = (dag_run.conf.get('Booking_Type') or '').strip().lower()

    # Hourly booking: Duration has value (prioritize Duration over No_of_Days_Booked)
    if duration:
        duration_seconds = int(float(duration) * 3600)
        specific_duration = {'seconds': duration_seconds}
        time_off_start = {
            "date": get_replicon_date(dag_run.conf['Entry_date']),
            "timeOfDay": null,
            "relativeDuration": null,
            "specificDuration": specific_duration
        }
        time_off_end = {
            "date": get_replicon_date(dag_run.conf['End_Date']),
            "timeOfDay": null,
            "relativeDuration": null,
            "specificDuration": specific_duration
        }
    else:
        # Day-based booking - determine relativeDuration
        if 'half day' in booking_type:
            relative_duration = half_day_uri
        elif 'full day' in booking_type:
            relative_duration = full_day_uri
        else:
            # Legacy logic - isdigit() check
            relative_duration = full_day_uri if no_of_days_booked.isdigit() else half_day_uri

        # Multi-day booking: start date is always full-day
        is_multi_day = dag_run.conf['Entry_date'] != dag_run.conf['End_Date']
        start_relative_duration = full_day_uri if is_multi_day else relative_duration

        time_off_start = {
            "date": get_replicon_date(dag_run.conf['Entry_date']),
            "timeOfDay": null,
            "relativeDuration": start_relative_duration,
            "specificDuration": null
        }
        time_off_end = {
            "date": get_replicon_date(dag_run.conf['End_Date']),
            "timeOfDay": null,
            "relativeDuration": relative_duration,
            "specificDuration": null
        }

    return {
        "timeOff": {
            "target": {
                "uri": rail.result('create_time_off_draft')
            },
            "owner": {
                "uri": rail.result('search_user')[0]['uri'],
                "loginName": null,
                "parameterCorrelationId": null
            },
            "timeOffType": {
                "uri": null,
                "name": dag_run.conf['Time_Type__externalcode_']
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": {
                "timeOffStart": time_off_start,
                "timeOffEnd": time_off_end
            },
            "userExplicitEntries": [],
            "comments": dag_run.conf.get('Comments', ''),
            "customFieldValues": []
        }
    }


def get_publish_time_off_draft_payload():
    return {
        "timeOff": rail.result('create_time_off_draft')
    }


def get_put_timeoff_entry_id_oef_value_payload(dag_run):
    return {
        "timeOffUri": rail.result('publish_time_off_draft')['uri'],
        "extensionFieldValues": [
            {
                "definition": {
                    "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":object-extension-tag-definition:" +
                    rail.result('get_hidden_oef_value')[0]['hiddenoefvalue'],
                    "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue": dag_run.conf['Source_Time_Off_Booking_ID'],
                "fileValue": null,
                "jsonValue": null
            }
        ]
    }


def get_all_assigned_time_off_type_for_user_payload():
    return {
        "userUri": rail.result('search_user')[0]['uri']
    }


def put_time_off_type_for_user_payload():
    timeofflist = [item['uri'] for item in rail.result(
        'get_all_assigned_time_off_type_for_user')]
    timeofflist.insert(
        len(timeofflist), ((rail.result('get_all_time_off_type')[0]['uri'])))
    return {
        "userUri": rail.result('search_user')[0]['uri'],
        "timeOffTypeUris": timeofflist
    }

def get_error_message():
    context = rail.get_current_context()
    failed_task_ids = rail.lib.errors.get_failed_task_ids(context)
    error_message = ''
    if failed_task_ids:
        error_key = (context['ti'].xcom_pull(failed_task_ids[0], key='error') or 'Unknown error occurred')
        error_message = (error_key.get("response").get("json").get("error").get("details") if error_key.get("response") \
                         else error_key.get('exc_message')) if isinstance(error_key, dict) else error_key

    # Convert error_message to string if it's a dictionary
    if isinstance(error_message, dict):
        # Check if it contains notifications array (Replicon API error format)
        if 'notifications' in error_message and isinstance(error_message['notifications'], list):
            # Extract displayText from notifications
            messages = [notif.get('displayText', '') for notif in error_message['notifications'] if notif.get('displayText')]
            error_message = '; '.join(messages) if messages else str(error_message)
        else:
            # Fallback to JSON string representation
            error_message = str(error_message)

    return error_message
