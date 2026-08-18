from datetime import datetime, timedelta
import uuid
from dateutil.parser import parse as date_parser
import rail

TIMESTAMP_FORMAT = "%I:%M %p"

def get_task_state(task_id):
    task_instance = rail.get_current_context()['dag_run'].get_task_instance(task_id)
    return task_instance.current_state() if task_instance else None

def get_shift_data(dag_run):
    return {
            "page": "1",
            "pagesize": "100",
            "columnUris": [
                "urn:replicon:shift-list-column:shift",
                "urn:replicon:shift-list-column:name",
                "urn:replicon:shift-list-column:description",
                "urn:replicon:shift-list-column:break-hours"
            ],
            "sort": [],
            "filterExpression": {
                "leftExpression": {
                "filterDefinitionUri": "urn:replicon:shift-list-filter:text"
                },
                "operatorUri": "urn:replicon:filter-operator:text-search",
                "rightExpression": {
                "value": {
                    "text": dag_run.conf['schedulename']
                }
            }
        }
    }

def get_hour_payload(source):
    return date_parser(source).hour

def get_start_and_end_time_for_create(name):
    data = rail.result("get_query_data")[name]
    return {
            "shiftUri": rail.result("create_shift_schedule_draft"),
            name: {
                "hour": get_hour_payload(data),
                "minute": int(data.split(':')[1][:2].strip()),
                "dayOffset": 0
            }
        }

def get_schema_for_breaks(start_time,duration_min,breaktype):
    return {
        "inTime": {
          "hour": get_hour_payload(start_time),
          "minute": start_time.split(':')[1][:2].strip(),
          "dayOffset": '0'
        },
        "duration": {
          "hours": '0',
          "minutes": duration_min,
          "seconds": '0',
          "milliseconds": '0',
          "microseconds": '0'
        },
        "breakType": {
          "name": breaktype
        }
      }


def get_create_break_hours(shift_type):
    data = rail.result("get_query_data")
    existing_shift_data = get_task_state('get_shift_break_details') == 'success'
    break1_st,brek1_dur, break1 = data['break1_start_time'],data['break1_duration'],data['break1']
    break2_st,brek2_dur, break2 = data['break2_start_time'],data['break2_duration'],data['break2']
    break_hours = []

    def get_single_shift_payload(data):
        minute = str(data['start_time_min']) if data['start_time_min'] !=0 else '00'
        start_time = str(data['start_time_hr']-12) + ':' + minute + 'PM' if data['start_time_hr'] > 12 else str(data['start_time_hr']) + ':' + minute + 'AM'
        duration = str(data['duration_min'])
        break_hours.append(get_schema_for_breaks(start_time,duration,data['name']))

    def get_combined_shifts_payload(start_time,duration_min,breaktype):
        data = rail.result('get_shift_break_details')
        if not existing_shift_data:
            return break_hours.append(get_schema_for_breaks(start_time,duration_min,breaktype))

        if len(data)>1:
            for i in data:
                if i['name'] != breaktype:
                    get_single_shift_payload(i)
            return break_hours.append(get_schema_for_breaks(start_time,duration_min,breaktype))

        if data[0]['name'] != breaktype:
            get_single_shift_payload(data[0])
            return break_hours.append(get_schema_for_breaks(start_time,duration_min,breaktype))

        return break_hours.append(get_schema_for_breaks(start_time,duration_min,breaktype))

    if shift_type == 'create':
        break_hours.append(get_schema_for_breaks(break1_st,brek1_dur, break1)) if data['break1'] != "NULL" else None
        break_hours.append(get_schema_for_breaks(break2_st,brek2_dur, break2)) if data['break2'] != "NULL" else None

    if shift_type == 'update':
        if data['break1'] != "NULL" and data['break2'] != "NULL":
            break_hours.append(get_schema_for_breaks(break1_st,brek1_dur, break1))
            break_hours.append(get_schema_for_breaks(break2_st,brek2_dur, break2))

        if data['break1'] == "NULL" and data['break2'] == "NULL":
            return None

        if data['break1'] != "NULL" and data['break2'] == "NULL":
            get_combined_shifts_payload(break1_st,brek1_dur, break1)

        if data['break1'] == "NULL" and data['break2'] != "NULL":
            get_combined_shifts_payload(break2_st,brek2_dur, break2)

    return break_hours

def get_break_hours_payload(shift_type):
    return {
        "shiftUri": rail.result("create_shift_schedule_draft") if shift_type == 'create' else rail.result("shift_details_in_replicon")[0]['uri'],
        "shiftBreakSegments": {
            "breakEntries": get_create_break_hours(shift_type)
        }
    }

def get_all_shifts_payload():
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": [
            "urn:replicon:shift-list-column:shift",
            "urn:replicon:shift-list-column:name",
            "urn:replicon:shift-list-column:description"
        ],
        "sort": [
            {
            "columnUri": "urn:replicon:shift-list-column:name",
            "isAscending": "true"
            }
        ],
        "filterExpression": {
            "leftExpression": {
            "filterDefinitionUri": "urn:replicon:shift-list-filter:is-enabled"
            },
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {
            "value": {
                "bool": "true"
            }
            }
        }
    }

def get_replicon_date(date_str, format_):
    if not date_str:
        return None

    try:
        date = datetime.strptime(date_str, format_)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None

def get_shift_schedule_list():
    shift_schedule_list =  list(map(lambda item: {
        "date": get_replicon_date(item['startdate'], '%Y-%m-%d'),
        "target": {
                "uri": None
                },
        "shift": {
            "name": item['schedulename']
        },
        "user": {
            "uri": item['useruri']
        },
        "startTime": {
            "hour": 12 if get_hour_payload(item['starttime']) == 24 else get_hour_payload(item['starttime']),
            "minute": int(item['starttime'].split(':')[1][:2]),
            "dayOffset": 0
        },
        "endTime": {
            "hour": 12 if get_hour_payload(item['endtime']) == 24 else get_hour_payload(item['endtime']),
            "minute": int(item['endtime'].split(':')[1][:2]),
            "dayOffset": get_dayoffset_value(item['starttime'],item['endtime'])
        },
        "publishState": "urn:replicon:shift-assignment-publish-state:published",
        "note": "Published by shift automation"
    }, rail.result("get_shifts_to_assign")))

    return {
        "assignments": shift_schedule_list,
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_dayoffset_value(start_time,end_time):
    start_time_updated = datetime.strptime(start_time, TIMESTAMP_FORMAT)
    end_time_updated = datetime.strptime(end_time, TIMESTAMP_FORMAT)
    if end_time_updated < start_time_updated:
        end_time_updated += timedelta(days=1)

    return '1' if (start_time_updated.day != end_time_updated.day) else '0'

def get_default_pto_shift_payload(dag_run):
    data = list(filter(lambda shift_data: shift_data['shift_assigned'] == 'no', rail.result(
        "get_assigned_shift_dates")['pto_result']))
    shift_schedule_list =  list(map(lambda item: {
        "date": get_replicon_date(item['startdate'], '%Y-%m-%d'),
        "target": {
                "uri": None
                },
        "shift": {
            "uri": dag_run.conf['shift_uri']
        },
        "user": {
            "uri": item['useruri']
        },
        "startTime": {
            "hour": 12 if get_hour_payload(item['starttime']) == 24 else get_hour_payload(item['starttime']),
            "minute": int(item['starttime'].split(':')[1][:2]),
            "dayOffset": 0
        },
        "endTime": {
            "hour": 12 if get_hour_payload(item['endtime']) == 24 else get_hour_payload(item['endtime']),
            "minute": int(item['endtime'].split(':')[1][:2]),
            "dayOffset": get_dayoffset_value(item['starttime'],item['endtime'])
        },
        "publishState": "urn:replicon:shift-assignment-publish-state:published",
        "note": "Published by shift automation"
    }, data))

    return {
        "assignments": shift_schedule_list,
        "unitOfWorkId": str(uuid.uuid4())
    }

def get_default_shift_payload(item):
    return {
        "userUri": item['useruri'],
        "scheduleEntries": [
            {
            "schedulePolicy": {
                "scheduleTypeUri": "urn:replicon:schedule-type:shift"
            }
            }
        ]
    }

def get_shift_schedule_summary_data():
    data = rail.load_all_records(rail.result("query_dates_for_shift"))[0]
    return {
        "userSearch": {
            "includeShiftAssignmentsWithNoUser": "false",
            "specificUserUris": [
                data['useruri']
            ]
        },
        "shiftSearch": None,
        "objectExtensionFieldSearches": [],
        "dateRange": {
            "startDate": get_replicon_date(data['MIN_startdate_'],"%Y-%m-%d"),
            "endDate": get_replicon_date(data['MAX_startdate_'],"%Y-%m-%d"),
            "relativeDateRangeUri": None,
            "relativeDateRangeAsOfDate": None
        }
    }

def get_assignment_uris():
    data = list(filter(lambda shift_data: shift_data['delete_shift'] == 'yes', rail.result(
        "get_assigned_shift_dates")['shift_result']))
    return {
        "shiftAssignmentUris": [x['assignmenturi'] for x in data]
    }

def get_put_holiday_payload(item):
    return {
        "timeOff": {
            "target": None,
            "owner": {
                "uri": item["useruri"]
            },
            "timeOffType": {
                "name": 'PTO'
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": get_replicon_date(item['startdate'], '%Y-%m-%d'),
                    "timeOfDay": None,
                    "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
                    "specificDuration": None
                },
                "timeOffEnd": {
                    "date": get_replicon_date(item['startdate'], '%Y-%m-%d'),
                    "timeOfDay": None,
                    "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
                    "specificDuration": None
                }
            },
            "userExplicitEntries": [],
            "comments": None,
            "customFieldValues": [],
            "objectExtensionFieldValues": []
        },
        "comments": "Submitted by Schedule Integration",
        "unitOfWorkId": str(uuid.uuid4())
    }
