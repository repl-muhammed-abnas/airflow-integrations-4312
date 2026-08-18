from datetime import datetime
import uuid
import os
import rail


null = None


def get_timeoff_details_payload():
    return {
        "timeOffTypeUris": rail.result('get_all_time_off_types_uris')
    }


def get_conf(item):
    return {
        'employeeid': item['employeeid'],
        'timeoffdetails': rail.result('get_timeoff_details'),
        'hidden_oef_value': rail.result('get_hidden_oef_value')[0]['hiddenoefvalue'],
        'filename': (rail.result('new_file_sensor')).split('/')[-1]
    }


def get_user_on_empid_payload(dag_run):
    return{
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
                "urn:replicon:user-list-column:user",
                "urn:replicon:user-list-column:employee-id",
                "urn:replicon:user-list-column:enabled"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {
                "leftExpression": null,
                "operatorUri": null,
                "rightExpression": null,
                "value": null,
                "filterDefinitionUri": "urn:replicon:user-list-filter:text"
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
                    "text": dag_run.conf['employeeid'],
                    "time": null,
                    "calendarDayDurationValue": null,
                    "workdayDurationValue": null,
                    "dateRange": null,
                    "dateTimeUtc": null,
                    "dateTimeUtcRange": null
                },
                "filterDefinitionUri": null
            },
            "value": null,
            "filterDefinitionUri": null
        }
    }


def get_time_off_details_on_entryid(dag_run):
    return {
        "page": "1",
        "pagesize": "100",
        "columnUris": [
                "urn:replicon:time-off-list-column:time-off",
                "urn:replicon:time-off-list-column:time-off-type",
                "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-column:" +
            dag_run.conf['hidden_oef_value'],
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
                "filterDefinitionUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":time-off-object-extension-filter:"+dag_run.conf['hidden_oef_value']
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
                    "text": dag_run.conf['timeoffentryid'],
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


def get_time_off_booking_details_payload(dag_run):
    return {
        "timeOffUri": rail.find_first_by_attr_and_get_attr(rail.result('get_time_off_details_on_entryid'),
                                                           'timeoffentryid', dag_run.conf['timeoffentryid'], 'timeoffuri')
    }


def get_child_conf(item, dag_run):
    def get_date(date):
        if not date:
            return None
        year = date['year']
        month = date['month']
        day = date['day']
        return str(year)+'-'+str(month).zfill(2)+'-'+str(day).zfill(2)

    return {
        'employeeid': item['employeeid'],
        'referenceid': item['referenceid'],
        'timeoffentryid': item['timeoffentryid'],
        'timeoffstartdate': item['timeoffstartdate'],
        'timeoffenddate': item['timeoffenddate'],
        'timeoffuri': rail.find_first_by_attr_and_get_attr(dag_run.conf['timeoffdetails'], 'description', item['referenceid'], 'uri'),
        'useruri': rail.result('user_details')['useruri'],
        'hidden_oef_value': dag_run.conf['hidden_oef_value'],
        'availabletimeoffuris': rail.result('get_all_assigned_time_off_type_for_user'),
        'userstartdate': get_date(rail.result('get_user_info')[0]['startdate']),
        'userenddate': get_date(rail.result('get_user_info')[0]['enddate']),
        'filename': dag_run.conf['filename'],
        'flag': item['flag']
    }


def is_update_required_test(dag_run):
    data = rail.result('get_time_off_details_on_entryid')[0]
    timeofftype_uri_from_file = dag_run.conf['timeoffuri']
    if timeofftype_uri_from_file == data['timeofftypeuri']:
        if dag_run.conf['timeoffstartdate'] == data['timeoffstartdate']:
            if dag_run.conf['timeoffenddate'] == data['timeoffenddate']:
                return False
    return True


def get_reopen_timeoff():
    data = rail.result('get_time_off_details_on_entryid')[0]
    return {
        "timeOffUri": data['timeoffuri'],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": null
    }


def get_create_time_off_draft_payload(dag_run):
    return {
        "ownerUri": dag_run.conf['useruri']
    }


def get_put_timeoff_entry_payload(status, dag_run):
    def get_time_off_uri():
        data = rail.result('get_time_off_details_on_entryid')[0]
        return data['timeoffuri']

    def get_replicon_date(date_str):
        if not date_str:
            return None
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d')
            return {
                'year': date.year,
                'month': date.month,
                'day': date.day
            }
        except:  # pylint: disable=bare-except
            return None

    return {
        "timeOff": {
            "target": {
                "uri": get_time_off_uri() if status == 'reopen' else rail.result('create_time_off_draft')
            },
            "owner": {
                "uri": dag_run.conf['useruri'],
                "loginName": null,
                "parameterCorrelationId": null
            },
            "timeOffType": {
                "uri": dag_run.conf['timeoffuri'],
                "name": null
            },
            "entryConfigurationMethodUri": "urn:replicon:time-off-entry-configuration-method:populate-daily-entries-using-start-end-date-and-schedule",
            "multiDayUsingStartEndDate": {
                "timeOffStart": {
                    "date": get_replicon_date(dag_run.conf['timeoffstartdate']),
                    "timeOfDay": null,
                    "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
                    "specificDuration": null
                },
                "timeOffEnd": {
                    "date": get_replicon_date(dag_run.conf['timeoffenddate']),
                    "timeOfDay": null,
                    "relativeDuration": "urn:replicon:time-off-relative-duration:full-day",
                    "specificDuration": null
                }
            },
            "userExplicitEntries": [],
            "comments": null,
            "customFieldValues": []
        }
    }


def get_publish_time_off_draft_payload():
    return {
        "timeOff": rail.result('create_time_off_draft')
    }


def get_submit_time_off_entry_payload(status):
    def get_time_off_uri():
        data = rail.result('get_time_off_details_on_entryid')[0]
        return data['timeoffuri']
    return {
        "timeOffUri": get_time_off_uri() if status == 'reopen' else rail.result('publish_time_off_draft')['uri'],
        "unitOfWorkId": str(uuid.uuid4()),
        "comments": null
    }


def get_put_timeoff_entry_id_oef_value_payload(dag_run):
    return {
        "timeOffUri": rail.result('publish_time_off_draft')['uri'],
        "extensionFieldValues": [
            {
                "definition": {
                    "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":object-extension-tag-definition:"+dag_run.conf['hidden_oef_value'],
                    "name": null
                },
                "tag": null,
                "numericValue": null,
                "textValue": dag_run.conf['timeoffentryid'],
                "fileValue": null,
                "jsonValue": null
            }
        ]
    }


def do_has_file_content():
    with rail.existing_artifact(rail.result('decrypt_file')) as artifact:
        return os.path.getsize(artifact.local_filename) > 0


def get_time_off_approval_status():
    return {
        "timeOffUri": rail.result('get_time_off_details_on_entryid')[0]['timeoffuri']
    }


def get_user_info_payload():
    return {
        "users": [
            {
                "uri": rail.result('user_details')['useruri'],
                "loginName": null,
                "parameterCorrelationId": null
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
    }


def get_all_assigned_time_off_type_for_user_payload():
    return {
        "userUri": rail.result('user_details')['useruri']
    }


def test_effective_date(dag_run):
    if dag_run.conf['userstartdate']:
        format_userstartdate = datetime.strptime(
            dag_run.conf['userstartdate'], '%Y-%m-%d')
        format_timeoffstartdate = datetime.strptime(
            dag_run.conf['timeoffstartdate'], '%Y-%m-%d')
        if dag_run.conf['userenddate']:
            format_userenddate = datetime.strptime(
                dag_run.conf['userenddate'], '%Y-%m-%d')
            format_timeoffenddate = datetime.strptime(
                dag_run.conf['timeoffenddate'], '%Y-%m-%d')
            return (format_userstartdate <= format_timeoffstartdate) and (format_userenddate >= format_timeoffenddate)
        return format_userstartdate <= format_timeoffstartdate
    return False


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
