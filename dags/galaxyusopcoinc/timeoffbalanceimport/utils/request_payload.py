from datetime import datetime
import hashlib
import os
import rail


null = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_timeoff_details_payload():
    return {
        "timeOffTypeUris": rail.result('get_all_time_off_types_uris')
    }


def get_user_on_empid_payload():
    dag_run_conf = get_dag_run_conf()
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
                    "text": dag_run_conf['employeeid'],
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


def get_user_time_off_policy_summary_payload():
    return {
        "userUri": rail.result('user_details')['useruri']
    }


def put_user_time_off_policy_set_schedule_payload(script_name, script_description):
    dag_run_conf = get_dag_run_conf()
    balance_event_scripts = []

    def set_starting_balance_setto_event_scripts():
        return balance_event_scripts.append({
            "scriptTarget": {
                "uri": dag_run_conf['setbalancetouri'],
                "description": script_description,
                "name": script_name
            },
            "additionalParameters": [
                {
                    "keyUri": 'urn:replicon:script-key:parameter:amount',
                    "value": {
                        "number": dag_run_conf['balance'],
                    }
                }
            ]
        })
    set_starting_balance_setto_event_scripts()

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

    return{
        "timeOffAccount": {
            "userUri": dag_run_conf['useruri'],
            "timeOffTypeUri": dag_run_conf['timeoffuri']
        },
        "policySetScheduleEntries": [
            {
                "effectiveDate": get_replicon_date(dag_run_conf['effectivedate']),
                "description": 'Added by Integration on ' + dag_run_conf['effectivedate'],
                "policySet":{
                    "timeOffBalanceEventScripts": balance_event_scripts,
                    "timeOffValidationScripts": []
                }
            }
        ]
    }


def put_timeoff_type_assignment_for_user_payload():
    data = rail.result('get_user_time_off_types')
    timeoffurifromfile = list(map(lambda x: x['timeoffuri'], data))

    time_off_uris = rail.result('get_all_enabled_timeoff_policy_uris')
    for uri in timeoffurifromfile:
        if uri not in time_off_uris:
            time_off_uris.append(uri)
    return {
        "userUri": rail.result('user_details')['useruri'],
        "timeOffTypeUris": time_off_uris
    }


def has_required_time_off_policy_test():
    dag_run_conf = get_dag_run_conf()

    def get_required_time_off_status():
        status = rail.find_first_by_attr_and_get_attr(rail.result(
            'get_user_required_time_off_policy_summary'), 'timeOffTypeuri', dag_run_conf['timeoffuri'], 'isTimeOffAllowedAgainstThisTimeOffType')
        return status if status else False
    return (get_required_time_off_status() if rail.result('get_user_required_time_off_policy_summary') else False)


def get_enable_timeoff_payload(item):
    return {
        "timeOffTypeUri": item
    }


def get_create_md5_data(item):
    if not item:
        return []
    res = {
        'batchid': item["batchid"],
        'employeeid': item["employeeid"],
        'absenceplan': item["absenceplan"],
        'referenceid': item["referenceid"],
        'effectivedate': item["effectivedate"],
        'balance': item["balance"],
        'units': item["units"],
        'md5': hashlib.md5((str(item["employeeid"])+","+ str(item["absenceplan"])+","+str(item["referenceid"])+","
                            + str(item["balance"])).encode('utf-8')).hexdigest()
    }

    return {k: v if v is not None else '' for k, v in res.items()}


def get_conf(item):
    return {
        'employeeid': item['employeeid'],
        'timeoffdetails': rail.result('get_timeoff_details'),
        'setbalancetouri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts'), 'displayText', 'Starting Balance Set To', 'uri'),
        'filename': (rail.result('new_file_sensor')).split('/')[-1]
    }


def get_each_time_off_policy_conf(item):
    dag_run_conf = get_dag_run_conf()
    return {
        **{k: v if v is not None else '' for k, v in item.items()},
        **{
            'useruri': rail.result('user_details')['useruri'],
            'timeoffuri': rail.find_first_by_attr_and_get_attr(dag_run_conf['timeoffdetails'], 'description', item['referenceid'], 'uri'),
            'setbalancetouri': dag_run_conf['setbalancetouri'],
            'filename': dag_run_conf['filename'],
            'create_employee_log': rail.result('create_employee_log')
        }
    }


def is_time_off_enabled_test():
    data = rail.result('get_user_time_off_types')
    return not list(filter(lambda x: not x['status'], map(lambda item: {'status': item['status']}, data)))


def get_timeoff_uri():
    data = rail.result('get_user_time_off_types')
    result = list(filter(lambda x: not x['status'], map(lambda item: {
                  'timeoffuri': item['timeoffuri'], 'status': item['status']}, data)))
    return list(map(lambda x: {'timeoffuri': x['timeoffuri']}, result))


def do_has_file_content():
    with rail.existing_artifact(rail.result('decrypt_file')) as artifact:
        return os.path.getsize(artifact.local_filename) > 0


def is_all_timeoff_policy_enabled_test():
    timeoff_uris_from_file = rail.result('get_user_time_off_types')
    for timeoffuri in timeoff_uris_from_file:
        result = rail.find_first_by_attr_and_get_attr(rail.result(
            'get_user_time_off_policy_summary'), 'timeOffTypeuri', timeoffuri['timeoffuri'], 'isTimeOffAllowedAgainstThisTimeOffType')
        if not result:
            return False
    return True
