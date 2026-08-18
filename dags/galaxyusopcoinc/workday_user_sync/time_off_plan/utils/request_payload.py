import json
import rail
null = None


def get_enabled_time_off_type_conf(item):
    return {
        "time_off_type_desc": item["ReferenceID"],
        "time_off_type_name": "NA",
        "unit_of_time": "NA",
        "country": "NA",
        "uri": item["uri"],
        "action": "enabled"
    }


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_create_time_off_type_conf(item):
    return {
        "time_off_type_desc": item["ReferenceID"],
        "time_off_type_name": item["TimeOffPlan"],
        "unit_of_time": item["UnitOfTime"],
        "country": item["Country"],
        "action": "create"
    }


def get_process_each_user_record_conf(item):
    return {
        "useruri": item['user uri'],
        "country": item['Country'],
        "time_off_types": rail.result('get_all_created_time_off')
    }


def get_put_time_off_type_payload(dag_run):
    existing_time_off_type = rail.result("get_all_assigned_time_off_type_user")
    new_time_off_type = rail.result("get_conf_time_off_types_uri")
    time_off_type_assign = existing_time_off_type + new_time_off_type
    return {
        "userUri": dag_run.conf['useruri'],
        "timeOffTypeUris": time_off_type_assign
    }


def get_put_key_value_payload(dag_run, mapper):
    created_time_off = rail.result("get_all_created_time_off")
    country = dag_run.conf['country']
    items = []
    for time_off in created_time_off:
        items.append({"type": "timeoff", "country": country,
                     "timeoffname": time_off})
    final_list = json.loads(rail.result(
        "get_key_value_country")['jsonValue']) + items
    return {
        "keyNamespace": mapper,
        "keyValue": {
            "key": country,
            "jsonValue": json.dumps(final_list)
        }
    }


def get_put_time_off_type_data():
    return {
        "timeOffType": {
            "target": {
                "uri": null,
                "name": get_dag_run_conf()['time_off_type_name']
            },
            "name": get_dag_run_conf()['time_off_type_name'],
            "description": get_dag_run_conf()['time_off_type_desc'],
            "enabled": "true",
            "timeOffBalanceTrackingOptionUri": "urn:replicon:time-off-balance-tracking-option:track-time-remaining",
            "minimumTimeOffIncrementPolicyUri": "urn:replicon:policy:time-off:minimum-increment:full-day",
            "startEndTimeSpecificationRequirementUri": "urn:replicon:policy:time-off:start-end-time-specification-requirement\
                :require-start-end-time-for-partial-days",
            "measurementUnitUri": "urn:replicon:time-off-measurement-unit:hours" if get_dag_run_conf()['unit_of_time'] == "Hours"
            else "urn:replicon:time-off-measurement-unit:work-days",
            "timeOffDisplayFormatUri": "urn:replicon:time-off-measurement-unit:hours" if get_dag_run_conf()['unit_of_time'] == "Hours"
            else "urn:replicon:time-off-measurement-unit:work-days",
            "payCodeUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:pay-code:3"
        }
    }
