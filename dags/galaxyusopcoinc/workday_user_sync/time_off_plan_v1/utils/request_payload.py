import json
import rail
null = None


def get_enabled_time_off_type_conf(item):
    return {
        "time_off_type_desc": item["ReferenceID"],
        "time_off_type_name": item['TimeOffPlan'],
        "timeoff_description": item['ReferenceID'],
        "unit_of_time": item['UnitOfTime'],
        "country": item['Country'],
        "timeoff_uri": item["uri"],
        "action": "enabled",
        "feed_timeoff_name": item['TimeOffPlan'],
        "replicon_timeoff_name":item["name"],
        "measurement_unit_uri":item['measurement_unit_uri'],
        "minimum_timeoff_increment_policy_uri": item['minimum_timeoff_increment_policy_uri'],
        "startEnd_time_specification_requirement_uri":item['startEnd_time_specification_requirement_uri'],
        "timeoff_balance_tracking_option_uri": item['timeoff_balance_tracking_option_uri'],
        "timeoff_display_format": item['timeoff_display_format'],
        "current_timeoff_status": item['enabled']
    }


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf

def can_timeoff_be_disabled(name: str, pre_fix_check_list):
    return not any(name.lower().startswith(pre_fix) for pre_fix in pre_fix_check_list)

def get_create_time_off_type_conf(item, config):
    return {
        "time_off_type_desc": item["ReferenceID"],
        "time_off_type_name": item["TimeOffPlan"],
        "unit_of_time": item["UnitOfTime"],
        "country": item["Country"],
        "action": "create",
        "create_as_enable": can_timeoff_be_disabled(item["TimeOffPlan"], config.TIMEOFF_DISABLE_CHECK_LIST)
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


def get_put_key_value_payload(dag_run, config):

    created_time_off = rail.result("get_all_created_time_off")
    country = dag_run.conf['country']
    timeoff_types_to_add = list(map(lambda time_off: {
                        "type": "timeoff",
                        "country": country,
                        "timeoffname": time_off
                        },
                      filter(lambda time_off: can_timeoff_be_disabled(name=time_off, pre_fix_check_list=config.TIMEOFF_DISABLE_CHECK_LIST),
                            created_time_off)))
    final_list = json.loads(rail.result(
        "get_key_value_country")['jsonValue']) + timeoff_types_to_add
    return {
        "keyNamespace": config.mapper_name,
        "keyValue": {
            "key": country,
            "jsonValue": json.dumps(final_list)
        }
    }

def get_put_key_value_payload_add(dag_run, config):

    created_time_off = rail.result("get_all_created_time_off")
    country = dag_run.conf['country']
    timeoff_types_to_add = list(map(lambda time_off: {
                        "type": "timeoff",
                        "country": country,
                        "timeoffname": time_off
                        },
                      filter(lambda time_off: can_timeoff_be_disabled(name=time_off, pre_fix_check_list=config.TIMEOFF_DISABLE_CHECK_LIST),
                            created_time_off)))
    return {
        "keyNamespace": config.mapper_name,
        "keyValue": {
            "key": country,
            "jsonValue": json.dumps(timeoff_types_to_add)
        }
    }


def get_minimum_timeoff_increment_policy_uri(dag_run):
    if dag_run.conf['unit_of_time'].lower() == "hours":
        return "urn:replicon:policy:time-off:minimum-increment:no-minimum"
    if dag_run.conf['unit_of_time'].lower() == "days":
        return "urn:replicon:policy:time-off:minimum-increment:quarter-day"
    raise Exception(f"Unit of Time is not Hours nor Days, received Unit Of Time: {dag_run.conf['unit_of_time']}")

def get_put_time_off_type_data(dag_run):
    DEFAULT_URIS = {
        "balance_tracking_option_uri": "urn:replicon:time-off-balance-tracking-option:track-time-taken",
        "time_specification_requirement_uri": "urn:replicon:policy:time-off:start-end-time-specification-requirement:require-start-end-time-for-partial-days",
        "default_pay_code": f"urn:replicon-tenant:{rail.get_tenant_slug()}:pay-code:3"
    }
    timeoff_measurement_unit_display_format_uri=f"urn:replicon:time-off-measurement-unit:{('hours' if dag_run.conf['unit_of_time']=='Hours' else 'work-days')}"
    return {
        "timeOffType": {
            "target": {
                "uri": null,
                "name":dag_run.conf['time_off_type_name']
            },
            "name":dag_run.conf['time_off_type_name'],
            "description":dag_run.conf['time_off_type_desc'],
            "enabled": dag_run.conf.get('create_as_enable', 'false'),
            "timeOffBalanceTrackingOptionUri": DEFAULT_URIS['balance_tracking_option_uri'],
            "minimumTimeOffIncrementPolicyUri": get_minimum_timeoff_increment_policy_uri(dag_run),
            "startEndTimeSpecificationRequirementUri": DEFAULT_URIS['time_specification_requirement_uri'],
            "measurementUnitUri": timeoff_measurement_unit_display_format_uri,
            "timeOffDisplayFormatUri": timeoff_measurement_unit_display_format_uri,
            "payCodeUri": DEFAULT_URIS['default_pay_code']
        }
    }

def get_update_timeoff_name(dag_run):
    return {
        "timeOffType": {
            "target": {
                "uri": dag_run.conf['timeoff_uri']
            },
            "name": dag_run.conf['feed_timeoff_name'],
            "description": dag_run.conf['timeoff_description'],
            "enabled": 0 if dag_run.conf['action'] == "disable" else (dag_run.conf['current_timeoff_status'] if dag_run.conf['action']=="update" else 1),
            "timeOffBalanceTrackingOptionUri": dag_run.conf['timeoff_balance_tracking_option_uri'],
            "minimumTimeOffIncrementPolicyUri": dag_run.conf['minimum_timeoff_increment_policy_uri'],
            "startEndTimeSpecificationRequirementUri": dag_run.conf['startEnd_time_specification_requirement_uri'],
            "measurementUnitUri": dag_run.conf['measurement_unit_uri'],
            "timeOffDisplayFormatUri": dag_run.conf['timeoff_display_format'],
            "payCodeUri": null
        }
    }
