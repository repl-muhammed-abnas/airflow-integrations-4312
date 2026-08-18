import json
import rail
from galaxyusopcoinc.workday_user_sync.time_off_plan_v1.utils import request_payload


def map_time_off_uri(response):
    data = response.json()['d']
    return list(map(lambda x: x['uri'], data))

def map_time_off_uri_description(response):
    data = response.json()['d']
    return list(map(lambda x: {
        "description": x['description'],
        "minimum_timeoff_increment_policy_uri": x["minimumTimeOffIncrementPolicyUri"],
        "startEnd_time_specification_requirement_uri":x["startEndTimeSpecificationRequirementUri"],
        "measurement_unit_uri":x["measurementUnitUri"],
        "timeoff_display_format":x["timeOffDisplayFormatUri"],
        "timeoff_balance_tracking_option_uri": x['timeOffBalanceTrackingOptionUri'],
        "name": x['displayText'],
        "enabled": x['enabled'],
        "uri": x['uri']}, data))


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        temp_doc = list(reader)
    return list(map(lambda x: x['TimeOffPlan'], temp_doc))


def active_user(load_report_data):
    jsonValue = rail.load_all_records(rail.result(load_report_data))
    return list(filter(lambda x: x["Country"] == request_payload.get_dag_run_conf()['country']
                                and x['Worker Type'] == "Employee"
                       , jsonValue))


def map_conf_time_off_uri(response):
    created_time_off = request_payload.get_dag_run_conf()['time_off_types']
    data = response.json()['d']
    return list(map(lambda x: x['uri'], list(filter(lambda x: x['name'] in created_time_off, data))))


def map_assigned_time_off_uri(response):
    data = response.json()[
        'd'][0]['timeOffTypeAssignmentsDetails']['timeOffTypes']
    return list(map(lambda x: x['uri'], data))

def get_updated_timeoff_mapper_callable(dag_run, action):
    current_mapper_for_location = rail.result("get_country_mapper") or []
    updated_mapper_for_location = []
    if not current_mapper_for_location and action == "remove":
        return json.dumps([])
    if current_mapper_for_location:
        current_mapper_for_location = json.loads(current_mapper_for_location['jsonValue'])
    if action == "remove":
        updated_mapper_for_location = list(filter(lambda timeoff: timeoff['timeoffname'] != dag_run.conf['feed_timeoff_name'], current_mapper_for_location))
    if action == "add":
        updated_mapper_for_location = current_mapper_for_location + [{
                                                "type": "timeoff",
                                                "country": dag_run.conf['country'],
                                                "timeoffname": dag_run.conf['time_off_type_name']
                                            }]
    return json.dumps(list({v['timeoffname']:v for v in updated_mapper_for_location}.values()))
