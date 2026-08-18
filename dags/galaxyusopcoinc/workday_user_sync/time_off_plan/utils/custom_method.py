import rail
from galaxyusopcoinc.workday_user_sync.time_off_plan.utils import request_payload


def map_time_off_uri(response):
    data = response.json()['d']
    return list(map(lambda x: x['uri'], data))


def map_time_off_uri_description(response):
    data = response.json()['d']
    return list(map(lambda x: {
        "description": x['description'],
        "enabled": x['enabled'],
        "uri": x['uri']}, data))


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        temp_doc = list(reader)
    return list(map(lambda x: x['TimeOffPlan'], temp_doc))


def active_user(load_report_data):
    jsonValue = rail.load_all_records(rail.result(load_report_data))
    return list(filter(lambda x: x["Country"] == request_payload.get_dag_run_conf()['country'], jsonValue))


def map_conf_time_off_uri(response):
    created_time_off = request_payload.get_dag_run_conf()['time_off_types']
    data = response.json()['d']
    return list(map(lambda x: x['uri'], list(filter(lambda x: x['name'] in created_time_off, data))))


def map_assigned_time_off_uri(response):
    data = response.json()[
        'd'][0]['timeOffTypeAssignmentsDetails']['timeOffTypes']
    return list(map(lambda x: x['uri'], data))
