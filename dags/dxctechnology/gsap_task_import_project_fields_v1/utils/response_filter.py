from itertools import chain
import rail
from dxctechnology.gsap_task_import_project_fields_v1.utils.request_payload import get_task_name

null = None

def is_task_name_same(x, dag_run):
    return x['cells'][0]['textValue'] == get_task_name(dag_run).strip()

def map_attribute_2_uri(response):
    data = response.json()['d']
    return list(filter(lambda x: x['displayText'] == "GSAP Task", data))


def map_get_specific_attribute_system_level(response, dag_run):
    data = response.json()['d']['rows']
    if data:
        return list(filter(
            lambda x: is_task_name_same(x, dag_run),data))
    return []


def map_get_project_details(response):
    data = response.json()['d']
    if not data[0]['error']:
        return list(map(lambda project: {
            "uri": project['projectDetails']['uri'],
            "status": project['projectDetails']['status']['name'],
            "start_date_year": project['projectDetails']['timeEntryDateRange']['startDate']['year']
            if project['projectDetails']['timeEntryDateRange']['startDate'] else null,
            "start_date_month": project['projectDetails']['timeEntryDateRange']['startDate']['month']
            if project['projectDetails']['timeEntryDateRange']['startDate'] else null,
            "start_date_day": project['projectDetails']['timeEntryDateRange']['startDate']['day']
            if project['projectDetails']['timeEntryDateRange']['startDate'] else null,
            "end_date_year": project['projectDetails']['timeEntryDateRange']['endDate']['year']
            if project['projectDetails']['timeEntryDateRange']['endDate'] else null,
            "end_date_month": project['projectDetails']['timeEntryDateRange']['endDate']['month']
            if project['projectDetails']['timeEntryDateRange']['endDate'] else null,
            "end_date_day": project['projectDetails']['timeEntryDateRange']['endDate']['day']
            if project['projectDetails']['timeEntryDateRange']['endDate'] else null,
            "extensionFieldValue": project['projectDetails']['extensionFieldValues']
        }, data))
    return []


def map_get_specific_attribute_project_level(response, dag_run):
    data = response.json()['d']
    task_name = get_task_name(dag_run)
    return list(filter(lambda atr: atr['tag']['displayText'] == task_name, data))


def map_get_attribute_system_level_project(response, dag_run):
    data = response.json()['d']['rows']
    return list(filter(
        lambda x: is_task_name_same(x, dag_run),data))


def map_parent_column_uri(response):
    data = response.json()['d']
    basic_uris = list(filter(lambda x: x['displayText'] == "Basic", data))
    return list(filter(lambda x: x['displayText'] == "Parent WBS", basic_uris[0]['columns']))


def map_parent_wbs_oef_uri(response):
    data = response.json()['d']
    return list(filter(lambda x: x['name'] == "Parent WBS", data))


def map_child_wbs(response, dag_run):
    data = response.json()['d']['rows']
    return list(map(lambda item: item['cells'][0]['textValue'], list(filter(lambda x: x['cells'][1]['textValue']
                                                                            == dag_run.conf['wbs'], data))))

def get_all_assigned_gsap_task_for_project_filter(response):
    return list(map(lambda task: {
        "name": task['tag']['displayText'],
        "code": task['tag']['displayText'],
        "actual_code": task['tag']['displayText'].split(" - ")[-1],
        "actual_name": task['tag']['displayText'].split(" - ")[0],
        "uri": task['tag']['uri'],
        "status": task['isEnabled'],
        "start_date": task['dateRange']['startDate'],
        "end_date": task['dateRange']['endDate']
    }, response))


def map_task_to_service_call_output(processed_records, processed_records_res):
    return list(map(lambda task: {
        **task,
        **{
            "task_details": rail.find_first_by_attr_and_get_attr(processed_records, 'replicon_oef_task_details.uri', task['target']['uri'], default={})
        }
    }, processed_records_res))

def combine_task_add_update_output(response, record_key=None):
    processed_records = rail.load_json_artifact(rail.result('get_task_add_update', record_key))
    return {
        "added": rail.write_json_artifact(map_task_to_service_call_output(processed_records, list(chain(*map(lambda item: item['added'], response))))),
        "errors": rail.write_json_artifact(map_task_to_service_call_output(processed_records, list(chain(*map(lambda item: item['errors'], response))))),
        "removed": []
    }
