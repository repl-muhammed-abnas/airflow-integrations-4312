import rail
import itertools
from functools import lru_cache


def get_value(data, index, pluck_key):
    return data['cells'][index].get(pluck_key)


def get_full_path(full_path_list):
    if not full_path_list:
        return ""
    return "|".join([item['textValue'] for item in full_path_list])


def filter_group_data(result):
    rows_list = list(itertools.chain(
        *list(map(lambda x: x['rows'], result))))

    if not rows_list:
        return []

    return list(map(lambda item: {
        "name": get_value(item, 0, "textValue"),
        "uri": get_value(item, 0, "uri"),
        "code": get_value(item, 1, "textValue"),
        "fullpath": get_full_path(item['cells'][2]['cellCollection']),
        "enabled": get_value(item, 3, "textValue"),
    }, rows_list))


def filter_full_path_code_data(response):
    if not response['rows']:
        return []

    return list(map(lambda data: {
        "name": get_value(data, 0, 'textValue'),
        "uri": get_value(data, 0, 'uri'),
        "full_path_code": get_full_path(data['cells'][1]['cellCollection'])
    }, response['rows']))


@lru_cache(maxsize=32)
def get_all_groups_exception_entries(filtered_group_log_artifact):
    return rail.load_all_records(filtered_group_log_artifact)


def get_filtered_replicon_time_off_types(response):
    return list(map(lambda item: {
        "name": item['displayText'],
        "uri": item['uri'],
    }, response))


def get_missing_permissions(response, dag_run):
    permissions_to_add = []

    if not rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision'):
        if not (dag_run.conf['supervisor_permission_set_uri']):
            return "Error: Permission set with name : 'Supervisor' not found in Replicon"
        permissions_to_add.append(
            dag_run.conf['supervisor_permission_set_uri'])

    return permissions_to_add
