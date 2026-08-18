import rail
from rail import find_first_by_attr_and_get_attr
from crl.project_import_v2.utils.request_payload import get_project_data
def get_client_data_from_list_service(response,dag_run):
    if not response['rows']:
        return []

    response_data = list(filter(lambda item: item['code'] == dag_run.conf['clientcode'],map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "code": row['cells'][1]['textValue'],
        "uri": row['cells'][0]['uri']
    }, response['rows'])))

    return response_data[0] if response_data else []

def get_project_data_from_list_service(response):
    if not response['rows']:
        return []

    return list(map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "uri": row['cells'][0]['uri']
    }, response['rows']))[0]

def get_data_from_list_service(response,items):
    if not response['rows']:
        return []

    return list(filter(lambda item: item['name'] == items['businessarea'],map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "uri": row['cells'][0]['uri']
    }, response['rows'])))

def format_project_task_details(response):
    return list(map(lambda task: {
        "task_name": task['name'],
        "task_code": task['code'],
        "uri": task['uri'],
        "company_code": find_first_by_attr_and_get_attr(task[
            'customFields'],"customField.displayText","WC Company Code", "text", "")
    }, response))
