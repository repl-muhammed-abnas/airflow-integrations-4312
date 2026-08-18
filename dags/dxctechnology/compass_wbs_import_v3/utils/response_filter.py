import rail
from dxctechnology.compass_wbs_import_v3.utils import request_payload

def map_client_name(response):
    client=request_payload.get_dag_run_conf()['client']
    data = response.json()['d']
    return (rail.find_first_by_attr_and_get_attr(
            list(
                map(
                    lambda x: x['cells'][0],
                    data['rows'])),
            'textValue',client, 'textValue'))

def map_program_name(response):
    program_name=request_payload.get_dag_run_conf()['program_name']
    data = response.json()['d']
    return (rail.find_first_by_attr_and_get_attr(
            list(
                map(
                    lambda x: x['cells'][0],
                    data['rows'])),
            'textValue',program_name, 'textValue'))


def map_client_uri(response):
    client=request_payload.get_dag_run_conf()['client']
    data = response.json()['d']
    if  len(data['rows']) > 0:
        row_index = rail.find_index_by_attr(
            list(map(lambda x: x['cells'][0], data['rows'])), 'textValue', client)
        if row_index >= 0:
            return data['rows'][row_index]['cells'][1]['uri']
    return None

def get_filtered_child_projects(response, dag_run):
    data = response.json()['d']['rows']
    return list(filter(lambda x: x['parentwbsname'] == dag_run.conf['wbs'], map(lambda item: {
        "slug": item['cells'][0]['slug'],
        "textValue": item['cells'][0]['textValue'].split(' - ')[0].strip(),
        "uri": item['cells'][0]['uri'],
        "parentwbsname": item['cells'][1].get('textValue'),
    }, data))) if data else []

def do_format_logs():
    log_records = []

    logs = [rail.result("create_log")] + (rail.result("gather_process_inactive_projects_logs")
        if rail.result("gather_process_inactive_projects_logs") else []) + (rail.result("gather_process_active_projects_logs")
            if rail.result("gather_process_active_projects_logs") else [])

    for log in logs:
        each_log_records = rail.load_all_records(log)
        if each_log_records:
            log_records.extend(each_log_records)

    final_log_records = []

    final_log_records = list(map(lambda log: {
        **log['properties'],
        'message': log['message'],
        'ecid': log['ecid']
        }, log_records))

    rail.set_result(key="get_success_projects", val=len(list(filter(lambda item: item['status']=="Success", final_log_records))))
    rail.set_result(key="get_errored_projects", val=len(list(filter(lambda item: item['status']=="Error", final_log_records))))
    rail.set_result(key="get_exception_projects", val=len(list(filter(lambda item: item['status']=="Exception", final_log_records))))
    rail.set_result(key="get_skipped_projects", val=len(list(filter(lambda item: item['status']=="Skipped", final_log_records))))

    return final_log_records
