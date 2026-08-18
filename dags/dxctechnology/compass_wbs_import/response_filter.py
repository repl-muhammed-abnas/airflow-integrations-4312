import rail
from dxctechnology.compass_wbs_import import request_payload

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
