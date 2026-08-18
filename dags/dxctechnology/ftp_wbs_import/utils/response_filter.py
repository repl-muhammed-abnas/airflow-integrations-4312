from dxctechnology.ftp_wbs_import.utils import python_callable_method


def get_filtered_company_code(response):
    data = response.json()['d']
    return list(map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "fullpath": " | ".join(list(map(lambda x: x['textValue'], row['cells'][1]['cellCollection'])))
                    if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else None,
        "uri": row['cells'][0]['uri'],
        "parent": (" | ".join(list(map(lambda x: x['textValue'], row['cells'][1]['cellCollection'])))).split(' | ', maxsplit=1)[0],
        "parenturi": (" | ".join(list(map(lambda x: x['uri'], row['cells'][1]['cellCollection'])))).split(' | ', maxsplit=1)[0]
    }, data['rows']))


def get_filtered_output_empid(response):
    data = response.json()['d']
    return list(map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "fullpath": "|".join(list(map(lambda x: x['textValue'], row['cells'][1]['cellCollection'])))
                    if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else "No Employee Group assigned",
        "uri": row['cells'][0]['uri'],
        "employeeid": row['cells'][2]['textValue'] if row['cells'][2]['dataType'] != 'urn:replicon:list-type:null' else None,
        "status": row['cells'][3]['textValue'],
        "enddate": row['cells'][4]['textValue']
    }, data['rows']))


def program_filter(response, program_name):
    data = response.json()['d']
    result = list(map(lambda row: {
        'slug': row['cells'][0]['slug'],
        'textValue': row['cells'][0]['textValue'],
        'uri': row['cells'][0]['uri']
    }, data['rows']))
    return [i for i in result if i['textValue'] == program_name]


def map_project_client(response):
    client_name = python_callable_method.get_dag_run_conf()['Clientname']
    data = response.json()['d']['rows']
    result = list(filter(lambda x: x['cells']
                  [0]['textValue'] == client_name, data))
    return result
