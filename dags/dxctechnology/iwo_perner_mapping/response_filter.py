 # pylint: disable=line-too-long
import rail


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_filtered_output_useruri(response):
    data = response.json()['d']
    dag_run_conf = get_dag_run_conf()
    useruri=dag_run_conf['C1useruri']
    return list(map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "fullpath": "|".join(list(map( lambda x: x['textValue'], row['cells'][1]['cellCollection']))) if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else\
                    None,
        "uri": useruri,
        "employeeid": row['cells'][2]['textValue'],
        "companycode": row['cells'][1]['cellCollection'][-1]['textValue'] if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else\
                    None,
        "type": row['cells'][1]['cellCollection'][0]['textValue'] if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else\
                    None,
        "status":row['cells'][3]['textValue']
    }, data['rows']))


def get_filtered_output_empid(response):
    data = response.json()['d']
    return list(map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "fullpath": "|".join(list(map( lambda x: x['textValue'], row['cells'][1]['cellCollection']))) if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else\
                    None,
        "uri": row['cells'][0]['uri'],
        "employeeid": row['cells'][2]['textValue'],
        "companycode": row['cells'][1]['cellCollection'][-1]['textValue'] if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else\
                    None,
        "type": row['cells'][1]['cellCollection'][0]['textValue'] if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else\
                    None,
        "status":row['cells'][3]['textValue']
    }, data['rows']))
