import rail

null = None


def get_client_uri(response, dag_run):
    data = response['rows']
    if not data:
        return []

    return list(filter(lambda x: x['clientname'] == dag_run.conf['clientname'], list(map(lambda item: {
        'uri': item['cells'][0]['uri'],
        'clientname': item['cells'][0]['textValue']
    }, data))))[0]['uri']


def get_project_lists(response, dag_run):
    rows = response['rows']
    return list(filter(lambda x: x['projectcode'] == str(dag_run.conf['webhook']['data']['Project_Code']),
                       list(map(lambda row: {
                           'projectname': (row['cells'][0]['textValue']).split('|')[0].strip(),
                           'projectcode':  row['cells'][1]['textValue'],
                           'uri': row['cells'][0]['uri']
                       }, rows)))) if rows else []


def get_users_details(response, dag_run):
    return list(filter(lambda x: x['employeeid'] == dag_run.conf['projectmanagerid'], list(map(lambda x: {
        'name': x['cells'][0]['textValue'],
        'uri': x['cells'][0]['uri'],
        'employeeid': x['cells'][1].get('textValue'),
        'status': x['cells'][2].get('textValue')
    }, response['rows']))))


def get_departmentlist(response, dag_run):
    fullpath = 'Technicolor / Entertainment Services Business Group / Creative Studios / Advertising Service Line / '
    fullpath += 'MPC - Advertising' if dag_run.conf[
        'millmpc'] == 'mpc' else 'The Mill (Advertising)'

    return list(filter(lambda x: x['fullpath'] == fullpath, list(map(lambda row: {
        'name': rail.find_first_by_attr_and_get_attr(row['cells'], 'dataType', 'urn:replicon:list-type:object', 'textValue'),
        'fullpath': " / ".join([x['textValue'] for x in row['cells'][-1]['cellCollection']]) if row['cells'][-1]['cellCollection'] else null,
        'uri': rail.find_first_by_attr_and_get_attr(row['cells'], 'dataType', 'urn:replicon:list-type:object', 'uri')
    }, response['rows']))))


def get_required_customfields(response):
    data = response.json()['d']
    return {
        'mill_mpc_uri': rail.find_first_by_attr_and_get_attr(data, 'displayText', 'Mill / MPC', 'uri'),
        'project_buckets_uri': rail.find_first_by_attr_and_get_attr(data, 'displayText', 'Project Buckets', 'uri'),
        'project_id_uri': rail.find_first_by_attr_and_get_attr(data, 'displayText', 'Project ID', 'uri'),
        'project_type_uri': rail.find_first_by_attr_and_get_attr(data, 'displayText', 'Project Type', 'uri'),
        'project_classification_uri': rail.find_first_by_attr_and_get_attr(data, 'displayText', 'Project Classification', 'uri'),
        'product_name_uri': rail.find_first_by_attr_and_get_attr(data, 'displayText', 'Product Name', 'uri'),
        'product_id_uri': rail.find_first_by_attr_and_get_attr(data, 'displayText', 'Product ID', 'uri')
    }


def get_customfields_mill_mpc(response, dag_run):
    mill_mpc = null if not dag_run.conf['millmpc'] else 'MPC' if dag_run.conf['millmpc'] == 'mpc' else 'Mill'
    return {
        "customField": {
            "uri": rail.result('get_required_customfields')['mill_mpc_uri']
        },
        "dropDownOption": {
            "uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', mill_mpc, 'uri')
        }
    } if rail.find_first_by_attr_and_get_attr(response, 'displayText', mill_mpc, 'uri') else null


def get_customfields_project_buckets(response):
    data = response.json()['d']
    return {
        "customField": {
            "uri": rail.result('get_required_customfields')['project_buckets_uri']
        },
        "dropDownOption": {
            "uri": rail.find_first_by_attr_and_get_attr(data, 'displayText', 'Client Project', 'uri')
        }
    } if rail.find_first_by_attr_and_get_attr(data, 'displayText', 'Client Project', 'uri') else null


def get_customfields_project_type(response, dag_run):
    project_type_dropdown_uri = null
    if dag_run.conf['projecttype']:
        project_type_dropdown_uri = rail.find_first_by_attr_and_get_attr(
            response, 'displayText', dag_run.conf['projecttype'], 'uri')
    else:
        project_type_dropdown_uri = rail.find_first_by_attr_and_get_attr(
            response, 'displayText', '-', 'uri')
    return {
        'project_type_dropdown_uri': project_type_dropdown_uri
    } if response else null


def get_dropdown_options_list(response, dag_run):
    dropdown_list = list(map(lambda item: {
        'target': {
            'uri': item['uri'],
            'name': item['displayText']
        },
        'name': item['displayText'],
        'isEnabled': item['isEnabled']
    }, response))
    dropdown_list.append({
        'target': {
            'uri': null,
            'name': null
        },
        'name': dag_run.conf['dropdownoption'],
        'isEnabled': True
    })
    return dropdown_list


def get_dropdown_options_project_buckets(response, dag_run):
    return {
        'project_type_dropdown_uri': rail.find_first_by_attr_and_get_attr(
            response, 'displayText', dag_run.conf['projecttype'], 'uri')
    }


def get_customfields_project_classification(response, dag_run):
    project_classification_dropdown_uri = null
    if dag_run.conf['projectclassification']:
        project_classification_dropdown_uri = rail.find_first_by_attr_and_get_attr(
            response, 'displayText', dag_run.conf['projectclassification'], 'uri')
    else:
        project_classification_dropdown_uri = rail.find_first_by_attr_and_get_attr(
            response, 'displayText', '-', 'uri')
    return {
        'project_classification_dropdown_uri': project_classification_dropdown_uri
    } if response else null


def get_dropdown_options_project_classification(response, dag_run):
    return {
        'project_classification_dropdown_uri': rail.find_first_by_attr_and_get_attr(
            response, 'displayText', dag_run.conf['projectclassification'], 'uri')
    }


def get_project_details_response(response):
    data = response.json()['d']

    def getcustomfields():
        return list(map(lambda x: {
            'name': x['customField']['displayText'],
            'value': x['text'],
            'customfielduri': x['customField']['uri']
        }, data['customFields'])) if data['customFields'] else []
    return {
        'projectname': data['name'],
        'statusname': data['status']['name'],
        'statusuri': data['status']['uri'],
        'projectleaderuri':  data['projectLeader']['uri'] if data['projectLeader'] else null,
        'customfields': getcustomfields()
    } if data else null
