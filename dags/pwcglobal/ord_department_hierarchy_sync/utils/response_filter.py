

def get_department_list_details(response):
    data = response.json()['d']

    def get_full_path(cell):
        values = []
        for collection in cell['cellCollection']:
            values.append(collection['textValue'])
        return "/".join(values)

    return list(map(lambda row: {
        "name": row['cells'][0].get('textValue'),
        "status": row['cells'][2].get('textValue'),
        "uri": row['cells'][0].get('uri'),
        "fullpath": get_full_path(row['cells'][1])
    }, data['rows'])) if data['rows'] else []


def get_department_list_details_with_code(response):
    data = response.json()['d']

    def get_full_path(cell):
        values = []
        for collection in cell['cellCollection']:
            values.append(collection['textValue'])
        return "/".join(values)

    return list(map(lambda row: {
        "name": row['cells'][0].get('textValue'),
        "status": row['cells'][2].get('textValue'),
        "uri": row['cells'][0].get('uri'),
        "fullpath": get_full_path(row['cells'][1]),
        "code": row['cells'][3].get('textValue')
    }, data['rows'])) if data['rows'] else []


def get_userlist(response):
    data = response.json()['d']

    return list(map(lambda row: {
        "name": row['cells'][0].get('textValue'),
        "status": row['cells'][1].get('textValue'),
        "uri": row['cells'][0].get('uri'),
        "departmentgroup": row['cells'][2].get('textValue'),
        "departmentgroupcode": row['cells'][2].get('textValue').rsplit(' ', 1)[-1] if row['cells'][2].get('textValue') else ''
    }, data['rows'])) if data['rows'] else []


def get_projectlist(response):
    data = response.json()['d']

    return list(map(lambda row: {
        "name": row['cells'][0].get('textValue'),
        "status": row['cells'][1].get('textValue'),
        "uri": row['cells'][0].get('uri')
    }, data['rows'])) if data['rows'] else []
