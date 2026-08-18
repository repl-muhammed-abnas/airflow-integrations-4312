def get_filtered__data(response):
    data = response.json()['d']
    return list(map(lambda row: {
        "timesheeturi": row['cells'][0]['uri']
    }, data['rows']))


def get_department_filtered__data(response, dag_run):
    data = response.json()['d']
    return list(filter(lambda x: bool(x['name']) and x['name'] == dag_run.conf['userdepartmentname'], map(lambda row: {
        "name": row['name'],
        "uri": row['uri']
    }, data)))


def get_all_paycode_filtered__data(response):
    data = response.json()['d']
    payload1 = list(filter(lambda x: bool(x['name']) and x['name'] == 'Regular Time' or x['name'] == 'Overtime', map(lambda row: {
        "name": row['name'],
        "uri": row['uri']
    }, data)))
    return payload1


def get_all_users__data(response):
    data = response.json()['d']
    return list(map(lambda row: {
        "useruri": row['cells'][0]['uri']
    }, data['rows']))
