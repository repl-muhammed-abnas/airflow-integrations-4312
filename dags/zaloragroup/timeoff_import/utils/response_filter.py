null = None
null_urn = "urn:replicon:list-type:null"

def get_value(item, index, pluck_key='textValue'):
    return item[index][pluck_key] if item[index]['dataType'] != null_urn else null

def get_user_details(response):
    data = response.json()['d']
    if data and data['rows']:
        return list((map(lambda row: {
            'user_name': get_value(row['cells'], 0, 'textValue'),
            'uri': get_value(row['cells'], 1, 'uri'),
            'loginname': get_value(row['cells'], 1, 'textValue'),
            'status': get_value(row['cells'], 2, 'textValue'),
        }, data['rows'])))
    return null

def get_timesheet_uri(response):
    data = response.json()['d']
    return data['timesheet']['uri'] if data and data['timesheet'] else ''

def get_timesheet_status(response):
    data = response.json()['d']
    return data['statusUri'].split(':')[-1] if data and data['uri'] else ''

def get_published_timeoff_draft_uri(response):
    data = response.json()['d']
    return data['uri']
