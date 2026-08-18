def get_time_off_type_uris(response):
    data = response.json()['d']
    return list(map(lambda row: row['uri'], data))


def get_filtered_output_empid(response):
    data = response.json()['d']
    return list(filter(lambda x: bool(x['employeeid']) and x['status'] == 'True', map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "uri": row['cells'][0]['uri'],
        "employeeid": row['cells'][1]['textValue']if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else None,
        "status": row['cells'][2]['textValue']
    }, data['rows'])))


def get_filtered_timeoff_summary(response):
    data = response.json()['d']['policiesByTimeOffType']
    return list(map(lambda row: {
        "timeOffType": row['timeOffType']['displayText'],
        "timeOffTypeuri": row['timeOffType']['uri'],
        "policySetSchedule": row['policySetSchedule'],
        "isTimeOffAllowedAgainstThisTimeOffType": row['isTimeOffAllowedAgainstThisTimeOffType']
    }, data))


def get_filtered_timeoff_details(response):
    data = response.json()['d']
    return list(map(lambda row: {
        "description": row['description'],
        "displayText": row['displayText'],
        "enabled": row['enabled'],
        "uri": row['uri']
    }, data))
