def get_time_off_type_uris(response):
    data = response.json()['d']
    return list(map(lambda row: row['uri'], data))


def get_filtered_timeoff_details(response):
    data = response.json()['d']
    return list(map(lambda row: {
        "description": row['description'],
        "displayText": row['displayText'],
        "enabled": row['enabled'],
        "uri": row['uri']
    }, data))


def get_filtered_output_empid(response):
    data = response.json()['d']
    return list(filter(lambda x: x['employeeid'] is not None and x['status'] == 'True', map(lambda row: {
        "name": row['cells'][0]['textValue'],
        "uri": row['cells'][0]['uri'],
        "employeeid": row['cells'][1]['textValue']if row['cells'][1]['dataType'] != 'urn:replicon:list-type:null' else None,
        "status": row['cells'][2]['textValue']
    }, data['rows'])))


def get_filtered_time_off_details_on_entryid(response, dag_run):
    data = response.json()['d']
    return list(filter(lambda x: x['timeoffentryid'] == dag_run.conf['timeoffentryid'], map(lambda row: {
        "timeoffuri": row['cells'][0]['uri'],
        "timeofftype": row['cells'][1]['textValue'],
        "timeofftypeuri": row['cells'][1]['uri'],
        "timeoffentryid": row['cells'][2]['textValue'],
        "timeoffstartdate": str(row['cells'][3]['dateValue']['year'])+'-'+str(row['cells'][3]['dateValue']['month']).zfill(2)
        + '-'+str(row['cells'][3]['dateValue']['day']).zfill(2),
        "timeoffenddate": str(row['cells'][4]['dateValue']['year'])+'-'+str(row['cells'][4]['dateValue']['month']).zfill(2)
        + '-'+str(row['cells'][4]['dateValue']['day']).zfill(2)
    }, data['rows'])))


def get_filtered_time_off_approval_status(response):
    return response.json()['d']['approvalStatus']['displayText']


def get_filtered_output_user_info(response):
    data = response.json()['d']
    return list(map(lambda row: {
        "timeofftemplate": row['timeOffTemplate'],
        "startdate": row['userDetails']['employmentDateRange']['startDate'],
        "enddate": row['userDetails']['employmentDateRange']['endDate'],
    }, data))


def get_assigned_time_off_uris(response):
    data = response.json()['d']
    return list(map(lambda row: row['uri'], data))


def get_hidden_oef_value(response):
    data = response.json()['d']
    return list(filter(lambda x: x['hiddenoefname'] == 'TimeOffEntryID', map(lambda row: {
        "hiddenoefname": row['cells'][0]['textValue'],
        "hiddenoefvalue": (row['cells'][1]['uri']).split(':')[-1],
    }, data['rows'])))
