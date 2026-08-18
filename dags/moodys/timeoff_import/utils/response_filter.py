
def get_hidden_oef_value(response):
    data = response.json()['d']
    return list(filter(lambda x: x['hiddenoefname'] == 'Unique ID', map(lambda row: {
        "hiddenoefname": row['cells'][0]['textValue'],
        "hiddenoefvalue": (row['cells'][1]['uri']).split(':')[-1],
    }, data['rows'])))


def get_filtered_time_off_details_on_entryid(response, dag_run):
    data = response.json()['d']
    return list(filter(lambda x: x['timeoffentryid'] == dag_run.conf['Source_Time_Off_Booking_ID'], map(lambda row: {
        "timeoffuri": row['cells'][0]['uri'],
        "timeofftype": row['cells'][1]['textValue'],
        "timeofftypeuri": row['cells'][1]['uri'],
        "timeoffentryid": row['cells'][2]['textValue'],
        "timeoffstartdate": str(row['cells'][3]['dateValue']['year'])+'-'+str(row['cells'][3]['dateValue']['month']).zfill(2)
        + '-'+str(row['cells'][3]['dateValue']['day']).zfill(2),
        "timeoffenddate": str(row['cells'][4]['dateValue']['year'])+'-'+str(row['cells'][4]['dateValue']['month']).zfill(2)
        + '-'+str(row['cells'][4]['dateValue']['day']).zfill(2)
    }, data['rows'])))


def get_filtered_user_data(response, dag_run):
    data = response.json()['d']
    return list(filter(lambda x: bool(x['textvalue']) and x['textvalue'] == dag_run.conf['Employee_ID'], map(lambda row: {
        "name": row['cells'][0]['textValue'] if row['cells'][0]['dataType'] != 'urn:replicon:list-type:null' else None,
        "uri": row['cells'][0]['uri'],
        "textvalue": row['cells'][1]['textValue']
    }, data['rows'])))


def get_assigned_time_off_uris(response):
    data = response.json()['d']
    return list(map(lambda row: {
        "name": row['name'],
        "uri": row['uri']
    }, data))


def get_all_time_off_types(response, dag_run):
    data = response.json()['d']
    return list(filter(lambda x: x['displayText'] == dag_run.conf['Time_Type__externalcode_'], data))

def get_user_data(response):
    return [] if response == [None] else response
