def get_time_off_type_uris(response):
    return list(map(lambda row: row['uri'], response))


def get_filtered_timeoff_details(response):
    return list(map(lambda row: {
        "description": row['description'],
        "displayText": row['displayText'],
        "enabled": row['enabled'],
        "uri": row['uri']
    }, response))


def get_filtered_time_off_details_on_booking_id(response, dag_run):
    unique_id = str(dag_run.conf['unique_id'])
    return list(filter(lambda x: x['booking_id'] == unique_id, map(lambda row: {
        "timeoff_uri": row['cells'][0]['uri'],
        "timeoff_type": row['cells'][1]['textValue'],
        "timeoff_type_uri": row['cells'][1]['uri'],
        "booking_id": row['cells'][2]['textValue'],
        "timeoff_start_date": str(row['cells'][3]['dateValue']['year'])+'-'+str(row['cells'][3]['dateValue']['month']).zfill(2)
        + '-'+str(row['cells'][3]['dateValue']['day']).zfill(2),
        "timeoff_end_date": str(row['cells'][4]['dateValue']['year'])+'-'+str(row['cells'][4]['dateValue']['month']).zfill(2)
        + '-'+str(row['cells'][4]['dateValue']['day']).zfill(2),
        "hours": row["cells"][5]["textValue"],
        "approval_status": row["cells"][6]["textValue"],
    }, response['rows'])))


def get_booking_id_oef_value(response):
    return list(filter(lambda x: x['booking_id_oef_name'] == 'Booking_ID', map(lambda row: {
        "booking_id_oef_name": row['cells'][0]['textValue'],
        "booking_id_oef_value": (row['cells'][1]['uri']).split(':')[-1],
    }, response['rows'])))[0]
