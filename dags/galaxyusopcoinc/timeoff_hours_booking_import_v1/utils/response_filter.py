import rail


def get_time_off_type_uris(response):
    return list(map(lambda row: row['uri'], response))


def get_filtered_timeoff_details(response):
    return rail.write_json_artifact(list(map(lambda row: {
        "displayText": row['displayText'],
        "description": row['description'],
        "enabled": row['enabled'],
        "uri": row['uri']
    }, response)))


def get_filtered_output_user_info(response):
    if not response:
        return []

    return list(map(lambda row: {
        "timeoff_template": row['timeOffTemplate'],
        "start_date": row['userDetails']['employmentDateRange']['startDate'],
        "end_date": row['userDetails']['employmentDateRange']['endDate'],
        'uri': row['userDetails']['uri'],
        'user_name': row['userDetails']['firstName'],
        'user_email': row['userDetails']['emailAddress']
    }, response))[0]


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
    return list(filter(lambda x: x['booking_id_oef_name'] == 'BookingReferenceID', map(lambda row: {
        "booking_id_oef_name": row['cells'][0]['textValue'],
        "booking_id_oef_value": (row['cells'][1]['uri']).split(':')[-1],
    }, response['rows'])))[0]


def get_filtered_time_off_details_on_sf_booking_id(response, dag_run):
    if not response['rows']:
        return []
    return list(filter(lambda x: x['booking_id'] == dag_run.conf['booking_id'], map(lambda row: {
        "timeoff_uri": row['cells'][0]['uri'],
        "timeoff_type": row['cells'][1]['textValue'],
        "timeoff_type_uri": row['cells'][1]['uri'],
        "booking_id": row['cells'][2]['textValue'],
        "timeoff_hours": row['cells'][3]['textValue'],
    }, response['rows'])))[0]


def get_value(item, index, pluck_key):
    return item[index].get(pluck_key)


def get_timesheet_details(response):
    if not response['rows']:
        return []
    return list(filter(lambda item: item['timesheet_status'] == 'Approved', map(lambda ts: {
        "timesheet_status": get_value(ts['cells'], 0, 'textValue'),
        "timesheet_status_uri": get_value(ts['cells'], 0, 'uri'),
        "timesheet_uri": get_value(ts['cells'], 1, 'uri'),
        "timesheet_date_range": get_value(ts['cells'], 2, 'textValue'),
        "user_uri": get_value(ts['cells'], 3, 'uri')
    }, response['rows'])))


def get_timesheet_details_after_process(response):
    if not response['rows']:
        return []
    resp = list(filter(lambda item: item['timesheet_status'] == 'Approved', map(lambda ts: {
        "timesheet_status": get_value(ts['cells'], 0, 'textValue'),
        "timesheet_status_uri": get_value(ts['cells'], 0, 'uri'),
        "timesheet_uri": get_value(ts['cells'], 1, 'uri'),
        "timesheet_date_range": get_value(ts['cells'], 2, 'textValue'),
        "user_uri": get_value(ts['cells'], 3, 'uri')
    }, response['rows'])))

    old_data = rail.result("get_user_timesheet_details")

    return [item['timesheet_date_range'] for item in old_data if item['timesheet_uri'] not in resp]
