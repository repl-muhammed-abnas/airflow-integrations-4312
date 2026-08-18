import rail

def get_date(date):
    if not date:
        return None
    year = date['year']
    month = date['month']
    day = date['day']
    return str(year)+'-'+str(month).zfill(2)+'-'+str(day).zfill(2)

def get_filtered_output_user_info(response):
    if not response:
        return []

    return list(map(lambda row: {
        "timeoff_template": row['timeOffTemplate'],
        "start_date": get_date(row['userDetails']['employmentDateRange']['startDate']),
        "end_date": get_date(row['userDetails']['employmentDateRange']['endDate']),
        'uri': row['userDetails']['uri'],
        'user_name': row['userDetails']['firstName'],
        'user_email': row['userDetails']['emailAddress'],
        'enabled': row['userDetails']['isEnabled']
    }, response))[0]

def get_booking_id_oef_value(response):
    return list(filter(lambda x: x['booking_id_oef_name'] == 'BookingReferenceID', map(lambda row: {
        "booking_id_oef_name": row['cells'][0]['textValue'],
        "booking_id_oef_value": (row['cells'][1]['uri']).split(':')[-1],
    }, response['rows'])))[0]


def get_timeoff_booking_details(response):
    return list(filter(lambda x: x['booking_id'] is not None, map(lambda row: {
        "booking_id": row['cells'][1]['textValue'] if row['cells'][1]['dataType'] != "urn:replicon:list-type:null" else None,
        "timeoff_uri": row['cells'][0]['uri'],
    }, response['rows'])))


def get_filtered_time_off_details_on_sf_booking_id(response, dag_run):
    if not response['rows']:
        return {
        "is_timeoff_uri_present": [],
        "timeoff_details": []
    }
    timeoff_list =  list(map(lambda row: {
        "timeoff_uri": row['cells'][0]['uri'],
        "start_date": row['cells'][1]['textValue'],
        "end_date": row['cells'][2]['textValue'],
        "timeoff_type": row['cells'][3]['uri'],
        "booking_id": row['cells'][4]['textValue'] if row['cells'][4]['dataType'] != "urn:replicon:list-type:null" else None,
        "user_uri": row['cells'][5]['uri'],
    }, response['rows']))

    check_booking_id = bool(list(filter(lambda x: x['booking_id'] == dag_run.conf['booking_id'], timeoff_list)))

    return {
        "is_timeoff_uri_present": check_booking_id,
        "timeoff_details": list(filter(lambda x: x['booking_id'] is None, timeoff_list))
    }


def get_timesheet_value(item, index, pluck_key):
    return item[index].get(pluck_key)


def get_timesheet_details(response):
    if not response['rows']:
        return []
    return list(map(lambda ts: {
        "timesheet_status": get_timesheet_value(ts['cells'], 0, 'textValue'),
        "timesheet_status_uri": get_timesheet_value(ts['cells'], 0, 'uri'),
        "timesheet_uri": get_timesheet_value(ts['cells'], 1, 'uri'),
        "timesheet_date_range": get_timesheet_value(ts['cells'], 2, 'dateRangeValue'),
        "timesheet_period": get_timesheet_value(ts['cells'], 2, 'textValue'),
        "user_uri": get_timesheet_value(ts['cells'], 3, 'uri')
    }, response['rows']))


def get_timesheet_details_to_approve(response):
    if not response['rows']:
        return []
    return list(filter(lambda ts: ts['timesheet_status'] == 'Not Submitted', map(lambda ts: {
        "timesheet_status": get_timesheet_value(ts['cells'], 0, 'textValue'),
        "timesheet_status_uri": get_timesheet_value(ts['cells'], 0, 'uri'),
        "timesheet_uri": get_timesheet_value(ts['cells'], 1, 'uri'),
        "timesheet_date_range": get_timesheet_value(ts['cells'], 2, 'dateRangeValue'),
        "timesheet_period": get_timesheet_value(ts['cells'], 2, 'textValue'),
        "user_uri": get_timesheet_value(ts['cells'], 3, 'uri')
    }, response['rows']))
)
