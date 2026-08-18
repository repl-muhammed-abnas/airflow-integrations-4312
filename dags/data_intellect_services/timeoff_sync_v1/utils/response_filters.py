import json

null = None

def filter_timeoff_data(response):
    timeoff_data = json.loads(response.text)["changes"]
    return {
        "created": list(filter(lambda timeoff_records: timeoff_records["employeeId"] != '' and timeoff_records["employeeId"] != null
                    and timeoff_records["requestId"] != '' and timeoff_records["requestId"] != null and timeoff_records["changeType"] == "Created", timeoff_data)),
        "canceled_deleted": list(filter(lambda timeoff_records: timeoff_records["employeeId"] and timeoff_records["requestId"] and 
                    timeoff_records["changeType"] in ["Canceled", "Deleted"], timeoff_data))
    }

def filter_users_data(response):
    users_data = json.loads(response.text)["employees"]
    return list(map(lambda user_records: {
        "employee_id": user_records["work"]["employeeIdInCompany"],
        "employee_name": user_records["displayName"],
        "unique_id": user_records["id"]
    }, users_data))


def get_booking_id_oef_value(response):
    return list(filter(lambda x: x['booking_id_oef_name'] == 'Booking_ID', map(lambda row: {
        "booking_id_oef_name": row['cells'][0]['textValue'],
        "booking_id_oef_value": (row['cells'][1]['uri']).split(':')[-1],
    }, response['rows'])))[0]

def get_filtered_time_off_details_on_booking_id(response, dag_run):
    request_id = str(dag_run.conf['booking_data']['requestId'])
    return list(filter(lambda x: x['booking_id'] == request_id, map(lambda row: {
        "timeoff_uri": row['cells'][0]['uri'],
        "timeoff_type": row['cells'][1]['textValue'],
        "timeoff_type_uri": row['cells'][1]['uri'],
        "booking_id": row['cells'][2]['textValue'],
        "timeoff_start_date": str(row['cells'][3]['dateValue']['year'])+'-'+str(row['cells'][3]['dateValue']['month']).zfill(2)
        + '-'+str(row['cells'][3]['dateValue']['day']).zfill(2),
        "timeoff_end_date": str(row['cells'][4]['dateValue']['year'])+'-'+str(row['cells'][4]['dateValue']['month']).zfill(2)
        + '-'+str(row['cells'][4]['dateValue']['day']).zfill(2)
    }, response['rows'])))

def get_timeoff_desc(response):
    return json.loads(response.text)["description"]
