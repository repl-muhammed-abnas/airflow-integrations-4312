def get_specific_locations(response, termination_balance_req_data):
    return [
        location_data for location_data in response
        if location_data["displayText"] in {data["location"] for data in termination_balance_req_data}
    ]

def get_specific_timeoff_types(response, timeoff_types):
    return [
        timeoff_type for timeoff_type in response
        if timeoff_type["displayText"] in {timeoff_item["leave_type"] for data in timeoff_types for timeoff_item in data["timeoff_types"]}
    ]
