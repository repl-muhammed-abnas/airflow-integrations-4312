def get_filtered_time_off_types(response,timeoff_types):
    return [x['uri'] for x in response if x['displayText'] in timeoff_types]

def get_filtered_licenses(response, licenses):
    return [x['uri'] for x in response if x['displayText'] in licenses]
