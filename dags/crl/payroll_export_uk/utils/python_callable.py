import pendulum

def get_time_in_formats(time_zone):
    current_time = pendulum.now(time_zone)
    return {
        "start_time": str(current_time),
        "ymd_format": current_time.strftime("%Y%m%d"),
        "hms_format": current_time.strftime("%H%M%S")
    }

def get_filtered_allowed_location_uris(response):
    if not response['rows']:
        return []
    location_list = list(filter(lambda x: x['displaytext'] == "GBR", list(map(lambda item: {
        "uri": item['cells'][0]['uri'],
        "displaytext": item['cells'][1]['cellCollection'][0]['textValue']
    }, response['rows']))))

    return [item['uri'] for item in location_list]


def get_enabled_employee(response):
    if not response:
        return []
    return list(set(map(lambda data: data['cells'][0]['uri'], response['rows'])))


def get_employee_types(response):
    if not response:
        return []

    return list(filter(lambda x: x['uri'], list(map(lambda item: {
        "uri": item['uri'],
        "displaytext": item['displayText']
    }, response))))
