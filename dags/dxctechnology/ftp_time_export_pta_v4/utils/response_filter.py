def map_all_past_time_export(response):
    data = response.json()['d']['rows']
    return list(filter(lambda x: x['cells'][1]['textValue'] == "Complete", data))
