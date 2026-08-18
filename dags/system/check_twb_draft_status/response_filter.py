def get_draft_timeexports(response):
    data = response.json()['d']
    if data['rows']:
        filtered_data = list(
            filter(lambda x: x['cells'][1]['textValue'] == "Draft", data['rows']))
        return list(map(lambda item: {
            "time_export_name": item["cells"][0]["textValue"],
            "creation_time": item["cells"][2]["textValue"],
            "creator": item["cells"][3]["textValue"],
            "twb_uri": item["cells"][0]["uri"]
        }, filtered_data))
    return []
