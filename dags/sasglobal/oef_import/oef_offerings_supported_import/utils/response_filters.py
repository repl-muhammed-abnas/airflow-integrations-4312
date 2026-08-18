null = None

def filter_object_extension_tags(response):
    return list(map(lambda data: {
                "name": data["name"],
                "uri": data["uri"],
                "status": "Enabled" if data["isEnabled"] else "Disabled"
    }, response.json()['d']['tags']))

def get_filtered_input_data(item):
    return [
        item['Name'].strip() if  item['Name'] != null else null,
        item['Values'].strip() if item['Values'] != null else null,
        item['Status'].strip() if item['Status'] != null else null
    ]
