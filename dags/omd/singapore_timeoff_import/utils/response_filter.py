
def get_timeofftypelist(response):
    data = response.json()['d']

    return list(map(lambda row: {
        "timeoffname": row['cells'][0].get('textValue'),
        "timeoffdescription": row['cells'][1].get('textValue'),
        "status": row['cells'][2].get('textValue'),
        "timeoffuri": row['cells'][0].get('uri'),
    }, data['rows'])) if data['rows'] else []
