def get_filter_task_data(response):
    if not response:
        return []

    return list(map(lambda item: {
        'Taskcode': item['cells'][1]['textValue'] if item['cells'][1]['dataType'] == 'urn:replicon:list-type:string' else None,
        'Taskname': item['cells'][0]['textValue'] if item['cells'][0]['objectType'] == 'urn:replicon:object-type:task' else None,
        'URI': item['cells'][0]['uri'] if item['cells'][0]['objectType'] == 'urn:replicon:object-type:task' else None
    },response['rows']))

def get_project_team_uris(response):
    if not response:
        return []

    uris= list(map(lambda item:{
        "departmenturi": item['resource']['department']['uri'] if item['resource']['department'] else None,
        "resourceuri": item['resource']['user']['uri'] if item['resource']['user'] else None
    },response))

    return [x['departmenturi'] or x['resourceuri']  for x in uris]
