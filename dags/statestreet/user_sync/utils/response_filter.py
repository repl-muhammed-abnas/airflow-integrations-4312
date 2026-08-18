null = None


def get_locationlist(data):
    return {
        "locationlistinput": list(map(lambda item: {
            "name": item['cells'][0]['textValue'],

            "fullpath":  (' | '.join([item['textValue'] for item in item['cells'][1]['cellCollection']])),

            "uri": item['cells'][0]['uri'],
        }, data['rows']))}


def get_uri(response):
    return {
        "input": list(map(lambda item: {
            "uri": item['cells'][0]['uri'] if item and item['cells'] else null
        }, response['rows']))}


def get_jobfamilyvalue(response):
    # pylint: disable=too-many-statements line-too-long
    return (response['rows'][0]['cells'][0]['cellCollection'][-1]['textValue'] if (response['rows'] and
                                                                                   'cellCollection' in list((response['rows'][0]['cells'][0]).keys())) else None)


def get_costcenteruri(response):
    # pylint: disable=too-many-statements line-too-long
    return (response['rows'][0]['cells'][0]['cellCollection'][-1]['textValue'] if (response['rows'] and
                                                                                   'cellCollection' in list((response['rows'][0]['cells'][0]).keys())) else None)


def get_currentlocation_uri(response):
    # pylint: disable=too-many-statements line-too-long
    return (response['rows'][0]['cells'][0]['cellCollection'][-1]['textValue'] if (response['rows'] and
                                                                                   'cellCollection' in list((response['rows'][0]['cells'][0]).keys())) else None)


def get_bussinessarea_uri(data):
    return {
        "listinput": list(map(lambda item: {
            'name': ' | '.join([cell['textValue'] for cell in item['cells'][0]['cellCollection']]),
            'uri': item['cells'][0]['cellCollection'][-1]['uri']
        }, data['rows']))}
