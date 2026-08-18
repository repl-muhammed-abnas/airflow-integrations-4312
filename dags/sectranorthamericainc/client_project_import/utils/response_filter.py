

def get_clientlist(response):
    return {
        "clientlist": list(map(lambda item: {
            "Code": item['cells'][1]['textValue'] if (item and
                                                      'textValue' in list((item['cells'][1]).keys())) else None,

            "Name":  item['cells'][0]['textValue'] if (item and
                                                       'textValue' in list((item['cells'][0]).keys())) else None,

            "URI": item['cells'][0]['uri'] if (item and
                                               'uri' in list((item['cells'][0]).keys())) else None,
        }, response['rows']))}


def get_projectlist(response):
    return {
        "projectlist": list(map(lambda item: {
            "Code": item['cells'][0]['textValue'] if (item and
                                                      'textValue' in list((item['cells'][0]).keys())) else None,

            "Name":  item['cells'][1]['textValue'] if (item and
                                                       'textValue' in list((item['cells'][1]).keys())) else None,

            "URI": item['cells'][2]['uri'] if (item and
                                               'uri' in list((item['cells'][2]).keys())) else None,
        }, response['rows']))}
