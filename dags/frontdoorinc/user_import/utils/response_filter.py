import rail
null = None


def get_customfields(response):
    return {
        'jobprofilecode': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Job Profile Code', 'uri', ''),
        'jobprofilename': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Job Profile Name', 'uri', ''),
        'adminmodified': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Admin Modified', 'uri', '')
    }


def get_costcenter(response):
    return {
        "costcenter": list(map(lambda item: {
            "Code": item['cells'][0]['textValue'] if (item and
                                                      'textValue' in list((item['cells'][0]).keys())) else None,

            "Text value":  item['cells'][1]['textValue'] if (item and
                                                             'textValue' in list((item['cells'][1]).keys())) else None,

            "URI": item['cells'][1]['uri'] if (item and
                                               'uri' in list((item['cells'][1]).keys())) else None,
        }, response['rows']))}


def get_employee_grouplist(response):
    return {
        "emp_grouplist": list(map(lambda item: {
            "Code": item['cells'][0]['textValue']if (item and
                                                     'textValue' in list((item['cells'][0]).keys())) else None,

            "Text value":  item['cells'][1]['textValue'].lower() if (item and
                                                                     'textValue' in list((item['cells'][1]).keys())) else None,

            "URI": item['cells'][1]['uri'] if (item and
                                               'uri' in list((item['cells'][1]).keys())) else None,
        }, response['rows']))}


def get_costcenterlist(response):
    return {
        "costcenterlist": list(map(lambda item: {
            "Code": item['cells'][0]['textValue']if (item and
                                                     'textValue' in list((item['cells'][0]).keys())) else None,

            "Text value":  item['cells'][1]['textValue'].lower() if (item and
                                                                     'textValue' in list((item['cells'][1]).keys())) else None,

            "URI": item['cells'][1]['uri'] if (item and
                                               'uri' in list((item['cells'][1]).keys())) else None,
        }, response['rows']))}
