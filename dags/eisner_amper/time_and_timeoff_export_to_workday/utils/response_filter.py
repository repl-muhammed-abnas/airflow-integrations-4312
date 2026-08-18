def get_object_tag_definitiondetails(response):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['uri'], list(map(lambda item: {
        "uri": item['uri'],
        "name": item['name']
    }, response['tags']))))


def get_object_tag_definitiondetails_project_type(response):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['uri'], list(map(lambda item: {
        "uri": item['uri'],
        "name": item['name']
    }, response['tags']))))


def getenabledemployeetypegroups(response):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['uri'], list(map(lambda item: {
        "uri": item['uri'],
        "displayText": item['displayText']
    }, response))))


def get_all_us_company_codes(response):
    response = response.json()['d']
    if not response:
        return []

    companycode = list(filter(lambda x: x['eligible'] == "Yes", list(map(lambda item: {
        "uri": item['cells'][0]['uri'],
        "eligible": "Yes" if str(item['cells'][1]['textValue']).startswith("US") else "No"
    }, response['rows']))))

    return list(list(map(lambda item:
                         item['uri'], companycode)))


def get_all_us_cost_codes(response):
    response = response.json()['d']
    if not response:
        return []

    costcenter = list(filter(lambda x: x['eligible'] == "No", list(map(lambda item: {
        "uri": item['cells'][0]['uri'],
        "eligible": "No" if ((str(item['cells'][1]['textValue']) == "US01102100")
                             or (str(item['cells'][1]['textValue']) == "US01102100")
                             or (str(item['cells'][1]['textValue']) == "US01102100")) else "Yes"
    }, response['rows']))))

    return list(list(map(lambda item:
                         item['uri'], costcenter)))
