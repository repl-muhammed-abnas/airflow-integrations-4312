def get_oef_uris(response):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['name'] == 'Eligibility', map(lambda item:{
        'name': item['displayText'],
        'uri': item['uri']
    }, response)))
