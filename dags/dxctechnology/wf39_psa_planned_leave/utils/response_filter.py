import rail

def get_filtered_psa_org_units(response,orgunit):
    data = response['rows']
    if not data:
        return []

    return list(filter(lambda x: orgunit in (x['parent'], x['name']), map(lambda row: {
            "name": row['cells'][0]['textValue'],
            "uri": row['cells'][0]['uri'],
            "parent": row['cells'][1]['cellCollection'][1]['textValue'] if len(
                row['cells'][1]['cellCollection']) > 1 else row['cells'][1]['cellCollection'][0]['textValue'],
        }, data)))


def get_division_uris(response,divisions):
    data = response['rows']
    if not data:
        return []

    return list(filter(lambda x: x['parent'] in divisions, map(lambda row: {
            "name": row['cells'][0]['textValue'],
            "uri": row['cells'][0]['uri'],
            "parent": row['cells'][1]['cellCollection'][0]['textValue'],
        }, data)))


def get_all_paycodes(response):
    return list(map(lambda item: {
        'paycode': item['payCode']['name'],
        'paycodeuri' : item['payCode']['uri'],
        'uri': item['uri']
    }, response))

def get_paycodes_codes(response):
    data = rail.result('get_all_paycodes')
    filter_data = list(map(lambda uri: uri['paycodeuri'],data))
    return list(filter(lambda x:x['paycodeuri'] in filter_data,map(lambda item: {
        'code': item['code'],
        'paycodename': item['name'],
        'paycodeuri': item['uri']
    }, response)))
