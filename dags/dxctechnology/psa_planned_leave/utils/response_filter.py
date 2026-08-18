import rail

def get_all_psa_cost_centers(response):
    cost_center_list = []
    data = response['rows']
    if data:
        cost_center_list = list(filter(lambda x: x['parent'] == 'PSA Cost Center', map(lambda row: {
            "name": row['cells'][0]['textValue'],
            "uri": row['cells'][0]['uri'],
            "parent": row['cells'][1]['cellCollection'][0]['textValue'],
        }, data)))
    return cost_center_list


def get_australia_division_uris(response, australia_company_codes):
    company_code_list = []
    data = response['rows']
    if data:
        company_code_list = list(filter(lambda x: x['parent'] == 'GSAP' and (x['name'] in australia_company_codes), map(lambda row: {
            "name": row['cells'][0]['textValue'],
            "uri": row['cells'][0]['uri'],
            "parent": row['cells'][1]['cellCollection'][0]['textValue'],
        }, data)))
    return company_code_list


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
