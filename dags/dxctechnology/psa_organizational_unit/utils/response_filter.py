null = None

def get_all_org_units(response):
    org_unit_list = []
    data = response['rows']
    if data:
        org_unit_list = list(map(lambda row: {
            "name": row['cells'][0]['textValue'],
            "uri": row['cells'][0]['uri'],
            "parent": row['cells'][1]['cellCollection'],
            "status": row['cells'][2]['boolValue']
        }, data))
    return org_unit_list
