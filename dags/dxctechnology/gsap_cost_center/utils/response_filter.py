def get_cost_centers(response):
    cost_center_list = []
    data = response['rows']
    if data:
        cost_center_list = list(map(lambda row: {
            "name": row['cells'][0]['textValue'],
            "uri": row['cells'][0]['uri'],
            "parent": row['cells'][1]['cellCollection'][0]['textValue'],
            "status": row['cells'][2]['boolValue']
        }, data))
    return cost_center_list
