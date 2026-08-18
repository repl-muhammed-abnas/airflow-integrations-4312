def get_filtered_custom_field_list(response):
    data = response.json()['d']
    return list(filter(lambda x: x['textValue'] == 'Updated', map(lambda row: {
        "orguri": row['cells'][0]['uri'],
        'textValue': row['cells'][0]['textValue'] if 'textValue' in list(row['cells'][0]) else '',
    }, data['rows'])))
