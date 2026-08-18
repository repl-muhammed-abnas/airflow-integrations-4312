def program_filter(response, program_name):
    data =response.json()['d']
    result = list(map(lambda row: {
        'slug': row['cells'][0]['slug'],
        'name':row['cells'][0]['textValue'],
        'uri':row['cells'][0]['uri']
    },data['rows']))
    return [i for i in result if i['name'] == program_name]