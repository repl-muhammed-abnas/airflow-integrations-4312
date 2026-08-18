def map_list_data_to_companycode_list(response):
    company_code_list = []
    data = response.json()['d']
    if data['rows']:
        company_code_list = list(map(lambda row: {
            "name": row['cells'][0]['textValue'],
            "uri": row['cells'][0]['uri']
        }, data['rows']))
    return company_code_list


def map_list_data_to_employeetype_list(response):
    employee_type_list = []
    data = response.json()['d']
    if data:
        employee_type_list = list(map(lambda row: {
            "name": row['displayText'],
            "uri": row['uri']
        }, data))
    return employee_type_list
