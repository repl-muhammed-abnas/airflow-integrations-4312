import rail

def get_all_terminated_users_list(response):
    response = response['rows']
    if not response:
        return []

    return list(map(lambda item:{
        "username": item['cells'][0].get('textValue'),
        "useruri": item['cells'][0].get('uri'),
        "employeeid": item['cells'][3].get('textValue'),
        "companycode": item['cells'][1].get('textValue'),
        "enddate": item['cells'][2].get('textValue'),
        "userid": item['cells'][0].get('uri', '').split(":")[-1],
        "companycodeinlist": "Yes" if rail.find_first_by_attr_and_get_attr(rail.result(
            'dxc_payroll_extract_mapper_aus_search_entries'), 'companycode', item['cells'][1].get('textValue')) else "No"
    },response))
