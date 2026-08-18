import rail

def map_list_data_to_companycode_list(response):
    company_code_list = []
    if response['rows']:
        company_code_list = list(map(lambda row: {
            "name": row['cells'][0]['textValue'],
            "uri": row['cells'][0]['uri'],
            "parent_division": row['cells'][1]['cellCollection'][0]['textValue'],
            "uri_value": (row['cells'][0]['uri']).split(':')[-1]
        }, response['rows']))
    c1_company_code_list = list(filter(lambda x: x['parent_division']=="C1", company_code_list))
    c1_company_code_values = list(map(lambda x: x['uri_value'], c1_company_code_list))

    compass_company_code_list = list(filter(lambda x: x['parent_division']=="COMPASS", company_code_list))
    compass_company_code_values = list(map(lambda x: x['uri_value'], compass_company_code_list))

    gsap_company_code_list = list(filter(lambda x: x['parent_division']=="GSAP", company_code_list))
    gsap_company_code_values = list(map(lambda x: x['uri_value'], gsap_company_code_list))
    
    rail.set_result(key='c1_company_code_values', val=c1_company_code_values)
    rail.set_result(key='compass_company_code_values', val=compass_company_code_values)
    rail.set_result(key='gsap_company_code_values', val=gsap_company_code_values)
    
    return company_code_list
