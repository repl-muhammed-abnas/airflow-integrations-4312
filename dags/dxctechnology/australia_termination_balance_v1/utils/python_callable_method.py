import rail

def get_required_data():
    required_data = list(map(lambda x: {
                "name": x['companycode'],
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_divisions'), 'displayText', x['companycode'], 'uri'),
                "script_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts'), 'displayText', x['fileformat_name'], 'uri'),
                "type": x['type'],
            }, rail.result('dxc_payroll_extract_mapper_aus_search_entries')))

    return {
        "companycodejson": list(set(map(lambda x: x['uri'], required_data))),
        "division": list(set(map(lambda x: x['name'], required_data))),
        "divisionuri": list(set(map(lambda x: x['uri'], required_data))),
        "companycode": list(set(map(lambda x: x['name'], required_data))),
        "script_uri": required_data[0]['script_uri']
    }
