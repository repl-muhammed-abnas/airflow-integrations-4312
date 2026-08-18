import rail

def check_custom_fielddata():
    user_data = rail.result("get_user_details")[0]
    broker_value = rail.load_all_records(rail.result("filter_logs_by_user"))[0]['properties']['broker_value'] if rail.result(
                            "filter_logs_by_user", "length") > 0 else None
    custom_field_data= list(filter(lambda item: item['name'] == 'Broker', map(lambda item:{
        'name': item['customField']['displayText'] if item['customField'] else None,
        'text': item['text']
    },user_data['userDetails']['customFieldValues'])))

    return {
        'value': (custom_field_data[0]['text'] != broker_value) if broker_value else False,
        'custom_filed': custom_field_data[0]['text']
    }
