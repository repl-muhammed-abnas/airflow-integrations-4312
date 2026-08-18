import itertools
import rail

def add_items_to_list(data, group_name):
    if not data['rows']:
        return[]
    return list(map(lambda item: {
        "name":item['cells'][0]['textValue'],
        "uri": item['cells'][0]['uri'],
        "group": group_name
    },data['rows']))

def get_user_uris():
    user_data = rail.result("get_resource_list_data")

    return list(map(lambda x: x['uri'],(itertools.chain(*user_data['value']))))

def has_group_data(group_data):
    if not group_data['rows']:
        return False

    return bool(group_data['rows'][0]['cells'][0]['uri'])
