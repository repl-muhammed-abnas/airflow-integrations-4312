import itertools
import rail

input_date_format = "%d/%m/%Y"

def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

def all_result_data_handler(result, loginname):
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], result))))
    user_list = list(filter(lambda x: x['status'] != "True", map(lambda row: {
        'username': row['cells'][0]['textValue'] if 'textValue' in row['cells'][0] else None,
        'employeeid': row['cells'][2]['textValue'] if 'textValue' in row['cells'][2] else None,
        'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
        'loginname': row['cells'][1]['textValue'],
        'useruri': row['cells'][1]['uri']
    }, flaten_rows)))

    if not user_list:
        return []

    prefix_mapper = {
          "C3": "C3",
          "C4": "Action Fund",
          "Delegate": "Delegate",
    }

    received_type = rail.result("for_each_create_entry")['profile_type']

    if received_type in prefix_mapper:
        user_list = list(filter(lambda x: prefix_mapper[received_type] in x['username'], user_list))
        if not user_list:
            return []

    return [{
        'username': user_list[0]['username'],
        'employeeid': user_list[0]['employeeid'],
        'status': user_list[0]['status'],
        'loginname': user_list[0]['loginname'],
        'useruri': user_list[0]['useruri']
    }]

def get_all_result_data_handler(result):
    flaten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], result))))
    return list(filter(lambda x: x['status'] == "True", map(lambda row: {
        'username': row['cells'][0]['textValue'] if 'textValue' in row['cells'][0] else None,
        'employeeid': row['cells'][2]['textValue'] if 'textValue' in row['cells'][2] else None,
        'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
        'loginname': row['cells'][1]['textValue'],
        'useruri': row['cells'][1]['uri']
    }, flaten_rows)))
