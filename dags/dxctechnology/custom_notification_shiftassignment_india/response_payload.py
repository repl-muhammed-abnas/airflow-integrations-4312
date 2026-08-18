def convert_data_to_target_format(response, item):
    return {
        'username': item['User_Name'],
        'count': response['publishedAssignments']['count'],
        'startdatetocheck': item['startdatetocheck'],
        'enddatetocheck': item['enddatetocheck']
    } if response else None
