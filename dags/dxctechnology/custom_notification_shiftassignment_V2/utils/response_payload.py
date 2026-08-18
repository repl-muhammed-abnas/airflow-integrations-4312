from datetime import datetime
def convert_data_to_target_format(response, item):
    today= datetime.now().weekday()
    return {
        'username': item['User_Name'],
        'count': response['publishedAssignments']['count'],
        'startdatetocheck': item['startdatetocheckforthursday'] if today == 3 else item['startdatetocheckforfriday'],
        'enddatetocheck': item['enddatetocheckforthursday'] if today == 3 else item['endatetocheckforfriday'],
        'code': item['Company_Code__Current_']
    } if response else None
