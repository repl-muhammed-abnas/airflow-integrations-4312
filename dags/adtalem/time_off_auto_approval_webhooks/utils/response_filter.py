def get_time_off_data(response):
    response = response.json()['d']
    if not response:
        return None

    return {
        'name': response['timeOffType']['name'],
        'uri': response['owner']['uri'],
        # pylint: disable=line-too-long
        "start_date": str(response['startDateDetails']['date']['month'])+'/'+str(response['startDateDetails']['date']['day'])+'/'+str(response['startDateDetails']['date']['year']),
        "end_date": str(response['endDateDetails']['date']['month'])+'/'+str(response['endDateDetails']['date']['day'])+'/'+str(response['endDateDetails']['date']['year']),
        "duration": str(response['totalDuration']['hours'])+'.'+str(response['totalDuration']['minutes'])
    }
