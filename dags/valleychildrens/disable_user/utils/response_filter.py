def get_filtered_policy(response, dag_run):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['is_time_off_allowed'] is True and bool(x['day']), list(map(lambda item: {
        "Timeoffuri": item['timeOffType']['uri'],
        "Useruri": dag_run.conf['item']['UserUri'],
        "Terminationdate": dag_run.conf['item']['User_End_Date'],
        "Policyset": item['policySetSchedule'],
        "is_time_off_allowed": item['isTimeOffAllowedAgainstThisTimeOffType'],
        "day": item['policySetSchedule'][0]['effectiveDate']['day'] if bool(item['policySetSchedule']) else None
    }, response['policiesByTimeOffType']))))
