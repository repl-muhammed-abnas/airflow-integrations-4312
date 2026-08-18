import rail


def get_entries_per_user():
    data = rail.result("add_multi_day_entries_data")

    data = list(filter(lambda x: x['approvalstatus'] == 'Not Submitted', list(map(lambda item: {
        'revisionuri': item['revisionuri'],
        'entrydate': item['entrydate'],
        'approvalstatus': item['Approvalstatus'],
    }, data))))

    return {
        'entries': data,
        'length': len(data)
    }


def add_data():
    data = rail.result("Get_TimeEntry_Revision_Groups_For_User_And_DateRange")

    return list(map(lambda x: {'revisionuri': x['uri'],
                               'entrydate': str(x['entryDate']['day']) + '/' + str(x['entryDate']['month'])+'/'+str(x['entryDate']['year']),
                               'Approvalstatus': x['approvalStatus']['displayText']
                               }, data)) if data else []
