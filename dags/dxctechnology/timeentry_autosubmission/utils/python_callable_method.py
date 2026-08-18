import rail


def get_entries_per_user():
    data = list(filter(lambda x: x['approvalstatus'] == 'Not Submitted', list(map(lambda item: {
        'revisionuri': item['revisionuri'],
        'entrydate': item['entrydate'],
        'approvalstatus': item['Approvalstatus'],
    }, rail.result("add_multi_day_entries_data")))))

    return {
        'entries': data,
        'length': len(data)
    }


def add_data():
    data = rail.result("get_timeEntry_revision_groups")

    return list(map(lambda x: {'revisionuri': x['uri'],
                               'entrydate': str(x['entryDate']['day']) + '/' + str(x['entryDate']['month'])+'/'+str(x['entryDate']['year']),
                               'Approvalstatus': x['approvalStatus']['displayText']
                               }, data)) if data else []
