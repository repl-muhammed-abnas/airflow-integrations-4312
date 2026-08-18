def get_policyschedule_entries(response, item):
    return {
        'timeOffTypeUri': item,
        'policySetScheduleEntries': response
    }
