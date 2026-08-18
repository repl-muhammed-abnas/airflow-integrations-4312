import rail

def get_entry_date():
    date = rail.result('foreach_item_in_csv_do')['ENTRY_DATE'].strip().split('/')
    return {
        'year': date[2],
        'month': date[1],
        'day': date[0]
    }
