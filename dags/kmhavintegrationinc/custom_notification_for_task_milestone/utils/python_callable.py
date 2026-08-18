import rail

def get_adminmail_ids():
    adminmail_report_data = rail.load_all_records(rail.result('load_adminmail_report_data'))
    adminmails_ids = list(map(lambda x: x['User Email'],adminmail_report_data))
    return rail.smartjoin_by_delim(adminmails_ids,',')

def get_milestone():
    milestone_data = rail.result('foreach_item_in_task_milestone_do')['Milestone']
    milesstonevalue = 0

    try:
        milesstonevalue = float((rail.smartjoin_by_delim(milestone_data.split(','))).split(" ", maxsplit=1)[0])
        return milesstonevalue

    except:  # pylint: disable=bare-except
        return milesstonevalue

def test_milestone_range(search_task_milestone_entry, search_task_milestone_entry_length, current_milestone_value):

    if not search_task_milestone_entry_length > 0:
        return False

    current_range_in_log_table = rail.load_all_records(search_task_milestone_entry)[0]['properties']['Milestone Range']

    length_of_milestone_key_value = len(current_range_in_log_table.split('-'))

    if length_of_milestone_key_value == 2:
        lower_limit_number = current_range_in_log_table.split('-')[0]
        upper_limit_number = current_range_in_log_table.split('-')[1]
        return bool(float(lower_limit_number) < current_milestone_value < float(upper_limit_number))

    limit_number = current_range_in_log_table.split('-')[0]
    return bool(current_milestone_value >= float(limit_number))
