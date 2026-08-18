import rail

def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)

def get_reference_shift_status(reference_shift, src_shift_list):
    for each_shift in src_shift_list:
        if each_shift['reference'] == reference_shift['reference']:
            return each_shift['status'].lower()
    return None

def add_dropdown_not_present_in_shift_to_dropdown_list():
    drop_down_list = []
    dropdown_not_present_as_shift = get_data_from_document(rail.result(
        "get_all_dropdown_not_present_as_shift")) if rail.result("get_all_dropdown_not_present_as_shift") else None
    if dropdown_not_present_as_shift:
        for each_shift in dropdown_not_present_as_shift:
            shift = [{
                'target':
                {
                    'uri': each_shift['uri'],
                },
                'name': each_shift['name'],
                'isEnabled': "false"
            }]
            drop_down_list.append(shift)
    return drop_down_list

def add_dropdown_present_as_shift_to_dropdown_list():
    drop_down_list = []
    dropdown_present_as_shift = get_data_from_document(rail.result(
        "get_all_dropdown_present_as_shift")) if rail.result("get_all_dropdown_present_as_shift") else None
    shift_data_list = get_data_from_document(rail.result(
        "query_all_shift_data_in_list")) if rail.result("query_all_shift_data_in_list") else None
    if dropdown_present_as_shift:
        for each_shift in dropdown_present_as_shift:
            shift = [{
                'target':
                {
                    'uri': each_shift['uri'],
                },
                'name': each_shift['name'],
                'isEnabled': get_reference_shift_status(each_shift, shift_data_list)
            }]
            drop_down_list.append(shift)
    return drop_down_list

def add_shift_not_in_dropdown_option_to_dropdown_list():
    drop_down_list = []
    shift_not_in_dropdown = get_data_from_document(rail.result(
        "get_shifts_not_in_dropdown_list")) if rail.result("get_shifts_not_in_dropdown_list") else None
    if shift_not_in_dropdown:
        for each_shift in shift_not_in_dropdown:
            shift = [{
                'target':{},
                'name': each_shift['name'] +"|" + each_shift['reference'],
                'isEnabled': each_shift['status'].lower()
            }]
            drop_down_list.append(shift)
    return drop_down_list

def check_new_dropdown_option_added_in_udf(drop_down_list_not_in_shift):
    """
        Check new dropdown added in udf through UI.
        In this case GetAllCustomFieldDropDownOptions service will sort the options in alphabetical order.
    """
    all_drop_down_options = get_data_from_document(rail.result(
        "get_all_dropdown_options"))
    list_of_dropdown_option_not_in_shift = [options for drop_down_options in drop_down_list_not_in_shift for options in drop_down_options]
    list_of_dropdown_uris_not_in_shift = [dropdown['target']['uri'] for dropdown in list_of_dropdown_option_not_in_shift]
    list_of_dropdown_uris_in_shift = [shift_dropdown['uri'] for shift_dropdown in all_drop_down_options]
    is_new_dropdown_option_added =  all(item in list_of_dropdown_uris_in_shift for item in list_of_dropdown_uris_not_in_shift)
    if list_of_dropdown_uris_not_in_shift and is_new_dropdown_option_added:
        return True
    return False

def get_final_dropdown_list():
    drop_down_list_not_in_shift = rail.result(
        "add_dropdown_not_present_in_shift_to_dropdown_list") or []
    drop_down_list_present_as_shift = rail.result(
        "add_dropdown_present_as_shift_to_dropdown_list") or []
    shift_list_not_in_drop_down = rail.result(
        "add_shift_not_in_dropdown_option_to_dropdown_list") or []
    has_newly_added_dropdown = check_new_dropdown_option_added_in_udf(drop_down_list_not_in_shift)
    combined_list_of_dropdown_and_existing_shift = [*drop_down_list_present_as_shift, *drop_down_list_not_in_shift]
    list_of_dropdownoption_shift = [options for drop_down_options in combined_list_of_dropdown_and_existing_shift for options in drop_down_options]
    sorted_list_of_drop_down_shift = list_of_dropdownoption_shift
    if has_newly_added_dropdown:
        sorted_list_of_drop_down_shift = sorted(list_of_dropdownoption_shift, key=lambda x: x['name'])
    new_shift_list = [options for drop_down_options in shift_list_not_in_drop_down for options in drop_down_options]
    final_drop_down_options = [*sorted_list_of_drop_down_shift, *new_shift_list]
    return final_drop_down_options

def get_auto_schedule_assignment_uri(auto_schedule_assignment_displaytext):
    all_user_custom_fields = get_data_from_document(rail.result(
        "get_all_user_custom_fields")) if rail.result("get_all_user_custom_fields") else None
    for user_custom_field in all_user_custom_fields:
        if user_custom_field['displayText'] == auto_schedule_assignment_displaytext and user_custom_field['uri']:
            return user_custom_field['uri']
    return None

def get_default_schedule_name(default_office_schedule_name):
    all_drop_down = get_data_from_document(rail.result("get_all_dropdown_options"))
    for dropdown in all_drop_down:
        if dropdown['name'] == default_office_schedule_name and dropdown['uri']:
            return dropdown['uri']
    return None
