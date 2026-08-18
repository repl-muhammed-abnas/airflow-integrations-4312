from rail import find_first_by_attr_and_get_attr

def get_custom_field_uris(response):
    return {
        'sick_payout_eligible': find_first_by_attr_and_get_attr(
                response, 'displayText', 'Sick Payout Eligible', 'uri')
    }

def get_sick_custom_field_dropdown_uris(response):
    return {
        'yes': find_first_by_attr_and_get_attr(
                response, 'displayText', 'Yes', 'uri'),
        'no': find_first_by_attr_and_get_attr(
                response, 'displayText', 'No', 'uri')
    }

def get_banked_custom_field_dropdown_uris(response):
    return {
        'yes': find_first_by_attr_and_get_attr(
                response, 'displayText', 'Yes', 'uri'),
        'no': find_first_by_attr_and_get_attr(
                response, 'displayText', 'No', 'uri')
    }
