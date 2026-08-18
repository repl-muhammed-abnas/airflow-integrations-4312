import rail

def get_effective_grp_membership_data_handler(response):
    return_data = {}
    return_data['costCenter'] = response['costCenters'][0]['costCenter']['costCenter'] if response['costCenters'] else {}
    return_data['department'] = response['departments'][0]['department']['department'] if response['departments'] else {}
    return_data['division'] = response['divisions'][0]['division']['division'] if response['divisions'] else {}
    return_data['employeeType'] = response['employeeTypes'][0]['employeeType']['employeeType'] if response['employeeTypes'] else {}
    return_data['location'] = response['locations'][0]['location']['location'] if response['locations'] else {}
    return_data['serviceCenter'] = response['serviceCenters'][0]['serviceCenter']['serviceCenter'] if response['serviceCenters'] else {}
    return_data['parent_location'] = response['locations'][0]['location']['parent'] if response['locations'] else {}
    return_data['parent_division'] = response['divisions'][0]['division']['parent'] if response['divisions'] else {}
    rail.set_result(key="response", val=response)
    return return_data
