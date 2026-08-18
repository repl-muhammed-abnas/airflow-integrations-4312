import json

def get_user_details(response):
    if not response:
        return [{
            "uri": "",
            "employee_id": "",
            "manager": ""
        }]
    formatted_data = []
    for item in response:
        supervisor = item["userDetails"].get("supervisor")
        formatted_data.append({
            "uri": item["userDetails"]["uri"] if item["userDetails"]["uri"] else None,
            "employee_id": item["userDetails"]["employeeId"] if item["userDetails"]["employeeId"] else None,
            "manager": supervisor["displayText"] if supervisor else None
        })
    return formatted_data

def get_user_details_permission(response):
    if not response:
        return None
    formatted_data = []
    for item in response:
        formatted_data.append({
            "uri": item["userDetails"]["uri"],
            "employeeid": item["userDetails"]["employeeId"],
            "permissionSets": item["permissionSets"],
            "status":item["userDetails"]["isEnabled"],
            "name":item["userDetails"]["displayText"]
        })
    return formatted_data

def get_effectivegroup_membership_filter(response):
    if not response:
        return []
    effective_groups = {}

    if response['locations'] and response['locations'][0]['location']:
        effective_groups['location'] = {
            "name": response['locations'][0]['location']['location']['displayText'],
            "uri": response['locations'][0]['location']['location']['uri']
        }
    return effective_groups