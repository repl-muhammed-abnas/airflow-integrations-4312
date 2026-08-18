import rail

def get_client_data(response):
    if not response:
        return []

    return rail.find_first_by_attr_and_get_attr(
        response,'displayText',rail.result("get_query_data")['clientname'], 'uri')

def get_program_data(response):
    if not response:
        return []

    data= rail.result("get_query_data")

    return [item['uri'] for item in response if item["displayText"].endswith("("+data['programcode']+")")]

def get_required_date_format(date):
    return (str(date['day'])+ '.' + str(date['month'])+ '.' +str(date['year'])) if date else None

def format_project_task_details(response):
    return list(map(lambda task: {
        "task_name": task['name'],
        "task_code": task['code'],
        "task_start_date": get_required_date_format(task['timeEntryDateRange']['startDate']),
        "task_end_date": get_required_date_format(task['timeEntryDateRange']['endDate']),
        "status": task['isClosed'],
        "uri": task['uri']
    }, response))

def get_project_type_oef_uri(response):
    return list(filter(lambda x: x['name'] == 'Project Type', map(lambda item:{
        'name': item['displayText'],
        'uri': item['uri']
    }, response)))

def get_effectivegroup_membership_filter(response):
    if not response:
        return []
    effective_groups = {}

    if response['employeeTypes']:
        effective_groups['employee_type'] = {
            "name": response['employeeTypes'][0]['employeeType']['employeeType']['displayText'],
            "uri": response['employeeTypes'][0]['employeeType']['employeeType']['uri']
        }

    return effective_groups['employee_type']['name'] == 'Indirect Office' if effective_groups else None
