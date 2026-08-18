import rail
from dxctechnology.compass_iwo_details.utils import custom_methods

null = None


def get_oef_details(response):
    data = response.json()['d']
    return {
        'parentcompanycode': rail.find_first_by_attr_and_get_attr(data, 'name', 'Parent Company Code', 'uri'),
        'parentwbs': rail.find_first_by_attr_and_get_attr(data, 'name', 'Parent WBS', 'uri'),
        'parentproject': rail.find_first_by_attr_and_get_attr(data, 'name', 'Parent Project', 'uri'),
        'parentserviceorder': rail.find_first_by_attr_and_get_attr(data, 'name', 'Parent Service Order', 'uri'),
        'projecttypeuri': rail.find_first_by_attr_and_get_attr(data, 'name', 'Project Type', 'uri'),
        'iwowbselement': rail.find_first_by_attr_and_get_attr(data, 'name', 'IWO WBS Element', 'uri')
    }


def map_resource_assignment_list(response):
    data = response.json()['d']
    return list(filter(lambda x: x['status'] == "Yes", list(map(lambda x, item: {
        'employeeid': x['employeeid'],
        'uri': x['useruri'],
        'status': 'Yes' if not item['resource']['uri'] else 'No' if rail.find_first_by_attr_and_get_attr(data, 'resource.uri', x['useruri'], 'uri') else 'Yes'
    }, rail.result('create_valid_task_list')['valid_tasks'], data))))


def map_resource_assignment_list_after_update(response):
    data = response.json()['d']
    return list(filter(lambda x: x['status'] == null, list(map(lambda x, item: {
        'uri': x['useruri'],
        'assignmentstartdate': item['projectAssignmentDateRange']['startDate'],
        'assignmentenddate': item['projectAssignmentDateRange']['endDate'],
        'status': null if not item['resource']['uri'] else rail.find_first_by_attr_and_get_attr(data, 'resource.uri', x['useruri'], 'uri'),
    }, rail.result('create_valid_task_list')['valid_tasks'], data))))


def map_billing_rates_name_list(response):
    data = response.json()['d']
    if data['results'][0]:
        billing_rates = data['results'][0].get(
            'timeAndMaterials', []).get('projectBillingRates', [])

    return list(map(lambda x: {
        'billingRate': {
            'name': x['billingRate']['name'],
            'uri': x['billingRate']['uri'],
        }
    }, billing_rates))


def map_tasks_list(response):
    data = response.json()['d']
    return list(map(lambda d: {
        'taskname': d['task']['name'],
        'taskcode': d['task']['code'],
        'description': d['task']['description'],
        'isclosed': d['task']['isClosed'],
        'startdate': custom_methods.get_string_date(d['task']['timeEntryDateRange']['startDate']),
        'enddate': custom_methods.get_string_date(d['task']['timeEntryDateRange']['endDate']),
        'uri': d['task']['uri'],
        'entrytype': d['task']['timeAndExpenseEntryType']['uri']
    }, data)) if response.json()['d'] else []
