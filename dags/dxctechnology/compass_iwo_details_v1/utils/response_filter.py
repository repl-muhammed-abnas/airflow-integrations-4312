import rail
from dxctechnology.compass_iwo_details_v1.utils.custom_methods import get_value, get_full_path

null = None
null_urn = "urn:replicon:list-type:null"


def get_oef_details(response):
    data = response.json()['d']
    return {
        'parentcompanycode': rail.find_first_by_attr_and_get_attr(data, 'name', 'Parent Company Code', 'uri'),
        'parentwbs': rail.find_first_by_attr_and_get_attr(data, 'name', 'Parent WBS', 'uri'),
        'parentproject': rail.find_first_by_attr_and_get_attr(data, 'name', 'Parent Project', 'uri'),
        'parentserviceorder': rail.find_first_by_attr_and_get_attr(data, 'name', 'Parent Service Order', 'uri'),
        'projecttypeuri': rail.find_first_by_attr_and_get_attr(data, 'name', 'Project Type', 'uri'),
        'iwowbselement': rail.find_first_by_attr_and_get_attr(data, 'name', 'IWO WBS Element', 'uri'),
        'gsap_task_required': rail.find_first_by_attr_and_get_attr(data, 'name', 'GSAP Task Required', 'uri'),
        "reference_mandatory": rail.find_first_by_attr_and_get_attr(data, 'name', 'Reference Mandatory', 'uri'),
        "comments_mandatory": rail.find_first_by_attr_and_get_attr(data, 'name', 'Comments Mandatory', 'uri'),
        "psa_flag": rail.find_first_by_attr_and_get_attr(data, 'name', 'PSA Flag', 'uri')
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


def all_task_response_filter(response):

    data = response.json()['d']
    if not data:
        return []

    return list(filter(lambda x: x['enabled'] == "True", map(lambda item: {
        "taskname": get_value(item, 0, 'textValue'),
        "uri": get_value(item, 0, 'uri'),
        "enabled": get_value(item, 3, 'textValue'),
        "task_fullpath": get_full_path(item) if item['cells'][1]['cellCollection'] else None,
        "parent_present": "True" if item['cells'][2]['dataType'] != null_urn else "False",
        "parent_task_name": get_value(item, 2, 'textValue'),
        "parent_task_uri": get_value(item, 2, 'uri'),
        "levels": len(item['cells'][1]['cellCollection']) if item['cells'][1]['cellCollection'] else 1,
        "code": get_value(item, 4, 'textValue'),
        "start_date": get_value(item, 5, 'textValue'),
        "end_date": get_value(item, 6, 'textValue')

    }, data['rows']))) if data['rows'] else []
