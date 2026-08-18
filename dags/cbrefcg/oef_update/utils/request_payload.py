import rail
from cbrefcg.oef_update.utils import custom_method

def get_group_memberships(response):
    if not response:
        return []

    employee_types= list(filter(lambda item: item['name'] in 'Broker', map(lambda item:{
        'name': item['employeeType']['employeeType']['displayText'] if item['employeeType'] else None
    },response['employeeTypes'])))

    return [x['name'] for x in employee_types if x is not None]


def oefs_from_mapper(mapper):

    list_of_oefs = list(filter(lambda item: item['allowed'] == 'yes', mapper))

    return list_of_oefs

def get_child_config(item, dag_run, name):
    return {
            "oefuri": rail.find_first_by_attr_and_get_attr(rail.result(f'get_all_oef_details_{name}'),'name', item['oef_name'], 'uri'),
            "oefname": item['oef_name'],
            "loginname": dag_run.conf['webhook']['data']['user']['loginName'],
            "useruri": dag_run.conf['webhook']['data']['user']['uri'],
            "brokeroefvalue_user": custom_method.check_custom_fielddata()['custom_filed'],
            "username": dag_run.conf['webhook']['data']['user']['displayText']
        }
