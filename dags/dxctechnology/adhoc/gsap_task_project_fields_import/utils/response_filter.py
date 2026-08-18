from dxctechnology.adhoc.gsap_task_project_fields_import.utils.request_payload import get_task_name
null = None

def is_task_name_same(x, dag_run):
    return x['cells'][0]['textValue'] == get_task_name(dag_run).strip()

def map_gsap_task_uri(response):
    data = response.json()['d']
    return list(filter(lambda x: x['displayText'] == "GSAP Task", data))


def map_get_specific_attribute_system_level(response, dag_run):
    data = response.json()['d']['rows']
    if data:
        return list(filter(
            lambda x: is_task_name_same(x, dag_run),data))
    return []
