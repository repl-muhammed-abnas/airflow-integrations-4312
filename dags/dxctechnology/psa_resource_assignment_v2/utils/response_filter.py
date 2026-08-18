from dxctechnology.psa_resource_assignment_v2.utils import custom_methods
null = None


def map_all_resource_assignments(response, dag_run=None):
    """
    Map all project team member assignments.
    Returns full list without filtering for bulk operations.
    """
    data = response.json()['d']
    return data


def map_parent_column_uri(response):
    """
    Extract Parent WBS column URI from column definitions.
    """
    data = response.json()['d']
    basic_uris = list(filter(lambda x: x['displayText'] == "Basic", data))
    return list(filter(lambda x: x['displayText'] == "Parent WBS", basic_uris[0]['columns']))


def map_parent_wbs_oef_uri(response):
    """
    Extract Parent WBS filter definition URI.
    """
    data = response.json()['d']
    return list(filter(lambda x: x['name'] == "Parent WBS", data))


def map_child_wbs(response):
    """
    Map child WBS projects for current parent WBS.
    """
    data = response.json()['d']['rows']
    return list(map(lambda item: item['cells'][0]['textValue'],
                   list(filter(lambda x: x['cells'][1]['textValue'] == custom_methods.get_conf()['wbs'],
                              data))))