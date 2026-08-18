import rail


def psa_parent_cost_center_uri():
    return rail.find_first_by_attr_and_get_attr(rail.result('get_cost_centers'), 'name', 'PSA Cost Center', 'uri')
