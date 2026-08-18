import itertools


def get_value(data, index, pluck_key):
    """Extract value from Replicon API response cell structure."""
    return data['cells'][index].get(pluck_key, '')


def filter_client_data(result):
    """Transform Replicon client list response into simplified dictionary format."""
    rows_list = list(itertools.chain(
        *list(map(lambda x: x['rows'], result))))

    if not rows_list:
        return []

    return list(map(lambda item: {
            'client_name': get_value(item, 0, "textValue"),
            'client_code': get_value(item, 1, "textValue"),
            'client_uri': get_value(item, 0, "uri"),
            'enabled': get_value(item, 2, "textValue")
        }, rows_list)) if rows_list else []


def filter_project_data(result):
    """Transform Replicon project list response into simplified dictionary format."""
    rows_list = list(itertools.chain(
        *list(map(lambda x: x['rows'], result))))

    if not rows_list:
        return []

    return list(map(lambda item: {
        'project_name': get_value(item, 0, "textValue"),
        'project_code': get_value(item, 1, "textValue"),
        'project_uri': get_value(item, 0, "uri"),
        'status': get_value(item, 2, "textValue"), }, rows_list)) if rows_list else []


def extract_cost_center_data(result):
    """Extract cost center data from Replicon Cost Center List Service response."""

    rows = result.get("rows", [])
    if not rows:
        return []

    cost_center = []

    for row in rows:
        cost_center.append({
            "cost_center": get_value(row, 0, "textValue"),
            "cost_center_uri": get_value(row, 0, "uri"),
            "cost_center_enabled": get_value(row, 1, "boolValue"),
        })

    return cost_center

