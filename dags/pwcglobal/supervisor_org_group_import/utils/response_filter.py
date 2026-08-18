import itertools
null = None

def get_update_each_row(row, hierarchy_hash):
    cell_collection = row['cells'][0]['cellCollection']
    enabled = row['cells'][1]['boolValue']
    levels = '|'.join([item['textValue'] for item in cell_collection])
    uri = cell_collection[-1]['uri']
    if levels not in hierarchy_hash:
        hierarchy_hash[levels] = {
            'full_path': levels,
            'enabled': enabled,
            'uri':uri
        }


def get_costcenter_hierarchy_list(response):
    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))
    hierarchy_hash = {}
    if not flatten_rows:
        return []
    for row in flatten_rows:
        get_update_each_row(row, hierarchy_hash)
    return hierarchy_hash