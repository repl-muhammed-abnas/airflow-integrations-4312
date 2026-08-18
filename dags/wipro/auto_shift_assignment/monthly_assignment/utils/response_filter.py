

def convert_location_hierarchy(resp):
    if len(resp['rows']) > 0:

        def map_row(cells):
            full_path_names = [elem['textValue']
                               for elem in cells[1]['cellCollection']]
            return {
                "name": cells[0]['textValue'],
                "fullpath": " | ".join(full_path_names),
                "uri": cells[0]['uri']
            }
        return [map_row(row['cells']) for row in resp['rows']]
    return None


def get_filter_default_shift(resp, dag_run):
    return list(filter(lambda x: x['shift_name'] == dag_run.conf['default_shift'], map(lambda item: {
        "shift_name": item['cells'][0]['textValue'],
        "enabled": item['cells'][1]['boolValue']
    }, resp['rows'])))
