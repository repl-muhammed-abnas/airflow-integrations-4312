import rail

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
    if dag_run.conf["country"].lower() == "romania":
        default_shift = dag_run.conf.get("default_shift")
        res = []
        _shifts = [
            val
            for i in default_shift
            for val in default_shift[i].values()
        ]
        rows = list(map(lambda item: {
                "shift_name": item['cells'][0]['textValue'],
                "enabled": item['cells'][1]['boolValue']
            }, resp['rows']))
        for shift in _shifts:
            for r in rows:
                if r['shift_name'] == shift:
                    res.append(r)
                    break
        return res

    return list(filter(lambda x: x['shift_name'] == dag_run.conf['default_shift'], map(lambda item: {
        "shift_name": item['cells'][0]['textValue'],
        "enabled": item['cells'][1]['boolValue']
    }, resp['rows'])))
