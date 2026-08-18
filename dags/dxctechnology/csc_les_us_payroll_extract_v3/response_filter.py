
def convert_location_hierarchy(resp):
    if len(resp.json()['d']['rows']) > 0:
        rows = [row["cells"] for row in resp.json()['d']['rows']]

        def map_row(cells):
            full_path_names = [elem['textValue']
                               for elem in cells[1]['cellCollection']]
            return {
                "name": cells[0]['textValue'],
                "fullpath": " | ".join(full_path_names),
                "uri": cells[0]['uri']
            }
        return [map_row(row) for row in rows]
    return None

def convert_employee_type_hierarchy(resp):
    if len(resp.json()['d']['rows']) > 0:
        rows = [row["cells"] for row in resp.json()['d']['rows']]

        def map_row(cells):
            full_path_names = [elem['textValue']
                               for elem in cells[1]['cellCollection']]
            return {
                "name": cells[0]['textValue'],
                "fullpath": " | ".join(full_path_names),
                "uri": cells[0]['uri']
            }
        return [map_row(row) for row in rows]
    return None
