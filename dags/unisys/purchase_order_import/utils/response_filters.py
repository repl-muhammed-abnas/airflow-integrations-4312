import json

def get_all_rows(response, hierarchy_level):
    department_group = []
    for row in response:
        if row.get("hierarchyLevel") == hierarchy_level:
            purchase_order_id = row["textValue"]
            department_group.append({"purchase_order_ids": purchase_order_id})
    return json.dumps(department_group)

def get_department_uri(response, hierarchy_level):
    for row in response:
        if row["hierarchyLevel"] == hierarchy_level:
            return row.get("uri")
    return None

def extract_department_groups(response):
    extracted_data = []
    for element in response:
        rows = element.get("rows", [])
        for row in rows:
            cell = row["cells"][0]
            extracted_data.append({
                "textValue": cell["textValue"],
                "uri": cell["uri"],
                "hierarchyLevel": row["hierarchyLevel"],
            })
    return extracted_data