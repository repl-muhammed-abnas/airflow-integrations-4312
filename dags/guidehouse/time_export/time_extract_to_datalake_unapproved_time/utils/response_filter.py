def get_location_name_and_code_pairs(response):
    """
    returns a dict with location name as key and location code as value for level 1 locations. Example:
    {
        "Belgium": "Z-BELGIUM",
        "France": "Z-FR"
    }
    """
    location_name_and_code_pairs = {}
    rows = response.get("rows", [])
    for row in rows:
        location_cell = row["cells"][0]
        code_cell = row["cells"][1]
        location_name_and_code_pairs[location_cell["textValue"]] = code_cell.get("textValue", "")
    return location_name_and_code_pairs

def extract_cost_centers(response):
    extracted_data = []
    for element in response:
        rows = element.get("rows", [])
        for row in rows:
            cost_center_cell = row["cells"][0]
            extracted_data.append({
                "cost_center_name": cost_center_cell["textValue"],
                "cost_center_uri": cost_center_cell["uri"],
                "hierarchy_level": row.get("hierarchyLevel")
            })
    return extracted_data


def get_usa_location_uri(response):
    rows = response.get("rows", [])
    for row in rows:
        location_cell = row["cells"][0]
        if location_cell["textValue"] == "United States of America":
            return location_cell["uri"]
    return ""