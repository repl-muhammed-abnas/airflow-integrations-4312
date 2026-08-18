
def get_locationlistinput(data):
    return {
        "locationlistinput": list(map(lambda item: {
            "Location": item['cells'][0]['textValue'],

            "locationdesc": item['cells'][1]['textValue'],

            "activity1": item['cells'][1]['textValue'].split("|")[0].split("=")[0].strip() if item['cells'][1]['textValue'] else "OT-Client",

            "activity1value": item['cells'][1]['textValue'].split(" | ")[0].split("=")[-1].strip() if item['cells'][1]['textValue'] else 1,

            "activity2": item['cells'][1]['textValue'].split("|")[-1].split("=")[0].strip() if item['cells'][1]['textValue'] else "HOT-Client",

            "activity2value": item['cells'][1]['textValue'].split(" | ")[-1].split("=")[-1].strip() if item['cells'][1]['textValue'] else 1
        }, data['rows']))}
