def map_billing_rates(response):
    data = response.json()['d']
    return list(map(lambda item: {
        "displayText": item['displayText'],
        "name": item['name'],
        "uri": item['uri']
    }, data))


def map_project_response(response):
    return (response.json()['d'][0:1] or [
            {"projectDetails": None}])[0]['projectDetails']

def get_assigned_labor_types(response):
    data = response['results'][0]['timeAndMaterials']['projectBillingRates']
    return list(map(lambda item: 
        item['billingRate']['displayText']
    ,data
    ))
