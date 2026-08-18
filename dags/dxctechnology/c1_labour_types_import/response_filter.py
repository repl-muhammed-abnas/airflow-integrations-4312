def map_billing_rates(response):
    data = response.json()['d']
    return list(map(lambda item: {
        "displayText": item['displayText'],
        "name": item['name'].replace("|Billable", "").replace("|Non-Billable", "").strip(),
        "uri": item['uri']
    }, data))


def map_project_response(response):
    return (response.json()['d'][0:1] or [
            {"projectDetails": None}])[0]['projectDetails']


def get_filtered_data(response, dag_run):
    data = response.json()['d']['rows']
    return list(filter(lambda x:x['parentwbsname'] == dag_run.conf['wbs'],map(lambda item: {
        "slug": item['cells'][0]['slug'],
        "textValue": item['cells'][0]['textValue'].split(' - ')[0].strip(),
        "uri": item['cells'][0]['uri'],
        "parentwbsname": item['cells'][1].get('textValue'),
    }, data))) if data else []


def map_division_name_or_code(response):
    return response.json()['d']['name'] if response.json()['d']['name'] == 'IWO' \
            else (response.json()['d']['code'] if response.json()['d']['code'] else response.json()['d']['parent']['displayText'])
