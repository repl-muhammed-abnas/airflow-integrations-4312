from dxctechnology.gsap_billing_key_master.utils import custom_methods
null = None


def map_parent_wbs_oef_uri(response):
    data = response.json()['d']
    return list(filter(lambda x: x['name'] == "Parent WBS", data))


def map_parent_column_uri(response):
    data = response.json()['d']
    basic_uris = list(filter(lambda x: x['displayText'] == "Basic", data))
    return list(filter(lambda x: x['displayText'] == "Parent WBS", basic_uris[0]['columns']))


def map_child_wbs(response):
    data = response.json()['d']['rows']
    return list(map(lambda item: item['cells'][0]['textValue'], list(filter(lambda x: x['cells'][1]['textValue']
                                                                            == custom_methods.get_conf()['projectname'], data))))

def map_child_wbs_new(response, dag_run):
    return list(map(lambda item: {"wbs":item['cells'][0]['textValue'],
                                  "wbs_uri": item['cells'][0]['uri']}, list(filter(lambda x: x['cells'][1]['textValue']
                                                                            == dag_run.conf['wbs'], response['rows']))))
