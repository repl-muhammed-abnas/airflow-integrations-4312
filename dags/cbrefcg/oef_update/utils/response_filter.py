def get_all_oef_tags(response, dag_run):
    if not response:
        return []

    return list(filter(lambda item: item['name'] == dag_run.conf['username'], map(lambda item: {
        'name': item['name'],
        'uri': item['uri']
    },response['tags'])))
