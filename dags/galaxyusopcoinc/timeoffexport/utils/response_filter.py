def get_script_uri(response):
    for timeoff_script in response.json()['d']:
        if timeoff_script['displayText'] == 'Time Off Export':
            return timeoff_script['uri']
    raise Exception('Unable to locate script Time Off Export')


def get_filter_timeoff_uris(response):
    data = response.json()['d']
    return list(map(lambda row: row['uri'], data))
