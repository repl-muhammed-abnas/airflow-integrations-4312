def get_script_uri_filter(display_text):
    def _filter(response):
        for script in response.json()['d']:
            if script['displayText'] == display_text:
                return script['uri']
        raise Exception(f'Unable to locate script: {display_text}')
    return _filter


def combine_timeoff_types(results):
    """Combines all pages of timeoff type data into a name → URI map.

    Args:
        results: List of API responses from each page

    Returns:
        Dict mapping timeoff type name to the last segment of its URI
    """
    timeoff_type_map = {}
    for page_response in results:
        for row in page_response.get('rows', []):
            uri = row['cells'][0].get('uri', '')
            name = row['cells'][0].get('textValue', '')
            if name and uri:
                timeoff_type_map[name] = uri.split(':')[-1]
    return timeoff_type_map
