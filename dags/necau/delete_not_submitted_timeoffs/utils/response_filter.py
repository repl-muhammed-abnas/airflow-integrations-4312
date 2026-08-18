import rail

def timeoff_off_details(response):
    return list(map(lambda item:{
        'timeoffuri': rail.find_first_by_attr_and_get_attr(
                            item['cells'], 'objectType', 'urn:replicon:object-type:time-off', 'uri'),
        'startdate': rail.find_first_by_attr_and_get_attr(
                            item['cells'], 'dataType', 'urn:replicon:list-type:date', 'textValue'),
        'enddate': rail.find_first_by_attr_and_get_attr(
                            item['cells'], 'dataType', 'urn:replicon:list-type:date', 'textValue'),
        'useruri': rail.find_first_by_attr_and_get_attr(
                            item['cells'], 'objectType', 'urn:replicon:object-type:user', 'uri'),
        'timeofftype': rail.find_first_by_attr_and_get_attr(
                            item['cells'], 'objectType', 'urn:replicon:object-type:time-off-type', 'textValue')
    },response['rows']))
