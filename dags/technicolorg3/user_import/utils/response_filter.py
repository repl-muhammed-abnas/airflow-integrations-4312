
from datetime import datetime, timezone
import itertools
import rail


null = None


def page_handler(request, result):
    if len(result['rows']) > 0:
        request['page'] += 1
        return request
    return null


def map_replicon_groups(response, group, seperator='|'):

    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))

    return list(map(lambda item: {
        f'{group}name': item['cells'][0]['textValue'],
        f'{group}uri': item['cells'][0]['uri'],
        'fullpath': rail.smartjoin_by_delim([x['textValue'] for x in item['cells'][1]['cellCollection']], seperator),
        'length': len([x['textValue'] for x in item['cells'][1]['cellCollection']])
    }, flatten_rows)) if flatten_rows else []


def get_required_usercustom_udfs(response):
    return {
        'fte_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'FTE', 'uri'),
        'standardweeklyhours_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Standard Weekly Hours', 'uri'),
        'departmentudf_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Department', 'uri'),
        'referencejobtitle_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Reference Job Title', 'uri'),
        'referencejobcode_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Reference Job code', 'uri'),
        'time': datetime.now(timezone.utc).strftime('%H%M%S'),
        'jobcategoryudf_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Job Category', 'uri')
    }


def map_supervisor_listdata(response):
    return list(map(lambda item: {
        'name': item['cells'][0]['textValue'],
        'loginname': item['cells'][1]['textValue'],
        'uri': item['cells'][0]['uri'],
        'employeeid': item['cells'][2].get('textValue'),
        'status': item['cells'][3]['textValue']
    }, response['rows'])) if response['rows'] else []


def is_assign_supervisorpermission(response):

    supervisor_permission = False
    if response:
        if not rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet'):
            supervisor_permission = True
    return supervisor_permission
