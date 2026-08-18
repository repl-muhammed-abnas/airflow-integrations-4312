from datetime import datetime
import itertools
from rail import result, find_first_by_attr_and_get_attr, smartjoin_by_delim


null = None


def page_handler(request, result_resp):
    if len(result_resp['rows']) > 0:
        request['page'] += 1
        return request
    return null


def get_user_response(response):

    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))

    return list(map(lambda item: {
        'user': item['cells'][0]['textValue'],
        'loginname': item['cells'][1].get('textValue', ''),
        'uri': item['cells'][0]['uri']
    }, flatten_rows)) if flatten_rows else []


def get_department_response(response):

    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))

    return list(map(lambda item: {
        'code': item['cells'][1].get('textValue', ''),
        'name': item['cells'][0]['textValue'],
        'uri': item['cells'][0]['uri']
    }, flatten_rows)) if flatten_rows else []


def is_assign_supervisorpermission(response):

    supervisor_permission = False
    if response:
        if not find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet'):
            supervisor_permission = True
    return supervisor_permission


def get_timeoff_uris_after_enddate(response, dag_run):
    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))

    timeoffuri_list = []

    for item in flatten_rows:
        booking_start_date = find_first_by_attr_and_get_attr(item['cells'], 'dataType',
                                                             'urn:replicon:list-type:date', 'textValue')
        if booking_start_date and (datetime.strptime(
            booking_start_date, '%m/%d/%Y').date() > datetime.strptime(
                dag_run.conf['enddate'], '%d/%m/%Y').date()):
            timeoffuri_list.append(item['cells'][0]['uri'])

    return timeoffuri_list


def get_timeofftype_list(response):

    return list(filter(lambda y: y['name'] != 'holiday' and y['status'], map(lambda x: {
        'uri': x['timeOffType']['uri'],
        'status': x['isTimeOffAllowedAgainstThisTimeOffType'],
        'name': x['timeOffType']['name'].lower()
    },  response['policiesByTimeOffType']))) if response['policiesByTimeOffType'] else []


def get_timeoff_uris(response):
    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))

    return [item['cells'][0]['uri'] for item in flatten_rows] if flatten_rows else []


def get_supervisor_uri_status(response, dag_run):
    user_uri = ''
    user_status = ''
    if response['rows']:
        user_uri = smartjoin_by_delim(
            [x['cells'][0]['uri'] for x in response['rows'] if x['cells'][0]['textValue'] == dag_run.conf['supervisoremployeeid']])
        user_status = smartjoin_by_delim(
            [x['cells'][1]['textValue'].lower() for x in response['rows'] if x['cells'][0]['textValue'] == dag_run.conf['supervisoremployeeid']])
    return {
        'uri': user_uri,
        'status': user_status
    }


def get_required_location(response, dag_run):
    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))

    location_data = list(map(lambda item: {
        'uri': item['cells'][0]['uri'],
        'name': item['cells'][0]['textValue'],
        'code': item['cells'][1].get('textValue', '')
    }, flatten_rows)) if flatten_rows else []

    required_locationname = find_first_by_attr_and_get_attr(location_data, 'name', dag_run.conf[
        'employee_location_state'], 'name', '')

    required_locationuri = find_first_by_attr_and_get_attr(location_data, 'name', dag_run.conf[
        'employee_location_state'], 'uri', '')

    return {
        'required_locationname': required_locationname,
        'required_locationuri': required_locationuri
    }


def get_required_division(response, dag_run):
    flatten_rows = list(itertools.chain(
        *list(map(lambda x: x['rows'], response))))

    division_data = list(map(lambda item: {
        'uri': item['cells'][0]['uri'],
        'name': item['cells'][0]['textValue'],
        'code': item['cells'][1].get('textValue', '')
    }, flatten_rows)) if flatten_rows else []

    required_divisionname = find_first_by_attr_and_get_attr(division_data, 'code', dag_run.conf[
        'principalstatus'], 'name', '')

    required_divisionuri = find_first_by_attr_and_get_attr(division_data, 'code', dag_run.conf[
        'principalstatus'], 'uri', '')

    return {
        'required_divisionname': required_divisionname,
        'required_divisionuri': required_divisionuri
    }


def get_timeoff_list_from_mapper(response, dag_run):

    timeoff_list = []
    mapper_timeoffs = result('timeoff_mapper.filtered_mapper_records')
    if mapper_timeoffs:
        timeoff_list.extend(list(map(lambda x: {
            'uri': find_first_by_attr_and_get_attr(response, 'displayText', x['timeofftype'], 'uri', ''),
            'name': x['timeofftype']
        }, mapper_timeoffs)))
    if dag_run.conf['hourly_salaried_code'] == 'Salaried':
        principalstatus = int(dag_run.conf['principalstatus'])
        if principalstatus in (6, 8):
            timeoff_list.append({
                'uri': find_first_by_attr_and_get_attr(response, 'displayText', 'Flexible Time Off', 'uri', ''),
                'name': 'Flexible Time Off'
            })
    if dag_run.conf['floating_holiday'] == 'Y':
        timeoff_list.append({
            'uri': find_first_by_attr_and_get_attr(response, 'displayText', 'Floating Holiday', 'uri', ''),
            'name': 'Floating Holiday'
        })
    return [x for x in timeoff_list if x['uri']] if timeoff_list else ''
