
import json
from dateutil import parser
import rail
from pwcglobal.user_import import request_payload


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def map_list_data(res):
    data = res.json()['d']['rows']
    return list(
        map(lambda item:
            {
                'name': item['cells'][0]['textValue'],
                'uri': item['cells'][0]['uri'],
                'code': item['cells'][1].get('textValue'),
            }, data)
    )


def map_timesheetperiod_search_result(res):
    data = res.json()['d']
    timesheetperiodtype = request_payload.get_conf()['timesheetperiodtype']
    timesheet_period = list(
        filter(lambda x: x['displayText'] == timesheetperiodtype, data))
    if len(timesheet_period) == 0:
        rail.set_result(
            f'Timesheet period not updated since {timesheetperiodtype} is not available in Replicon', 'log')
        return None
    return timesheet_period[0]


def map_supervisor_list(res):
    data = res.json()['d']['rows']
    return list(
        filter(lambda x: x['employeeid'] == request_payload.get_conf()['supervisor'].split('||')[0],
               map(lambda item:
                   {
                       'useruri': item['cells'][0]['uri'],
                       'employeeid': item['cells'][1].get('textValue'),
                       'enabled': item['cells'][2].get('boolValue'),
                   }, data))
    )


def map_supervisor_by_legalentity(res):
    data = res.json()['d']['rows']
    return list(
        map(lambda item:
            {
                'useruri': item['cells'][0]['uri'],
                'enabled': item['cells'][2].get('boolValue'),
            }, data)
    )


def map_impersonate_and_create_interactive_session(res):
    data = res.json()['d']
    auth_token = list(
        filter(lambda x: x['name'] == 'AUTHTOKEN', data['sessionCookies']))[0]['value']
    tenant = list(
        filter(lambda x: x['name'] == 'TENANT', data['sessionCookies']))[0]['value']
    return {'cookie': f'AUTHTOKEN={auth_token};TENANT={tenant}', 'Path': '/'}


def do_format_logs():
    master_log = json.loads(rail.result('load_master_log'))
    for log in (rail.result('gather_logs') or []):
        log_records = rail.load_all_records(log)
        if log_records:
            master_log.extend(log_records)
    users = list(
        set(map(lambda x: x['properties'].get('userpartyid', ''), master_log)))
    logs = []
    # pylint: disable=cell-var-from-loop
    for userid in users:
        user_logs = list(
            filter(lambda x: x['properties'].get('userpartyid', '') == userid and x['properties'].get('message', ''), master_log))
        if len(user_logs) > 0:
            first = user_logs[0]
            logs.append({
                # 2022-04-29 08:20:49
                'Date': parser.parse(first['timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
                'LegalentityID': first['properties'].get('legalentityid'),
                'UserPartyID': userid,
                # formatting WARN EXCEPTION ERROR SUCCESS
                'Status': first['properties'].get('status', 'Error').upper(),
                'Details': ', '.join(list(map(lambda x: x['properties'].get('message'), user_logs))),
                'Ecid': first['ecid'],
            })
    return json.dumps(logs, ensure_ascii=False)
