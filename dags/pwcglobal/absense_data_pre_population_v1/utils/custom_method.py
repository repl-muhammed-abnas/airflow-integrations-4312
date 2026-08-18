import json
from datetime import datetime
from dateutil import parser

import rail


def get_replicon_date(date_str):
    if not date_str:
        return None
    # date format in 2006040
    try:
        date = datetime.strptime(date_str, '%Y%m%d')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def get_epoch_time():
    return str(round((datetime.utcnow() - datetime(1970, 1, 1, 0, 0, 0)).total_seconds()))


def do_format_logs():
    log_artifacts = []
    log_records = []

    child_logs = rail.result("gather_child_logs")
    otherlogs = rail.result("create_log")

    if child_logs:
        if isinstance(child_logs, list):
            log_artifacts.extend(child_logs)
        else:
            log_artifacts.append(child_logs)

    if otherlogs:
        if isinstance(otherlogs, list):
            log_artifacts.extend(otherlogs)
        else:
            log_artifacts.append(otherlogs)

    if log_artifacts:
        for log in log_artifacts:
            each_log_records = rail.load_all_records(log)
            if each_log_records:
                log_records.extend(each_log_records)

    final_log_records = []
    # pylint: disable=cell-var-from-loop
    for log_entry in log_records:
        final_log_records.append({
            # 2022-04-29 08:20:49
            'Date': parser.parse(log_entry['timestamp']).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            'UserPartyID': log_entry['properties']['userpartyid'],
            # formatting WARN EXCEPTION ERROR SUCCESS
            'Status': log_entry['properties'].get('status', 'Error').upper(),
            'Message': log_entry['message'],
            'Details': log_entry['properties']['details'].replace(" ", ""),
            'Ecid': log_entry['ecid'],
        })

    rail.set_result(key="error_record_count",val= len(list(filter(lambda x: x['Status'] == 'ERROR', final_log_records ))))
    rail.set_result(key="success_record_count",val= len(list(filter(lambda x: x['Status'] == 'SUCCESS', final_log_records ))))
    rail.set_result(key="exception_record_count",val= len(list(filter(lambda x: x['Status'] == 'EXCEPTION', final_log_records ))))

    return json.dumps(final_log_records, ensure_ascii=False)


def build_message(item):
    message_list = []
    if item['HoursQuantity'] and float(item['HoursQuantity']) < 0:
        message_list.append("Hours value received is negative")
    if not rail.find_first_by_attr_and_get_attr(item['InternalWorkRelationship']['PartyAlternateIdentifier'],
                                                'AlternateIdentifierType', 'PwC GUID', 'AlternateIdentifierValue'):
        message_list.append("Userid is missing")
    if not item['TransactionDate']:
        message_list.append("TransactionDate is missing")
    if not item['ChargeCode']['ChargeCode']:
        message_list.append("ChargeCode is missing")
    if not item['HoursQuantity']:
        message_list.append("HoursQuantity is missing")
    return ", ".join(message_list)


def get_invalid_log_properties(item, action, status):
    return {
        'timeentryid': item['TimeEntryID'],
        'userpartyid': item['InternalWorkRelationship']['InternalPerson']['PartyId'] if item['InternalWorkRelationship']['InternalPerson']['PartyId'] else '',
        'action': action,
        'status': status,
        'details': 'Time entry date:' + item['TransactionDate'] + '|Hours:' + item['HoursQuantity']
        + '|Project-Task:' + item['ChargeCode']['ChargeCode'] +
        '-' + item['ChargeCode']['WorkItem']['WorkItemType'],
        'unitloggeddatetime': '{{ current_time() }}'
    }
