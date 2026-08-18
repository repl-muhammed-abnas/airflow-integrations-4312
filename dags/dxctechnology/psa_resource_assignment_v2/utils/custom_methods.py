import json
import rail

null = None


def get_conf():
    """Get current DAG run configuration"""
    return rail.get_current_context()['dag_run'].conf


def get_details_list(logs, wbs, employee_id, status):
    """Get details list for specific WBS, employee and status"""
    if status == "Success":
        return ", ".join(list(map(lambda record: record['properties']["details"].split("Child WBS", 1)[1].strip(),
            filter(lambda record: record['properties']["wbs"] == wbs and record['properties']["empid"] == employee_id and
                    record['properties']["status"] == status and len(record['properties']["details"].split("Child WBS", 1)) > 1, logs))))
    return ", ".join(list(map(lambda record: record['properties']["details"],
        filter(lambda record: record['properties']["wbs"] == wbs and record['properties']["empid"] == employee_id and
            record['properties']["status"] == status, logs))))


def get_logs_length(logs, wbs, employee_id, status):
    """Get count of logs for specific WBS, employee and status"""
    return len(list(map(lambda record: record['properties']["details"],
        filter(lambda record: record['properties']["wbs"] == wbs and
            record['properties']["empid"] == employee_id and record['properties']["status"] == status, logs))))


def delete_records(wbs, employee_id, records):
    """Delete record from dictionary if exists"""
    if (wbs, employee_id) in records:
        del records[(wbs, employee_id)]


def get_filtered_logs(logs):
    """
    CR 02053847: Filter and consolidate logs to ensure only 1 log entry per input record.

    Priority: Error > Success > Exception

    For multiple exceptions: Replace with single generic message "no child WBS present in the parent"
    For multiple successes: Combine child WBS names into single message
    """
    success_records = {}
    exception_records = {}
    error_records = {}

    for entry in logs:
        wbs = entry['properties']['wbs']
        employee_id = entry['properties']['empid']
        action = entry['properties']['action']
        status = entry['properties']['status']
        details = entry['properties']['details']
        ecid = entry['ecid']

        if status == 'Error':
            delete_records(wbs, employee_id, exception_records)
            delete_records(wbs, employee_id, success_records)
            details_list = get_details_list(logs, wbs, employee_id, "Error")
            error_records[(wbs, employee_id)] = {'wbs': wbs, 'empid': employee_id, 'action': action,
                'status': 'Error', 'details': details_list, "ecid": ecid}
        elif status == 'Success':
            if (wbs, employee_id) not in success_records and (wbs, employee_id) not in error_records:
                delete_records(wbs, employee_id, exception_records)
                if get_logs_length(logs, wbs, employee_id, "Success") > 1 and get_details_list(logs, wbs, employee_id, status):
                    details_list = get_details_list(logs, wbs, employee_id, status)
                    message = "Gsap PSA Resource Assignment Sync Successful for this User on Child WBS " + details_list
                    success_records[(wbs, employee_id)] = {'wbs': wbs, 'empid': employee_id, 'action': action,
                        'status': 'Success', 'details': message, "ecid": ecid}
                else:
                    success_records[(wbs, employee_id)] = {'wbs': wbs, 'empid': employee_id, 'action': action,
                        'status': 'Success', 'details': details, "ecid": ecid}
        elif status == 'Exception':
            if (wbs, employee_id) not in exception_records and (wbs, employee_id) not in success_records and (wbs, employee_id) not in error_records:
                if get_logs_length(logs, wbs, employee_id, "Exception") > 1:
                    message = "Gsap PSA Resource Assignment Sync Skipped for this User as no child WBS present in the parent"
                    exception_records[(wbs, employee_id)] = {'wbs': wbs, 'empid': employee_id, 'action': action,
                        'status': 'Exception', 'details': message, "ecid": ecid}
                else:
                    exception_records[(wbs, employee_id)] = {'wbs': wbs, 'empid': employee_id, 'action': action,
                        'status': 'Exception', 'details': details, "ecid": ecid}

    combined_records = list(error_records.values()) + list(success_records.values()) + list(exception_records.values())
    return combined_records