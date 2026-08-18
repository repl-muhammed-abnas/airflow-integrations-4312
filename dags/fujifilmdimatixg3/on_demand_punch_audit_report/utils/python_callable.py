# pylint: disable=too-many-statements
from datetime import datetime
from rail import find_first_by_attr_and_get_attr, result


def get_report_params():
    department_uris = result('get_department_details')
    enabledFilters = result('get_report_details')[
        'filterConfiguration']['enabledFilters']
    department_filter_uri = find_first_by_attr_and_get_attr(
        enabledFilters, 'displayText', 'DepartmentFilter', 'uri')

    def get_filter_values():
        return list(map(lambda item: {
            "reportFilterUri": department_filter_uri,
            "value": item.split(":")[-1]
        }, department_uris))
    return {
        "reportParameters": [{
            "reportUri": result('get_report_details')['uri'],
            "filterValues": get_filter_values(),
            "outputFormatUri": "urn:replicon:report-output-format-option:csv"
        }
        ]
    }


def get_year_month_date(date_str):
    date = datetime.strptime(date_str, '%m/%d/%Y')
    return {
        "year": date.year,
        "month": date.month,
        "day": date.day
    }


def check_if_length_is_1_and_actual_user_uri_does_not_equal_new_user_uri():
    line_resp = result('for_each_time_punch_audit_details')['auditRecords']
    actual_user_uri = ''
    new_punch_user_uri = ''
    if len(line_resp) == 1:
        actual_user_uri = line_resp[0].get('actualUser', {}).get(
            'uri', '') if line_resp[0].get('actualUser', {}) else ''
        new_punch_user_uri = line_resp[0].get('newPunchUser', {}).get(
            'uri', '') if line_resp[0].get('newPunchUser', {}) else ''
    return (len(line_resp) == 1 and not actual_user_uri == new_punch_user_uri)


def check_if_length_is_greater():
    return len(result('for_each_time_punch_audit_details')['auditRecords']) > 1


def check_if_audit_record_edited():
    return result('for_each_audit_records').get('modificationTypeUri', '').split(':')[-1].lower() == 'edited'


def check_if_audit_record_deleted():
    return result('for_each_audit_records').get('modificationTypeUri', '').split(':')[-1].lower() == 'deleted'


def check_all_mandatory_fields(dag_run):
    return bool(dag_run.conf.get('webhook', {}).get('start_date') and dag_run.conf.get('webhook', {}).get('end_date') and
                dag_run.conf.get('webhook', {}).get('email_address') and dag_run.conf.get('webhook', {}).get('department'))


def get_start_end_date_difference(dag_run):
    return int((datetime.strptime(dag_run.conf['webhook']['end_date'], '%m/%d/%Y').timestamp() -
                datetime.strptime(dag_run.conf['webhook']['start_date'], '%m/%d/%Y').timestamp())/86400)


def get_writelog_properties_if_audit_record_edited():
    original_punch_time = result('for_each_audit_records')['originalPunchTime']
    new_punch_time = result('for_each_audit_records')['newPunchTime']
    modificationTypeUri = result('for_each_audit_records').get(
        'modificationTypeUri', '')
    newPunchActionUri = result('for_each_audit_records').get(
        'newPunchActionUri', '')
    return {
        "user_name": result('get_user_details').get('firstName', '') + " " + result('get_user_details').get('lastName', ''),
        "employee_id": result('get_user_details').get("employeeId"),
        "department_name": result('get_user_details').get("department", {}).get('displayText'),
        "original_punch": original_punch_time.get('displayText', '') if original_punch_time else "",
        "action": modificationTypeUri.split(':')[-1].title() if modificationTypeUri else "",
        "punch_type": newPunchActionUri.split(':')[-1].title() if newPunchActionUri else "",
        "modified_punch": new_punch_time.get("displayText", "") if result('for_each_audit_records').get('newPunchTime', '') else "",
        "modified_by": result('for_each_audit_records')['actualUser']['displayText'] if result('for_each_audit_records').get('actualUser', '') else "",
        "punch_date": f"{original_punch_time['month']}/{original_punch_time['day']}/{original_punch_time['year']}" if original_punch_time else "",
    }


def get_writelog_properties_if_audit_record_deleted():
    new_punch_time = result('for_each_audit_records')['newPunchTime']
    modificationTypeUri = result('for_each_audit_records').get(
        'modificationTypeUri', '')
    newPunchActionUri = result('for_each_audit_records').get(
        'newPunchActionUri', '')
    return {
        "user_name": result('get_user_details').get('firstName', '') + " " + result('get_user_details').get('lastName', ''),
        "employee_id": result('get_user_details').get("employeeId"),
        "department_name": result('get_user_details').get("department", {}).get('displayText'),
        "original_punch": new_punch_time.get('displayText', '') if new_punch_time else "",
        "action": modificationTypeUri.split(':')[-1].title() if modificationTypeUri else "",
        "punch_type": newPunchActionUri.split(':')[-1].title() if newPunchActionUri else "",
        "modified_punch": "",
        "modified_by": result('for_each_audit_records')['actualUser']['displayText'] if result('for_each_audit_records').get('actualUser', {}) else "",
        "punch_date": f"{new_punch_time.get('month', '')}/{new_punch_time.get('day', '')}/{new_punch_time.get('year', '')}" if new_punch_time else "",
    }


def get_writelog_properties_if_length_is_1_and_actual_user_uri_does_not_equal_new_user_uri():
    punchTime = result('for_each_time_punch_audit_details').get(
        'timePunch', {}).get('punchTime', {})
    return {
        "user_name": result('get_user_details').get('firstName', '') + " " + result('get_user_details').get('lastName', ''),
        "employee_id": result('get_user_details').get("employeeId"),
        "department_name": result('get_user_details').get("department", {}).get('displayText'),
        "original_punch": "",
        "action": "Created",
        "punch_type": result('for_each_time_punch_audit_details').get('timePunch', {}).get("actionUri", "").split(':')[-1].title(),
        "modified_punch": punchTime.get("displayText", ""),
        "modified_by": result('for_each_time_punch_audit_details').get('auditRecords', [{}])[0].get('actualUser', {}).get("displayText", ""),
        "punch_date": f"{punchTime.get('month', '')}/{punchTime.get('day', '')}/{punchTime.get('year', '')}",
    }
