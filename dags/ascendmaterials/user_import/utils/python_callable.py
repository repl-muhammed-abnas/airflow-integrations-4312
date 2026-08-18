from datetime import datetime
import rail


def format_logs_callable():
    all_records = rail.load_all_records(rail.result('create_import_log'))
    props = lambda r: r.get('properties') or {}
    # Deduplicate by userloginname keeping the last (most recently written) entry.
    # The supervisor DAG appends an updated entry after the user-update entry, so the
    # last entry per user always reflects the final combined status and details.
    seen = {}
    for record in all_records:
        key = props(record).get('userloginname', '')
        if key:
            seen[key] = record
    no_key = [r for r in all_records if not props(r).get('userloginname')]
    all_records = list(seen.values()) + no_key
    rail.set_result(key="error_record_count", val=len([r for r in all_records if props(r).get('status', '').lower() == 'error']))
    rail.set_result(key="exception_record_count", val=len([r for r in all_records if props(r).get('status', '').lower() in ('exception', 'warning')]))
    rail.set_result(key="success_record_count", val=len([r for r in all_records if props(r).get('status', '').lower() == 'success']))
    rail.set_result(key="skipped_record_count", val=len([r for r in all_records if props(r).get('status', '').lower() == 'skipped']))
    return rail.write_json_artifact(all_records)

def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_supervisor_entries():
    supervisor_details = []
    supervisor_log_informations = get_data_from_document(
        rail.result('create_ascend_supervisor_assignments_log_lookuptable'))
    for supervisor_info in supervisor_log_informations:
        if supervisor_info['properties']:
            supervisor_details.append({
                "userloginname": supervisor_info['properties'].get('userloginname'),
                "useruri": supervisor_info['properties'].get('useruri'),
                "supervisorloginname": supervisor_info['properties'].get('supervisorloginname'),
                "action": supervisor_info['properties'].get('action'),
            })
    return supervisor_details


def get_value_in_datetime_formated(date_string, fmt):
    return datetime.strptime(date_string, fmt)


def get_today_date():
    now = datetime.utcnow()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_datetime_obj(date_str, fmt='%m/%d/%Y'):
    datetime_obj = datetime.strptime(date_str, fmt)
    return {
        'year': datetime_obj.year,
        'month': datetime_obj.month,
        'day': datetime_obj.day
    }

def split_todaysdate():
    return{
        "day" : datetime.utcnow().strftime("%d"),
        "month" : datetime.utcnow().strftime("%m"),
        "year" : datetime.utcnow().strftime("%Y")
    }


def get_supervisor_status(existing_status):
    if existing_status.lower() in ('error', 'exception'):
        return existing_status
    if any([
        rail.result('log_errorfor_supervisorand_userslogin_nameissame'),
        rail.result('log_errorwhensupervisorisdisabled'),
        rail.result('log_erroras_supervisorisnotavailable'),
    ]):
        return 'Exception'
    return existing_status


def get_detail_message_34(existing_details=''):
    parts = []
    if existing_details:
        parts.append(str(existing_details))
    for val in [
        rail.result('log_errorfor_supervisorand_userslogin_nameissame'),
        rail.result('log_errorwhensupervisorisdisabled'),
        rail.result('log_erroras_supervisorisnotavailable'),
    ]:
        if val:
            parts.append(str(val))
    return "; ".join(parts)


def get_detail_message_39():
    parts = []
    existing = (rail.result('log_search_entries_2') or {}).get('details', '')
    if existing:
        parts.append(str(existing))
    error_msg = rail.render_template("{{get_error_message()}}")
    if error_msg:
        parts.append(str(error_msg))
    return "; ".join(parts)

def get_detail_messgae_4(dag_run):
    if dag_run.conf['enabled']:
        if str(dag_run.conf['enabled']).lower().strip() == "yes":
            return ""
        return "Enabled (User Status) is not set to yes"
    return "Enabled (User Status) is blank"


def get_detail_messgae_10(dag_run):
    if dag_run.conf['startdate']:
        if "/" in dag_run.conf['startdate']:
            return ""
        return "Start date is not in predefined format"
    return "Start date is blank"

def get_subject_line_18():
    if rail.result('log_checkifthereareerrors_16'):
        return "completed with errors"
    if rail.result('log_checkifthereareexceptions_17'):
        return "completed with exceptions"
    return "completed successfully"
