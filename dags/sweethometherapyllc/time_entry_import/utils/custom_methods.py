from datetime import datetime
from pendulum import now
from ast import literal_eval
from typing import Dict, List, Any
import rail
from functools import lru_cache


MANDATORY_FIELDS = ['therapist', 'school', 'service_name', 'type1', 'hours', 'num_students', 'date_of_service']
STANDARD_EMAIL_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
entry_dateformat = '%m/%d/%Y'
null = None

def validate_mandatory_fields(csv_record: Dict[str, Any]) -> List[str]:
    missing_fields = []
    missing_field_names = []

    for field in MANDATORY_FIELDS:
        if not csv_record.get(field) or str(csv_record[field]).strip() == '':
            missing_field_names.append(field.replace('_', ' ').title())

    if missing_field_names:
        missing_fields.append(
            f"Blank {', '.join(missing_field_names)} field(s) found in input file"
        )

    date_of_service = csv_record.get('date_of_service')
    if date_of_service and str(date_of_service).strip():
        try:
            datetime.strptime(str(date_of_service).strip(), entry_dateformat)
        except Exception:
            missing_fields.append('Invalid/Incorrect Work Date Format Received')

    return missing_fields



def get_validation_error_message(record: Dict[str, Any]) -> str:
    original_record = {
        'Therapist': record.get('therapist', ''),
        'Date of Service': record.get('date_of_service', '')
    }
    errors = validate_mandatory_fields(record)
    if errors:
        return "; ".join(errors)
    return "No validation errors"

def get_submitted_timesheet_uris(timesheet_data: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    submitted_uris = set()
    
    for timesheet in timesheet_data:
        if timesheet.get('timesheet_status') != 'Not Submitted':
            timesheet_uri = timesheet.get('timesheet_uri')
            if timesheet_uri:
                submitted_uris.add(timesheet_uri)
    
    return list(map(lambda ts: {
        'ts_uri': ts
    }, list(submitted_uris)))

def get_successful_entry_dates(dag_run) -> List[str]:
    records = load_records(dag_run.conf.get('log')) or []
    return list({
        r['properties']['date_of_service']
        for r in records
        if r['properties'].get('therapist') == dag_run.conf['therapist']
        and get_status(r['properties'], 'Success')
    })


def get_timesheet_uris_for_dates(timesheet_data: List[Dict[str, Any]], successful_dates: List[str], submitted_uris: List[Dict[str, str]]) -> List[Dict[str, str]]:
    reopened_uris = {u['ts_uri'] for u in submitted_uris}
    uris = {
        ts['timesheet_uri']
        for ts in timesheet_data
        if ts.get('timesheet_uri') and (
            ts.get('date_of_service') in successful_dates
            or ts['timesheet_uri'] in reopened_uris
        )
    }
    return [{'ts_uri': uri} for uri in uris]

@lru_cache(maxsize=8)
def load_records(log_artifact):
    return rail.load_all_records(log_artifact)

def get_status(item: Dict[str, Any], logstatus: str) -> bool:
    return item['status'].lower() == logstatus.lower()

def get_record_count_by_status(dag_run):

    logs = dag_run.conf.get('log')

    error_count = 0
    exception_count = 0

    if logs:
        records = load_records(logs)

        if records:
            error_count = len([
                r for r in records
                if get_status(r['properties'], 'error')
            ])

            exception_count = len([
                r for r in records
                if get_status(r['properties'], 'exception')
            ])


    rail.set_result(key="error_record_count",val= error_count)
    rail.set_result(key="exception_record_count",val= exception_count)


    return {
        "error_record_count": error_count,
        "exception_record_count": exception_count
    }

def get_date_from_replicon_date(replicon_date):
    if not replicon_date:
        return datetime.max
    return datetime(day=replicon_date['day'], month=replicon_date['month'], year=replicon_date['year'])

def get_email_details(timezone, log_file_path, dag_run):
    current_time = now(timezone)
    start_time_str = dag_run.conf['start_time']
    filename = dag_run.conf['input_filename']
    return {
        "start_time": start_time_str,
        "job_end_time": current_time.isoformat(),
        "job_duration_minutes": (((current_time - datetime.strptime(start_time_str, STANDARD_EMAIL_DATE_FORMAT)).seconds)//60),
        "log_timestamp": current_time.strftime("%Y%m%dT%H%M%S"),
        "email_timestamp": current_time.strftime(STANDARD_EMAIL_DATE_FORMAT),
        "log_filepath": log_file_path,
        "log_file_name": f'Log_{filename}',
        "input_filename": filename,
    }

def format_tags(response):

    rows = response.get("rows", [])

    tag_options = [
        {
            "name": row["cells"][0]["textValue"],
            "uri": row["cells"][1]["uri"],
        }
        for row in rows
    ]

    return tag_options if tag_options else None


def get_tags_object(response, item):
    return {
        **item,
        "tags": format_tags(response)
    }


def aggregate_entries(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    aggregated = {}

    for record in records:
        key = (
            record.get('school', '').strip(),
            record.get('service_name', '').strip(),
            record.get('type1', '').strip(),
            record.get('num_students', '').strip(),
            record.get('therapist', '').strip(),
            record.get('date_of_service', '').strip()
        )

        try:
            hours = float(str(record.get('hours', '0')).strip())
        except (ValueError, TypeError):
            hours = 0.0

        entry_keyid = record.get('entry_keyid', '')

        if key in aggregated:
            aggregated[key]['hours'] = round(aggregated[key]['hours'] + hours, 2)
            if entry_keyid:
                aggregated[key]['entry_keyid'] = f"{aggregated[key]['entry_keyid']},{entry_keyid}"
        else:
            aggregated[key] = {
                'entry_keyid': entry_keyid,
                'school': key[0],
                'service_name': key[1],
                'type1': key[2],
                'num_students': key[3],
                'therapist': key[4],
                'date_of_service': key[5],
                'hours': round(hours, 2)
            }

    return list(aggregated.values())