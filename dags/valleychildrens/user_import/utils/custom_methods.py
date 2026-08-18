import pendulum

def logging_details(time_zone):
    now = pendulum.now(time_zone)
    return {
        "log_filename": f"user_import_log_{now.strftime('%Y%m%d_%H%M%S')}.csv",
        "run_date": now.strftime("%Y-%m-%d"),
        "run_time": now.strftime("%H:%M:%S"),
    }

def do_format_logs():
    """The log artifact passed via dag_run.conf['log_id'] / ['supervisor_log_id']
    contains the full log written by master + every child during this run.
    Read its entries via rail.read_log_entries() and merge with supervisor log."""
    import rail as _rail
    from airflow.operators.python import get_current_context
    ctx = get_current_context()
    user_log_id = ctx['dag_run'].conf.get('log_id')
    sup_log_id = ctx['dag_run'].conf.get('supervisor_log_id')
    user_logs = _rail.read_log_entries(user_log_id) if user_log_id else []
    sup_logs = _rail.read_log_entries(sup_log_id) if sup_log_id else []
    all_entries = []
    for entry in (user_logs + sup_logs):
        props = entry.get('properties', {}) if isinstance(entry, dict) else {}
        all_entries.append({
            'employee_id': props.get('employee_id', ''),
            'first_name': props.get('first_name', ''),
            'last_name': props.get('last_name', ''),
            'action': props.get('action', ''),
            'status': props.get('status', ''),
            'details': props.get('details', ''),
            'jobid': props.get('jobid', ''),
        })
    success_count = sum(1 for e in all_entries if e['status'] == 'Success')
    error_count = sum(1 for e in all_entries if e['status'] == 'Error')
    exception_count = sum(1 for e in all_entries if e['status'] == 'Exception')
    return {
        'entries': all_entries,
        'total_record_count': len(all_entries),
        'success_record_count': success_count,
        'error_record_count': error_count,
        'exception_record_count': exception_count,
    }

