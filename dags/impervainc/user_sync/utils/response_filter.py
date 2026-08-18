from rail import result

def get_filtered_user_data(response, dag_run):
    return list(filter(lambda x: x['loginname'].lower() == dag_run.conf['Manager'].lower(), map(lambda row: {
        'loginname': row['cells'][0]['textValue'],
        "uri": row['cells'][0]['uri'],
        "status": row['cells'][1]['boolValue']
    }, response['rows'])))

def get_activityuris(response, dag_run):
    resp = list(filter(lambda x: x['code'] == dag_run.conf['Country_ISO_Code'], response))
    return [rec['uri'] for rec in resp]

def add_variable_to_list():
    time_off_type_assignments = result("for_each_time_off_type_assignments")
    balance_summary = result("get_balance_summary_for_account")
    return {
        'name': time_off_type_assignments['name'],
        'balance':balance_summary['timeRemaining'],
        'uri':time_off_type_assignments['uri']
    }

def get_filtered_supervisor_data(response, dag_run):
    return list(filter(lambda x: x['loginname'].lower() == dag_run.conf['supervisorloginname'].lower(), map(lambda row: {
        'loginname': row['cells'][0]['textValue'],
        "uri": row['cells'][0]['uri'],
        "status": row['cells'][1]['boolValue']
    }, response['rows'])))
