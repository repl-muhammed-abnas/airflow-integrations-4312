def get_converted_date(_date):
    return str(_date['year']) +'/'+ str(_date['month']) +'/'+ str(_date['day'])

def get_converted_time(_time):
    return str(_time['hour']) +':'+ str("{:02d}".format(_time['minute'])) +':'+ str("{:02d}".format(_time['second'])) if _time else ''

def get_non_deleted_timeoff_details(response):
    if not response:
        return None
    return {
        "comments": response['comments'],
        "start_date" : get_converted_date(response['startDateDetails']['date']),
        "end_date" : get_converted_date(response['endDateDetails']['date']),
        "user_uri": response['owner']['uri'],
        "absence_type_text": response['timeOffType']['displayText'],
        "status": response['timeOffStatus']['displayText'],
        "start_time": get_converted_time(response['startDateDetails']['timeOfDay']),
        "end_time": get_converted_time(response['endDateDetails']['timeOfDay']),
        "total_hours": str(response['totalDuration']['decimalWorkdays'])
    }

def get_deleted_timeoff_details(dag_run):
    return{
        "start_date" : get_converted_date(dag_run.conf['data']['startDate']['date']),
        "end_date" : get_converted_date(dag_run.conf['data']['endDate']['date']),
        "user_uri": dag_run.conf['data']['owner']['uri'],
        "absence_type_text": dag_run.conf['data']['timeOffType']['displayText'],
        "status": 'Deleted',
        "start_time": get_converted_time(dag_run.conf['data']['startDate']['timeOfDay']),
        "end_time": get_converted_time(dag_run.conf['data']['endDate']['timeOfDay']),
        "total_hours": str(dag_run.conf['data']['totalDuration']['decimalWorkdays'])
    }

def get_user_details(response):
    if not response:
        return None
    return {
        "employee_id": response['employeeId'],
        "name": response['firstName'] +" "+ response['lastName'],
        "country_uri": response['serviceCenterSchedule'][-1]['serviceCenter']['uri'] if response[
            'serviceCenterSchedule'] else None,
        "manager_uri": response['supervisorSchedule'][-1]['supervisor']['uri'] if response[
            'supervisorSchedule'] else None,
    }

def get_approval_history_details(response):
    if not response:
        return []
    updated_date= response[0]['timeOffHistoryDetails']['entries'][-1]['timeStamp']['displayText'] if response[0][
        'timeOffHistoryDetails']['entries'] else None
    return {
            "date": updated_date.split(" ")[0] if updated_date else None,
            "time": updated_date.split(" ")[1] if updated_date else None
        }
