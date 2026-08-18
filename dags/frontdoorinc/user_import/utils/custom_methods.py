import hashlib


def get_report_data(reportdata):
    for user in reportdata:
        for key, value in user.items():
            if value is None:
                user[key] = ""
    return reportdata


def get_formated_user_row(item):
    user_md5 = hashlib.md5((str(
        str(item.get('lastName')) + "_" +
        str(item.get('Company')) + "_" +
        str(item.get('hireDate')) + "_" +
        str(item.get('jobProfileCode')) + "_" +
        str(item.get('timeType')) + "_" +
        str(item.get('timeZone')) + "_" +
        str(item.get('employeeID')) + "_" +
        str(item.get('managerID')) + "_" +
        str(item.get('terminationDate')) + "_" +
        str(item.get('firstName')) + "_" +
        str(item.get('emailAddress')) + "_" +
        str(item.get('jobProfileName')) + "_" +
        str(item.get('costCenterID')) + "_" +
        str(item.get('stateLocation')) + "_" +
        str(item.get('costCenterName')) + "_" +
        str(item.get('hourlyRate')))).encode('utf-8')).hexdigest()
    return user_md5


def is_user_exist_with_same_login_name(search_user_result, employee_id):
    existing_user_login_name = search_user_result['rows'][0]['cells'][0]['textValue'] if search_user_result and search_user_result['rows'] and search_user_result['rows'][0] and search_user_result['rows'][0]['cells'] and search_user_result['rows'][0]['cells'][0] and search_user_result['rows'][0]['cells'][0]['textValue'] else None
    if existing_user_login_name == employee_id:
        return True
    return False


def get_exception_log(dag_run):
    exception = []

    if dag_run.conf.get("company"):
        if not dag_run.conf.get('departmenturi'):
            exception.append("Company provided is not available in Replicon")
    else:
        exception.append("Department not assigned as it is blank in feedfile")
    
    if dag_run.conf.get("costcentername"):
        if not dag_run.conf.get('costcenterid'):
            exception.append("Cost center provided is not available in Replicon")
    else:
        exception.append("Cost center not provided in feedfile")
    
    if dag_run.conf.get("statelocation"):
        if not dag_run.conf.get('locationuri'):
            exception.append("Location is not available in Replicon")
    else:
        exception.append("Location is blank in feedfile")
    
    if dag_run.conf.get("timetype"):
        if not dag_run.conf.get('employeetypeuri'):
            exception.append("Employee type is not available in Replicon")
    else:
        exception.append("Employee type is blank in feedfile")
    
    return ",".join(exception)