import rail

null = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_user_details():
    dag_run_conf = get_dag_run_conf()
    check_user = 'get_user_on_empid_both' if dag_run_conf[
        'Coprojectmanager'] != null else 'get_user_on_empid_single'
    return {
        'useruri': rail.find_first_by_attr_and_get_attr(rail.result(check_user), 'employeeid', dag_run_conf['Projectmanager'], 'uri', null),
        'name': rail.find_first_by_attr_and_get_attr(rail.result(check_user), 'employeeid', dag_run_conf['Projectmanager'], 'name', null),
        'status': rail.find_first_by_attr_and_get_attr(rail.result(check_user), 'employeeid', dag_run_conf['Projectmanager'], 'status', null),
        'employeegroup': rail.find_first_by_attr_and_get_attr(rail.result(check_user), 'employeeid', dag_run_conf['Projectmanager'], 'fullpath', null),
        'comanageruri': rail.find_first_by_attr_and_get_attr(rail.result(check_user), 'employeeid', dag_run_conf['Coprojectmanager'], 'uri', null),
        'comanagername': rail.find_first_by_attr_and_get_attr(rail.result(check_user), 'employeeid', dag_run_conf['Coprojectmanager'], 'name', null),
        'comanagerstatus': rail.find_first_by_attr_and_get_attr(rail.result(check_user), 'employeeid', dag_run_conf['Coprojectmanager'], 'status', null),
        'comanageremployeegroup': rail.find_first_by_attr_and_get_attr(rail.result(check_user), 'employeeid',
                                                                       dag_run_conf['Coprojectmanager'], 'fullpath', null),
        'enddate': rail.find_first_by_attr_and_get_attr(rail.result(check_user), 'employeeid', dag_run_conf['Projectmanager'], 'enddate', null),
        'comanagerenddate': rail.find_first_by_attr_and_get_attr(rail.result(check_user), 'employeeid', dag_run_conf['Coprojectmanager'], 'enddate', null),
    }


def update_scenario_check():
    client_name = get_dag_run_conf()['Clientname']
    if rail.result('load_project'):
        if not rail.result('load_project')["clients"]:
            return True
        data = rail.result('load_project')["clients"]
        is_client_present = list(
            filter(lambda x: x['client']['name'] == client_name, data))
        if is_client_present:
            return False
        return True
    return True
