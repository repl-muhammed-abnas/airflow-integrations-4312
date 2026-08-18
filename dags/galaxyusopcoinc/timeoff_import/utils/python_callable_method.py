import rail


def get_user_details(dag_run):
    return{
        'useruri': rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_empid"), 'employeeid', dag_run.conf['employeeid'], 'uri'),
        'name': rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_empid"), 'employeeid', dag_run.conf['employeeid'], 'name'),
        'status': rail.find_first_by_attr_and_get_attr(rail.result("get_user_on_empid"), 'employeeid', dag_run.conf['employeeid'], 'status')
    }
