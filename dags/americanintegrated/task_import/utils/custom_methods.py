import rail


def check_code_and_name_in_replicon_for_wage(dag_run):
    if not rail.find_first_by_attr_and_get_attr(
        dag_run.conf["get_all_tasks"],
        "taskcode",
        dag_run.conf["taskcode"],
        "taskuri"
    ) and\
        not rail.find_first_by_attr_and_get_attr(
        dag_run.conf["get_all_tasks"],
        "displayText",
        dag_run.conf["taskname"],
        'taskuri'
    ):
        return True
    return False


def check_code_and_name_in_replicon(dag_run):
    if not rail.find_first_by_attr_and_get_attr(
        dag_run.conf["get_all_tasks"],
        "taskcode",
        dag_run.conf["taskcode"],
        "taskuri"
    ) and\
        not rail.find_first_by_attr_and_get_attr(
        dag_run.conf["get_all_tasks"],
        "displayText",
        dag_run.conf["taskname"],
        'taskuri'
    ):
        return True
    return False


def check_for_code(dag_run):
    if rail.find_first_by_attr_and_get_attr(
        dag_run.conf["get_all_tasks"],
        "taskcode",
        dag_run.conf["taskcode"],
        "taskuri"
    ) and\
        not rail.find_first_by_attr_and_get_attr(
        dag_run.conf["get_all_tasks"],
        "displayText",
        dag_run.conf["taskname"],
        'taskuri'
    ):
        return True
    return False
