import rail

null = None


def get_conf():
    return rail.get_current_context()['dag_run'].conf
