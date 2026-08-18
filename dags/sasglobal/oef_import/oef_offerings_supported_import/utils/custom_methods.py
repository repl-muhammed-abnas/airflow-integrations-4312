import rail

null = None

def get_input_status(dag_run):
    return dag_run.conf["object_data"]["status"] if dag_run.conf["object_data"]["status"] else null

def get_value_uri_already_exists(dag_run):
    return rail.find_first_by_attr_and_get_attr(
        dag_run.conf["available_tags"], 'name', dag_run.conf["object_data"]["value"], 'uri')

def get_existed_value_status(dag_run):
    return rail.find_first_by_attr_and_get_attr(
        dag_run.conf["available_tags"], 'name', dag_run.conf["object_data"]["value"], 'status')

def check_statuses(dag_run):
    return bool(dag_run.conf["object_data"]["status"] == get_existed_value_status(dag_run))
