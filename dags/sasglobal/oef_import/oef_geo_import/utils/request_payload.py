import rail

def get_update_oef_name_payload(dag_run):
    return {
        "objectExtensionTagUri": rail.result('create_oef_draft'),
        "name": dag_run.conf["object_data"]["value"]
    }

def get_update_oef_code_payload(dag_run):
    return {
        "objectExtensionTagUri": rail.result('create_oef_draft'),
        "code": dag_run.conf["object_data"]["value"]
    }
