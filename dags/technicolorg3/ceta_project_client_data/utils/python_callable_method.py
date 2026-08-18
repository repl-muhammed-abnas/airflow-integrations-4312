import rail
from technicolorg3.ceta_project_client_data.utils import custom_methods
from airflow.models import Variable

null = None


def get_client_project_message_to_log(dag_run):
    message_list = []
    payload = (dag_run.conf['webhook']).get('data')
    if not payload.get('mill_mpc', ''):
        message_list.append('MILL/MPC is blank')
    if not payload.get('Project_Code', ''):
        message_list.append('Project code is blank')
    if not payload.get('Project_Name', ''):
        message_list.append('Project Name is blank')
    if not payload.get('Project_Manager', ''):
        message_list.append('Project Manager is blank')
    if not payload.get('Project_Status', ''):
        message_list.append('Project Status is blank')
    if not payload.get('Project_Producer_Global_ID', ''):
        message_list.append('Project manager global id is blank')
    if not payload.get('Product_Name', ''):
        message_list.append('Product name is blank')
    if not payload.get('Client_Code', ''):
        message_list.append('Client code is blank')
    if not payload.get('Client_Name', ''):
        message_list.append('Client Name is blank')

    message = 'The Client_Project transfer from CETA to Replicon with job reference '
    message += dag_run.conf['_ecid'] + \
        ' has not been completed due to following reason. -' + \
        ', '.join(message_list)
    return message if message_list else null


def get_project_message_to_log(dag_run):
    message_list = []
    if not dag_run.conf['projectname']:
        message_list.append('Project name not present')
    if not dag_run.conf['projectcode']:
        message_list.append('Project code not present')
    if not dag_run.conf['millmpc']:
        message_list.append('Mill or Mpc identifer not present')
    if not dag_run.conf['projectstatus']:
        message_list.append('Project status not present')

    message = 'The Client_Project transfer from CETA to Replicon with job reference '
    message += dag_run.conf['_ecid'] + \
        ' has not been completed due to following missing value(s). -' + \
        ', '.join(message_list)
    return message if message_list else null


def get_exception_messages(caller):
    all_exception_messages = []
    if rail.result(f'add_projectmanagerid_blank_exception_{caller}'):
        all_exception_messages.append(rail.result(
            f'add_projectmanagerid_blank_exception_{caller}'))
    if rail.result(f'add_projectmanagerid_notfound_exception_{caller}'):
        all_exception_messages.append(rail.result(
            f'add_projectmanagerid_notfound_exception_{caller}'))
    if rail.result(f'add_projectmanagerid_disabled_exception_{caller}'):
        all_exception_messages.append(rail.result(
            f'add_projectmanagerid_disabled_exception_{caller}'))
    if rail.result(f'add_required_permission_notfound_exception_{caller}'):
        all_exception_messages.append(rail.result(
            f'add_required_permission_notfound_exception_{caller}'))
    if rail.result(f'add_client_not_found_exception_{caller}'):
        all_exception_messages.append(rail.result(
            f'add_client_not_found_exception_{caller}'))
    return ', '.join(all_exception_messages)


def get_customfields_project_id():
    return {
        "customField": {
            "uri": rail.result('get_required_customfields')['project_id_uri']
        },
        "number": str(custom_methods.get_dag_run_conf()['projectid']),
        "dropDownOption": {}
    } if rail.result('get_required_customfields')['project_id_uri'] and custom_methods.get_dag_run_conf()['projectid'] else null


def get_customfields_product_name():
    return {
        "customField": {
            "uri": rail.result('get_required_customfields')['product_name_uri']
        },
        "text": custom_methods.get_dag_run_conf()['productname'],
        "dropDownOption": {}
    } if rail.result('get_required_customfields')['product_name_uri'] and custom_methods.get_dag_run_conf()['productname'] else null


def add_customfields_project_type():
    project_type_dropdown_uri = null
    if rail.result('get_enabled_dropdown_project_type'):
        project_type_dropdown_uri = rail.result(
            'get_enabled_dropdown_project_type')['project_type_dropdown_uri']
    if rail.result('get_enabled_dropdown_options_project_buckets'):
        project_type_dropdown_uri = rail.result(
            'get_enabled_dropdown_options_project_buckets')['project_type_dropdown_uri']

    return {
        "customField": {
            "uri": rail.result('get_required_customfields')['project_type_uri']
        },
        "dropDownOption": {
            "uri": project_type_dropdown_uri
        }
    } if project_type_dropdown_uri else null


def add_customfields_project_classification():
    project_classification_dropdown_uri = null
    if rail.result('get_enabled_customfield_dropdown_project_classification'):
        project_classification_dropdown_uri = rail.result(
            'get_enabled_customfield_dropdown_project_classification')['project_classification_dropdown_uri']
    if rail.result('get_enabled_dropdown_options_project_classification'):
        project_classification_dropdown_uri = rail.result(
            'get_enabled_dropdown_options_project_classification')['project_classification_dropdown_uri']

    return {
        "customField": {
            "uri": rail.result('get_required_customfields')['project_classification_uri']
        },
        "dropDownOption": {
            "uri": project_classification_dropdown_uri
        }
    } if project_classification_dropdown_uri else null


def get_exception_message():
    all_exception_messages = []
    if rail.result('add_projectmanagerid_notfound_exception'):
        all_exception_messages.append(rail.result(
            'add_projectmanagerid_notfound_exception'))
    if rail.result('add_projectmanagerid_disabled_exception'):
        all_exception_messages.append(rail.result(
            'add_projectmanagerid_disabled_exception'))
    if rail.result('add_required_permission_notfound_exception'):
        all_exception_messages.append(rail.result(
            'add_required_permission_notfound_exception'))
    return ', '.join(all_exception_messages)


def get_default_tasks(project_tasks_config):
    project_tasks_mapper = Variable.get(project_tasks_config, default_var=[])
    return project_tasks_mapper
