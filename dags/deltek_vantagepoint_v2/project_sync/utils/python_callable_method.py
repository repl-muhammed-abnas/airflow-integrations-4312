import rail

from deltek_vantagepoint_v2.project_sync.utils import request_payload


def get_invalidity_reason(dag_run, config):
    if not (dag_run.conf.get('webhook') and dag_run.conf['webhook'].get('data')):
        return 'Webhook payload is invalid'

    webhook_data = dag_run.conf['webhook']['data']
    action = webhook_data.get('Action', '')
    if action == '' or config.WEBHOOK_ACTION.get(action, False) == False:
        return f'"{action}" is an invalid Action'

    required = []
    for field in config.WEBHOOK_DATA[action]:
        if webhook_data.get(field, False) == False:
            required.append(field)

    if required:
        fields = ', '.join(required)
        return f'{fields} is/are missing from webhook payload'

    return False


def get_filtered_projects(dag_run, config):
    UPDATE = config.WEBHOOK_ACTION['UPDATE']
    projects_to_sync = config.PROJECTS_TO_SYNC
    is_full_sync = rail.result('is_full_sync') == 'fetch_all_vp_projects'
    all_projects = rail.result('fetch_all_vp_projects') \
        if is_full_sync else rail.result('fetch_project_hierarchy')

    def should_sync_project(project):
        if is_full_sync:
            if project['parentId']:
                return False

            if config.FILTER_BY_STATUS and config.FILTER_BY_READY_FOR_PROCESSING:
                return project['Status'] in projects_to_sync and project['ReadyForProcessing'] == 'Y'

            if config.FILTER_BY_STATUS:
                return project['Status'] in projects_to_sync

            if config.FILTER_BY_READY_FOR_PROCESSING:
                return project['ReadyForProcessing'] == 'Y'

            return True

        if project['Action'] == config.WEBHOOK_ACTION['DELETE']:
            return True

        is_subtask = project['WBS3'] != ' '
        is_task = project['WBS2'] != ' ' and project['WBS3'] == ' '

        if is_task or is_subtask:
            return True

        if config.FILTER_BY_STATUS and config.FILTER_BY_READY_FOR_PROCESSING:

            is_valid_status = project['Status'] in projects_to_sync
            if not is_valid_status and project['Action'] == UPDATE and project['OldStatus'] in projects_to_sync:
                is_valid_status = True

            is_valid_ready_for_processing = project['ReadyForProcessing'] == 'Y'
            if not is_valid_ready_for_processing and project['Action'] == UPDATE and project['OldReadyForProcessing'] == 'Y':
                is_valid_ready_for_processing = True

            return is_valid_status and is_valid_ready_for_processing


        elif config.FILTER_BY_STATUS:

            is_valid_status = project['Status'] in projects_to_sync
            if not is_valid_status and project['Action'] == UPDATE and project['OldStatus'] in projects_to_sync:
                return True

            return is_valid_status


        elif config.FILTER_BY_READY_FOR_PROCESSING:

            is_valid_ready_for_processing = project['ReadyForProcessing'] == 'Y'
            if not is_valid_ready_for_processing and project['Action'] == UPDATE and project['OldReadyForProcessing'] == 'Y':
                return True

            return is_valid_ready_for_processing

        return True


    if is_full_sync:
        return list(filter(should_sync_project, all_projects))

    return list(filter(
        lambda x: x.get('parentId') == '',
        all_projects
    )) if should_sync_project(dag_run.conf['webhook']['data']) else []


def get_child_dag_confs(dag_run, config):
    is_full_sync = rail.result('is_full_sync') == 'fetch_all_vp_projects'
    all_projects = rail.result('fetch_all_vp_projects') \
        if is_full_sync else rail.result('fetch_project_hierarchy')

    action = dag_run.conf['webhook']['data']['Action'] \
        if dag_run.conf.get('webhook', False) \
        else config.WEBHOOK_ACTION['UPDATE']

    all_users_uri = f'urn:replicon-tenant:{rail.get_tenant_slug()}:department:{config.ALL_USERS_DEPARTMENT_ID}'

    return [
        _build_child_dag_conf(dag_run, item, config, is_full_sync, all_projects, action, all_users_uri)
        for item in (rail.result('filtered_projects') or [])
    ]


def _build_child_dag_conf(dag_run, item, config, is_full_sync, all_projects, action, all_users_uri):
    if is_full_sync or dag_run.conf['webhook']['data']['WBS2'] == ' ':
        return{
                **item,
                'Action': action,
                'ROLES': config.ROLES,
                'all_users_uri': all_users_uri,
                'CHARGE_TYPES': config.CHARGE_TYPES,
                'WEBHOOK_ACTION': config.WEBHOOK_ACTION,
                'task_hierarchy': request_payload.get_tasks(item['WBS1'], all_projects, all_users_uri),
                'company_key': dag_run.conf.get('company_key'),
                'vantagepoint_conn_id': dag_run.conf.get('vantagepoint_conn_id'),
                'replicon_conn_id': dag_run.conf.get('replicon_conn_id')
            }

    parent_phase = next(iter(filter(
        lambda x: x['WBS2'] == dag_run.conf['webhook']['data']['WBS2'] and x['WBS3'] == ' ',
        all_projects
    ))) if dag_run.conf['webhook']['data']['WBS3'] != ' ' else {}

    common_conf = {
        'Action': action,
        **dag_run.conf['webhook']['data'],
        'CHARGE_TYPES': config.CHARGE_TYPES,
        'WEBHOOK_ACTION': config.WEBHOOK_ACTION,
        'phase_ready_for_processing': parent_phase.get('ReadyForProcessing') == 'Y',
        'company_key': dag_run.conf.get('company_key'),
        'vantagepoint_conn_id': dag_run.conf.get('vantagepoint_conn_id'),
        'replicon_conn_id': dag_run.conf.get('replicon_conn_id')
    }
    if action == config.WEBHOOK_ACTION['DELETE']:
        return {
            **common_conf,
            'ChargeType': all_projects[0]['ChargeType']
        }
    return {
        **common_conf,
        'all_users_uri': all_users_uri
    }

def should_apply_user(dag_run, role):
    user_id_field = 'Supervisor' if role == 'SUPERVISOR' else 'Principal'

    updated_user = dag_run.conf[user_id_field]
    is_user_enabled = list(filter(
        lambda x: x['isEnabled'] and x['loginName'] == updated_user,
        rail.result('fetch_replicon_users')
    )) if updated_user else True
    project = rail.result('fetch_project_details')[0]['projectDetails']
    if project and project['extensionFieldValues']:
        is_user_already_present = list(filter(
            lambda x: x['definition']['displayText'] == dag_run.conf['ROLES'][role] \
                and x['textValue'] == updated_user, project['extensionFieldValues']
        ))
        if is_user_already_present:
            return not is_user_enabled

    project_oefs = rail.result('fetch_project_oefs')
    is_oef_present = any(x for x in project_oefs if x['name'] == dag_run.conf['ROLES'][role])
    return is_oef_present and is_user_enabled


def check_exceptions(dag_run):
    def check_user_in_replicon(user_id, role):
        params = {
            'code': f"{dag_run.conf['Name']} ({dag_run.conf['WBS1']})",
            'action': dag_run.conf['Action'],
            'status': 'Exception'
        }
        user = list(filter(
            lambda x: x['loginName'] == user_id,
            rail.result('fetch_replicon_users')
        ))

        if not user:
            return {
                **params,
                'reason': f'Failed to assign {role}, {user_id} is not present in Replicon' 
            }
        elif user[0]['isEnabled'] == False:
            return {
                **params,
                'reason': f'Failed to assign {role}, {user_id} is disabled in Replicon' 
            }
        return False

    manager = dag_run.conf['ProjMgr']
    manager_exception = check_user_in_replicon(manager, 'Manager') if manager else None

    principal = dag_run.conf['Principal']
    principal_exception = check_user_in_replicon(principal, 'Principal') if principal else None

    supervisor = dag_run.conf['Supervisor']
    supervisor_exception = check_user_in_replicon(supervisor, 'Supervisor') if supervisor else None

    return {
        'MANAGER': manager_exception,
        'PRINCIPAL': principal_exception,
        'SUPERVISOR': supervisor_exception
    }