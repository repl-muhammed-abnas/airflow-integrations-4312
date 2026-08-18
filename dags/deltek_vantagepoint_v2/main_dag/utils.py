import json

from rail.lib.util import DEFAULT_NOTIFICATION_EMAIL


def get_connector_clientids_with_initial_settings(connector_info, workflows: list):
    """
    Variant of rail.get_connector_clientids_by_integration that additionally
    propagates the initial_setup workflow's customSettings into every other
    workflow (and custom_integrations) under the key 'initial_custom_settings'.

    This lets downstream DAGs (user-sync, timesheet-sync, custom integrations)
    access settings that are only configured once in the initial_setup payload
    (e.g., laborCodeSetting) without an extra DB fetch or Airflow Variable.
    """
    connector_info = json.loads(connector_info)
    client_ids = {workflow: [] for workflow in workflows}
    client_ids['custom_integrations'] = []

    for each_client in connector_info:
        if not each_client.get('notification_email'):
            default_email = DEFAULT_NOTIFICATION_EMAIL.replace('@', f"+{each_client['company_key']}@")
            each_client['notification_email'] = default_email
        dag_settings = each_client.pop('dag_settings')

        initial_custom_settings = {}
        if dag_settings:
            for dag in dag_settings:
                if dag.get('workflowId') == 'initial_setup' and dag.get('customSettings'):
                    initial_custom_settings = dag['customSettings']
                    break

        if dag_settings:
            for dag in dag_settings:
                if dag.get('isCustom') and dag['isCustom'] != dag['workflowId'] and dag['enabled'].lower() == 'yes':
                    entry = {
                        **each_client,
                        'dagId': dag['isCustom'],
                        'customSettings': dag['customSettings']
                    }
                    if initial_custom_settings:
                        entry['initial_custom_settings'] = initial_custom_settings
                    client_ids['custom_integrations'].append(entry)
                elif (dag['workflowId'] in workflows) and dag['enabled'].lower() == 'yes':
                    entry = {
                        **each_client,
                        'customSettings': dag['customSettings']
                    }
                    if dag['workflowId'] != 'initial_setup' and initial_custom_settings:
                        entry['initial_custom_settings'] = initial_custom_settings
                    client_ids[dag['workflowId']].append(entry)

    return client_ids
