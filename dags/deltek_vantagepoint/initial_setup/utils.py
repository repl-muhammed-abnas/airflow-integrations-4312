import itertools
from uuid import uuid4
from airflow.models import Variable
from rail import result, get_current_context


def extract_airflow_event_ids():
    events = result('get_existing_workflow_events')
    return [
        {
            'EventID': event['EventID'],
            'EventType': event['EventType']
        }
        for event in events if 'airflow' in event['Description'].lower()
    ]


def is_airflow_webhook_action(action, event_lookup):
    event_id = action.get('EventID')
    return (
        event_id in event_lookup and
        action.get('ActionType') == 'Webhook' and
        action.get('Active') == 'Y' and
        'airflow' in action.get('Description', '').lower()
    )


def get_existing_webhook_actions_for_airflow():
    workflow_actions = result('get_existing_workflow_actions')
    workflow_event_ids = result('existing_workflow_event_ids')

    if not workflow_actions or not workflow_event_ids:
        return []

    event_lookup = {event['EventID']: event for event in workflow_event_ids}
    webhook_actions = []

    for action in workflow_actions:
        if is_airflow_webhook_action(action, event_lookup):
            event_id = action['EventID']
            webhook_actions.append({
                'ActionID': action['ActionID'],
                'EventID': event_id,
                'EventType': event_lookup[event_id]['EventType']
            })

    return webhook_actions



def add_field_conditions(conditions, event_id, field_name, field_values, condition_order, entity_type):
    for index, value in enumerate(field_values):
        if len(field_values) == 1:
            operator = "AND"
        else:
            if index == len(field_values) - 1:
                operator = "AND"
            else:
                operator = "OR"

        condition = {
            "ID": event_id,
            "ConditionID": str(uuid4()).replace('-', '').lower(),
            "ColumnName": f"{entity_type}AllCompany.{field_name}",
            "DataType": "varchar",
            "Operator": "=",
            "ExpectedValue": str(value),
            "ConditionOrder": condition_order,
            "ConditionOperator": operator,
            "SQLExpression": ""
        }
        conditions.append(condition)
        condition_order += 1
    return condition_order

def build_conditions_for_event(employee_filters, event_id, entity_type):
    conditions = []
    condition_order = 1

    for field_name, field_values in employee_filters.items():
        if field_name in ('Status', 'ReadyForProcessing') or not isinstance(field_values, list) or not field_values:
            continue
        condition_order = add_field_conditions(
            conditions, event_id, field_name, field_values, condition_order, entity_type
        )

    if conditions:
        conditions[-1]["ConditionOperator"] = ""

    return conditions

def get_conditions_to_update_or_create():
    dag_run_conf = get_current_context()['dag_run'].conf
    filter_var = dag_run_conf['filter_var']
    entity_type = dag_run_conf['entity_type']
    existing_conditions = result('get_existing_filter_conditions')
    webhook_actions = result('existing_webhook_actions_for_airflow')
    filters = Variable.get(
        filter_var,
        deserialize_json=True,
        default_var={}
    )

    if not filters or not webhook_actions:
        return []

    event_ids = {action['EventID'] for action in webhook_actions}
    existing_conditions_by_event = {}

    for condition in existing_conditions or []:
        if condition.get('ID') in event_ids:
            event_id = condition['ID']
            if event_id not in existing_conditions_by_event:
                existing_conditions_by_event[event_id] = []
            existing_conditions_by_event[event_id].append(condition)

    conditions_to_process = []

    for action in webhook_actions:
        event_id = action['EventID']
        new_conditions = build_conditions_for_event(filters, event_id, entity_type)
        existing_for_event = existing_conditions_by_event.get(event_id, [])

        for i, new_condition in enumerate(new_conditions):
            if i < len(existing_for_event):
                existing_condition = existing_for_event[i]
                conditions_to_process.append({
                    **new_condition,
                    'ConditionID': existing_condition['ConditionID'],
                    '_method': 'PUT',
                    '_endpoint': f"/Workflow/dlgWorkflowConditions/{existing_condition['ConditionID']}"
                })
            else:
                conditions_to_process.append({
                    **new_condition,
                    '_method': 'POST',
                    '_endpoint': '/Workflow/dlgWorkflowConditions'
                })

        if len(existing_for_event) > len(new_conditions):
            for excess_condition in existing_for_event[len(new_conditions):]:
                conditions_to_process.append({
                    'ConditionID': excess_condition['ConditionID'],
                    '_method': 'DELETE',
                    '_endpoint': f"/Workflow/dlgWorkflowConditions/{excess_condition['ConditionID']}"
                })

    return conditions_to_process

def build_all_workflow_conditions(usersync_filter_var):
    employee_filters = Variable.get(
        usersync_filter_var, deserialize_json=True, default_var={})
    if not employee_filters:
        return []

    webhook_actions = result('existing_webhook_actions_for_airflow')
    all_conditions = []

    for action in webhook_actions:
        event_conditions = build_conditions_for_event(employee_filters, action['EventID'])
        all_conditions.extend(event_conditions)

    return all_conditions


def build_combined_labor_code_options(labor_codes):
    by_level = {}
    for labor_code in labor_codes or []:
        try:
            level = int(labor_code.get('LCLevel'))
        except (TypeError, ValueError):
            continue
        code = (labor_code.get('Code') or '').strip()
        description = (labor_code.get('Description') or '').strip()
        if code == '':
            continue
        by_level.setdefault(level, []).append({'code': code, 'description': description})

    if not by_level:
        return []

    levels_in_order = [by_level[level] for level in sorted(by_level)]

    options = []
    seen_values = set()
    for combination in itertools.product(*levels_in_order):
        combined_code = ''.join(part['code'] for part in combination)
        combined_description = '/'.join(part['description'] for part in combination)
        value = f'{combined_code}-{combined_description}'
        # Dedup only on the combined code-description value.
        if value in seen_values:
            continue
        seen_values.add(value)
        options.append({
            'Description': value,
            'Category': combined_code
        })
    return options
