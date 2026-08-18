import itertools
import logging
import re
from uuid import uuid4
from rail import result, get_current_context



EMPLOYEE_TABLE = "EMAllCompany"
HOMECOMPANY_FIELD = "HomeCompany"
ORG_FIELD = "Org"
COUNTRY_FIELD = "Country"
STATE_FIELD = "State"


def build_company_name_to_code_map(companies):
    """Map VP firm display name (lowercased) -> company code, from /Settings/Company."""
    name_to_code = {}
    for company in companies or []:
        firm_name = (company.get('FirmName') or '').strip()
        code = (company.get('Company') or '').strip()
        if not firm_name or not code:
            continue
        key = firm_name.lower()
        if key in name_to_code and name_to_code[key] != code:
            logging.getLogger(__name__).warning(
                "Ambiguous HomeCompany name '%s' maps to multiple codes ('%s', '%s'); keeping first.",
                firm_name, name_to_code[key], code
            )
            continue
        name_to_code[key] = code
    return name_to_code


def build_org_name_to_path_map(organizations):
    """Map VP organization display name (lowercased) -> full org path, from /organization
    (e.g. 'Chicago' -> '00:00:000'). The company code is the first ':'-segment of the path."""
    name_to_path = {}
    for org in organizations or []:
        name = (org.get('Name') or '').strip()
        org_path = (org.get('Org') or '').strip()
        if not name or not org_path:
            continue
        key = name.lower()
        if key in name_to_path and name_to_path[key] != org_path:
            logging.getLogger(__name__).warning(
                "Ambiguous Org name '%s' maps to multiple paths ('%s', '%s'); keeping first.",
                name, name_to_path[key], org_path
            )
            continue
        name_to_path[key] = org_path
    return name_to_path


def build_country_name_to_code_map(countries):
    """Map VP country display name (lowercased) -> code, from /codeTable/FW_CFGCountry
    (e.g. 'United States' -> 'US'). Source rows look like {Code, Description}."""
    name_to_code = {}
    for country in countries or []:
        name = (country.get('Description') or '').strip()
        code = (country.get('Code') or '').strip()
        if not name or not code:
            continue
        key = name.lower()
        if key in name_to_code and name_to_code[key] != code:
            logging.getLogger(__name__).warning(
                "Ambiguous Country name '%s' maps to multiple codes ('%s', '%s'); keeping first.",
                name, name_to_code[key], code
            )
            continue
        name_to_code[key] = code
    return name_to_code


def build_state_name_to_code_map(states):
    """Map VP state display name (lowercased) -> code, from /codeTable/CFGStates
    (e.g. 'California' -> 'CA'). Source rows look like {Code, Description}.
    Note: state codes are country-scoped (e.g. 'WA' = Washington (US) and Western
    Australia (AU)), but name -> code is unambiguous since names differ."""
    name_to_code = {}
    for state in states or []:
        name = (state.get('Description') or '').strip()
        code = (state.get('Code') or '').strip()
        if not name or not code:
            continue
        key = name.lower()
        if key in name_to_code and name_to_code[key] != code:
            logging.getLogger(__name__).warning(
                "Ambiguous State name '%s' maps to multiple codes ('%s', '%s'); keeping first.",
                name, name_to_code[key], code
            )
            continue
        name_to_code[key] = code
    return name_to_code


# Labor-code filter fields are DefaultLC1..DefaultLC5; the level is the trailing digit.
# EMAllCompany.DefaultLC<N> stores the code; the UI may send the label name (e.g. 'WFH').
LABORCODE_FIELD_RE = re.compile(r'^defaultlc([1-9]\d*)$')


def build_laborcode_name_to_code_map(labor_codes):
    """Map labor-code description -> code, scoped by level, from /accountConfiguration/laborCode
    (rows look like {LCLevel, Code, Description}). Returns {level(int): {description_lower: code}}
    so e.g. DefaultLC3 'WFH' -> '0'. Codes are only unique within a level."""
    by_level = {}
    for lc in labor_codes or []:
        try:
            level = int(lc.get('LCLevel'))
        except (TypeError, ValueError):
            continue
        name = (lc.get('Description') or '').strip()
        code = (lc.get('Code') or '').strip()
        if not name or code == '':
            continue
        level_map = by_level.setdefault(level, {})
        key = name.lower()
        if key in level_map and level_map[key] != code:
            logging.getLogger(__name__).warning(
                "Ambiguous labor code name '%s' at level %s maps to multiple codes ('%s', '%s'); keeping first.",
                name, level, level_map[key], code
            )
            continue
        level_map[key] = code
    return by_level


def resolve_field_values(field_name, field_values, company_name_to_code=None, org_name_to_path=None,
                         country_name_to_code=None, state_name_to_code=None, laborcode_name_to_code=None):
    """Translate UI display values to what VP actually stores on the employee:
      - HomeCompany:   company name -> company code   (e.g. 'America' -> '00')
      - Org:           organization name -> org path  (e.g. 'Chicago' -> '00:00:000')
      - Country:       country name -> code           (e.g. 'United States' -> 'US')
      - State:         state name -> code             (e.g. 'California' -> 'CA')
      - DefaultLC<N>:  labor-code name -> code for that level (e.g. 'WFH' -> '0')
    Values not found in the VP lookup pass through unchanged so the condition is
    still created (e.g. EMAllCompany.HomeCompany = 'PC') but matches nothing,
    causing no incorrect sync. Deduplicates while preserving order."""
    fname = (field_name or '').strip().lower()
    lc_match = LABORCODE_FIELD_RE.match(fname)
    if fname == HOMECOMPANY_FIELD.lower() and company_name_to_code:
        lookup = company_name_to_code
    elif fname == ORG_FIELD.lower() and org_name_to_path:
        lookup = org_name_to_path
    elif fname == COUNTRY_FIELD.lower() and country_name_to_code:
        lookup = country_name_to_code
    elif fname == STATE_FIELD.lower() and state_name_to_code:
        lookup = state_name_to_code
    elif lc_match and laborcode_name_to_code:
        lookup = laborcode_name_to_code.get(int(lc_match.group(1)), {})
    else:
        return field_values

    resolved = []
    seen = set()
    for value in field_values:
        out = lookup.get(str(value).strip().lower(), f"EMAllCompany.{value}")
        if out not in seen:
            seen.add(out)
            resolved.append(out)
    return resolved


def add_field_conditions(conditions, event_id, field_name, field_values, condition_order, table_prefix):
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
            "ColumnName": f"{table_prefix}.{field_name}",
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


def build_conditions_for_event(employee_filters, event_id, table_prefix=EMPLOYEE_TABLE, company_name_to_code=None, org_name_to_path=None, country_name_to_code=None, state_name_to_code=None, laborcode_name_to_code=None):
    conditions = []
    condition_order = 1

    for field_name, field_values in employee_filters.items():
        if field_name in ('Status', 'ReadyForProcessing') or not isinstance(field_values, list) or not field_values:
            continue
        resolved_values = resolve_field_values(field_name, field_values, company_name_to_code, org_name_to_path, country_name_to_code, state_name_to_code, laborcode_name_to_code)
        condition_order = add_field_conditions(
            conditions, event_id, field_name, resolved_values, condition_order, table_prefix
        )

    if conditions:
        conditions[-1]["ConditionOperator"] = ""

    return conditions


def get_expected_labor_code_level_count(custom_settings):
    custom_settings = custom_settings or {}
    if 'M' in custom_settings and isinstance(custom_settings.get('M'), dict):
        custom_settings = custom_settings['M']
    labor_code_setting = custom_settings.get('laborCodeSetting', {}) or {}
    if not labor_code_setting.get('configureLaborCode', False):
        return 0
    return len(labor_code_setting.get('levels', []) or [])


def get_filters_from_custom_settings(custom_settings):
    filters = {}
    for filter_item in custom_settings.get('userSyncFilters', []):
        field_name = (filter_item.get('key') or filter_item.get('field', '')).strip()
        value = filter_item.get('value', '').strip()
        if field_name and value:
            filters.setdefault(field_name, []).append(value)
    return filters


def has_translatable_filters(employee_filters):
    """True if any configured filter field needs name->code resolution via a VP lookup
    fetch (HomeCompany/Org/Country/State/DefaultLC<N>). Status/ReadyForProcessing and
    unknown fields need no lookup, so when none of these are present the five lookup API
    calls can be skipped entirely."""
    translatable = {HOMECOMPANY_FIELD.lower(), ORG_FIELD.lower(), COUNTRY_FIELD.lower(), STATE_FIELD.lower()}
    for field_name in (employee_filters or {}):
        fname = (field_name or '').strip().lower()
        if fname in translatable or LABORCODE_FIELD_RE.match(fname):
            return True
    return False


def filters_need_lookups(custom_settings):
    """Whether the configured user-sync filters require the name->code lookup fetches.
    Used to gate the lookup tasks so they don't run when no translatable filter is set."""
    custom_settings = custom_settings or {}
    filters = get_filters_from_custom_settings(custom_settings) if custom_settings.get('userSyncFilters') else {}
    return has_translatable_filters(filters)


def get_conditions_to_update_or_create_for_child():
    dag_run = get_current_context()['dag_run']
    event_id = result('get_webhook_event_id')
    # API returns all conditions for ApplicationName, not scoped to this event — filter here
    # is the only event scoping. VP may use 'ID' or 'EventID', string or int. sorted() is
    # load-bearing: index-based PUT/POST/DELETE pairing below requires ConditionOrder sequence.
    existing_for_event = result('get_existing_conditions_for_event') or []

    custom_settings = dag_run.conf.get('customSettings', {})
    filters = get_filters_from_custom_settings(custom_settings) if custom_settings.get('userSyncFilters') else {}

    existing_for_event = sorted(
        [c for c in existing_for_event
         if str(c.get('ID') or c.get('EventID') or '') == str(event_id)],
        key=lambda c: int(c.get('ConditionOrder') or 0)
    )

    conditions_to_process = []

    if not filters:
        for condition in existing_for_event:
            conditions_to_process.append({
                'ConditionID': condition['ConditionID'],
                '_method': 'DELETE',
                '_endpoint': f"/Workflow/dlgWorkflowConditions/{condition['ConditionID']}"
            })
        return conditions_to_process

    # Lookup tasks only run when a translatable filter is present (gated by needs_filter_lookups).
    # When skipped, leave the maps empty so resolve_field_values passes values through unchanged.
    if has_translatable_filters(filters):
        company_name_to_code = build_company_name_to_code_map(result('get_all_companies_for_filter'))
        org_name_to_path = build_org_name_to_path_map(result('get_all_orgs_for_filter'))
        country_name_to_code = build_country_name_to_code_map(result('get_all_countries_for_filter'))
        state_name_to_code = build_state_name_to_code_map(result('get_all_states_for_filter'))
        laborcode_name_to_code = build_laborcode_name_to_code_map(result('get_all_labor_codes_for_filter'))
    else:
        company_name_to_code = org_name_to_path = country_name_to_code = state_name_to_code = laborcode_name_to_code = {}
    new_conditions = build_conditions_for_event(
        filters, event_id,
        company_name_to_code=company_name_to_code,
        org_name_to_path=org_name_to_path,
        country_name_to_code=country_name_to_code,
        state_name_to_code=state_name_to_code,
        laborcode_name_to_code=laborcode_name_to_code
    )

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


def get_missing_args_for_child():
    dag_run = get_current_context()['dag_run']
    event_id = result('get_webhook_event_id')
    all_args = result('get_existing_args_for_action') or []

    existing_for_action = [a for a in all_args if a.get('ActionID') == event_id]
    existing_arg_names = {a.get('ArgName') for a in existing_for_action}

    company_key = dag_run.conf['company_key']
    desired_args = [
        {
            'ActionID': event_id,
            'ArgName': 'Employee Number',
            'SQLExpression': f"'[:{EMPLOYEE_TABLE}.Employee]'",
            'ArgOrder': 1
        },
        {
            'ActionID': event_id,
            'ArgName': 'company_key',
            'SQLExpression': f"'{company_key}'",
            'ArgOrder': 2
        }
    ]

    return [arg for arg in desired_args if arg['ArgName'] not in existing_arg_names]


def get_missing_project_args_for_child(config):
    def _get_missing():
        dag_run = get_current_context()['dag_run']
        event_id = result('get_webhook_event_id')
        all_args = result('get_existing_args_for_action') or []

        existing_for_action = [a for a in all_args if a.get('ActionID') == event_id]
        existing_arg_names = {a.get('ArgName') for a in existing_for_action}

        company_key = dag_run.conf['company_key']
        event_type = dag_run.conf['event_type']

        max_base_arg_order = max(arg['ArgOrder'] for arg in config.project_webhook_args)

        raw_args = config.project_webhook_args + [
            {'ArgName': 'company_key', 'SQLExpression': 'dag_run.conf["company_key"]', 'ArgOrder': max_base_arg_order + 1, 'change_only': False},
            {'ArgName': 'OldReadyForProcessing', 'SQLExpression': "'[:PR.ReadyForProcessing.Old]'", 'ArgOrder': max_base_arg_order + 2, 'change_only': True},
            {'ArgName': 'OldStatus', 'SQLExpression': "'[:PR.Status.Old]'", 'ArgOrder': max_base_arg_order + 3, 'change_only': True},
        ]

        desired_args = []
        for arg in raw_args:
            if arg.get('change_only', False) and event_type != 'change':
                continue

            sql_expression = arg['SQLExpression']
            if sql_expression == 'event_type_action':
                actual_sql = "'INSERT'" if event_type == 'insert' else "'UPDATE'"
            elif sql_expression.startswith('dag_run.conf'):
                conf_key = sql_expression.split('[')[1].split(']')[0].strip('"\'')
                actual_sql = f"'{dag_run.conf[conf_key]}'"
            else:
                actual_sql = sql_expression

            desired_args.append({
                'ActionID': event_id,
                'ArgName': arg['ArgName'],
                'SQLExpression': actual_sql,
                'ArgOrder': arg['ArgOrder']
            })

        return [arg for arg in desired_args if arg['ArgName'] not in existing_arg_names]

    return _get_missing


def get_event_types():
    return [
        {'event_type': 'insert'},
        {'event_type': 'change'}
    ]


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
