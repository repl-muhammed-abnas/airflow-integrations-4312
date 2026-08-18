# pylint: disable=line-too-long
import ast
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import rail
from momentive.common_recipes_userimport.mappers.momentive_othercountries_mapper import momentive_othercountries_mapper

# Time-off type display names that drive the country-specific policy branches.
# Korean names kept as exact unicode escapes (recipe is source of truth).
KOR_MONTHLY_LEAVE = "KOR_Monthly Leave 월차휴가"
KOR_ANNUAL_LEAVE = "KOR_Annual Leave 연차휴가"
BEL_ADV = "[Bel] ADV"
BEL_ANNUAL = "[Bel] Jaarlijkse vakantie / Annual leave"
UK_HOLIDAY = "UK_Holiday Paid"


def split_date_string(date_str, split_type='string'):
    """Split a 'YYYY-MM-DD' date string into day/month/year parts.
    'string' -> string parts; 'int'/'datetime' -> integer parts.
    """
    d = datetime.strptime(date_str, "%Y-%m-%d")
    if split_type in ('int', 'datetime'):
        return {'day': d.day, 'month': d.month, 'year': d.year}
    return {'day': str(d.day), 'month': str(d.month), 'year': str(d.year)}


def past_policyset_entries(summary, timeoff_uri, termination_date):
    """Recipe (0-balance payout) steps 6-18: from GetUserTimeOffTypePolicySummary,
    take the matching time-off type's policySetSchedule, keep only entries whose
    effectiveDate is strictly before the termination date, and apply the
    null -> "effective" / "script" -> "scriptTarget" transform. Returns [] when none.
    """
    policies = (summary or {}).get('policiesByTimeOffType') or []
    schedule = rail.find_first_by_attr_and_get_attr(
        policies, 'timeOffType.uri', timeoff_uri, 'policySetSchedule', []) or []
    term = datetime.strptime(termination_date, "%Y-%m-%d").date()
    kept = []
    for entry in schedule:
        eff = entry.get('effectiveDate') or {}
        if not eff.get('day'):
            continue
        if datetime(eff['year'], eff['month'], eff['day']).date() < term:
            kept.append(entry)
    return json.loads(json.dumps(kept).replace('null', '"effective"').replace('"script"', '"scriptTarget"'))


# ---------------------------------------------------------------------------
# Timeoff Add New User (other-countries) helpers - recipe flow 1435239.
# ---------------------------------------------------------------------------


def _start_date(dag_run):
    return datetime.strptime(dag_run.conf['startdate'], "%Y-%m-%d")


def years_of_service(dag_run):
    """Recipe step 6: (startdate..today).count / 365 == inclusive day-span / 365."""
    start = _start_date(dag_run).date()
    today = datetime.now().date()
    return ((today - start).days + 1) / 365


def build_assignment_list(dag_run):
    """Recipe steps 4-14: split conf.timeofftypes on '|', and for each piped
    display name resolve {uri, name} from GetEnabledTimeOffTypes, applying the
    3 insert rules:
      - KOR Monthly Leave: only when years-of-service < 2
      - KOR Annual Leave: always
      - all others: always
    Returns {'assignments': [{uri, name}, ...], 'timeoff_type_uris': [uri, ...]}.
    """
    enabled = rail.result('get_enabled_timeofftypes') or []
    yos = years_of_service(dag_run)
    assignments = []
    for raw in dag_run.conf['timeofftypes'].split('|'):
        name = raw.strip()
        if not name:
            continue
        if name == KOR_MONTHLY_LEAVE and not yos < 2:
            continue
        uri = rail.find_first_by_attr_and_get_attr(enabled, 'displayText', name, 'uri', '')
        resolved = rail.find_first_by_attr_and_get_attr(enabled, 'displayText', name, 'displayText', '')
        if not uri:
            continue
        assignments.append({'uri': uri, 'name': resolved})
    return {
        'assignments': assignments,
        'timeoff_type_uris': [a['uri'] for a in assignments],
    }


def _flatten_policyset(default_response):
    """The GetDefault... response 'd' is [{description, effectiveDate, policySet}].
    Its policySet is the object the Put expects. Recipe pluck('policySet').first.
    Returns the first entry's policySet dict (or None when absent)."""
    rows = default_response or []
    if not rows:
        return None
    first = rows[0] if isinstance(rows, list) else rows
    return (first or {}).get('policySet')


def _normalise_policyset(policy_set):
    """Recipe null -> "effective" / "script" -> "scriptTarget" transform, done as
    a structured pass over a deep-copied dict (equivalent to the recipe gsub)."""
    text = json.dumps(policy_set, ensure_ascii=False)
    text = text.replace('null', '"effective"').replace('"script"', '"scriptTarget"')
    return json.loads(text)


def _set_param_amount(policy_set, script_name, key_uri, amount):
    """In policy_set.timeOffBalanceEventScripts, find the script whose
    scriptTarget/script name == script_name, then set its additionalParameters
    entry with keyUri == key_uri to value.number = amount. Mutates and returns."""
    for script in policy_set.get('timeOffBalanceEventScripts') or []:
        target = script.get('scriptTarget') or script.get('script') or {}
        if target.get('name') == script_name:
            for param in script.get('additionalParameters') or []:
                if param.get('keyUri') == key_uri:
                    param.setdefault('value', {})
                    param['value']['number'] = amount
    return policy_set


def _round_half(value):
    """Recipe step 88: snap to nearest 0.5 using the recipe's exact branch logic."""
    value = float(value)
    if (value % 0.5) == 0:
        return float(value)
    text = '%s' % value
    frac = text.split('.')[1] if '.' in text else '0'
    measure = int(frac) * 10 if len(frac) < 2 else int(frac)
    if measure > 50:
        return float(int(value) + 1)
    return float(str(int(value)) + '.50')


def build_timeoff_policy_entries(type_name, default_response, dag_run, scripts=None):
    """Single per-type policy builder (recipe steps 19-154). Given the time-off
    type display name, the GetDefaultTimeOffTypePolicyScheduleForUser response,
    the dag_run (for startdate) and (for KOR Monthly) the GetAllScripts uris,
    return the policySetScheduleEntries list to PUT, or '' when nothing applies.
    """
    start = _start_date(dag_run)
    eff = split_date_string(dag_run.conf['startdate'], 'datetime')
    is_jan_first = (start.month == 1 and start.day == 1)

    if type_name in (BEL_ADV, BEL_ANNUAL):
        policy_set = _flatten_policyset(default_response)
        if not policy_set:
            return ''
        policy_set = _normalise_policyset(policy_set)
        if is_jan_first:
            # Recipe steps 22-34: zero the January "Starting Balance Per Calendar Month".
            _set_param_amount(
                policy_set, "Starting Balance Per Calendar Month",
                "urn:replicon:script-key:parameter:january", 0)
        # else: re-apply default unchanged (recipe steps 36-40).
        return [{
            'description': f"Policy effective from {dag_run.conf['startdate']}",
            'effectiveDate': eff,
            'policySet': policy_set,
        }]

    if type_name == UK_HOLIDAY:
        # Recipe steps 44-62: prorate "Yearly Accrual"/"Starting Balance Set To".
        policy_set = _flatten_policyset(default_response)
        if not policy_set:
            return ''
        annual = _find_param_number(
            policy_set, "Yearly Accrual",
            "urn:replicon:script-key:parameter:accrual-annual-amount")
        next_year_start = _next_year_start(start)
        end_of_year = next_year_start - timedelta(days=1)
        days_in_year = float(end_of_year.timetuple().tm_yday)
        days_from_start = (next_year_start - start).days
        if start > start.replace(month=1, day=1):
            required = round((float(annual) / days_in_year) * days_from_start, 2)
        else:
            required = 0
        policy_set = _normalise_policyset(policy_set)
        _set_param_amount(
            policy_set, "Starting Balance Set To",
            "urn:replicon:script-key:parameter:amount", required)
        return [{
            'description': f"Policy effective from {dag_run.conf['startdate']}",
            'effectiveDate': eff,
            'policySet': policy_set,
        }]

    if type_name == KOR_ANNUAL_LEAVE:
        # Recipe steps 71-94.
        policy_set = _flatten_policyset(default_response)
        if not policy_set:
            return ''
        this_year = datetime.now().year
        start_year = start.year
        yr_diff = this_year - start_year
        next_year_start = _next_year_start(start)
        end_of_year = next_year_start - timedelta(days=1)
        days_in_year = float(end_of_year.timetuple().tm_yday)
        days_from_start = (next_year_start - start).days
        entitlement = 15.0
        if yr_diff < 2:
            entitlement = round((entitlement / days_in_year) * days_from_start, 2)
        if yr_diff > 1.99:
            entitlement = round((yr_diff - 2) + entitlement, 2)
        entitlement = _round_half(entitlement)
        policy_set = _normalise_policyset(policy_set)
        _set_param_amount(
            policy_set, "Yearly Accrual",
            "urn:replicon:script-key:parameter:accrual-annual-amount", entitlement)
        yos = years_of_service(dag_run)
        year = this_year if yos > 0.99 else this_year + 1
        return [{
            'description': f"Policy effective from 01/01/{year}",
            'effectiveDate': {'day': 1, 'month': 1, 'year': year},
            'policySet': policy_set,
        }]

    if type_name == KOR_MONTHLY_LEAVE:
        # Recipe step 98: self-contained two-entry schedule built from startdate and
        # the GetAllScripts uris (Starting Balance Set To + Yearly Accrual).
        return _kor_monthly_entries(dag_run, scripts or {})

    # Generic type (recipe steps 64-68): re-apply default unchanged when present.
    rows = default_response or []
    first = rows[0] if isinstance(rows, list) and rows else {}
    if not (first.get('effectiveDate') or {}).get('day'):
        return ''
    return _normalise_policyset(default_response)


def _find_param_number(policy_set, script_name, key_uri):
    """Read value.number for keyUri under the named script in a default policySet."""
    for script in (policy_set or {}).get('timeOffBalanceEventScripts') or []:
        target = script.get('scriptTarget') or script.get('script') or {}
        if target.get('name') == script_name:
            for param in script.get('additionalParameters') or []:
                if param.get('keyUri') == key_uri:
                    return float((param.get('value') or {}).get('number') or 0)
    return 0.0


def _next_year_start(start):
    """beginning_of_year of (startdate + 12 months) == Jan 1 of the year after start
    (recipe '(startdate + 12.months).beginning_of_year')."""
    plus = (start + timedelta(days=365))
    return datetime(plus.year, 1, 1)


def _kor_monthly_entries(dag_run, scripts):
    """Recipe step 98 PutUserTimeOffAccountPolicySetSchedule body for KOR Monthly Leave."""
    start = _start_date(dag_run)
    start_year = start.year
    start_month = start.month
    sbst_uri = scripts.get('starting_balance_set_to')
    accrual_uri = scripts.get('yearly_accrual')

    def amount_param(amount):
        return {
            'keyUri': 'urn:replicon:script-key:parameter:amount',
            'value': {'number': amount, 'uri': None},
        }

    def precedence_param(value):
        return {
            'keyUri': 'urn:replicon:script-key:parameter:precedence',
            'value': {'number': value, 'uri': None},
        }

    accrue_params = [
        {'keyUri': 'urn:replicon:script-key:parameter:proration-option',
         'value': {'number': None, 'uri': 'urn:replicon:time-off-policy-proration-option:do-not-prorate'}},
        precedence_param("30"),
        {'keyUri': 'urn:replicon:script-key:parameter:accrue-on-month',
         'value': {'number': None, 'uri': 'urn:replicon:time-off-policy-anniversary-option:anniversary-of-user-start-date'}},
        {'keyUri': 'urn:replicon:script-key:parameter:accrue-on-day-of-month',
         'value': {'number': None, 'uri': 'urn:replicon:time-off-policy-anniversary-option:anniversary-of-user-start-date'}},
        {'keyUri': 'urn:replicon:script-key:parameter:accrual-annual-amount',
         'value': {'number': str(12 - start_month), 'uri': None}},
    ]
    entry_one = {
        'description': f"Policy effective from 01/01/{start_year}",
        'effectiveDate': {'day': 1, 'month': 1, 'year': start_year},
        'policySet': {
            'timeOffBalanceEventScripts': [
                {'scriptTarget': {'description': 'Set initial balance for the first day of a policy',
                                  'name': 'Starting Balance Set To', 'uri': sbst_uri},
                 'additionalParameters': [amount_param("0")]},
                {'additionalParameters': accrue_params,
                 'scriptTarget': {'description': 'Accrues time once per year.',
                                  'name': 'Yearly Accrual', 'uri': accrual_uri}},
            ]
        },
    }
    accrue_params_two = [
        {'keyUri': 'urn:replicon:script-key:parameter:proration-option',
         'value': {'uri': 'urn:replicon:time-off-policy-proration-option:do-not-prorate', 'number': None}},
        precedence_param("30"),
        {'value': {'number': None, 'uri': 'urn:replicon:month:january'},
         'keyUri': 'urn:replicon:script-key:parameter:accrue-on-month'},
        {'value': {'number': None, 'uri': 'urn:replicon:monthly-frequency-start-day-option:1st'},
         'keyUri': 'urn:replicon:script-key:parameter:accrue-on-day-of-month'},
        {'keyUri': 'urn:replicon:script-key:parameter:accrual-annual-amount',
         'value': {'uri': None, 'number': str(11 - (12 - start_month))}},
    ]
    entry_two = {
        'description': f"Policy effective from 01/01/{start_year + 1}",
        'effectiveDate': {'day': 1, 'month': 1, 'year': start_year + 1},
        'policySet': {
            'timeOffBalanceEventScripts': [
                {'additionalParameters': [amount_param("0"), precedence_param("10")],
                 'scriptTarget': {'description': 'Set initial balance for the first day of a policy',
                                  'name': 'Starting Balance Set To', 'uri': sbst_uri}},
                {'additionalParameters': accrue_params_two,
                 'scriptTarget': {'description': 'Accrues time once per year.',
                                  'name': 'Yearly Accrual', 'uri': accrual_uri}},
            ]
        },
    }
    return [entry_one, entry_two]


# ---------------------------------------------------------------------------
# Policy Assignment Rehire / Update-days (other-countries) helpers - flow 1435236.
# ---------------------------------------------------------------------------


def past_rehire_policyset_entries(summary, timeoff_uri, start_date):
    """Recipe steps 6-13: from GetUserTimeOffTypePolicySummary, take this time-off
    type's policySetSchedule and keep only entries whose effectiveDate is strictly
    before the (re)start date. Kept raw - the "script" -> "scriptTarget" rename is
    applied later over the whole combined list (recipe step 28). Returns [] when none.
    """
    policies = (summary or {}).get('policiesByTimeOffType') or []
    schedule = rail.find_first_by_attr_and_get_attr(
        policies, 'timeOffType.uri', timeoff_uri, 'policySetSchedule', []) or []
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    kept = []
    for entry in schedule:
        eff = entry.get('effectiveDate') or {}
        if not eff.get('day'):
            continue
        if datetime(eff['year'], eff['month'], eff['day']).date() < start:
            kept.append(entry)
    return kept


def build_rehire_policy_entries(past_entries, default_schedule, start_date):
    """Recipe steps 18-28: for each default policy-set tier whose startOffset.offsetValue
    is a recognised years-of-service step, append a new schedule entry re-anchored onto
    the (re)start date, then rename "script" -> "scriptTarget" over the whole combined
    list. Tier -> effective date (recipe steps 19-26):
      0  -> the start date                (description "Policy <startdate>")
      1  -> Jan 1 of the year after start (description "Policy <startdate>")
      5  -> Jan 1 of start year + 5       (description "Added for rehire<dd/mm/yyyy>")
      10 -> Jan 1 of start year + 10      (description "Policy <startdate>")
    Other offset values have no branch in the recipe and are ignored. Returns the
    combined policySetScheduleEntries list (preserved past + re-anchored tiers), or [].
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    entries = list(past_entries or [])
    for tier in default_schedule or []:
        value = (tier.get('startOffset') or {}).get('offsetValue')
        try:
            years = int(float(value))
        except (TypeError, ValueError):
            continue
        if years == 0:
            eff = {'day': start.day, 'month': start.month, 'year': start.year}
            description = f"Policy {start_date}"
        elif years == 1:
            eff = {'day': 1, 'month': 1, 'year': start.year + 1}
            description = f"Policy {start_date}"
        elif years == 5:
            eff = {'day': 1, 'month': 1, 'year': start.year + 5}
            description = f"Added for rehire{start.strftime('%d/%m/%Y')}"
        elif years == 10:
            eff = {'day': 1, 'month': 1, 'year': start.year + 10}
            description = f"Policy {start_date}"
        else:
            continue
        entries.append({
            'effectiveDate': eff,
            'policySet': tier.get('policySet'),
            'description': description,
        })
    if not entries:
        return []
    return json.loads(
        json.dumps(entries, ensure_ascii=False).replace('"script"', '"scriptTarget"'))


# ---------------------------------------------------------------------------
# Timeoff Add New User REHIRE (other-countries) helpers - recipe flow 1435238.
# Ported verbatim from the equivalent South Korea implementation
# (dags/momentive/user_import_south_korea) which is a faithful port of the same
# recipe (generic / KOR Annual / KOR Monthly 3-year branches).
# ---------------------------------------------------------------------------


def get_policy_to_assign(response):
    """Generic-branch data_handler (recipe steps 8-11): re-apply the default policy
    schedule with the null->effective / script->scriptTarget transform. Returns a
    JSON string, or None when the default schedule is empty."""
    if not response:
        return None
    res = list(map(lambda item: {
        'description': 'effective',
        'effectiveDate': item['effectiveDate'],
        'policySet': item['policySet']
    }, response))
    return json.dumps(ast.literal_eval(str(res).replace("'script'", "'scriptTarget'")))


def get_number_of_days_proration(dag_run):
    """Recipe step 28/52: days from the (re)start date to Jan 1 of the year after start."""
    start_date = datetime.strptime(dag_run.conf["startdate"], "%Y-%m-%d")
    begining_year = start_date + relativedelta(months=12)
    start_of_year = begining_year.replace(month=1, day=1)
    return (start_of_year.timestamp() - start_date.timestamp()) / 86400


def update_yearlyentitilement_val_30(dag_run):
    """Recipe step 29 (calendar YoS < 2): prorate the 15-day annual entitlement by
    (days from start to next year start) / (days in the start year)."""
    start_date = datetime.strptime(dag_run.conf['startdate'], "%Y-%m-%d")
    begining_year = start_date + relativedelta(months=12)
    end_of_year = begining_year.replace(month=1, day=1) - timedelta(days=1)
    return round((float(rail.get_dag_run_var(
        rail.result('create_yearlyentitilement')['name']))/int(end_of_year.strftime('%j'))) * int(
            rail.result('log_numberofdaysforproration_for_yearly')), 2)


def accurals_rounded_val():
    """Recipe step 33: snap the annual entitlement to the nearest 0.5."""
    yearly_entitlement = str(rail.get_dag_run_var(rail.result('create_yearlyentitilement')['name']))
    if (float(yearly_entitlement) % 0.5) == 0:
        return float(yearly_entitlement)
    if len(yearly_entitlement.split('.')[1]) < 2:
        if int(yearly_entitlement.split('.')[1]) * 10 > 50:
            return float(int(yearly_entitlement) + 1)
        return float(str(int(yearly_entitlement)) + '.50')
    if int(yearly_entitlement.split('.')[1]) > 50:
        return float(int(yearly_entitlement) + 1)
    return float(str(int(yearly_entitlement)) + '.50')


# ---------------------------------------------------------------------------
# Supervisor assignment (other-countries) helpers - recipe supervisor-assignment_v2.
# Ported verbatim from the South Korea implementation (same recipe).
# ---------------------------------------------------------------------------


def get_userdata_list_for_managerid(response, dag_run):
    """Recipe nodes 9/10/15/16: from the user-list GetData, keep rows whose
    employee-id column matches conf.managerid; return [{uri, loginname, managerid_txt}]."""
    if list(filter(lambda x: 'textValue' in x['cells'][0] and x['cells'][0]['textValue'] == dag_run.conf['managerid'], response['rows'])):
        return list(filter(lambda x: 'textValue' in x['managerid_txt'] and x['managerid_txt']['textValue'] == dag_run.conf['managerid'], list(map(
            lambda d: {
                'uri': d['cells'][1]['uri'],
                'loginname': d['cells'][1]['textValue'],
                'managerid_txt': d['cells'][0]
            }, response['rows']))))
    return []


def get_exceptions():
    """Concatenate the supervisor soft-failure reasons (multiple-match / disabled /
    foreign-supervisor-not-received) for the log-entry rewrite."""
    return (rail.result('log_multiple_user_for_same_managerid') if rail.result(
        'log_multiple_user_for_same_managerid') else '') + (rail.result('log_supervisor_disabled') if rail.result(
            'log_supervisor_disabled') else '') + (rail.result('log_foreign_supervisor_not_received') if rail.result(
                'log_foreign_supervisor_not_received') else '')


def get_supervisor_status_escalation():
    """Recipe col5 rule: only multiple-match (node 13) and supervisor-disabled (node 30)
    escalate the log status to 'Exception'. Foreign-created (node 41) and
    foreign-not-received (node 44) KEEP the existing status (their message is appended to
    details only). This is a subset of get_exceptions() used for the status decision."""
    return (rail.result('log_multiple_user_for_same_managerid') if rail.result(
        'log_multiple_user_for_same_managerid') else '') + (rail.result('log_supervisor_disabled') if rail.result(
            'log_supervisor_disabled') else '')


# ---------------------------------------------------------------------------
# Update user timeoff assign (other-countries) helpers - recipe update-user-time-off_assign_v2.
# ---------------------------------------------------------------------------


def get_previoustimeoff_list():
    """Recipe nodes 12-14: de-duped list of the user's currently-assigned time-off uris."""
    assigned = rail.result('get_assigned_timeofftypes') or {}
    if isinstance(assigned, list) and assigned:
        assigned = assigned[0]
    timeoff_types = (assigned.get('timeOffTypeAssignmentsDetails', {}) or {}).get('timeOffTypes', [])
    if not isinstance(timeoff_types, list):
        return []
    return list({d.get('uri') for d in timeoff_types if isinstance(d, dict) and d.get('uri')})


def get_final_timeoff(dag_run):
    """Recipe nodes 20-51: from the requested (pipe-delimited) timeofftypes, compute the
    fan-out buckets. The rehire buckets are populated only for types already assigned AND
    when it is a rehire (oldstartdate != hiredate), routed by type-name (recipe nodes 42-51):
        KOR annual/monthly       -> timeoff_add_rehire        (child 1435238)
        name startswith '[BEL]'  -> bel_policy_rehire         (child 1362490)
        name == 'UK_Holiday Paid'-> skipped (UK branch ON HOLD - not routed anywhere)
        else (generic)           -> annual_leave_policy_rehire(child 1435236)
    FIX vs the SK port: SK dropped the BEL (1362490) bucket and inverted the generic filter
    (it required name.startswith('[BEL]') where the recipe requires not_starts_with '[BEL]');
    this restores the recipe's routing. The UK (1435242) branch is intentionally on hold, so
    UK_Holiday Paid types are explicitly skipped here (NOT misrouted to the generic child).
    """
    enabled = rail.result('get_alltimeoff_types') or []
    requested = dag_run.conf['timeofftypes'].split('|')
    final_timeoff_set = [
        {'name': d['displayText'], 'uri': d['uri']}
        for d in enabled if d['displayText'] in requested
    ]
    final_uris = list(set(map(lambda x: x['uri'], final_timeoff_set)))
    previous_uris = rail.result('get_previoustimeofflist') or []

    timeoff_previously_assigned_to_be_notassigned = list(set(filter(lambda x: x not in final_uris, previous_uris)))
    timeoff_not_previously_assigned = [it for it in final_timeoff_set if it['uri'] not in previous_uris]

    is_rehire = datetime.strptime(dag_run.conf['oldstartdate'], '%Y-%m-%d') != datetime.strptime(
        dag_run.conf['hiredate'], '%Y-%m-%d')

    timeoff_add_rehire, bel_policy_rehire, annual_leave_policy_rehire = [], [], []
    for item in final_timeoff_set:
        if item['uri'] in previous_uris and is_rehire:
            name = item['name']
            if name in (KOR_ANNUAL_LEAVE, KOR_MONTHLY_LEAVE):
                timeoff_add_rehire.append(item)
            elif name.startswith('[BEL]'):
                bel_policy_rehire.append(item)
            elif name == UK_HOLIDAY:
                continue  # UK branch ON HOLD - do not route (and do not fall through to generic)
            else:
                annual_leave_policy_rehire.append(item)

    return {
        'final_timeoff_assign_val': final_uris,
        'final_timeoff_list': final_timeoff_set,
        'timeoff_previously_assigned_to_be_notassigned': timeoff_previously_assigned_to_be_notassigned,
        'timeoff_not_previously_assigned': timeoff_not_previously_assigned,
        'timeoff_add_rehire': timeoff_add_rehire,
        'bel_policy_rehire': bel_policy_rehire,
        'annual_leave_policy_rehire': annual_leave_policy_rehire,
    }


# ---------------------------------------------------------------------------
# Disable user (other-countries) helpers - recipe child_workflow-to-disable-user.
# ---------------------------------------------------------------------------


def validate_terminationdate(dag_run):
    """Recipe node 6: DisableLogin only when the termination date is on/before today.
    Uses a truthiness check, not `in conf`: a blank string is present in conf but unparseable."""
    if dag_run.conf.get('terminationdate'):
        if datetime.strptime(dag_run.conf['terminationdate'], "%Y-%m-%d").date() <= datetime.now().date():
            return True
    return False


# ---------------------------------------------------------------------------
# Add user (other-countries) helpers - recipe user-sync-add_v3. Ported from the SK
# implementation; mapper key-access adapted to the common lowercase mapper
# (momentive_othercountries_mapper: type/workertype/location/exemptstatus/shift/
# legalentity/gender/country/value).
# ---------------------------------------------------------------------------


def get_iniial_country_lookup_value(dag_run):
    """Recipe node 16: map the Workday Country to the mapper country_lookup value."""
    return "South Korea" if "Korea, Republic of" in dag_run.conf['country'] else "UAE" if "United Arab Emirates" in dag_run.conf['country'] else \
        "Belgium" if "Belgium" in dag_run.conf['country'] else "France" if "France" in dag_run.conf['country'] else \
            "United Kingdom" if "United Kingdom" in dag_run.conf['country'] else "Null"


def get_input_validationlog(dag_run):
    """Recipe nodes 4-10: required-field validation for the add row."""
    exception_list = []
    if not dag_run.conf['userid']:
        exception_list.append('Login name not present')
    if not dag_run.conf['firstname']:
        exception_list.append('First_Name not present')
    if not dag_run.conf['lastname']:
        exception_list.append('Last_Name not present')
    if not dag_run.conf['hiredate']:
        exception_list.append('Hire date not present')
    if not dag_run.conf['emailaddress']:
        exception_list.append('Email_Address not present')
    if not dag_run.conf['exemptionstatus']:
        exception_list.append('Excemption Status not present')
    if not dag_run.conf['workertype']:
        exception_list.append('Worker type not present')
    if not dag_run.conf['location']:
        exception_list.append('Department (location) not present')
    if not dag_run.conf['active']:
        exception_list.append('Employee status not present')
    if not dag_run.conf['managerid']:
        exception_list.append('Manager ID not present')
    if not dag_run.conf['country']:
        exception_list.append('Country not present')
    if len(exception_list) > 0:
        return {'exc_present': True, 'exc_value': ','.join(exception_list)}
    return {'exc_present': False, 'exc_value': ''}


# pylint: disable=too-many-boolean-expressions
def search_in_mapper_for_employeetype(dag_run):
    """Recipe node 36: mapper 'Employee Type' lookup (col9 value)."""
    for data in momentive_othercountries_mapper:
        if (data['type'] == 'Employee Type') and \
            (data['workertype'] == dag_run.conf["workertype"]) and \
            (data['location'] == rail.result('get_location_lookup_variable')['value']) and \
            (data['exemptstatus'] == ('Yes' if '1' in dag_run.conf['exemptionstatus'] else 'No')) and \
            (data['shift'] == 'Any') and \
            (data['legalentity'] == rail.result('get_workersubshift_lookup_variable')['value']) and \
            (data['gender'] == 'Any') and \
            (data['country'] == rail.result('get_country_lookup_variable')['value']):
            return {'value': data['value']}
    return {'value': None}


def get_details_for_employeetype_and_departmentygrpuri_not_exist(dag_run):
    """Recipe node 39: exception details when employee-type / department group is missing."""
    details = ''
    if (not rail.result('search_entry_in_mapper_for_employeetype_37')['value']) or (
        rail.result('search_entry_in_mapper_for_employeetype_37')['value'] and not rail.result('get_required_employeetype_uri')):
        details = details + 'User not created, since Employee type group does not exist in Replicon or is disabled'
    if not dag_run.conf['departmentgroupuri']:
        details = details + 'User not created, since Department (location)  does not exist in Replicon or is disabled'
    return details


def search_momentivemapper_workertype_country(dag_run):
    """Recipe node 43: load the mapper rows for this worker-type + country."""
    output = []
    for data in momentive_othercountries_mapper:
        if data['workertype'] == dag_run.conf["workertype"] and data['country'] == rail.result('get_country_lookup_variable')['value']:
            output.append(data)
    return output


# pylint: disable=too-many-branches
def user_mappings_mapper(workertype, exemptionstatus, gender, arg):
    """Recipe node 45: resolve every mapper category (timesheet template, payrule, schedule,
    holiday calendar, punch entry policy, time zone, work week, activity, language, time-off
    types, ...) for this worker."""
    timesheet = ''
    timesheetapprovalpath = ''
    payrule = ''
    schedule = ''
    activities = ''
    punchentrypolicy = ''
    timezone = ''
    holidaycalendar = ''
    timeoffs = ''
    language = ''
    timeoffapprovalpath = 'Supervisor'
    if arg == 'add':
        timesheetperiod = None
        workweek = ''

    # pylint: disable=too-many-nested-blocks, too-many-boolean-expressions
    for data in rail.result('search_momentive_mapper_values'):
        if data['type']:
            if (data['workertype'] == workertype) and \
                (data['location'] == rail.result('get_location_lookup_variable')['value']) and \
                (data['exemptstatus'] == ('Yes' if '1' in exemptionstatus else 'No')) and \
                (data['legalentity'] == rail.result('get_workersubshift_lookup_variable')['value']) and \
                (data['country'] == rail.result('get_country_lookup_variable')['value']):

                if data['shift'] == rail.result('get_shift_lookup_variable')['value']:
                    if data['gender'] == 'Any':
                        if data['type'] == 'Timesheet Template':
                            timesheet = data['value']
                        if data['type'] == 'Timesheet approval path':
                            timesheetapprovalpath = data['value']
                        if data['type'] == 'Payrule':
                            payrule = data['value']
                        if data['type'] == 'Schedule':
                            schedule = data['value']
                        if data['type'] == 'Activity':
                            activities = data['value']
                        if data['type'] == 'Punch entry policy':
                            punchentrypolicy = data['value']

                if data['shift'] == 'Any':
                    if data['gender'] == 'Any':
                        if data['type'] == 'Holiday Calendar':
                            holidaycalendar = data['value']
                        if data['type'] == 'Time zone':
                            timezone = data['value']
                        if arg == 'add':
                            if data['type'] == 'Work week':
                                workweek = data['value']

                    if data['gender'] == gender:
                        if data['type'] == 'Time off types':
                            timeoffs = data['value']

            # pylint: disable=too-many-boolean-expressions
            if (data['workertype'] == workertype) and \
                (data['legalentity'] == rail.result('get_workersubshift_lookup_variable')['value']) and \
                (data['country'] == rail.result('get_country_lookup_variable')['value']) and \
                (data['location'] == 'Any') and (data['exemptstatus'] == 'Any') and (data['shift'] == 'Any') and (data['gender'] == 'Any'):
                if data['type'] == 'Language':
                    language = data['value']
    return {
        'timesheet': timesheet,
        'timesheetapprovalpath': timesheetapprovalpath,
        'payrule': payrule,
        'schedule': schedule,
        'activities': activities,
        'punchentrypolicy': punchentrypolicy,
        'timezone': timezone,
        'holidaycalendar': holidaycalendar,
        'workweek': workweek,
        'timeoffs': timeoffs,
        'language': language,
        'timeoffapprovalpath': timeoffapprovalpath,
        'timesheetperiod': timesheetperiod
    } if arg == 'add' else {
        'timesheet': timesheet,
        'timesheetapprovalpath': timesheetapprovalpath,
        'payrule': payrule,
        'schedule': schedule,
        'activities': activities,
        'punchentrypolicy': punchentrypolicy,
        'timezone': timezone,
        'holidaycalendar': holidaycalendar,
        'timeoffs': timeoffs,
        'language': language,
        'timeoffapprovalpath': timeoffapprovalpath,
    }


def get_status_and_details_for_add(dag_run):
    """Recipe node 198: the add success/exception user-import log entry."""
    message = "Success"
    details = "User created successfully"
    has_exception_message = ','.join(list(map(lambda v: v['value'], rail.load_all_records(rail.result('write_log_user_import')))))
    if has_exception_message:
        message = "Exception"
        details = "User created with exception" + ' ' + has_exception_message
    return {
        "userid": dag_run.conf['userid'],
        "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
        "action": "Add",
        "status": message,
        'details': details,
        "country": rail.result('get_country_lookup_variable')['value']
    }


# ---------------------------------------------------------------------------
# Update user (other-countries) helpers - recipe user-sync-update_v3. Ported from SK
# (mapper lookups reuse the add-path helpers above; these do not touch mapper keys).
# ---------------------------------------------------------------------------


def validate_hiredate(dag_run):
    """Truthiness check, not `in conf`: a blank string is present in conf but unparseable."""
    if dag_run.conf.get('hiredate'):
        if datetime.strptime(dag_run.conf['hiredate'], "%Y-%m-%d").date() <= datetime.now().date():
            return True
    return False


def validate_hiredate_startdate(dag_run):
    """True when the user's current employment start date equals the conf hire date."""
    if datetime.strptime(str(rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']['year']) + '-' + str(
            rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']['month']) + '-' + str(
                rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']['day']), "%Y-%m-%d") == datetime.strptime(
                    dag_run.conf['hiredate'], "%Y-%m-%d"):
        return True
    return False


def validate_terminationdate_enddate(dag_run):
    """True when the user's current employment end date already equals the conf termination date.

    A blank termination date means 'no end date received', which can never equal a stored one,
    so it returns False without parsing (the caller additionally gates on
    `terminationdate | is_truthy`). Parsing it unconditionally raised
    ValueError: time data '' does not match format '%Y-%m-%d' for every ordinary update of a
    user who already carries an employment end date in Replicon."""
    if not dag_run.conf.get('terminationdate'):
        return False
    enddate = datetime.strptime('2099-01-01', "%Y-%m-%d")
    userend_date = rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['endDate']
    if userend_date and 'day' in userend_date:
        enddate = datetime.strptime(
            str(userend_date['year']) + '-' + str(userend_date['month']) + '-' + str(userend_date['day']), "%Y-%m-%d")
        if enddate == datetime.strptime(dag_run.conf['terminationdate'], "%Y-%m-%d"):
            return True
    return False


def get_udf_values_from_userdetails():
    """Current UDF (custom field) text values + field uris from the fetched user details."""
    user_customfield = rail.result('get_user_data')[0]['userDetails']['customFieldValues']
    return {
        'dob': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Date of Birth', 'text', ''),
        'title': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Title', 'text', ''),
        'worker_subType': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Worker Sub Type', 'text', ''),
        'yearsofservice': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Years of Service', 'text', ''),
        'hrm': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'HRM', 'text', ''),
        'cont_yearsofservice': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Continuous Years of Service - YOS', 'text', ''),
        'timeoffservcdate': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Time off Service Date - YOSS', 'text', ''),
        'gender': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Gender', 'text', ''),
        'function': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Function', 'text', ''),
        'work_shift': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Work Shift', 'text', ''),
        'dob_uri': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Date of Birth', 'customField.uri', ''),
        'title_uri': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Title', 'customField.uri', ''),
        'workersubtype_uri': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Worker Sub Type', 'customField.uri', ''),
        'yearsofservice_uri': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Years of Service', 'customField.uri', ''),
        'hrm_uri': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'HRM', 'customField.uri', ''),
        'cont_yearsofservice_uri': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Continuous Years of Service - YOS', 'customField.uri', ''),
        'timeoffservcdate_uri': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Time off Service Date - YOSS', 'customField.uri', ''),
        'gender_uri': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Gender', 'customField.uri', ''),
        'function_uri': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Function', 'customField.uri', ''),
        'workshift_uri': rail.find_first_by_attr_and_get_attr(user_customfield, 'customField.displayText', 'Work Shift', 'customField.uri', ''),
    }


def compare_dates_to_today(dag_run):
    """Which change-effective dates equal today (drives exemption/shift/worker-type/location
    change flags + the time-off trigger)."""
    exemptioneff_date = False
    workshiftchangeeffective_date = False
    effectivedateof_workertype = False
    cflrvlocationchange_effectivedate = False
    if dag_run.conf['cf_lrv_job_exempt_eff_date']:
        if datetime.strptime(dag_run.conf['cf_lrv_job_exempt_eff_date'], "%Y-%m-%d").date() == datetime.now().date():
            exemptioneff_date = True
    if dag_run.conf['work_shift_change_effective_date']:
        if datetime.strptime(dag_run.conf['work_shift_change_effective_date'], "%Y-%m-%d").date() == datetime.now().date():
            workshiftchangeeffective_date = True
    if dag_run.conf['effective_date_of_worker_type']:
        if datetime.strptime(dag_run.conf['effective_date_of_worker_type'], "%Y-%m-%d").date() == datetime.now().date():
            effectivedateof_workertype = True
    if dag_run.conf['location_change_eff_date']:
        if datetime.strptime(dag_run.conf['location_change_eff_date'], "%Y-%m-%d").date() == datetime.now().date():
            cflrvlocationchange_effectivedate = True
    return {
        'exemption_eff_date': exemptioneff_date,
        'workshift_change_effective_date': workshiftchangeeffective_date,
        'effective_date_of_workertype': effectivedateof_workertype,
        'cf_lrv_location_change_effective_date': cflrvlocationchange_effectivedate
    }


def get_startday_of_nexttimesheet():
    """Day after the current timesheet's end date (else today) - the effective date for updates."""
    if 'day' in rail.result('get_timesheet_details')['dateRange']['endDate']:
        return str((datetime.strptime(
            str(rail.result('get_timesheet_details')['dateRange']['endDate']['year']) + '-' + str(
                rail.result('get_timesheet_details')['dateRange']['endDate']['month']) + '-' + str(
                    rail.result('get_timesheet_details')['dateRange']['endDate']['day']), "%Y-%m-%d") + timedelta(days=1)).date())
    return str(datetime.now().date())


def get_current_data(arg1, arg2):
    """Current payrule/schedule schedule entry whose effectiveDate is nearest today.

    Entries that carry no arg2 object are SKIPPED: a Shift-type schedule entry has
    officeSchedule = None by design (the recipe reads officeSchedule.uri unguarded, which
    yields nil in Workato instead of raising, so such an entry simply never wins the
    nearest-effectiveDate comparison). SK subscripted it directly -> TypeError:
    'NoneType' object is not subscriptable for any user whose history contains a Shift
    entry, even a closed/past one. When no entry qualifies we return a blank uri, which
    the caller's mismatch gate already treats as 'needs updating'."""
    data_dict = {}
    data = rail.result('get_user_data')[0][arg1]
    emplpoyment_daterange_data = rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']
    for p_data in data:
        if not p_data.get(arg2) or not p_data[arg2].get('uri'):
            continue
        if p_data['effectiveDate']:
            effective_date = str(p_data['effectiveDate']['month']) + "/" + str(p_data['effectiveDate']['day']) \
                + "/" + str(p_data['effectiveDate']['year'])
        else:
            effective_date = str(emplpoyment_daterange_data['month']) + "/" + str(emplpoyment_daterange_data['day']) \
                + "/" + str(emplpoyment_daterange_data['year'])
        date_diff = (datetime.strptime(datetime.now().strftime("%m/%d/%Y"), "%m/%d/%Y") - datetime.strptime(effective_date, "%m/%d/%Y")).days
        data_dict[p_data[arg2]['uri']] = date_diff
    if not data_dict:
        return {'uri': '', 'text': ''}
    current_uri = min(data_dict.keys(), key=lambda k: data_dict[k])
    return {
        'uri': current_uri,
        'text': rail.find_first_by_attr_and_get_attr(data, 'payRuleScript.uri', current_uri,
                                                     'payRuleScript.displayText', '') if (arg2 == 'payRuleScript') else
            rail.find_first_by_attr_and_get_attr(data, 'officeSchedule.uri', current_uri,
                                                 'officeSchedule.displayText', '')
    }


def is_dob_mismatch(dag_run):
    """Recipe node 49: does the stored 'Date of Birth' UDF differ from the incoming DOB?

    The recipe compares the RAW stored text (node 47/48 logger: pluck('text').first) against
    the incoming date, so a profile with no DOB recorded yields nil -> 'not equal' -> the DOB
    gets written. SK instead fed the blank straight into strptime, raising
    ValueError: time data '' does not match format '%Y/%m/%d' and killing the branch.
    An unparseable stored value is treated the same way (mismatch -> update), matching the
    recipe's update bias rather than failing the run."""
    stored_dob = rail.result('get_user_udf_values')['dob']
    if not stored_dob:
        return True
    try:
        return datetime.strptime(dag_run.conf['date_of_birth'], '%Y-%m-%d') != \
            datetime.strptime(stored_dob, '%Y/%m/%d')
    except ValueError:
        return True


def get_status_and_details_for_update(dag_run):
    """Recipe final log: aggregate Update status (Success/Exception) + details."""
    message = "Success"
    details = "No field updates received"
    has_log_entries = ','.join(list(map(lambda v: v['value'], rail.load_all_records(rail.result('write_log_entry')))))
    if has_log_entries:
        details = "User updated successfully" + ' ' + has_log_entries
    has_exception_message = ','.join(list(map(lambda v: v['value'], rail.load_all_records(rail.result('write_log_exception')))))
    if has_exception_message:
        message = "Exception"
        details = has_exception_message
        if has_log_entries:
            details = has_exception_message + ' ' + has_log_entries
    return {
        "userid": dag_run.conf['userid'],
        "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
        "action": "Update",
        "status": message,
        'details': details,
        "country": rail.result('get_country_lookup_variable')['value']
    }
