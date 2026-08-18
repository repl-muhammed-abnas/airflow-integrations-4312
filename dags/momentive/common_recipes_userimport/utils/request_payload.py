# pylint: disable=line-too-long
import json
from datetime import datetime
import rail
from momentive.common_recipes_userimport.utils import python_callable

null = None


def _number_value(number):
    """Full Replicon parameter value object carrying a number (sent as a string)."""
    return {
        "uri": null, "slug": null, "bool": null, "date": null, "number": number,
        "text": null, "time": null, "calendarDayDurationValue": null,
        "workdayDurationValue": null, "dateRange": null, "collection": []
    }


def get_user_timeoff_policy_summary_payload(dag_run):
    """0-balance payout step 6: the user's time-off policy summary."""
    return {"userUri": dag_run.conf['useruri']}


def put_zero_balance_payout_payload(dag_run):
    """0-balance payout step 20: preserved past-dated policy entries + one new entry
    effective on the termination date that sets the (remaining/zero) starting balance.
    The caller (Disable / 0-balance-timeoff-update) computes and passes 'balance'.
    """
    eff = python_callable.split_date_string(dag_run.conf['terminationdate'], 'datetime')
    past_entries = rail.result('get_past_policyset_entries') or []
    balance = str(float(dag_run.conf['balance'])) if dag_run.conf.get('balance') else "0"
    new_entry = {
        "effectiveDate": eff,
        "description": f"Effective on {eff['month']}/{eff['day']}/{eff['year']}",
        "policySet": {
            "timeOffBalanceEventScripts": [
                {
                    "scriptTarget": {"uri": dag_run.conf['startingbalancesettouri'], "slug": null, "name": null},
                    "additionalParameters": [
                        {"keyUri": "urn:replicon:script-key:parameter:amount", "value": _number_value(balance)},
                        {"keyUri": "urn:replicon:script-key:parameter:precedence", "value": _number_value("20")},
                    ]
                }
            ],
            "timeOffValidationScripts": []
        }
    }
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuri']
        },
        "policySetScheduleEntries": past_entries + [new_entry]
    }


# ---------------------------------------------------------------------------
# Timeoff Add New User (other-countries) payloads - recipe flow 1435239.
# ---------------------------------------------------------------------------


def put_timeoff_type_assignments_payload(dag_run):
    """Recipe step 15: full-replace assignment of the collected time-off type uris."""
    return {
        "userUri": dag_run.conf['useruri'],
        "timeOffTypeUris": rail.result('build_assignment_list')['timeoff_type_uris'],
    }


def get_default_policy_schedule_payload(dag_run):
    """Recipe steps 23/36/44/.../103: GetDefaultTimeOffTypePolicyScheduleForUser
    for the current foreach time-off type."""
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result('foreach_timeofftype')['uri'],
        }
    }


def put_user_timeoff_policy_schedule_payload(dag_run):
    """Recipe Put for the current foreach time-off type with built entries."""
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result('foreach_timeofftype')['uri'],
        },
        "policySetScheduleEntries": rail.result('build_policy_entries'),
    }


def build_policy_entries(dag_run):
    """Compute the policySetScheduleEntries for the current foreach time-off type
    (delegates the per-branch math/transform to python_callable)."""
    item = rail.result('foreach_timeofftype')
    return python_callable.build_timeoff_policy_entries(
        item['name'],
        rail.result('get_default_policy_schedule'),
        dag_run,
        rail.result('get_all_scripts_for_monthly') if item['name'] == python_callable.KOR_MONTHLY_LEAVE else None,
    )


# ---------------------------------------------------------------------------
# Policy Assignment Rehire / Update-days (other-countries) payloads - flow 1435236.
# ---------------------------------------------------------------------------


def get_default_policyset_schedule_for_type_payload(dag_run):
    """Recipe step 16: GetDefaultTimeOffPolicySetScheduleForTimeOffType for the
    time-off type being re-anchored (returns the seniority-tier default schedule)."""
    return {"timeOffTypeUri": dag_run.conf['timeoffuri']}


def put_rehire_policy_schedule_payload(dag_run):
    """Recipe step 29: PutUserTimeOffAccountPolicySetSchedule with the preserved
    past entries plus the re-anchored seniority-tier entries."""
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuri'],
        },
        "policySetScheduleEntries": rail.result('build_rehire_policy_entries'),
    }


# ---------------------------------------------------------------------------
# Timeoff Add New User REHIRE (other-countries) payloads - recipe flow 1435238.
# Ported verbatim from the South Korea implementation (same recipe).
# ---------------------------------------------------------------------------


def get_datetime_obj(effectivedate):
    """A 'YYYY-MM-DD' string -> {year, month, day} effectiveDate object."""
    effective_date = datetime.strptime(effectivedate, '%Y-%m-%d')
    return {
        "year": effective_date.year,
        "month": effective_date.month,
        "day": effective_date.day
    }


def get_default_timeofftype_policy_sched_payload(dag_run):
    """GetDefaultTimeOffTypePolicyScheduleForUser for the (single) time-off type."""
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuri'] if 'timeoffuri' in dag_run.conf else rail.result('foreach_timeoffuri')['uri']
        }
    }


def get_user_timeoff_policy_payload(dag_run):
    """Generic-branch Put (recipe step 12): re-apply the transformed default schedule."""
    return {
        "timeOffAccount": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": dag_run.conf['timeoffuri']
        },
        "policySetScheduleEntries": json.loads(rail.result('get_default_time_off_type_policy_schedule_for_user'))
    }


def add_to_policy_38():
    """KOR Annual entry (recipe step 37): effective Jan 1 (this year if yos>0.99 else next)."""
    yr = str(int(datetime.now().year) + 1)
    if float(rail.result('get_years_of_service')) > 0.99:
        yr = str(datetime.now().year)
    return {
        'description': "Policy effective from 01/01/" + yr,
        'effectiveDate': {'day': '01', 'month': '01', 'year': yr},
        'policySet': rail.result('log_new_policy_to_assign_36')
    }


def add_to_policy_68():
    """KOR Monthly - current calendar year full-accrual entry (recipe step 67)."""
    return {
        'description': "Policy effective from 01/01/" + str(datetime.now().year),
        'effectiveDate': {'day': '01', 'month': '01', 'year': str(datetime.now().year)},
        'policySet': rail.result('log_new_policy_to_assign_62')
    }


def add_to_policy_72():
    """KOR Monthly - year+2 no-accrual reset entry, prior-year-hire branch (recipe step 71)."""
    return {
        'description': "Policy effective from 01/01/" + str(int(datetime.now().year) + 2),
        'effectiveDate': {'day': '01', 'month': '01', 'year': str(int(datetime.now().year) + 2)},
        'policySet': rail.result('log_3rd_yr_policyset')
    }


def add_to_policy_81(dag_run):
    """KOR Monthly - 1st-year entry effective on the start date, Jan-1 start branch (recipe step 80)."""
    return {
        'description': "Policy effective from" + dag_run.conf['startdate'],
        'effectiveDate': get_datetime_obj(dag_run.conf['startdate']),
        'policySet': rail.result('log_new_policy_to_assign_79')
    }


def add_to_policy_87(dag_run):
    """KOR Monthly - 1st-year entry effective on the start date, mid-year start branch (recipe step 86)."""
    return {
        'description': "Policy effective from" + dag_run.conf['startdate'],
        'effectiveDate': get_datetime_obj(dag_run.conf['startdate']),
        'policySet': rail.result('log_new_policy_to_assign_85')
    }


def add_to_policy_92():
    """KOR Monthly - year+1 entry, mid-year start branch (recipe step 91)."""
    return {
        'description': "Policy effective from 01/01/" + str(int(datetime.now().year) + 1),
        'effectiveDate': {'day': '01', 'month': '01', 'year': str(int(datetime.now().year) + 1)},
        'policySet': rail.result('log_new_policy_to_assign_90')
    }


def add_to_policy_97():
    """KOR Monthly - year+2 no-accrual reset entry, mid-year start branch (recipe step 96)."""
    return {
        'description': "Policy effective from 01/01/" + str(int(datetime.now().year) + 2),
        'effectiveDate': {'day': '01', 'month': '01', 'year': str(int(datetime.now().year) + 2)},
        'policySet': rail.result('log_3rd_yr_policyset_95')
    }


def add_to_noaccrualpolicy_94():
    """KOR Monthly - the "Starting Balance Set To 0 + Prevent overdraw 0" reset shell
    (recipe steps 68/93), with script uris resolved from GetAllScripts."""
    return {
        'policySet': {
            'timeOffBalanceEventScripts': {
                'additionalParameters': {
                    'keyUri': 'urn:replicon:script-key:parameter:amount',
                    'value': {'number': 0}
                },
                'script': {
                    'description': 'Set initial balance for the first day of a policy',
                    'name': 'Starting Balance Set To',
                    'uri': rail.result('get_timeoffbalance_event_script_administration_service')['startring_balance']
                }
            },
            'timeOffValidationScripts': {
                'additionalParameters': {
                    'keyUri': 'urn:replicon:script-key:parameter:maximum-overdraw',
                    'value': {'number': 0}
                },
                'script': {
                    'description': "Do not allow the user's time off balance to go below the overdraw threshold",
                    'name': 'Prevent balance overdraw',
                    'uri': rail.result('get_all_scripts_timeOff_validation_script')['prevent_bal']
                }
            }
        }
    }


# ---------------------------------------------------------------------------
# Supervisor assignment (other-countries) payloads - recipe supervisor-assignment_v2.
# Ported verbatim from the South Korea implementation (same recipe).
# ---------------------------------------------------------------------------


def get_manager_details_payload():
    """Recipe: BulkGetUsers3 for the found supervisor (enabled check)."""
    return {
        "users": [
            {
                "uri": rail.result('search_for_user_with_empid')[0]['uri']
            }
        ],
        "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
    }


def add_missing_supervisor_permission_payload():
    """Recipe node 22: assign the 'Supervisor - Edit' permission set to the supervisor."""
    return {
        'userUri': rail.result('search_for_user_with_empid')[0]['uri'],
        'permissionSetUri': rail.result('get_all_permissionsets')['supervisor']
    }


def create_supervisor_payload(dag_run):
    """Recipe node 33: create a foreign supervisor (SSO login=email, Momentive dept
    group + 'Foreign Supervisors' employee-type group, Supervisor - Edit perm).
    Codebase uses PutUser3 (recipe's literal call is PutUser2)."""
    return {
        "user": {
            "target": {
                "loginName": dag_run.conf['sup_email']
            },
            "firstname": dag_run.conf['sup_firstname'],
            "lastname": dag_run.conf['sup_lastname'],
            "emailAddress": dag_run.conf['sup_email'],
            "employeeId": dag_run.conf['managerid'],
            "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
            "employmentDateRange": {
                "startDate": get_datetime_obj(dag_run.conf['sup_change_effective_date'])
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['sup_email'],
                "SSOName": dag_run.conf['sup_email'],
                "password": "Replicon@12"
            },
            "permissionSets": [
                {
                    "uri": rail.result('get_all_permissionsets')['supervisor']
                }
            ],
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                        "name": "Momentive",
                    }
                }
            ],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "name": "Foreign Supervisors",
                    }
                }
            ]
        }
    }


# ---------------------------------------------------------------------------
# 0-balance for timeoff update (other-countries) payloads - recipe flow 1435235.
# Ported from the South Korea implementation (same recipe).
# ---------------------------------------------------------------------------


def get_balancesummary_foraccount(dag_run):
    """Recipe node 8: GetBalanceSummaryForAccount as of today for the current
    foreach time-off type (result is not consumed downstream; balance is forced to 0)."""
    effective_date = datetime.now().date()
    return {
        "account": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result('foreach_policiesby_timeofftype')['timeOffType']['uri']
        },
        "asOfDate": {"year": effective_date.year, "month": effective_date.month, "day": effective_date.day}
    }


def put_remaining_balance_for_payout_parameter(dag_run):
    """Recipe node 11: conf for the put-0-balance-for-payout child. terminationdate is
    today; balance forced to 0. NOTE: SK passed terminationdate as int-concatenated
    'd/m/y' (a TypeError bug); here it is a 'YYYY-MM-DD' string to both fix the bug and
    match the format the common payout child (put_zero_balance_for_payout_child) parses."""
    effective_date = datetime.now().date()
    return {
        'timeoffuri': rail.result('foreach_policiesby_timeofftype')['timeOffType']['uri'],
        'useruri': dag_run.conf['useruri'],
        'terminationdate': str(effective_date),
        'startingbalancesettouri': rail.result('get_all_scripts'),
        'balance': 0
    }


# ---------------------------------------------------------------------------
# Update user timeoff assign (other-countries) fan-out conf builders -
# recipe update-user-time-off_assign_v2.
# ---------------------------------------------------------------------------


def trigger_child_0_balance_timeoff_payload(item, dag_run):
    """Conf for the 0-balance-for-timeoff-update child (1435235). item is a time-off uri."""
    return {
        "userid": dag_run.conf['useruri'],
        "hiredate": dag_run.conf['hiredate'],
        "terminationdate": dag_run.conf['terminationdate'],
        "active": dag_run.conf['active'],
        "useruri": dag_run.conf['useruri'],
        "timeoffupdate": 'yes',
        "timeoffuri": item
    }


def trigger_timeoff_add_rehire_payload(item, dag_run):
    """Conf for the timeoff-add-new-user-rehire child (1435238). item is {name, uri}.
    FIX vs SK: the type-name key is 'timeofftypes' (SK had the typo 'tiemofftypes', which
    left the rehire child's KOR gate unmatched)."""
    return {
        "loginname": dag_run.conf['useruri'],
        "startdate": dag_run.conf['hiredate'],
        "terminationdate": dag_run.conf['terminationdate'],
        "active": dag_run.conf['active'],
        "useruri": dag_run.conf['useruri'],
        "timeofftypes": item['name'],
        "continuous_service_date": dag_run.conf.get('continuousservicedate', ''),
        "rehire": dag_run.conf['rehire'],
        "timeoffuri": item['uri'],
    }


def trigger_bel_policy_rehire_payload(item, dag_run):
    """Recipe node 47: conf for the BEL policy update_rehire child (1362490 - STUB)."""
    return {
        "useruri": dag_run.conf['useruri'],
        "startdate": dag_run.conf['hiredate'],
        "timeoffuri": item['uri'],
        "username": dag_run.conf['useruri'],
    }


# ---------------------------------------------------------------------------
# Disable user (other-countries) payloads - recipe child_workflow-to-disable-user.
# ---------------------------------------------------------------------------


def update_emp_date_for_disableuser(dag_run):
    """Recipe node 11: UpdateEmploymentDateRange (start=hire, end=termination)."""
    hire = datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d')
    term = datetime.strptime(dag_run.conf['terminationdate'], '%Y-%m-%d')
    return {
        "userUri": dag_run.conf['useruri'],
        "dateRange": {
            "startDate": {"year": hire.year, "month": hire.month, "day": hire.day},
            "endDate": {"year": term.year, "month": term.month, "day": term.day}
        }
    }


def disable_balance_summary_payload(dag_run):
    """Recipe node 17: GetBalanceSummaryForAccount as of the TERMINATION date (FIX vs SK,
    which used today) for the current foreach time-off type."""
    eff = datetime.strptime(dag_run.conf['terminationdate'], '%Y-%m-%d')
    return {
        "account": {
            "userUri": dag_run.conf['useruri'],
            "timeOffTypeUri": rail.result('foreach_policiesby_timeofftype')['timeOffType']['uri']
        },
        "asOfDate": {"year": eff.year, "month": eff.month, "day": eff.day}
    }


def disable_payout_annual_payload(dag_run):
    """Recipe node 21: payout child conf for annual-leave types (balance = remaining).
    terminationdate is the 'YYYY-MM-DD' Termination_Date (FIX vs SK's int-concat 'd/m/y'
    TypeError); the common payout child parses 'YYYY-MM-DD'.
    balance is passed through UNTRUNCATED (review decision; Japan does the same): the
    recipe's `.to_i` would drop fractional days (15.5 -> 15) and understate the payout;
    the payout child normalizes via str(float(balance))."""
    return {
        'timeoffuri': rail.result('foreach_policiesby_timeofftype')['timeOffType']['uri'],
        'useruri': dag_run.conf['useruri'],
        'terminationdate': dag_run.conf['terminationdate'],
        'startingbalancesettouri': rail.result('get_all_scripts'),
        'balance': rail.result('get_balance_summary_foraccount')
    }


def disable_payout_zero_payload(dag_run):
    """Recipe node 23: payout child conf for non-annual types (balance = 0)."""
    return {
        'timeoffuri': rail.result('foreach_policiesby_timeofftype')['timeOffType']['uri'],
        'useruri': dag_run.conf['useruri'],
        'terminationdate': dag_run.conf['terminationdate'],
        'startingbalancesettouri': rail.result('get_all_scripts'),
        'balance': 0
    }


def log_user_disable_payload(dag_run):
    """Recipe nodes 26/28: the user-import log entry for the disable outcome.
    NOTE (recipe divergence, same as SK): status is a static 'Success'; the recipe derives
    Error when a payout child reports an error. Hard payout failures still surface via the
    one_failed catch, but soft errors returned by the payout child are not reflected here."""
    if dag_run.conf['terminationdate']:
        details = "User profile disabled successfully with end date ;"
    else:
        details = "User profile disabled successfully however no end date was received ;"
    return {
        "userid": dag_run.conf['userid'],
        "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
        "action": "Disable user",
        "status": "Success",
        'details': details,
        'country': ''
    }


# ---------------------------------------------------------------------------
# Add user (other-countries) payloads - recipe user-sync-add_v3. Ported from SK.
# ---------------------------------------------------------------------------


def effective_dateformat_payload(effective_date):
    """{year, month, day} from a datetime object."""
    return {"year": effective_date.year, "month": effective_date.month, "day": effective_date.day}


def get_timesheetperiod_val_92(dag_run):
    """Recipe node 92: Monthly timesheet period effective on hire date."""
    hiredate = datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d')
    return [
        {
            "timesheetPeriod": {"uri": None, "name": 'Monthly'},
            "effectiveDate": effective_dateformat_payload(hiredate)
        }
    ]


def get_timesheetperiod_val_109(dag_run):
    """Recipe node 109: Korea_Weekly Timesheet period effective on hire date."""
    hiredate = datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d')
    return [
        {
            "timesheetPeriod": {"uri": None, "name": 'Korea_Weekly Timesheet'},
            "effectiveDate": effective_dateformat_payload(hiredate)
        }
    ]


def _blank_to_none(value):
    """Replicon Import/Put bodies reject empty strings ('') where they expect a URI/name
    or null (e.g. InvalidPolicySetTargetParameterError when a policy set name is '').
    Recursively convert every '' to None across the assembled payload. Countries whose
    mapper omits a category (e.g. France has no Timesheet approval path / Timesheet
    Template) leave those variables at their '' default, which this normalizes to null."""
    if isinstance(value, dict):
        return {key: _blank_to_none(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_blank_to_none(item) for item in value]
    return None if value == '' else value


def create_user_payload(dag_run):
    """Recipe node 117: PutUser3 body (mapper-driven schedules/groups/policy sets; SSO login).
    Codebase uses PutUser3 (recipe's literal call is PutUser2)."""
    hiredate = datetime.strptime(dag_run.conf['hiredate'], '%Y-%m-%d')

    # Drop policy sets without a name — e.g. France has no Timesheet Template mapped, so the
    # recipe builds {"name": "", "uri": None}, which Replicon rejects.
    policy_sets = rail.result('get_policyset_variable')['value']
    if isinstance(policy_sets, list):
        policy_sets = [ps for ps in policy_sets if isinstance(ps, dict) and (ps.get('name') or ps.get('uri'))]

    payload = {
        "user": {
            "target": {
                "loginName": dag_run.conf['userid']
            },
            "firstname": dag_run.conf['firstname'],
            "lastname": dag_run.conf['lastname'],
            "emailAddress": dag_run.conf['emailaddress'],
            "employeeId": dag_run.conf['workerreferenceemployeeid'],
            "schedulePolicySchedule": rail.result('get_schedule_variable')['value'],
            "workWeekStartDayUri": rail.result('usermappings_mapper')['workweek'],
            "employmentDateRange": {
                "startDate": effective_dateformat_payload(hiredate)
            },
            "securityConfiguration": {
                "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                ],
                "isLoginEnabled": "true",
                "loginName": dag_run.conf['userid'],
                "SSOName": dag_run.conf['userid']
            },
            "holidayCalendar": rail.result('get_holidaycalendar_variable')['value'],
            "permissionSets": [
                {
                    "uri": rail.result('get_all_permissionsets')['basic_user_with_report_uri']
                }
            ],
            "policySets": policy_sets,
            "timesheetApprovalPath": rail.result('get_timesheetapprovalpath_variable')['value'],
            "timeOffApprovalPath": rail.result('get_timeoffapprovalpath_variable')['value'],
            "timeZone": {
                "IANAName": rail.result('usermappings_mapper')['timezone']
            },
            "divisionSchedule": rail.result('get_legalentity_division_variable')['value'],
            "costCenterSchedule": rail.result('get_costcenter_variable')['value'],
            "serviceCenterSchedule": rail.result('get_paygrp_srvcenter_variable')['value'],
            "departmentGroupSchedule": [
                {
                    "departmentGroup": {
                        "uri": dag_run.conf['departmentgroupuri']
                    }
                }
            ],
            "employeeTypeGroupSchedule": [
                {
                    "employeeTypeGroup": {
                        "uri": rail.result('get_required_employeetype_uri')
                    }
                }
            ],
            "timesheetPeriodSchedule": rail.result('get_timesheetperiod_variable')['value'],
            "payRuleScriptSchedule": rail.result('get_payrule_variable')['value']
        }
    }

    # Replicon rejects '' anywhere it expects a uri/name/null — normalize the whole body.
    return _blank_to_none(payload)


def trigger_timeoff_addnew_user(dag_run):
    """Recipe node 197: conf for the timeoff-add-new-user child (1435239)."""
    return {
        "loginname": dag_run.conf['userid'],
        "startdate": dag_run.conf['hiredate'],
        "useruri": rail.result('create_user')['uri'],
        "terminationdate": dag_run.conf['terminationdate'],
        "active": dag_run.conf['active'],
        "timeofftypes": rail.result('usermappings_mapper')['timeoffs'],
        "continous_service_date": dag_run.conf['continous_service_date'] if dag_run.conf['continous_service_date'] else '',
        "rehire": 'add'
    }


def assign_policydataaccessscope_department(dag_run):
    """Recipe node 119: PutPolicyDataAccessScopesForUser (time-off scope = department group)."""
    return {
        "userUri": dag_run.conf['useruri'] if 'useruri' in dag_run.conf else rail.result('create_user')['uri'],
        "policyDataAccessScopes": [{
            "policyUri": "urn:replicon:policy:time-off",
            "departmentGroups": [{
                "departmentGroup": {
                    "uri": dag_run.conf['departmentgroupuri']
                }
            }]
        }]
    }


def supervisor_assignment_log_payload(dag_run):
    """Supervisor-assignment fan-out entry deferred by add/update for the master to trigger.
    FIX vs SK: reads the lowercase conf keys the master actually sends
    (cf_lrv_manager_* / effective_date_of_manager_change); SK read capitalized names that
    are never in conf (latent KeyError on every add/update with a manager)."""
    return {
        "loginid": dag_run.conf['userid'],
        "supervisorempid": dag_run.conf['managerid'],
        "useruri": dag_run.conf['useruri'] if 'useruri' in dag_run.conf else rail.result('create_user')['uri'],
        'type': "update" if 'useruri' in dag_run.conf else "add",
        "sup_email": dag_run.conf['cf_lrv_manager_email'] if dag_run.conf.get('cf_lrv_manager_email') else '',
        "sup_firstname": dag_run.conf['cf_lrv_manager_first_name'] if dag_run.conf.get('cf_lrv_manager_first_name') else '',
        "sup_lastname": dag_run.conf['cf_lrv_manager_last_name'] if dag_run.conf.get('cf_lrv_manager_last_name') else '',
        "sup_change_effective_date": dag_run.conf['effective_date_of_manager_change']
            if dag_run.conf.get('effective_date_of_manager_change') else str(datetime.strftime(datetime.now().date(), '%Y-%m-%d')),
        # The log artifact holding THIS user's Add/Update entry. The supervisor child filters
        # that entry out of `conf.logger` and rewrites it with the supervisor outcome, so a
        # master that gives each user its own log (UAE) must tell the child where to look;
        # masters that share one artifact simply pass their own back.
        "logger": dag_run.conf['logger'],
    }


def search_supervisor_payload():
    """UserListService GetData columns for the supervisor employee-id search."""
    return {
        "page": "1",
        "pagesize": "10000",
        "columnUris": [
            "urn:replicon:user-list-column:employee-id",
            "urn:replicon:user-list-column:login-name"
        ]
    }


# ---------------------------------------------------------------------------
# Update user (other-countries) payloads - recipe user-sync-update_v3. Ported from SK.
# ---------------------------------------------------------------------------


def get_data_sup_emp_grp_dept_grp(dag_run):
    """UserList GetData (department-group / employee-type-group / supervisor columns) for the user."""
    return {
        "page": "1",
        "pagesize": "100000",
        "columnUris": [
            "urn:replicon:user-list-column:department-group",
            "urn:replicon:user-list-column:employee-type-group",
            "urn:replicon:user-list-column:supervisor"
        ],
        "sort": [],
        "filterExpression": {
            "leftExpression": {"filterDefinitionUri": "urn:replicon:user-list-filter:user"},
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {"value": {"uri": dag_run.conf['useruri']}}
        }
    }


def get_current_supervisorempid():
    """UserList GetData (employee-id) for the user's current supervisor."""
    return {
        "page": "1",
        "pagesize": "1000",
        "columnUris": ["urn:replicon:user-list-column:employee-id"],
        "filterExpression": {
            "leftExpression": {"filterDefinitionUri": "urn:replicon:user-list-filter:user"},
            "operatorUri": "urn:replicon:filter-operator:equal",
            "rightExpression": {"value": {"uri": rail.result('getdata_sup_emp_grp_dept_grp')['rows'][0]['cells'][2]['uri']}}
        }
    }


def update_employeetypegrp_payload(dag_run):
    """ApplyUserModifications2: update employee-type group schedule (effective today)."""
    return {
        "user": {"uri": dag_run.conf['useruri']},
        "modifications": {
            "employeeTypeGroupScheduleToApply": {
                "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementEmployeeTypeGroupSchedule": [],
                "updateEmployeeTypeGroupScheduleOverDateRange": {
                    "replacementEmployeeTypeGroupScheduleEntries": [
                        {
                            "employeeTypeGroup": {"uri": rail.result('get_all_employee_type')},
                            "effectiveDate": effective_dateformat_payload(datetime.now())
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def payrule_update_payload(dag_run):
    """ApplyUserModifications2: update pay-rule schedule (effective next timesheet start / today)."""
    return {
        "user": {"uri": dag_run.conf['useruri']},
        "modifications": {
            "payRulesScheduleModifications": {
                "scheduleEntries": [
                    {
                        "payRuleScript": {"uri": rail.result('get_req_payrule_script')},
                        "effectiveDate": get_datetime_obj(rail.result('get_startdate_of_next_timesheet')) if rail.result(
                            'get_startdate_of_next_timesheet') else get_datetime_obj(str(datetime.now().date()))
                    }
                ]
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def update_servicecenter_payload(dag_run):
    """ApplyUserModifications2: update service-center (paygroup) schedule (effective today)."""
    return {
        "user": {"uri": dag_run.conf['useruri']},
        "modifications": {
            "serviceCenterScheduleToApply": {
                "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementServiceCenterSchedule": [],
                "updateServiceCenterScheduleOverDateRange": {
                    "replacementServiceCenterScheduleEntries": [
                        {
                            "serviceCenter": {"uri": dag_run.conf['paygroupuri']},
                            "effectiveDate": effective_dateformat_payload(datetime.now())
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_update_costcenter_param(dag_run):
    """ApplyUserModifications2: update cost-center schedule (effective change date / today)."""
    return {
        "user": {"uri": dag_run.conf['useruri']},
        "modifications": {
            "costCenterScheduleToApply": {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementCostCenterSchedule": [],
                "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                        {
                            "costCenter": {"uri": dag_run.conf['costcenteruri']},
                            "effectiveDate": get_datetime_obj(dag_run.conf['worker_cc_change_date']) if dag_run.conf['worker_cc_change_date']
                                else effective_dateformat_payload(datetime.now())
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def apply_user_modifications_division(dag_run):
    """ApplyUserModifications2: update division (legal entity) schedule (effective today).
    FIX vs SK: 'uri': dag_run.conmf['useruri'] (a typo) -> dag_run.conf['useruri']."""
    return {
        "user": {"uri": dag_run.conf['useruri']},
        "modifications": {
            "divisionScheduleToApply": {
                "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDivisionSchedule": [],
                "updateDivisionScheduleOverDateRange": {
                    "replacementDivisionScheduleEntries": [{
                        "division": {"uri": dag_run.conf['legalentityuri']},
                        "effectiveDate": effective_dateformat_payload(datetime.now())
                    }]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def department_update_payload(dag_run):
    """ApplyUserModifications2: update department-group schedule (effective location-change date / today)."""
    return {
        "user": {"uri": dag_run.conf['useruri']},
        "modifications": {
            "departmentGroupScheduleToApply": {
                "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementDepartmentGroupSchedule": [],
                "updateDepartmentGroupScheduleOverDateRange": {
                    "replacementDepartmentGroupScheduleEntries": [
                        {
                            "departmentGroup": {"uri": dag_run.conf['departmentgroupuri']},
                            # FIX vs SK: masters send 'location_change_eff_date' (SK read the
                            # capitalized report column name, never present in conf).
                            "effectiveDate": get_datetime_obj(dag_run.conf['location_change_eff_date'])
                                if dag_run.conf.get('location_change_eff_date') else effective_dateformat_payload(datetime.now())
                        }
                    ]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def schedule_update_payload(dag_run):
    """ApplyUserModifications2: update schedule policy (office schedule / Shift) (effective shift-change date / today)."""
    return {
        "user": {"uri": dag_run.conf['useruri']},
        "modifications": {
            "schedulePolicyToApply": {
                "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "updateScheduleOverDateRange": {
                    "replacementScheduleEntries": [{
                        "schedulePolicy": {
                            "officeScheduleUri": None if rail.result('usermappings_mapper')['schedule'] == 'Shift' else rail.result('get_req_schedule_script'),
                            "officeSchedule": {
                                "officeScheduleUri": None if rail.result('usermappings_mapper')['schedule'] == 'Shift' else rail.result('get_req_schedule_script'),
                            },
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        },
                        "effectiveDate": get_datetime_obj(dag_run.conf['work_shift_change_effective_date'])
                            if dag_run.conf['work_shift_change_effective_date'] else effective_dateformat_payload(datetime.now())
                    }]
                }
            }
        },
        "userModificationOptionUri": "urn:replicon:user-modification-option:save"
    }


def get_timesheet_for_date2_payload(dag_run):
    """GetTimesheetForDate2 (create-if-necessary) as of today, for the next-timesheet lookups."""
    todays_date = datetime.now()
    return {
        "userUri": dag_run.conf['useruri'],
        "date": {"day": todays_date.day, "month": todays_date.month, "year": todays_date.year},
        "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
    }


def trigger_updateuser_timeoff(dag_run):
    """Recipe node 309: conf for the update_user_timeoff_assign hub (1435229).
    FIX vs SK: keys renamed to what the common hub actually reads - 'oldstartdate'
    (SK sent 'old_startdate') and 'continuousservicedate' (SK sent 'continous_service_date'),
    which in SK mismatched the hub's conf reads (latent KeyError)."""
    strt_date = rail.result('get_user_data')[0]['userDetails']['employmentDateRange']['startDate']
    return {
        "hiredate": dag_run.conf['hiredate'],
        "terminationdate": dag_run.conf['terminationdate'],
        "active": dag_run.conf['active'],
        "rehire": dag_run.conf['rehireupdate'],
        "timeofftypes": rail.result('usermappings_mapper')['timeoffs'],
        "continuousservicedate": dag_run.conf['continous_service_date'],
        "timeoff_service_date": dag_run.conf['timeoff_service_date'] if dag_run.conf['timeoff_service_date'] else dag_run.conf['continous_service_date'],
        "oldstartdate": str(strt_date['year']) + '-' + str(strt_date['month']) + '-' + str(strt_date['day']),
        "useruri": dag_run.conf['useruri'],
    }
