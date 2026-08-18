from functools import lru_cache
from json import dumps, loads
import rail

null = None

def is_profile_enabled(dag_run):
    return dag_run.conf['mapper_data']['profile_status'].lower() == 'enabled'

def company_code_aues_fte_less_than_one_timeoff_name_starts_with_aus_annual_leave_test(dag_run, timeoff_name):
    return ((dag_run.conf['company_code'] == 'AUES') and (float(dag_run.conf['fte']) < 1) and (timeoff_name.startswith("[AUS] Annual Leave")))

def get_trigger_id_callable(dag_run, config):
    name:str = rail.result("for_each_all_assigned_timeoff_data")['name']
    if is_fte_based_timeoff_calculation_present_test(name, config):
        return config.workday_user_import_australia_users_aus_personal_carers_leave_timeoff_assignment_child_dag
    else:
        if company_code_aues_fte_less_than_one_timeoff_name_starts_with_aus_annual_leave_test(dag_run, name):
            return config.workday_user_import_australia_users_aus_annual_leave_parttime_timeoff_assignment_child_dag
        return config.workday_user_import_australia_users_aus_annual_leave_timeoff_assignment_child_dag


@lru_cache(maxsize=16)
def get_fte_based_timeoff_calculation_mapper_data(config):
    return list(filter(lambda row: row['Type']=='FTE Based Timeoff Calculation' and
                                    row['Function']=="Workday User Sync" and
                                    row['Country']=="Australia", config.MAPPER))

def is_fte_based_timeoff_calculation_present_test(to_name, config):
    res = list(filter(lambda row: row['Source']==to_name, get_fte_based_timeoff_calculation_mapper_data(config)))
    if res and res[0]['Value'] == "Yes":
        return True
    return False

def get_weekly_accrual_parameters(script_payload, fte):
    weekly_accrual_parameters = loads(dumps(rail.find_first_by_attr_and_get_attr(
            script_payload,
            'script.name',
            'Yearly Accrual',
            'additionalParameters',
            default=[]
        )).replace("[[{", "[{").replace("}]]","}]"))

    get_last_accrual_amt = list(filter(lambda row: row['keyUri'] == 'urn:replicon:script-key:parameter:accrual-annual-amount',
                                            weekly_accrual_parameters))
    if get_last_accrual_amt:
        get_last_accrual_amt = get_last_accrual_amt[-1]['value']['number']
    else:
        # workato treats null.to_f as 0 
        get_last_accrual_amt = null
    accrual_amt_based_on_schedule = float(0 if get_last_accrual_amt is null else get_last_accrual_amt) * float(fte if fte else 0)
    
    
    return weekly_accrual_parameters, accrual_amt_based_on_schedule, get_last_accrual_amt

def get_prevent_balance_overdraw(script_payload, fte):
    prevent_balance_overdraw = loads(dumps(rail.find_first_by_attr_and_get_attr(
        script_payload,
        'script.name',
        'Prevent balance overdraw',
        'additionalParameters'
    )).replace("[[{", "[{").replace("}]]","}]"))
    
    prevent_balance_overdraw_amount = rail.find_first_by_attr_and_get_attr(prevent_balance_overdraw, 'keyUri', 'urn:replicon:script-key:parameter:maximum-overdraw', 'number')
    if not prevent_balance_overdraw:
        prevent_balance_overdraw_amount = null

    prevent_balance_overdraw_amount_on_fte = float(prevent_balance_overdraw_amount if prevent_balance_overdraw_amount else 0) * float(fte if fte else 0)

    return prevent_balance_overdraw, prevent_balance_overdraw_amount, prevent_balance_overdraw_amount_on_fte


def is_caller_add_test(dag_run):
    return dag_run.conf['caller'] == 'Add'

def has_any_policy_to_assign_test(dag_run, add_task_id, update_task_id):
    if dag_run.conf['caller'] == 'Add':
        if rail.result(add_task_id):
            rail.set_result(key="policy_to_use", val=rail.result(add_task_id))
            return True
        return False
    if rail.result(update_task_id):
        rail.set_result(key="policy_to_use", val=rail.result(update_task_id))
        return True

    return False

