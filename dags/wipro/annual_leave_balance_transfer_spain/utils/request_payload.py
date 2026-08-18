import json
from math import ceil
import rail
from wipro.annual_leave_balance_transfer_spain.utils import python_callable

null = None

def round_up_to_next_half(number):
    return ceil(float(number) * 2) / 2

def get_final_policyset(dag_run):
        user_timeoff_policysetschedule = json.loads(json.dumps(rail.result("get_user_timeoff_policysetschedule"), ensure_ascii=False).replace('"null"', '"effective"').replace(
            '"script"', '"scriptTarget"'))
        default_policyset_for_0_offset = rail.find_first_by_attr_and_get_attr(rail.result(
            'get_default_policy_from_global_level'), 'startOffset.offsetValue', 0, 'policySet')

        starting_balance_script_with_0_balance = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": 0.0}})
        modified_script_with_required_starting_balance = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:amount", "value": {"number": round_up_to_next_half(dag_run.conf['balance_to_transfer'])}})

        policyset_json = json.dumps(
            default_policyset_for_0_offset, ensure_ascii=False)

        starting_expiry_date = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:expiry-date", "value": {}})
        modified_starting_expiry_date = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:expiry-date", "value": {"date":python_callable.get_expire_on_date()}})

        policyset_json = policyset_json.replace(
            starting_expiry_date, modified_starting_expiry_date)

        policyset_to_add = json.loads(policyset_json.replace(
            starting_balance_script_with_0_balance, modified_script_with_required_starting_balance).replace('"null"', '"effective"').replace(
            '"script"', '"scriptTarget"'))

        user_timeoff_policysetschedule.append({
            "description": "Effective on - " + dag_run.conf['efective_date_for_new_policyset'],
            "effectiveDate": python_callable.get_split_date(dag_run.conf['efective_date_for_new_policyset'], 'int'),
            "policySet": policyset_to_add
        })
        return user_timeoff_policysetschedule

def get_accrual_annual_amount(dag_run, default_yearly_entitlement, yearly_entitlement_mapper):

    if not yearly_entitlement_mapper:
        return {
            "number": default_yearly_entitlement
    }
    country = dag_run.conf['country'].lower()
    acquired_company = dag_run.conf['acquired_company']
    for company in (acquired_company, 'All'):
        for item in yearly_entitlement_mapper:
            if (item['country'].lower() == country and
                item['legal_entity_code'] == dag_run.conf['legal_entity_code'] and
                item['acquired_company'] == company):
                return {"number": item['annual_leave_entitlement']}
    return {
        "number": default_yearly_entitlement
    }

def get_final_policyset_for_annual_leave(dag_run, yearly_entitlement_mapper):
        user_timeoff_policysetschedule = json.loads(json.dumps(rail.result("get_user_timeoff_policysetschedule_for_annual_leave"), ensure_ascii=False).replace('"null"', '"effective"').replace(
            '"script"', '"scriptTarget"'))
        default_policyset_for_0_offset = rail.find_first_by_attr_and_get_attr(rail.result(
            'get_default_policy_from_global_level_for_annual_leave'), 'startOffset.offsetValue', 0, 'policySet')

        policyset_json = json.dumps(
            default_policyset_for_0_offset, ensure_ascii=False)
        
        default_yearly_entitlement = 0.0
        for item in default_policyset_for_0_offset['timeOffBalanceEventScripts']:
            if item['script']['name'] == 'ESP - Custom annual accrual rule':
                for value in item['additionalParameters']:
                    if value['keyUri']=="urn:replicon:script-key:parameter:accrual-annual-amount":
                        default_yearly_entitlement = float(value['value']['number'])
                        break

        starting_annual_amount = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": default_yearly_entitlement}})
        modified_starting_annual_amount = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": get_accrual_annual_amount(dag_run, default_yearly_entitlement, yearly_entitlement_mapper)})

        policyset_json = policyset_json.replace(
            starting_annual_amount, modified_starting_annual_amount)

        policyset_to_add = json.loads(policyset_json.replace('"null"', '"effective"').replace(
            '"script"', '"scriptTarget"'))

        user_timeoff_policysetschedule.append({
            "description": "Effective on - " + dag_run.conf['efective_date_for_new_policyset'],
            "effectiveDate": python_callable.get_split_date(dag_run.conf['efective_date_for_new_policyset'], 'int'),
            "policySet": policyset_to_add
        })

        return user_timeoff_policysetschedule


def get_report_parameters():
    filter_values = []
    pick_balance_from_uri = rail.result("get_required_timeoff_type_uris")['timeoff_uris_to_pick_balance_from']

    filter_values.append({
        "reportFilterUri": rail.result('get_required_filters')['as_of_date_filter_uri'],
        "value": "DateRange"
    })
    filter_values.append({
        "reportFilterUri": rail.result('get_required_filters')['as_of_date_filter_uri'],
        "value": rail.result('log_dag_run_date_and_time_details')['report_run_date']
    })
    filter_values.append({
        "reportFilterUri": rail.result('get_required_filters')['as_of_date_filter_uri'],
        "value": rail.result('log_dag_run_date_and_time_details')['report_run_date']
    })

    filter_values.append({
        "reportFilterUri": rail.result('get_required_filters')['timeoff_type_filter_uri'],
        "value": pick_balance_from_uri.split(":")[-1]
    })

    filter_values.append({
        "reportFilterUri": rail.result('get_required_filters')['country_service_centre_filter_uri'],
        "value": rail.result("get_required_country_service_center_uri").split(":")[-1]
    })

    report_filters = {
        "reportParameters": [
            {
                "reportUri": rail.result('get_report_details')['uri'],
                "filterValues": filter_values,
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        ]
    }
    return report_filters