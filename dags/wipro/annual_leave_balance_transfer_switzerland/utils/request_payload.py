import rail
import json
from datetime import datetime
from wipro.annual_leave_balance_transfer_switzerland.utils.python_callable import get_split_date
null = None

def get_final_policyset(dag_run, config):
        get_user_timeoff_policysetschedule = rail.find_first_by_attr_and_get_attr(rail.result(
            "get_user_details")["timeoffpolicies"], 'timeOffType.uri', dag_run.conf['timeoff_type_uri_for_transferring_balance_into'], 'policySetSchedule', [])

        user_timeoff_policysetschedule = json.loads(json.dumps(get_user_timeoff_policysetschedule, ensure_ascii=False).replace('"null"', '"effective"').replace(
            '"script"', '"scriptTarget"'))
        default_policyset_for_0_offset = rail.find_first_by_attr_and_get_attr(rail.load_json_artifact(
            dag_run.conf['get_default_policy']), 'startOffset.offsetValue', 0, 'policySet')

        timeoff_type_data = rail.result('get_timeoff_type_and_balance_to_transfer')
        balance_to_transfer = float(timeoff_type_data['balance'])
        
        policyset_json = json.dumps(default_policyset_for_0_offset, ensure_ascii=False)
        
        policyset_to_add = json.loads(
            policyset_json
            .replace('"null"', '"effective"')
            .replace('"script"', '"scriptTarget"')
        )
        
        yearly_entitlement_value = balance_to_transfer if balance_to_transfer > 0 else 0
        starting_balance_script_with_0_balance = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": 0.0}})
        yearly_entitlement_script = json.dumps(
            {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": yearly_entitlement_value}})

        policyset_updated_json = json.dumps(policyset_to_add).replace(
            starting_balance_script_with_0_balance, yearly_entitlement_script)
        updated_policyset_to_add = json.loads(policyset_updated_json)

        user_timeoff_policysetschedule.append({
            "description": f"Annual Leave Transfer - {dag_run.conf['effective_date_for_new_policyset']}",
            "effectiveDate": get_split_date(dag_run.conf['effective_date_for_new_policyset'], 'int'),
            "policySet": updated_policyset_to_add
        })

        new_line_policyset = json.dumps(policyset_to_add)
        new_line_policyset_to_add = json.loads(new_line_policyset)
        user_timeoff_policysetschedule.append({
            "description": f"Annual Leave Transfer - {dag_run.conf['next_effective_date']}",
            "effectiveDate": get_split_date(dag_run.conf['next_effective_date'], 'int'),
            "policySet": new_line_policyset_to_add
        })

        return user_timeoff_policysetschedule

