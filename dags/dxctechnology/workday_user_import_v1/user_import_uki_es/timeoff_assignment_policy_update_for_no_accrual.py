from json import dumps, loads
from typing import Optional
from pendulum import datetime
from dateutil.parser import parse as date_parser
from datetime import timedelta,  datetime as dt
import pendulum
import rail
from airflow.models import Variable
from airflow.exceptions import AirflowException
from dxctechnology.workday_user_import_v1.user_import_uki_es.utils.custom_methods import (
    get_timeoff_polices_to_assign_callable,
    format_timeoff_polices_to_assign_callable,
    get_annual_brought_sold_holiday_leave_list
)

from dxctechnology.workday_user_import_v1.user_import_uki_es.utils.request_payload import (
    get_update_policy_payload,
    get_user_timeoff_balance_summary_payload, convert_json_date_to_date,
    INPUT_DATE_FORMAT
)


def create_dag(config):
    _dags = []
    for batch_index in range(1, config.DAG_BATCH_COUNT + 1):
        prefix = f"_{batch_index}"
        if batch_index == 1:
            prefix = ""
        with rail.create_airflow_dag(
            dag_id=f"{config.workday_user_import_uki_es_process_time_off_accrual_dag}{prefix}",
            description="UK&I CSC timeoff assignment policy update for no accrual",
            replicon_conn_id=config.replicon_conn_id,
            company_key=config.company_key,
            start_date=datetime(2024, 1, 1),
            max_active_runs=10
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

            can_run_batch_task = rail.IfOperator(
                task_id = "can_run_batch_task",
                test=lambda: Variable.get(
                config.can_run_batch_task_var_name_uki_es, default_var='true').lower() == 'true',
                yes_task="batch_task",
                no_task="is_end_date_present"
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id = "batch_task",
                start_task="is_end_date_present",
                end_task="catch_errors",
                execution_timeout=timedelta(days=14)
            )

            is_end_date_present = rail.IfOperator(
                task_id="is_end_date_present",
                test=lambda dag_run: bool(
                    dag_run.conf['user_end_date_json'].get('year', False)),
                yes_task="if_country_is_uk_or_ireland"
            )

            """
                Validate if the country is UKI
                - Validate if the timeoff is [UK] Annual Leave OR / [UK] P/T Annual Leave Hrs
                - Get Annual Leave, Bought, Sold timeoff details
                - Calc the balance
                - Add the balance as starting balance to Annual leave
                - Add the 0 Balance as starting balance to Bought and Sold
            
            """

            if_country_is_uk_or_ireland = rail.IfOperator(
                task_id = "if_country_is_uk_or_ireland",
                test = lambda dag_run: dag_run.conf['file_data']['country'].lower() in ['united kingdom', 'ireland'],
                yes_task="process_uki_logic",
                no_task="get_timeoff_details_dummy"
            )

            get_timeoff_details_dummy = rail.EmptyOperator(
                task_id = "get_timeoff_details_dummy"
            )

            process_uki_logic = rail.EmptyOperator(
                task_id = "process_uki_logic"
            )

            is_timeoff_bought_or_sold = rail.IfOperator(
                task_id = "is_timeoff_bought_or_sold",
                test=lambda dag_run: dag_run.conf['timeoff_type_name'] in ['[UK] Bought A/L', '[UK] P/T Bought A/L Hrs', '[IRL] Bought A/L', '[IRL] P/T Bought A/L Hrs', '[IRL] P/T Sold A/L Hrs', '[IRL] Sold A/L', '[UK] P/T Sold A/L Hrs', '[UK] Sold A/L', '[UK] Public Holiday', '[IRL] Public Holiday'],
                yes_task = "skip_processing",
                no_task = "is_timeoff_annual_leave_dummy"
            )


            """
                Ideally the policy line will be added when the Time-Off Annual timeoff is processed for UK & Ireland 
            """
            skip_processing = rail.EmptyOperator(
                task_id = "skip_processing"
            )

            is_timeoff_annual_leave_dummy = rail.EmptyOperator(
                task_id = "is_timeoff_annual_leave_dummy",
            )

            is_timeoff_annual_leave = rail.IfOperator(
                task_id = "is_timeoff_annual_leave",
                test = lambda dag_run: dag_run.conf['timeoff_type_name'] in ['[UK] Annual Leave', '[UK] P/T Annual Leave Hrs', '[IRL] Annual Leave', '[IRL] P/T Annual Leave Hrs'],
                yes_task = "process_uki_annual_leave_specific_logic",
                no_task = "is_timeoff_annual_leave_no_task_get_timeoff_details"
            )

            is_timeoff_annual_leave_no_task_get_timeoff_details = rail.EmptyOperator(
                task_id = "is_timeoff_annual_leave_no_task_get_timeoff_details"
            )

            process_uki_annual_leave_specific_logic = rail.EmptyOperator(
                task_id = "process_uki_annual_leave_specific_logic"
            )

            def get_two_date_diff(effective_date, user_end_date):
                if effective_date:
                    return convert_json_date_to_date(user_end_date) - convert_json_date_to_date(effective_date)

            def get_annual_bought_sold_leave_policy_summary_data_handler(response, dag_run):
                # return response

                timeoff_and_policies = response['policiesByTimeOffType']
                rail.set_result(key="service_call_resp_policies", val = timeoff_and_policies)
                res_list = []

                user_end_date = dag_run.conf['user_end_date_json']

                is_uk, is_irl = dag_run.conf['file_data']['country'].lower() == "united kingdom", dag_run.conf['file_data']['country'].lower() == "ireland"

                if not is_uk and not is_irl:
                    raise AirflowException("Action should not be performed, as its only for UK and IRL")

                is_user_part_time = dag_run.conf['file_data']['fte_pct'] != '100'
                timeoff_to_use = get_annual_brought_sold_holiday_leave_list(
                    return_for="country",
                    country="UK" if is_uk else "IRL",
                    part_time_full_time="parttime" if is_user_part_time else "fulltime"
                )

                for each_timeoff in timeoff_and_policies:
                    if each_timeoff['timeOffType']['name'] in timeoff_to_use and each_timeoff['isTimeOffAllowedAgainstThisTimeOffType']:
                        res_list.append(
                            each_timeoff
                        )

                # return res_list
                def get_effective_policies(timeoff_policy):
                    # Filter out future schedules and find the one with minimum day difference
                    valid_schedules = [
                        (timeoff, get_two_date_diff(timeoff['effectiveDate'], user_end_date))
                            for timeoff in timeoff_policy
                                if get_two_date_diff(timeoff['effectiveDate'], user_end_date).days >= 0
                    ]
                    if not valid_schedules:
                        return None
                    # Return the schedule with the smallest day difference (most recent)
                    return min(valid_schedules, key=lambda x: x[1])[0]

                # return res_list
                res = dict()
                full_policy_res = dict()
                for timeoff in res_list:
                    full_policy_res[timeoff['timeOffType']['name']] = timeoff['policySetSchedule']
                    effective_policy = get_effective_policies(timeoff['policySetSchedule'])
                    if not effective_policy:
                        res[timeoff['timeOffType']['name']] = 0
                        continue
                    additional_params = rail.find_first_by_attr_and_get_attr(
                        effective_policy['policySet']['timeOffBalanceEventScripts'],
                        'script.name',
                        'Starting Balance Set To',
                        'additionalParameters',
                        default=[]
                    )

                    starting_balance = rail.find_first_by_attr_and_get_attr(
                        additional_params,
                        'keyUri',
                        'urn:replicon:script-key:parameter:amount',
                        'value.number',
                        default=0
                    )
                    res[timeoff['timeOffType']['name']] = starting_balance
                rail.set_result(key="policies", val = res_list)
                rail.set_result(key="full_policy_res", val=full_policy_res)

                annual, bought, sold = 0, 0, 0

                for timeoff, starting_balance in res.items():
                    if 'bought' in timeoff.lower():
                        bought = starting_balance
                    elif 'sold' in timeoff.lower():
                        sold = starting_balance
                    elif 'annual' in timeoff.lower():
                        annual = starting_balance

                return  {
                    "res": res,
                    "bought": bought,
                    "sold": sold,
                    "annual": annual,
                    "is_hour": is_user_part_time, # only part-time users have the hours timeoff types
                    "holidays": res.get('[UK] Public Holiday', 0) if is_uk else res.get('[IRL] Public Holiday', 0)
                }

            get_annual_bought_sold_leave_policy_summary = rail.RepliconServiceOperator(
                task_id = "get_annual_bought_sold_leave_policy_summary",
                endpoint = "/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
                data = {
                    "userUri": "{{ dag_run.conf['user_uri'] }}"
                },
                data_handler = lambda response,dag_run : get_annual_bought_sold_leave_policy_summary_data_handler(response, dag_run)
            )

            def get_annual_bought_sold_leave_balance_summary_data_handler(response):
                return response

            get_annual_bought_sold_leave_balance_summary = rail.RepliconServiceCallForEachItemOperator(
                task_id = "get_annual_bought_sold_leave_balance_summary",
                endpoint = "/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
                data = lambda dag_run, item: {
                        "account": {
                            "userUri": dag_run.conf['user_uri'],
                            "timeOffTypeUri": item['timeOffType']['uri']
                        },
                        "asOfDate": dag_run.conf['user_end_date_json']
                    },
                items = lambda : rail.result("get_annual_bought_sold_leave_policy_summary", "policies"),
                all_result_data_handler=get_annual_bought_sold_leave_balance_summary_data_handler
            )

            def parse_date(date_string: Optional[str], default=None) -> Optional[pendulum.DateTime]:
                if not date_string:
                    return default
                
                try:
                    return pendulum.parse(date_string)
                except (ValueError, TypeError):
                    return default

            def standard_round(num):
                return round(num)

            def calculate_prorated_timeoff(entitlement, termination_date, hire_date=None, leaves_used=0, is_hours=False):
                term_date = parse_date(termination_date)
                current_year_start = pendulum.now().start_of('year')
                
                if hire_date and parse_date(hire_date).year == term_date.year:
                    # New hire termination
                    start_date = parse_date(hire_date)
                    total_months = 12 - start_date.month + 1 # +1 to include the starting month
                    months_worked = term_date.month - start_date.month + 1 # +1 to include the starting month
                else:
                    # Regular termination
                    total_months = 12
                    months_worked = term_date.month

                # Calculate prorated amount
                prorated = (entitlement / total_months) * months_worked
                
                # Standard rounding: .0-.4 rounds down, .5-.9 rounds up
                if prorated > 0:
                    prorated = standard_round(prorated)
                
                # Deduct used leaves
                final_balance = prorated - leaves_used
                
                return final_balance

            def calculate_required_balance(dag_run):
                # ANNUAL_TIMEOFF_LIST = ['[UK] Annual Leave', '[UK] P/T Annual Leave Hrs', '[IRL] Annual Leave', '[IRL] P/T Annual Leave Hrs']
                # BOUGHT_TIMEOFF_LIST = ['[UK] Bought A/L', '[UK] P/T Bought A/L Hrs', '[IRL] Bought A/L', '[IRL] P/T Bought A/L Hrs']
                _timeoffs = get_annual_brought_sold_holiday_leave_list(
                    return_for="country",
                    country="UK" if dag_run.conf['file_data']['country'].lower() == "united kingdom" else "IRL",
                    part_time_full_time="parttime" if dag_run.conf['file_data']['fte_pct'] != '100' else "fulltime",
                    return_type="json"
                )
                ANNUAL_TIMEOFF_LIST = [_timeoffs['annual']]
                BOUGHT_TIMEOFF_LIST = [_timeoffs['bought']]

                balance_summary = rail.result("get_annual_bought_sold_leave_balance_summary")

                annual_leave_used, bought_leave_used = 0, 0
                for summary in balance_summary:
                    if summary['account']['timeOffType']['name'] in ANNUAL_TIMEOFF_LIST:
                        annual_leave_used = summary['timeTakenForPeriod']
                    if summary['account']['timeOffType']['name'] in BOUGHT_TIMEOFF_LIST:
                        bought_leave_used = summary['timeTakenForPeriod']

                annual_bought_sold_entitlement = rail.result('get_annual_bought_sold_leave_policy_summary')
                annual, bought, sold, holiday = annual_bought_sold_entitlement['annual'], annual_bought_sold_entitlement['bought'], annual_bought_sold_entitlement['sold'], annual_bought_sold_entitlement['holidays']
                is_hours = annual_bought_sold_entitlement['is_hour']

                entitlement = ((annual + bought)) if not is_hours else (annual + bought + holiday)

                termination_date = dag_run.conf['user_end_date_json']
                hire_date = dag_run.conf['user_start_date_json']

                # Sold leaves are NOT included in proration calculations per business requirements
                leaves_used = annual_leave_used + bought_leave_used


                return calculate_prorated_timeoff(entitlement, convert_json_date_to_date(termination_date).strftime("%Y-%m-%d"), convert_json_date_to_date(hire_date).strftime("%Y-%m-%d"), leaves_used, is_hours)

            
            def add_new_policy_line_annual_bought_sold(dag_run, balance_to_add):
                _date = dt.strptime(dag_run.conf['end_date'], INPUT_DATE_FORMAT)
                return {
                    "effectiveDate": {
                        "day": _date.day,
                        "month": _date.month,
                        "year": _date.year
                    },
                    "description": f"Added by Integration on {_date.strftime('%d-%m-%Y')}",
                    "policySet": {
                        "timeOffBalanceEventScripts": [{
                            "additionalParameters": [{
                                "keyUri": "urn:replicon:script-key:parameter:amount",
                                "value": {
                                    "number": balance_to_add
                                }
                            }],
                            "script": {
                                "description": "Set initial balance for the first day of a policy",
                                "name": "Starting Balance Set To",
                                "uri": dag_run.conf['starting_balance_set_to_uri']
                            }
                        }],
                        "timeOffValidationScripts": [{
                            "additionalParameters": [{
                                "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                                "value": {
                                    "number": "0"
                                }
                            }],
                            "script": {
                                "description": "Do not allow the user's time off balance to go below the overdraw threshold",
                                "name": "Prevent balance overdraw",
                                "uri": dag_run.conf['prevent_balance_overdraw_uri']
                            }
                        }]
                    }
                }



            def get_timeoff_polices_to_assign_annual_bought_sold_callable(dag_run):

                prorata_balance = rail.result("get_required_balance_to_update")

                res = dict()
                user_end_date: datetime = convert_json_date_to_date(
                        dag_run.conf['user_end_date_json'])
                for timeoff in rail.result("get_annual_bought_sold_leave_policy_summary", "policies"):
                    existing_policy = list()
                    policies = timeoff['policySetSchedule']
                    existing_policy = list(filter(
                        lambda policy: convert_json_date_to_date(
                            policy['effectiveDate']) < user_end_date,
                        policies
                    ))
                    balance_to_add = prorata_balance if 'annual' in timeoff['timeOffType']['name'].lower() else 0

                    existing_policy.append(add_new_policy_line_annual_bought_sold(dag_run, balance_to_add))
                    timeoff_type = 'annual'
                    if 'annual' in timeoff['timeOffType']['name'].lower():
                        pass
                    elif 'bought' in timeoff['timeOffType']['name'].lower():
                        timeoff_type = 'bought'
                    elif 'sold' in timeoff['timeOffType']['name'].lower():
                        timeoff_type = 'sold'
                    elif 'holiday' in timeoff['timeOffType']['name'].lower():
                        timeoff_type = 'holiday'

                    res[timeoff_type] = {
                        'uri': timeoff['timeOffType']['uri'],
                        'name': timeoff['timeOffType']['name'],
                        "policy_to_assign": existing_policy,
                        'update_policy' :dumps(existing_policy
                                                ).replace("/null/", "\"effective\""
                                                ).replace("\"script\"", "\"scriptTarget\""
                                                ).replace('":{"additionalParameters', '":[{"additionalParameters'
                                                ).replace(':{"keyUri"', ':[{"keyUri"'
                                                ).replace('}},"scriptTarget"', '}}],"scriptTarget"'
                                                ).replace('}},"timeOffValidationScripts', '}}],"timeOffValidationScripts'
                                                ).replace('}}},"description', '}}]},"description')
                        }

                temp = []
                for _, v in res.items():
                    temp.append(v)

                res['process'] = temp

                return res




            get_required_balance_to_update = rail.PythonOperator(
                task_id="get_required_balance_to_update",
                python_callable=calculate_required_balance
            )

            get_timeoff_polices_to_assign_annual_bought_sold = rail.PythonOperator(
                task_id="get_timeoff_polices_to_assign_annual_bought_sold",
                python_callable=get_timeoff_polices_to_assign_annual_bought_sold_callable
            )

            update_policy_annual_bought_sold = rail.RepliconServiceCallForEachItemOperator(
                task_id="update_policy_annual_bought_sold",
                endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
                items=lambda: rail.result("get_timeoff_polices_to_assign_annual_bought_sold")['process'],
                data=lambda dag_run, item: {
                    "timeOffAccount": {
                        "userUri": dag_run.conf['user_uri'],
                        "timeOffTypeUri": item['uri']
                    },
                    "policySetScheduleEntries": loads(item['update_policy'])
                }
            )

            get_timeoff_details = rail.RepliconServiceOperator(
                task_id="get_timeoff_details",
                endpoint="/services/TimeOffService1.svc/BulkGetTimeOffTypes",
                data={
                    "timeOffTypeUris": [
                        "{{dag_run.conf.timeoff_type_uri}}"
                    ]
                }
            )

            get_users_effective_group_membership = rail.RepliconServiceOperator(
                task_id="get_users_effective_group_membership",
                endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
                data={
                    "userUri": "{{dag_run.conf.user_uri}}",
                    "dateRange": None
                }
            )

            get_user_timeoff_balance_summary = rail.RepliconServiceOperator(
                task_id="get_user_timeoff_balance_summary",
                endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
                data=get_user_timeoff_balance_summary_payload
            )

            get_timeoff_polices_to_assign = rail.PythonOperator(
                task_id="get_timeoff_polices_to_assign",
                python_callable=get_timeoff_polices_to_assign_callable
            )

            format_timeoff_polices_to_assign = rail.PythonOperator(
                task_id="format_timeoff_polices_to_assign",
                python_callable=format_timeoff_polices_to_assign_callable
            )

            update_policy = rail.RepliconServiceOperator(
                task_id="update_policy",
                endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
                data=get_update_policy_payload
            )

            catch_errors = rail.PythonOperator(
                task_id="catch_errors",
                trigger_rule="one_failed",
                python_callable=lambda: rail.render_template(
                    "{{ get_error_message() }}")
            )

            can_run_batch_task >> rail.Label("Yes") >> batch_task >> rail.Label("On Error") >> catch_errors
            can_run_batch_task >> rail.Label("No") >> is_end_date_present

            is_end_date_present >> rail.Label("Yes") >> if_country_is_uk_or_ireland 
            get_timeoff_details >> get_users_effective_group_membership \
                >> get_user_timeoff_balance_summary >> get_timeoff_polices_to_assign >> format_timeoff_polices_to_assign >> update_policy  >> rail.Label("On Error") >> catch_errors

            is_end_date_present >> if_country_is_uk_or_ireland >> rail.Label("Yes") >> process_uki_logic >> is_timeoff_bought_or_sold
            if_country_is_uk_or_ireland >> rail.Label("No") >> get_timeoff_details_dummy >> get_timeoff_details

            is_timeoff_bought_or_sold >> rail.Label("Yes") >> skip_processing >> rail.Label("On Error") >> catch_errors
            is_timeoff_bought_or_sold >> rail.Label("No") >> is_timeoff_annual_leave_dummy >> is_timeoff_annual_leave

            is_timeoff_annual_leave >> rail.Label("Yes") >> process_uki_annual_leave_specific_logic
            is_timeoff_annual_leave >> rail.Label("No") >> is_timeoff_annual_leave_no_task_get_timeoff_details >> get_timeoff_details

            process_uki_annual_leave_specific_logic >> get_annual_bought_sold_leave_policy_summary >> get_annual_bought_sold_leave_balance_summary >> get_required_balance_to_update \
                >> get_timeoff_polices_to_assign_annual_bought_sold >> update_policy_annual_bought_sold >> rail.Label("On Error") >> catch_errors

        _dags.append(dag)

    return _dags

rail.for_each_instance(create_dag)
