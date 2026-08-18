from datetime import datetime, timedelta
import json
from airflow.models import Variable
import rail
from adtalem.user_import.mappers.annual_leave_asia_aus_paygroup_mapper import annual_leave_asia_aus_paygroup_mapper
from adtalem.user_import.utils.python_callable_method import get_timeoff_policy_assignments_aus_asia
from adtalem.user_import.utils.request_payload import get_user_tenure


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_timeoff_annualleaveday_aus_asiapolicyaddupdate_cr2021_v1_{config.instance}',
        description=f'Timeoff_Annual leave Day_Aus_Asia Policy Add/Update_CR2021_V1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_timeofftype_equals_to_annualleaveasia'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_timeofftype_equals_to_annualleaveasia',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_timeofftype_equals_to_annualleaveasia = rail.IfOperator(
            task_id='if_timeofftype_equals_to_annualleaveasia',
            test="{{ dag_run.conf.timeofftype == 'Annual Leave(Asia)' }}",
            yes_task="search_annualleave_asiaaus_paygroup_mapper",
            no_task="declare_var_initialaccrual_6",
        )

        search_annualleave_asiaaus_paygroup_mapper = rail.PythonOperator(
            task_id='search_annualleave_asiaaus_paygroup_mapper',
            python_callable=lambda dag_run: next(iter(filter(
                lambda x: x['paygroup'] == dag_run.conf['paygroup'] and x["job_code"] == dag_run.conf['jobcode'], annual_leave_asia_aus_paygroup_mapper)), '')
        )

        get_accrual_values = rail.PythonOperator(
            task_id='get_accrual_values',
            python_callable=lambda: rail.result(
                'search_annualleave_asiaaus_paygroup_mapper')['values'].split('|')
        )

        declare_var_initialaccrual_6 = rail.SetVariableOperator(
            task_id='declare_var_initialaccrual_6',
            append=False,
            name='acrualstoassign',
            value=20
        )

        declare_policylist = rail.SetVariableOperator(
            task_id='declare_policylist',
            append=False,
            name='policy',
            value=[]
        )

        is_type_add = rail.IfOperator(
            task_id='is_type_add',
            test="{{ dag_run.conf.type == 'Add' }}",
            yes_task="get_defaulttimeoff_policy_set_schedule",
            no_task="if_request_type_equals_to_update_34",
        )

        get_defaulttimeoff_policy_set_schedule = rail.RepliconServiceOperator(
            task_id='get_defaulttimeoff_policy_set_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        foreach_response_13 = rail.ForEachOperator(
            task_id='foreach_response_13',
            items=lambda: rail.result(
                'get_defaulttimeoff_policy_set_schedule'),
            start_task='monthly_accrualscript',
            end_task='foreach_response_13_end'
        )

        monthly_accrualscript = rail.PythonOperator(
            task_id='monthly_accrualscript',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                    rail.result('foreach_response_13')[
                        'policySet']['timeOffBalanceEventScripts'],
                    'script.name', 'Monthly Accrual', 'additionalParameters', '')
        )

        log_accrualamount_16 = rail.PythonOperator(
            task_id='log_accrualamount_16',
            python_callable=lambda: float(rail.find_first_by_attr_and_get_attr(
                    rail.result('monthly_accrualscript'),
                    'keyUri', 'urn:replicon:script-key:parameter:accrual-annual-amount', 'value', '')['number'])
        )

        log_existingaccrual_17 = rail.PythonOperator(
            task_id='log_existingaccrual_17',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                    "number": rail.result('log_accrualamount_16')
                }
            }, ensure_ascii=False)
        )

        if_request_timeofftype_equals_to_annualleaveasia_18 = rail.IfOperator(
            task_id='if_request_timeofftype_equals_to_annualleaveasia_18',
            test="{{ dag_run.conf.timeofftype == 'Annual Leave(Asia)' }}",
            yes_task="update_variable_19",
            no_task="get_accrual_value",
        )

        def get_accrual_val():
            offset_value = rail.result('foreach_response_13')[
                'startOffset']['offsetValue']
            if offset_value == 0:
                return rail.result('get_accrual_values')[0]
            if offset_value == 3:
                return rail.result('get_accrual_values')[1]
            if offset_value == 6:
                return rail.result('get_accrual_values')[2]
            if offset_value == 9:
                return rail.result('get_accrual_values')[3]
            return rail.result('get_accrual_values')[0]
        update_variable_19 = rail.SetVariableOperator(
            task_id='update_variable_19',
            append=False,
            name='{{ result("declare_var_initialaccrual_6").name }}',
            value=get_accrual_val
        )

        get_accrual_value = rail.GetVariableOperator(
            task_id='get_accrual_value',
            name='{{ result("declare_var_initialaccrual_6").name }}'
        )

        log_requiredaccrualbalance_20 = rail.PythonOperator(
            task_id='log_requiredaccrualbalance_20',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                    "number": rail.result('get_accrual_value')['value']
                }
            }, ensure_ascii=False)
        )

        log_requiredpolicy_21 = rail.PythonOperator(
            task_id='log_requiredpolicy_21',
            python_callable=lambda: json.loads(json.dumps(rail.result('foreach_response_13')['policySet'],
                                                          ensure_ascii=False).replace(
                rail.result('log_existingaccrual_17'), rail.result('log_requiredaccrualbalance_20')))
        )

        if_startoffset_offsetvalue_equals_to_0_23 = rail.IfOperator(
            task_id='if_startoffset_offsetvalue_equals_to_0_23',
            test="{{ result('foreach_response_13').startOffset.offsetValue == 0 }}",
            yes_task="insert_to_list_24",
            no_task="if_startoffset_offsetvalue_equals_to_3_25",
        )

        insert_to_list_24 = rail.SetVariableOperator(
            task_id='insert_to_list_24',
            append=True,
            name='{{ result("declare_policylist").name }}',
            value=lambda: get_timeoff_policy_assignments_aus_asia(0)
        )

        if_startoffset_offsetvalue_equals_to_3_25 = rail.IfOperator(
            task_id='if_startoffset_offsetvalue_equals_to_3_25',
            test="{{ result('foreach_response_13').startOffset.offsetValue == 3 }}",
            yes_task="insert_to_list_26",
            no_task="if_startoffset_offsetvalue_equals_to_6_27",
        )

        insert_to_list_26 = rail.SetVariableOperator(
            task_id='insert_to_list_26',
            append=True,
            name='{{ result("declare_policylist").name }}',
            value=lambda: get_timeoff_policy_assignments_aus_asia(3)
        )

        if_startoffset_offsetvalue_equals_to_6_27 = rail.IfOperator(
            task_id='if_startoffset_offsetvalue_equals_to_6_27',
            test="{{ result('foreach_response_13').startOffset.offsetValue == 6 }}",
            yes_task="insert_to_list_28",
            no_task="if_startoffset_offsetvalue_equals_to_9_29",
        )

        insert_to_list_28 = rail.SetVariableOperator(
            task_id='insert_to_list_28',
            append=True,
            name='{{ result("declare_policylist").name }}',
            value=lambda: get_timeoff_policy_assignments_aus_asia(6)
        )

        if_startoffset_offsetvalue_equals_to_9_29 = rail.IfOperator(
            task_id='if_startoffset_offsetvalue_equals_to_9_29',
            test="{{ result('foreach_response_13').startOffset.offsetValue == 9 }}",
            yes_task="insert_to_list_30",
            no_task="foreach_response_13_end",
        )

        insert_to_list_30 = rail.SetVariableOperator(
            task_id='insert_to_list_30',
            append=True,
            name='{{ result("declare_policylist").name }}',
            value=lambda: get_timeoff_policy_assignments_aus_asia(9)
        )

        foreach_response_13_end = rail.EmptyOperator(
            task_id='foreach_response_13_end',
        )

        get_policy_set = rail.GetVariableOperator(
            task_id='get_policy_set',
            name='policy'
        )

        log_policy_31 = rail.PythonOperator(
            task_id='log_policy_31',
            python_callable=lambda: json.loads(json.dumps(
                rail.result('get_policy_set')['value'], ensure_ascii=False).replace(
                '"script"', '"scriptTarget"')) if rail.result('get_policy_set')['value'] else ''
        )

        if_log_policy_31_present_32 = rail.IfOperator(
            task_id='if_log_policy_31_present_32',
            test="{{ result('log_policy_31') | is_truthy }}",
            yes_task="put_user_time_off_account_policy_set_schedule_33",
            no_task="if_request_type_equals_to_update_34",
        )

        put_user_time_off_account_policy_set_schedule_33 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_33',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoffuri']
                },
                "policySetScheduleEntries": rail.result('log_policy_31')
            }
        )

        if_request_type_equals_to_update_34 = rail.IfOperator(
            task_id='if_request_type_equals_to_update_34',
            test="{{ dag_run.conf.type == 'Update' }}",
            yes_task="log_tenure_36",
            no_task="log_to_sumo",
        )

        log_tenure_36 = rail.PythonOperator(
            task_id='log_tenure_36',
            python_callable=get_user_tenure,
            op_args=['{{ dag_run.conf.servicedate }}',
                     "{{ dag_run.conf.rehiredate if 'Rehire' in dag_run.conf.type else '' }}"]
        )

        log_datetoconsiderforeffectivedate_37 = rail.PythonOperator(
            task_id='log_datetoconsiderforeffectivedate_37',
            python_callable=get_user_tenure,
            op_args=['{{ dag_run.conf.servicedate }}',
                     "{{ dag_run.conf.rehiredate if 'Rehire' in dag_run.conf.type else dag_run.conf.startdate }}"]
        )

        get_user_time_off_type_policy_summary_38 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_38',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        for_each_timeofftype_39 = rail.ForEachOperator(
            task_id='for_each_timeofftype_39',
            items="{{ result('get_user_time_off_type_policy_summary_38').policiesByTimeOffType }}",
            start_task='for_each_timeofftype_is_timeoff_allowed',
            end_task='for_each_timeofftype_39_end'
        )

        for_each_timeofftype_is_timeoff_allowed = rail.IfOperator(
            task_id='for_each_timeofftype_is_timeoff_allowed',
            test="{{ result('for_each_timeofftype_39').isTimeOffAllowedAgainstThisTimeOffType | is_truthy \
                and result('for_each_timeofftype_39').timeOffType.uri == dag_run.conf.timeoffuri }}",
            yes_task="foreach_policysetschedule",
            no_task="for_each_timeofftype_39_end",
        )

        foreach_policysetschedule = rail.ForEachOperator(
            task_id='foreach_policysetschedule',
            items="{{ result('for_each_timeofftype_39').policySetSchedule }}",
            start_task='log_effective_date_42',
            end_task='foreach_policysetschedule_end'
        )

        log_effective_date_42 = rail.PythonOperator(
            task_id='log_effective_date_42',
            # pylint: disable=line-too-long
            python_callable=lambda: f"{rail.result('foreach_policysetschedule')['effectiveDate']['day']}/{rail.result('foreach_policysetschedule')['effectiveDate']['month']}/{rail.result('foreach_policysetschedule')['effectiveDate']['year']}"
        )

        if_to_date_less_than_datalogger22dc7ab8messageto_date_43 = rail.IfOperator(
            task_id='if_to_date_less_than_datalogger22dc7ab8messageto_date_43',
            test=lambda: datetime.strptime(rail.result(
                'log_effective_date_42'), '%d/%m/%Y').date() <= datetime.now().date(),
            yes_task="insert_to_list_44",
            no_task="foreach_policysetschedule_end",
        )

        insert_to_list_44 = rail.SetVariableOperator(
            task_id='insert_to_list_44',
            append=True,
            name='{{ result("declare_policylist").name }}',
            value={
                "effectiveDate": {
                    "day": "{{ result('foreach_policysetschedule').effectiveDate.day }}",
                    "month": "{{ result('foreach_policysetschedule').effectiveDate.month }}",
                    "year": "{{ result('foreach_policysetschedule').effectiveDate.year }}"
                },
                "description": "{{ result('foreach_policysetschedule').description }}",
                "policySet": "{{ result('foreach_policysetschedule').policySet }}"
            }
        )

        foreach_policysetschedule_end = rail.EmptyOperator(
            task_id='foreach_policysetschedule_end',
        )

        for_each_timeofftype_39_end = rail.EmptyOperator(
            task_id='for_each_timeofftype_39_end',
        )

        get_default_time_off_policy_set_schedule_for_time_off_type_46 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_policy_set_schedule_for_time_off_type_46',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
            }
        )

        if_request_timeofftype_equals_to_annualleaveasia_48 = rail.IfOperator(
            task_id='if_request_timeofftype_equals_to_annualleaveasia_48',
            test="{{ dag_run.conf.timeofftype == 'Annual Leave(Asia)' }}",
            yes_task="log_monthlyaccrual_49",
            no_task="foreach_response_63",
        )

        log_monthlyaccrual_49 = rail.PythonOperator(
            task_id='log_monthlyaccrual_49',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_default_time_off_policy_set_schedule_for_time_off_type_46')[0]['policySet']['timeOffBalanceEventScripts'],
                'script.name', "Monthly Accrual", "additionalParameters")
        )

        parse_json_50 = rail.PythonOperator(
            task_id='parse_json_50',
            python_callable=lambda: json.loads(
                rail.result('log_monthlyaccrual_49'))
        )

        log_accrualamounnt_51 = rail.PythonOperator(
            task_id='log_accrualamounnt_51',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('parse_json_50'),
                                                                         'keyUri',
                                                                         'urn:replicon:script-key:parameter:accrual-annual-amount',
                                                                         'value.number')[0]
        )

        log_existingaccrual_52 = rail.PythonOperator(
            task_id='log_existingaccrual_52',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": 26.0}},
                ensure_ascii=False)
        )

        if_to_i_greater_than_6_53 = rail.IfOperator(
            task_id='if_to_i_greater_than_6_53',
            test="{{ result('log_tenure_36') > 6 and result('log_tenure_36') < 9 }}",
            yes_task="update_variable_54",
            no_task="if_to_i_greater_than_0_55",
        )

        update_variable_54 = rail.SetVariableOperator(
            task_id='update_variable_54',
            append=False,
            name='{{ result("declare_var_initialaccrual_6").name }}',
            value=lambda: rail.result('get_accrual_values')[2]
        )

        if_to_i_greater_than_0_55 = rail.IfOperator(
            task_id='if_to_i_greater_than_0_55',
            test="{{ result('log_tenure_36') > 0 and result('log_tenure_36') < 3 }}",
            yes_task="update_variable_56",
            no_task="if_to_i_greater_than_3_57",
        )

        update_variable_56 = rail.SetVariableOperator(
            task_id='update_variable_56',
            append=False,
            name='{{ result("declare_var_initialaccrual_6").name }}',
            value=lambda: rail.result('get_accrual_values')[0]
        )

        if_to_i_greater_than_3_57 = rail.IfOperator(
            task_id='if_to_i_greater_than_3_57',
            test="{{ result('log_tenure_36') > 3 and result('log_tenure_36') < 6 }}",
            yes_task="update_variable_58",
            no_task="log_requiiredaccrualbalance_59",
        )

        update_variable_58 = rail.SetVariableOperator(
            task_id='update_variable_58',
            append=False,
            name='{{ result("declare_var_initialaccrual_6").name }}',
            value=lambda: rail.result('get_accrual_values')[1]
        )

        log_requiiredaccrualbalance_59 = rail.PythonOperator(
            task_id='log_requiiredaccrualbalance_59',
            python_callable=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                                "value": {"number": rail.result('declare_var_initialaccrual_6')['value']}}, ensure_ascii=False)
        )

        log_policytoassign_60 = rail.PythonOperator(
            task_id='log_policytoassign_60',
            python_callable=lambda: json.loads(json.dumps(rail.result(
                'get_default_time_off_policy_set_schedule_for_time_off_type_46')['policySet'][-1], ensure_ascii=False).replace(
                rail.result('log_existingaccrual_52'), rail.result('log_requiiredaccrualbalance_59')))
        )

        foreach_response_63 = rail.ForEachOperator(
            task_id='foreach_response_63',
            items="{{ result('get_default_time_off_policy_set_schedule_for_time_off_type_46') }}",
            start_task='if_request_timeofftype_equals_to_annualleaveasia_64',
            end_task='foreach_response_63_end'
        )

        if_request_timeofftype_equals_to_annualleaveasia_64 = rail.IfOperator(
            task_id='if_request_timeofftype_equals_to_annualleaveasia_64',
            test="{{ dag_run.conf.timeofftype == 'Annual Leave(Asia)' }}",
            yes_task="if_startoffset_offsetvalue_equals_to_dataloggerdca4f968messageto_i_65",
            no_task="foreach_response_63_end",
        )

        if_startoffset_offsetvalue_equals_to_dataloggerdca4f968messageto_i_65 = rail.IfOperator(
            task_id='if_startoffset_offsetvalue_equals_to_dataloggerdca4f968messageto_i_65',
            test="{{ result('foreach_response_63').startOffset.offsetValue == result('log_tenure_36') \
                or result('foreach_response_63').startOffset.offsetValue > result('log_tenure_36') }}",
            yes_task="log_monthlyaccrual_66",
            no_task="else_82",
        )

        def get_templated_value(value):
            return value
        log_monthlyaccrual_66 = rail.PythonOperator(
            task_id='log_monthlyaccrual_66',
            python_callable=get_templated_value,
            op_args=["{{ result('foreach_response_63').policySet.timeOffBalanceEventScripts | \
                find_first_by_attr_and_get_attr('script.name', 'Monthly Accrual', 'additionalParameters') | \
                    to_json | replace('[[', '[') | replace(']]', ']') }}"]
        )

        parse_json_67 = rail.PythonOperator(
            task_id='parse_json_67',
            python_callable=lambda: json.loads(
                rail.result('log_monthlyaccrual_66'))
        )

        log_accrualamounnt_68 = rail.PythonOperator(
            task_id='log_accrualamounnt_68',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('parse_json_67'),
                                                                         'keyUri',
                                                                         'urn:replicon:script-key:parameter:accrual-annual-amount',
                                                                         'value.number')[0]
        )

        log_existingaccrual_69 = rail.PythonOperator(
            task_id='log_existingaccrual_69',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                    "number": rail.result('log_accrualamounnt_68')
                }
            }, ensure_ascii=False)
        )

        def get_accrual_val2():
            offset_value = rail.result('foreach_response_63')[
                'startOffset']['offsetValue']
            if offset_value == 0:
                return int(rail.result('get_accrual_values')[0])
            if offset_value == 3:
                return int(rail.result('get_accrual_values')[1])
            if offset_value == 6:
                return int(rail.result('get_accrual_values')[2])
            if offset_value == 9:
                return int(rail.result('get_accrual_values')[3])
            return int(rail.result('get_accrual_values')[0])

        update_variable_70 = rail.SetVariableOperator(
            task_id='update_variable_70',
            append=False,
            name='{{ result("declare_var_initialaccrual_6").name }}',
            value=get_accrual_val2
        )

        log_requiiredaccrualbalance_71 = rail.PythonOperator(
            task_id='log_requiiredaccrualbalance_71',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                    "number": rail.result('declare_var_initialaccrual_6')['value']
                }
            }, ensure_ascii=False)
        )

        log_policytoassign_72 = rail.PythonOperator(
            task_id='log_policytoassign_72',
            python_callable=lambda: json.loads(json.dumps(rail.result('foreach_response_63')['policySet'], ensure_ascii=False).replace(
                rail.result('log_existingaccrual_69'), rail.result('log_requiiredaccrualbalance_71')))
        )

        if_startoffset_offsetvalue_equals_to_0_74 = rail.IfOperator(
            task_id='if_startoffset_offsetvalue_equals_to_0_74',
            test="{{ result('foreach_response_63').startOffset.offsetValue == 0 }}",
            yes_task="insert_to_list_75",
            no_task="if_startoffset_offsetvalue_equals_to_3_76",
        )

        insert_to_list_75 = rail.SetVariableOperator(
            task_id='insert_to_list_75',
            append=True,
            name='{{ result("declare_policylist").name }}',
            value=lambda: get_timeoff_policy_assignments_aus_asia(0)
        )

        if_startoffset_offsetvalue_equals_to_3_76 = rail.IfOperator(
            task_id='if_startoffset_offsetvalue_equals_to_3_76',
            test="{{ result('foreach_response_63').startOffset.offsetValue == 3 }}",
            yes_task="insert_to_list_77",
            no_task="if_startoffset_offsetvalue_equals_to_6_78",
        )

        insert_to_list_77 = rail.SetVariableOperator(
            task_id='insert_to_list_77',
            append=True,
            name='{{ result("declare_policylist").name }}',
            value=lambda: get_timeoff_policy_assignments_aus_asia(3)
        )

        if_startoffset_offsetvalue_equals_to_6_78 = rail.IfOperator(
            task_id='if_startoffset_offsetvalue_equals_to_6_78',
            test="{{ result('foreach_response_63').startOffset.offsetValue == 6 }}",
            yes_task="insert_to_list_79",
            no_task="if_startoffset_offsetvalue_equals_to_9_80",
        )

        insert_to_list_79 = rail.SetVariableOperator(
            task_id='insert_to_list_79',
            append=True,
            name='{{ result("declare_policylist").name }}',
            value=lambda: get_timeoff_policy_assignments_aus_asia(6)
        )

        if_startoffset_offsetvalue_equals_to_9_80 = rail.IfOperator(
            task_id='if_startoffset_offsetvalue_equals_to_9_80',
            test="{{ result('foreach_response_63').startOffset.offsetValue == 9 }}",
            yes_task="insert_to_list_81",
            no_task="else_82",
        )

        insert_to_list_81 = rail.SetVariableOperator(
            task_id='insert_to_list_81',
            append=True,
            name='{{ result("declare_policylist").name }}',
            value=lambda: get_timeoff_policy_assignments_aus_asia(9)
        )

        else_82 = rail.EmptyOperator(
            task_id='else_82',
        )

        log_monthlyaccrual_83 = rail.PythonOperator(
            task_id='log_monthlyaccrual_83',
            python_callable=get_templated_value,
            op_args=["{{ result('foreach_response_63').policySet.timeOffBalanceEventScripts | \
                find_first_by_attr_and_get_attr('script.name', 'Monthly Accrual', 'additionalParameters') | \
                    to_json | replace('[[', '[') | replace(']]', ']') }}"]
        )

        parse_json_84 = rail.PythonOperator(
            task_id='parse_json_84',
            python_callable=lambda: json.loads(
                rail.result("{{ result('log_monthlyaccrual_83') }}"))
        )

        log_accrualamounnt_85 = rail.PythonOperator(
            task_id='log_accrualamounnt_85',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('parse_json_84'),
                                                                         'keyUri',
                                                                         'urn:replicon:script-key:parameter:accrual-annual-amount',
                                                                         'value.number')[0]
        )

        log_existingaccrual_86 = rail.PythonOperator(
            task_id='log_existingaccrual_86',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                    "number": rail.result('log_accrualamounnt_85')
                }
            }, ensure_ascii=False)
        )

        log_requiiredaccrualbalance_87 = rail.PythonOperator(
            task_id='log_requiiredaccrualbalance_87',
            python_callable=lambda: json.dumps({
                "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                "value": {
                    "number": rail.result('declare_var_initialaccrual_6')['value']
                }
            }, ensure_ascii=False)
        )

        log_policytoassign_88 = rail.PythonOperator(
            task_id='log_policytoassign_88',
            python_callable=lambda: json.loads(json.dumps(rail.result('foreach_response_63')['policySet'], ensure_ascii=False).replace(
                rail.result('log_existingaccrual_86'), rail.result('log_requiiredaccrualbalance_87')))
        )

        if_startoffset_offsetvalue_equals_to_0_90 = rail.IfOperator(
            task_id='if_startoffset_offsetvalue_equals_to_0_90',
            test="{{ result('foreach_response_63').startOffset.offsetValue == 0 }}",
            yes_task="insert_to_list_90",
            no_task="foreach_response_63_end"
        )

        insert_to_list_90 = rail.SetVariableOperator(
            task_id='insert_to_list_90',
            append=True,
            name='{{ result("declare_policylist").name }}',
            value=lambda: get_timeoff_policy_assignments_aus_asia(0)
        )

        foreach_response_63_end = rail.EmptyOperator(
            task_id='foreach_response_63_end',
        )

        get_policy_set2 = rail.GetVariableOperator(
            task_id='get_policy_set2',
            name='policy'
        )

        def get_policy_sets():
            policy_sets = rail.result('get_policy_set2')['value']
            if policy_sets and policy_sets[0]['effectiveDate']['day']:
                return json.loads(json.dumps(policy_sets.replace(''), ensure_ascii=False).replace(
                    '"script"', '"scriptTarget"'))
            return ''
        log_policy_92 = rail.PythonOperator(
            task_id='log_policy_92',
            python_callable=get_policy_sets
        )

        if_log_policy_92_present_93 = rail.IfOperator(
            task_id='if_log_policy_92_present_93',
            test="{{ result('log_policy_92') | is_truthy }}",
            yes_task="put_user_time_off_account_policy_set_schedule_94",
            no_task="log_to_sumo",
        )

        put_user_time_off_account_policy_set_schedule_94 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_94',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ dag_run.conf.timeoffuri }}"
                },
                "policySetScheduleEntries": "{{ result('log_policy_92') }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> if_timeofftype_equals_to_annualleaveasia
        if_timeofftype_equals_to_annualleaveasia >> rail.Label(
            'Yes') >> search_annualleave_asiaaus_paygroup_mapper >> get_accrual_values >> declare_var_initialaccrual_6
        if_timeofftype_equals_to_annualleaveasia >> rail.Label(
            'No') >> declare_var_initialaccrual_6
        declare_var_initialaccrual_6 >> declare_policylist >> is_type_add
        is_type_add >> rail.Label(
            'Yes') >> get_defaulttimeoff_policy_set_schedule >> foreach_response_13
        foreach_response_13 >> monthly_accrualscript >> log_accrualamount_16 >> log_existingaccrual_17 >> \
            if_request_timeofftype_equals_to_annualleaveasia_18
        if_request_timeofftype_equals_to_annualleaveasia_18 >> rail.Label(
            'Yes') >> update_variable_19 >> get_accrual_value
        if_request_timeofftype_equals_to_annualleaveasia_18 >> rail.Label(
            'No') >> get_accrual_value
        get_accrual_value >> log_requiredaccrualbalance_20 >> log_requiredpolicy_21 >> \
            if_startoffset_offsetvalue_equals_to_0_23
        if_startoffset_offsetvalue_equals_to_0_23 >> rail.Label(
            'Yes') >> insert_to_list_24 >> foreach_response_13_end
        if_startoffset_offsetvalue_equals_to_0_23 >> rail.Label(
            'No') >> if_startoffset_offsetvalue_equals_to_3_25
        if_startoffset_offsetvalue_equals_to_3_25 >> rail.Label(
            'Yes') >> insert_to_list_26 >> foreach_response_13_end
        if_startoffset_offsetvalue_equals_to_3_25 >> rail.Label(
            'No') >> if_startoffset_offsetvalue_equals_to_6_27
        if_startoffset_offsetvalue_equals_to_6_27 >> rail.Label(
            'Yes') >> insert_to_list_28 >> foreach_response_13_end
        if_startoffset_offsetvalue_equals_to_6_27 >> rail.Label(
            'No') >> if_startoffset_offsetvalue_equals_to_9_29
        if_startoffset_offsetvalue_equals_to_9_29 >> rail.Label(
            'Yes') >> insert_to_list_30 >> foreach_response_13_end
        if_startoffset_offsetvalue_equals_to_9_29 >> rail.Label(
            'No') >> foreach_response_13_end
        foreach_response_13 >> foreach_response_13_end
        foreach_response_13_end >> get_policy_set >> log_policy_31 >> if_log_policy_31_present_32
        if_log_policy_31_present_32 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_33 >> if_request_type_equals_to_update_34
        if_log_policy_31_present_32 >> rail.Label(
            'No') >> if_request_type_equals_to_update_34
        is_type_add >> rail.Label(
            'No') >> if_request_type_equals_to_update_34
        if_request_type_equals_to_update_34 >> rail.Label(
            'Yes') >> log_tenure_36 >> log_datetoconsiderforeffectivedate_37 >> \
            get_user_time_off_type_policy_summary_38 >> for_each_timeofftype_39
        for_each_timeofftype_39 >> for_each_timeofftype_39_end
        for_each_timeofftype_39 >> for_each_timeofftype_is_timeoff_allowed
        for_each_timeofftype_is_timeoff_allowed >> rail.Label(
            'Yes') >> foreach_policysetschedule
        foreach_policysetschedule >> foreach_policysetschedule_end
        foreach_policysetschedule >> log_effective_date_42 >> if_to_date_less_than_datalogger22dc7ab8messageto_date_43
        if_to_date_less_than_datalogger22dc7ab8messageto_date_43 >> rail.Label(
            'Yes') >> insert_to_list_44 >> foreach_policysetschedule_end
        if_to_date_less_than_datalogger22dc7ab8messageto_date_43 >> rail.Label(
            'No') >> foreach_policysetschedule_end
        foreach_policysetschedule_end >> get_default_time_off_policy_set_schedule_for_time_off_type_46
        for_each_timeofftype_is_timeoff_allowed >> rail.Label(
            'No') >> for_each_timeofftype_39_end
        for_each_timeofftype_39_end >> get_default_time_off_policy_set_schedule_for_time_off_type_46 >> if_request_timeofftype_equals_to_annualleaveasia_48
        if_request_timeofftype_equals_to_annualleaveasia_48 >> rail.Label(
            'Yes') >> log_monthlyaccrual_49 >> parse_json_50 >> log_accrualamounnt_51 >> log_existingaccrual_52 >> if_to_i_greater_than_6_53
        if_to_i_greater_than_6_53 >> rail.Label(
            'Yes') >> update_variable_54 >> foreach_response_63
        if_to_i_greater_than_6_53 >> rail.Label(
            'No') >> if_to_i_greater_than_0_55
        if_to_i_greater_than_0_55 >> rail.Label(
            'Yes') >> update_variable_56 >> foreach_response_63
        if_to_i_greater_than_0_55 >> rail.Label(
            'No') >> if_to_i_greater_than_3_57
        if_to_i_greater_than_3_57 >> rail.Label(
            'Yes') >> update_variable_58 >> foreach_response_63
        if_to_i_greater_than_3_57 >> rail.Label(
            'No') >> log_requiiredaccrualbalance_59 >> log_policytoassign_60 >> foreach_response_63
        if_request_timeofftype_equals_to_annualleaveasia_48 >> rail.Label(
            'No') >> foreach_response_63

        foreach_response_63 >> if_request_timeofftype_equals_to_annualleaveasia_64
        if_request_timeofftype_equals_to_annualleaveasia_64 >> rail.Label(
            'Yes') >> if_startoffset_offsetvalue_equals_to_dataloggerdca4f968messageto_i_65
        if_startoffset_offsetvalue_equals_to_dataloggerdca4f968messageto_i_65 >> rail.Label(
            'Yes') >> log_monthlyaccrual_66 >> parse_json_67 >> log_accrualamounnt_68 >> \
            log_existingaccrual_69 >> update_variable_70 >> log_requiiredaccrualbalance_71 >> \
            log_policytoassign_72 >> if_startoffset_offsetvalue_equals_to_0_74
        if_startoffset_offsetvalue_equals_to_0_74 >> rail.Label(
            'Yes') >> insert_to_list_75 >> else_82
        if_startoffset_offsetvalue_equals_to_0_74 >> rail.Label(
            'No') >> if_startoffset_offsetvalue_equals_to_3_76
        if_startoffset_offsetvalue_equals_to_3_76 >> rail.Label(
            'Yes') >> insert_to_list_77 >> else_82
        if_startoffset_offsetvalue_equals_to_3_76 >> rail.Label(
            'No') >> if_startoffset_offsetvalue_equals_to_6_78
        if_startoffset_offsetvalue_equals_to_6_78 >> rail.Label(
            'Yes') >> insert_to_list_79 >> else_82
        if_startoffset_offsetvalue_equals_to_6_78 >> rail.Label(
            'No') >> if_startoffset_offsetvalue_equals_to_9_80
        if_startoffset_offsetvalue_equals_to_9_80 >> rail.Label(
            'Yes') >> insert_to_list_81 >> else_82
        if_startoffset_offsetvalue_equals_to_9_80 >> rail.Label(
            'No') >> else_82
        if_startoffset_offsetvalue_equals_to_dataloggerdca4f968messageto_i_65 >> rail.Label(
            'No') >> else_82
        else_82 >> log_monthlyaccrual_83 >> parse_json_84 >> log_accrualamounnt_85 >> \
            log_existingaccrual_86 >> log_requiiredaccrualbalance_87 >> log_policytoassign_88 >> \
            if_startoffset_offsetvalue_equals_to_0_90
        if_startoffset_offsetvalue_equals_to_0_90 >> rail.Label(
            'Yes') >> insert_to_list_90 >> foreach_response_63_end
        if_startoffset_offsetvalue_equals_to_0_90 >> rail.Label(
            'No') >> foreach_response_63_end

        if_request_timeofftype_equals_to_annualleaveasia_64 >> rail.Label(
            'No') >> foreach_response_63_end
        foreach_response_63 >> foreach_response_63_end
        foreach_response_63_end >> get_policy_set2 >> log_policy_92
        log_policy_92 >> if_log_policy_92_present_93
        if_log_policy_92_present_93 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_94 >> log_to_sumo
        if_log_policy_92_present_93 >> rail.Label(
            'No') >> log_to_sumo
        if_request_type_equals_to_update_34 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
