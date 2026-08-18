from datetime import datetime, timedelta
import json
from airflow.models import Variable
import rail
from adtalem.user_import.utils import python_callable_method
from adtalem.user_import.utils.request_payload import get_today_date


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_child_timeoff_update_user_cr2021_v1_{config.instance}',
        description=f'Update User - Time Off_CR2021_V1 {config.instance}',
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
            no_task='declare_variable_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_variable_2',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        declare_variable_2 = rail.SetVariableOperator(
            task_id='declare_variable_2',
            append=False,
            name='Floating holiday Trigger',
            value=None
        )

        declare_variable_3 = rail.SetVariableOperator(
            task_id='declare_variable_3',
            append=False,
            name='PTO RPT/RFT Trigger',
            value=None
        )

        declare_variable_5 = rail.SetVariableOperator(
            task_id='declare_variable_5',
            append=False,
            name='Mapper Lookup',
            value=None
        )

        getassigned_time_offtypes_7 = rail.RepliconServiceOperator(
            task_id='getassigned_time_offtypes_7',
            endpoint="/services/TimeOffService1.svc/BulkGetTimeOffTypeAssignmentsForUsers",
            data={
                "userUris": ["{{ dag_run.conf.useruri }}"]
            }
        )

        getenabled_time_offtypes_8 = rail.RepliconServiceOperator(
            task_id='getenabled_time_offtypes_8',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        def is_previousfullparttime_not_equals_fullparttime(dag_run):
            previous_fulltime = dag_run.conf['previousfullparttimevalue']
            fullparttime = dag_run.conf['fullparttime']
            assigned_to_types = rail.result('getassigned_time_offtypes_7')[
                0]['timeOffTypeAssignmentsDetails']['timeOffTypes']
            pto_rpt_uri = rail.find_first_by_attr_and_get_attr(
                assigned_to_types, 'name', 'PTO (RPT)', 'uri', '')
            pto_rft_uri = rail.find_first_by_attr_and_get_attr(
                assigned_to_types, 'name', 'PTO (RFT)', 'uri', '')
            return previous_fulltime != fullparttime and (pto_rpt_uri or pto_rft_uri)
        is_previousfulltime_notequal_fullparttime = rail.IfOperator(
            task_id='is_previousfulltime_notequal_fullparttime',
            test=is_previousfullparttime_not_equals_fullparttime,
            yes_task="get_balance_summary_for_account_10",
            no_task="if_request_ususer_not_equals_to_yes_13"
        )

        get_balance_summary_for_account_10 = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account_10',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run: {
                "account": {
                    "useruri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.find_first_by_attr_and_get_attr(rail.result(
                        'getassigned_time_offtypes_7')[0]['timeOffTypeAssignmentsDetails']['timeOffTypes'],
                        'name', 'PTO (RPT)', 'uri', '') or rail.find_first_by_attr_and_get_attr(
                        rail.result('getassigned_time_offtypes_7')[
                            0]['timeOffTypeAssignmentsDetails']['timeOffTypes'],
                        'name', 'PTO (RFT)', 'uri', '')
                },
                "asOfDate": get_today_date()
            }
        )

        if_request_ususer_not_equals_to_yes_13 = rail.IfOperator(
            task_id='if_request_ususer_not_equals_to_yes_13',
            test="{{ dag_run.conf.ususer != 'yes' }}",
            yes_task="should_update_newmapperlookup",
            no_task="invoke_custom_ruby_code_19"
        )

        should_update_newmapperlookup = rail.IfOperator(
            task_id='should_update_newmapperlookup',
            test="{{ dag_run.conf.paygroup == 'ACATW' \
                or dag_run.conf.paygroup == 'ACASG' \
                    or dag_run.conf.paygroup == 'ACAJP' \
                        or dag_run.conf.paygroup == 'HK' \
                            or dag_run.conf.paygroup == 'ACACH' \
                                or dag_run.conf.paygroup == 'ACAIN' \
                                    or dag_run.conf.paygroup == 'ACAAS' }}",
            yes_task="update_variable_15",
            no_task="else_16"
        )

        update_variable_15 = rail.SetVariableOperator(
            task_id='update_variable_15',
            append=False,
            name='{{ result("declare_variable_5").name }}',
            value='{{ dag_run.conf.newmapperlookup }}'
        )

        else_16 = rail.EmptyOperator(
            task_id='else_16'
        )

        if_request_regulartemp_equals_to_r_17 = rail.IfOperator(
            task_id='if_request_regulartemp_equals_to_r_17',
            test="{{ dag_run.conf.regulartemp == 'R' and dag_run.conf.fullparttime == 'F' }}",
            yes_task="update_variable_18",
            no_task="invoke_custom_ruby_code_19"
        )

        update_variable_18 = rail.SetVariableOperator(
            task_id='update_variable_18',
            append=False,
            name='{{ result("declare_variable_5").name }}',
            value="{{ dag_run.conf.newmapperlookup }} + '/RF'"
        )

        invoke_custom_ruby_code_19 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_19',
            python_callable=python_callable_method.get_date_based_on_userstatus
        )

        log_sicktimeofftypebasedonthepaygroup_20 = rail.PythonOperator(
            task_id='log_sicktimeofftypebasedonthepaygroup_20',
            python_callable=python_callable_method.get_sicktimeoff_based_on_paygroup,
            op_args=['{{ dag_run.conf.paygroup }}']
        )

        declare_list_21 = rail.SetVariableOperator(
            task_id='declare_list_21',
            append=False,
            name='timeoffnamelist',
            value=[]
        )

        declare_list_22 = rail.SetVariableOperator(
            task_id='declare_list_22',
            append=False,
            name='Time off uri',
            value=[]
        )

        declare_list_23 = rail.SetVariableOperator(
            task_id='declare_list_23',
            append=False,
            name='previoustimeofflist',
            value=[]
        )

        if_request_flextobeassigned_equals_to_yes_24 = rail.IfOperator(
            task_id='if_request_flextobeassigned_equals_to_yes_24',
            test="{{ dag_run.conf.flextobeassigned == 'yes' }}",
            yes_task="insert_to_list_25",
            no_task="get_mapper_entries"
        )

        insert_to_list_25 = rail.SetVariableOperator(
            task_id='insert_to_list_25',
            append=True,
            name='{{ result("declare_list_21").name }}',
            value={
                "name": "FTO"
            }
        )

        insert_to_list_26 = rail.SetVariableOperator(
            task_id='insert_to_list_26',
            append=True,
            name='{{ result("declare_list_22").name }}',
            value=lambda: {
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('getenabled_time_offtypes_8'), 'displayText', "FTO", "uri", '')
            }
        )

        insert_to_list_27 = rail.SetVariableOperator(
            task_id='insert_to_list_27',
            append=True,
            name='{{ result("declare_list_22").name }}',
            value=lambda: {
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('getenabled_time_offtypes_8'), 'displayText', "Holiday", "uri", '')
            }
        )

        insert_to_list_28 = rail.SetVariableOperator(
            task_id='insert_to_list_28',
            append=True,
            name='{{ result("declare_list_22").name }}',
            value=lambda: {
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('getenabled_time_offtypes_8'), 'displayText', "Jury Duty", "uri", '')
            }
        )

        insert_to_list_29 = rail.SetVariableOperator(
            task_id='insert_to_list_29',
            append=True,
            name='{{ result("declare_list_22").name }}',
            value=lambda: {
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('getenabled_time_offtypes_8'), 'displayText', "Bereavement", "uri", '')
            }
        )

        foreach_response_28 = rail.ForEachOperator(
            task_id='foreach_response_28',
            items="{{ result('getassigned_time_offtypes_7') }}",
            start_task='foreach_timeofftypeassignmentsdetails_29',
            end_task='foreach_response_28_end'
        )

        foreach_timeofftypeassignmentsdetails_29 = rail.ForEachOperator(
            task_id='foreach_timeofftypeassignmentsdetails_29',
            items="{{ result('foreach_response_28').timeOffTypeAssignmentsDetails.timeOffTypes }}",
            start_task='is_all_timeoff_type',
            end_task='foreach_timeofftypeassignmentsdetails_29_end'
        )

        is_all_timeoff_type = rail.IfOperator(
            task_id='is_all_timeoff_type',
            test="{{ result('foreach_timeofftypeassignmentsdetails_29').name == 'PTO (RPT)' \
                or result('foreach_timeofftypeassignmentsdetails_29').name == 'PTO (RFT)' \
                    or result('foreach_timeofftypeassignmentsdetails_29').name == 'SICK CARRY-OVER' }}",
            yes_task="insert_to_list_31",
            no_task="foreach_timeofftypeassignmentsdetails_29_end"
        )

        insert_to_list_31 = rail.SetVariableOperator(
            task_id='insert_to_list_31',
            append=True,
            name='{{ result("declare_list_22").name }}',
            value={
                "uri": "{{ result('foreach_timeofftypeassignmentsdetails_29').uri }}"
            }
        )

        foreach_timeofftypeassignmentsdetails_29_end = rail.EmptyOperator(
            task_id='foreach_timeofftypeassignmentsdetails_29_end'
        )

        foreach_response_28_end = rail.EmptyOperator(
            task_id='foreach_response_28_end'
        )

        get_mapper_entries = rail.PythonOperator(
            task_id='get_mapper_entries',
            python_callable=python_callable_method.get_mapper_entries_from_adtalem_mapperfile,
            op_args=["{{ dag_run_var('Mapper Lookup') }}", 'new']
        )

        adtalem_mapper_file_search_entries_32 = rail.PythonOperator(
            task_id='adtalem_mapper_file_search_entries_32',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Time Off Types']
        )

        if_request_flextobeassigned_equals_to_no_33 = rail.IfOperator(
            task_id='if_request_flextobeassigned_equals_to_no_33',
            test="{{ dag_run.conf.flextobeassigned == 'no' }}",
            yes_task="if_entry_col3_present_34",
            no_task="log_checkifbevearement_assigned"
        )

        if_entry_col3_present_34 = rail.IfOperator(
            task_id='if_entry_col3_present_34',
            test="{{ result('adtalem_mapper_file_search_entries_32') | is_truthy }}",
            yes_task="log_removedelimiterfromthe_time_offtypeslisted_35",
            no_task="log_checkifbevearement_assigned"
        )

        log_removedelimiterfromthe_time_offtypeslisted_35 = rail.PythonOperator(
            task_id='log_removedelimiterfromthe_time_offtypeslisted_35',
            python_callable=lambda: rail.result(
                'adtalem_mapper_file_search_entries_32').split('|')
        )

        foreach_create_list_37_38 = rail.ForEachOperator(
            task_id='foreach_create_list_37_38',
            items=lambda: rail.result(
                'log_removedelimiterfromthe_time_offtypeslisted_35'),
            start_task='insert_to_list_time_offsfrom_mapper_39',
            end_task='foreach_create_list_37_38_end'
        )

        insert_to_list_time_offsfrom_mapper_39 = rail.SetVariableOperator(
            task_id='insert_to_list_time_offsfrom_mapper_39',
            append=True,
            name='{{ result("declare_list_21").name }}',
            value={
                "name": "{{ result('foreach_create_list_37_38') }}"
            }
        )

        foreach_create_list_37_38_end = rail.EmptyOperator(
            task_id='foreach_create_list_37_38_end'
        )

        get_timeoffnamelist = rail.GetVariableOperator(
            task_id='get_timeoffnamelist',
            name="{{ result('declare_list_21').name }}"
        )

        foreach_declare_list_21_40 = rail.ForEachOperator(
            task_id='foreach_declare_list_21_40',
            items=lambda: rail.result('get_timeoffnamelist')['value'],
            start_task='insert_to_list_41',
            end_task='foreach_declare_list_21_40_end'
        )

        insert_to_list_41 = rail.SetVariableOperator(
            task_id='insert_to_list_41',
            append=True,
            name='{{ result("declare_list_22").name }}',
            value=lambda: {
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('getenabled_time_offtypes_8'),
                                                            'displayText',
                                                            rail.result('foreach_declare_list_21_40')['name'], "uri")
            }
        )

        foreach_declare_list_21_40_end = rail.EmptyOperator(
            task_id='foreach_declare_list_21_40_end'
        )

        log_checkif_p_t_o_buy_upwasassigned_42 = rail.PythonOperator(
            task_id='log_checkif_p_t_o_buy_upwasassigned_42',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result(
                    "getassigned_time_offtypes_7")[0]['timeOffTypeAssignmentsDetails'][
                        'timeOffTypes'], 'displayText', "PTO Buy Up", 'uri', '')
        )

        if_log_checkif_p_t_o_buy_upwasassigned_42_present_yes_43 = rail.IfOperator(
            task_id='if_log_checkif_p_t_o_buy_upwasassigned_42_present_yes_43',
            test="{{ result('log_checkif_p_t_o_buy_upwasassigned_42') | is_truthy }}",
            yes_task="insert_to_list_44",
            no_task="log_checkif_annual_leave_buy_upwasassigned_45"
        )

        insert_to_list_44 = rail.SetVariableOperator(
            task_id='insert_to_list_44',
            append=True,
            name='{{ result("declare_list_22").name }}',
            value=lambda: {
                "uri": rail.result('log_checkif_p_t_o_buy_upwasassigned_42')
            }
        )

        log_checkif_annual_leave_buy_upwasassigned_45 = rail.PythonOperator(
            task_id='log_checkif_annual_leave_buy_upwasassigned_45',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result(
                    "getassigned_time_offtypes_7")[0]['timeOffTypeAssignmentsDetails'][
                        'timeOffTypes'], 'displayText', "Annual Leave Buy Up", 'uri', '')
        )

        if_log_checkif_annual_leave_buy_upwasassigned_45_present_yes_46 = rail.IfOperator(
            task_id='if_log_checkif_annual_leave_buy_upwasassigned_45_present_yes_46',
            test="{{ result('log_checkif_annual_leave_buy_upwasassigned_45') | is_truthy }}",
            yes_task="insert_to_list_47",
            no_task="log_checkif_paid_time_off_buy_upwasassigned_48"
        )

        insert_to_list_47 = rail.SetVariableOperator(
            task_id='insert_to_list_47',
            append=True,
            name='{{ result("declare_list_22").name }}',
            value=lambda: {
                "uri": rail.result('log_checkif_annual_leave_buy_upwasassigned_45')
            }
        )

        log_checkif_paid_time_off_buy_upwasassigned_48 = rail.PythonOperator(
            task_id='log_checkif_paid_time_off_buy_upwasassigned_48',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result(
                    "getassigned_time_offtypes_7")[0]['timeOffTypeAssignmentsDetails'][
                        'timeOffTypes'], 'displayText', "Paid Time Off Buy Up", 'uri', '')
        )

        if_log_checkif_paid_time_off_buy_upwasassigned_48_present_yes_49 = rail.IfOperator(
            task_id='if_log_checkif_paid_time_off_buy_upwasassigned_48_present_yes_49',
            test="{{ result('log_checkif_paid_time_off_buy_upwasassigned_48') | is_truthy }}",
            yes_task="insert_to_list_50",
            no_task="log_checkifsickcarryoverwasassigned_51"
        )

        insert_to_list_50 = rail.SetVariableOperator(
            task_id='insert_to_list_50',
            append=True,
            name='{{ result("declare_list_22").name }}',
            value=lambda: {
                "uri": rail.result('log_checkif_paid_time_off_buy_upwasassigned_48')
            }
        )

        log_checkifsickcarryoverwasassigned_51 = rail.PythonOperator(
            task_id='log_checkifsickcarryoverwasassigned_51',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(
                rail.result(
                    "getassigned_time_offtypes_7")[0]['timeOffTypeAssignmentsDetails'][
                        'timeOffTypes'], 'displayText', "SICK CARRY-OVER", 'uri', '')
        )

        if_log_checkifsickcarryoverwasassigned_51_present_52 = rail.IfOperator(
            task_id='if_log_checkifsickcarryoverwasassigned_51_present_52',
            test="{{ result('log_checkifsickcarryoverwasassigned_51') | is_truthy }}",
            yes_task="insert_to_list_53",
            no_task="log_checkifbevearement_assigned"
        )

        insert_to_list_53 = rail.SetVariableOperator(
            task_id='insert_to_list_53',
            append=True,
            name='{{ result("declare_list_22").name }}',
            value={
                "uri": "{{ result('log_checkifsickcarryoverwasassigned_51') }}"
            }
        )

        log_checkifbevearement_assigned = rail.PythonOperator(
            task_id='log_checkifbevearement_assigned',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(
                rail.result(
                    "getassigned_time_offtypes_7")[0]['timeOffTypeAssignmentsDetails'][
                        'timeOffTypes'], 'displayText', "Bereavement", 'uri', '')
        )

        if_log_checkifbevearement_assigned_51_present_52 = rail.IfOperator(
            task_id='if_log_checkifbevearement_assigned_51_present_52',
            test="{{ result('log_checkifbevearement_assigned') | is_truthy }}",
            yes_task="insert_to_list_58",
            no_task="log_checkifjuryduty_assigned"
        )

        insert_to_list_58 = rail.SetVariableOperator(
            task_id='insert_to_list_58',
            append=True,
            name='{{ result("declare_list_22").name }}',
            value={
                "uri": "{{ result('log_checkifbevearement_assigned') }}"
            }
        )

        log_checkifjuryduty_assigned = rail.PythonOperator(
            task_id='log_checkifjuryduty_assigned',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(
                rail.result(
                    "getassigned_time_offtypes_7")[0]['timeOffTypeAssignmentsDetails'][
                        'timeOffTypes'], 'displayText', "Jury Duty", 'uri', '')
        )

        if_log_checkifjuryduty_assigned_51_present_52 = rail.IfOperator(
            task_id='if_log_checkifjuryduty_assigned_51_present_52',
            test="{{ result('log_checkifjuryduty_assigned') | is_truthy }}",
            yes_task="insert_to_list_61",
            no_task="final_set_timeoff_uris"
        )

        insert_to_list_61 = rail.SetVariableOperator(
            task_id='insert_to_list_61',
            append=True,
            name='{{ result("declare_list_22").name }}',
            value={
                "uri": "{{ result('log_checkifjuryduty_assigned') }}"
            }
        )

        final_set_timeoff_uris = rail.GetVariableOperator(
            task_id='final_set_timeoff_uris',
            name="{{ result('declare_list_22').name }}"
        )

        if_log_finalsetof_time_offuris_54_present_55 = rail.IfOperator(
            task_id='if_log_finalsetof_time_offuris_54_present_55',
            test="{{ result('final_set_timeoff_uris').value | length > 0 }}",
            yes_task="assignrequired_timeofftypes_57",
            no_task="foreach_response_73"
        )

        assignrequired_timeofftypes_57 = rail.RepliconServiceOperator(
            task_id='assignrequired_timeofftypes_57',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('final_set_timeoff_uris')
            }
        )

        foreach_declare_list_21_58 = rail.ForEachOperator(
            task_id='foreach_declare_list_21_58',
            items=lambda: rail.result('get_timeoffnamelist')['value'],
            start_task='is_newsickleavetimeoffpolicyassignnmentfor_asiaor_a_u_sgroups',
            end_task='foreach_declare_list_21_58_end'
        )

        is_newsickleavetimeoffpolicyassignnmentfor_asiaor_a_u_sgroups = rail.IfOperator(
            task_id='is_newsickleavetimeoffpolicyassignnmentfor_asiaor_a_u_sgroups',
            test="{{ result('foreach_declare_list_21_58').name == result('log_sicktimeofftypebasedonthepaygroup_20') }}",
            yes_task="log_ifsickleavetobeassignedisassignedalready_60",
            no_task="if_foreach_611f2efb_58_name_equals_to_fto_68"
        )

        log_ifsickleavetobeassignedisassignedalready_60 = rail.PythonOperator(
            task_id='log_ifsickleavetobeassignedisassignedalready_60',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('getassigned_time_offtypes_7'), 'displayText', rail.result(
                    'foreach_declare_list_21_58')['name'], 'uri', '')
        )

        if_log_ifsickleavetobeassignedisassignedalready_60_blank_61 = rail.IfOperator(
            task_id='if_log_ifsickleavetobeassignedisassignedalready_60_blank_61',
            test="{{ result('log_ifsickleavetobeassignedisassignedalready_60') | is_falsy }}",
            yes_task="log_sickleavemeoffuri_62",
            no_task="if_foreach_611f2efb_58_name_equals_to_fto_68"
        )

        log_sickleavemeoffuri_62 = rail.PythonOperator(
            task_id='log_sickleavemeoffuri_62',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('getassigned_time_offtypes_7'), 'displayText', rail.result(
                    'log_sicktimeofftypebasedonthepaygroup_20'), 'uri', '')
        )

        if_log_sickleavemeoffuri_62_present_63 = rail.IfOperator(
            task_id='if_log_sickleavemeoffuri_62_present_63',
            test="{{ result('log_sickleavemeoffuri_62') | is_truthy }}",
            yes_task="get_default_time_off_type_policy_schedule_for_user_64",
            no_task="if_foreach_611f2efb_58_name_equals_to_fto_68"
        )

        get_default_time_off_type_policy_schedule_for_user_64 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_64',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('log_sickleavemeoffuri_62') }}"
                }
            }
        )

        if_effectivedate_day_present_65 = rail.IfOperator(
            task_id='if_effectivedate_day_present_65',
            test="{{ result('get_default_time_off_type_policy_schedule_for_user_64') | \
                first_or_default | attr_or_default('effectiveDate.day') | is_truthy }}",
            yes_task="log_globalpolicy_66",
            no_task="if_foreach_611f2efb_58_name_equals_to_fto_68"
        )

        log_globalpolicy_66 = rail.PythonOperator(
            task_id='log_globalpolicy_66',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_64'), ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"'))
        )

        put_user_time_off_account_policy_set_schedule_67 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_67',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('log_sickleavemeoffuri_62')
                },
                "policySetScheduleEntries": rail.result('log_globalpolicy_66')
            }
        )

        if_foreach_611f2efb_58_name_equals_to_fto_68 = rail.IfOperator(
            task_id='if_foreach_611f2efb_58_name_equals_to_fto_68',
            test="{{ result('foreach_declare_list_21_58').name == 'FTO' }}",
            yes_task="log_f_t_otmeoffuri_69",
            no_task="foreach_declare_list_21_58_end"
        )

        log_f_t_otmeoffuri_69 = rail.PythonOperator(
            task_id='log_f_t_otmeoffuri_69',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('getenabled_time_offtypes_8'), 'displayText', "FTO", 'uri')
        )

        put_default_time_off_type_for_bookings_for_user_70 = rail.RepliconServiceOperator(
            task_id='put_default_time_off_type_for_bookings_for_user_70',
            endpoint="/services/TimeOffService1.svc/PutDefaultTimeOffTypeForBookingsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeOffTypeUri": "{{ result('log_f_t_otmeoffuri_69') }}"
            }
        )

        foreach_declare_list_21_58_end = rail.EmptyOperator(
            task_id='foreach_declare_list_21_58_end'
        )

        foreach_response_73 = rail.ForEachOperator(
            task_id='foreach_response_73',
            items=lambda: rail.result('getassigned_time_offtypes_7'),
            start_task='foreach_timeofftypeassignmentsdetails_74',
            end_task='foreach_response_73_end'
        )

        foreach_timeofftypeassignmentsdetails_74 = rail.ForEachOperator(
            task_id='foreach_timeofftypeassignmentsdetails_74',
            items=lambda: rail.result('foreach_response_73')[
                'timeOffTypeAssignmentsDetails']['timeOffTypes'],
            start_task='insert_to_list_urispreviouslyassigned_75',
            end_task='foreach_timeofftypeassignmentsdetails_74_end'
        )

        insert_to_list_urispreviouslyassigned_75 = rail.SetVariableOperator(
            task_id='insert_to_list_urispreviouslyassigned_75',
            append=True,
            name='{{ result("declare_list_23").name }}',
            value={
                "uri": "{{ result('foreach_timeofftypeassignmentsdetails_74').uri }}",
                "name": "{{ result('foreach_timeofftypeassignmentsdetails_74').displayText }}"
            }
        )

        foreach_timeofftypeassignmentsdetails_74_end = rail.EmptyOperator(
            task_id='foreach_timeofftypeassignmentsdetails_74_end'
        )

        foreach_response_73_end = rail.EmptyOperator(
            task_id='foreach_response_73_end'
        )

        get_previoustimeofflist = rail.GetVariableOperator(
            task_id='get_previoustimeofflist',
            name='previoustimeofflist'
        )

        foreach_declare_list_23_76 = rail.ForEachOperator(
            task_id='foreach_declare_list_23_76',
            items=lambda: rail.result('get_previoustimeofflist')['value'],
            start_task='is_timeoff_not_assigned_in_new_set',
            end_task='foreach_declare_list_23_76_end'
        )

        is_timeoff_not_assigned_in_new_set = rail.IfOperator(
            task_id='is_timeoff_not_assigned_in_new_set',
            test="{{ dag_run_var(result('declare_list_22').name) | \
                find_first_by_attr_and_get_attr('uri', result('foreach_declare_list_23_76').uri, 'uri', '') | \
                is_falsy }}",
            yes_task="trigger_dag_run_live_put_0_balance_for_update_users_cr14_079",
            no_task="if_request_flextobeassigned_equals_to_yes_80"
        )

        trigger_dag_run_live_put_0_balance_for_update_users_cr14_079 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_put_0_balance_for_update_users_cr14_079',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_put_0_balance_for_update_users_cr14.0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "timeoffuri": "{{ result('foreach_declare_list_23_76').uri }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "terminationdate": "{{ current_time('%m/%d/%Y') }}"
            }
        )

        foreach_declare_list_23_76_end = rail.EmptyOperator(
            task_id='foreach_declare_list_23_76_end'
        )

        if_request_flextobeassigned_equals_to_yes_80 = rail.IfOperator(
            task_id='if_request_flextobeassigned_equals_to_yes_80',
            test="{{ dag_run.conf.flextobeassigned == 'yes' }}",
            yes_task="foreach_declare_list_23_81",
            no_task="foreach_declare_list_23_81_end"
        )

        foreach_declare_list_23_81 = rail.ForEachOperator(
            task_id='foreach_declare_list_23_81',
            items="{{ dag_run_var('previoustimeofflist') }}",
            start_task='if_name_equals_ptorpt_timeofftype',
            end_task='foreach_declare_list_23_81_end'
        )

        if_name_equals_ptorpt_timeofftype = rail.IfOperator(
            task_id='if_name_equals_ptorpt_timeofftype',
            test="{{ result('foreach_declare_list_23_81').name == 'PTO (RPT)' \
                or result('foreach_declare_list_23_81').name == 'PTO (RFT)' or \
                    result('foreach_declare_list_23_81').name == 'SICK CARRY-OVER' }}",
            yes_task="trigger_dag_run_live_put_blank_balance_for_update_users_cr14_083",
            no_task="foreach_declare_list_23_81_end"
        )

        trigger_dag_run_live_put_blank_balance_for_update_users_cr14_083 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_put_blank_balance_for_update_users_cr14_083',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_put_0_balance_for_update_users_cr14.0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "timeoffuri": "{{ result('foreach_declare_list_23_81').uri }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "terminationdate": "{{ current_time('%m/%d/%Y') }}"
            }
        )

        foreach_declare_list_23_81_end = rail.EmptyOperator(
            task_id='foreach_declare_list_23_81_end'
        )

        def get_templated_value(value):
            return value
        log_checkif_floating_holidaysisassigned_84 = rail.PythonOperator(
            task_id='log_checkif_floating_holidaysisassigned_84',
            python_callable=get_templated_value,
            op_args=["{{ dag_run_var(result('declare_list_21').name) | \
                find_first_by_attr_and_get_attr('name', 'Floating Holidays', 'name', '') }}"]
        )

        log_checkif_anniversary_dayassigned_85 = rail.PythonOperator(
            task_id='log_checkif_anniversary_dayassigned_85',
            python_callable=get_templated_value,
            op_args=["{{ dag_run_var(result('declare_list_21').name) | \
                find_first_by_attr_and_get_attr('name', 'Anniversary Day', 'name', '') }}"]
        )

        if_log_checkif_anniversary_dayassigned_85_present_86 = rail.IfOperator(
            task_id='if_log_checkif_anniversary_dayassigned_85_present_86',
            test="{{ result('log_checkif_anniversary_dayassigned_85') | is_truthy }}",
            yes_task="log_checkif_anniversary_dayisalreadyassignedandenabled_87",
            no_task="if_log_checkif_floating_holidaysisassigned_84_present_91"
        )

        log_checkif_anniversary_dayisalreadyassignedandenabled_87 = rail.PythonOperator(
            task_id='log_checkif_anniversary_dayisalreadyassignedandenabled_87',
            python_callable=get_templated_value,
            op_args=["{{ result('getassigned_time_offtypes_7') | first_or_default | \
                attr_or_default('timeOffTypeAssignmentsDetails.timeOffTypes') | \
                    find_first_by_attr_and_get_attr('displayText', 'Anniversary Day' , 'uri', '') }}"]
        )

        if_log_checkif_anniversary_dayisalreadyassignedandenabled_87_blank_88 = rail.IfOperator(
            task_id='if_log_checkif_anniversary_dayisalreadyassignedandenabled_87_blank_88',
            test="{{ result('log_checkif_anniversary_dayisalreadyassignedandenabled_87') | is_falsy }}",
            yes_task="log_time_offurifor_anniversary_day_89",
            no_task="if_log_checkif_floating_holidaysisassigned_84_present_91"
        )

        log_time_offurifor_anniversary_day_89 = rail.PythonOperator(
            task_id='log_time_offurifor_anniversary_day_89',
            python_callable=get_templated_value,
            op_args=["{{ result('getenabled_time_offtypes_8') | \
                find_first_by_attr_and_get_attr('displayText', 'Anniversary Day' , 'uri', '') }}"]
        )

        trigger_dag_run_live_timeoff_anniversary_day_policy_add_update_cr14_090 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_timeoff_anniversary_day_policy_add_update_cr14_090',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_timeoff_anniversaryday_policyaddupdate_cr14.0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "lastname": "{{ dag_run.conf.lastname }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "jobcode": "{{ dag_run.conf.jobcode }}",
                "paygroup": "{{ dag_run.conf.paygroup }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "rehiredate": "{{ dag_run.conf.rehiredate }}",
                "servicedate": "{{ dag_run.conf.servicedate }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "ususer": "{{ dag_run.conf.ususer }}",
                "type": "{{ dag_run.conf.type }}",
                "timeoffuri": "{{ result('log_time_offurifor_anniversary_day_89') }}"
            }
        )

        if_log_checkif_floating_holidaysisassigned_84_present_91 = rail.IfOperator(
            task_id='if_log_checkif_floating_holidaysisassigned_84_present_91',
            test="{{ result('log_checkif_floating_holidaysisassigned_84') | is_truthy }}",
            yes_task="log_checkif_floating_holidaysisalreadyassignedandenabled_92",
            no_task="if_request_ususer_equals_to_no_156"
        )

        log_checkif_floating_holidaysisalreadyassignedandenabled_92 = rail.PythonOperator(
            task_id='log_checkif_floating_holidaysisalreadyassignedandenabled_92',
            python_callable=get_templated_value,
            op_args=["{{ result('getassigned_time_offtypes_7') | first_or_default | \
                attr_or_default('timeOffTypeAssignmentsDetails.timeOffTypes') | \
                    find_first_by_attr_and_get_attr('displayText', 'Floating Holidays' , 'uri', '') }}"]
        )

        if_log_checkif_floating_holidaysisalreadyassignedandenabled_92_blank_93 = rail.IfOperator(
            task_id='if_log_checkif_floating_holidaysisalreadyassignedandenabled_92_blank_93',
            test="{{ result('log_checkif_floating_holidaysisalreadyassignedandenabled_92') | is_falsy }}",
            yes_task="update_variable_94",
            no_task="if_request_fullparttime_not_equals_to_dataworkato_service0fafa311requestpreviousfull_parttime_value_95"
        )

        update_variable_94 = rail.SetVariableOperator(
            task_id='update_variable_94',
            append=False,
            name='{{ result("declare_variable_2").name }}',
            value='yes'
        )

        if_request_fullparttime_not_equals_to_dataworkato_service0fafa311requestpreviousfull_parttime_value_95 = rail.IfOperator(
            task_id='if_request_fullparttime_not_equals_to_dataworkato_service0fafa311requestpreviousfull_parttime_value_95',
            test="{{ dag_run.conf.fullparttime != dag_run.conf.previousfull_parttime_value }}",
            yes_task="update_variable_96",
            no_task="if_request_userstatus_equals_to_disabled_97"
        )

        update_variable_96 = rail.SetVariableOperator(
            task_id='update_variable_96',
            append=False,
            name='{{ result("declare_variable_2").name }}',
            value='yes'
        )

        if_request_userstatus_equals_to_disabled_97 = rail.IfOperator(
            task_id='if_request_userstatus_equals_to_disabled_97',
            test="{{ dag_run.conf.userstatus == 'Disabled' }}",
            yes_task="update_variable_98",
            no_task="if_declare_variable_2_value_equals_to_yes_99"
        )

        update_variable_98 = rail.SetVariableOperator(
            task_id='update_variable_98',
            append=False,
            name='{{ result("declare_variable_2").name }}',
            value='yes'
        )

        if_declare_variable_2_value_equals_to_yes_99 = rail.IfOperator(
            task_id='if_declare_variable_2_value_equals_to_yes_99',
            test="{{ dag_run_var(result('declare_variable_2').name) == 'yes' }}",
            yes_task="declare_list_100",
            no_task="if_request_ususer_equals_to_no_156"
        )

        declare_list_100 = rail.SetVariableOperator(
            task_id='declare_list_100',
            append=False,
            name='policyset',
            value=[]
        )

        log_time_offurifor_floating_holidays_101 = rail.PythonOperator(
            task_id='log_time_offurifor_floating_holidays_101',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getenabled_time_offtypes_8'), 'displayText', 'Floating Holidays', 'uri', '')
        )

        getassignedpolicyforthetimeofftype_103 = rail.RepliconServiceOperator(
            task_id='getassignedpolicyforthetimeofftype_103',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_existing_policy_schedule_106 = rail.PythonOperator(
            task_id='log_existing_policy_schedule_106',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getassignedpolicyforthetimeofftype_103')['policiesByTimeOffType'], 'timeOffType.uri', rail.result(
                    'log_time_offurifor_floating_holidays_101'), 'policySetSchedule')
        )

        if_to_s_contains_urn_107 = rail.IfOperator(
            task_id='if_to_s_contains_urn_107',
            test="{{ result('log_existing_policy_schedule_106') | to_json | matches('urn') }}",
            yes_task="foreach_document_109",
            no_task="getdefaultpolicyfor_flotingholidays_119"
        )

        foreach_document_109 = rail.ForEachOperator(
            task_id='foreach_document_109',
            items=lambda: rail.result('log_existing_policy_schedule_106'),
            start_task='foreach_foreach_document_109_110',
            end_task='foreach_document_109_end'
        )

        foreach_foreach_document_109_110 = rail.ForEachOperator(
            task_id='foreach_foreach_document_109_110',
            items="{{ result('foreach_document_109') }}",
            start_task='log_effectivedate_111',
            end_task='foreach_foreach_document_109_110_end'
        )

        def dateformatmmddyyyy_to_time_less_than_today(effectivedate):
            effective_date = datetime.strptime(effectivedate, '%m/%d/%Y')
            return effective_date < datetime.now()
        log_effectivedate_111 = rail.PythonOperator(
            task_id='log_effectivedate_111',
            python_callable=dateformatmmddyyyy_to_time_less_than_today,
            # pylint: disable=line-too-long
            op_args=["{{ result('foreach_foreach_document_109_110').effectiveDate.month }}/{{ result('foreach_foreach_document_109_110').effectiveDate.day }}/{{ result('foreach_foreach_document_109_110').effectiveDate.year }}"]
        )

        if_to_dateformatmmddyyyy_to_time_less_than_today_112 = rail.IfOperator(
            task_id='if_to_dateformatmmddyyyy_to_time_less_than_today_112',
            test="{{ result('log_effectivedate_111') | is_truthy }}",
            yes_task="accumulate_list_items_113",
            no_task="foreach_foreach_document_109_110_end"
        )

        accumulate_list_items_113 = rail.SetVariableOperator(
            task_id='accumulate_list_items_113',
            name='previous policies',
            append=True,
            value=lambda: {
                "description": rail.result('foreach_foreach_document_109_110')['description'],
                "effective_date": rail.result('foreach_foreach_document_109_110')['effectiveDate'],
                "policy_set": rail.result('foreach_foreach_document_109_110')['policySet']
            }
        )

        foreach_foreach_document_109_110_end = rail.EmptyOperator(
            task_id='foreach_foreach_document_109_110_end'
        )

        foreach_document_109_end = rail.EmptyOperator(
            task_id='foreach_document_109_end'
        )

        previous_policies_floating_holiday_trigger = rail.GetVariableOperator(
            task_id='previous_policies_floating_holiday_trigger',
            name='previous policies'
        )

        log_previouspoliciesmodified_final_117 = rail.PythonOperator(
            task_id='log_previouspoliciesmodified_final_117',
            python_callable=lambda: json.loads(json.dumps(rail.result(
                'previous_policies_floating_holiday_trigger')['value'], ensure_ascii=False).replace(
                    'null', '"effective"').replace('"script"', '"scriptTarget"'))
        )

        getdefaultpolicyfor_flotingholidays_119 = rail.RepliconServiceOperator(
            task_id='getdefaultpolicyfor_flotingholidays_119',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('log_time_offurifor_floating_holidays_101') }}"
            }
        )

        if_request_fullparttime_equals_to_f_122 = rail.IfOperator(
            task_id='if_request_fullparttime_equals_to_f_122',
            test="{{ dag_run.conf.fullparttime == 'F' }}",
            yes_task="log_policysetmodified_123",
            no_task="if_request_ususer_equals_to_no_156"
        )

        log_policysetmodified_123 = rail.PythonOperator(
            task_id='log_policysetmodified_123',
            python_callable=lambda: json.loads(json.dumps(rail.result(
                'getdefaultpolicyfor_flotingholidays_119')[0]['policySet'], ensure_ascii=False).replace('"script"', '"scriptTarget"'))
        )

        if_to_s_contains_urn_125 = rail.IfOperator(
            task_id='if_to_s_contains_urn_125',
            test="{{ result('log_previouspoliciesmodified_final_117') | is_truthy }}",
            yes_task="assign_floatingholidayspolicy_126",
            no_task="else_131"
        )

        assign_floatingholidayspolicy_126 = rail.RepliconServiceOperator(
            task_id='assign_floatingholidayspolicy_126',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('log_time_offurifor_floating_holidays_101')
                },
                "policySetScheduleEntries": [rail.result('log_previouspoliciesmodified_final_117')] + [
                    {
                        "effectiveDate": get_today_date(),
                        "description": f"Effective on {get_today_date()['month']}-{get_today_date()['day']}-{get_today_date()['year']}",
                        "policySet": rail.result('log_policysetmodified_123')
                    }
                ]
            }
        )

        else_127 = rail.EmptyOperator(
            task_id='else_127'
        )

        assign_floatingholidayspolicy_128 = rail.RepliconServiceOperator(
            task_id='assign_floatingholidayspolicy_128',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('log_time_offurifor_floating_holidays_101')
                },
                "policySetScheduleEntries": [
                    {
                        "effectiveDate": get_today_date(),
                        "description": f"Effective on {get_today_date()['month']}-{get_today_date()['day']}-{get_today_date()['year']}",
                        "policySet": rail.result('log_policysetmodified_123')
                    }
                ]
            }
        )

        else_131 = rail.EmptyOperator(
            task_id='else_131'
        )

        log_policysetmodified_132 = rail.PythonOperator(
            task_id='log_policysetmodified_132',
            python_callable=get_templated_value,
            op_args=[
                "{{ result('getdefaultpolicyfor_flotingholidays_119') | attr_or_default('policySet') | first_or_default }}"]
        )

        log_yearlyentitilement_133 = rail.PythonOperator(
            task_id='log_yearlyentitilement_133',
            python_callable=get_templated_value,
            op_args=["{{ result('getdefaultpolicyfor_flotingholidays_119') | attr_or_default('policySet') | \
                attr_or_default('timeOffBalanceEventScripts') | find_first_by_attr_and_get_attr('script.name', 'Yearly Accrual') }}"]
        )

        log_yearlyaccrualpolicy_135 = rail.PythonOperator(
            task_id='log_yearlyaccrualpolicy_135',
            python_callable=lambda: json.loads(json.dumps(rail.result(
                    'log_yearlyentitilement_133'), ensure_ascii=False).replace(
                'null', '"effective"').replace('"script"', '"scriptTarget"'))
        )

        foreach_document_137 = rail.ForEachOperator(
            task_id='foreach_document_137',
            items=lambda: rail.result('log_yearlyaccrualpolicy_135'),
            start_task='foreach_foreach_document_137_138',
            end_task='foreach_document_137_end'
        )

        foreach_foreach_document_137_138 = rail.ForEachOperator(
            task_id='foreach_foreach_document_137_138',
            items="{{ result('foreach_document_137').additionalParameters }}",
            start_task='if_foreach_1a732118_138_keyuri_equals_to_urnrepliconscriptkeyparameteraccrualannualamount_139',
            end_task='foreach_foreach_document_137_138_end'
        )

        if_foreach_1a732118_138_keyuri_equals_to_urnrepliconscriptkeyparameteraccrualannualamount_139 = rail.IfOperator(
            task_id='if_foreach_1a732118_138_keyuri_equals_to_urnrepliconscriptkeyparameteraccrualannualamount_139',
            test="{{ result('foreach_foreach_document_137_138').keyUri == 'urn:replicon:script-key:parameter:accrual-annual-amount' }}",
            yes_task="log_yearlyaccrualvalue_140",
            no_task="foreach_foreach_document_137_138_end"
        )

        log_yearlyaccrualvalue_140 = rail.PythonOperator(
            task_id='log_yearlyaccrualvalue_140',
            python_callable=lambda: rail.result('foreach_foreach_document_137_138')[
                'value']['number']
        )

        foreach_foreach_document_137_138_end = rail.EmptyOperator(
            task_id='foreach_foreach_document_137_138_end'
        )

        foreach_document_137_end = rail.EmptyOperator(
            task_id='foreach_document_137_end'
        )

        log_yearlyaccrualexistingvalue_141 = rail.PythonOperator(
            task_id='log_yearlyaccrualexistingvalue_141',
            # pylint: disable=line-too-long
            python_callable=lambda: json.dumps({"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {
                                               "number": "{{ result('log_yearlyaccrualvalue_140') }}"}}, ensure_ascii=False)
        )

        log_yearlyaccrualnewvalue_142 = rail.PythonOperator(
            task_id='log_yearlyaccrualnewvalue_142',
            python_callable=lambda: json.dumps(
                {"keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount", "value": {"number": 8.0}}, ensure_ascii=False)
        )

        log_newpolocywiththevalue_143 = rail.PythonOperator(
            task_id='log_newpolocywiththevalue_143',
            python_callable=lambda: json.loads(json.dumps(rail.result(
                    'log_policysetmodified_132'), ensure_ascii=False).replace(
                'null', '"effective"').replace('"script"', '"scriptTarget"').replace(
                rail.result('log_yearlyaccrualexistingvalue_141'), rail.result('log_yearlyaccrualnewvalue_142')))
        )

        insert_to_list_145 = rail.SetVariableOperator(
            task_id='insert_to_list_145',
            append=True,
            name='{{ result("declare_list_100").name }}',
            # pylint: disable=line-too-long
            value=lambda: {
                "description": f"Effective from {rail.result('invoke_custom_ruby_code_19')['month'] }/{rail.result('invoke_custom_ruby_code_19')['day'] }/{rail.result('invoke_custom_ruby_code_19')['year'] }",
                "effectiveDate": {
                    "day": rail.result('invoke_custom_ruby_code_19')['day'],
                    "month": rail.result('invoke_custom_ruby_code_19')['month'],
                    "year": rail.result('invoke_custom_ruby_code_19')['year']
                },
                "policySet": rail.result('log_newpolocywiththevalue_143')
            }
        )

        log_finalpolicywithnewvaluewithpreviouspolicy_148 = rail.GetVariableOperator(
            task_id='log_finalpolicywithnewvaluewithpreviouspolicy_148',
            name="{{ result('declare_list_100').name }}"
        )

        if_to_s_contains_urn_150 = rail.IfOperator(
            task_id='if_to_s_contains_urn_150',
            test="{{ result('log_previouspoliciesmodified_final_117') | is_truthy }}",
            yes_task="assign_floatingholidayspolicy_151",
            no_task="else_152"
        )

        assign_floatingholidayspolicy_151 = rail.RepliconServiceOperator(
            task_id='assign_floatingholidayspolicy_151',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('log_time_offurifor_floating_holidays_101')
                },
                "policySetScheduleEntries": [
                    rail.result('log_previouspoliciesmodified_final_117')] + rail.result(
                        'log_finalpolicywithnewvaluewithpreviouspolicy_148')['value']
            }
        )

        else_152 = rail.EmptyOperator(
            task_id='else_152'
        )

        assign_floatingholidayspolicy_153 = rail.RepliconServiceOperator(
            task_id='assign_floatingholidayspolicy_153',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('log_time_offurifor_floating_holidays_101')
                },
                "policySetScheduleEntries": rail.result('log_finalpolicywithnewvaluewithoutpreviouspolicy_146')['value']
            }
        )

        if_request_ususer_equals_to_no_156 = rail.IfOperator(
            task_id='if_request_ususer_equals_to_no_156',
            test="{{ dag_run.conf.ususer == 'no' }}",
            yes_task="if_request_paygroup_equals_to_acaau_157",
            no_task="if_request_ususer_equals_to_yes_200"
        )

        if_request_paygroup_equals_to_acaau_157 = rail.IfOperator(
            task_id='if_request_paygroup_equals_to_acaau_157',
            test="{{ dag_run.conf.paygroup == 'ACAAU' \
                or dag_run.conf.paygroup == 'ACADE' \
                    or dag_run.conf.paygroup == 'ACAFR' \
                        or dag_run.conf.paygroup == 'ACAUK' }}",
            yes_task="log_time_offurifor_paid_time_off_158",
            no_task="if_request_paygroup_equals_to_acatw_186"
        )

        log_time_offurifor_paid_time_off_158 = rail.PythonOperator(
            task_id='log_time_offurifor_paid_time_off_158',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getenabled_time_offtypes_8'), 'displayText', 'Paid Time Off', 'uri', '')
        )

        log_checkif_p_t_oisassigned_159 = rail.PythonOperator(
            task_id='log_checkif_p_t_oisassigned_159',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'getassigned_time_offtypes_7'), 'name', 'Paid Time Off', 'name', '')
        )

        getdefaultpolicyfor_p_t_o_161 = rail.RepliconServiceOperator(
            task_id='getdefaultpolicyfor_p_t_o_161',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('log_time_offurifor_paid_time_off_158') }}"
            }
        )

        log_policysetmodified_162 = rail.PythonOperator(
            task_id='log_policysetmodified_162',
            python_callable=lambda: json.loads(json.dumps(
                rail.result('getdefaultpolicyfor_p_t_o_161')[0]['policySet'], ensure_ascii=False).replace('"script"', '"scriptTarget"'))
        )

        getassignedpolicyforthetimeofftype_163 = rail.RepliconServiceOperator(
            task_id='getassignedpolicyforthetimeofftype_163',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_existing_policy_schedule_164 = rail.PythonOperator(
            task_id='log_existing_policy_schedule_164',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getassignedpolicyforthetimeofftype_163')['policiesByTimeOffType'], 'timeOffType.uri', rail.result(
                    'log_time_offurifor_paid_time_off_158'), 'policySetSchedule', '')
        )

        if_log_checkif_p_t_oisassigned_159_present_167 = rail.IfOperator(
            task_id='if_log_checkif_p_t_oisassigned_159_present_167',
            test="{{ result('log_checkif_p_t_oisassigned_159') | is_truthy }}",
            yes_task="if_to_s_contains_urn_168",
            no_task="if_log_previouspoliciesmodified_final_178_contains_description_180"
        )

        if_to_s_contains_urn_168 = rail.IfOperator(
            task_id='if_to_s_contains_urn_168',
            test="{{ result('log_existing_policy_schedule_164') | is_truthy }}",
            yes_task="foreach_document_170",
            no_task="if_request_paygroup_equals_to_acatw_186"
        )

        foreach_document_170 = rail.ForEachOperator(
            task_id='foreach_document_170',
            items="{{ result('log_existing_policy_schedule_164') }}",
            start_task='foreach_foreach_document_170_171',
            end_task='foreach_document_170_end'
        )

        foreach_foreach_document_170_171 = rail.ForEachOperator(
            task_id='foreach_foreach_document_170_171',
            items="{{ result('foreach_document_170') }}",
            start_task='log_effectivedate_172',
            end_task='foreach_foreach_document_170_171_end'
        )

        log_effectivedate_172 = rail.PythonOperator(
            task_id='log_effectivedate_172',
            python_callable=dateformatmmddyyyy_to_time_less_than_today,
            # pylint: disable=line-too-long
            op_args=["{{ result('foreach_foreach_document_170_171').effectiveDate.month }}/{{ result('foreach_foreach_document_170_171').effectiveDate.day }}/{{ result('foreach_foreach_document_170_171').effectiveDate.year }}"]
        )

        if_to_dateformatmmddyyyy_to_time_less_than_today_173 = rail.IfOperator(
            task_id='if_to_dateformatmmddyyyy_to_time_less_than_today_173',
            test="{{ result('log_effectivedate_172') | is_truthy }}",
            yes_task="accumulate_list_items_174",
            no_task="foreach_foreach_document_170_171_end"
        )

        accumulate_list_items_174 = rail.SetVariableOperator(
            task_id='accumulate_list_items_174',
            name='previous policies',
            append=True,
            value=lambda: {
                "description": rail.result('foreach_a6e2e372_171')['description'],
                "effective_date": rail.result('foreach_a6e2e372_171')['effectiveDate'],
                "policy_set": rail.result('foreach_a6e2e372_171')['policySet']
            }
        )

        foreach_foreach_document_170_171_end = rail.EmptyOperator(
            task_id='foreach_foreach_document_170_171_end'
        )

        foreach_document_170_end = rail.EmptyOperator(
            task_id='foreach_document_170_end'
        )

        previous_policies_pto_trigger = rail.GetVariableOperator(
            task_id='previous_policies_pto_trigger',
            name='previous policies'
        )

        if_log_previouspoliciesmodified_final_178_contains_description_180 = rail.IfOperator(
            task_id='if_log_previouspoliciesmodified_final_178_contains_description_180',
            test="{{ result('previous_policies_pto_trigger') | to_json | matches('description') }}",
            yes_task="assign_paid_time_off_policy_181",
            no_task="if_request_ususer_equals_to_yes_200"
        )

        assign_paid_time_off_policy_181 = rail.RepliconServiceOperator(
            task_id='assign_paid_time_off_policy_181',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('log_time_offurifor_paid_time_off_158')
                },
                "policySetScheduleEntries": [rail.result('previous_policies_pto_trigger')] + [
                    {
                        "effectiveDate": get_today_date(),
                        "description": f"Effective on {get_today_date()['month']}-{get_today_date()['day']}-{get_today_date()['year']}",
                        "policySet": rail.result('log_policysetmodified_162')
                    }
                ]
            }
        )

        else_182 = rail.EmptyOperator(
            task_id='else_182'
        )

        assign_paid_time_off_policy_183 = rail.RepliconServiceOperator(
            task_id='assign_paid_time_off_policy_183',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('log_time_offurifor_paid_time_off_158')
                },
                "policySetScheduleEntries": [
                    {
                        "effectiveDate": get_today_date(),
                        "description": f"Effective on {get_today_date()['month']}-{get_today_date()['day']}-{get_today_date()['year']}",
                        "policySet": rail.result('log_policysetmodified_162')
                    }
                ]
            }
        )

        if_request_paygroup_equals_to_acatw_186 = rail.IfOperator(
            task_id='if_request_paygroup_equals_to_acatw_186',
            test="{{ dag_run.conf.paygroup == 'ACATW' \
                or dag_run.conf.paygroup == 'ACASG' \
                    or dag_run.conf.paygroup == 'ACAJP' \
                        or dag_run.conf.paygroup == 'HK' \
                            or dag_run.conf.paygroup == 'ACACH' \
                                or dag_run.conf.paygroup == 'ACAIN' \
                                    or dag_run.conf.paygroup == 'ACAAS' }}",
            yes_task="if_log_sicktimeofftypebasedonthepaygroup_20_present_187",
            no_task="if_request_ususer_equals_to_yes_200"
        )

        if_log_sicktimeofftypebasedonthepaygroup_20_present_187 = rail.IfOperator(
            task_id='if_log_sicktimeofftypebasedonthepaygroup_20_present_187',
            test="{{ result('log_sicktimeofftypebasedonthepaygroup_20') | is_truthy }}",
            yes_task="log_checkif_sick_leaveisalreadyassignedandenabled_188",
            no_task="log_annualleavetimeofftypebasedonthepaygroup_191"
        )

        log_checkif_sick_leaveisalreadyassignedandenabled_188 = rail.PythonOperator(
            task_id='log_checkif_sick_leaveisalreadyassignedandenabled_188',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getassigned_time_offtypes_7')[0]['timeOffTypeAssignmentsDetails']['timeOffTypes'], 'displayText', rail.result(
                    'log_sicktimeofftypebasedonthepaygroup_20'), 'uri', '')
        )

        if_log_checkif_sick_leaveisalreadyassignedandenabled_188_present_189 = rail.IfOperator(
            task_id='if_log_checkif_sick_leaveisalreadyassignedandenabled_188_present_189',
            test="{{ result('log_checkif_sick_leaveisalreadyassignedandenabled_188') | is_truthy and dag_run.conf.type == 'Rehire' }}",
            yes_task="trigger_dag_run_live_timeoff_sick_leave_aus_asia_policy_rehire_update_cr2021_v1190",
            no_task="log_annualleavetimeofftypebasedonthepaygroup_191"
        )

        trigger_dag_run_live_timeoff_sick_leave_aus_asia_policy_rehire_update_cr2021_v1190 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_timeoff_sick_leave_aus_asia_policy_rehire_update_cr2021_v1190',
            retries=0,
            trigger_dag_id=f'adtalem_user_import_timeoff_sick_leave_aus_asia_policy_rehire_update_cr2021_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "lastname": "{{ dag_run.conf.lastname }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "jobcode": "{{ dag_run.conf.jobcode }}",
                "paygroup": "{{ dag_run.conf.paygroup }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "rehiredate": "{{ dag_run.conf.rehiredate }}",
                "servicedate": "{{ dag_run.conf.servicedate }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "ususer": "{{ dag_run.conf.ususer }}",
                "type": "Rehire",
                "timeoffuri": "{{ result('getenabled_time_offtypes_8') | \
                    find_first_by_attr_and_get_attr('displayText', result('log_sicktimeofftypebasedonthepaygroup_20'), 'uri', '') }}",
                "timeofftype": "{{ result('log_sicktimeofftypebasedonthepaygroup_20') }}"
            }
        )

        log_annualleavetimeofftypebasedonthepaygroup_191 = rail.PythonOperator(
            task_id='log_annualleavetimeofftypebasedonthepaygroup_191',
            python_callable=python_callable_method.get_annualleavetimeoff_based_on_paygroup,
            op_args=['dag_run.conf.paygroup']
        )

        if_log_annualleavetimeofftypebasedonthepaygroup_191_present_192 = rail.IfOperator(
            task_id='if_log_annualleavetimeofftypebasedonthepaygroup_191_present_192',
            test="{{ result('log_annualleavetimeofftypebasedonthepaygroup_191') | is_truthy }}",
            yes_task="log_checkif_annual_leaveisalreadyassignedandenabled_193",
            no_task="if_request_ususer_equals_to_yes_200"
        )

        log_checkif_annual_leaveisalreadyassignedandenabled_193 = rail.PythonOperator(
            task_id='log_checkif_annual_leaveisalreadyassignedandenabled_193',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('getassigned_time_offtypes_7')[0]['timeOffTypeAssignmentsDetails']['timeOffTypes'], 'displayText', rail.result(
                    'log_annualleavetimeofftypebasedonthepaygroup_191'), "displayText", "")
        )

        if_log_annualleavetimeofftypebasedonthepaygroup_191_not_equals_to_dataloggerc55e9949message_194 = rail.IfOperator(
            task_id='if_log_annualleavetimeofftypebasedonthepaygroup_191_not_equals_to_dataloggerc55e9949message_194',
            test="{{ result('log_annualleavetimeofftypebasedonthepaygroup_191') != \
                result('log_checkif_annual_leaveisalreadyassignedandenabled_193') \
                    or dag_run.conf.type == 'Rehire' }}",
            yes_task="trigger_dag_run_live_timeoff_sick_leave_aus_asia_policy_rehire_update_cr2021_v1195",
            no_task="if_request_ususer_equals_to_yes_200"
        )

        trigger_dag_run_live_timeoff_sick_leave_aus_asia_policy_rehire_update_cr2021_v1195 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_timeoff_sick_leave_aus_asia_policy_rehire_update_cr2021_v1195',
            retries=0,
            trigger_dag_id=f'adtalem_user_import_timeoff_sick_leave_aus_asia_policy_rehire_update_cr2021_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "lastname": "{{ dag_run.conf.lastname }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "jobcode": "{{ dag_run.conf.jobcode }}",
                "paygroup": "{{ dag_run.conf.paygroup }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "rehiredate": "{{ dag_run.conf.rehiredate }}",
                "servicedate": "{{ dag_run.conf.servicedate }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "ususer": "{{ dag_run.conf.ususer }}",
                "type": "{{ dag_run.conf.type }}",
                "timeoffuri": "{{ result('getenabled_time_offtypes_8') | \
                    find_first_by_attr_and_get_attr('displayText', result('log_annualleavetimeofftypebasedonthepaygroup_191'), 'uri', '') }}",
                "timeofftype": "{{ result('log_annualleavetimeofftypebasedonthepaygroup_191') }}"
            }
        )

        else_196 = rail.EmptyOperator(
            task_id='else_196'
        )

        if_log_annualleavetimeofftypebasedonthepaygroup_191_equals_to_annualleaveasia_197 = rail.IfOperator(
            task_id='if_log_annualleavetimeofftypebasedonthepaygroup_191_equals_to_annualleaveasia_197',
            test="{{ result('log_annualleavetimeofftypebasedonthepaygroup_191') == 'Annual Leave(Asia)' }}",
            yes_task="if_request_jobcode_not_equals_to_dataworkato_service0fafa311requestpreviousjobcode_198",
            no_task="if_request_ususer_equals_to_yes_200"
        )

        if_request_jobcode_not_equals_to_dataworkato_service0fafa311requestpreviousjobcode_198 = rail.IfOperator(
            task_id='if_request_jobcode_not_equals_to_dataworkato_service0fafa311requestpreviousjobcode_198',
            test="{{ dag_run.conf.jobcode != dag_run.conf.previousjobcode }}",
            yes_task="trigger_dag_run_live_timeoff_annual_leave_day_aus_asia_policy_add_update_cr2021_v1199",
            no_task="if_request_ususer_equals_to_yes_200"
        )

        trigger_dag_run_live_timeoff_annual_leave_day_aus_asia_policy_add_update_cr2021_v1199 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_timeoff_annual_leave_day_aus_asia_policy_add_update_cr2021_v1199',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_timeoff_annualleaveday_aus_asiapolicyaddupdate_cr2021_v1_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "lastname": "{{ dag_run.conf.lastname }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "jobcode": "{{ dag_run.conf.jobcode }}",
                "paygroup": "{{ dag_run.conf.paygroup }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "rehiredate": "{{ dag_run.conf.rehiredate }}",
                "servicedate": "{{ dag_run.conf.servicedate }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "ususer": "{{ dag_run.conf.ususer }}",
                "type": "{{ dag_run.conf.type }}",
                "timeoffuri": "{{ result('getenabled_time_offtypes_8') | \
                    find_first_by_attr_and_get_attr('displayText', result('log_annualleavetimeofftypebasedonthepaygroup_191'), 'uri', '') }}",
                "timeofftype": "{{ result('log_annualleavetimeofftypebasedonthepaygroup_191') }}"
            }
        )

        if_request_ususer_equals_to_yes_200 = rail.IfOperator(
            task_id='if_request_ususer_equals_to_yes_200',
            test="{{ dag_run.conf.ususer == 'yes' }}",
            yes_task="if_request_chamberlain_equals_to_yes_201",
            no_task="log_to_sumo"
        )

        if_request_chamberlain_equals_to_yes_201 = rail.IfOperator(
            task_id='if_request_chamberlain_equals_to_yes_201',
            test="{{ dag_run.conf.chamberlain == 'yes' }}",
            yes_task="log_checkif_p_t_o_r_f_tisassigned_202",
            no_task="get_timeoffnamelist2"
        )

        log_checkif_p_t_o_r_f_tisassigned_202 = rail.PythonOperator(
            task_id='log_checkif_p_t_o_r_f_tisassigned_202',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result("getassigned_time_offtypes_7")[
                                                                         0]['timeOffTypeAssignmentsDetails']['timeOffTypes'], 'name', 'PTO (RFT)', 'uri', '')
        )

        get_timeoffnamelist2 = rail.GetVariableOperator(
            task_id='get_timeoffnamelist2',
            name="{{ result('declare_list_21').name }}"
        )

        log_checkif_p_t_o_r_f_tisassignedtotheuser_203 = rail.PythonOperator(
            task_id='log_checkif_p_t_o_r_f_tisassignedtotheuser_203',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_timeoffnamelist2')['value'], 'name', 'PTO (RFT)', 'name', '')
        )

        log_checkif_p_t_o_r_p_tisassignedtotheuser_204 = rail.PythonOperator(
            task_id='log_checkif_p_t_o_r_p_tisassignedtotheuser_204',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_timeoffnamelist2')['value'], 'name', 'PTO (RPT)', 'name', '')
        )

        log_a_ssigned_p_t_otimeofftype_205 = rail.PythonOperator(
            task_id='log_a_ssigned_p_t_otimeofftype_205',
            python_callable=lambda: rail.result('log_checkif_p_t_o_r_f_tisassignedtotheuser_203') or rail.result(
                'log_checkif_p_t_o_r_p_tisassignedtotheuser_204')
        )

        if_log_a_ssigned_p_t_otimeofftype_205_present_206 = rail.IfOperator(
            task_id='if_log_a_ssigned_p_t_otimeofftype_205_present_206',
            test="{{ result('log_a_ssigned_p_t_otimeofftype_205') | is_truthy \
                or result('log_checkif_p_t_o_r_f_tisassigned_202') | is_truthy }}",
            yes_task="declare_list_207",
            no_task="log_to_sumo"
        )

        declare_list_207 = rail.SetVariableOperator(
            task_id='declare_list_207',
            append=False,
            name='Previouspolicylist',
            value=[]
        )

        log_checkif_p_t_oisassignedtotheuseruri_208 = rail.PythonOperator(
            task_id='log_checkif_p_t_oisassignedtotheuseruri_208',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('getenabled_time_offtypes_8'), 'displayText', rail.result(
                    'log_a_ssigned_p_t_otimeofftype_205'), 'uri', '')
        )

        log_final_p_t_ouri_209 = rail.PythonOperator(
            task_id='log_final_p_t_ouri_209',
            python_callable=lambda: rail.result('log_checkif_p_t_oisassignedtotheuseruri_208') or rail.result(
                'log_checkif_p_t_o_r_f_tisassigned_202')
        )

        log_checkif_p_t_owasalreadyassignedandenabled_210 = rail.PythonOperator(
            task_id='log_checkif_p_t_owasalreadyassignedandenabled_210',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('getassigned_time_offtypes_7')[0]['timeOffTypeAssignmentsDetails']['timeOffTypes'], 'uri', rail.result(
                    'log_final_p_t_ouri_209'), 'uri', '')
        )

        if_log_checkif_p_t_owasalreadyassignedandenabled_210_blank_211 = rail.IfOperator(
            task_id='if_log_checkif_p_t_owasalreadyassignedandenabled_210_blank_211',
            test="{{ result('log_checkif_p_t_owasalreadyassignedandenabled_210') | is_falsy \
                and dag_run.conf.userstatus == 'Enabled' }}",
            yes_task="update_variable_212",
            no_task="if_request_chamberlain_equals_to_yes_216"
        )

        update_variable_212 = rail.SetVariableOperator(
            task_id='update_variable_212',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value='yes'
        )

        if_request_previousfull_parttime_value_not_equals_to_dataworkato_service0fafa311requestfullparttime_213 = rail.IfOperator(
            task_id='if_request_previousfull_parttime_value_not_equals_to_dataworkato_service0fafa311requestfullparttime_213',
            test="{{ dag_run.conf.previousfull_parttime_value != dag_run.conf.fullparttime }}",
            yes_task="log_214",
            no_task="if_request_chamberlain_equals_to_yes_216"
        )

        log_214 = rail.PythonOperator(
            task_id='log_214',
            python_callable=lambda dag_run: "PTO (RFT)" if dag_run.conf[
                'previousfull_parttime_value'] == "F" else "PTO (RPT)"
        )

        trigger_dag_run_live_adtalem_delete_future_time_off_bookings_before_pto_transfer_child_v1_0215 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_adtalem_delete_future_time_off_bookings_before_pto_transfer_child_v1_0215',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_child_delete_future_timeoffbookings_beforepto_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "useruri": dag_run.conf['useruri'],
                "rundate": get_today_date(),
                "ptotypeuri": rail.find_first_by_attr_and_get_attr(rail.result('getenabled_time_offtypes_8'), 'name', rail.result('log_214'), 'uri', '')
            }
        )

        if_request_chamberlain_equals_to_yes_216 = rail.IfOperator(
            task_id='if_request_chamberlain_equals_to_yes_216',
            test="{{ dag_run.conf.chamberlain == 'yes' }}",
            yes_task="update_variable_217",
            no_task="if_request_userstatus_equals_to_disabled_218"
        )

        update_variable_217 = rail.SetVariableOperator(
            task_id='update_variable_217',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value='yes'
        )

        if_request_userstatus_equals_to_disabled_218 = rail.IfOperator(
            task_id='if_request_userstatus_equals_to_disabled_218',
            test="{{ dag_run.conf.userstatus == 'Disabled' }}",
            yes_task="update_variable_219",
            no_task="if_request_homestate_equals_to_ca_220"
        )

        update_variable_219 = rail.SetVariableOperator(
            task_id='update_variable_219',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value='yes'
        )

        if_request_homestate_equals_to_ca_220 = rail.IfOperator(
            task_id='if_request_homestate_equals_to_ca_220',
            test="{{ dag_run.conf.homestate == 'CA' and dag_run.conf.previoushomestate != 'CA' }}",
            yes_task="update_variable_221",
            no_task="if_request_homestate_not_equals_to_ca_222"
        )

        update_variable_221 = rail.SetVariableOperator(
            task_id='update_variable_221',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value='yes'
        )

        if_request_homestate_not_equals_to_ca_222 = rail.IfOperator(
            task_id='if_request_homestate_not_equals_to_ca_222',
            test="{{ dag_run.conf.homestate != 'CA' and dag_run.conf.previoushomestate == 'CA' }}",
            yes_task="update_variable_223",
            no_task="if_declare_variable_3_value_equals_to_yes_224"
        )

        update_variable_223 = rail.SetVariableOperator(
            task_id='update_variable_223',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value='yes'
        )

        if_declare_variable_3_value_equals_to_yes_224 = rail.IfOperator(
            task_id='if_declare_variable_3_value_equals_to_yes_224',
            test="{{ result('declare_variable_3').value == 'yes' }}",
            yes_task="put_default_time_off_type_for_bookings_for_user_226",
            no_task="log_to_sumo"
        )

        put_default_time_off_type_for_bookings_for_user_226 = rail.RepliconServiceOperator(
            task_id='put_default_time_off_type_for_bookings_for_user_226',
            endpoint="/services/TimeOffService1.svc/PutDefaultTimeOffTypeForBookingsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeOffTypeUri": "{{ result('log_final_p_t_ouri_209') }}"
            }
        )

        getassignedpolicyforthetimeofftype_227 = rail.RepliconServiceOperator(
            task_id='getassignedpolicyforthetimeofftype_227',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_h_isotricalpolicies_230 = rail.PythonOperator(
            task_id='log_h_isotricalpolicies_230',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getassignedpolicyforthetimeofftype_227')['policiesByTimeOffType'], 'timeOffType.uri', rail.result(
                    'log_final_p_t_ouri_209'), 'policySetSchedule', '')
        )

        if_first_description_present_232 = rail.IfOperator(
            task_id='if_first_description_present_232',
            test="{{ result('log_h_isotricalpolicies_230') | first_or_default | attr_or_default('description') | is_truthy }}",
            yes_task="foreach_document_233",
            no_task="get_required_timeoff_jobcode_mapper"
        )

        foreach_document_233 = rail.ForEachOperator(
            task_id='foreach_document_233',
            items="{{ result('log_h_isotricalpolicies_230') }}",
            start_task='foreach_foreach_document_233_234',
            end_task='foreach_document_233_end'
        )

        foreach_foreach_document_233_234 = rail.ForEachOperator(
            task_id='foreach_foreach_document_233_234',
            items="{{ result('foreach_document_233') }}",
            start_task='log_effectivedate_235',
            end_task='foreach_foreach_document_233_234_end'
        )

        log_effectivedate_235 = rail.PythonOperator(
            task_id='log_effectivedate_235',
            python_callable=dateformatmmddyyyy_to_time_less_than_today,
            # pylint: disable=line-too-long
            op_args=[
                "{{ result('foreach_foreach_document_233_234').effectiveDate.month }}/{{ result('foreach_foreach_document_233_234').effectiveDate.day }}/{{ result('foreach_foreach_document_233_234').effectiveDate.year }}"]
        )

        log_servicedate_236 = rail.PythonOperator(
            task_id='log_servicedate_236',
            python_callable=lambda dag_run: str(
                dag_run.conf['servicedate']).replace("-", "/")
        )

        if_to_dateformatmmddyyyy_to_time_less_than_today_237 = rail.IfOperator(
            task_id='if_to_dateformatmmddyyyy_to_time_less_than_today_237',
            test="{{ result('log_effectivedate_235') | is_truthy }}",
            yes_task="insert_to_list_238",
            no_task="foreach_foreach_document_233_234_end"
        )

        insert_to_list_238 = rail.SetVariableOperator(
            task_id='insert_to_list_238',
            append=True,
            name='{{ result("declare_list_207").name }}',
            value={
                "description": "{{ result('foreach_foreach_document_233_234').description }}",
                "effective_date": "{{ result('foreach_foreach_document_233_234').effectiveDate }}",
                "policy_set": "{{ result('foreach_foreach_document_233_234').policySet }}"
            }
        )

        foreach_foreach_document_233_234_end = rail.EmptyOperator(
            task_id='foreach_foreach_document_233_234_end'
        )

        foreach_document_233_end = rail.EmptyOperator(
            task_id='foreach_document_233_end'
        )

        log_repeatcount_239 = rail.GetVariableOperator(
            task_id='log_repeatcount_239',
            name="{{ result('declare_list_207').name }}"
        )

        log_previouspoliciesmodified_241 = rail.PythonOperator(
            task_id='log_previouspoliciesmodified_241',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('log_repeatcount_239')['value'], ensure_ascii=False).replace(
                        'null', '"effective"').replace('"script"', '"scriptTarget"'))
        )

        get_required_timeoff_jobcode_mapper = rail.PythonOperator(
            task_id='get_required_timeoff_jobcode_mapper',
            python_callable=python_callable_method.search_chamberlain_jobcode_mapper,
            op_args=['{{ dag_run.conf.jobcode }}']
        )

        final_policy_mapper_vacation = rail.PythonOperator(
            task_id='final_policy_mapper_vacation',
            python_callable=python_callable_method.get_final_policy_mapper,
            op_args=['new']
        )

        is_final_policy_mapper_vacation = rail.IfOperator(
            task_id='is_final_policy_mapper_vacation',
            test="{{ result('final_policy_mapper_vacation') | is_truthy }}",
            yes_task="if_request_userstatus_equals_to_disabled_275",
            no_task="log_to_sumo",
        )

        if_request_userstatus_equals_to_disabled_275 = rail.IfOperator(
            task_id='if_request_userstatus_equals_to_disabled_275',
            test="{{ dag_run.conf.userstatus == 'Disabled' }}",
            yes_task="trigger_dag_run_live_pto_policy_assignment_rehire_user_2021276",
            no_task="if_request_userstatus_equals_to_enabled_277"
        )

        trigger_dag_run_live_pto_policy_assignment_rehire_user_2021276 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_pto_policy_assignment_rehire_user_2021276',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_ptopolicyassignmentrehireuser_2021_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "policyname": "{{ result('final_policy_mapper_vacation') }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "servicedate": "{{ dag_run.conf.servicedate }}",
                "previouspolicy": "{{ result('log_previouspoliciesmodified_241') }}",
                "status": "{{ dag_run.conf.userstatus }}",
                "rehiredate": "{{ dag_run.conf.rehiredate }}",
                "timeoffuri": "{{ result('log_final_p_t_ouri_209') }}"
            }
        )

        if_request_userstatus_equals_to_enabled_277 = rail.IfOperator(
            task_id='if_request_userstatus_equals_to_enabled_277',
            test="{{ dag_run.conf.userstatus == 'Enabled' }}",
            yes_task="trigger_dag_run_live_pto_policy_assignment_update_user_2021278",
            no_task="log_to_sumo"
        )

        trigger_dag_run_live_pto_policy_assignment_update_user_2021278 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_pto_policy_assignment_update_user_2021278',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_ptopolicyassignmentupdateuser_2021_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "policyname": "{{ result('final_policy_mapper_vacation') }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "servicedate": "{{ dag_run.conf.servicedate }}",
                "previouspolicy": "{{ result('log_previouspoliciesmodified_241') }}",
                "status": "{{ dag_run.conf.userstatus }}",
                "rehiredate": "{{ dag_run.conf.rehiredate }}",
                "timeoffuri": "{{ result('log_final_p_t_ouri_209') }}",
                "balancetotransfer": "{{ result('get_balance_summary_for_account_10').timeRemaining }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> declare_variable_2
        declare_variable_2 >> declare_variable_3 >> declare_variable_5 >> \
            getassigned_time_offtypes_7 >> getenabled_time_offtypes_8 >> is_previousfulltime_notequal_fullparttime
        is_previousfulltime_notequal_fullparttime >> rail.Label(
            'Yes') >> get_balance_summary_for_account_10 >> if_request_ususer_not_equals_to_yes_13
        is_previousfulltime_notequal_fullparttime >> rail.Label(
            'No') >> if_request_ususer_not_equals_to_yes_13
        if_request_ususer_not_equals_to_yes_13 >> rail.Label(
            'Yes') >> should_update_newmapperlookup
        should_update_newmapperlookup >> rail.Label(
            'Yes') >> update_variable_15 >> if_request_regulartemp_equals_to_r_17
        should_update_newmapperlookup >> rail.Label(
            'No') >> else_16 >> if_request_regulartemp_equals_to_r_17
        if_request_regulartemp_equals_to_r_17 >> rail.Label(
            'Yes') >> update_variable_18 >> invoke_custom_ruby_code_19
        if_request_regulartemp_equals_to_r_17 >> rail.Label(
            'No') >> invoke_custom_ruby_code_19
        if_request_ususer_not_equals_to_yes_13 >> rail.Label(
            'No') >> invoke_custom_ruby_code_19
        invoke_custom_ruby_code_19 >> log_sicktimeofftypebasedonthepaygroup_20 >> \
            declare_list_21 >> declare_list_22 >> declare_list_23 >> \
            if_request_flextobeassigned_equals_to_yes_24
        if_request_flextobeassigned_equals_to_yes_24 >> rail.Label(
            'Yes') >> insert_to_list_25 >> insert_to_list_26 >> insert_to_list_27 >> insert_to_list_28 >> \
            insert_to_list_29 >> foreach_response_28
        foreach_response_28 >> foreach_timeofftypeassignmentsdetails_29
        foreach_timeofftypeassignmentsdetails_29 >> \
            is_all_timeoff_type
        is_all_timeoff_type >> rail.Label(
            'Yes') >> insert_to_list_31 >> foreach_timeofftypeassignmentsdetails_29_end
        is_all_timeoff_type >> rail.Label(
            'No') >> foreach_timeofftypeassignmentsdetails_29_end
        foreach_timeofftypeassignmentsdetails_29 >> foreach_timeofftypeassignmentsdetails_29_end
        foreach_response_28 >> foreach_response_28_end
        foreach_timeofftypeassignmentsdetails_29_end >> foreach_response_28_end
        foreach_response_28_end >> get_mapper_entries
        if_request_flextobeassigned_equals_to_yes_24 >> rail.Label(
            'No') >> get_mapper_entries
        get_mapper_entries >> adtalem_mapper_file_search_entries_32 >> if_request_flextobeassigned_equals_to_no_33
        if_request_flextobeassigned_equals_to_no_33 >> rail.Label(
            'Yes') >> if_entry_col3_present_34
        if_entry_col3_present_34 >> rail.Label(
            'Yes') >> log_removedelimiterfromthe_time_offtypeslisted_35 >> foreach_create_list_37_38
        foreach_create_list_37_38 >> insert_to_list_time_offsfrom_mapper_39 >> \
            foreach_create_list_37_38_end
        foreach_create_list_37_38 >> foreach_create_list_37_38_end
        foreach_create_list_37_38_end >> get_timeoffnamelist >> foreach_declare_list_21_40
        foreach_declare_list_21_40 >> insert_to_list_41 >> foreach_declare_list_21_40_end
        foreach_declare_list_21_40 >> foreach_declare_list_21_40_end
        foreach_declare_list_21_40_end >> log_checkif_p_t_o_buy_upwasassigned_42 >> if_log_checkif_p_t_o_buy_upwasassigned_42_present_yes_43
        if_log_checkif_p_t_o_buy_upwasassigned_42_present_yes_43 >> rail.Label(
            'Yes') >> insert_to_list_44 >> log_checkif_annual_leave_buy_upwasassigned_45
        if_log_checkif_p_t_o_buy_upwasassigned_42_present_yes_43 >> rail.Label(
            'No') >> log_checkif_annual_leave_buy_upwasassigned_45 >> if_log_checkif_annual_leave_buy_upwasassigned_45_present_yes_46
        if_log_checkif_annual_leave_buy_upwasassigned_45_present_yes_46 >> rail.Label(
            'Yes') >> insert_to_list_47 >> log_checkif_paid_time_off_buy_upwasassigned_48
        if_log_checkif_annual_leave_buy_upwasassigned_45_present_yes_46 >> rail.Label(
            'No') >> log_checkif_paid_time_off_buy_upwasassigned_48 >> if_log_checkif_paid_time_off_buy_upwasassigned_48_present_yes_49
        if_log_checkif_paid_time_off_buy_upwasassigned_48_present_yes_49 >> rail.Label(
            'Yes') >> insert_to_list_50 >> log_checkifsickcarryoverwasassigned_51
        if_log_checkif_paid_time_off_buy_upwasassigned_48_present_yes_49 >> rail.Label(
            'No') >> log_checkifsickcarryoverwasassigned_51 >> if_log_checkifsickcarryoverwasassigned_51_present_52
        if_log_checkifsickcarryoverwasassigned_51_present_52 >> rail.Label(
            'Yes') >> insert_to_list_53 >> log_checkifbevearement_assigned
        if_log_checkifsickcarryoverwasassigned_51_present_52 >> rail.Label(
            'No') >> log_checkifbevearement_assigned
        if_entry_col3_present_34 >> rail.Label(
            'No') >> log_checkifbevearement_assigned
        if_request_flextobeassigned_equals_to_no_33 >> rail.Label(
            'No') >> log_checkifbevearement_assigned
        log_checkifbevearement_assigned >> if_log_checkifbevearement_assigned_51_present_52
        if_log_checkifbevearement_assigned_51_present_52 >> rail.Label(
            'Yes') >> insert_to_list_58 >> log_checkifjuryduty_assigned
        if_log_checkifbevearement_assigned_51_present_52 >> rail.Label(
            'No') >> log_checkifjuryduty_assigned
        log_checkifjuryduty_assigned >> if_log_checkifjuryduty_assigned_51_present_52
        if_log_checkifjuryduty_assigned_51_present_52 >> rail.Label(
            'Yes') >> insert_to_list_61 >> final_set_timeoff_uris
        if_log_checkifjuryduty_assigned_51_present_52 >> rail.Label(
            'No') >> final_set_timeoff_uris
        final_set_timeoff_uris >> if_log_finalsetof_time_offuris_54_present_55 >> rail.Label(
            'Yes') >> assignrequired_timeofftypes_57 >> foreach_declare_list_21_58 >> \
            is_newsickleavetimeoffpolicyassignnmentfor_asiaor_a_u_sgroups
        is_newsickleavetimeoffpolicyassignnmentfor_asiaor_a_u_sgroups >> rail.Label(
            'Yes') >> log_ifsickleavetobeassignedisassignedalready_60 >> if_log_ifsickleavetobeassignedisassignedalready_60_blank_61
        if_log_ifsickleavetobeassignedisassignedalready_60_blank_61 >> rail.Label(
            'Yes') >> log_sickleavemeoffuri_62 >> if_log_sickleavemeoffuri_62_present_63
        if_log_sickleavemeoffuri_62_present_63 >> rail.Label(
            'Yes') >> get_default_time_off_type_policy_schedule_for_user_64 >> if_effectivedate_day_present_65
        if_effectivedate_day_present_65 >> rail.Label(
            'Yes') >> log_globalpolicy_66 >> put_user_time_off_account_policy_set_schedule_67 >> \
            if_foreach_611f2efb_58_name_equals_to_fto_68
        if_effectivedate_day_present_65 >> rail.Label(
            'No') >> if_foreach_611f2efb_58_name_equals_to_fto_68
        if_log_sickleavemeoffuri_62_present_63 >> rail.Label(
            'No') >> if_foreach_611f2efb_58_name_equals_to_fto_68
        if_log_ifsickleavetobeassignedisassignedalready_60_blank_61 >> rail.Label(
            'No') >> if_foreach_611f2efb_58_name_equals_to_fto_68
        is_newsickleavetimeoffpolicyassignnmentfor_asiaor_a_u_sgroups >> rail.Label(
            'No') >> if_foreach_611f2efb_58_name_equals_to_fto_68
        if_foreach_611f2efb_58_name_equals_to_fto_68 >> rail.Label(
            'Yes') >> log_f_t_otmeoffuri_69 >> put_default_time_off_type_for_bookings_for_user_70 >> foreach_response_73
        if_foreach_611f2efb_58_name_equals_to_fto_68 >> rail.Label(
            'No') >> foreach_declare_list_21_58_end >> foreach_response_73
        if_log_finalsetof_time_offuris_54_present_55 >> rail.Label(
            'No') >> foreach_response_73
        foreach_response_73 >> foreach_timeofftypeassignmentsdetails_74 >> \
            insert_to_list_urispreviouslyassigned_75 >> \
            foreach_timeofftypeassignmentsdetails_74_end
        foreach_timeofftypeassignmentsdetails_74 >> \
            foreach_timeofftypeassignmentsdetails_74_end >> foreach_response_73_end
        foreach_response_73 >> foreach_response_73_end
        foreach_response_73_end >> get_previoustimeofflist >> foreach_declare_list_23_76
        foreach_declare_list_23_76 >> is_timeoff_not_assigned_in_new_set
        is_timeoff_not_assigned_in_new_set >> rail.Label(
            'Yes') >> trigger_dag_run_live_put_0_balance_for_update_users_cr14_079 >> \
            if_request_flextobeassigned_equals_to_yes_80
        is_timeoff_not_assigned_in_new_set >> rail.Label(
            'No') >> if_request_flextobeassigned_equals_to_yes_80
        foreach_declare_list_23_76 >> foreach_declare_list_23_76_end
        foreach_declare_list_23_76_end >> if_request_flextobeassigned_equals_to_yes_80
        if_request_flextobeassigned_equals_to_yes_80 >> rail.Label(
            'Yes') >> foreach_declare_list_23_81
        foreach_declare_list_23_81 >> if_name_equals_ptorpt_timeofftype
        if_name_equals_ptorpt_timeofftype >> rail.Label(
            'Yes') >> trigger_dag_run_live_put_blank_balance_for_update_users_cr14_083 >> foreach_declare_list_23_81_end
        if_name_equals_ptorpt_timeofftype >> rail.Label(
            'No') >> foreach_declare_list_23_81_end
        if_request_flextobeassigned_equals_to_yes_80 >> rail.Label(
            'No') >> foreach_declare_list_23_81_end
        foreach_declare_list_23_81 >> foreach_declare_list_23_81_end
        foreach_declare_list_23_81_end >> log_checkif_floating_holidaysisassigned_84
        log_checkif_floating_holidaysisassigned_84 >> log_checkif_anniversary_dayassigned_85 >> \
            if_log_checkif_anniversary_dayassigned_85_present_86
        if_log_checkif_anniversary_dayassigned_85_present_86 >> rail.Label(
            'Yes') >> log_checkif_anniversary_dayisalreadyassignedandenabled_87 >> \
            if_log_checkif_anniversary_dayisalreadyassignedandenabled_87_blank_88
        if_log_checkif_anniversary_dayisalreadyassignedandenabled_87_blank_88 >> rail.Label(
            'Yes') >> log_time_offurifor_anniversary_day_89 >> \
            trigger_dag_run_live_timeoff_anniversary_day_policy_add_update_cr14_090 >> \
            if_log_checkif_floating_holidaysisassigned_84_present_91
        if_log_checkif_anniversary_dayisalreadyassignedandenabled_87_blank_88 >> rail.Label(
            'No') >> if_log_checkif_floating_holidaysisassigned_84_present_91
        if_log_checkif_anniversary_dayassigned_85_present_86 >> rail.Label(
            'No') >> if_log_checkif_floating_holidaysisassigned_84_present_91
        if_log_checkif_floating_holidaysisassigned_84_present_91 >> rail.Label(
            'Yes') >> log_checkif_floating_holidaysisalreadyassignedandenabled_92 >> \
            if_log_checkif_floating_holidaysisalreadyassignedandenabled_92_blank_93
        if_log_checkif_floating_holidaysisalreadyassignedandenabled_92_blank_93 >> rail.Label(
            'Yes') >> update_variable_94 >> if_request_ususer_equals_to_no_156
        if_log_checkif_floating_holidaysisalreadyassignedandenabled_92_blank_93 >> rail.Label(
            'No') >> if_request_fullparttime_not_equals_to_dataworkato_service0fafa311requestpreviousfull_parttime_value_95
        if_request_fullparttime_not_equals_to_dataworkato_service0fafa311requestpreviousfull_parttime_value_95 >> rail.Label(
            'Yes') >> update_variable_96 >> if_request_ususer_equals_to_no_156
        if_request_fullparttime_not_equals_to_dataworkato_service0fafa311requestpreviousfull_parttime_value_95 >> rail.Label(
            'No') >> if_request_userstatus_equals_to_disabled_97
        if_request_userstatus_equals_to_disabled_97 >> rail.Label(
            'Yes') >> update_variable_98 >> if_request_ususer_equals_to_no_156
        if_request_userstatus_equals_to_disabled_97 >> rail.Label(
            'No') >> if_declare_variable_2_value_equals_to_yes_99
        if_declare_variable_2_value_equals_to_yes_99 >> rail.Label(
            'Yes') >> declare_list_100 >> log_time_offurifor_floating_holidays_101 >> \
            getassignedpolicyforthetimeofftype_103 >> log_existing_policy_schedule_106 >> if_to_s_contains_urn_107
        if_to_s_contains_urn_107 >> rail.Label(
            'Yes') >> foreach_document_109
        foreach_document_109 >> foreach_foreach_document_109_110
        foreach_foreach_document_109_110 >> log_effectivedate_111 >> if_to_dateformatmmddyyyy_to_time_less_than_today_112
        if_to_dateformatmmddyyyy_to_time_less_than_today_112 >> rail.Label(
            'Yes') >> accumulate_list_items_113 >> foreach_foreach_document_109_110_end
        if_to_dateformatmmddyyyy_to_time_less_than_today_112 >> rail.Label(
            'No') >> foreach_foreach_document_109_110_end
        foreach_foreach_document_109_110 >> foreach_foreach_document_109_110_end >> foreach_document_109_end
        foreach_document_109 >> foreach_document_109_end
        foreach_document_109_end >> previous_policies_floating_holiday_trigger >> log_previouspoliciesmodified_final_117 >> \
            getdefaultpolicyfor_flotingholidays_119
        if_to_s_contains_urn_107 >> rail.Label(
            'No') >> getdefaultpolicyfor_flotingholidays_119 >> if_request_fullparttime_equals_to_f_122
        if_request_fullparttime_equals_to_f_122 >> rail.Label(
            'Yes') >> log_policysetmodified_123 >> if_to_s_contains_urn_125
        if_to_s_contains_urn_125 >> rail.Label(
            'Yes') >> assign_floatingholidayspolicy_126 >> else_127 >> \
            assign_floatingholidayspolicy_128 >> else_131
        if_to_s_contains_urn_125 >> rail.Label(
            'No') >> else_131 >> log_policysetmodified_132 >> \
            log_yearlyentitilement_133 >> log_yearlyaccrualpolicy_135 >> foreach_document_137
        foreach_document_137 >> foreach_foreach_document_137_138 >> \
            if_foreach_1a732118_138_keyuri_equals_to_urnrepliconscriptkeyparameteraccrualannualamount_139
        if_foreach_1a732118_138_keyuri_equals_to_urnrepliconscriptkeyparameteraccrualannualamount_139 >> rail.Label(
            'Yes') >> log_yearlyaccrualvalue_140 >> log_yearlyaccrualexistingvalue_141
        if_foreach_1a732118_138_keyuri_equals_to_urnrepliconscriptkeyparameteraccrualannualamount_139 >> rail.Label(
            'No') >> foreach_foreach_document_137_138_end
        foreach_foreach_document_137_138 >> foreach_foreach_document_137_138_end >> foreach_document_137_end
        foreach_document_137 >> foreach_document_137_end
        foreach_document_137_end >> log_yearlyaccrualexistingvalue_141 >> \
            log_yearlyaccrualnewvalue_142 >> log_newpolocywiththevalue_143 >> insert_to_list_145 >> \
            log_finalpolicywithnewvaluewithpreviouspolicy_148 >> if_to_s_contains_urn_150
        if_to_s_contains_urn_150 >> rail.Label(
            'Yes') >> assign_floatingholidayspolicy_151 >> else_152 >> \
            assign_floatingholidayspolicy_153 >> if_request_ususer_equals_to_no_156
        if_to_s_contains_urn_150 >> rail.Label(
            'No') >> else_152
        if_request_fullparttime_equals_to_f_122 >> rail.Label(
            'No') >> if_request_ususer_equals_to_no_156
        if_declare_variable_2_value_equals_to_yes_99 >> rail.Label(
            'No') >> if_request_ususer_equals_to_no_156
        if_log_checkif_floating_holidaysisassigned_84_present_91 >> rail.Label(
            'No') >> if_request_ususer_equals_to_no_156
        if_request_ususer_equals_to_no_156 >> rail.Label(
            'Yes') >> if_request_paygroup_equals_to_acaau_157
        if_request_paygroup_equals_to_acaau_157 >> rail.Label(
            'Yes') >> log_time_offurifor_paid_time_off_158 >> log_checkif_p_t_oisassigned_159 >> \
            getdefaultpolicyfor_p_t_o_161 >> log_policysetmodified_162 >> getassignedpolicyforthetimeofftype_163 >> \
            log_existing_policy_schedule_164 >> if_log_checkif_p_t_oisassigned_159_present_167
        if_log_checkif_p_t_oisassigned_159_present_167 >> rail.Label(
            'Yes') >> if_to_s_contains_urn_168
        if_to_s_contains_urn_168 >> rail.Label(
            'Yes') >> foreach_document_170
        foreach_document_170 >> foreach_foreach_document_170_171 >> \
            log_effectivedate_172 >> if_to_dateformatmmddyyyy_to_time_less_than_today_173
        if_to_dateformatmmddyyyy_to_time_less_than_today_173 >> rail.Label(
            'Yes') >> accumulate_list_items_174 >> foreach_foreach_document_170_171_end
        if_to_dateformatmmddyyyy_to_time_less_than_today_173 >> rail.Label(
            'No') >> foreach_foreach_document_170_171_end
        foreach_foreach_document_170_171 >> foreach_foreach_document_170_171_end >> \
            foreach_document_170_end
        foreach_document_170 >> foreach_document_170_end
        foreach_document_170_end >> previous_policies_pto_trigger >> \
            if_log_previouspoliciesmodified_final_178_contains_description_180
        if_to_s_contains_urn_168 >> rail.Label(
            'No') >> if_request_paygroup_equals_to_acatw_186
        if_log_checkif_p_t_oisassigned_159_present_167 >> rail.Label(
            'No') >> if_log_previouspoliciesmodified_final_178_contains_description_180
        if_log_previouspoliciesmodified_final_178_contains_description_180 >> rail.Label(
            'Yes') >> assign_paid_time_off_policy_181 >> else_182 >> \
            assign_paid_time_off_policy_183 >> if_request_ususer_equals_to_yes_200
        if_log_previouspoliciesmodified_final_178_contains_description_180 >> rail.Label(
            'No') >> if_request_ususer_equals_to_yes_200
        if_request_paygroup_equals_to_acaau_157 >> rail.Label(
            'No') >> if_request_paygroup_equals_to_acatw_186
        if_request_paygroup_equals_to_acatw_186 >> rail.Label(
            'Yes') >> if_log_sicktimeofftypebasedonthepaygroup_20_present_187
        if_log_sicktimeofftypebasedonthepaygroup_20_present_187 >> rail.Label(
            'Yes') >> log_checkif_sick_leaveisalreadyassignedandenabled_188 >> \
            if_log_checkif_sick_leaveisalreadyassignedandenabled_188_present_189
        if_log_checkif_sick_leaveisalreadyassignedandenabled_188_present_189 >> rail.Label(
            'Yes') >> trigger_dag_run_live_timeoff_sick_leave_aus_asia_policy_rehire_update_cr2021_v1190 >> \
            log_annualleavetimeofftypebasedonthepaygroup_191
        if_log_checkif_sick_leaveisalreadyassignedandenabled_188_present_189 >> rail.Label(
            'No') >> log_annualleavetimeofftypebasedonthepaygroup_191
        if_log_sicktimeofftypebasedonthepaygroup_20_present_187 >> rail.Label(
            'No') >> log_annualleavetimeofftypebasedonthepaygroup_191 >> if_log_annualleavetimeofftypebasedonthepaygroup_191_present_192
        if_log_annualleavetimeofftypebasedonthepaygroup_191_present_192 >> rail.Label(
            'Yes') >> log_checkif_annual_leaveisalreadyassignedandenabled_193 >> \
            if_log_annualleavetimeofftypebasedonthepaygroup_191_not_equals_to_dataloggerc55e9949message_194
        if_log_annualleavetimeofftypebasedonthepaygroup_191_not_equals_to_dataloggerc55e9949message_194 >> rail.Label(
            'Yes') >> trigger_dag_run_live_timeoff_sick_leave_aus_asia_policy_rehire_update_cr2021_v1195 >> \
            else_196 >> if_log_annualleavetimeofftypebasedonthepaygroup_191_equals_to_annualleaveasia_197
        if_log_annualleavetimeofftypebasedonthepaygroup_191_equals_to_annualleaveasia_197 >> rail.Label(
            'Yes') >> if_request_jobcode_not_equals_to_dataworkato_service0fafa311requestpreviousjobcode_198
        if_request_jobcode_not_equals_to_dataworkato_service0fafa311requestpreviousjobcode_198 >> rail.Label(
            'Yes') >> trigger_dag_run_live_timeoff_annual_leave_day_aus_asia_policy_add_update_cr2021_v1199 >> \
            if_request_ususer_equals_to_yes_200
        if_request_jobcode_not_equals_to_dataworkato_service0fafa311requestpreviousjobcode_198 >> rail.Label(
            'No') >> if_request_ususer_equals_to_yes_200
        if_log_annualleavetimeofftypebasedonthepaygroup_191_equals_to_annualleaveasia_197 >> rail.Label(
            'No') >> if_request_ususer_equals_to_yes_200
        if_log_annualleavetimeofftypebasedonthepaygroup_191_not_equals_to_dataloggerc55e9949message_194 >> rail.Label(
            'No') >> if_request_ususer_equals_to_yes_200
        if_log_annualleavetimeofftypebasedonthepaygroup_191_present_192 >> rail.Label(
            'No') >> if_request_ususer_equals_to_yes_200
        if_request_paygroup_equals_to_acatw_186 >> rail.Label(
            'No') >> if_request_ususer_equals_to_yes_200
        if_request_ususer_equals_to_no_156 >> rail.Label(
            'No') >> if_request_ususer_equals_to_yes_200
        if_request_ususer_equals_to_yes_200 >> rail.Label(
            'Yes') >> if_request_chamberlain_equals_to_yes_201
        if_request_chamberlain_equals_to_yes_201 >> rail.Label(
            'Yes') >> log_checkif_p_t_o_r_f_tisassigned_202 >> \
            log_a_ssigned_p_t_otimeofftype_205
        if_request_chamberlain_equals_to_yes_201 >> rail.Label(
            'No') >> get_timeoffnamelist2 >> log_checkif_p_t_o_r_f_tisassignedtotheuser_203 >> \
            log_checkif_p_t_o_r_p_tisassignedtotheuser_204 >> log_a_ssigned_p_t_otimeofftype_205 >> if_log_a_ssigned_p_t_otimeofftype_205_present_206
        if_log_a_ssigned_p_t_otimeofftype_205_present_206 >> rail.Label(
            'Yes') >> declare_list_207 >> log_checkif_p_t_oisassignedtotheuseruri_208 >> \
            log_final_p_t_ouri_209 >> log_checkif_p_t_owasalreadyassignedandenabled_210 >> \
            if_log_checkif_p_t_owasalreadyassignedandenabled_210_blank_211
        if_log_checkif_p_t_owasalreadyassignedandenabled_210_blank_211 >> rail.Label(
            'Yes') >> update_variable_212 >> \
            if_request_previousfull_parttime_value_not_equals_to_dataworkato_service0fafa311requestfullparttime_213
        if_request_previousfull_parttime_value_not_equals_to_dataworkato_service0fafa311requestfullparttime_213 >> rail.Label(
            'Yes') >> log_214 >> trigger_dag_run_live_adtalem_delete_future_time_off_bookings_before_pto_transfer_child_v1_0215 >> \
            if_request_chamberlain_equals_to_yes_216
        if_request_previousfull_parttime_value_not_equals_to_dataworkato_service0fafa311requestfullparttime_213 >> rail.Label(
            'No') >> if_request_chamberlain_equals_to_yes_216
        if_log_checkif_p_t_owasalreadyassignedandenabled_210_blank_211 >> rail.Label(
            'No') >> if_request_chamberlain_equals_to_yes_216
        if_request_chamberlain_equals_to_yes_216 >> rail.Label(
            'Yes') >> update_variable_217 >> if_request_userstatus_equals_to_disabled_218
        if_request_chamberlain_equals_to_yes_216 >> rail.Label(
            'No') >> if_request_userstatus_equals_to_disabled_218
        if_request_userstatus_equals_to_disabled_218 >> rail.Label(
            'Yes') >> update_variable_219 >> if_request_homestate_equals_to_ca_220
        if_request_userstatus_equals_to_disabled_218 >> rail.Label(
            'No') >> if_request_homestate_equals_to_ca_220
        if_request_homestate_equals_to_ca_220 >> rail.Label(
            'Yes') >> update_variable_221 >> if_request_homestate_not_equals_to_ca_222
        if_request_homestate_equals_to_ca_220 >> rail.Label(
            'No') >> if_request_homestate_not_equals_to_ca_222
        if_request_homestate_not_equals_to_ca_222 >> rail.Label(
            'Yes') >> update_variable_223 >> if_declare_variable_3_value_equals_to_yes_224
        if_request_homestate_not_equals_to_ca_222 >> rail.Label(
            'No') >> if_declare_variable_3_value_equals_to_yes_224
        if_declare_variable_3_value_equals_to_yes_224 >> rail.Label(
            'Yes') >> put_default_time_off_type_for_bookings_for_user_226 >> \
            getassignedpolicyforthetimeofftype_227 >> log_h_isotricalpolicies_230 >> if_first_description_present_232
        if_first_description_present_232 >> rail.Label(
            'Yes') >> foreach_document_233 >> foreach_foreach_document_233_234 >> \
            log_effectivedate_235 >> log_servicedate_236 >> \
            if_to_dateformatmmddyyyy_to_time_less_than_today_237
        if_to_dateformatmmddyyyy_to_time_less_than_today_237 >> rail.Label(
            'Yes') >> insert_to_list_238 >> log_repeatcount_239
        if_to_dateformatmmddyyyy_to_time_less_than_today_237 >> rail.Label(
            'No') >> foreach_foreach_document_233_234_end
        foreach_foreach_document_233_234 >> foreach_foreach_document_233_234_end >> \
            foreach_document_233_end
        foreach_document_233 >> foreach_document_233_end >> log_repeatcount_239 >> \
            log_previouspoliciesmodified_241 >> get_required_timeoff_jobcode_mapper
        if_first_description_present_232 >> rail.Label(
            'No') >> get_required_timeoff_jobcode_mapper
        get_required_timeoff_jobcode_mapper >> final_policy_mapper_vacation >> is_final_policy_mapper_vacation
        is_final_policy_mapper_vacation >> rail.Label(
            'Yes') >> if_request_userstatus_equals_to_disabled_275
        if_request_userstatus_equals_to_disabled_275 >> rail.Label(
            'Yes') >> trigger_dag_run_live_pto_policy_assignment_rehire_user_2021276 >> \
            if_request_userstatus_equals_to_enabled_277
        if_request_userstatus_equals_to_disabled_275 >> rail.Label(
            'No') >> if_request_userstatus_equals_to_enabled_277
        if_request_userstatus_equals_to_enabled_277 >> rail.Label(
            'Yes') >> trigger_dag_run_live_pto_policy_assignment_update_user_2021278 >> log_to_sumo
        if_request_userstatus_equals_to_enabled_277 >> rail.Label(
            'No') >> log_to_sumo
        is_final_policy_mapper_vacation >> rail.Label(
            'No') >> log_to_sumo
        if_declare_variable_3_value_equals_to_yes_224 >> rail.Label(
            'No') >> log_to_sumo
        if_log_a_ssigned_p_t_otimeofftype_205_present_206 >> rail.Label(
            'No') >> log_to_sumo
        if_request_ususer_equals_to_yes_200 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
