from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from momentive.user_import_india.utils import python_callable, request_payload
from momentive.user_import_india.mappers.momentive_user_import_mapper import momentive_userimport_mapper

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_india_user_sync_child_update_user_timeoff_assign_id,
        description=f'Momentive_user_sync_Timeoff_assign_update_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_split_dates'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_split_dates',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_split_dates = rail.PythonOperator(
            task_id="get_split_dates",
            python_callable=lambda dag_run: {
                "hire_date": python_callable.split_date_string(dag_run.conf['hiredate'], 'int'),
                "today": request_payload.effective_dateformat_payload(datetime.now(), 'int')
            }
        )

        log_final_number_of_days_in_year_and_day_of_the_year = rail.PythonOperator(
            task_id="log_final_number_of_days_in_year_and_day_of_the_year",
            python_callable=python_callable.get_number_of_days_to_be_considered_for_accrual
        )

        log_numberofdaystobeconsideredforaccrual_14 = rail.PythonOperator(
            task_id='log_numberofdaystobeconsideredforaccrual_14',
            python_callable=lambda: rail.result(
                "log_final_number_of_days_in_year_and_day_of_the_year")['no_of_days_for_accrual']
        )

        log_1_i_n_d_privilege_leaveaccrualcalculation_15 = rail.PythonOperator(
            task_id='log_1_i_n_d_privilege_leaveaccrualcalculation_15',
            python_callable=lambda:  float(
                20.0 / rail.result('log_final_number_of_days_in_year_and_day_of_the_year')['number_of_days_in_year'])
        )

        log_2_i_n_d_casual_leaveaccrualcalculation_16 = rail.PythonOperator(
            task_id='log_2_i_n_d_casual_leaveaccrualcalculation_16',
            python_callable=lambda:  float(
                10.0 / rail.result('log_final_number_of_days_in_year_and_day_of_the_year')['number_of_days_in_year'])
        )

        getassigned_time_offtypes_18 = rail.RepliconServiceOperator(
            task_id='getassigned_time_offtypes_18',
            endpoint="/services/TimeOffService1.svc/BulkGetTimeOffTypeAssignmentsForUsers",
            data={
                "userUris": [
                    "{{ dag_run.conf.useruri }}"
                ]
            }
        )

        getenabled_time_offtypes_19 = rail.RepliconServiceOperator(
            task_id='getenabled_time_offtypes_19',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        momentive_userimport_mapper_search_entries_23 = rail.PythonOperator(
            task_id='momentive_userimport_mapper_search_entries_23',
            python_callable=lambda dag_run:  list(filter(lambda x: x["type"] == "Time off Types" and
                                                         x["exemptstatus"] == dag_run.conf['exemptionstatus'], momentive_userimport_mapper))
        )

        log_24 = rail.PythonOperator(
            task_id='log_24',
            python_callable=lambda dag_run: dag_run.conf[
                'businesstitle'] if dag_run.conf['businesstitle'] else null
        )

        log_timeofftypestobeassigned_25 = rail.PythonOperator(
            task_id='log_timeofftypestobeassigned_25',
            python_callable=lambda dag_run:  list(filter(lambda x: x["type"] == "Time off Types" and x["workertype"] == dag_run.conf['workertype'] and
                                                         x["location"] == dag_run.conf['location'] and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and
                                                         (x['businesstitle'] == dag_run.conf['businesstitle'] if dag_run.conf['businesstitle'] else dag_run.conf['businesstitle']), rail.result(
                "momentive_userimport_mapper_search_entries_23")))[0]['value']
        )

        if_log_timeofftypestobeassigned_25_present_26 = rail.IfOperator(
            task_id='if_log_timeofftypestobeassigned_25_present_26',
            test='''{{ result('log_timeofftypestobeassigned_25') | is_truthy }}''',
            yes_task="log_removedelimiterfromthe_time_offtypeslisted_27",
            no_task="finish"
        )

        log_removedelimiterfromthe_time_offtypeslisted_27 = rail.PythonOperator(
            task_id='log_removedelimiterfromthe_time_offtypeslisted_27',
            python_callable=lambda:  rail.result(
                'log_timeofftypestobeassigned_25').split("|")
        )

        def get_to_be_assigned_timeoff_uris():
            final_timeoff_uri_name_list = []
            enabled_timeoff_uris = rail.result("getenabled_time_offtypes_19")
            for item in rail.result("log_removedelimiterfromthe_time_offtypeslisted_27"):
                name = item.strip()
                final_timeoff_uri_name_list.append({
                    "name": name,
                    "uri": rail.find_first_by_attr_and_get_attr(enabled_timeoff_uris, 'displayText', name, 'uri')
                })

            return final_timeoff_uri_name_list

        timeoff_name_list = rail.PythonOperator(
            task_id='timeoff_name_list',
            python_callable=get_to_be_assigned_timeoff_uris
        )

        log_finalsetof_time_offuris_35 = rail.PythonOperator(
            task_id='log_finalsetof_time_offuris_35',
            python_callable=lambda: [item['uri']
                                     for item in rail.result("timeoff_name_list")]
        )

        if_log_finalsetof_time_offuris_35_present_36 = rail.IfOperator(
            task_id='if_log_finalsetof_time_offuris_35_present_36',
            test='''{{ result('log_finalsetof_time_offuris_35') | is_truthy }}''',
            yes_task="assignrequired_timeofftypes_38",
            no_task="get_previously_assigned_timeoff_uris",
        )

        assignrequired_timeofftypes_38 = rail.RepliconServiceOperator(
            task_id='assignrequired_timeofftypes_38',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_finalsetof_time_offuris_35')
            }
        )

        def get_previously_assigned_timeoff_uri_list():
            previosly_assigned_uris_list = []
            for item in rail.result("getassigned_time_offtypes_18"):
                for entry in item['timeOffTypeAssignmentsDetails']['timeOffTypes']:
                    previosly_assigned_uris_list.append(entry['uri'])

            return previosly_assigned_uris_list

        get_previously_assigned_timeoff_uris = rail.PythonOperator(
            task_id='get_previously_assigned_timeoff_uris',
            python_callable=get_previously_assigned_timeoff_uri_list
        )

        log_checkif9_i_n_d_optional_holidayisassigned_45 = rail.PythonOperator(
            task_id='log_checkif9_i_n_d_optional_holidayisassigned_45',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                "timeoff_name_list"), 'name', "9. IND_Optional Holiday", 'name') if rail.result("timeoff_name_list") else ''
        )

        if_log_checkif9_i_n_d_optional_holidayisassigned_45_present_46 = rail.IfOperator(
            task_id='if_log_checkif9_i_n_d_optional_holidayisassigned_45_present_46',
            test='''{{ result('log_checkif9_i_n_d_optional_holidayisassigned_45') | is_truthy }}''',
            yes_task="log_checkif9_i_n_d_optional_holidayisalreadyassignedandenabled_47",
            no_task="log_checkif1_i_n_d_privilegeleaveisassigned_88",
        )

        log_checkif9_i_n_d_optional_holidayisalreadyassignedandenabled_47 = rail.PythonOperator(
            task_id='log_checkif9_i_n_d_optional_holidayisalreadyassignedandenabled_47',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('getassigned_time_offtypes_18')[
                                                                         0]['timeOffTypeAssignmentsDetails']['timeOffTypes'], 'displayText', "9. IND_Optional Holiday", 'uri')
        )

        if_log_checkif9_i_n_d_optional_holidayisalreadyassignedandenabled_47_blank_48 = rail.IfOperator(
            task_id='if_log_checkif9_i_n_d_optional_holidayisalreadyassignedandenabled_47_blank_48',
            test=lambda dag_run: bool(not (rail.result(
                'log_checkif9_i_n_d_optional_holidayisalreadyassignedandenabled_47')) or dag_run.conf['rehire'] == 'rehire'),
            yes_task="log_timeoff_uri_for_ind_optional_holiday_50",
            no_task="log_checkif1_i_n_d_privilegeleaveisassigned_88",
        )

        log_timeoff_uri_for_ind_optional_holiday_50 = rail.PythonOperator(
            task_id='log_timeoff_uri_for_ind_optional_holiday_50',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getenabled_time_offtypes_19'), 'displayText', "9. IND_Optional Holiday", 'uri')
        )

        getassignedpolicyforthetimeofftype_52 = rail.RepliconServiceOperator(
            task_id='getassignedpolicyforthetimeofftype_52',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response['policiesByTimeOffType'], "timeOffType.uri",  rail.result(
                'log_timeoff_uri_for_ind_optional_holiday_50'), 'policySetSchedule')
        )

        if_to_s_contains_urn_56 = rail.IfOperator(
            task_id='if_to_s_contains_urn_56',
            test=lambda: bool('urn' in json.dumps(
                rail.result('getassignedpolicyforthetimeofftype_52'))),
            yes_task="log_previouspoliciesmodified_final_66",
            no_task="getdefaultpolicyfor9_i_n_d_optional_holiday_68",
        )

        log_previouspoliciesmodified_final_66 = rail.PythonOperator(
            task_id='log_previouspoliciesmodified_final_66',
            python_callable=lambda:  python_callable.modify_previous_policy(
                rail.result("getassignedpolicyforthetimeofftype_52"))
        )

        getdefaultpolicyfor9_i_n_d_optional_holiday_68 = rail.RepliconServiceOperator(
            task_id='getdefaultpolicyfor9_i_n_d_optional_holiday_68',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('log_timeoff_uri_for_ind_optional_holiday_50') }}"
            }
        )

        log_policysetmodified_71 = rail.PythonOperator(
            task_id='log_policysetmodified_71',
            python_callable=lambda:  rail.result(
                'getdefaultpolicyfor9_i_n_d_optional_holiday_68')[0]['policySet']
        )

        get_final_modified_policyset_schedule = rail.PythonOperator(
            task_id="get_final_modified_policyset_schedule",
            python_callable=lambda: python_callable.modify_new_policy(rail.result(
                "log_policysetmodified_71"), rail.result("log_previouspoliciesmodified_final_66"))
        )

        assign_ind_optional_holiday = rail.RepliconServiceOperator(
            task_id='assign_ind_optional_holiday',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('log_timeoff_uri_for_ind_optional_holiday_50')
                },
                "policySetScheduleEntries": rail.result('get_final_modified_policyset_schedule')
            }
        )

        log_checkif1_i_n_d_privilegeleaveisassigned_88 = rail.PythonOperator(
            task_id='log_checkif1_i_n_d_privilegeleaveisassigned_88',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                "timeoff_name_list"), 'name', "1. IND_Privilege leave", 'name') if rail.result("timeoff_name_list") else ''
        )

        if_log_checkif1_i_n_d_privilegeleaveisassigned_88_present_89 = rail.IfOperator(
            task_id='if_log_checkif1_i_n_d_privilegeleaveisassigned_88_present_89',
            test='''{{ result('log_checkif1_i_n_d_privilegeleaveisassigned_88') | is_truthy }}''',
            yes_task="log_checkif1_i_n_d_privilegeleaveisalreadyassignedandenabled_90",
            no_task="log_checkif2_i_n_d_casual_leaveisassigned_225",
        )

        log_checkif1_i_n_d_privilegeleaveisalreadyassignedandenabled_90 = rail.PythonOperator(
            task_id='log_checkif1_i_n_d_privilegeleaveisalreadyassignedandenabled_90',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getassigned_time_offtypes_18')[0]['timeOffTypeAssignmentsDetails']['timeOffTypes'], 'displayText', "1. IND_Privilege leave", 'uri')
        )

        if_log_checkif1_i_n_d_privilegeleaveisalreadyassignedandenabled_90_blank_no_91 = rail.IfOperator(
            task_id='if_log_checkif1_i_n_d_privilegeleaveisalreadyassignedandenabled_90_blank_no_91',
            test='''{{ result('log_checkif1_i_n_d_privilegeleaveisalreadyassignedandenabled_90') | is_falsy}}''',
            yes_task="log_daystobeaccruedstartingbalance_92",
            no_task="if_request_rehire_equals_to_rehire_158",
        )

        log_daystobeaccruedstartingbalance_92 = rail.PythonOperator(
            task_id='log_daystobeaccruedstartingbalance_92',
            python_callable=lambda:  str(round((rail.result(
                'log_1_i_n_d_privilege_leaveaccrualcalculation_15') * rail.result('log_numberofdaystobeconsideredforaccrual_14')), 2))
        )

        final_accrual_amount = rail.PythonOperator(
            task_id="final_accrual_amount",
            python_callable=lambda:  python_callable.round_off_accrual_value(
                rail.result("log_daystobeaccruedstartingbalance_92"))
        )

        log_time_offurifor1_i_n_d_privilegeleave_113 = rail.PythonOperator(
            task_id='log_time_offurifor1_i_n_d_privilegeleave_113',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getenabled_time_offtypes_19'), 'displayText', "1. IND_Privilege leave", 'uri')
        )

        getassignedpolicyforthetimeofftype_115 = rail.RepliconServiceOperator(
            task_id='getassignedpolicyforthetimeofftype_115',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], "timeOffType.uri",  rail.result('log_time_offurifor1_i_n_d_privilegeleave_113'), 'policySetSchedule')
        )

        if_to_s_contains_urn_119 = rail.IfOperator(
            task_id='if_to_s_contains_urn_119',
            test=lambda: bool('urn' in json.dumps(
                rail.result('getassignedpolicyforthetimeofftype_115'))),
            yes_task="log_previouspoliciesmodified_final_129",
            no_task="if_request_rehire_equals_to_rehire_158",
        )

        log_previouspoliciesmodified_final_129 = rail.PythonOperator(
            task_id='log_previouspoliciesmodified_final_129',
            python_callable=lambda:  python_callable.modify_previous_policy(
                rail.result("getassignedpolicyforthetimeofftype_115"))
        )

        getdefaultpolicyfor1_i_n_d_privilegeleave_131 = rail.RepliconServiceOperator(
            task_id='getdefaultpolicyfor1_i_n_d_privilegeleave_131',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('log_time_offurifor1_i_n_d_privilegeleave_113') }}"
            }
        )

        log_policysetmodified_134 = rail.PythonOperator(
            task_id='log_policysetmodified_134',
            python_callable=lambda:  rail.result(
                'getdefaultpolicyfor1_i_n_d_privilegeleave_131')[0]['policySet']
        )

        log_policy_modified_for_starting_balance = rail.PythonOperator(
            task_id='log_policy_modified_for_starting_balance',
            python_callable=lambda: python_callable.final_policy_starting_balance_modified(
                rail.result("log_policysetmodified_134"), rail.result("final_accrual_amount"))
        )

        log_finalpolicywithnewvaluewithpreviouspolicy_150 = rail.PythonOperator(
            task_id='log_finalpolicywithnewvaluewithpreviouspolicy_150',
            python_callable=lambda:  python_callable.new_old_policyset_append(rail.result("log_policy_modified_for_starting_balance"), rail.result(
                "log_previouspoliciesmodified_final_129"), rail.result('get_split_dates')['today'])
        )

        assign1_i_n_d_privilegeleavepolicy_153 = rail.RepliconServiceOperator(
            task_id='assign1_i_n_d_privilegeleavepolicy_153',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('log_time_offurifor1_i_n_d_privilegeleave_113')
                },
                "policySetScheduleEntries": rail.result("log_finalpolicywithnewvaluewithpreviouspolicy_150")
            }
        )

        if_request_rehire_equals_to_rehire_158 = rail.IfOperator(
            task_id='if_request_rehire_equals_to_rehire_158',
            test='''{{ dag_run.conf.rehire == 'rehire' }}''',
            yes_task="log_daystobeaccruedstartingbalance_159",
            no_task="log_checkif2_i_n_d_casual_leaveisassigned_225",
        )

        log_daystobeaccruedstartingbalance_159 = rail.PythonOperator(
            task_id='log_daystobeaccruedstartingbalance_159',
            python_callable=lambda:  str(round((rail.result(
                'log_1_i_n_d_privilege_leaveaccrualcalculation_15') * rail.result('log_numberofdaystobeconsideredforaccrual_14')), 2))
        )

        final_accrual_amount_privilage_leave = rail.PythonOperator(
            task_id="final_accrual_amount_privilage_leave",
            python_callable=lambda:  python_callable.round_off_accrual_value(
                rail.result("log_daystobeaccruedstartingbalance_159"))
        )

        log_time_offurifor1_i_n_d_privilegeleave_180 = rail.PythonOperator(
            task_id='log_time_offurifor1_i_n_d_privilegeleave_180',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getenabled_time_offtypes_19'), 'displayText', "1. IND_Privilege leave", 'uri')
        )

        getassignedpolicyforthetimeofftype_182 = rail.RepliconServiceOperator(
            task_id='getassignedpolicyforthetimeofftype_182',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], "timeOffType.uri",  rail.result('log_time_offurifor1_i_n_d_privilegeleave_180'), 'policySetSchedule')
        )

        if_to_s_contains_urn_186 = rail.IfOperator(
            task_id='if_to_s_contains_urn_186',
            test=lambda: bool('urn' in json.dumps(
                rail.result('getassignedpolicyforthetimeofftype_182'))),
            yes_task="log_previouspoliciesmodified_final_196",
            no_task="getdefaultpolicyfor1_i_n_d_privilegeleave_198",
        )

        log_previouspoliciesmodified_final_196 = rail.PythonOperator(
            task_id='log_previouspoliciesmodified_final_196',
            python_callable=lambda:  python_callable.modify_previous_policy(
                rail.result("getassignedpolicyforthetimeofftype_182"))
        )

        getdefaultpolicyfor1_i_n_d_privilegeleave_198 = rail.RepliconServiceOperator(
            task_id='getdefaultpolicyfor1_i_n_d_privilegeleave_198',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('log_time_offurifor1_i_n_d_privilegeleave_180') }}"
            }
        )

        log_policysetmodified_201 = rail.PythonOperator(
            task_id='log_policysetmodified_201',
            python_callable=lambda:  rail.result(
                'getdefaultpolicyfor1_i_n_d_privilegeleave_198')[0]['policySet']
        )

        log_starting_balance_modified_new_policyset = rail.PythonOperator(
            task_id="log_starting_balance_modified_new_policyset",
            python_callable=lambda:  python_callable.final_policy_starting_balance_modified(rail.result("log_policysetmodified_201"), rail.result(
                "final_accrual_amount_privilage_leave"))
        )

        log_finalpolicywithnewvaluewithpreviouspolicy_217 = rail.PythonOperator(
            task_id='log_finalpolicywithnewvaluewithpreviouspolicy_217',
            python_callable=lambda: python_callable.new_old_policyset_append(rail.result("log_starting_balance_modified_new_policyset"), rail.result(
                "log_previouspoliciesmodified_final_196"), rail.result('get_split_dates')['hire_date'])
        )

        assign1_i_n_d_privilegeleavepolicy_220 = rail.RepliconServiceOperator(
            task_id='assign1_i_n_d_privilegeleavepolicy_220',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('log_time_offurifor1_i_n_d_privilegeleave_180')
                },
                "policySetScheduleEntries": rail.result("log_finalpolicywithnewvaluewithpreviouspolicy_217")
            }
        )

        log_checkif2_i_n_d_casual_leaveisassigned_225 = rail.PythonOperator(
            task_id='log_checkif2_i_n_d_casual_leaveisassigned_225',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                "timeoff_name_list"), 'name', "2. IND_Casual Leave", 'name') if rail.result("timeoff_name_list") else ''
        )

        if_log_checkif2_i_n_d_casual_leaveisassigned_225_present_226 = rail.IfOperator(
            task_id='if_log_checkif2_i_n_d_casual_leaveisassigned_225_present_226',
            test='''{{ result('log_checkif2_i_n_d_casual_leaveisassigned_225') | is_truthy }}''',
            yes_task="log_checkif2_i_n_d_casual_leaveisalreadyassignedandenabled_227",
            no_task="finish",
        )

        log_checkif2_i_n_d_casual_leaveisalreadyassignedandenabled_227 = rail.PythonOperator(
            task_id='log_checkif2_i_n_d_casual_leaveisalreadyassignedandenabled_227',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('getassigned_time_offtypes_18')[
                                                                         0]['timeOffTypeAssignmentsDetails']['timeOffTypes'], 'displayText', "2. IND_Casual Leave", 'uri')
        )

        log_daystobeaccruedstartingbalance_228 = rail.PythonOperator(
            task_id='log_daystobeaccruedstartingbalance_228',
            python_callable=lambda:  str(round(float(rail.result(
                'log_2_i_n_d_casual_leaveaccrualcalculation_16') * rail.result('log_numberofdaystobeconsideredforaccrual_14')), 2))
        )

        final_accrual_days_casual_leave = rail.PythonOperator(
            task_id="final_accrual_days_casual_leave",
            python_callable=lambda:  python_callable.round_off_accrual_value(
                rail.result("log_daystobeaccruedstartingbalance_228"))
        )

        if_log_checkif2_i_n_d_casual_leaveisalreadyassignedandenabled_227_blank_no_248 = rail.IfOperator(
            task_id='if_log_checkif2_i_n_d_casual_leaveisalreadyassignedandenabled_227_blank_no_248',
            test='''{{ result('log_checkif2_i_n_d_casual_leaveisalreadyassignedandenabled_227') | is_falsy  or dag_run.conf.rehire == 'rehire' }}''',
            yes_task="log_time_offurifor2_i_n_d_casual_leave_250",
            no_task="finish",
        )

        log_time_offurifor2_i_n_d_casual_leave_250 = rail.PythonOperator(
            task_id='log_time_offurifor2_i_n_d_casual_leave_250',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getenabled_time_offtypes_19'), 'displayText', "2. IND_Casual Leave", 'uri')
        )

        getassignedpolicyforthetimeofftype_252 = rail.RepliconServiceOperator(
            task_id='getassignedpolicyforthetimeofftype_252',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], "timeOffType.uri",  rail.result('log_time_offurifor2_i_n_d_casual_leave_250'), 'policySetSchedule')
        )

        if_to_s_contains_urn_256 = rail.IfOperator(
            task_id='if_to_s_contains_urn_256',
            test=lambda: bool('urn' in json.dumps(
                rail.result('getassignedpolicyforthetimeofftype_252'))),
            yes_task="log_previouspoliciesmodified_final_266",
            no_task="getdefaultpolicyfor2_i_n_d_casual_leave_268",
        )

        log_previouspoliciesmodified_final_266 = rail.PythonOperator(
            task_id='log_previouspoliciesmodified_final_266',
            python_callable=lambda:  python_callable.modify_previous_policy(
                rail.result("getassignedpolicyforthetimeofftype_252"))
        )

        getdefaultpolicyfor2_i_n_d_casual_leave_268 = rail.RepliconServiceOperator(
            task_id='getdefaultpolicyfor2_i_n_d_casual_leave_268',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('log_time_offurifor2_i_n_d_casual_leave_250') }}"
            }
        )

        log_policysetmodified_271 = rail.PythonOperator(
            task_id='log_policysetmodified_271',
            python_callable=lambda:  rail.result(
                'getdefaultpolicyfor2_i_n_d_casual_leave_268')[0]['policySet']
        )

        log_newpolocywiththevalue_282 = rail.PythonOperator(
            task_id='log_newpolocywiththevalue_282',
            python_callable=lambda:  python_callable.final_policy_starting_balance_modified(rail.result("log_policysetmodified_271"), rail.result(
                "final_accrual_days_casual_leave"))
        )

        log_finalpolicywithnewvaluewithpreviouspolicy_290 = rail.PythonOperator(
            task_id='log_finalpolicywithnewvaluewithpreviouspolicy_290',
            python_callable=lambda dag_run: python_callable.modify_new_policy(rail.result(
                "log_newpolocywiththevalue_282"), rail.result("log_previouspoliciesmodified_final_266"), dag_run.conf['rehire'])
        )

        assign2_i_n_d_casual_leave_293 = rail.RepliconServiceOperator(
            task_id='assign2_i_n_d_casual_leave_293',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('log_time_offurifor2_i_n_d_casual_leave_250')
                },
                "policySetScheduleEntries": rail.result("log_finalpolicywithnewvaluewithpreviouspolicy_290")
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_split_dates

        get_split_dates >> log_final_number_of_days_in_year_and_day_of_the_year >> log_numberofdaystobeconsideredforaccrual_14 >> log_1_i_n_d_privilege_leaveaccrualcalculation_15 \
            >> log_2_i_n_d_casual_leaveaccrualcalculation_16 >> getassigned_time_offtypes_18 >> getenabled_time_offtypes_19

        getenabled_time_offtypes_19 >> momentive_userimport_mapper_search_entries_23 >> log_24 >> log_timeofftypestobeassigned_25 \
            >> if_log_timeofftypestobeassigned_25_present_26

        if_log_timeofftypestobeassigned_25_present_26 >> rail.Label(
            'No') >> finish
        if_log_timeofftypestobeassigned_25_present_26 >> rail.Label('Yes') >> log_removedelimiterfromthe_time_offtypeslisted_27 >> timeoff_name_list \
            >> log_finalsetof_time_offuris_35 >> if_log_finalsetof_time_offuris_35_present_36

        if_log_finalsetof_time_offuris_35_present_36 >> rail.Label(
            'No') >> get_previously_assigned_timeoff_uris
        if_log_finalsetof_time_offuris_35_present_36 >> rail.Label(
            'Yes') >> assignrequired_timeofftypes_38 >> get_previously_assigned_timeoff_uris

        get_previously_assigned_timeoff_uris >> log_checkif9_i_n_d_optional_holidayisassigned_45 >> if_log_checkif9_i_n_d_optional_holidayisassigned_45_present_46

        if_log_checkif9_i_n_d_optional_holidayisassigned_45_present_46 >> rail.Label(
            'No') >> log_checkif1_i_n_d_privilegeleaveisassigned_88
        if_log_checkif9_i_n_d_optional_holidayisassigned_45_present_46 >> rail.Label('Yes') \
            >> log_checkif9_i_n_d_optional_holidayisalreadyassignedandenabled_47 >> if_log_checkif9_i_n_d_optional_holidayisalreadyassignedandenabled_47_blank_48

        if_log_checkif9_i_n_d_optional_holidayisalreadyassignedandenabled_47_blank_48 >> rail.Label(
            'No') >> log_checkif1_i_n_d_privilegeleaveisassigned_88
        if_log_checkif9_i_n_d_optional_holidayisalreadyassignedandenabled_47_blank_48 >> rail.Label('Yes') >> log_timeoff_uri_for_ind_optional_holiday_50 \
            >> getassignedpolicyforthetimeofftype_52 >> if_to_s_contains_urn_56

        if_to_s_contains_urn_56 >> rail.Label(
            'No') >> getdefaultpolicyfor9_i_n_d_optional_holiday_68
        if_to_s_contains_urn_56 >> rail.Label(
            'Yes') >> log_previouspoliciesmodified_final_66 >> getdefaultpolicyfor9_i_n_d_optional_holiday_68

        getdefaultpolicyfor9_i_n_d_optional_holiday_68 >> log_policysetmodified_71 >> get_final_modified_policyset_schedule \
            >> assign_ind_optional_holiday >> log_checkif1_i_n_d_privilegeleaveisassigned_88

        log_checkif1_i_n_d_privilegeleaveisassigned_88 >> if_log_checkif1_i_n_d_privilegeleaveisassigned_88_present_89

        if_log_checkif1_i_n_d_privilegeleaveisassigned_88_present_89 >> rail.Label(
            'No') >> log_checkif2_i_n_d_casual_leaveisassigned_225
        if_log_checkif1_i_n_d_privilegeleaveisassigned_88_present_89 >> rail.Label(
            'Yes') >> log_checkif1_i_n_d_privilegeleaveisalreadyassignedandenabled_90 >> if_log_checkif1_i_n_d_privilegeleaveisalreadyassignedandenabled_90_blank_no_91

        if_log_checkif1_i_n_d_privilegeleaveisalreadyassignedandenabled_90_blank_no_91 >> rail.Label(
            'No') >> if_request_rehire_equals_to_rehire_158
        if_log_checkif1_i_n_d_privilegeleaveisalreadyassignedandenabled_90_blank_no_91 >> rail.Label('Yes') >> log_daystobeaccruedstartingbalance_92 \
            >> final_accrual_amount >> log_time_offurifor1_i_n_d_privilegeleave_113 >> getassignedpolicyforthetimeofftype_115 >> if_to_s_contains_urn_119

        if_to_s_contains_urn_119 >> rail.Label(
            'No') >> if_request_rehire_equals_to_rehire_158
        if_to_s_contains_urn_119 >> rail.Label('Yes') >> log_previouspoliciesmodified_final_129 >> getdefaultpolicyfor1_i_n_d_privilegeleave_131 \
            >> log_policysetmodified_134 >> log_policy_modified_for_starting_balance >> log_finalpolicywithnewvaluewithpreviouspolicy_150 >> assign1_i_n_d_privilegeleavepolicy_153 >> if_request_rehire_equals_to_rehire_158

        if_request_rehire_equals_to_rehire_158 >> rail.Label(
            'No') >> log_checkif2_i_n_d_casual_leaveisassigned_225
        if_request_rehire_equals_to_rehire_158 >> rail.Label('Yes') >> log_daystobeaccruedstartingbalance_159 >> final_accrual_amount_privilage_leave \
            >> log_time_offurifor1_i_n_d_privilegeleave_180 >> getassignedpolicyforthetimeofftype_182 >> if_to_s_contains_urn_186

        if_to_s_contains_urn_186 >> rail.Label(
            'No') >> getdefaultpolicyfor1_i_n_d_privilegeleave_198
        if_to_s_contains_urn_186 >> rail.Label(
            'Yes') >> log_previouspoliciesmodified_final_196 >> getdefaultpolicyfor1_i_n_d_privilegeleave_198

        getdefaultpolicyfor1_i_n_d_privilegeleave_198 >> log_policysetmodified_201 >> log_starting_balance_modified_new_policyset >> log_finalpolicywithnewvaluewithpreviouspolicy_217 >> assign1_i_n_d_privilegeleavepolicy_220 \
            >> log_checkif2_i_n_d_casual_leaveisassigned_225 >> if_log_checkif2_i_n_d_casual_leaveisassigned_225_present_226

        if_log_checkif2_i_n_d_casual_leaveisassigned_225_present_226 >> rail.Label(
            'No') >> finish
        if_log_checkif2_i_n_d_casual_leaveisassigned_225_present_226 >> rail.Label(
            'Yes') >> log_checkif2_i_n_d_casual_leaveisalreadyassignedandenabled_227

        log_checkif2_i_n_d_casual_leaveisalreadyassignedandenabled_227 >> log_daystobeaccruedstartingbalance_228 >> final_accrual_days_casual_leave >> if_log_checkif2_i_n_d_casual_leaveisalreadyassignedandenabled_227_blank_no_248

        if_log_checkif2_i_n_d_casual_leaveisalreadyassignedandenabled_227_blank_no_248 >> rail.Label(
            'No') >> finish
        if_log_checkif2_i_n_d_casual_leaveisalreadyassignedandenabled_227_blank_no_248 >> rail.Label(
            'Yes') >> log_time_offurifor2_i_n_d_casual_leave_250

        log_time_offurifor2_i_n_d_casual_leave_250 >> getassignedpolicyforthetimeofftype_252 >> if_to_s_contains_urn_256

        if_to_s_contains_urn_256 >> rail.Label(
            'No') >> getdefaultpolicyfor2_i_n_d_casual_leave_268
        if_to_s_contains_urn_256 >> rail.Label('Yes') >> log_previouspoliciesmodified_final_266 \
            >> getdefaultpolicyfor2_i_n_d_casual_leave_268 >> log_policysetmodified_271 >> log_newpolocywiththevalue_282 \
            >> log_finalpolicywithnewvaluewithpreviouspolicy_290 >> assign2_i_n_d_casual_leave_293 >> finish

    return dag


rail.for_each_instance(create_dag)
