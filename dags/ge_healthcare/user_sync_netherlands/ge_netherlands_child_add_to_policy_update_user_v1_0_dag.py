
from datetime import timedelta
import json
from airflow.models import Variable
from ge_healthcare.user_sync_netherlands.netherlands_timeoff_mapper import netherlands_timeoff_mapper
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'gehealthcare_netherlands_child_add_to_policy_update_user_v1_0_{config.instance}',
        description=f'GE_netherlands_Child Workflow to add timeoff policy for Update user v1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
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
            no_task='if_foreach_declare_list_12_24_uri_present_25'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_foreach_declare_list_12_24_uri_present_25',
            end_task='add_timeoff_type_logs_23',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_foreach_declare_list_12_24_uri_present_25 = rail.IfOperator(
            task_id='if_foreach_declare_list_12_24_uri_present_25',
            test='''{{ dag_run.conf.uri | is_truthy }}''',
            yes_task="if_request_type_equals_to_jobtypeupdate_27",
            no_task="add_timeoff_type_logs_23",
        )

        if_request_type_equals_to_jobtypeupdate_27 = rail.IfOperator(
            task_id='if_request_type_equals_to_jobtypeupdate_27',
            test='''{{ dag_run.conf.type == 'Job Type Update'  and dag_run.conf.name != 'Bank Holiday' }}''',
            yes_task="ge_netherlands_timeoff_mapper_search_entries_27",
            no_task="if_request_type_equals_to_update_53",
        )

        def get_timeoff_type_from_mapper(dag_run):
            legacypayrollid = dag_run.conf['legacypayrollid'] + "|" + dag_run.conf['payrule'] if dag_run.conf[
                'name'] == '03. NL_ATV' and dag_run.conf['legacypayrollid'] == '00013105' else dag_run.conf['legacypayrollid']
            to_info = list(filter(lambda x: x["timeoff_type_name"] == dag_run.conf['name']
                           and x['legacy_payroll_id_|_payrule'] == legacypayrollid, netherlands_timeoff_mapper))
            return to_info[0] if to_info else None

        ge_netherlands_timeoff_mapper_search_entries_27 = rail.PythonOperator(
            task_id='ge_netherlands_timeoff_mapper_search_entries_27',
            python_callable=get_timeoff_type_from_mapper
        )

        if_first_id_blank_28 = rail.IfOperator(
            task_id='if_first_id_blank_28',
            test='''{{ result('ge_netherlands_timeoff_mapper_search_entries_27') | is_falsy }}''',
            yes_task="get_default_time_off_type_policy_schedule_for_user_30",
            no_task="if_entry_col5_equals_to_yes_36",
        )

        get_default_time_off_type_policy_schedule_for_user_30 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_30',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ dag_run.conf.uri }}"
                }
            }
        )

        log_timeoff_policy_32 = rail.PythonOperator(
            task_id='log_timeoff_policy_32',
            python_callable=lambda:  json.loads(json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_30'), ensure_ascii=False).replace(
                'null', '"effective"').replace('"script"', '"scriptTarget"')) if rail.result('get_default_time_off_type_policy_schedule_for_user_30') else None
        )

        if_log_timeoff_policy_32_present_33 = rail.IfOperator(
            task_id='if_log_timeoff_policy_32_present_33',
            test='''{{ result('log_timeoff_policy_32') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_34",
            no_task="if_request_type_equals_to_update_53",
        )

        put_user_time_off_account_policy_set_schedule_34 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_34',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_32')
            }
        )

        if_entry_col5_equals_to_yes_36 = rail.IfOperator(
            task_id='if_entry_col5_equals_to_yes_36',
            test='''{{ result('ge_netherlands_timeoff_mapper_search_entries_27').assign_policy == 'Yes' }}''',
            yes_task="if_entry_col3_equals_to_yes_37",
            no_task="accumulate_list_items_51",
        )

        if_entry_col3_equals_to_yes_37 = rail.IfOperator(
            task_id='if_entry_col3_equals_to_yes_37',
            test='''{{ result('ge_netherlands_timeoff_mapper_search_entries_27').default == 'Yes' }}''',
            yes_task="trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_038",
            no_task="if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_40",
        )

        trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_038 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_038',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_netherlands_child_timeoff_type_proration_assignment_for_default_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "userloginname": "{{ dag_run.conf.userloginname }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "type": "Add",
                "timeoffuri": "{{ dag_run.conf.uri }}",
                "scheduledweeklyhours": "{{ dag_run.conf.scheduledweeklyhours }}",
                "fullpart": "{{ dag_run.conf.fullpart }}",
                "timeofftype": "{{ dag_run.conf.name }}"
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_038 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_038',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_038") }}'
        )

        if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_40 = rail.IfOperator(
            task_id='if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_40',
            test='''{{ dag_run.conf.name == '06. NL_Wedding Leave' }}''',
            yes_task="trigger_dag_run_timeoff_type_proration_assignment_for_06_nl_wedding_leave_v1_042",
            no_task="if_foreach_declare_list_12_24_name_equals_to_02nl_extraholidays_42",
        )

        trigger_dag_run_timeoff_type_proration_assignment_for_06_nl_wedding_leave_v1_042 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_timeoff_type_proration_assignment_for_06_nl_wedding_leave_v1_042',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_netherlands_child_timeoff_type_proration_assignment_for_06_nl_wedding_leave_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['userloginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": "Add",
                "timeoffuri": dag_run.conf['uri'],
                "timeofftype": dag_run.conf['name'],
                "accrual": rail.result('ge_netherlands_timeoff_mapper_search_entries_27')['accural_need_to_be_added_|_accrual'].split('|')[-1]
            }
        )

        wait_for_completion_trigger_dag_run_timeoff_type_proration_assignment_for_06_nl_wedding_leave_v1_042 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_timeoff_type_proration_assignment_for_06_nl_wedding_leave_v1_042',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_timeoff_type_proration_assignment_for_06_nl_wedding_leave_v1_042") }}'
        )

        if_foreach_declare_list_12_24_name_equals_to_02nl_extraholidays_42 = rail.IfOperator(
            task_id='if_foreach_declare_list_12_24_name_equals_to_02nl_extraholidays_42',
            test='''{{ dag_run.conf.name == '04. NL_Seniority Days (Start Date)' }}''',
            yes_task="trigger_dag_run_child_timeoff_type_proration_assignment_for_04_nl_seniority_days_44",
            no_task="if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_45",
        )

        trigger_dag_run_child_timeoff_type_proration_assignment_for_04_nl_seniority_days_44 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_child_timeoff_type_proration_assignment_for_04_nl_seniority_days_44',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_netherlands_child_timeoff_type_proration_assignment_for_04_nl_seniority_days_start_date_v1_0{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['userloginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": "Add",
                "timeoffuri": dag_run.conf['uri'],
                "legacypayrollid": dag_run.conf['legacypayrollid']
            }
        )

        wait_for_completion_trigger_dag_run_child_timeoff_type_proration_assignment_for_04_nl_seniority_days_44 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_child_timeoff_type_proration_assignment_for_04_nl_seniority_days_44',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_child_timeoff_type_proration_assignment_for_04_nl_seniority_days_44") }}'
        )

        if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_45 = rail.IfOperator(
            task_id='if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_45',
            test='''{{ dag_run.conf.name == '01. NL_Legal Holidays' }}''',
            yes_task="trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_046",
            no_task="if_foreach_declare_list_12_24_name_equals_to_06nl_weddingleave_46",
        )

        trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_046 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_046',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_user_sync_netherlands_child_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['userloginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": "Add",
                "timeoffuri": dag_run.conf['uri'],
                "scheduledweeklyhours": dag_run.conf['scheduledweeklyhours'],
                "fullpart": dag_run.conf['fullpart'],
                "timeofftype": dag_run.conf['name'],
                "carryover": float(rail.result('ge_netherlands_timeoff_mapper_search_entries_27')['carryover|units'].split('|')[0]),
                "units": rail.result('ge_netherlands_timeoff_mapper_search_entries_27')['carryover|units'].split('|')[-1]
            }
        )

        wait_for_completion_trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_046 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_046',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_046") }}'
        )

        if_foreach_declare_list_12_24_name_equals_to_06nl_weddingleave_46 = rail.IfOperator(
            task_id='if_foreach_declare_list_12_24_name_equals_to_06nl_weddingleave_46',
            test='''{{ dag_run.conf.name == '02. NL_Extra Holidays' }}''',
            yes_task="trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_048",
            no_task="if_foreach_declare_list_12_24_name_equals_to_03nl_atv_48",
        )

        trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_048 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_048',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_user_sync_netherlands_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['userloginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": "Add",
                "timeoffuri": dag_run.conf['uri'],
                "scheduledweeklyhours": dag_run.conf['scheduledweeklyhours'],
                "fullpart": dag_run.conf['fullpart'],
                "timeofftype": dag_run.conf['name'],
                "accrual": float(rail.result('ge_netherlands_timeoff_mapper_search_entries_27')['accural_need_to_be_added_|_accrual'].split('|')[-1])
            }
        )

        wait_for_completion_trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_048 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_048',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_048") }}'
        )

        if_foreach_declare_list_12_24_name_equals_to_03nl_atv_48 = rail.IfOperator(
            task_id='if_foreach_declare_list_12_24_name_equals_to_03nl_atv_48',
            test='''{{ dag_run.conf.name == '03. NL_ATV' }}''',
            yes_task="trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_049",
            no_task="if_request_type_equals_to_update_53",
        )

        trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_049 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_049',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_user_sync_ge_netherlands_child_timeoff_type_proration_assignment_for_03_nl_atv_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['userloginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": "Add",
                "timeoffuri": dag_run.conf['uri'],
                "timeofftype": dag_run.conf['name'],
                "scheduledweeklyhours": dag_run.conf['scheduledweeklyhours'],
                "fullpart": dag_run.conf['fullpart'],
                "payrule": dag_run.conf['payrule'],
                "legacypayrollid": dag_run.conf['legacypayrollid']
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_049 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_049',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_049") }}'
        )

        accumulate_list_items_51 = rail.SetVariableOperator(
            task_id='accumulate_list_items_51',
            name='No Policy Assigned',
            append=True,
            value={
                "timeoff_type_name": "{{ dag_run.conf.name }}",
                "details": "Assign Policy Set to No"
            }
        )

        if_request_type_equals_to_update_53 = rail.IfOperator(
            task_id='if_request_type_equals_to_update_53',
            test='''{{ dag_run.conf.type == 'Update' }}''',
            yes_task="ge_netherlands_timeoff_mapper_search_entries_54",
            no_task="add_timeoff_type_logs_23",
        )

        def get_update_timeoff_type_from_mapper(dag_run):
            legacypayrollid = dag_run.conf['legacypayrollid'] + "|" + dag_run.conf['payrule'] if dag_run.conf[
                'name'] == '03. NL_ATV' and dag_run.conf['legacypayrollid'] == '00013105' else dag_run.conf['legacypayrollid']
            to_info = list(filter(lambda x: x["timeoff_type_name"] == dag_run.conf['name']
                           and x['legacy_payroll_id_|_payrule'] == legacypayrollid, netherlands_timeoff_mapper))
            return to_info[0] if to_info else None

        ge_netherlands_timeoff_mapper_search_entries_54 = rail.PythonOperator(
            task_id='ge_netherlands_timeoff_mapper_search_entries_54',
            python_callable=get_update_timeoff_type_from_mapper
        )

        if_first_id_present_55 = rail.IfOperator(
            task_id='if_first_id_present_55',
            test='''{{ result('ge_netherlands_timeoff_mapper_search_entries_54') | is_truthy  and result('ge_netherlands_timeoff_mapper_search_entries_54').assign_policy == 'Yes' }}''',
            yes_task="if_entry_col3_equals_to_yes_56",
            no_task="add_timeoff_type_logs_23",
        )

        if_entry_col3_equals_to_yes_56 = rail.IfOperator(
            task_id='if_entry_col3_equals_to_yes_56',
            test='''{{ result('ge_netherlands_timeoff_mapper_search_entries_54').default == 'Yes' }}''',
            yes_task="trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_57",
            no_task="if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_59",
        )

        trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_57 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_57',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_netherlands_child_timeoff_type_proration_assignment_for_default_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "userloginname": "{{ dag_run.conf.userloginname }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "type": "{{ dag_run.conf.type }}",
                "timeoffuri": "{{ dag_run.conf.uri }}",
                "scheduledweeklyhours": "{{ dag_run.conf.scheduledweeklyhours }}",
                "fullpart": "{{ dag_run.conf.fullpart }}",
                "timeofftype": "{{ dag_run.conf.name }}"
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_57 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_57',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_57") }}'
        )

        if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_59 = rail.IfOperator(
            task_id='if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_59',
            test='''{{ dag_run.conf.name == '01. NL_Legal Holidays' }}''',
            yes_task="trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_60",
            no_task="if_foreach_declare_list_12_24_name_equals_to_02nl_extraholidays_61",
        )

        trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_60 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_60',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_user_sync_netherlands_child_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['userloginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": dag_run.conf['type'],
                "timeoffuri": dag_run.conf['uri'],
                "scheduledweeklyhours": dag_run.conf['scheduledweeklyhours'],
                "fullpart": dag_run.conf['fullpart'],
                "timeofftype": dag_run.conf['name'],
                "carryover": float(rail.result('ge_netherlands_timeoff_mapper_search_entries_54')['carryover|units'].split('|')[0]),
                "units": rail.result('ge_netherlands_timeoff_mapper_search_entries_54')['carryover|units'].split('|')[-1]
            }
        )

        wait_for_completion_trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_60 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_60',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_60") }}'
        )

        if_foreach_declare_list_12_24_name_equals_to_02nl_extraholidays_61 = rail.IfOperator(
            task_id='if_foreach_declare_list_12_24_name_equals_to_02nl_extraholidays_61',
            test='''{{ dag_run.conf.name == '02. NL_Extra Holidays' }}''',
            yes_task="trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_62",
            no_task="if_foreach_declare_list_12_24_name_equals_to_03nl_atv_63",
        )

        trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_62 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_62',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_user_sync_netherlands_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['userloginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": dag_run.conf['type'],
                "timeoffuri": dag_run.conf['uri'],
                "scheduledweeklyhours": dag_run.conf['scheduledweeklyhours'],
                "fullpart": dag_run.conf['fullpart'],
                "timeofftype": dag_run.conf['name'],
                "accrual": float(rail.result('ge_netherlands_timeoff_mapper_search_entries_54')['accural_need_to_be_added_|_accrual'].split('|')[-1])
            }
        )

        wait_for_completion_trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_62 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_62',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_62") }}'
        )

        if_foreach_declare_list_12_24_name_equals_to_03nl_atv_63 = rail.IfOperator(
            task_id='if_foreach_declare_list_12_24_name_equals_to_03nl_atv_63',
            test='''{{ dag_run.conf.name == '03. NL_ATV' }}''',
            yes_task="trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_64",
            no_task="add_timeoff_type_logs_23",
        )

        trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_64 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_64',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_user_sync_ge_netherlands_child_timeoff_type_proration_assignment_for_03_nl_atv_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['userloginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": "Add",
                "timeoffuri": dag_run.conf['uri'],
                "timeofftype": dag_run.conf['name'],
                "scheduledweeklyhours": dag_run.conf['scheduledweeklyhours'],
                "fullpart": dag_run.conf['fullpart'],
                "payrule": dag_run.conf['payrule'],
                "legacypayrollid": dag_run.conf['legacypayrollid']
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_64 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_64',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_64") }}'
        )

        dummy_operator_1 = rail.EmptyOperator(
            task_id="dummy_operator_1"
        )

        add_timeoff_type_logs_23 = rail.WriteLogOperator(
            task_id='add_timeoff_type_logs_23',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "action": "{{ dag_run.conf.type }}",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.userloginname }}",
                "username": ""
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> add_timeoff_type_logs_23
        can_run_batch_task >> rail.Label(
            'No') >> if_foreach_declare_list_12_24_uri_present_25
        if_foreach_declare_list_12_24_uri_present_25 >> rail.Label(
            'No') >> add_timeoff_type_logs_23
        if_foreach_declare_list_12_24_uri_present_25 >> rail.Label(
            'Yes') >> if_request_type_equals_to_jobtypeupdate_27
        if_request_type_equals_to_jobtypeupdate_27 >> rail.Label(
            'No') >> if_request_type_equals_to_update_53
        if_request_type_equals_to_jobtypeupdate_27 >> rail.Label('Yes') >> ge_netherlands_timeoff_mapper_search_entries_27 >> \
            if_first_id_blank_28
        if_first_id_blank_28 >> rail.Label(
            'No') >> if_entry_col5_equals_to_yes_36
        if_first_id_blank_28 >> rail.Label('Yes') >> get_default_time_off_type_policy_schedule_for_user_30 >> log_timeoff_policy_32 >> \
            if_log_timeoff_policy_32_present_33
        if_log_timeoff_policy_32_present_33 >> rail.Label(
            'No') >> if_request_type_equals_to_update_53
        if_log_timeoff_policy_32_present_33 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_34 >> if_request_type_equals_to_update_53
        if_entry_col5_equals_to_yes_36 >> rail.Label(
            'No') >> accumulate_list_items_51 >> if_request_type_equals_to_update_53
        if_entry_col5_equals_to_yes_36 >> rail.Label(
            'Yes') >> if_entry_col3_equals_to_yes_37
        if_entry_col3_equals_to_yes_37 >> rail.Label(
            'No') >> if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_40
        if_entry_col3_equals_to_yes_37 >> rail.Label('Yes') >> trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_038 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_038 >> if_request_type_equals_to_update_53
        if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_40 >> rail.Label(
            'No') >> if_foreach_declare_list_12_24_name_equals_to_02nl_extraholidays_42
        if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_40 >> rail.Label('Yes') >> trigger_dag_run_timeoff_type_proration_assignment_for_06_nl_wedding_leave_v1_042 >> \
            wait_for_completion_trigger_dag_run_timeoff_type_proration_assignment_for_06_nl_wedding_leave_v1_042 >> if_foreach_declare_list_12_24_name_equals_to_02nl_extraholidays_42
        if_foreach_declare_list_12_24_name_equals_to_02nl_extraholidays_42 >> rail.Label(
            'No') >> if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_45
        if_foreach_declare_list_12_24_name_equals_to_02nl_extraholidays_42 >> rail.Label('Yes') >> trigger_dag_run_child_timeoff_type_proration_assignment_for_04_nl_seniority_days_44 >> \
            wait_for_completion_trigger_dag_run_child_timeoff_type_proration_assignment_for_04_nl_seniority_days_44 >> \
            if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_45
        if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_45 >> rail.Label('No') >> \
            if_foreach_declare_list_12_24_name_equals_to_06nl_weddingleave_46
        if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_45 >> rail.Label('Yes') >> \
            trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_046 >> \
            wait_for_completion_trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_046 >> \
            if_foreach_declare_list_12_24_name_equals_to_06nl_weddingleave_46
        if_foreach_declare_list_12_24_name_equals_to_06nl_weddingleave_46 >> rail.Label(
            'No') >> if_foreach_declare_list_12_24_name_equals_to_03nl_atv_48
        if_foreach_declare_list_12_24_name_equals_to_06nl_weddingleave_46 >> rail.Label('Yes') >> \
            trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_048 >> \
            wait_for_completion_trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_048 >> \
            if_foreach_declare_list_12_24_name_equals_to_03nl_atv_48
        if_foreach_declare_list_12_24_name_equals_to_03nl_atv_48 >> rail.Label(
            'No') >> if_request_type_equals_to_update_53
        if_foreach_declare_list_12_24_name_equals_to_03nl_atv_48 >> rail.Label('Yes') >> \
            trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_049 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_049 >> \
            if_request_type_equals_to_update_53
        if_request_type_equals_to_update_53 >> rail.Label(
            'No') >> add_timeoff_type_logs_23
        if_request_type_equals_to_update_53 >> rail.Label(
            'Yes') >> ge_netherlands_timeoff_mapper_search_entries_54 >> if_first_id_present_55
        if_first_id_present_55 >> rail.Label('No') >> add_timeoff_type_logs_23
        if_first_id_present_55 >> rail.Label(
            'Yes') >> if_entry_col3_equals_to_yes_56
        if_entry_col3_equals_to_yes_56 >> rail.Label(
            'No') >> if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_59
        if_entry_col3_equals_to_yes_56 >> rail.Label('Yes') >> trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_57 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_child_timeoff_type_proration_assignment_for_default_v1_57 >> dummy_operator_1
        if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_59 >> rail.Label(
            'No') >> if_foreach_declare_list_12_24_name_equals_to_02nl_extraholidays_61
        if_foreach_declare_list_12_24_name_equals_to_01nl_legalholidays_59 >> rail.Label('Yes') >> \
            trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_60 >> \
            wait_for_completion_trigger_dag_run_ge_timeoff_type_proration_assignment_for_01_nl_legal_holidays_v1_60 >> \
            if_foreach_declare_list_12_24_name_equals_to_02nl_extraholidays_61
        if_foreach_declare_list_12_24_name_equals_to_02nl_extraholidays_61 >> rail.Label(
            'No') >> if_foreach_declare_list_12_24_name_equals_to_03nl_atv_63
        if_foreach_declare_list_12_24_name_equals_to_02nl_extraholidays_61 >> rail.Label('Yes') >> \
            trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_62 >> \
            wait_for_completion_trigger_dag_run_ge_child_timeoff_type_proration_assignment_for_02_nl_extra_holidays_v1_62 >> \
            if_foreach_declare_list_12_24_name_equals_to_03nl_atv_63
        if_foreach_declare_list_12_24_name_equals_to_03nl_atv_63 >> rail.Label(
            'No') >> add_timeoff_type_logs_23
        if_foreach_declare_list_12_24_name_equals_to_03nl_atv_63 >> rail.Label('Yes') >> \
            trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_64 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_timeoff_type_proration_assignment_for_03_nl_atv_v1_64 >> \
            dummy_operator_1 >> add_timeoff_type_logs_23 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
