from datetime import timedelta
import json
from airflow.models import Variable
import rail
from momentive.user_import_japan.utils import python_callable, request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_japan_user_sync_child_add_timeoff_new_user_dag_id,
        description=f'Momentive_user_sync_Timeoff_add_new_user_child_{config.instance}',
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
            no_task='create_child_trigger_list'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_child_trigger_list',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_child_trigger_list = rail.SetVariableOperator(
            task_id='create_child_trigger_list',
            name='childtriggeredlist',
            append=False,
            value=[]
        )

        if_workshift_eff_date_present = rail.IfOperator(
            task_id='if_workshift_eff_date_present',
            test='{{ dag_run.conf.work_shift_change_effective_date | is_truthy }}',
            yes_task='split_workshift_eff_date',
            no_task='get_enabled_timeoff_types'
        )

        split_workshift_eff_date = rail.PythonOperator(
            task_id='split_workshift_eff_date',
            python_callable= lambda dag_run: python_callable.split_date_string(dag_run.conf['work_shift_change_effective_date']),
        )

        get_enabled_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_enabled_timeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        extract_employment_month = rail.PythonOperator(
            task_id='extract_employment_month',
            python_callable=lambda dag_run: int(dag_run.conf.get('startdate', '').split('-')[1]) if dag_run.conf.get('startdate', '').count('-') >= 2 else None
        )

        parse_timeoff_types_input = rail.PythonOperator(
            task_id='parse_timeoff_types_input',
            python_callable=lambda dag_run: dag_run.conf.get('timeofftypes', '').split('|') if dag_run.conf.get('timeofftypes') else []
        )

        create_matched_timeoff_uris = rail.SetVariableOperator(
            task_id='create_matched_timeoff_uris',
            append=False,
            name='matched_timeoff_uris',
            value=[]
        )

        # Get prorated timeoff accrual based on employment month
        get_timeoff_accrual_prorated_value = rail.PythonOperator(
            task_id='get_timeoff_accrual_prorated_value',
            python_callable= lambda: python_callable.get_timeoff_accrual_hours(rail.result('extract_employment_month'))
        )

        # STEP 3: Build complete matched timeoff types list in ONE task
        # Replaces the broken ForEach accumulation pattern
        build_matched_types = rail.PythonOperator(
            task_id='build_matched_types',
            python_callable= lambda: python_callable.build_timeoff_uri_list(rail.result('get_enabled_timeoff_types'),rail.result('parse_timeoff_types_input'))
        )

        # Store the matched types list to a DAG variable (following SetVariableOperator pattern)
        store_matched_types = rail.SetVariableOperator(
            task_id='store_matched_types',
            append=False,
            name='{{ result("create_matched_timeoff_uris").name }}',
            value=lambda: rail.result('build_matched_types')
        )

        # Assign selected timeoff types to user
        assign_timeoff_types_to_user = rail.RepliconServiceOperator(
            task_id='assign_timeoff_types_to_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.assign_timeoff_types_payload
        )

        foreach_timeofftype_uri = rail.ForEachOperator(
            task_id='foreach_timeofftype_uri',
            items=lambda: rail.get_dag_run_var('matched_timeoff_uris') or [],
            start_task='if_timeoff_uris_present',
            end_task='foreach_timeofftype_uri_end'
        )

        if_timeoff_uris_present = rail.IfOperator(
            task_id='if_timeoff_uris_present',
            test=lambda: rail.result('foreach_timeofftype_uri').get('uri') is not None,
            yes_task='if_name_equals_annual_paid_leave_newhire',
            no_task='foreach_timeofftype_uri_end'
        )

        if_name_equals_annual_paid_leave_newhire = rail.IfOperator(
            task_id='if_name_equals_annual_paid_leave_newhire',
            test=lambda:  rail.result('foreach_timeofftype_uri').get('name') == '02. JPN_年次有給休暇 Annual Paid Leave (New Hire)',
            yes_task='foreach_timeofftype_uri_end',
            no_task='if_continuos_service_date_present'
        )

        if_continuos_service_date_present = rail.IfOperator(
            task_id='if_continuos_service_date_present',
            test='{{ dag_run.conf.continous_service_date | is_truthy }}',
            yes_task='if_name_equals_annual_paid_leave_leave_fixed_term_standard',
            no_task='if_name_not_equals_1345678910_42'
        )

        if_name_equals_annual_paid_leave_leave_fixed_term_standard = rail.IfOperator(
            task_id='if_name_equals_annual_paid_leave_leave_fixed_term_standard',
            test=lambda:  rail.result('foreach_timeofftype_uri').get('name') == '05. JPN_年次有給休暇 Annual Paid Leave (Fixed Term - Standard)',
            yes_task='trigger_annual_leave_policy_fixed_term_standard_parttime_assignment',
            no_task='if_name_equals_annual_paid_leave_leave_fixed_term_parttime'
        )

        trigger_annual_leave_policy_fixed_term_standard_parttime_assignment = rail.TriggerDagRunOperator(
            task_id='trigger_annual_leave_policy_fixed_term_standard_parttime_assignment',
            trigger_dag_id=config.momentive_japan_user_sync_child_annual_leave_policy_fixed_term_standard_parttime_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "userloginname": dag_run.conf['loginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": "add",
                "timeoffuri": rail.result('foreach_timeofftype_uri').get('uri'),
                "timeofftype": rail.result('foreach_timeofftype_uri').get('name'),
                "yoss" : dag_run.conf['continous_service_date']  if dag_run.conf['continous_service_date'] else None,
                "yos": dag_run.conf['continous_service_date']  if dag_run.conf['continous_service_date'] else None
            }
        )

        insert_childid_to_wait_list_1 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_1',
            name="{{result('create_child_trigger_list').name}}",
            append=True,
            value="{{result('trigger_annual_leave_policy_fixed_term_standard_parttime_assignment')}}"
        )

        if_name_equals_annual_paid_leave_leave_fixed_term_parttime = rail.IfOperator(
            task_id='if_name_equals_annual_paid_leave_leave_fixed_term_parttime',
            test=lambda:  rail.result('foreach_timeofftype_uri').get('name') == '04. JPN_年次有給休暇 Annual Paid Leave (Fixed Term - Part Time)',
            yes_task='trigger_annual_leave_policy_fixed_term_parttime_assignment_add',
            no_task='if_name_equals_annual_paid_leave_leave_regular'
        )

        trigger_annual_leave_policy_fixed_term_parttime_assignment_add = rail.TriggerDagRunOperator(
            task_id='trigger_annual_leave_policy_fixed_term_parttime_assignment_add',
            trigger_dag_id=config.momentive_japan_user_sync_child_annual_leave_policy_parttime_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "userloginname": dag_run.conf['loginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": "add",
                "timeoffuri": rail.result('foreach_timeofftype_uri').get('uri'),
                "timeofftype": rail.result('foreach_timeofftype_uri').get('name'),
                "yoss" : dag_run.conf['continous_service_date']  if dag_run.conf['continous_service_date'] else None,
                "yos": dag_run.conf['continous_service_date']  if dag_run.conf['continous_service_date'] else None
            }
        )

        insert_childid_to_wait_list_2 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_2',
            name="{{result('create_child_trigger_list').name}}",
            append=True,
            value="{{result('trigger_annual_leave_policy_fixed_term_parttime_assignment_add')}}"
        )

        if_name_equals_annual_paid_leave_leave_regular = rail.IfOperator(
            task_id='if_name_equals_annual_paid_leave_leave_regular',
            test=lambda:  rail.result('foreach_timeofftype_uri').get('name') == '01. JPN_年次有給休暇 Annual Paid Leave (Regular)',
            yes_task='trigger_annual_leave_policy_regular_assignment_add',
            no_task='if_name_equals_annual_paid_leave_leave_fixed_term_rehireafterretirement'
        )

        trigger_annual_leave_policy_regular_assignment_add = rail.TriggerDagRunOperator(
            task_id='trigger_annual_leave_policy_regular_assignment_add',
            trigger_dag_id=config.momentive_japan_user_sync_child_annual_leave_policy_regular_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "userloginname": dag_run.conf['loginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": "add",
                "timeoffuri": rail.result('foreach_timeofftype_uri').get('uri'),
                "timeofftype": rail.result('foreach_timeofftype_uri').get('name'),
                "yoss" : dag_run.conf['continous_service_date']  if dag_run.conf['continous_service_date'] else None,
                "yos": dag_run.conf['continous_service_date']  if dag_run.conf['continous_service_date'] else None
            }
        )

        insert_childid_to_wait_list_3 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_3',
            name="{{result('create_child_trigger_list').name}}",
            append=True,
            value="{{result('trigger_annual_leave_policy_regular_assignment_add')}}"
        )

        if_name_equals_annual_paid_leave_leave_fixed_term_rehireafterretirement = rail.IfOperator(
            task_id='if_name_equals_annual_paid_leave_leave_fixed_term_rehireafterretirement',
            test=lambda:  rail.result('foreach_timeofftype_uri').get('name') == '03. JPN_年次有給休暇 Annual Paid Leave (Fixed Term - Rehire after Retirement)',
            yes_task='trigger_annual_leave_policy_fixed_term_rehire_assignment',
            no_task='if_name_not_equals_1345678910_42'
        )

        trigger_annual_leave_policy_fixed_term_rehire_assignment = rail.TriggerDagRunOperator(
            task_id='trigger_annual_leave_policy_fixed_term_rehire_assignment',
            trigger_dag_id=config.momentive_japan_user_sync_child_annual_leave_policy_fixed_term_rehire_assignment_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "userloginname": dag_run.conf['loginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": "add",
                "timeoffuri": rail.result('foreach_timeofftype_uri').get('uri'),
                "timeofftype": rail.result('foreach_timeofftype_uri').get('name'),
                "yoss" : dag_run.conf['continous_service_date']  if dag_run.conf['continous_service_date'] else None,
                "yos": dag_run.conf['continous_service_date']  if dag_run.conf['continous_service_date'] else None
            }
        )

        insert_childid_to_wait_list_4 = rail.SetVariableOperator(
            task_id='insert_childid_to_wait_list_4',
            name="{{result('create_child_trigger_list').name}}",
            append=True,
            value="{{result('trigger_annual_leave_policy_fixed_term_rehire_assignment')}}"
        )

        if_name_not_equals_1345678910_42 = rail.IfOperator(
            task_id='if_name_not_equals_1345678910_42',
            test=lambda: rail.result('foreach_timeofftype_uri').get('name') not in [
                '03. JPN_年次有給休暇 Annual Paid Leave (Fixed Term - Rehire after Retirement)',
                '01. JPN_年次有給休暇 Annual Paid Leave (Regular)',
                '04. JPN_年次有給休暇 Annual Paid Leave (Fixed Term - Part Time)',
                '05. JPN_年次有給休暇 Annual Paid Leave (Fixed Term - Standard)',
                '06. JPN_個別休日 - 誕生日休日 Shift Worker Holiday - Birthday Holiday',
                '07. JPN_個別休日 - 個別連休（上期） Shift Worker Holiday - Consecutive Holiday - 1st Half',
                '08. JPN_個別休日 - 個別連休（下期） Shift Worker Holiday - Consecutive Holiday - 2nd Half',
                '09. JPN_個別休日 - 個別休日 Shift Worker Holiday - Inconsecutive Holiday',
                '10. JPN_個別休暇 - 夏期 Shift Worker Special Vacation (Summer)'
            ],
            yes_task='get_defaul_timeoff_typepolicy_scheduleforuser',
            no_task='if_name_equals_678910_51'
        )

        get_defaul_timeoff_typepolicy_scheduleforuser = rail.RepliconServiceOperator(
            task_id='get_defaul_timeoff_typepolicy_scheduleforuser',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run :{
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeofftype_uri').get('uri')
                }
            }
        )

        if_default_policyset_present = rail.IfOperator(
            task_id='if_default_policyset_present',
            test=lambda:  bool(rail.result('get_defaul_timeoff_typepolicy_scheduleforuser') and (
                rail.result('get_defaul_timeoff_typepolicy_scheduleforuser')[0].get('effectiveDate') or {}).get('day')),
            yes_task='extract_default_policyset',
            no_task='if_name_equals_678910_51'
        )

        extract_default_policyset = rail.PythonOperator(
            task_id='extract_default_policyset',
            python_callable=lambda: python_callable.convert_policy_set_with_script_target(
                rail.result('get_defaul_timeoff_typepolicy_scheduleforuser')
            )
        )

        assign_default_timeoff_policy = rail.RepliconServiceOperator(
            task_id='assign_default_timeoff_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeofftype_uri').get('uri')
                },
                "policySetScheduleEntries": rail.result('extract_default_policyset')
            }
        )

        if_name_equals_678910_51 = rail.IfOperator(
            task_id='if_name_equals_678910_51',
            test=lambda: rail.result('foreach_timeofftype_uri').get('name') in [
                '06. JPN_個別休日 - 誕生日休日 Shift Worker Holiday - Birthday Holiday',
                '07. JPN_個別休日 - 個別連休（上期） Shift Worker Holiday - Consecutive Holiday - 1st Half',
                '08. JPN_個別休日 - 個別連休（下期） Shift Worker Holiday - Consecutive Holiday - 2nd Half',
                '09. JPN_個別休日 - 個別休日 Shift Worker Holiday - Inconsecutive Holiday',
                '10. JPN_個別休暇 - 夏期 Shift Worker Special Vacation (Summer)'
            ],
            yes_task='get_default_policy_for_shift_worker_holiday',
            no_task='foreach_timeofftype_uri_end'
        )

        get_default_policy_for_shift_worker_holiday = rail.RepliconServiceOperator(
            task_id='get_default_policy_for_shift_worker_holiday',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=lambda:{
                "timeOffTypeUri": rail.result('foreach_timeofftype_uri').get('uri')
            }
        )

        # Optimized: Extract and validate shift worker policy with offset check consolidated into one task
        # Replaces separate if_offset_value_present and extract tasks
        # Handles offsetValue==0 (current date) and offsetValue==1 (01/01 next year)
        extract_shift_worker_policyset = rail.PythonOperator(
            task_id='extract_shift_worker_policyset',
            python_callable=lambda dag_run: python_callable.build_shift_worker_policy_with_offset_check(
                rail.result('get_default_policy_for_shift_worker_holiday'),
                dag_run
            )
        )

        if_shift_worker_policy_entries_present = rail.IfOperator(
            task_id='if_shift_worker_policy_entries_present',
            test=lambda: bool(rail.result('extract_shift_worker_policyset')),
            yes_task='assign_shift_worker_holiday_policy',
            no_task='foreach_timeofftype_uri_end'
        )

        assign_shift_worker_holiday_policy = rail.RepliconServiceOperator(
            task_id='assign_shift_worker_holiday_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeofftype_uri').get('uri')
                },
                "policySetScheduleEntries": rail.result('extract_shift_worker_policyset')
            }
        )

        foreach_timeofftype_uri_end = rail.EmptyOperator(
            task_id='foreach_timeofftype_uri_end'
        )

        child_dag_ids = rail.PythonOperator(
            task_id='child_dag_ids',
            python_callable=lambda: [
                int(item) for item in rail.get_dag_run_var('childtriggeredlist')] if rail.get_dag_run_var('childtriggeredlist') else []
        )

        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('child_dag_ids') | to_json}}"
        )

        gather_responses_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_responses_from_child',
            dag_runs='{{ result("child_dag_ids") }}',
            dagrun_task_id='final_response_from_dag',
            execution_timeout=timedelta(
                hours=config.responses_from_child_timeout),
            flatten=True
        )

        filter_error_responses = rail.PythonOperator(
            task_id='filter_error_responses',
            python_callable=lambda: [item for item in rail.result(
                'gather_responses_from_child') if item]
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Add Timeoff for new user- Dag_Run Error - {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result('catch_and_log_error') if rail.result('catch_and_log_error') else (rail.result('filter_error_responses') or null)
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> create_child_trigger_list

        create_child_trigger_list >> if_workshift_eff_date_present >> rail.Label('Yes') >> split_workshift_eff_date >> get_enabled_timeoff_types
        if_workshift_eff_date_present >> rail.Label('No') >> get_enabled_timeoff_types

        get_enabled_timeoff_types >> extract_employment_month >> parse_timeoff_types_input >> create_matched_timeoff_uris >> \
            get_timeoff_accrual_prorated_value >> build_matched_types >> store_matched_types >> assign_timeoff_types_to_user >> foreach_timeofftype_uri
        
        foreach_timeofftype_uri >> if_timeoff_uris_present>> rail.Label('Yes') >> if_name_equals_annual_paid_leave_newhire >> rail.Label('Yes') >> foreach_timeofftype_uri_end
        if_timeoff_uris_present >> rail.Label('No') >> foreach_timeofftype_uri_end

        if_name_equals_annual_paid_leave_newhire >> rail.Label('No') >> if_continuos_service_date_present >> rail.Label('Yes') >> if_name_equals_annual_paid_leave_leave_fixed_term_standard 

        if_name_equals_annual_paid_leave_leave_fixed_term_standard >> rail.Label('Yes') >> trigger_annual_leave_policy_fixed_term_standard_parttime_assignment >> insert_childid_to_wait_list_1 >> if_name_equals_annual_paid_leave_leave_fixed_term_parttime
        if_name_equals_annual_paid_leave_leave_fixed_term_standard >> rail.Label('No') >> if_name_equals_annual_paid_leave_leave_fixed_term_parttime

        if_name_equals_annual_paid_leave_leave_fixed_term_parttime >> rail.Label('Yes') >> trigger_annual_leave_policy_fixed_term_parttime_assignment_add >> insert_childid_to_wait_list_2 >> if_name_equals_annual_paid_leave_leave_regular
        if_name_equals_annual_paid_leave_leave_fixed_term_parttime >> rail.Label('No') >> if_name_equals_annual_paid_leave_leave_regular

        if_name_equals_annual_paid_leave_leave_regular >> rail.Label('Yes') >> trigger_annual_leave_policy_regular_assignment_add >> insert_childid_to_wait_list_3 >> if_name_equals_annual_paid_leave_leave_fixed_term_rehireafterretirement
        if_name_equals_annual_paid_leave_leave_regular >> rail.Label('No') >> if_name_equals_annual_paid_leave_leave_fixed_term_rehireafterretirement

        if_name_equals_annual_paid_leave_leave_fixed_term_rehireafterretirement >> rail.Label('Yes') >> trigger_annual_leave_policy_fixed_term_rehire_assignment >> insert_childid_to_wait_list_4 >> if_name_not_equals_1345678910_42
        if_name_equals_annual_paid_leave_leave_fixed_term_rehireafterretirement >> rail.Label('No') >> if_name_not_equals_1345678910_42

        if_continuos_service_date_present >> rail.Label('No') >> if_name_not_equals_1345678910_42 

        if_name_not_equals_1345678910_42 >> rail.Label('Yes') >> get_defaul_timeoff_typepolicy_scheduleforuser >> if_default_policyset_present >> rail.Label('Yes') >> extract_default_policyset >> assign_default_timeoff_policy >> if_name_equals_678910_51
        if_default_policyset_present >> rail.Label('No') >> if_name_equals_678910_51
        if_name_not_equals_1345678910_42 >> rail.Label('No') >> if_name_equals_678910_51

        if_name_equals_678910_51 >> rail.Label('Yes') >> get_default_policy_for_shift_worker_holiday >> extract_shift_worker_policyset >> if_shift_worker_policy_entries_present
        if_shift_worker_policy_entries_present >> rail.Label('Yes') >> assign_shift_worker_holiday_policy >> foreach_timeofftype_uri_end
        if_shift_worker_policy_entries_present >> rail.Label('No') >> foreach_timeofftype_uri_end
        if_name_equals_678910_51 >> rail.Label('No') >> foreach_timeofftype_uri_end
        
        foreach_timeofftype_uri >> foreach_timeofftype_uri_end >> child_dag_ids >> wait_for_child_dags >> gather_responses_from_child >> filter_error_responses\
            >> catch_and_log_error >> final_response_from_dag

        return dag
    
rail.for_each_instance(create_dag)
        