from datetime import timedelta
import json
from airflow.models import Variable
import rail
from adtalem.user_import.utils import python_callable_method
from adtalem.user_import.utils.request_payload import get_assign_pto_policy
from adtalem.user_import.utils.response_filter import map_impersonate_and_create_interactive_session


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


# pylint: disable=too-many-statements
def create_timeoff_adduser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_child_timeoff_add_new_user_cr2021_v1_{config.instance}',
        description=f'Adtalem Userimport child Timeoff_add_new_user_CR2021_V1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_mapper_lookup'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_mapper_lookup',
            end_task='dagrun_log_to_sumo',
        )

        def get_mapper_lookup_value():
            dag_run_conf = rail.get_current_context()['dag_run'].conf
            if dag_run_conf['ususer'] != 'yes':
                if dag_run_conf['paygroup'] in ('ACATW', 'ACASG', 'ACAJP', 'HK', 'ACACH', 'ACAIN', 'ACAAS'):
                    return dag_run_conf['mapperlookup']
                if dag_run_conf['regulartemp'] == 'R' and dag_run_conf['fullparttime'] == 'F':
                    return f"{dag_run_conf['mapperlookup']}/RF"
            return dag_run_conf['mapperlookup']
        get_mapper_lookup = rail.PythonOperator(
            task_id='get_mapper_lookup',
            python_callable=get_mapper_lookup_value
        )

        get_alltimeoff_types = rail.RepliconServiceOperator(
            task_id='get_alltimeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        get_flex_to_be_assigned = rail.PythonOperator(
            task_id='get_flex_to_be_assigned',
            python_callable=python_callable_method.get_flex_to_be_assigned,
            op_args=['{{ dag_run.conf.paygrade }}',
                     '{{ dag_run.conf.paygroup }}', '{{ dag_run.conf.salaryhourly }}']
        )

        is_flextobeassigned_yes = rail.IfOperator(
            task_id='is_flextobeassigned_yes',
            test="{{ result('get_flex_to_be_assigned') == 'yes' }}",
            yes_task="add_timeoffs",
            no_task="get_mapper_entries",
        )

        add_timeoffs = rail.PythonOperator(
            task_id='add_timeoffs',
            python_callable=python_callable_method.add_timeoffs
        )

        get_mapper_entries = rail.PythonOperator(
            task_id='get_mapper_entries',
            python_callable=python_callable_method.get_mapper_entries_from_adtalem_mapperfile,
            op_args=["{{ result('get_mapper_lookup') }}", 'new']
        )

        get_timeofftypes_from_mapper = rail.PythonOperator(
            task_id='get_timeofftypes_from_mapper',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Time Off Types']
        )

        is_timeofftypes_present = rail.IfOperator(
            task_id='is_timeofftypes_present',
            test="{{ result('get_timeofftypes_from_mapper') | is_truthy }}",
            yes_task="add_timeoffs",
            no_task="dagrun_log_to_sumo",
        )

        assign_required_timeofftypes = rail.RepliconServiceOperator(
            task_id='assign_required_timeofftypes',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": [x['uri'] for x in rail.result('add_timeoffs')]
            }
        )

        is_fto_timeofftype = rail.IfOperator(
            task_id='is_fto_timeofftype',
            test="{{ result('add_timeoffs') | find_first_by_attr_and_get_attr('name', 'FTO', 'uri', '') | \
                is_truthy }}",
            yes_task="put_default_timeoffbookings_fto",
            no_task="is_not_ususer",
        )

        put_default_timeoffbookings_fto = rail.RepliconServiceOperator(
            task_id='put_default_timeoffbookings_fto',
            endpoint="/services/TimeOffService1.svc/PutDefaultTimeOffTypeForBookingsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeOffTypeUri": "{{ result('add_timeoffs') | \
                    find_first_by_attr_and_get_attr('name', 'FTO', 'uri', '') }}"
            }
        )

        is_not_ususer = rail.IfOperator(
            task_id='is_not_ususer',
            test="{{ dag_run.conf.ususer == 'no' }}",
            yes_task="is_paygroup_equals_to",
            no_task="process_each_timeofftype"
        )

        is_paygroup_equals_to = rail.IfOperator(
            task_id='is_paygroup_equals_to',
            test=lambda dag_run: dag_run.conf['paygroup'] in (
                'ACAUK', 'ACAFR', 'ACADE', 'ACAAU'),
            yes_task="get_pto_timeofftype_uri",
            no_task="process_each_timeofftype"
        )

        get_pto_timeofftype_uri = rail.PythonOperator(
            task_id='get_pto_timeofftype_uri',
            python_callable=python_callable_method.get_ptotimeofftype_uri
        )

        get_pto_policyset = rail.RepliconServiceOperator(
            task_id='get_pto_policyset',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('get_pto_timeofftype_uri') }}"
            },
            data_handler=lambda response: json.loads(json.dumps(
                response[0]['policySet'], ensure_ascii=False).replace('"script"', '"scriptTarget"'))
        )

        assign_pto_timeoffpolicy = rail.RepliconServiceOperator(
            task_id='assign_pto_timeoffpolicy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_assign_pto_policy
        )

        process_each_timeofftype = rail.EmptyOperator(
            task_id='process_each_timeofftype'
        )

        foreach_timeofftype_uri = rail.ForEachOperator(
            task_id='foreach_timeofftype_uri',
            items=lambda: rail.result('add_timeoffs'),
            start_task='is_sicktimeoff',
            end_task='foreach_timeofftype_uri_end'
        )

        is_sicktimeoff = rail.IfOperator(
            task_id='is_sicktimeoff',
            test="{{ result('foreach_timeofftype_uri').name == 'Sick Time Off - ACACH' \
                or result('foreach_timeofftype_uri').name == 'Sick Time Off - ACAJP' \
                    or result('foreach_timeofftype_uri').name == 'Sick Time Off - HK' \
                        or result('foreach_timeofftype_uri').name == 'Sick Time Off - ACASG' \
                            or result('foreach_timeofftype_uri').name == 'Sick Time Off - ACATW' \
                                or result('foreach_timeofftype_uri').name == 'Sick Time Off - ACAIN' \
                                    or result('foreach_timeofftype_uri').name == 'Leave of Absence - Australia' }}",
            yes_task="get_default_time_off_type_policy_schedule_for_user",
            no_task="is_timeofftype_name_anniversary_day",
        )

        get_default_time_off_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_timeofftype_uri').uri }}"
                }
            }
        )

        if_effectivedate_day_present_59 = rail.IfOperator(
            task_id='if_effectivedate_day_present_59',
            test="{{ result('get_default_time_off_type_policy_schedule_for_user') | first_or_default | \
                attr_or_default('effectiveDate') | attr_or_default('day') | is_truthy }}",
            yes_task="get_sickpolicy_to_assign",
            no_task="is_timeofftype_name_anniversary_day",
        )

        get_sickpolicy_to_assign = rail.PythonOperator(
            task_id='get_sickpolicy_to_assign',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user'), ensure_ascii=False).replace('null', '"effective"').replace(
                        '"script"', '"scriptTarget"'))
        )

        put_user_time_off_account_policy_set_schedule_61 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_61',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeofftype_uri')['uri']
                },
                "policySetScheduleEntries": rail.result('get_sickpolicy_to_assign')
            }
        )

        is_timeofftype_name_anniversary_day = rail.IfOperator(
            task_id='is_timeofftype_name_anniversary_day',
            test="{{ result('foreach_timeofftype_uri').name == 'Anniversary Day' }}",
            yes_task="trigger_dag_run_adtalem_userimport_timeoff_anniversarydaypolicyaddupdate_cr14",
            no_task="if_timeoff_name_equals_annualleaveasia",
        )

        trigger_dag_run_adtalem_userimport_timeoff_anniversarydaypolicyaddupdate_cr14 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_adtalem_userimport_timeoff_anniversarydaypolicyaddupdate_cr14',
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
                "type": "Add",
                "timeoffuri": "{{ result('foreach_timeofftype_uri').uri }}"
            }
        )

        if_timeoff_name_equals_annualleaveasia = rail.IfOperator(
            task_id='if_timeoff_name_equals_annualleaveasia',
            test="{{ result('foreach_timeofftype_uri').name == 'Annual Leave(Asia)' or \
                result('foreach_timeofftype_uri').name == 'Annual Leave(Australia)' \
                    or result('foreach_timeofftype_uri').name == 'Annual Leave(India)' }}",
            yes_task="trigger_dag_run_live_timeoff_annual_leave_day_aus_asia_policy_add_update_cr2021_v1",
            no_task="is_timeoff_name_floatingholidays",
        )

        trigger_dag_run_live_timeoff_annual_leave_day_aus_asia_policy_add_update_cr2021_v1 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_timeoff_annual_leave_day_aus_asia_policy_add_update_cr2021_v1',
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
                "type": "Add",
                "timeoffuri": "{{ result('foreach_timeofftype_uri').uri }}",
                "timeofftype": "{{ result('foreach_timeofftype_uri').name }}"
            }
        )

        is_timeoff_name_floatingholidays = rail.IfOperator(
            task_id='is_timeoff_name_floatingholidays',
            test="{{ result('foreach_timeofftype_uri').name == 'Floating Holidays' }}",
            yes_task="get_default_time_off_type_policy_schedule_for_user_floatingholiday",
            no_task="foreach_timeofftype_uri_end",
        )

        get_default_time_off_type_policy_schedule_for_user_floatingholiday = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_floatingholiday',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_timeofftype_uri').uri }}"
                }
            }
        )

        is_floating_holiday_policy_present = rail.IfOperator(
            task_id='is_floating_holiday_policy_present',
            test="{{ result('get_default_time_off_type_policy_schedule_for_user_floatingholiday') | first_or_default | \
                attr_or_default('effectiveDate') | attr_or_default('day') | is_truthy }}",
            yes_task="is_fullparttime_equals_f",
            no_task="foreach_timeofftype_uri_end",
        )

        is_fullparttime_equals_f = rail.IfOperator(
            task_id='is_fullparttime_equals_f',
            test="{{ dag_run.conf.fullparttime == 'F' }}",
            yes_task="log_floatingholiday_policy",
            no_task="get_yearlyaccrual_policyset",
        )

        log_floatingholiday_policy = rail.PythonOperator(
            task_id='log_floatingholiday_policy',
            python_callable=lambda: json.loads(json.dumps(
                rail.result('get_default_time_off_type_policy_schedule_for_user_floatingholiday'), ensure_ascii=False).replace(
                    'null', '"effective"').replace('"script"', '"scriptTarget"'))
        )

        put_user_time_off_account_policy_set_schedule_76 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_76',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeofftype_uri')['uri']
                },
                "policySetScheduleEntries": rail.result('log_floatingholiday_policy')
            }
        )

        get_yearlyaccrual_policyset = rail.PythonOperator(
            task_id='get_yearlyaccrual_policyset',
            python_callable=python_callable_method.get_yearlyaccrual_policyset
        )

        put_user_time_off_account_policy_set_schedule_94 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_94',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeofftype_uri')['uri']
                },
                "policySetScheduleEntries": rail.result('get_yearlyaccrual_policyset')
            }
        )

        foreach_timeofftype_uri_end = rail.EmptyOperator(
            task_id='foreach_timeofftype_uri_end'
        )

        is_ptorft_timeofftype = rail.IfOperator(
            task_id='is_ptorft_timeofftype',
            test=lambda: any(x in ['PTO (RFT)', 'PTO (RPT)'] for x in rail.result(
                'add_timeoffs', 'required_timeofftypes')) if rail.result(
                'add_timeoffs', 'required_timeofftypes') else False,
            yes_task="put_default_time_off_type_for_bookings_for_user_pto",
            no_task="is_assign_pto_timeoffpolicy_present",
        )

        put_default_time_off_type_for_bookings_for_user_pto = rail.RepliconServiceOperator(
            task_id='put_default_time_off_type_for_bookings_for_user_pto',
            endpoint="/services/TimeOffService1.svc/PutDefaultTimeOffTypeForBookingsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('add_timeoffs'), 'name', 'PTO (RFT)', 'uri', '') or rail.find_first_by_attr_and_get_attr(
                        rail.result('add_timeoffs'), 'name', 'PTO (RPT)', 'uri', '')
            }
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
            yes_task="trigger_dag_run_live_pto_policy_assignment_new_user_2021",
            no_task="is_assign_pto_timeoffpolicy_present",
        )

        trigger_dag_run_live_pto_policy_assignment_new_user_2021 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_pto_policy_assignment_new_user_2021',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_ptopolicyassignmentnewuser_2021_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "policyname": rail.result('final_policy_mapper_vacation'),
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.find_first_by_attr_and_get_attr(
                    rail.result('add_timeoffs'), 'name', 'PTO (RFT)', 'uri', '') or rail.find_first_by_attr_and_get_attr(
                        rail.result('add_timeoffs'), 'name', 'PTO (RPT)', 'uri', ''),
                "rehiredate": dag_run.conf['rehiredate']
            }
        )

        is_assign_pto_timeoffpolicy_present = rail.IfOperator(
            task_id='is_assign_pto_timeoffpolicy_present',
            test="{{ result('assign_pto_timeoffpolicy') | sn | is_truthy }}",
            yes_task="impersonate_and_create_interactive_session",
            no_task="dagrun_log_to_sumo",
        )

        impersonate_and_create_interactive_session = rail.RepliconServiceOperator(
            task_id='impersonate_and_create_interactive_session',
            endpoint="/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession",
            data={
                "impersonatedUserUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=map_impersonate_and_create_interactive_session
        )

        update_my_default_time_off_type_for_bookings = rail.RepliconServiceOperator(
            task_id='update_my_default_time_off_type_for_bookings',
            endpoint="/services/LegacyUIService1.svc/UpdateMyDefaultTimeOffTypeForBookings",
            data={
                "timeOffTypeUri": "{{ result('get_pto_timeofftype_uri') }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session')
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_mapper_lookup

        get_mapper_lookup >> get_alltimeoff_types >> get_flex_to_be_assigned >> is_flextobeassigned_yes

        is_flextobeassigned_yes >> rail.Label(
            'Yes') >> add_timeoffs
        is_flextobeassigned_yes >> rail.Label(
            'No') >> get_mapper_entries >> get_timeofftypes_from_mapper >> is_timeofftypes_present
        is_timeofftypes_present >> rail.Label(
            'Yes') >> add_timeoffs
        is_timeofftypes_present >> rail.Label(
            'No') >> dagrun_log_to_sumo
        add_timeoffs >> assign_required_timeofftypes >> is_fto_timeofftype
        is_fto_timeofftype >> rail.Label(
            'Yes') >> put_default_timeoffbookings_fto >> is_not_ususer
        is_fto_timeofftype >> rail.Label(
            'No') >> is_not_ususer
        is_not_ususer >> rail.Label(
            'Yes') >> is_paygroup_equals_to
        is_paygroup_equals_to >> rail.Label(
            'Yes') >> get_pto_timeofftype_uri >> get_pto_policyset >> assign_pto_timeoffpolicy >> process_each_timeofftype
        is_paygroup_equals_to >> rail.Label(
            'No') >> process_each_timeofftype
        is_not_ususer >> rail.Label(
            'No') >> process_each_timeofftype
        process_each_timeofftype >> foreach_timeofftype_uri >> is_sicktimeoff
        is_sicktimeoff >> rail.Label(
            'Yes') >> get_default_time_off_type_policy_schedule_for_user >> if_effectivedate_day_present_59
        if_effectivedate_day_present_59 >> rail.Label(
            'Yes') >> get_sickpolicy_to_assign >> put_user_time_off_account_policy_set_schedule_61 >> is_timeofftype_name_anniversary_day
        if_effectivedate_day_present_59 >> rail.Label(
            'No') >> is_timeofftype_name_anniversary_day
        is_sicktimeoff >> rail.Label(
            'No') >> is_timeofftype_name_anniversary_day
        is_timeofftype_name_anniversary_day >> rail.Label(
            'Yes') >> trigger_dag_run_adtalem_userimport_timeoff_anniversarydaypolicyaddupdate_cr14 >> \
            foreach_timeofftype_uri_end
        is_timeofftype_name_anniversary_day >> rail.Label(
            'No') >> if_timeoff_name_equals_annualleaveasia
        if_timeoff_name_equals_annualleaveasia >> rail.Label(
            'Yes') >> trigger_dag_run_live_timeoff_annual_leave_day_aus_asia_policy_add_update_cr2021_v1 >> \
            foreach_timeofftype_uri_end
        if_timeoff_name_equals_annualleaveasia >> rail.Label(
            'No') >> is_timeoff_name_floatingholidays
        is_timeoff_name_floatingholidays >> rail.Label(
            'Yes') >> get_default_time_off_type_policy_schedule_for_user_floatingholiday >> is_floating_holiday_policy_present
        is_floating_holiday_policy_present >> rail.Label(
            'Yes') >> is_fullparttime_equals_f
        is_fullparttime_equals_f >> rail.Label(
            'Yes') >> log_floatingholiday_policy >> put_user_time_off_account_policy_set_schedule_76 >> \
            foreach_timeofftype_uri_end
        is_fullparttime_equals_f >> rail.Label(
            'No') >> get_yearlyaccrual_policyset >> put_user_time_off_account_policy_set_schedule_94 >> \
            foreach_timeofftype_uri_end
        is_floating_holiday_policy_present >> rail.Label(
            'No') >> foreach_timeofftype_uri_end
        is_timeoff_name_floatingholidays >> rail.Label(
            'No') >> foreach_timeofftype_uri_end
        foreach_timeofftype_uri_end >> is_ptorft_timeofftype
        is_ptorft_timeofftype >> rail.Label(
            'Yes') >> put_default_time_off_type_for_bookings_for_user_pto >> get_required_timeoff_jobcode_mapper >> \
            final_policy_mapper_vacation >> is_final_policy_mapper_vacation
        is_final_policy_mapper_vacation >> rail.Label(
            'Yes') >> trigger_dag_run_live_pto_policy_assignment_new_user_2021 >> \
            is_assign_pto_timeoffpolicy_present
        is_final_policy_mapper_vacation >> rail.Label(
            'No') >> is_assign_pto_timeoffpolicy_present
        is_ptorft_timeofftype >> rail.Label(
            'No') >> is_assign_pto_timeoffpolicy_present
        is_assign_pto_timeoffpolicy_present >> rail.Label(
            'Yes') >> impersonate_and_create_interactive_session >> update_my_default_time_off_type_for_bookings >> \
            dagrun_log_to_sumo
        is_assign_pto_timeoffpolicy_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_timeoff_adduser_child_dag)
