from datetime import timedelta
import json
from airflow.models import Variable
import rail
from adtalem.user_import.utils import python_callable_method
from adtalem.user_import.utils import response_filter


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


# pylint: disable=too-many-statements
def create_timeoff_updateuser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_child_timeoff_update_user_crv4.0_{config.instance}',
        description=f'Adtalem Update User-Time Off CRV4.0 {config.instance}',
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
            no_task='mapper_lookup'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='mapper_lookup',
            end_task='dagrun_log_to_sumo'
        )

        mapper_lookup = rail.PythonOperator(
            task_id='mapper_lookup',
            python_callable=python_callable_method.get_mapper_lookup
        )

        get_mapper_entries = rail.PythonOperator(
            task_id='get_mapper_entries',
            python_callable=python_callable_method.get_mapper_entries_from_adtalem_mapperfile,
            op_args=["{{ result('mapper_lookup') }}"]
        )

        get_timeofftypes_from_mapper = rail.PythonOperator(
            task_id='get_timeofftypes_from_mapper',
            python_callable=python_callable_method.get_mapper_entry_value,
            op_args=['Time Off Types']
        )

        get_assigned_timeofftypes = rail.RepliconServiceOperator(
            task_id='get_assigned_timeofftypes',
            endpoint="/services/TimeOffService1.svc/BulkGetTimeOffTypeAssignmentsForUsers",
            data={
                "userUris": [
                    "{{ dag_run.conf.useruri }}"
                ]
            }
        )

        get_alltimeoff_types = rail.RepliconServiceOperator(
            task_id='get_alltimeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes"
        )

        is_mapper_timeoff_types_present = rail.IfOperator(
            task_id='is_mapper_timeoff_types_present',
            test="{{ result('get_timeofftypes_from_mapper') | is_truthy }}",
            yes_task="get_timeofftype_uris_to_assign",
            no_task="dagrun_log_to_sumo",
        )

        def get_timeofftype_uris_update(dag_run):
            add_timeoff_typeuris = python_callable_method.get_timeofftype_uris()
            if dag_run.conf['ususer'] == 'yes':
                add_timeoff_typeuris.append(rail.find_first_by_attr_and_get_attr(
                    rail.result('get_alltimeoff_types'), 'displayText', 'PTO Buy Up', 'uri', ''))
            rail.set_result('add_timeoff_typeuris', add_timeoff_typeuris)
            return add_timeoff_typeuris
        get_timeofftype_uris_to_assign = rail.PythonOperator(
            task_id='get_timeofftype_uris_to_assign',
            python_callable=get_timeofftype_uris_update
        )

        is_timeofftype_uris_to_assign = rail.IfOperator(
            task_id='is_timeofftype_uris_to_assign',
            test="{{ result('get_timeofftype_uris_to_assign') | length > 0 }}",
            yes_task="assign_required_timeofftypes",
            no_task="get_timeoff_previously_not_newly_assigned",
        )

        assign_required_timeofftypes = rail.RepliconServiceOperator(
            task_id='assign_required_timeofftypes',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('get_timeofftype_uris_to_assign')
            }
        )

        get_timeoff_previously_not_newly_assigned = rail.PythonOperator(
            task_id='get_timeoff_previously_not_newly_assigned',
            python_callable=python_callable_method.get_timeoffs_not_in_newset,
            op_args=['add_timeoff_typeuris']
        )

        is_timeoff_previously_not_newly_assigned_present = rail.IfOperator(
            task_id='is_timeoff_previously_not_newly_assigned_present',
            test="{{ result('get_timeoff_previously_not_newly_assigned') | length > 0 }}",
            yes_task="trigger_put_0_balance_production_crv20",
            no_task="if_request_ususer_equals_to_no_61",
        )

        trigger_put_0_balance_production_crv20 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_put_0_balance_production_crv20',
            retries=0,
            items=lambda: rail.result(
                'get_timeoff_previously_not_newly_assigned'),
            trigger_dag_id=f'adtalem_userimport_put_0_balance_crv2.0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "timeoffuri": "{{ item }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "terminationdate": "{{ current_time('%m/%d/%Y') }}",
            }
        )

        if_request_ususer_equals_to_no_61 = rail.IfOperator(
            task_id='if_request_ususer_equals_to_no_61',
            test="{{ dag_run.conf.ususer == 'no' }}",
            yes_task="if_request_paygroup_equals_to_acaau_62",
            no_task="if_request_ususer_equals_to_yes_88",
        )

        if_request_paygroup_equals_to_acaau_62 = rail.IfOperator(
            task_id='if_request_paygroup_equals_to_acaau_62',
            test=lambda dag_run: dag_run.conf['paygroup'] in (
                'ACAAU', 'ACADE', 'ACAFR', 'ACAUK'),
            yes_task="log_checkif_p_t_oisassigned_63",
            no_task="if_request_ususer_equals_to_yes_88",
        )

        def get_pto_assigned():
            paid_timeoffs = [x for x in rail.result(
                'get_timeofftype_uris_to_assign', 'required_timeofftypes') if x == 'Paid Time Off']
            return paid_timeoffs[0] if paid_timeoffs else ''
        log_checkif_p_t_oisassigned_63 = rail.PythonOperator(
            task_id='log_checkif_p_t_oisassigned_63',
            python_callable=get_pto_assigned
        )

        if_log_checkif_p_t_oisassigned_63_present_64 = rail.IfOperator(
            task_id='if_log_checkif_p_t_oisassigned_63_present_64',
            test="{{ result('log_checkif_p_t_oisassigned_63') | is_truthy }}",
            yes_task="log_time_offurifor_paid_time_off_65",
            no_task="get_requiredpolicy_fortimeoff_type_paidtimeoff",
        )

        log_time_offurifor_paid_time_off_65 = rail.PythonOperator(
            task_id='log_time_offurifor_paid_time_off_65',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_alltimeoff_types'), 'displayText', "Paid Time Off", 'uri', '')
        )

        getdefaultpolicyfor_p_t_o_66 = rail.RepliconServiceOperator(
            task_id='getdefaultpolicyfor_p_t_o_66',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('log_time_offurifor_paid_time_off_65') }}"
            }
        )

        log_policysetmodified_67 = rail.PythonOperator(
            task_id='log_policysetmodified_67',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('getdefaultpolicyfor_p_t_o_66')['policySet'], ensure_ascii=False).replace(
                        '"script"', '"scriptTarget"'))
        )

        get_requiredpolicy_fortimeoff_type_paidtimeoff = rail.RepliconServiceOperator(
            task_id='get_requiredpolicy_fortimeoff_type_paidtimeoff',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=response_filter.get_assigned_timeoff_policy_update_paidtimeoff_v40
        )

        assign_sick_timeoff_policy_paidtimeoff = rail.RepliconServiceOperator(
            task_id='assign_sick_timeoff_policy_paidtimeoff',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: rail.result(
                'get_requiredpolicy_fortimeoff_type_paidtimeoff')
        )

        if_request_ususer_equals_to_yes_88 = rail.IfOperator(
            task_id='if_request_ususer_equals_to_yes_88',
            test="{{ dag_run.conf.ususer == 'yes' }}",
            yes_task="if_log_final_namefor_sick_leave_37_present_for_existing_employees_sick_policy_assignment_89",
            no_task="dagrun_log_to_sumo",
        )

        if_log_final_namefor_sick_leave_37_present_for_existing_employees_sick_policy_assignment_89 = rail.IfOperator(
            task_id='if_log_final_namefor_sick_leave_37_present_for_existing_employees_sick_policy_assignment_89',
            test="{{ result('get_timeofftype_uris_to_assign', 'sick_timeoff_name') | is_truthy \
                and dag_run.conf.userstatus == 'Enabled' }}",
            yes_task="get_policy_schedule_existingusers",
            no_task="if_log_final_namefor_sick_leave_37_present_for_rehire_employees_sick_policy_assignment_132",
        )

        get_policy_schedule_existingusers = rail.PythonOperator(
            task_id='get_policy_schedule_existingusers',
            python_callable=python_callable_method.get_policy_schedule_entries,
            op_args=[config.adtalem_sicktime_policies_existing_users_mapper_old]
        )

        get_requiredpolicy_fortimeoff_type_existingusers = rail.RepliconServiceOperator(
            task_id='get_requiredpolicy_fortimeoff_type_existingusers',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=response_filter.get_assigned_timeoff_policy_update_v40_existingusers
        )

        assign_sick_timeoff_policy_existingusers = rail.RepliconServiceOperator(
            task_id='assign_sick_timeoff_policy_existingusers',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: rail.result(
                'get_requiredpolicy_fortimeoff_type_existingusers')
        )

        if_log_final_namefor_sick_leave_37_present_for_rehire_employees_sick_policy_assignment_132 = rail.IfOperator(
            task_id='if_log_final_namefor_sick_leave_37_present_for_rehire_employees_sick_policy_assignment_132',
            test="{{ result('get_timeofftype_uris_to_assign', 'sick_timeoff_name') | is_truthy and \
                dag_run.conf.userstatus == 'Disabled' }}",
            yes_task="get_policy_schedule_rehireusers",
            no_task="check_if_vacationtimeoff_to_assign"
        )

        get_policy_schedule_rehireusers = rail.PythonOperator(
            task_id='get_policy_schedule_rehireusers',
            python_callable=python_callable_method.get_policy_schedule_entries,
            op_args=[config.adtalem_sicktime_timeoffpolicy_schedule_mapper_old]
        )

        get_requiredpolicy_fortimeoff_type_rehireusers = rail.RepliconServiceOperator(
            task_id='get_requiredpolicy_fortimeoff_type_rehireusers',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=response_filter.get_assigned_timeoff_policy_update_v40_rehireusers
        )

        assign_sick_timeoff_policy_rehireusers = rail.RepliconServiceOperator(
            task_id='assign_sick_timeoff_policy_rehireusers',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: rail.result(
                'get_requiredpolicy_fortimeoff_type_rehireusers')
        )

        check_if_vacationtimeoff_to_assign = rail.PythonOperator(
            task_id='check_if_vacationtimeoff_to_assign',
            python_callable=python_callable_method.get_vacationtimeoff_to_assign
        )

        is_vacationtimeoff_present = rail.IfOperator(
            task_id='is_vacationtimeoff_present',
            test="{{ result('check_if_vacationtimeoff_to_assign') | sn | is_truthy }}",
            yes_task="get_previous_policies",
            no_task="dagrun_log_to_sumo",
        )

        get_previous_policies = rail.RepliconServiceOperator(
            task_id='get_previous_policies',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=response_filter.get_previous_vacationtimeoff_policies
        )

        get_required_timeoff_jobcode_mapper = rail.PythonOperator(
            task_id='get_required_timeoff_jobcode_mapper',
            python_callable=python_callable_method.get_jobcode_timeoff_jobcode_mapper,
            op_args=['{{ dag_run.conf.jobcode }}']
        )

        final_policy_mapper_vacation = rail.PythonOperator(
            task_id='final_policy_mapper_vacation',
            python_callable=python_callable_method.get_final_policy_mapper
        )

        is_final_policy_mapper_vacation_present = rail.IfOperator(
            task_id='is_final_policy_mapper_vacation_present',
            test="{{ result('final_policy_mapper_vacation') | is_truthy }}",
            yes_task="trigger_vacation_policy_newuser",
            no_task="dagrun_log_to_sumo",
        )

        trigger_vacation_policy_newuser = rail.TriggerDagRunOperator(
            task_id='trigger_vacation_policy_newuser',
            retries=0,
            trigger_dag_id=f'adtalem_userimport_child_assign_vacation_policy_rehire_users_crv2.0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "policyname": "{{ result('final_policy_mapper_vacation') }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "servicedate": "{{ dag_run.conf.servicedate }}",
                "previouspolicy": "{{ result('get_previous_policies') }}",
                "status": "{{ dag_run.conf.userstatus }}",
                "rehiredate": "{{ dag_run.conf.rehiredate }}"
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> mapper_lookup

        mapper_lookup >> get_mapper_entries >> get_timeofftypes_from_mapper >> get_assigned_timeofftypes >> \
            get_alltimeoff_types >> is_mapper_timeoff_types_present
        is_mapper_timeoff_types_present >> rail.Label(
            'Yes') >> get_timeofftype_uris_to_assign >> is_timeofftype_uris_to_assign
        is_timeofftype_uris_to_assign >> rail.Label(
            'Yes') >> assign_required_timeofftypes >> get_timeoff_previously_not_newly_assigned
        is_timeofftype_uris_to_assign >> rail.Label(
            'No') >> get_timeoff_previously_not_newly_assigned

        get_timeoff_previously_not_newly_assigned >> is_timeoff_previously_not_newly_assigned_present

        is_timeoff_previously_not_newly_assigned_present >> rail.Label(
            'Yes') >> trigger_put_0_balance_production_crv20 >> if_request_ususer_equals_to_no_61
        is_timeoff_previously_not_newly_assigned_present >> rail.Label(
            'No') >> if_request_ususer_equals_to_no_61
        if_request_ususer_equals_to_no_61 >> rail.Label(
            'Yes') >> if_request_paygroup_equals_to_acaau_62
        if_request_paygroup_equals_to_acaau_62 >> rail.Label(
            'Yes') >> log_checkif_p_t_oisassigned_63 >> if_log_checkif_p_t_oisassigned_63_present_64
        if_log_checkif_p_t_oisassigned_63_present_64 >> rail.Label(
            'Yes') >> log_time_offurifor_paid_time_off_65 >> getdefaultpolicyfor_p_t_o_66 >> \
            log_policysetmodified_67 >> get_requiredpolicy_fortimeoff_type_paidtimeoff >> \
            assign_sick_timeoff_policy_paidtimeoff
        if_log_checkif_p_t_oisassigned_63_present_64 >> rail.Label(
            'No') >> get_requiredpolicy_fortimeoff_type_paidtimeoff
        get_requiredpolicy_fortimeoff_type_paidtimeoff >> assign_sick_timeoff_policy_paidtimeoff >> if_request_ususer_equals_to_yes_88
        if_request_paygroup_equals_to_acaau_62 >> rail.Label(
            'No') >> if_request_ususer_equals_to_yes_88
        if_request_ususer_equals_to_no_61 >> rail.Label(
            'No') >> if_request_ususer_equals_to_yes_88
        if_request_ususer_equals_to_yes_88 >> rail.Label(
            'Yes') >> if_log_final_namefor_sick_leave_37_present_for_existing_employees_sick_policy_assignment_89
        if_log_final_namefor_sick_leave_37_present_for_existing_employees_sick_policy_assignment_89 >> rail.Label(
            'Yes') >> get_policy_schedule_existingusers >> get_requiredpolicy_fortimeoff_type_existingusers >> \
            assign_sick_timeoff_policy_existingusers >> if_log_final_namefor_sick_leave_37_present_for_rehire_employees_sick_policy_assignment_132
        if_log_final_namefor_sick_leave_37_present_for_existing_employees_sick_policy_assignment_89 >> rail.Label(
            'No') >> if_log_final_namefor_sick_leave_37_present_for_rehire_employees_sick_policy_assignment_132
        if_log_final_namefor_sick_leave_37_present_for_rehire_employees_sick_policy_assignment_132 >> rail.Label(
            'Yes') >> get_policy_schedule_rehireusers >> get_requiredpolicy_fortimeoff_type_rehireusers >> \
            assign_sick_timeoff_policy_rehireusers >> check_if_vacationtimeoff_to_assign
        if_log_final_namefor_sick_leave_37_present_for_rehire_employees_sick_policy_assignment_132 >> rail.Label(
            'No') >> check_if_vacationtimeoff_to_assign

        check_if_vacationtimeoff_to_assign >> is_vacationtimeoff_present

        is_vacationtimeoff_present >> rail.Label(
            'Yes') >> get_previous_policies >> get_required_timeoff_jobcode_mapper >> \
            final_policy_mapper_vacation >> is_final_policy_mapper_vacation_present

        is_final_policy_mapper_vacation_present >> rail.Label(
            'Yes') >> trigger_vacation_policy_newuser >> dagrun_log_to_sumo
        is_final_policy_mapper_vacation_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

        is_vacationtimeoff_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

        if_request_ususer_equals_to_yes_88 >> rail.Label(
            'No') >> dagrun_log_to_sumo
        is_mapper_timeoff_types_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_timeoff_updateuser_child_dag)
