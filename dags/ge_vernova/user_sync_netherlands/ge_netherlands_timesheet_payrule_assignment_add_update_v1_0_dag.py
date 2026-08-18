
from datetime import timedelta, datetime
import pendulum
from ge.user_sync_netherlands.netherlands_master_mapper import netherlands_master_mapper
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_user_sync_netherlands_payrule_assignment_add_update_v1_0_{config.instance}',
        description=f'GE Netherlands Timesheet/Payrule assignment Add/Update V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
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
            no_task='declare_list_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_3',
            end_task='catch_100_100_100',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_3 = rail.SetVariableOperator(
            task_id='declare_list_3',
            append=False,
            name='exception logger',
            value=[]
        )

        declare_list_4 = rail.SetVariableOperator(
            task_id='declare_list_4',
            append=False,
            name='logs',
            value=[]
        )

        invoke_custom_ruby_code_5 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_5',
            python_callable=lambda: pendulum.now(
                config.pacific_timezone).strftime('%d/%m/%Y')
        )

        if_request_type_contains_timesheet_6 = rail.IfOperator(
            task_id='if_request_type_contains_timesheet_6',
            test='''{{ dag_run.conf.type | matches('timesheet') }}''',
            yes_task="declare_variable_7",
            no_task="if_request_type_contains_payrule_42",
        )

        declare_variable_7 = rail.SetVariableOperator(
            task_id='declare_variable_7',
            append=False,
            name='Timesheet Template',
            value=None
        )

        def get_mapper_entry(dag_run):
            supervisor = dag_run.conf['SupervisorSSOID'] if dag_run.conf['SupervisorSSOID'] else ""
            jobtype = dag_run.conf['jobtype'] if dag_run.conf['jobtype'] else ""
            healthcare_product_line = dag_run.conf[
                'HealthcareProductLineEIT'] if dag_run.conf['HealthcareProductLineEIT'] else ""
            mapper_entry = list(filter(lambda x: x['type'] == "timesheettemplate" and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] ==
                                dag_run.conf['LegalEntity'] and x['supervisor'] == supervisor and x['jobtype'] == jobtype and x['healthcare_product_line'] == healthcare_product_line, netherlands_master_mapper))
            return mapper_entry[0]['value'] if mapper_entry else None

        ge_netherlands_user_sync_master_mapper_search_entries_8 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_8',
            python_callable=get_mapper_entry
        )

        if_entry_col7_present_9 = rail.IfOperator(
            task_id='if_entry_col7_present_9',
            test='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_8') | is_truthy }}''',
            yes_task="update_variable_10",
            no_task="ge_netherlands_user_sync_master_mapper_search_entries_12",
        )

        update_variable_10 = rail.SetVariableOperator(
            task_id='update_variable_10',
            append=False,
            name='{{ result("declare_variable_7").name }}',
            value='''{{ result("ge_netherlands_user_sync_master_mapper_search_entries_8") }}'''
        )

        def get_mapper_entry_12(dag_run):
            supervisor = dag_run.conf['SupervisorSSOID'] if dag_run.conf['SupervisorSSOID'] else ""
            jobtype = dag_run.conf['jobtype'] if dag_run.conf['jobtype'] else ""
            mapper_entry = list(filter(lambda x: x['type'] == "timesheettemplate" and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] ==
                                dag_run.conf['LegalEntity'] and x['supervisor'] == supervisor and x['jobtype'] == jobtype and x['healthcare_product_line'] == "NA", netherlands_master_mapper))
            return mapper_entry[0]['value'] if mapper_entry else None

        ge_netherlands_user_sync_master_mapper_search_entries_12 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_12',
            python_callable=get_mapper_entry_12
        )

        if_entry_col7_present_13 = rail.IfOperator(
            task_id='if_entry_col7_present_13',
            test='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_12') | is_truthy }}''',
            yes_task="update_variable_14",
            no_task="ge_netherlands_user_sync_master_mapper_search_entries_16",
        )

        update_variable_14 = rail.SetVariableOperator(
            task_id='update_variable_14',
            append=False,
            name='{{ result("declare_variable_7").name }}',
            value='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_12') }}'''
        )

        def get_mapper_entry_16(dag_run):
            jobtype = dag_run.conf['jobtype'] if dag_run.conf['jobtype'] else ""
            mapper_entry = list(filter(lambda x: x['type'] == "timesheettemplate" and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] ==
                                dag_run.conf['LegalEntity'] and x['supervisor'] == "NA" and x['jobtype'] == jobtype and x['healthcare_product_line'] == "NA", netherlands_master_mapper))
            return mapper_entry[0]['value'] if mapper_entry else None

        ge_netherlands_user_sync_master_mapper_search_entries_16 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_16',
            python_callable=get_mapper_entry_16
        )

        if_entry_col7_present_17 = rail.IfOperator(
            task_id='if_entry_col7_present_17',
            test='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_16') | is_truthy }}''',
            yes_task="update_variable_18",
            no_task="ge_netherlands_user_sync_master_mapper_search_entries_20",
        )

        update_variable_18 = rail.SetVariableOperator(
            task_id='update_variable_18',
            append=False,
            name='{{ result("declare_variable_7").name }}',
            value='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_16') }}'''
        )

        def get_mapper_entry_20(dag_run):
            supervisor = dag_run.conf['SupervisorSSOID'] if dag_run.conf['SupervisorSSOID'] else ""
            healthcareproductlineeit = dag_run.conf[
                'HealthcareProductLineEIT'] if dag_run.conf['HealthcareProductLineEIT'] else ""
            mapper_entry = list(filter(lambda x: x['type'] == "timesheettemplate" and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] ==
                                dag_run.conf['LegalEntity'] and x['supervisor'] == supervisor and x['jobtype'] == "NA" and x['healthcare_product_line'] == healthcareproductlineeit, netherlands_master_mapper))
            return mapper_entry[0]['value'] if mapper_entry else None

        ge_netherlands_user_sync_master_mapper_search_entries_20 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_20',
            python_callable=get_mapper_entry_20
        )

        if_entry_col7_present_21 = rail.IfOperator(
            task_id='if_entry_col7_present_21',
            test='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_20') | is_truthy }}''',
            yes_task="update_variable_22",
            no_task="ge_netherlands_user_sync_master_mapper_search_entries_24",
        )

        update_variable_22 = rail.SetVariableOperator(
            task_id='update_variable_22',
            append=False,
            name='{{ result("declare_variable_7").name }}',
            value='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_20') }}'''
        )

        def get_mapper_entry_24(dag_run):
            healthcareproductlineeit = dag_run.conf[
                'HealthcareProductLineEIT'] if dag_run.conf['HealthcareProductLineEIT'] else ""
            mapper_entry = list(filter(lambda x: x['type'] == "timesheettemplate" and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] ==
                                dag_run.conf['LegalEntity'] and x['supervisor'] == "NA" and x['jobtype'] == "NA" and x['healthcare_product_line'] == healthcareproductlineeit, netherlands_master_mapper))
            return mapper_entry[0]['value'] if mapper_entry else None

        ge_netherlands_user_sync_master_mapper_search_entries_24 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_24',
            python_callable=get_mapper_entry_24
        )

        if_entry_col7_present_25 = rail.IfOperator(
            task_id='if_entry_col7_present_25',
            test='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_24') | is_truthy }}''',
            yes_task="update_variable_26",
            no_task="ge_netherlands_user_sync_master_mapper_search_entries_28",
        )

        update_variable_26 = rail.SetVariableOperator(
            task_id='update_variable_26',
            append=False,
            name='{{ result("declare_variable_7").name }}',
            value='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_24') }}'''
        )

        def get_mapper_entry_28(dag_run):
            mapper_entry = list(filter(lambda x: x['type'] == "timesheettemplate" and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity']
                                == dag_run.conf['LegalEntity'] and x['supervisor'] == "NA" and x['jobtype'] == "NA" and x['healthcare_product_line'] == "NA", netherlands_master_mapper))
            return mapper_entry[0]['value'] if mapper_entry else None

        ge_netherlands_user_sync_master_mapper_search_entries_28 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_28',
            python_callable=get_mapper_entry_28
        )

        if_entry_col7_present_29 = rail.IfOperator(
            task_id='if_entry_col7_present_29',
            test='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_28') | is_truthy }}''',
            yes_task="update_variable_30",
            no_task="if_declare_variable_7_value_present_31",
        )

        update_variable_30 = rail.SetVariableOperator(
            task_id='update_variable_30',
            append=False,
            name='{{ result("declare_variable_7").name }}',
            value='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_28') }}'''
        )

        def timesheet_validation(dag_run):
            timsheettemplate = rail.get_dag_run_var('Timesheet Template')
            if timsheettemplate:
                return bool(timsheettemplate != dag_run.conf['currenttimesheettemplate'])
            return False

        if_declare_variable_7_value_present_31 = rail.IfOperator(
            task_id='if_declare_variable_7_value_present_31',
            test=timesheet_validation,
            yes_task="log_required_timesheet_templatename_32",
            no_task="insert_to_list_41",
        )

        log_required_timesheet_templatename_32 = rail.PythonOperator(
            task_id='log_required_timesheet_templatename_32',
            python_callable=lambda:  rail.get_dag_run_var('Timesheet Template')
        )

        _adhoc_http_action_33 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_33',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        def get_tstemplate_uri():
            if rail.result('_adhoc_http_action_33'):
                current_template = list(filter(lambda x: x['name'] and x['name'].lower() == rail.result(
                    'log_required_timesheet_templatename_32').lower(), rail.result('_adhoc_http_action_33')))
                return current_template[0]['uri'] if current_template else None
            return None

        log_required_timesheet_template_uri_34 = rail.PythonOperator(
            task_id='log_required_timesheet_template_uri_34',
            python_callable=get_tstemplate_uri
        )

        if_log_required_timesheet_template_uri_34_present_35 = rail.IfOperator(
            task_id='if_log_required_timesheet_template_uri_34_present_35',
            test='''{{ result('log_required_timesheet_template_uri_34') | is_truthy }}''',
            yes_task="assign_policy_set_to_user_36",
            no_task="insert_to_list_39",
        )

        assign_policy_set_to_user_36 = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user_36',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('log_required_timesheet_template_uri_34') }}"
            }
        )

        insert_to_list_37 = rail.SetVariableOperator(
            task_id='insert_to_list_37',
            append=True,
            name='{{ result("declare_list_4").name }}',
            value={
                "joblog": '''Timesheet template assigned "{{ result('log_required_timesheet_templatename_32') }}'''
            }
        )

        insert_to_list_39 = rail.SetVariableOperator(
            task_id='insert_to_list_39',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": '''Timesheet template "{{ result('log_required_timesheet_templatename_32') }}" not available in Replicon'''
            }
        )

        insert_to_list_41 = rail.SetVariableOperator(
            task_id='insert_to_list_41',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": '''Timesheet template not available in mapper for combination of legal entity "{{ dag_run.conf.LegalEntity }}" and Legacy Payroll ID "{{ dag_run.conf.LegacyPayrollID }}"'''
            }
        )

        if_request_type_contains_payrule_42 = rail.IfOperator(
            task_id='if_request_type_contains_payrule_42',
            test='''{{ dag_run.conf.type | matches('payrule') }}''',
            yes_task="declare_variable_43",
            no_task="catch_100_100_100",
        )

        declare_variable_43 = rail.SetVariableOperator(
            task_id='declare_variable_43',
            append=False,
            name='Payrule',
            value=None
        )

        def get_mapper_entry_44(dag_run):
            jobtype = dag_run.conf['jobtype'] if dag_run.conf['jobtype'] else ""
            healthcareproductlineeit = dag_run.conf[
                'HealthcareProductLineEIT'] if dag_run.conf['HealthcareProductLineEIT'] else ""
            supervisor = dag_run.conf['SupervisorSSOID'] if dag_run.conf['SupervisorSSOID'] else ""
            mapper_entry = list(filter(lambda x: x['type'] == "payrule" and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity']
                                and x['supervisor'] == supervisor and x['jobtype'] == jobtype and x['healthcare_product_line'] == healthcareproductlineeit, netherlands_master_mapper))
            return mapper_entry[0]['value'] if mapper_entry else None

        ge_netherlands_user_sync_master_mapper_search_entries_44 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_44',
            python_callable=get_mapper_entry_44
        )

        if_entry_col7_present_45 = rail.IfOperator(
            task_id='if_entry_col7_present_45',
            test='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_44') | is_truthy }}''',
            yes_task="update_variable_46",
            no_task="ge_netherlands_user_sync_master_mapper_search_entries_48",
        )

        update_variable_46 = rail.SetVariableOperator(
            task_id='update_variable_46',
            append=False,
            name='{{ result("declare_variable_43").name }}',
            value='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_44') }}'''
        )

        def get_mapper_entry_48(dag_run):
            jobtype = dag_run.conf['jobtype'] if dag_run.conf['jobtype'] else ""
            supervisor = dag_run.conf['SupervisorSSOID'] if dag_run.conf['SupervisorSSOID'] else ""
            mapper_entry = list(filter(lambda x: x['type'] == "payrule" and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] ==
                                dag_run.conf['LegalEntity'] and x['supervisor'] == supervisor and x['jobtype'] == jobtype and x['healthcare_product_line'] == "NA", netherlands_master_mapper))
            return mapper_entry[0]['value'] if mapper_entry else None

        ge_netherlands_user_sync_master_mapper_search_entries_48 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_48',
            python_callable=get_mapper_entry_48
        )

        if_entry_col7_present_49 = rail.IfOperator(
            task_id='if_entry_col7_present_49',
            test='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_48') | is_truthy }}''',
            yes_task="update_variable_50",
            no_task="ge_netherlands_user_sync_master_mapper_search_entries_52",
        )

        update_variable_50 = rail.SetVariableOperator(
            task_id='update_variable_50',
            append=False,
            name='{{ result("declare_variable_43").name }}',
            value='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_48') }}'''
        )

        def get_mapper_entry_52(dag_run):
            jobtype = dag_run.conf['jobtype'] if dag_run.conf['jobtype'] else ""
            mapper_entry = list(filter(lambda x: x['type'] == "payrule" and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] ==
                                dag_run.conf['LegalEntity'] and x['supervisor'] == "NA" and x['jobtype'] == jobtype and x['healthcare_product_line'] == "NA", netherlands_master_mapper))
            return mapper_entry[0]['value'] if mapper_entry else None

        ge_netherlands_user_sync_master_mapper_search_entries_52 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_52',
            python_callable=get_mapper_entry_52
        )

        if_entry_col7_present_53 = rail.IfOperator(
            task_id='if_entry_col7_present_53',
            test='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_52') | is_truthy }}''',
            yes_task="update_variable_54",
            no_task="ge_netherlands_user_sync_master_mapper_search_entries_56",
        )

        update_variable_54 = rail.SetVariableOperator(
            task_id='update_variable_54',
            append=False,
            name='{{ result("declare_variable_43").name }}',
            value='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_52') }}'''
        )

        def get_mapper_entry_56(dag_run):
            healthcareproductlineeit = dag_run.conf[
                'HealthcareProductLineEIT'] if dag_run.conf['HealthcareProductLineEIT'] else ""
            supervisor = dag_run.conf['SupervisorSSOID'] if dag_run.conf['SupervisorSSOID'] else ""
            mapper_entry = list(filter(lambda x: x['type'] == "payrule" and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity']
                                and x['supervisor'] == supervisor and x['jobtype'] == "NA" and x['healthcare_product_line'] == healthcareproductlineeit, netherlands_master_mapper))
            return mapper_entry[0]['value'] if mapper_entry else None

        ge_netherlands_user_sync_master_mapper_search_entries_56 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_56',
            python_callable=get_mapper_entry_56
        )

        if_entry_col7_present_57 = rail.IfOperator(
            task_id='if_entry_col7_present_57',
            test='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_56') | is_truthy }}''',
            yes_task="update_variable_58",
            no_task="ge_netherlands_user_sync_master_mapper_search_entries_60",
        )

        update_variable_58 = rail.SetVariableOperator(
            task_id='update_variable_58',
            append=False,
            name='{{ result("declare_variable_43").name }}',
            value='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_56') }}'''
        )

        def get_mapper_entry_60(dag_run):
            healthcareproductlineeit = dag_run.conf[
                'HealthcareProductLineEIT'] if dag_run.conf['HealthcareProductLineEIT'] else ""
            mapper_entry = list(filter(lambda x: x['type'] == "payrule" and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] == dag_run.conf['LegalEntity']
                                and x['supervisor'] == "NA" and x['jobtype'] == "NA" and x['healthcare_product_line'] == healthcareproductlineeit, netherlands_master_mapper))
            return mapper_entry[0]['value'] if mapper_entry else None

        ge_netherlands_user_sync_master_mapper_search_entries_60 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_60',
            python_callable=get_mapper_entry_60
        )

        if_entry_col7_present_61 = rail.IfOperator(
            task_id='if_entry_col7_present_61',
            test='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_60') | is_truthy }}''',
            yes_task="update_variable_62",
            no_task="ge_netherlands_user_sync_master_mapper_search_entries_64",
        )

        update_variable_62 = rail.SetVariableOperator(
            task_id='update_variable_62',
            append=False,
            name='{{ result("declare_variable_43").name }}',
            value='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_60') }}'''
        )

        def get_mapper_entry_64(dag_run):
            mapper_entry = list(filter(lambda x: x['type'] == "payrule" and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID'] and x['legal_entity'] ==
                                dag_run.conf['LegalEntity'] and x['supervisor'] == "NA" and x['jobtype'] == "NA" and x['healthcare_product_line'] == "NA", netherlands_master_mapper))
            return mapper_entry[0]['value'] if mapper_entry else None

        ge_netherlands_user_sync_master_mapper_search_entries_64 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_64',
            python_callable=get_mapper_entry_64
        )

        if_entry_col7_present_65 = rail.IfOperator(
            task_id='if_entry_col7_present_65',
            test='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_64') | is_truthy }}''',
            yes_task="update_variable_66",
            no_task="if_declare_variable_43_value_present_67",
        )

        update_variable_66 = rail.SetVariableOperator(
            task_id='update_variable_66',
            append=False,
            name='{{ result("declare_variable_43").name }}',
            value='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_64') }}'''
        )

        if_declare_variable_43_value_present_67 = rail.IfOperator(
            task_id='if_declare_variable_43_value_present_67',
            test=lambda: bool(rail.get_dag_run_var('Payrule')),
            yes_task="log_required_payrule_name_68",
            no_task="insert_to_list_98",
        )

        log_required_payrule_name_68 = rail.PythonOperator(
            task_id='log_required_payrule_name_68',
            python_callable=lambda:  rail.get_dag_run_var('Payrule')
        )

        get_all_pay_rule_scripts_69 = rail.RepliconServiceOperator(
            task_id='get_all_pay_rule_scripts_69',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
        )

        def get_payrule_uri():
            if rail.result('get_all_pay_rule_scripts_69'):
                current_payrule = list(filter(lambda x: x['displayText'] and x['displayText'].lower() == rail.result(
                    'log_required_payrule_name_68').lower(), rail.result('get_all_pay_rule_scripts_69')))
                return current_payrule[0]['uri'] if current_payrule else None
            return None

        log_required_payrule_uri_70 = rail.PythonOperator(
            task_id='log_required_payrule_uri_70',
            python_callable=get_payrule_uri
        )

        if_log_required_payrule_uri_70_present_71 = rail.IfOperator(
            task_id='if_log_required_payrule_uri_70_present_71',
            test='''{{ result('log_required_payrule_uri_70') | is_truthy }}''',
            yes_task="if_request_action_contains_update_72",
            no_task="insert_to_list_96",
        )

        if_request_action_contains_update_72 = rail.IfOperator(
            task_id='if_request_action_contains_update_72',
            test='''{{ dag_run.conf.action | matches('update') }}''',
            yes_task="declare_list_73",
            no_task="put_pay_rule_script_assignment_schedule_for_user_93",
        )

        declare_list_73 = rail.SetVariableOperator(
            task_id='declare_list_73',
            append=False,
            name='payrule schedule list',
            value=[]
        )

        declare_list_74 = rail.SetVariableOperator(
            task_id='declare_list_74',
            append=False,
            name='scheduleEntries',
            value=[]
        )

        get_pay_rule_script_assignment_schedule_for_user_75 = rail.RepliconServiceOperator(
            task_id='get_pay_rule_script_assignment_schedule_for_user_75',
            endpoint="/services/PayRuleScriptService2.svc/GetPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        def get_datetime_obj(effectiveDate):
            year = effectiveDate['year']
            month = effectiveDate['month']
            day = effectiveDate['day']
            return datetime.strptime(f"{year}/{month}/{day}", '%Y/%m/%d')

        def get_assignment_date(dag_run):
            if dag_run.conf['AssignmentEffectiveDate']:
                assigment_eff_date = datetime.strptime(
                    dag_run.conf['AssignmentEffectiveDate'], '%d/%m/%Y')
                return {
                    "year": assigment_eff_date.year,
                    "month": assigment_eff_date.month,
                    "day": assigment_eff_date.day
                }
            return {
                "year": pendulum.now(config.pacific_timezone).year,
                "month": pendulum.now(config.pacific_timezone).month,
                "day": pendulum.now(config.pacific_timezone).day
            }

        def payrule_script_schedule_list(dag_run):
            pay_schedules = []
            payrule_list = []
            payrule_schedules = rail.result(
                'get_pay_rule_script_assignment_schedule_for_user_75')
            todays_date = pendulum.now(config.pacific_timezone)
            for payrule_schedule in payrule_schedules:
                if payrule_schedule['effectiveDate']:
                    effective_date = get_datetime_obj(
                        payrule_schedule['effectiveDate'])
                    if effective_date.date() != pendulum.now(config.pacific_timezone).date():
                        payrule_list.append({
                            "uri": payrule_schedule['payRuleScript']['uri'],
                            "effectiveDate": payrule_schedule['effectiveDate'],
                            "name": payrule_schedule['payRuleScript']['displayText'],
                        })
                        pay_schedules.append({
                            "payRuleScript": {
                                "uri": payrule_schedule['payRuleScript']['uri'],
                                "name": null
                            },
                            "effectiveDate": payrule_schedule['effectiveDate']
                        })
                else:
                    user_start_date = datetime.strptime(
                        dag_run.conf['userstartdate'], '%d/%m/%Y')
                    differential_date = todays_date - user_start_date
                    payrule_list.append({
                        "uri": payrule_schedule['payRuleScript']['uri'],
                        "effectiveDate": {
                            "year": differential_date.year,
                            "month": differential_date.month,
                            "day": differential_date.day
                        },
                        "name": payrule_schedule['payRuleScript']['displayText'],
                    })
                    pay_schedules.append({
                        "payRuleScript": {
                            "uri": payrule_schedule['payRuleScript']['uri'],
                            "name": null
                        },
                        "effectiveDate": null
                    })

            current_assignment_date = max(get_datetime_obj(
                x['effectiveDate']) for x in payrule_list)
            current_assignment_date = current_assignment_date if current_assignment_date else pendulum.now(
                config.pacific_timezone)
            current_payrule_name_info = list(filter(lambda x: get_datetime_obj(
                x['effectiveDate']).date() == current_assignment_date.date(), payrule_list))
            current_payrule_name = current_payrule_name_info[0][
                'name'] if current_payrule_name_info else None
            payrule_name = rail.get_dag_run_var(
                rail.result('declare_variable_43')['name'])
            if current_payrule_name != payrule_name:
                pay_schedules.append({
                    "payRuleScript": {
                        "uri": rail.result('log_required_payrule_uri_70'),
                        "name": null
                    },
                    "effectiveDate": {
                        "year": todays_date.year,
                        "month": todays_date.month,
                        "day": todays_date.day
                    }
                })

                eff_assignment_date = get_assignment_date(dag_run)
                pay_schedules.append({
                    "payRuleScript": {
                        "uri": rail.result('log_required_payrule_uri_70'),
                        "name": null
                    },
                    "effectiveDate": eff_assignment_date
                })
                return pay_schedules

            return []

        log_required_payrule_uri_86 = rail.PythonOperator(
            task_id='log_required_payrule_uri_86',
            python_callable=payrule_script_schedule_list
        )

        if_payrule_changed_87 = rail.IfOperator(
            task_id='if_payrule_changed_87',
            test='''{{ result('log_required_payrule_uri_86')') | length > 0 }}''',
            yes_task="put_pay_rule_script_assignment_schedule_for_user_90",
            no_task="insert_to_list_98",
        )

        put_pay_rule_script_assignment_schedule_for_user_90 = rail.RepliconServiceOperator(
            task_id='put_pay_rule_script_assignment_schedule_for_user_90',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": payrule_script_schedule_list(dag_run)
            }
        )

        insert_to_list_91 = rail.SetVariableOperator(
            task_id='insert_to_list_91',
            append=True,
            name='{{ result("declare_list_4").name }}',
            value={
                "joblog": '''Payrule updated "{{ result('log_required_payrule_name_68') }}'''
            }
        )

        put_pay_rule_script_assignment_schedule_for_user_93 = rail.RepliconServiceOperator(
            task_id='put_pay_rule_script_assignment_schedule_for_user_93',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": rail.result('log_required_payrule_uri_70'),
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        insert_to_list_94 = rail.SetVariableOperator(
            task_id='insert_to_list_94',
            append=True,
            name='{{ result("declare_list_4").name }}',
            value={
                "joblog": '''Payrule assigned "{{ result('log_required_payrule_name_68') }}'''
            }
        )

        insert_to_list_96 = rail.SetVariableOperator(
            task_id='insert_to_list_96',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": '''Payrule "{{ result('log_required_payrule_name_68') }}" not available in Replicon'''
            }
        )

        insert_to_list_98 = rail.SetVariableOperator(
            task_id='insert_to_list_98',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": '''Payrule not available in mapper for combination of legal entity "{{ dag_run.conf.LegalEntity }}" and Legacy Payroll ID "{{ dag_run.conf.LegacyPayrollID }}"'''
            }
        )

        catch_100_100_100 = rail.EmptyOperator(
            task_id='catch_100_100_100',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_100_100_100
        can_run_batch_task >> rail.Label('No') >> declare_list_3
        declare_list_3 >> declare_list_4 >> invoke_custom_ruby_code_5 >> if_request_type_contains_timesheet_6
        if_request_type_contains_timesheet_6 >> rail.Label(
            'Yes') >> declare_variable_7 >> ge_netherlands_user_sync_master_mapper_search_entries_8 >> if_entry_col7_present_9
        if_entry_col7_present_9 >> rail.Label(
            'Yes') >> update_variable_10 >> if_declare_variable_7_value_present_31
        if_entry_col7_present_9 >> rail.Label(
            'No') >> ge_netherlands_user_sync_master_mapper_search_entries_12 >> if_entry_col7_present_13
        if_entry_col7_present_13 >> rail.Label(
            'Yes') >> update_variable_14 >> if_declare_variable_7_value_present_31
        if_entry_col7_present_13 >> rail.Label(
            'No') >> ge_netherlands_user_sync_master_mapper_search_entries_16 >> if_entry_col7_present_17
        if_entry_col7_present_17 >> rail.Label(
            'Yes') >> update_variable_18 >> if_declare_variable_7_value_present_31
        if_entry_col7_present_17 >> rail.Label(
            'No') >> ge_netherlands_user_sync_master_mapper_search_entries_20 >> if_entry_col7_present_21
        if_entry_col7_present_21 >> rail.Label(
            'Yes') >> update_variable_22 >> if_declare_variable_7_value_present_31
        if_entry_col7_present_21 >> rail.Label(
            'No') >> ge_netherlands_user_sync_master_mapper_search_entries_24 >> if_entry_col7_present_25
        if_entry_col7_present_25 >> rail.Label(
            'Yes') >> update_variable_26 >> if_declare_variable_7_value_present_31
        if_entry_col7_present_25 >> rail.Label(
            'No') >> ge_netherlands_user_sync_master_mapper_search_entries_28 >> if_entry_col7_present_29
        if_entry_col7_present_29 >> rail.Label(
            'Yes') >> update_variable_30 >> if_declare_variable_7_value_present_31
        if_entry_col7_present_29 >> rail.Label(
            'No') >> if_declare_variable_7_value_present_31
        if_declare_variable_7_value_present_31 >> rail.Label(
            'No') >> insert_to_list_41 >> if_request_type_contains_payrule_42
        if_declare_variable_7_value_present_31 >> rail.Label(
            'Yes') >> log_required_timesheet_templatename_32 >> _adhoc_http_action_33 >> \
            log_required_timesheet_template_uri_34 >> if_log_required_timesheet_template_uri_34_present_35
        if_log_required_timesheet_template_uri_34_present_35 >> rail.Label(
            'Yes') >> assign_policy_set_to_user_36 >> insert_to_list_37 >> if_request_type_contains_payrule_42
        if_log_required_timesheet_template_uri_34_present_35 >> rail.Label(
            'No') >> insert_to_list_39 >> if_request_type_contains_payrule_42
        if_request_type_contains_payrule_42 >> rail.Label(
            'No') >> catch_100_100_100
        if_request_type_contains_timesheet_6 >> rail.Label(
            'No') >> if_request_type_contains_payrule_42
        if_request_type_contains_payrule_42 >> rail.Label(
            'Yes') >> declare_variable_43 >> ge_netherlands_user_sync_master_mapper_search_entries_44 >> if_entry_col7_present_45
        if_entry_col7_present_45 >> rail.Label(
            'Yes') >> update_variable_46 >> if_declare_variable_43_value_present_67
        if_entry_col7_present_45 >> rail.Label(
            'No') >> ge_netherlands_user_sync_master_mapper_search_entries_48 >> if_entry_col7_present_49
        if_entry_col7_present_49 >> rail.Label(
            'Yes') >> update_variable_50 >> if_declare_variable_43_value_present_67
        if_entry_col7_present_49 >> rail.Label(
            'No') >> ge_netherlands_user_sync_master_mapper_search_entries_52 >> if_entry_col7_present_53
        if_entry_col7_present_53 >> rail.Label(
            'Yes') >> update_variable_54 >> if_declare_variable_43_value_present_67
        if_entry_col7_present_53 >> rail.Label(
            'No') >> ge_netherlands_user_sync_master_mapper_search_entries_56 >> if_entry_col7_present_57
        if_entry_col7_present_57 >> rail.Label(
            'Yes') >> update_variable_58 >> if_declare_variable_43_value_present_67
        if_entry_col7_present_57 >> rail.Label(
            'No') >> ge_netherlands_user_sync_master_mapper_search_entries_60 >> if_entry_col7_present_61
        if_entry_col7_present_61 >> rail.Label(
            'Yes') >> update_variable_62 >> if_declare_variable_43_value_present_67
        if_entry_col7_present_61 >> rail.Label(
            'No') >> ge_netherlands_user_sync_master_mapper_search_entries_64 >> if_entry_col7_present_65
        if_entry_col7_present_65 >> rail.Label(
            'Yes') >> update_variable_66 >> if_declare_variable_43_value_present_67
        if_entry_col7_present_65 >> rail.Label(
            'No') >> if_declare_variable_43_value_present_67
        if_declare_variable_43_value_present_67 >> rail.Label(
            'No') >> insert_to_list_98
        if_declare_variable_43_value_present_67 >> rail.Label(
            'Yes') >> log_required_payrule_name_68 >> get_all_pay_rule_scripts_69 >> \
            log_required_payrule_uri_70 >> if_log_required_payrule_uri_70_present_71
        if_log_required_payrule_uri_70_present_71 >> rail.Label(
            'Yes') >> if_request_action_contains_update_72
        if_request_action_contains_update_72 >> rail.Label(
            'Yes') >> declare_list_73 >> declare_list_74 >> get_pay_rule_script_assignment_schedule_for_user_75 >> \
            log_required_payrule_uri_86 >> if_payrule_changed_87
        if_payrule_changed_87 >> rail.Label('No') >> insert_to_list_98
        if_payrule_changed_87 >> rail.Label(
            'Yes') >> put_pay_rule_script_assignment_schedule_for_user_90 >> insert_to_list_91 >> catch_100_100_100
        if_request_action_contains_update_72 >> rail.Label(
            'No') >> put_pay_rule_script_assignment_schedule_for_user_93 >> insert_to_list_94 >> catch_100_100_100
        if_log_required_payrule_uri_70_present_71 >> rail.Label(
            'No') >> insert_to_list_96 >> catch_100_100_100
        rail.Label('No') >> insert_to_list_98 >> catch_100_100_100
        if_request_type_contains_payrule_42 >> rail.Label(
            'No') >> catch_100_100_100 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
