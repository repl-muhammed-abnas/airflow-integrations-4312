
from datetime import timedelta, datetime
import json
from ge.user_sync_czech.czech_master_mapper import czech_master_mapper
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_czech_child_workflow_to_add_timeoff_type_for_new_user_v1_0_{config.instance}',
        description=f'GE_Czech_Child Workflow to add timeoff type for new user v1.0 {config.instance}',
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
            no_task='declare_list_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_3',
            end_task='add_timeoff_type_logs_40',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_3 = rail.SetVariableOperator(
            task_id='declare_list_3',
            append=False,
            name='time off assignment list',
            value=[]
        )

        _adhoc_http_action_4 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_4',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        if_first_displaytext_present_5 = rail.IfOperator(
            task_id='if_first_displaytext_present_5',
            test='''{{ result('_adhoc_http_action_4') | is_truthy and result('_adhoc_http_action_4') | length > 0 }}''',
            yes_task="czech_master_mapper_search_entries_6",
            no_task="add_timeoff_type_logs_40",
        )

        def get_entity_from_mapper(LegalEntity, to_type):
            emp_types = list(filter(
                lambda x: x['legal_entity'] == LegalEntity
                and x['type'] == to_type, czech_master_mapper))
            return [emp_type['value'] for emp_type in emp_types]

        czech_master_mapper_search_entries_6 = rail.PythonOperator(
            task_id='czech_master_mapper_search_entries_6',
            python_callable=lambda dag_run: get_entity_from_mapper(
                dag_run.conf['legalentity'], 'Time Off Type')
        )

        if_entry_col1_blank_7 = rail.IfOperator(
            task_id='if_entry_col1_blank_7',
            test='''{{ result('czech_master_mapper_search_entries_6') | is_falsy }}''',
            yes_task="add_timeoff_type_logs_8",
            no_task="log_final_set_timeoff_info_12",
        )

        add_timeoff_type_logs_8 = rail.WriteLogOperator(
            task_id='add_timeoff_type_logs_8',
            message="Timeoff not assigned/updated as no timeoff is defined in mapper for legal entity - {{ dag_run.conf.Legalentity}}",
            severity="Error",
            properties={
                "action": "{{ dag_run.conf.type }}",
                "status": "Error",
                "details": "Timeoff not assigned/updated as no timeoff is defined in mapper for legal entity - {{ dag_run.conf.Legalentity}}",
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}"
            }
        )

        def get_final_set_timeoff_info():
            timeoff_info = []
            for mapper_to_info in rail.result('czech_master_mapper_search_entries_6'):
                timeoff_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                    '_adhoc_http_action_4'), 'displayText', mapper_to_info, 'uri')
                if timeoff_uri:
                    timeoff_info.append({
                        "name": mapper_to_info,
                        "uri": timeoff_uri
                    })

            return timeoff_info

        log_final_set_timeoff_info_12 = rail.PythonOperator(
            task_id='log_final_set_timeoff_info_12',
            python_callable=get_final_set_timeoff_info
        )

        log_final_set_timeoff_uris_12 = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris_12',
            python_callable=lambda: [to['uri']
                                     for to in rail.result('log_final_set_timeoff_info_12')]
        )

        if_log_12_present_13 = rail.IfOperator(
            task_id='if_log_12_present_13',
            test='''{{ result('log_final_set_timeoff_uris_12') | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user_15",
            no_task="add_timeoff_type_logs_40",
        )

        put_time_off_type_assignments_for_user_15 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_15',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_final_set_timeoff_uris_12')
            }
        )

        get_all_scriptsfor_time_off_balance_event_script_administration_service_16 = rail.RepliconServiceOperator(
            task_id='get_all_scriptsfor_time_off_balance_event_script_administration_service_16',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts"
        )

        get_all_scriptsfor_time_off_validation_script_administration_service1_17 = rail.RepliconServiceOperator(
            task_id='get_all_scriptsfor_time_off_validation_script_administration_service1_17',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts"
        )

        log_start_date_month_18 = rail.PythonOperator(
            task_id='log_start_date_month_18',
            python_callable=lambda:  ""
        )

        if_request_jobpositiontitle_equals_to_engineerremotetechnicalsupport_19 = rail.IfOperator(
            task_id='if_request_jobpositiontitle_equals_to_engineerremotetechnicalsupport_19',
            test='''{{ dag_run.conf.jobpositiontitle == 'Engineer - Remote Technical Support' or dag_run.conf.jobpositiontitle == 'Field Engineer 2' }}''',
            yes_task="log_timeoffurifor_c_z_compensation_time_20",
            no_task="log_timeoffurifor_c_z_vacation_27",
        )

        log_timeoffurifor_c_z_compensation_time_20 = rail.PythonOperator(
            task_id='log_timeoffurifor_c_z_compensation_time_20',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'log_final_set_timeoff_info_12'), 'name', "CZ_Compensation Time", 'uri', '')
        )

        get_default_time_off_type_policy_schedule_for_user_22 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_22',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('log_timeoffurifor_c_z_compensation_time_20') }}"
                }
            }
        )

        log_timeoff_policy_24 = rail.PythonOperator(
            task_id='log_timeoff_policy_24',
            python_callable=lambda: json.loads(json.dumps(
                    rail.result('get_default_time_off_type_policy_schedule_for_user_22'), ensure_ascii=False).replace('null', '"effective"').replace(
                        '"script"', '"scriptTarget"'))
        )

        if_log_timeoff_policy_24_present_25 = rail.IfOperator(
            task_id='if_log_timeoff_policy_24_present_25',
            test='''{{ result('log_timeoff_policy_24') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_26",
            no_task="log_timeoffurifor_c_z_vacation_27",
        )

        put_user_time_off_account_policy_set_schedule_26 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_26',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('log_timeoffurifor_c_z_compensation_time_20')
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_24')
            }
        )

        log_timeoffurifor_c_z_vacation_27 = rail.PythonOperator(
            task_id='log_timeoffurifor_c_z_vacation_27',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('log_final_set_timeoff_info_12'), 'name', "CZ_Vacation", 'uri')
        )

        log_startingbalancesetto_script_target_28 = rail.PythonOperator(
            task_id='log_startingbalancesetto_script_target_28',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_scriptsfor_time_off_balance_event_script_administration_service_16'), 'displayText', 'Starting Balance Set To', 'uri')
        )

        log_yearly_accrualwith_expiry_script_target_29 = rail.PythonOperator(
            task_id='log_yearly_accrualwith_expiry_script_target_29',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_scriptsfor_time_off_balance_event_script_administration_service_16'), 'displayText', 'Yearly Accrual with Expiry', 'uri')
        )

        def is_start_date_greater_than_begining_year(dag_run):
            start_date = datetime.strptime(
                dag_run.conf['startdate'], '%d/%m/%Y')
            start_of_year = start_date.replace(month=1, day=1)
            if start_date > start_of_year:
                return True
            return False

        if_startdate_to_date_greater_than_dataworkato_servicereceive_requestrequeststartdateto_datebeginning_of_year_30 = rail.IfOperator(
            task_id='if_startdate_to_date_greater_than_dataworkato_servicereceive_requestrequeststartdateto_datebeginning_of_year_30',
            test=is_start_date_greater_than_begining_year,
            yes_task="put_user_time_off_account_policy_set_schedule_37",
            no_task="put_user_time_off_account_policy_set_schedule_whenstartdateis1stjanwithoutstartingbalancesetto_39",
        )

        def get_to_policy_schedule(dag_run):
            start_date = datetime.strptime(
                dag_run.conf['startdate'], '%d/%m/%Y')
            year_end_date = start_date.replace(month=12, day=31)
            year_end_diff = (year_end_date - start_date).days
            balance_days = float(25 / 365)
            starting_balance = round(float(balance_days * year_end_diff))
            return {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('log_timeoffurifor_c_z_vacation_27'),
                },
                "policySetScheduleEntries": [
                    {
                        "effectiveDate": {
                            "year": start_date.year,
                            "month": start_date.month,
                            "day": start_date.day
                        },
                        "description": "Effective On " + str(start_date.month) + "-" + str(start_date.day)+"-" + str(start_date.year),
                        "policySet": {
                            "timeOffBalanceEventScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": rail.result('log_startingbalancesetto_script_target_28')
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:amount",
                                            "value": {
                                                "number": starting_balance
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "number": "10"
                                            }
                                        }
                                    ]
                                },
                                {
                                    "scriptTarget": {
                                        "uri": rail.result('log_yearly_accrualwith_expiry_script_target_29')
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                            "value": {
                                                "number": "25"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrue-on-month",
                                            "value": {
                                                "uri": "urn:replicon:month:january"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month",
                                            "value": {
                                                "uri": "urn:replicon:monthly-frequency-start-day-option:1st"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:proration-option",
                                            "value": {
                                                "uri": "urn:replicon:time-off-policy-proration-option:do-not-prorate"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:expire-after",
                                            "value": {
                                                "number": 2
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:expire-after-unit",
                                            "value": {
                                                "uri": "urn:replicon:time-off-expire-after-unit:years"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:expiry-upon-option",
                                            "value": {
                                                "uri": "urn:replicon:time-off-upon-expiry-option:do-not-pay-out"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "number": "30"
                                            }
                                        }
                                    ]
                                }
                            ],
                            "timeOffValidationScripts": []
                        }
                    }
                ]
            }

        put_user_time_off_account_policy_set_schedule_37 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_37',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=get_to_policy_schedule
        )

        put_user_time_off_account_policy_set_schedule_whenstartdateis1stjanwithoutstartingbalancesetto_39 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_whenstartdateis1stjanwithoutstartingbalancesetto_39',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('log_timeoffurifor_c_z_vacation_27')
                },
                "policySetScheduleEntries": [
                    {
                        "effectiveDate": {
                            "year": datetime.strptime(dag_run.conf['startdate'], '%d/%m/%Y').year,
                            "month": datetime.strptime(dag_run.conf['startdate'], '%d/%m/%Y').month,
                            "day": datetime.strptime(dag_run.conf['startdate'], '%d/%m/%Y').day
                        },
                        "description": "Effective On " + str(datetime.strptime(dag_run.conf['startdate'], '%d/%m/%Y').month) + "-" + str(datetime.strptime(dag_run.conf['startdate'], '%d/%m/%Y').day)+"-" + str(datetime.strptime(dag_run.conf['startdate'], '%d/%m/%Y').year),
                        "policySet": {
                            "timeOffBalanceEventScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": rail.result('log_yearly_accrualwith_expiry_script_target_29')
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                                            "value": {
                                                "number": "25"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrue-on-month",
                                            "value": {
                                                "uri": "urn:replicon:month:january"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month",
                                            "value": {
                                                "uri": "urn:replicon:monthly-frequency-start-day-option:1st"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:proration-option",
                                            "value": {
                                                "uri": "urn:replicon:time-off-policy-proration-option:do-not-prorate"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:expire-after",
                                            "value": {
                                                "number": 2
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:expire-after-unit",
                                            "value": {
                                                "uri": "urn:replicon:time-off-expire-after-unit:years"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:expiry-upon-option",
                                            "value": {
                                                "uri": "urn:replicon:time-off-upon-expiry-option:do-not-pay-out"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "number": "30"
                                            }
                                        }
                                    ]
                                }
                            ],
                            "timeOffValidationScripts": []
                        }
                    }
                ]
            }
        )

        add_timeoff_type_logs_40 = rail.WriteLogOperator(
            task_id='add_timeoff_type_logs_40',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "action": "{{ dag_run.conf.type }}",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.EmployeeFirstName }} {{ dag_run.conf.EmployeeLastName }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> add_timeoff_type_logs_40
        can_run_batch_task >> rail.Label('No') >> declare_list_3
        declare_list_3 >> _adhoc_http_action_4 >> if_first_displaytext_present_5
        if_first_displaytext_present_5 >> rail.Label(
            'Yes') >> czech_master_mapper_search_entries_6 >> if_entry_col1_blank_7
        if_entry_col1_blank_7 >> rail.Label(
            'Yes') >> add_timeoff_type_logs_8 >> add_timeoff_type_logs_40
        if_entry_col1_blank_7 >> rail.Label(
            'No') >> log_final_set_timeoff_info_12 >> log_final_set_timeoff_uris_12 >> if_log_12_present_13
        if_log_12_present_13 >> rail.Label(
            'Yes') >> put_time_off_type_assignments_for_user_15 >>\
            get_all_scriptsfor_time_off_balance_event_script_administration_service_16 >>\
            get_all_scriptsfor_time_off_validation_script_administration_service1_17 >> log_start_date_month_18 >>\
            if_request_jobpositiontitle_equals_to_engineerremotetechnicalsupport_19
        if_request_jobpositiontitle_equals_to_engineerremotetechnicalsupport_19 >> rail.Label(
            'Yes') >> log_timeoffurifor_c_z_compensation_time_20 >> get_default_time_off_type_policy_schedule_for_user_22 >>\
            log_timeoff_policy_24 >> if_log_timeoff_policy_24_present_25
        if_log_timeoff_policy_24_present_25 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_26 >> log_timeoffurifor_c_z_vacation_27
        if_log_timeoff_policy_24_present_25 >> rail.Label(
            'No') >> log_timeoffurifor_c_z_vacation_27
        if_request_jobpositiontitle_equals_to_engineerremotetechnicalsupport_19 >> rail.Label(
            'No') >> log_timeoffurifor_c_z_vacation_27 >> log_startingbalancesetto_script_target_28 >>\
            log_yearly_accrualwith_expiry_script_target_29 >>\
            if_startdate_to_date_greater_than_dataworkato_servicereceive_requestrequeststartdateto_datebeginning_of_year_30
        if_startdate_to_date_greater_than_dataworkato_servicereceive_requestrequeststartdateto_datebeginning_of_year_30 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_37 >> add_timeoff_type_logs_40
        if_startdate_to_date_greater_than_dataworkato_servicereceive_requestrequeststartdateto_datebeginning_of_year_30 >> rail.Label(
            'No') >> put_user_time_off_account_policy_set_schedule_whenstartdateis1stjanwithoutstartingbalancesetto_39 >> add_timeoff_type_logs_40
        if_log_12_present_13 >> rail.Label('No') >> add_timeoff_type_logs_40
        if_first_displaytext_present_5 >> rail.Label(
            'No') >> add_timeoff_type_logs_40 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
