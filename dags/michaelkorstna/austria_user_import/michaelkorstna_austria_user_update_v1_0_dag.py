
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail

from michaelkorstna.austria_user_import.mappers.michael_kors_gmbh_user_sync_master_mapper_austria import michael_kors_gmbh_user_sync_master_mapper_austria

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_austria_user_update_child_{config.instance}',
        description=f'MichaelKorsTnA Austria User Update V1.0 {config.instance}',
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_list_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_2',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_2 = rail.SetVariableOperator(
            task_id='declare_list_2',
            append=False,
            name='logs',
            value=[]
        )

        declare_list_3 = rail.SetVariableOperator(
            task_id='declare_list_3',
            append=False,
            name='Exception',
            value=[]
        )

        def get_date_string(dateobj):
            return str(dateobj['day']) + "/" + str(dateobj['month']) + "/" + str(dateobj['year'])

        michael_kors_gmbh_user_sync_master_mapper_austria_search_entries_5 = rail.PythonOperator(
            task_id='michael_kors_gmbh_user_sync_master_mapper_austria_search_entries_5',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["country"] == dag_run.conf['country'], michael_kors_gmbh_user_sync_master_mapper_austria))
        )

        if_first_id_blank_6 = rail.IfOperator(
            task_id='if_first_id_blank_6',
            test=lambda: len(rail.result('michael_kors_gmbh_user_sync_master_mapper_austria_search_entries_5')) < 1,
            yes_task="michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_add_entry_297_297_7",
            no_task="bulk_get_users3_9",
        )

        michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_add_entry_297_297_7 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_add_entry_297_297_7',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Exception",
            properties={
                "loginname": "{{dag_run.conf.employeeid}}",
                "action": "Update",
                "status": "Exception",
                "details": 'Country "{{ dag_run.conf.country }}" not available in mapper',
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"
            }
        )

        bulk_get_users3_9 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_9',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.useruri }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        log_startdate_10 = rail.PythonOperator(
            task_id='log_startdate_10',
            python_callable=lambda: get_date_string(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate'])
        )

        def get_todays_date():
            todaydate = datetime.now()
            return {
                'day': todaydate.day,
                'month': todaydate.month,
                'year': todaydate.year
            }

        invoke_custom_ruby_code_todays_date_11 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_todays_date_11',
            python_callable=get_todays_date
        )

        log_annual_leavetimeoff_uri_14 = rail.PythonOperator(
            task_id='log_annual_leavetimeoff_uri_14',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_9')[0]['timeOffTypePolicySummary']['policiesByTimeOffType'], 'timeOffType.name', '[AT] Annual leave', 'timeOffType.uri', '')
        )

        if_division_displaytext_present_15 = rail.IfOperator(
            task_id='if_division_displaytext_present_15',
            test=lambda: rail.result('bulk_get_users3_9') and rail.result('bulk_get_users3_9')[0]['divisionSchedule'] and rail.result(
                'bulk_get_users3_9')[0]['divisionSchedule'][0]['division'] and rail.result(
                'bulk_get_users3_9')[0]['divisionSchedule'][0]['division']['displayText'] and rail.result(
                'bulk_get_users3_9')[0]['divisionSchedule'][0]['division']['displayText'] != 'Austria',
            yes_task="if_userdetails_isenabled_is_true_16",
            no_task="if_division_displaytext_blank_30",
        )

        if_userdetails_isenabled_is_true_16 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_16',
            test=lambda: (rail.result('bulk_get_users3_9')[
                0]['userDetails']['isEnabled']),
            yes_task="disable_login_17",
            no_task="if_userdetails_isenabled_is_not_true_24",
        )

        disable_login_17 = rail.RepliconServiceOperator(
            task_id='disable_login_17',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_enddate_18 = rail.PythonOperator(
            task_id='log_enddate_18',
            python_callable=lambda: datetime.now().strftime(
                "%m%d") + (datetime.now().strftime("%Y"))[2:4]
        )

        updateloginname_19 = rail.RepliconServiceOperator(
            task_id='updateloginname_19',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ result('bulk_get_users3_9')[0].securityConfiguration.loginName }}{{ result('log_enddate_18') }}",
                "password": "Replicon@12#",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        trigger_child_timesheet_recalculation = rail.TriggerDagRunOperator(
            task_id='trigger_child_timesheet_recalculation',
            retries=0,
            trigger_dag_id=f'michaelkorstna_austria_user_import_timesheet_recalculation_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "callerjobid": "{{dag_run.conf.callerjobid}}",
                "userloginname": "{{ dag_run.conf.employeeid }}",
                "useruri": "{{ dag_run.conf.useruri }}"
            }
        )

        wait_for_child_timesheet_recalculation = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_timesheet_recalculation',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_timesheet_recalculation") }}'
        )

        trigger_child_add_user = rail.TriggerDagRunOperator(
            task_id='trigger_child_add_user',
            retries=0,
            trigger_dag_id=f'michaelkorstna_austria_user_import_add_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "hiredate": "{{ dag_run.conf.hiredate }}",
                "originalhiredate": "{{ dag_run.conf.originalhiredate }}",
                "businesstitle": "{{ dag_run.conf.businesstitle }}",
                "jobprofile": "{{ dag_run.conf.jobprofile }}",
                "jobprofilecode": "{{ dag_run.conf.jobprofilecode }}",
                "jobfamily": "{{ dag_run.conf.jobfamily }}",
                "jobfamilygroup": "{{ dag_run.conf.jobfamilygroup }}",
                "compensationgrade": "{{ dag_run.conf.compensationgrade }}",
                "costcenterid": "{{ dag_run.conf.costcenterid }}",
                "costcentername": "{{ dag_run.conf.costcentername }}",
                "costcenterhierarchy": "{{ dag_run.conf.costcenterhierarchy }}",
                "businessorganization": "{{ dag_run.conf.businessorganization }}",
                "country": "{{ dag_run.conf.country }}",
                "location": "{{ dag_run.conf.location }}",
                "locationtype": "{{ dag_run.conf.locationtype }}",
                "scheduledweeklyhours": "{{ dag_run.conf.scheduledweeklyhours }}",
                "defaultweeklyhours": "{{ dag_run.conf.defaultweeklyhours }}",
                "employeetype": "{{ dag_run.conf.employeetype }}",
                "contracttype": "{{ dag_run.conf.contracttype }}",
                "contractenddate": "{{ dag_run.conf.contractenddate }}",
                "collectiveagreement": "{{ dag_run.conf.collectiveagreement }}",
                "managerid": "{{ dag_run.conf.managerid }}",
                "workersmanager": "{{ dag_run.conf.workersmanager }}",
                "terminationdate": "{{ dag_run.conf.terminationdate }}",
                "lastdayofwork": "{{ dag_run.conf.lastdayofwork }}",
                "locationaddress": "{{ dag_run.conf.locationaddress }}",
                "workemail": "{{ dag_run.conf.workemail }}",
                "type": "Rehire",
                "departmenturi": "{{ dag_run.conf.departmenturi }}",
                "callerjobid": "{{dag_run.conf.callerjobid}}",
                "locationaddressuri": "{{ dag_run.conf.locationaddressuri }}",
                "lastdayofworkuri": "{{ dag_run.conf.lastdayofworkuri }}",
                "collectiveagreementuri": "{{ dag_run.conf.collectiveagreementuri }}",
                "contractenddateuri": "{{ dag_run.conf.contractenddateuri }}",
                "contracttypeuri": "{{ dag_run.conf.contracttypeuri }}",
                "defaultweeklyhoursuri": "{{ dag_run.conf.defaultweeklyhoursuri }}",
                "scheduledweeklyhoursuri": "{{ dag_run.conf.scheduledweeklyhoursuri }}",
                "compensationgradeuri": "{{ dag_run.conf.compensationgradeuri }}",
                "jobprofilecodeuri": "{{ dag_run.conf.jobprofilecodeuri }}",
                "jobprofileuri": "{{ dag_run.conf.jobprofileuri }}",
                "businesstitleuri": "{{ dag_run.conf.businesstitleuri }}",
                "originalhiredateuri": "{{ dag_run.conf.originalhiredateuri }}",
                "costcenteruri": "{{ dag_run.conf.costcenteruri }}",
                "locationuri": "{{ dag_run.conf.locationuri }}",
                "weeklyscheduleeffectivedate": "{{ dag_run.conf.weeklyscheduleeffectivedate }}",
                "weeklyscheduleuri": "{{ dag_run.conf.weeklyscheduleuri }}",
                "userimportlogtable": "{{dag_run.conf.userimportlogtable}}",
                "supervisorlookup": "{{dag_run.conf.supervisorlookup}}"
            }
        )

        wait_for_child_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_add_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_add_user") }}'
        )

        log_log_22 = rail.PythonOperator(
            task_id='log_log_22',
            python_callable=lambda: "Existing user profile found in " + rail.result('bulk_get_users3_9')[
                0]['divisionSchedule'][0]['division']['displayText'] + " country. Disabled the user and created a new user profile."
        )

        if_userdetails_isenabled_is_not_true_24 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_24',
            test=lambda: not(rail.result('bulk_get_users3_9')[
                0]['userDetails']['isEnabled']),
            yes_task="log_enddate_25",
            no_task="catch_error",
        )

        log_enddate_25 = rail.PythonOperator(
            task_id='log_enddate_25',
            python_callable=lambda:  datetime.now().strftime(
                "%m%d") + (datetime.now().strftime("%Y"))[2:4]
        )

        updateloginname_26 = rail.RepliconServiceOperator(
            task_id='updateloginname_26',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ result('bulk_get_users3_9')[0].securityConfiguration.loginName }}{{ result('log_enddate_25') }}",
                "password": "Replicon@12#",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        trigger_child_user_add = rail.TriggerDagRunOperator(
            task_id='trigger_child_user_add',
            retries=0,
            trigger_dag_id=f'michaelkorstna_austria_user_import_add_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "hiredate": "{{ dag_run.conf.hiredate }}",
                "originalhiredate": "{{ dag_run.conf.originalhiredate }}",
                "businesstitle": "{{ dag_run.conf.businesstitle }}",
                "jobprofile": "{{ dag_run.conf.jobprofile }}",
                "jobprofilecode": "{{ dag_run.conf.jobprofilecode }}",
                "jobfamily": "{{ dag_run.conf.jobfamily }}",
                "jobfamilygroup": "{{ dag_run.conf.jobfamilygroup }}",
                "compensationgrade": "{{ dag_run.conf.compensationgrade }}",
                "costcenterid": "{{ dag_run.conf.costcenterid }}",
                "costcentername": "{{ dag_run.conf.costcentername }}",
                "costcenterhierarchy": "{{ dag_run.conf.costcenterhierarchy }}",
                "businessorganization": "{{ dag_run.conf.businessorganization }}",
                "country": "{{ dag_run.conf.country }}",
                "location": "{{ dag_run.conf.location }}",
                "locationtype": "{{ dag_run.conf.locationtype }}",
                "scheduledweeklyhours": "{{ dag_run.conf.scheduledweeklyhours }}",
                "defaultweeklyhours": "{{ dag_run.conf.defaultweeklyhours }}",
                "employeetype": "{{ dag_run.conf.employeetype }}",
                "contracttype": "{{ dag_run.conf.contracttype }}",
                "contractenddate": "{{ dag_run.conf.contractenddate }}",
                "collectiveagreement": "{{ dag_run.conf.collectiveagreement }}",
                "managerid": "{{ dag_run.conf.managerid }}",
                "workersmanager": "{{ dag_run.conf.workersmanager }}",
                "terminationdate": "{{ dag_run.conf.terminationdate }}",
                "lastdayofwork": "{{ dag_run.conf.lastdayofwork }}",
                "locationaddress": "{{ dag_run.conf.locationaddress }}",
                "workemail": "{{ dag_run.conf.workemail }}",
                "type": "Rehire",
                "departmenturi": "{{ dag_run.conf.departmenturi }}",
                "callerjobid": "{{dag_run.conf.callerjobid}}",
                "locationaddressuri": "{{ dag_run.conf.locationaddressuri }}",
                "lastdayofworkuri": "{{ dag_run.conf.lastdayofworkuri }}",
                "collectiveagreementuri": "{{ dag_run.conf.collectiveagreementuri }}",
                "contractenddateuri": "{{ dag_run.conf.contractenddateuri }}",
                "contracttypeuri": "{{ dag_run.conf.contracttypeuri }}",
                "defaultweeklyhoursuri": "{{ dag_run.conf.defaultweeklyhoursuri }}",
                "scheduledweeklyhoursuri": "{{ dag_run.conf.scheduledweeklyhoursuri }}",
                "compensationgradeuri": "{{ dag_run.conf.compensationgradeuri }}",
                "jobprofilecodeuri": "{{ dag_run.conf.jobprofilecodeuri }}",
                "jobprofileuri": "{{ dag_run.conf.jobprofileuri }}",
                "businesstitleuri": "{{ dag_run.conf.businesstitleuri }}",
                "originalhiredateuri": "{{ dag_run.conf.originalhiredateuri }}",
                "costcenteruri": "{{ dag_run.conf.costcenteruri }}",
                "locationuri": "{{ dag_run.conf.locationuri }}",
                "weeklyscheduleeffectivedate": "{{ dag_run.conf.weeklyscheduleeffectivedate }}",
                "weeklyscheduleuri": "{{ dag_run.conf.weeklyscheduleuri }}",
                "userimportlogtable": "{{dag_run.conf.userimportlogtable}}",
                "supervisorlookup": "{{dag_run.conf.supervisorlookup}}"
            }
        )

        wait_for_child_user_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_user_add',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_user_add") }}'
        )

        log_log_28 = rail.PythonOperator(
            task_id='log_log_28',
            python_callable=lambda: "Existing user profile found in " + rail.result('bulk_get_users3_9')[
                0]['divisionSchedule'][0]['division']['displayText'] + " country. Disabled the user and created a new user profile."
        )

        if_division_displaytext_blank_30 = rail.IfOperator(
            task_id='if_division_displaytext_blank_30',
            test=lambda: not (rail.result('bulk_get_users3_9') and rail.result('bulk_get_users3_9')[0]['divisionSchedule'] and rail.result(
                'bulk_get_users3_9')[0]['divisionSchedule'][0]['division'] and rail.result(
                'bulk_get_users3_9')[0]['divisionSchedule'][0]['division']['displayText']),
            yes_task="gototask_check_user_not_enabled",
            no_task="goto_task_40",
        )

        goto_task_40 = rail.EmptyOperator(
            task_id = 'goto_task_40'
        )

        gototask_check_user_not_enabled = rail.EmptyOperator(
            task_id = 'gototask_check_user_not_enabled'
        )

        if_userdetails_isenabled_is_not_true_31 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_31',
            test=lambda: not(rail.result('bulk_get_users3_9')[
                0]['userDetails']['isEnabled']),
            yes_task="log_enddate_32",
            no_task="gototask_check_user_enabled",
        )

        gototask_check_user_enabled = rail.EmptyOperator(
            task_id = 'gototask_check_user_enabled'
        )

        log_enddate_32 = rail.PythonOperator(
            task_id='log_enddate_32',
            python_callable=lambda:  datetime.now().strftime(
                "%m%d") + (datetime.now().strftime("%Y"))[2:4]
        )

        updateloginname_33 = rail.RepliconServiceOperator(
            task_id='updateloginname_33',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ result('bulk_get_users3_9')[0].securityConfiguration.loginName }}{{ result('log_enddate_32') }}",
                "password": "Replicon@12#",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        trigger_child_to_add_user = rail.TriggerDagRunOperator(
            task_id='trigger_child_to_add_user',
            retries=0,
            trigger_dag_id=f'michaelkorstna_austria_user_import_add_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "hiredate": "{{ dag_run.conf.hiredate }}",
                "originalhiredate": "{{ dag_run.conf.originalhiredate }}",
                "businesstitle": "{{ dag_run.conf.businesstitle }}",
                "jobprofile": "{{ dag_run.conf.jobprofile }}",
                "jobprofilecode": "{{ dag_run.conf.jobprofilecode }}",
                "jobfamily": "{{ dag_run.conf.jobfamily }}",
                "jobfamilygroup": "{{ dag_run.conf.jobfamilygroup }}",
                "compensationgrade": "{{ dag_run.conf.compensationgrade }}",
                "costcenterid": "{{ dag_run.conf.costcenterid }}",
                "costcentername": "{{ dag_run.conf.costcentername }}",
                "costcenterhierarchy": "{{ dag_run.conf.costcenterhierarchy }}",
                "businessorganization": "{{ dag_run.conf.businessorganization }}",
                "country": "{{ dag_run.conf.country }}",
                "location": "{{ dag_run.conf.location }}",
                "locationtype": "{{ dag_run.conf.locationtype }}",
                "scheduledweeklyhours": "{{ dag_run.conf.scheduledweeklyhours }}",
                "defaultweeklyhours": "{{ dag_run.conf.defaultweeklyhours }}",
                "employeetype": "{{ dag_run.conf.employeetype }}",
                "contracttype": "{{ dag_run.conf.contracttype }}",
                "contractenddate": "{{ dag_run.conf.contractenddate }}",
                "collectiveagreement": "{{ dag_run.conf.collectiveagreement }}",
                "managerid": "{{ dag_run.conf.managerid }}",
                "workersmanager": "{{ dag_run.conf.workersmanager }}",
                "terminationdate": "{{ dag_run.conf.terminationdate }}",
                "lastdayofwork": "{{ dag_run.conf.lastdayofwork }}",
                "locationaddress": "{{ dag_run.conf.locationaddress }}",
                "workemail": "{{ dag_run.conf.workemail }}",
                "type": "Rehire",
                "departmenturi": "{{ dag_run.conf.departmenturi }}",
                "callerjobid": "{{dag_run.conf.callerjobid}}",
                "locationaddressuri": "{{ dag_run.conf.locationaddressuri }}",
                "lastdayofworkuri": "{{ dag_run.conf.lastdayofworkuri }}",
                "collectiveagreementuri": "{{ dag_run.conf.collectiveagreementuri }}",
                "contractenddateuri": "{{ dag_run.conf.contractenddateuri }}",
                "contracttypeuri": "{{ dag_run.conf.contracttypeuri }}",
                "defaultweeklyhoursuri": "{{ dag_run.conf.defaultweeklyhoursuri }}",
                "scheduledweeklyhoursuri": "{{ dag_run.conf.scheduledweeklyhoursuri }}",
                "compensationgradeuri": "{{ dag_run.conf.compensationgradeuri }}",
                "jobprofilecodeuri": "{{ dag_run.conf.jobprofilecodeuri }}",
                "jobprofileuri": "{{ dag_run.conf.jobprofileuri }}",
                "businesstitleuri": "{{ dag_run.conf.businesstitleuri }}",
                "originalhiredateuri": "{{ dag_run.conf.originalhiredateuri }}",
                "costcenteruri": "{{ dag_run.conf.costcenteruri }}",
                "locationuri": "{{ dag_run.conf.locationuri }}",
                "weeklyscheduleeffectivedate": "{{ dag_run.conf.weeklyscheduleeffectivedate }}",
                "weeklyscheduleuri": "{{ dag_run.conf.weeklyscheduleuri }}",
                "userimportlogtable": "{{dag_run.conf.userimportlogtable}}",
                "supervisorlookup": "{{dag_run.conf.supervisorlookup}}"
            }
        )

        wait_for_child_to_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_to_add_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_to_add_user") }}'
        )

        log_log_35 = rail.PythonOperator(
            task_id='log_log_35',
            python_callable=lambda: "Existing user profile found in " + (rail.result('bulk_get_users3_9')[
                0]['divisionSchedule'][0]['division']['displayText'] if rail.result('bulk_get_users3_9')[
                0]['divisionSchedule'] else '') + " country. Disabled the user and created a new user profile"
        )

        if_userdetails_isenabled_is_true_37 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_37',
            test=lambda: rail.result('bulk_get_users3_9')[
                0]['userDetails']['isEnabled'],
            yes_task="update_country_divison_38",
            no_task="if_userdetails_isenabled_is_not_true_rehire_40",
        )

        update_country_divison_38 = rail.RepliconServiceOperator(
            task_id='update_country_divison_38',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "division": {
                            "uri": null,
                            "parentUri": null,
                            "name": "{{ dag_run.conf.country }}"
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        insert_to_list_39 = rail.SetVariableOperator(
            task_id='insert_to_list_39',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Country assigned"
            }
        )

        if_userdetails_isenabled_is_not_true_rehire_40 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_rehire_40',
            test=lambda dag_run: not(rail.result('bulk_get_users3_9')[
                0]['userDetails']['isEnabled']) and not (dag_run.conf['terminationdate']),
            yes_task="if_enddate_day_blank_41",
            no_task="get_current_customfield_values",
        )

        if_enddate_day_blank_41 = rail.IfOperator(
            task_id='if_enddate_day_blank_41',
            test=lambda: not (rail.result('bulk_get_users3_9') and rail.result(
                'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['endDate'] and rail.result(
                'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['endDate']['day']),
            yes_task="michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_add_entry_297_297_42",
            no_task="log_enddate_44",
        )

        michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_add_entry_297_297_42 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_add_entry_297_297_42',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.employeeid}}",
                "action": "Rehire",
                "status": "Skipped",
                "details": "The existing profile doesn't have an end date in Replicon",
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"
            }
        )

        log_enddate_44 = rail.PythonOperator(
            task_id='log_enddate_44',
            python_callable=lambda: (str(rail.result('bulk_get_users3_9')[
                                     0]['userDetails']['employmentDateRange']['endDate']['year']))[2:4]
        )

        updateloginname_45 = rail.RepliconServiceOperator(
            task_id='updateloginname_45',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                #pylint: disable = line-too-long
                "loginName": "{{ result('bulk_get_users3_9')[0].securityConfiguration.loginName }}{{ result('bulk_get_users3_9')[0].userDetails.employmentDateRange.endDate.month }}{{ result('bulk_get_users3_9')[0].userDetails.employmentDateRange.endDate.day }}{{ result('log_enddate_44') }}",
                "password": "Replicon@12#",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        trigger_child_for_add_user = rail.TriggerDagRunOperator(
            task_id='trigger_child_for_add_user',
            retries=0,
            trigger_dag_id=f'michaelkorstna_austria_user_import_add_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "hiredate": "{{ dag_run.conf.hiredate }}",
                "originalhiredate": "{{ dag_run.conf.originalhiredate }}",
                "businesstitle": "{{ dag_run.conf.businesstitle }}",
                "jobprofile": "{{ dag_run.conf.jobprofile }}",
                "jobprofilecode": "{{ dag_run.conf.jobprofilecode }}",
                "jobfamily": "{{ dag_run.conf.jobfamily }}",
                "jobfamilygroup": "{{ dag_run.conf.jobfamilygroup }}",
                "compensationgrade": "{{ dag_run.conf.compensationgrade }}",
                "costcenterid": "{{ dag_run.conf.costcenterid }}",
                "costcentername": "{{ dag_run.conf.costcentername }}",
                "costcenterhierarchy": "{{ dag_run.conf.costcenterhierarchy }}",
                "businessorganization": "{{ dag_run.conf.businessorganization }}",
                "country": "{{ dag_run.conf.country }}",
                "location": "{{ dag_run.conf.location }}",
                "locationtype": "{{ dag_run.conf.locationtype }}",
                "scheduledweeklyhours": "{{ dag_run.conf.scheduledweeklyhours }}",
                "defaultweeklyhours": "{{ dag_run.conf.defaultweeklyhours }}",
                "employeetype": "{{ dag_run.conf.employeetype }}",
                "contracttype": "{{ dag_run.conf.contracttype }}",
                "contractenddate": "{{ dag_run.conf.contractenddate }}",
                "collectiveagreement": "{{ dag_run.conf.collectiveagreement }}",
                "managerid": "{{ dag_run.conf.managerid }}",
                "workersmanager": "{{ dag_run.conf.workersmanager }}",
                "terminationdate": "{{ dag_run.conf.terminationdate }}",
                "lastdayofwork": "{{ dag_run.conf.lastdayofwork }}",
                "locationaddress": "{{ dag_run.conf.locationaddress }}",
                "workemail": "{{ dag_run.conf.workemail }}",
                "type": "Rehire",
                "departmenturi": "{{ dag_run.conf.departmenturi }}",
                "callerjobid": "{{dag_run.conf.callerjobid}}",
                "locationaddressuri": "{{ dag_run.conf.locationaddressuri }}",
                "lastdayofworkuri": "{{ dag_run.conf.lastdayofworkuri }}",
                "collectiveagreementuri": "{{ dag_run.conf.collectiveagreementuri }}",
                "contractenddateuri": "{{ dag_run.conf.contractenddateuri }}",
                "contracttypeuri": "{{ dag_run.conf.contracttypeuri }}",
                "defaultweeklyhoursuri": "{{ dag_run.conf.defaultweeklyhoursuri }}",
                "scheduledweeklyhoursuri": "{{ dag_run.conf.scheduledweeklyhoursuri }}",
                "compensationgradeuri": "{{ dag_run.conf.compensationgradeuri }}",
                "jobprofilecodeuri": "{{ dag_run.conf.jobprofilecodeuri }}",
                "jobprofileuri": "{{ dag_run.conf.jobprofileuri }}",
                "businesstitleuri": "{{ dag_run.conf.businesstitleuri }}",
                "originalhiredateuri": "{{ dag_run.conf.originalhiredateuri }}",
                "costcenteruri": "{{ dag_run.conf.costcenteruri }}",
                "locationuri": "{{ dag_run.conf.locationuri }}",
                "weeklyscheduleeffectivedate": "{{ dag_run.conf.weeklyscheduleeffectivedate }}",
                "weeklyscheduleuri": "{{ dag_run.conf.weeklyscheduleuri }}",
                "userimportlogtable": "{{dag_run.conf.userimportlogtable}}",
                "supervisorlookup": "{{dag_run.conf.supervisorlookup}}"
            }
        )

        wait_for_child_for_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_for_add_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_for_add_user") }}'
        )

        def get_current_values_for_customfields():
            customfieldvalues = rail.result('bulk_get_users3_9')[
                0]['userDetails']['customFieldValues']
            scheduledweeklyhrs = list(filter(
                lambda field: field['customField']['displayText'] == 'Scheduled Weekly Hours', customfieldvalues))
            return {
                'lastdayofwork': rail.find_first_by_attr_and_get_attr(customfieldvalues, 'customField.displayText', "Last Day of Work", 'text', ''),
                'contractenddate': rail.find_first_by_attr_and_get_attr(customfieldvalues, 'customField.displayText', "Contract End Date", 'text', ''),
                'businesstitle': rail.find_first_by_attr_and_get_attr(customfieldvalues, 'customField.displayText', "Business Title", 'text', ''),
                'jobprofile': rail.find_first_by_attr_and_get_attr(customfieldvalues, 'customField.displayText', "Job Profile", 'text', ''),
                'defaultweeklyhours': rail.find_first_by_attr_and_get_attr(customfieldvalues, 'customField.displayText', "Default Weekly Hours", 'text', ''),
                'compensationgrade': rail.find_first_by_attr_and_get_attr(customfieldvalues, 'customField.displayText', "Compensation Grade", 'text', ''),
                'jobprofilecode': rail.find_first_by_attr_and_get_attr(customfieldvalues, 'customField.displayText', "Job Profile Code", 'text', ''),
                'contracttype': rail.find_first_by_attr_and_get_attr(customfieldvalues, 'customField.displayText', "Contract Type", 'text', ''),
                'collectiveagreement': rail.find_first_by_attr_and_get_attr(customfieldvalues, 'customField.displayText', "Collective Agreement", 'text', ''),
                'locationaddress': rail.find_first_by_attr_and_get_attr(customfieldvalues, 'customField.displayText', "Location Address", 'text', ''),
                'scheduledweeklyhours': float(scheduledweeklyhrs[0].get('number')) if scheduledweeklyhrs and scheduledweeklyhrs[0].get('number') else 0,
                'originalhiredate': rail.find_first_by_attr_and_get_attr(customfieldvalues, 'customField.displayText', "Original Hire Date", 'text', ''),
            }

        get_current_customfield_values = rail.PythonOperator(
            task_id='get_current_customfield_values',
            python_callable=get_current_values_for_customfields
        )

        if_request_lastdayofwork_present_49 = rail.IfOperator(
            task_id='if_request_lastdayofwork_present_49',
            test=lambda dag_run: dag_run.conf['lastdayofwork'] and (not(rail.result('get_current_customfield_values')['lastdayofwork']) or datetime.strptime(
                dag_run.conf['lastdayofwork'], '%Y-%m-%d') != datetime.strptime(rail.result('get_current_customfield_values')['lastdayofwork'], "%d/%m/%Y")),
            yes_task="invoke_custom_ruby_code_last_dayof_work_50",
            no_task="if_request_contractenddate_present_54",
        )

        def get_date_object(datestring):
            dateobj = datetime.strptime(datestring, '%Y-%m-%d')
            return {
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year
            }

        invoke_custom_ruby_code_last_dayof_work_50 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_last_dayof_work_50',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['lastdayofwork'])
        )

        update_date_value_last_dayof_work_51 = rail.RepliconServiceOperator(
            task_id='update_date_value_last_dayof_work_51',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.lastdayofworkuri }}",
                "value": {
                    "year": "{{ result('invoke_custom_ruby_code_last_dayof_work_50').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_last_dayof_work_50').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_last_dayof_work_50').day }}"
                }
            }
        )

        insert_to_list_52 = rail.SetVariableOperator(
            task_id='insert_to_list_52',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Last Day of Work updated"
            }
        )

        if_request_contractenddate_present_54 = rail.IfOperator(
            task_id='if_request_contractenddate_present_54',
            test=lambda dag_run: dag_run.conf['contractenddate'] and (not(rail.result(
                'get_current_customfield_values')['contractenddate']) or datetime.strptime(dag_run.conf['contractenddate'], '%Y-%m-%d') != datetime.strptime(
                rail.result('get_current_customfield_values')['contractenddate'], "%d/%m/%Y")),
            yes_task="invoke_custom_ruby_code_contract_end_date_55",
            no_task="if_userdetails_isenabled_is_true_disable_58",
        )

        invoke_custom_ruby_code_contract_end_date_55 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_contract_end_date_55',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['contractenddate'])
        )

        update_date_value_contract_end_date_56 = rail.RepliconServiceOperator(
            task_id='update_date_value_contract_end_date_56',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.contractenddateuri }}",
                "value": {
                    "year": "{{ result('invoke_custom_ruby_code_contract_end_date_55').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_contract_end_date_55').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_contract_end_date_55').day }}"
                }
            }
        )

        insert_to_list_57 = rail.SetVariableOperator(
            task_id='insert_to_list_57',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Contract End Date updated"
            }
        )

        if_userdetails_isenabled_is_true_disable_58 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_disable_58',
            test=lambda dag_run: rail.result('bulk_get_users3_9')[0]['userDetails']['isEnabled'] and dag_run.conf['terminationdate'],
            yes_task="invoke_custom_ruby_code_enddate_59",
            no_task="if_userdetails_isenabled_is_not_true_disable_67",
        )

        invoke_custom_ruby_code_enddate_59 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_enddate_59',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['terminationdate'])
        )

        update_end_dateon_profile_60 = rail.RepliconServiceOperator(
            task_id='update_end_dateon_profile_60',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('bulk_get_users3_9')[0].userDetails.employmentDateRange.startDate.year }}",
                        "month": "{{ result('bulk_get_users3_9')[0].userDetails.employmentDateRange.startDate.month }}",
                        "day": "{{ result('bulk_get_users3_9')[0].userDetails.employmentDateRange.startDate.day }}"
                    },
                    "endDate": {
                        "year": "{{ result('invoke_custom_ruby_code_enddate_59').year }}",
                        "month": "{{ result('invoke_custom_ruby_code_enddate_59').month }}",
                        "day": "{{ result('invoke_custom_ruby_code_enddate_59').day }}"
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_terminationdate_to_date_equals_to_todayto_date_61 = rail.IfOperator(
            task_id='if_terminationdate_to_date_equals_to_todayto_date_61',
            test=lambda dag_run: datetime.strptime(
                dag_run.conf['terminationdate'], "%Y-%m-%d") == datetime.strptime(datetime.now().strftime("%d/%m/%Y"), "%d/%m/%Y"),
            yes_task="trigger_child_annual_leave_timeoff_type_termination_proration_assignment",
            no_task="trigger_dag_run_live_michaelkorstna_child_timesheet_recalculation_v1_0async_64",
        )

        trigger_child_annual_leave_timeoff_type_termination_proration_assignment = rail.TriggerDagRunOperator(
            task_id='trigger_child_annual_leave_timeoff_type_termination_proration_assignment',
            retries=0,
            trigger_dag_id=f'michaelkorstna_austria_user_import_annual_leave_timeoff_type_termination_proration_assignment_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{dag_run.conf.callerjobid}}",
                "userloginname": "{{ dag_run.conf.employeeid }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "timeoffuri": "{{ result('log_annual_leavetimeoff_uri_14') }}",
                "timeofftype": "[AT] Annual leave",
                #pylint: disable = line-too-long
                "disabledate": "{{ result('invoke_custom_ruby_code_enddate_59').day }}/{{ result('invoke_custom_ruby_code_enddate_59').month }}/{{ result('invoke_custom_ruby_code_enddate_59').year }}"
            }
        )

        wait_for_child_annual_leave_timeoff_type_termination_proration_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_annual_leave_timeoff_type_termination_proration_assignment',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_annual_leave_timeoff_type_termination_proration_assignment") }}'
        )

        disable_login_63 = rail.RepliconServiceOperator(
            task_id='disable_login_63',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        trigger_dag_run_live_michaelkorstna_child_timesheet_recalculation_v1_0async_64 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_michaelkorstna_child_timesheet_recalculation_v1_0async_64',
            retries=0,
            trigger_dag_id=f'michaelkorstna_austria_user_import_timesheet_recalculation_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{dag_run.conf.callerjobid}}",
                "userloginname": "{{ dag_run.conf.employeeid }}",
                "useruri": "{{ dag_run.conf.useruri }}"
            }
        )

        wait_for_child_timesheetrecalculation = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_timesheetrecalculation',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_michaelkorstna_child_timesheet_recalculation_v1_0async_64") }}'
        )

        add_log_enddate_updated = rail.WriteLogOperator(
            task_id='add_log_enddate_updated',
            log="{{ dag_run.conf.userimportlogtable}}",
            message="na",
            severity="Success",
            properties={
                "loginname": "{{dag_run.conf.employeeid}}",
                "action": "Disable",
                "status": "Success",
                "details": "End date updated",
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"
            }
        )

        if_userdetails_isenabled_is_not_true_disable_67 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_disable_67',
            test=lambda dag_run: not(rail.result('bulk_get_users3_9')[
                0]['userDetails']['isEnabled']) and dag_run.conf['terminationdate'],
            yes_task="michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_add_entry_297_297_68",
            no_task="if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_70",
        )

        michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_add_entry_297_297_68 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_add_entry_297_297_68',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.employeeid}}",
                "action": "Disable",
                "status": "Skipped",
                "details": "User already disabled",
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"
            }
        )

        if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_70 = rail.IfOperator(
            task_id='if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_70',
            test=lambda dag_run: dag_run.conf['firstname'] and (rail.result('bulk_get_users3_9')[
                0]['userDetails']['firstName']).lower() != (dag_run.conf['firstname']).lower(),
            yes_task="update_first_name_71",
            no_task="if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_73",
        )

        update_first_name_71 = rail.RepliconServiceOperator(
            task_id='update_first_name_71',
            endpoint="/services/userService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        insert_to_list_72 = rail.SetVariableOperator(
            task_id='insert_to_list_72',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "First name updated"
            }
        )

        if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_73 = rail.IfOperator(
            task_id='if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_73',
            test=lambda dag_run: dag_run.conf['lastname'] and (rail.result('bulk_get_users3_9')[
                0]['userDetails']['lastName']).lower() != (dag_run.conf['lastname']).lower(),
            yes_task="update_last_name_74",
            no_task="if_request_workemail_present_76",
        )

        update_last_name_74 = rail.RepliconServiceOperator(
            task_id='update_last_name_74',
            endpoint="/services/userService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        insert_to_list_75 = rail.SetVariableOperator(
            task_id='insert_to_list_75',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Last name updated"
            }
        )

        if_request_workemail_present_76 = rail.IfOperator(
            task_id='if_request_workemail_present_76',
            test=lambda dag_run: dag_run.conf['workemail'] and ((dag_run.conf['workemail']).lower() != (rail.result(
                'bulk_get_users3_9')[0]['userDetails']['emailAddress'] if rail.result('bulk_get_users3_9') and rail.result(
                'bulk_get_users3_9')[0]['userDetails']['emailAddress'] else '').lower()),
            yes_task="update_email_77",
            no_task="if_request_businesstitle_present_80",
        )

        update_email_77 = rail.RepliconServiceOperator(
            task_id='update_email_77',
            endpoint="/services/userService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.workemail }}"
            }
        )

        insert_to_list_78 = rail.SetVariableOperator(
            task_id='insert_to_list_78',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Email updated"
            }
        )

        if_request_businesstitle_present_80 = rail.IfOperator(
            task_id='if_request_businesstitle_present_80',
            test=lambda dag_run: dag_run.conf['businesstitle'] and (dag_run.conf['businesstitle']).lower(
            ) != (rail.result('get_current_customfield_values')['businesstitle']).lower(),
            yes_task="update_text_value_business_title_81",
            no_task="if_request_jobprofile_present_87",
        )

        update_text_value_business_title_81 = rail.RepliconServiceOperator(
            task_id='update_text_value_business_title_81',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.businesstitleuri }}",
                "value": "{{ dag_run.conf.businesstitle }}"
            }
        )

        insert_to_list_82 = rail.SetVariableOperator(
            task_id='insert_to_list_82',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Business Title updated"
            }
        )

        if_request_jobprofile_present_87 = rail.IfOperator(
            task_id='if_request_jobprofile_present_87',
            test=lambda dag_run: dag_run.conf['jobprofile'] and ((dag_run.conf['jobprofile']).lower(
            ) != (rail.result('get_current_customfield_values')['jobprofile']).lower()),
            yes_task="update_text_value_job_profile_88",
            no_task="if_request_defaultweeklyhours_present_91",
        )

        update_text_value_job_profile_88 = rail.RepliconServiceOperator(
            task_id='update_text_value_job_profile_88',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.jobprofileuri }}",
                "value": "{{ dag_run.conf.jobprofile }}"
            }
        )

        insert_to_list_89 = rail.SetVariableOperator(
            task_id='insert_to_list_89',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Job Profile updated"
            }
        )

        if_request_defaultweeklyhours_present_91 = rail.IfOperator(
            task_id='if_request_defaultweeklyhours_present_91',
            test=lambda dag_run: dag_run.conf['defaultweeklyhours'] and ((dag_run.conf['defaultweeklyhours']).lower(
            ) != (rail.result('get_current_customfield_values')['defaultweeklyhours']).lower()),
            yes_task="update_text_value_default_weekly_hours_92",
            no_task="if_request_compensationgrade_present_95",
        )

        update_text_value_default_weekly_hours_92 = rail.RepliconServiceOperator(
            task_id='update_text_value_default_weekly_hours_92',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.defaultweeklyhoursuri }}",
                "value": "{{ dag_run.conf.defaultweeklyhours }}"
            }
        )

        insert_to_list_93 = rail.SetVariableOperator(
            task_id='insert_to_list_93',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Default Weekly Hours updated"
            }
        )

        if_request_compensationgrade_present_95 = rail.IfOperator(
            task_id='if_request_compensationgrade_present_95',
            test=lambda dag_run: dag_run.conf['compensationgrade'] and (dag_run.conf['compensationgrade']).lower(
            ) != (rail.result('get_current_customfield_values')['compensationgrade']).lower(),
            yes_task="update_text_value_compensation_grade_96",
            no_task="if_request_jobprofilecode_present_99",
        )

        update_text_value_compensation_grade_96 = rail.RepliconServiceOperator(
            task_id='update_text_value_compensation_grade_96',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.compensationgradeuri }}",
                "value": "{{ dag_run.conf.compensationgrade }}"
            }
        )

        insert_to_list_97 = rail.SetVariableOperator(
            task_id='insert_to_list_97',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Compensation Grade updated"
            }
        )

        if_request_jobprofilecode_present_99 = rail.IfOperator(
            task_id='if_request_jobprofilecode_present_99',
            test=lambda dag_run: dag_run.conf['jobprofilecode'] and ((dag_run.conf['jobprofilecode']).lower(
            ) != (rail.result('get_current_customfield_values')['jobprofilecode']).lower()),
            yes_task="update_text_value_job_profile_code_100",
            no_task="if_request_contracttype_present_103",
        )

        update_text_value_job_profile_code_100 = rail.RepliconServiceOperator(
            task_id='update_text_value_job_profile_code_100',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.jobprofilecodeuri }}",
                "value": "{{ dag_run.conf.jobprofilecode }}"
            }
        )

        insert_to_list_101 = rail.SetVariableOperator(
            task_id='insert_to_list_101',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "HRM Name updated"
            }
        )

        if_request_contracttype_present_103 = rail.IfOperator(
            task_id='if_request_contracttype_present_103',
            test=lambda dag_run: dag_run.conf['contracttype'] and ((dag_run.conf['contracttype']).lower(
            ) != (rail.result('get_current_customfield_values')['contracttype']).lower()),
            yes_task="update_text_value_contract_type_104",
            no_task="if_request_collectiveagreement_present_107",
        )

        update_text_value_contract_type_104 = rail.RepliconServiceOperator(
            task_id='update_text_value_contract_type_104',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.contracttypeuri }}",
                "value": "{{ dag_run.conf.contracttype }}"
            }
        )

        insert_to_list_105 = rail.SetVariableOperator(
            task_id='insert_to_list_105',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Contract Type updated"
            }
        )

        if_request_collectiveagreement_present_107 = rail.IfOperator(
            task_id='if_request_collectiveagreement_present_107',
            test=lambda dag_run: dag_run.conf['collectiveagreement'] and ((dag_run.conf['collectiveagreement']).lower(
            ) != (rail.result('get_current_customfield_values')['collectiveagreement']).lower()),
            yes_task="update_text_value_collective_agreement_108",
            no_task="if_request_locationaddress_present_111",
        )

        update_text_value_collective_agreement_108 = rail.RepliconServiceOperator(
            task_id='update_text_value_collective_agreement_108',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.collectiveagreementuri }}",
                "value": "{{ dag_run.conf.collectiveagreement }}"
            }
        )

        insert_to_list_109 = rail.SetVariableOperator(
            task_id='insert_to_list_109',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Collective Agreement updated"
            }
        )

        if_request_locationaddress_present_111 = rail.IfOperator(
            task_id='if_request_locationaddress_present_111',
            test=lambda dag_run: dag_run.conf['locationaddress'] and ((dag_run.conf['locationaddress']).lower(
            ) != (rail.result('get_current_customfield_values')['locationaddress']).lower()),
            yes_task="update_text_value_location_address_112",
            no_task="if_request_scheduledweeklyhours_present_115",
        )

        update_text_value_location_address_112 = rail.RepliconServiceOperator(
            task_id='update_text_value_location_address_112',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.locationaddressuri }}",
                "value": "{{ dag_run.conf.locationaddress }}"
            }
        )

        insert_to_list_113 = rail.SetVariableOperator(
            task_id='insert_to_list_113',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Collective Agreement updated"
            }
        )

        if_request_scheduledweeklyhours_present_115 = rail.IfOperator(
            task_id='if_request_scheduledweeklyhours_present_115',
            test=lambda dag_run: dag_run.conf['scheduledweeklyhours'] and (float(dag_run.conf['scheduledweeklyhours']) != float(rail.result(
                'get_current_customfield_values')['scheduledweeklyhours'])),
            yes_task="update_numeric_value_scheduled_weekly_hours_116",
            no_task="if_request_originalhiredate_present_147",
        )

        update_numeric_value_scheduled_weekly_hours_116 = rail.RepliconServiceOperator(
            task_id='update_numeric_value_scheduled_weekly_hours_116',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.scheduledweeklyhoursuri }}",
                "value": "{{ dag_run.conf.scheduledweeklyhours }}"
            }
        )

        insert_to_list_117 = rail.SetVariableOperator(
            task_id='insert_to_list_117',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Scheduled Weekly Hours updated"
            }
        )

        trigger_child_timeoff_type_proration_assignment = rail.TriggerDagRunOperator(
            task_id='trigger_child_timeoff_type_proration_assignment',
            retries=0,
            trigger_dag_id=f'michaelkorstna_austria_user_import_timeoff_type_proration_assignment_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "callerjobid": dag_run.conf['callerjobid'],
                "userloginname": dag_run.conf['employeeid'],
                "useruri": dag_run.conf['useruri'],
                "startdate": datetime.now().strftime("%d/%m/%Y"),
                "type": "Update",
                "timeoffuri": rail.result('log_annual_leavetimeoff_uri_14'),
                "scheduledweeklyhours": (38.5 if float(dag_run.conf['scheduledweeklyhours']) >= 38.5 else dag_run.conf[
                    'scheduledweeklyhours']) if dag_run.conf['scheduledweeklyhours'] else 38.5,
                "fullpart": ("Full Time" if float(dag_run.conf['scheduledweeklyhours']) >= 40 else 'Part Time') if dag_run.conf[
                    'scheduledweeklyhours'] else 'Full Time',
                "timeofftype": "[AT] Annual leave",
                "actualstartdate": rail.result('log_startdate_10')
            }
        )

        wait_for_child_timeoff_type_proration_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_timeoff_type_proration_assignment',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_timeoff_type_proration_assignment") }}'
        )

        insert_to_list_119 = rail.SetVariableOperator(
            task_id='insert_to_list_119',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "[AT] Annual leave policy updated"
            }
        )

        if_request_weeklyscheduleuri_present_120 = rail.IfOperator(
            task_id='if_request_weeklyscheduleuri_present_120',
            test='''{{ dag_run.conf.weeklyscheduleuri | is_truthy }}''',
            yes_task="declare_list_121",
            no_task="if_request_originalhiredate_present_147",
        )

        declare_list_121 = rail.SetVariableOperator(
            task_id='declare_list_121',
            append=False,
            name='Weekly_schedule_(ServiceCentre)',
            value=[]
        )

        declare_list_122 = rail.SetVariableOperator(
            task_id='declare_list_122',
            append=False,
            name='Weekly_schedule_(ServiceCentre)_list',
            value=[]
        )

        if_log_weeklyschedule_service_centreschedule_123_contains_urn_124 = rail.IfOperator(
            task_id='if_log_weeklyschedule_service_centreschedule_123_contains_urn_124',
            test=lambda: 'urn' in json.dumps(rail.result('bulk_get_users3_9')[0]['serviceCenterSchedule']),
            yes_task="foreach_document_126",
            no_task="if_first_uri_present_136",
        )

        foreach_document_126 = rail.ForEachOperator(
            task_id='foreach_document_126',
            items=lambda: rail.result('bulk_get_users3_9')[
                0]['serviceCenterSchedule'],
            start_task='if_effectivedate_day_blank_127',
            end_task='foreach_document_126_end'
        )

        if_effectivedate_day_blank_127 = rail.IfOperator(
            task_id='if_effectivedate_day_blank_127',
            test=lambda: not (rail.result('foreach_document_126')[
                    'effectiveDate'] and rail.result('foreach_document_126')['effectiveDate']['day']),
            yes_task="insert_to_list_128",
            no_task="log_effectivedate_131",
        )


        insert_to_list_128 = rail.SetVariableOperator(
            task_id='insert_to_list_128',
            append=True,
            name='{{ result("declare_list_121").name }}',
            value=lambda: {
                "uri": rail.result('foreach_document_126')['serviceCenter']['uri'],
                "effectivedate": (datetime.strptime(get_date_string(rail.result(
                    'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']), "%d/%m/%Y")).strftime("%Y-%m-%d"),
                "name": ''
            }
        )

        insert_to_list_129 = rail.SetVariableOperator(
            task_id='insert_to_list_129',
            append=True,
            name='{{ result("declare_list_122").name }}',
            value={
                "effectiveDate": null,
                "serviceCenter": {
                    "uri": "{{ result('foreach_document_126').serviceCenter.uri }}",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        log_effectivedate_131 = rail.PythonOperator(
            task_id='log_effectivedate_131',
            python_callable=lambda: (datetime.strptime(get_date_string(rail.result(
                'foreach_document_126')['effectiveDate']), '%d/%m/%Y')).strftime("%Y-%m-%d")
        )

        if_to_date_less_than_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date1days_132 = rail.IfOperator(
            task_id='if_to_date_less_than_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date1days_132',
            test=lambda dag_run: datetime.strptime(rail.result('log_effectivedate_131'), "%Y-%m-%d") < (
                datetime.strptime(dag_run.conf['weeklyscheduleeffectivedate'], "%Y-%m-%d") + timedelta(days=1)),
            yes_task="insert_to_list_133",
            no_task="if_to_date_not_equals_to_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date_134",
        )

        insert_to_list_133 = rail.SetVariableOperator(
            task_id='insert_to_list_133',
            append=True,
            name='{{ result("declare_list_121").name }}',
            value={
                "uri": "{{ result('foreach_document_126').serviceCenter.uri }}",
                "effectivedate": "{{result('log_effectivedate_131')}}",
                "name": ""
            }
        )

        if_to_date_not_equals_to_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date_134 = rail.IfOperator(
            task_id='if_to_date_not_equals_to_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date_134',
            test=lambda dag_run: datetime.strptime(rail.result('log_effectivedate_131'), "%Y-%m-%d") != (
                datetime.strptime(dag_run.conf['weeklyscheduleeffectivedate'], "%Y-%m-%d")),
            yes_task="insert_to_list_135",
            no_task="foreach_document_126_end",
        )

        insert_to_list_135 = rail.SetVariableOperator(
            task_id='insert_to_list_135',
            append=True,
            name='{{ result("declare_list_122").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('foreach_document_126').effectiveDate.year }}",
                    "month": "{{ result('foreach_document_126').effectiveDate.month }}",
                    "day": "{{ result('foreach_document_126').effectiveDate.day }}"
                },
                "serviceCenter": {
                    "uri": "{{ result('foreach_document_126').serviceCenter.uri }}",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        foreach_document_126_end = rail.EmptyOperator(
            task_id='foreach_document_126_end',
        )

        if_first_uri_present_136 = rail.IfOperator(
            task_id='if_first_uri_present_136',
            test=lambda: bool(rail.get_dag_run_var('Weekly_schedule_(ServiceCentre)')
                              and rail.get_dag_run_var('Weekly_schedule_(ServiceCentre)')[0]['uri']),
            yes_task="log_max_effectivedate_137",
            no_task="if_log_currentweeklyscheduleservicecenteruri_138_blank_140",
        )

        log_max_effectivedate_137 = rail.PythonOperator(
            task_id='log_max_effectivedate_137',
            python_callable=lambda: max(rail.get_dag_run_var(
                'Weekly_schedule_(ServiceCentre)'), key=lambda x: x['effectivedate'])
        )

        log_currentweeklyscheduleservicecenteruri_138 = rail.PythonOperator(
            task_id='log_currentweeklyscheduleservicecenteruri_138',
            python_callable=lambda: {
                'currentservicecenter': rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var(
                    'Weekly_schedule_(ServiceCentre)'), 'effectivedate', rail.result('log_max_effectivedate_137')['effectivedate'], 'uri', ''),
                'currentservicecentername': rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var(
                    'Weekly_schedule_(ServiceCentre)'), 'effectivedate', rail.result('log_max_effectivedate_137')['effectivedate'], 'name', ''),
            }
        )

        if_log_currentweeklyscheduleservicecenteruri_138_blank_140 = rail.IfOperator(
            task_id='if_log_currentweeklyscheduleservicecenteruri_138_blank_140',
            test=lambda dag_run: not(rail.result('log_currentweeklyscheduleservicecenteruri_138') and rail.result(
                'log_currentweeklyscheduleservicecenteruri_138')['currentservicecenter']) or (rail.result(
                'log_currentweeklyscheduleservicecenteruri_138')['currentservicecenter'] != dag_run.conf['weeklyscheduleuri']),
            yes_task="date_split_weeklyscheduledate_141",
            no_task="if_request_originalhiredate_present_147",
        )

        date_split_weeklyscheduledate_141 = rail.PythonOperator(
            task_id='date_split_weeklyscheduledate_141',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['weeklyscheduleeffectivedate'] if dag_run.conf['weeklyscheduleeffectivedate'] else datetime.now().strftime("%Y-%m-%d"))
        )

        insert_to_list_142 = rail.SetVariableOperator(
            task_id='insert_to_list_142',
            append=True,
            name='{{ result("declare_list_122").name }}',
            value={
                "effectiveDate": {
                    "year": "{{result('date_split_weeklyscheduledate_141').year}}",
                    "month": "{{result('date_split_weeklyscheduledate_141').month}}",
                    "day": "{{result('date_split_weeklyscheduledate_141').day}}"
                },
                "serviceCenter": {
                    "parentUri": null,
                    "uri": "{{ dag_run.conf.weeklyscheduleuri }}",
                    "name": null
                }
            }
        )

        log_weekly_schedule_143 = rail.PythonOperator(
            task_id='log_weekly_schedule_143',
            python_callable=lambda: json.dumps(rail.get_dag_run_var(
                'Weekly_schedule_(ServiceCentre)_list')).replace('effectiveDate":{}', 'effectiveDate":null')
        )

        put_service_center_schedule_for_user_weekly_scheduleupdate_144 = rail.RepliconServiceOperator(
            task_id='put_service_center_schedule_for_user_weekly_scheduleupdate_144',
            endpoint="/services/ServiceCenterService1.svc/PutServiceCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": json.loads(rail.result('log_weekly_schedule_143'))
            }
        )

        insert_to_list_145 = rail.SetVariableOperator(
            task_id='insert_to_list_145',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Weekly schedule updated"
            }
        )

        if_request_originalhiredate_present_147 = rail.IfOperator(
            task_id='if_request_originalhiredate_present_147',
            test=lambda dag_run: dag_run.conf['originalhiredate'] and ( not(rail.result(
                'get_current_customfield_values')['originalhiredate']) or (datetime.strptime(
                dag_run.conf['originalhiredate'], "%Y-%m-%d") != datetime.strptime(rail.result(
                'get_current_customfield_values')['originalhiredate'], "%d/%m/%Y"))),
            yes_task="invoke_custom_ruby_code_original_hire_date_148",
            no_task="if_request_managerid_present_151",
        )

        invoke_custom_ruby_code_original_hire_date_148 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_original_hire_date_148',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['originalhiredate'])
        )

        update_date_value_original_hire_date_149 = rail.RepliconServiceOperator(
            task_id='update_date_value_original_hire_date_149',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.originalhiredateuri }}",
                "value": {
                    "year": "{{ result('invoke_custom_ruby_code_original_hire_date_148').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_original_hire_date_148').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_original_hire_date_148').day }}"
                }
            }
        )

        insert_to_list_150 = rail.SetVariableOperator(
            task_id='insert_to_list_150',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Original Hire Date updated"
            }
        )

        if_request_managerid_present_151 = rail.IfOperator(
            task_id='if_request_managerid_present_151',
            test='''{{ dag_run.conf.managerid | is_truthy }}''',
            yes_task="if_request_employeeid_not_equals_to_dataworkato_servicereceive_requestrequestmanagerid_152",
            no_task="log_required_department_name_217",
        )

        if_request_employeeid_not_equals_to_dataworkato_servicereceive_requestrequestmanagerid_152 = rail.IfOperator(
            task_id='if_request_employeeid_not_equals_to_dataworkato_servicereceive_requestrequestmanagerid_152',
            test='''{{ dag_run.conf.employeeid != dag_run.conf.managerid }}''',
            yes_task="declare_list_153",
            no_task="if_request_employeeid_equals_to_dataworkato_servicereceive_requestrequestmanagerid_189",
        )

        declare_list_153 = rail.SetVariableOperator(
            task_id='declare_list_153',
            append=False,
            name='supevisorschedule',
            value=[]
        )

        if_log_supervisorschedule_154_contains_urn_155 = rail.IfOperator(
            task_id='if_log_supervisorschedule_154_contains_urn_155',
            test=lambda: 'urn' in json.dumps(rail.result('bulk_get_users3_9')[
                                             0]['supervisorAssignmentSchedule']),
            yes_task="foreach_document_157",
            no_task="if_first_uri_present_164",
        )

        foreach_document_157 = rail.ForEachOperator(
            task_id='foreach_document_157',
            items=lambda: rail.result('bulk_get_users3_9')[
                0]['supervisorAssignmentSchedule'],
            start_task='if_effectivedate_day_blank_158',
            end_task='foreach_document_157_end'
        )

        if_effectivedate_day_blank_158 = rail.IfOperator(
            task_id='if_effectivedate_day_blank_158',
            test=lambda: not (rail.result('foreach_document_157')[
                              'effectiveDate'] and rail.result('foreach_document_157')['effectiveDate']['day']),
            yes_task="insert_to_list_159",
            no_task="log_effectivedate_161",
        )

        insert_to_list_159 = rail.SetVariableOperator(
            task_id='insert_to_list_159',
            append=True,
            name='{{ result("declare_list_153").name }}',
            value=lambda: {
                "loginname": rail.result('foreach_document_157')['supervisor']['user']['loginName'],
                "uri": rail.result('foreach_document_157')['supervisor']['user']['uri'],
                "effectivedate": (datetime.strptime(get_date_string(rail.result(
                    'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']),"%d/%m/%Y")).strftime("%Y-%m-%d"),
                "name": rail.result('foreach_document_157')['supervisor']['displayText']
            }
        )

        log_effectivedate_161 = rail.PythonOperator(
            task_id='log_effectivedate_161',
            python_callable=lambda: (datetime.strptime(get_date_string(
                rail.result('foreach_document_157')['effectiveDate']),"%d/%m/%Y")).strftime("%Y-%m-%d")
        )

        if_to_time_less_than_todayto_time1days_162 = rail.IfOperator(
            task_id='if_to_time_less_than_todayto_time1days_162',
            test=lambda: datetime.strptime(rail.result('log_effectivedate_161'), "%Y-%m-%d") < (
                datetime.strptime(datetime.now().strftime("%d/%m/%Y"), "%d/%m/%Y") + timedelta(days=1)),
            yes_task="insert_to_list_163",
            no_task="foreach_document_157_end",
        )

        insert_to_list_163 = rail.SetVariableOperator(
            task_id='insert_to_list_163',
            append=True,
            name='{{ result("declare_list_153").name }}',
            value={
                "loginname": "{{ result('foreach_document_157').supervisor.user.loginName }}",
                "uri": "{{ result('foreach_document_157').supervisor.user.uri }}",
                "effectivedate": "{{result('log_effectivedate_161')}}",
                "name": "{{ result('foreach_document_157').supervisor.displayText }}"
            }
        )

        foreach_document_157_end = rail.EmptyOperator(
            task_id='foreach_document_157_end',
        )

        if_first_uri_present_164 = rail.IfOperator(
            task_id='if_first_uri_present_164',
            test=lambda: bool(rail.get_dag_run_var('supevisorschedule')),
            yes_task="log_max_effectivedate_165",
            no_task="if_log_currentsupervisorloginname_166_blank_167",
        )

        log_max_effectivedate_165 = rail.PythonOperator(
            task_id='log_max_effectivedate_165',
            python_callable=lambda: max(rail.get_dag_run_var(
                'supevisorschedule'), key=lambda x: x['effectivedate'])
        )

        log_currentsupervisorloginname_166 = rail.PythonOperator(
            task_id='log_currentsupervisorloginname_166',
            python_callable=lambda: (rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var(
                'supevisorschedule'), 'effectivedate', rail.result('log_max_effectivedate_165')['effectivedate'], 'loginname', '')).lower()
        )

        if_log_currentsupervisorloginname_166_blank_167 = rail.IfOperator(
            task_id='if_log_currentsupervisorloginname_166_blank_167',
            test="{{ result('log_currentsupervisorloginname_166') | is_falsy  or result('log_currentsupervisorloginname_166') != dag_run.conf.managerid }}",
            yes_task="search_users_168",
            no_task="if_request_employeeid_equals_to_dataworkato_servicereceive_requestrequestmanagerid_189",
        )

        def get_supervisor_uri_and_status(response, dag_run):
            users_found = response['rows']
            supervisor = {}
            for user in users_found:
                if user['cells'][0]['textValue'] == dag_run.conf['managerid']:
                    supervisor = user
                    break
            return {
                'uri': supervisor['cells'][0]['uri'] if supervisor else '',
                'status': supervisor['cells'][1]['textValue'] if supervisor else ''
            }

        search_users_168 = rail.RepliconServiceOperator(
            task_id='search_users_168',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": "{{ dag_run.conf.managerid }}",
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=get_supervisor_uri_and_status
        )

        if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisnotavailable_170 = rail.IfOperator(
            task_id='if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisnotavailable_170',
            test=lambda: not(rail.result('search_users_168')['uri']),
            yes_task="add_supervisor_assignment_to_queue",
            no_task="if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_173",
        )

        add_supervisor_assignment_to_queue = rail.WriteLogOperator(
            task_id='add_supervisor_assignment_to_queue',
            log="{{ dag_run.conf.supervisorlookup }}",
            message="na",
            severity="queued",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{ dag_run.conf.employeeid }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "supervisorloginname": "{{ dag_run.conf.managerid }}",
                "action": "update",
                "childjobid": "{{ dag_run_ecid() }}",
                "supervisoreffectivedate": '{{current_time("%d/%m/%Y")}}',
                "status": "queued",
                "supervisorusername": "{{ dag_run.conf.workersmanager }}",
                "country": "{{ dag_run.conf.country }}"
            }
        )

        if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_173 = rail.IfOperator(
            task_id='if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_173',
            test=lambda: 'False' in rail.result('search_users_168')['status'],
            yes_task="add_supervisorassignment_toqueue",
            no_task="if_log_5_present_175",
        )

        add_supervisorassignment_toqueue = rail.WriteLogOperator(
            task_id='add_supervisorassignment_toqueue',
            log="{{ dag_run.conf.supervisorlookup }}",
            message="na",
            severity="queued",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{ dag_run.conf.employeeid }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "supervisorloginname": "{{ dag_run.conf.managerid }}",
                "action": "update",
                "childjobid": "{{ dag_run_ecid() }}",
                "supervisoreffectivedate": '{{current_time("%d/%m/%Y")}}',
                "status": "queued",
                "supervisorusername": "{{ dag_run.conf.workersmanager }}",
                "country": "{{ dag_run.conf.country }}"
            }
        )

        if_log_5_present_175 = rail.IfOperator(
            task_id='if_log_5_present_175',
            test=lambda: bool(rail.result('search_users_168')[
                              'uri'] and 'True' in rail.result('search_users_168')['status']),
            yes_task="log_requiredsupervisorpermissiontoassigned_176",
            no_task="if_request_employeeid_equals_to_dataworkato_servicereceive_requestrequestmanagerid_189",
        )

        log_requiredsupervisorpermissiontoassigned_176 = rail.PythonOperator(
            task_id='log_requiredsupervisorpermissiontoassigned_176',
            python_callable=lambda: rail.smartjoin_by_delim([entry['value'] for entry in list(filter(
                lambda entry: entry['type'] == 'Permission' and entry['identifier__1'] == 'Supervisor',
                michael_kors_gmbh_user_sync_master_mapper_austria))], ';')
        )

        get_assigned_permission_sets_for_user2_177 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_177',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_168').uri }}"
            },
            data_handler=lambda response: {
                'managerpermission': rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.name', ''),
                'endusermanagerpermission': rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:user', 'permissionSet.name', ''),
                'schedulemanagementpermission': rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:schedule-management',
                    'permissionSet.name', '')
            }
        )

        if_required_permissions_not_present = rail.IfOperator(
            task_id='if_required_permissions_not_present',
            test=lambda: (rail.result('get_assigned_permission_sets_for_user2_177')['managerpermission'] not in rail.result(
                'log_requiredsupervisorpermissiontoassigned_176')) or (rail.result(
                'get_assigned_permission_sets_for_user2_177')['endusermanagerpermission'] not in rail.result(
                'log_requiredsupervisorpermissiontoassigned_176')) or (rail.result('get_assigned_permission_sets_for_user2_177')[
                'schedulemanagementpermission'] not in rail.result('log_requiredsupervisorpermissiontoassigned_176') or not (rail.result(
                'get_assigned_permission_sets_for_user2_177')['schedulemanagementpermission']) or not (rail.result(
                'get_assigned_permission_sets_for_user2_177')['endusermanagerpermission']) or not (rail.result(
                'get_assigned_permission_sets_for_user2_177')['managerpermission'])),
            yes_task="get_all_permission_sets_183",
            no_task="update_supervisor_assignment_schedule_over_date_range_187",
        )

        get_all_permission_sets_183 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_183',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        foreach_document_184 = rail.ForEachOperator(
            task_id='foreach_document_184',
            items=lambda: (rail.result(
                'log_requiredsupervisorpermissiontoassigned_176')).split(";"),
            start_task='assign_permission_set_to_user_manager_186',
            end_task='foreach_document_184_end'
        )

        assign_permission_set_to_user_manager_186 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_manager_186',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda: {
                "userUri": rail.result('search_users_168')['uri'],
                "permissionSetUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets_183'), 'name', rail.result(
                    'foreach_document_184'), 'uri', '')
            }
        )

        foreach_document_184_end = rail.EmptyOperator(
            task_id='foreach_document_184_end',
        )

        update_supervisor_assignment_schedule_over_date_range_187 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_187',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('search_users_168').uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('invoke_custom_ruby_code_todays_date_11').year }}",
                        "month": "{{ result('invoke_custom_ruby_code_todays_date_11').month }}",
                        "day": "{{ result('invoke_custom_ruby_code_todays_date_11').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        insert_to_list_188 = rail.SetVariableOperator(
            task_id='insert_to_list_188',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Supervisor updated"
            }
        )

        if_request_employeeid_equals_to_dataworkato_servicereceive_requestrequestmanagerid_189 = rail.IfOperator(
            task_id='if_request_employeeid_equals_to_dataworkato_servicereceive_requestrequestmanagerid_189',
            test='''{{ dag_run.conf.employeeid == dag_run.conf.managerid }}''',
            yes_task="insert_to_list_190",
            no_task="log_required_department_name_217",
        )

        insert_to_list_190 = rail.SetVariableOperator(
            task_id='insert_to_list_190',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": "Supervisor not assigned/updated since the user and manager IDs are same"
            }
        )

        log_required_department_name_217 = rail.PythonOperator(
            task_id='log_required_department_name_217',
            python_callable=lambda dag_run:  rail.smartjoin_by_delim(
                (dag_run.conf['jobfamilygroup'] + "/" + dag_run.conf['jobfamily']).split("/"), "/")
        )

        if_log_required_department_name_217_present_dataworkato_servicereceive_requestrequestsupervisorssoid_218 = rail.IfOperator(
            task_id='if_log_required_department_name_217_present_dataworkato_servicereceive_requestrequestsupervisorssoid_218',
            test='''{{ result('log_required_department_name_217') | is_truthy }}''',
            yes_task="log_required_department_nametoconsider_219",
            no_task="log_required_location_name_243",
        )

        log_required_department_nametoconsider_219 = rail.PythonOperator(
            task_id='log_required_department_nametoconsider_219',
            python_callable=lambda:  "Michael Kors/" +
            rail.result('log_required_department_name_217')
        )

        declare_list_220 = rail.SetVariableOperator(
            task_id='declare_list_220',
            append=False,
            name='departmentschedule',
            value=[]
        )

        declare_list_221 = rail.SetVariableOperator(
            task_id='declare_list_221',
            append=False,
            name='departmentlist',
            value=[]
        )


        if_log_departmentschedule_222_contains_urn_223 = rail.IfOperator(
            task_id='if_log_departmentschedule_222_contains_urn_223',
            test=lambda: 'urn' in json.dumps(rail.result('bulk_get_users3_9')[
                                             0]['departmentGroupSchedule']),
            yes_task="foreach_document_225",
            no_task="if_first_uri_present_235",
        )

        foreach_document_225 = rail.ForEachOperator(
            task_id='foreach_document_225',
            items=lambda: rail.result('bulk_get_users3_9')[
                0]['departmentGroupSchedule'],
            start_task='if_effectivedate_day_blank_226',
            end_task='foreach_document_225_end'
        )

        if_effectivedate_day_blank_226 = rail.IfOperator(
            task_id='if_effectivedate_day_blank_226',
            test=lambda: not(rail.result('foreach_document_225')[
                              'effectiveDate'] and rail.result('foreach_document_225')['effectiveDate']['day']),
            yes_task="insert_to_list_227",
            no_task="log_effectivedate_230",
        )

        insert_to_list_227 = rail.SetVariableOperator(
            task_id='insert_to_list_227',
            append=True,
            name='{{ result("declare_list_220").name }}',
            value=lambda: {
                "uri": rail.result('foreach_document_225')['departmentGroup']['uri'],
                "effectivedate": (datetime.strptime(get_date_string(rail.result(
                    'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']),"%d/%m/%Y")).strftime("%Y-%m-%d"),
                "name": rail.result('foreach_document_225')['departmentGroup']['displayText']
            }
        )

        insert_to_list_228 = rail.SetVariableOperator(
            task_id='insert_to_list_228',
            append=True,
            name='{{ result("declare_list_221").name }}',
            value={
                "effectiveDate": null,
                "departmentGroup": {
                    "parentUri": null,
                    "uri": "{{ result('foreach_document_225').departmentGroup.uri }}",
                    "name": null
                }
            }
        )

        log_effectivedate_230 = rail.PythonOperator(
            task_id='log_effectivedate_230',
            python_callable=lambda: (datetime.strptime(get_date_string(
                rail.result('foreach_document_225')['effectiveDate']),"%d/%m/%Y")).strftime("%Y-%m-%d")
        )

        if_to_date_less_than_todayto_date_231 = rail.IfOperator(
            task_id='if_to_date_less_than_todayto_date_231',
            test=lambda: datetime.strptime(rail.result(
                'log_effectivedate_230'), '%Y-%m-%d') < datetime.strptime(datetime.now().strftime("%d/%m/%Y"), "%d/%m/%Y"),
            yes_task="insert_to_list_232",
            no_task="if_to_date_not_equals_to_todayto_date_233",
        )

        insert_to_list_232 = rail.SetVariableOperator(
            task_id='insert_to_list_232',
            append=True,
            name='{{ result("declare_list_220").name }}',
            value={
                "uri": "{{ result('foreach_document_225').departmentGroup.uri }}",
                "effectivedate": "{{result('log_effectivedate_230')}}",
                "name": "{{ result('foreach_document_225').departmentGroup.displayText }}"
            }
        )

        if_to_date_not_equals_to_todayto_date_233 = rail.IfOperator(
            task_id='if_to_date_not_equals_to_todayto_date_233',
            test=lambda: datetime.strptime(rail.result(
                'log_effectivedate_230'), '%Y-%m-%d') != datetime.strptime(datetime.now().strftime("%d/%m/%Y"), "%d/%m/%Y"),
            yes_task="insert_to_list_234",
            no_task="foreach_document_225_end",
        )

        insert_to_list_234 = rail.SetVariableOperator(
            task_id='insert_to_list_234',
            append=True,
            name='{{ result("declare_list_221").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('foreach_document_225').effectiveDate.year }}",
                    "month": "{{ result('foreach_document_225').effectiveDate.month }}",
                    "day": "{{ result('foreach_document_225').effectiveDate.day }}"
                },
                "departmentGroup": {
                    "uri": "{{ result('foreach_document_225').departmentGroup.uri }}",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        foreach_document_225_end = rail.EmptyOperator(
            task_id='foreach_document_225_end',
        )

        if_first_uri_present_235 = rail.IfOperator(
            task_id='if_first_uri_present_235',
            test=lambda: bool(rail.get_dag_run_var('departmentschedule')),
            yes_task="log_max_effectivedate_236",
            no_task="if_log_currentdepartmentname_237_blank_238",
        )

        log_max_effectivedate_236 = rail.PythonOperator(
            task_id='log_max_effectivedate_236',
            python_callable=lambda: max(rail.get_dag_run_var(
                'departmentschedule'), key=lambda x: x['effectivedate'])
        )

        log_currentdepartmentname_237 = rail.PythonOperator(
            task_id='log_currentdepartmentname_237',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var(
                'departmentschedule'), 'effectivedate', rail.result('log_max_effectivedate_236')['effectivedate'], 'name', '')
        )

        if_log_currentdepartmentname_237_blank_238 = rail.IfOperator(
            task_id='if_log_currentdepartmentname_237_blank_238',
            test=lambda: not (rail.result('log_currentdepartmentname_237')) or ((rail.result('log_currentdepartmentname_237')).lower(
            ) != ((rail.result('log_required_department_name_217').split('/'))[-1]).lower()),
            yes_task="insert_to_list_239",
            no_task="log_required_location_name_243",
        )

        insert_to_list_239 = rail.SetVariableOperator(
            task_id='insert_to_list_239',
            append=True,
            name='{{ result("declare_list_221").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('invoke_custom_ruby_code_todays_date_11').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_todays_date_11').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_todays_date_11').day }}"
                },
                "departmentGroup": {
                    "uri": "{{ dag_run.conf.departmenturi }}",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        log_department_schedule_240 = rail.PythonOperator(
            task_id='log_department_schedule_240',
            python_callable=lambda: (json.dumps(rail.get_dag_run_var(
                'departmentlist'))).replace('effectiveDate":{}', 'effectiveDate":null')
        )

        put_department_group_schedule_for_user_241 = rail.RepliconServiceOperator(
            task_id='put_department_group_schedule_for_user_241',
            endpoint="/services/DepartmentGroupService1.svc/PutDepartmentGroupScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": json.loads(rail.result('log_department_schedule_240'))
            }
        )

        insert_to_list_242 = rail.SetVariableOperator(
            task_id='insert_to_list_242',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Department group updated"
            }
        )

        log_required_location_name_243 = rail.PythonOperator(
            task_id='log_required_location_name_243',
            python_callable=lambda dag_run: rail.smartjoin_by_delim(
                (dag_run.conf['businessorganization'] + "/" + dag_run.conf['location']).split("/"), "/")
        )

        if_log_required_location_name_243_present_dataworkato_servicereceive_requestrequestsupervisorssoid_244 = rail.IfOperator(
            task_id='if_log_required_location_name_243_present_dataworkato_servicereceive_requestrequestsupervisorssoid_244',
            test='''{{ result('log_required_location_name_243') | is_truthy }}''',
            yes_task="declare_list_245",
            no_task="log_required_cost_center_268",
        )

        declare_list_245 = rail.SetVariableOperator(
            task_id='declare_list_245',
            append=False,
            name='Locationschedule',
            value=[]
        )

        declare_list_246 = rail.SetVariableOperator(
            task_id='declare_list_246',
            append=False,
            name='locationlist',
            value=[]
        )


        if_log_locationschedule_247_contains_urn_248 = rail.IfOperator(
            task_id='if_log_locationschedule_247_contains_urn_248',
            test=lambda: 'urn' in json.dumps(rail.result(
                'bulk_get_users3_9')[0]['locationSchedule']),
            yes_task="foreach_document_250",
            no_task="if_first_uri_present_260",
        )


        foreach_document_250 = rail.ForEachOperator(
            task_id='foreach_document_250',
            items=lambda: rail.result('bulk_get_users3_9')[
                0]['locationSchedule'],
            start_task='if_effectivedate_day_blank_251',
            end_task='foreach_document_250_end'
        )

        if_effectivedate_day_blank_251 = rail.IfOperator(
            task_id='if_effectivedate_day_blank_251',
            test=lambda: not (rail.result('foreach_document_250')[
                              'effectiveDate'] and rail.result('foreach_document_250')['effectiveDate']['day']),
            yes_task="insert_to_list_252",
            no_task="log_effectivedate_255",
        )

        insert_to_list_252 = rail.SetVariableOperator(
            task_id='insert_to_list_252',
            append=True,
            name='{{ result("declare_list_245").name }}',
            value=lambda: {
                "uri": rail.result('foreach_document_250')['location']['uri'],
                "effectivedate": (datetime.strptime(get_date_string(rail.result(
                    'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']),"%d/%m/%Y")).strftime("%Y-%m-%d"),
                "name": rail.result('foreach_document_250')['location']['displayText']
            }
        )

        insert_to_list_253 = rail.SetVariableOperator(
            task_id='insert_to_list_253',
            append=True,
            name='{{ result("declare_list_246").name }}',
            value={
                "effectiveDate": null,
                "location": {
                    "parentUri": null,
                    "uri": "{{ result('foreach_document_250').location.uri }}",
                    "name": null
                }
            }
        )

        log_effectivedate_255 = rail.PythonOperator(
            task_id='log_effectivedate_255',
            python_callable=lambda: (datetime.strptime(get_date_string(
                rail.result('foreach_document_250')['effectiveDate']),"%d/%m/%Y")).strftime("%Y-%m-%d")
        )

        if_to_date_less_than_todayto_date_256 = rail.IfOperator(
            task_id='if_to_date_less_than_todayto_date_256',
            test=lambda: datetime.strptime(rail.result(
                'log_effectivedate_255'), "%Y-%m-%d") < datetime.strptime(datetime.now().strftime("%d/%m/%Y"), "%d/%m/%Y"),
            yes_task="insert_to_list_257",
            no_task="if_to_date_not_equals_to_todayto_date_258",
        )

        insert_to_list_257 = rail.SetVariableOperator(
            task_id='insert_to_list_257',
            append=True,
            name='{{ result("declare_list_245").name }}',
            value={
                "uri": "{{ result('foreach_document_250').location.uri }}",
                "effectivedate": "{{result('log_effectivedate_255')}}",
                "name": "{{ result('foreach_document_250').location.displayText }}"
            }
        )

        if_to_date_not_equals_to_todayto_date_258 = rail.IfOperator(
            task_id='if_to_date_not_equals_to_todayto_date_258',
            test=lambda: datetime.strptime(rail.result(
                'log_effectivedate_255'), "%Y-%m-%d") != datetime.strptime(datetime.now().strftime("%d/%m/%Y"), "%d/%m/%Y"),
            yes_task="insert_to_list_259",
            no_task="foreach_document_250_end",
        )

        insert_to_list_259 = rail.SetVariableOperator(
            task_id='insert_to_list_259',
            append=True,
            name='{{ result("declare_list_246").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('foreach_document_250').effectiveDate.year }}",
                    "month": "{{ result('foreach_document_250').effectiveDate.month }}",
                    "day": "{{ result('foreach_document_250').effectiveDate.day }}"
                },
                "location": {
                    "name": null,
                    "parentUri": null,
                    "uri": "{{ result('foreach_document_250').location.uri }}"
                }
            }
        )

        foreach_document_250_end = rail.EmptyOperator(
            task_id='foreach_document_250_end',
        )

        if_first_uri_present_260 = rail.IfOperator(
            task_id='if_first_uri_present_260',
            test=lambda: bool(rail.get_dag_run_var('Locationschedule')),
            yes_task="log_max_effectivedate_261",
            no_task="if_log_currentlocationname_262_blank_263",
        )

        log_max_effectivedate_261 = rail.PythonOperator(
            task_id='log_max_effectivedate_261',
            python_callable=lambda: max(rail.get_dag_run_var(
                'Locationschedule'), key=lambda x: x['effectivedate'])
        )

        log_currentlocationname_262 = rail.PythonOperator(
            task_id='log_currentlocationname_262',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var(
                'Locationschedule'), 'effectivedate', rail.result('log_max_effectivedate_261')['effectivedate'], 'name', '')
        )

        if_log_currentlocationname_262_blank_263 = rail.IfOperator(
            task_id='if_log_currentlocationname_262_blank_263',
            test=lambda: not (rail.result('log_currentlocationname_262')) or ((rail.result('log_currentlocationname_262')).lower(
            ) != (((rail.result('log_required_location_name_243')).split("/"))[-1]).lower()),
            yes_task="insert_to_list_264",
            no_task="log_required_cost_center_268",
        )

        insert_to_list_264 = rail.SetVariableOperator(
            task_id='insert_to_list_264',
            append=True,
            name='{{ result("declare_list_246").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('invoke_custom_ruby_code_todays_date_11').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_todays_date_11').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_todays_date_11').day }}"
                },
                "location": {
                    "uri": "{{ dag_run.conf.locationuri }}",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        log_location_schedule_265 = rail.PythonOperator(
            task_id='log_location_schedule_265',
            python_callable=lambda: (json.dumps(rail.get_dag_run_var('locationlist'))).replace(
                'effectiveDate":{}', 'effectiveDate":null')
        )

        put_location_schedule_for_user_266 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_266',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": json.loads(rail.result('log_location_schedule_265'))
            }
        )

        insert_to_list_267 = rail.SetVariableOperator(
            task_id='insert_to_list_267',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Location updated"
            }
        )

        log_required_cost_center_268 = rail.PythonOperator(
            task_id='log_required_cost_center_268',
            python_callable=lambda dag_run: rail.smartjoin_by_delim(
                ((dag_run.conf['costcenterhierarchy']) + "/" + dag_run.conf['costcenterid']).split("/"), "/")
        )

        if_log_required_cost_center_268_present_dataworkato_servicereceive_requestrequestsupervisorssoid_269 = rail.IfOperator(
            task_id='if_log_required_cost_center_268_present_dataworkato_servicereceive_requestrequestsupervisorssoid_269',
            test='''{{ result('log_required_cost_center_268') | is_truthy}}''',
            yes_task="declare_list_270",
            no_task="add_final_log_for_user",
        )

        declare_list_270 = rail.SetVariableOperator(
            task_id='declare_list_270',
            append=False,
            name='costcenterschedule',
            value=[]
        )

        declare_list_271 = rail.SetVariableOperator(
            task_id='declare_list_271',
            append=False,
            name='costcenterlist',
            value=[]
        )


        if_log_costcenterschedule_272_contains_urn_273 = rail.IfOperator(
            task_id='if_log_costcenterschedule_272_contains_urn_273',
            test=lambda: 'urn' in json.dumps(rail.result(
                'bulk_get_users3_9')[0]['costCenterSchedule']),
            yes_task="foreach_document_275",
            no_task="if_first_uri_present_285",
        )

        foreach_document_275 = rail.ForEachOperator(
            task_id='foreach_document_275',
            items=lambda: rail.result('bulk_get_users3_9')[
                0]['costCenterSchedule'],
            start_task='if_effectivedate_day_blank_276',
            end_task='foreach_document_275_end'
        )

        if_effectivedate_day_blank_276 = rail.IfOperator(
            task_id='if_effectivedate_day_blank_276',
            test=lambda: not (rail.result('foreach_document_275')[
                              'effectiveDate'] and rail.result('foreach_document_275')['effectiveDate']['day']),
            yes_task="insert_to_list_277",
            no_task="log_effectivedate_280",
        )

        insert_to_list_277 = rail.SetVariableOperator(
            task_id='insert_to_list_277',
            append=True,
            name='{{ result("declare_list_270").name }}',
            value=lambda: {
                "uri": rail.result('foreach_document_275')['costCenter']['uri'],
                "effectivedate": (datetime.strptime(get_date_string(rail.result(
                    'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']),"%d/%m/%Y")).strftime("%Y-%m-%d"),
                "name": rail.result('foreach_document_275')['costCenter']['displayText']
            }
        )

        insert_to_list_278 = rail.SetVariableOperator(
            task_id='insert_to_list_278',
            append=True,
            name='{{ result("declare_list_271").name }}',
            value={
                "costCenter": {
                    "parentUri": null,
                    "uri": "{{ result('foreach_document_275').costCenter.uri }}",
                    "name": null
                },
                "effectiveDate": null
            }
        )

        log_effectivedate_280 = rail.PythonOperator(
            task_id='log_effectivedate_280',
            python_callable=lambda:  (datetime.strptime(get_date_string(
                rail.result('foreach_document_275')['effectiveDate']),"%d/%m/%Y")).strftime("%Y-%m-%d")
        )

        if_to_date_less_than_todayto_date_281 = rail.IfOperator(
            task_id='if_to_date_less_than_todayto_date_281',
            test=lambda: datetime.strptime(rail.result(
                'log_effectivedate_280'), "%Y-%m-%d") < datetime.strptime(datetime.now().strftime("%d/%m/%Y"), "%d/%m/%Y"),
            yes_task="insert_to_list_282",
            no_task="if_to_date_not_equals_to_todayto_date_283",
        )

        insert_to_list_282 = rail.SetVariableOperator(
            task_id='insert_to_list_282',
            append=True,
            name='{{ result("declare_list_270").name }}',
            value={
                "uri": "{{ result('foreach_document_275').costCenter.uri }}",
                "effectivedate": "{{result('log_effectivedate_280')}}",
                "name": "{{ result('foreach_document_275').costCenter.displayText }}"
            }
        )

        if_to_date_not_equals_to_todayto_date_283 = rail.IfOperator(
            task_id='if_to_date_not_equals_to_todayto_date_283',
            test=lambda: datetime.strptime(rail.result(
                'log_effectivedate_280'), "%Y-%m-%d") != datetime.strptime(datetime.now().strftime("%d/%m/%Y"), "%d/%m/%Y"),
            yes_task="insert_to_list_284",
            no_task="foreach_document_275_end",
        )

        insert_to_list_284 = rail.SetVariableOperator(
            task_id='insert_to_list_284',
            append=True,
            name='{{ result("declare_list_271").name }}',
            value={
                "costCenter": {
                    "parentUri": null,
                    "uri": "{{ result('foreach_document_275').costCenter.uri }}",
                    "name": null
                },
                "effectiveDate": {
                    "year": "{{ result('foreach_document_275').effectiveDate.year }}",
                    "month": "{{ result('foreach_document_275').effectiveDate.month }}",
                    "day": "{{ result('foreach_document_275').effectiveDate.day }}"
                }
            }
        )

        foreach_document_275_end = rail.EmptyOperator(
            task_id='foreach_document_275_end',
        )

        if_first_uri_present_285 = rail.IfOperator(
            task_id='if_first_uri_present_285',
            test=lambda: bool(rail.get_dag_run_var('costcenterschedule')),
            yes_task="log_max_effectivedate_286",
            no_task="if_log_currentcostcentername_287_blank_288",
        )

        log_max_effectivedate_286 = rail.PythonOperator(
            task_id='log_max_effectivedate_286',
            python_callable=lambda: max(rail.get_dag_run_var(
                'costcenterschedule'), key=lambda x: x['effectivedate'])
        )

        log_currentcostcentername_287 = rail.PythonOperator(
            task_id='log_currentcostcentername_287',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var(
                'costcenterschedule'), 'effectivedate', rail.result('log_max_effectivedate_286')['effectivedate'], 'name', '')
        )

        if_log_currentcostcentername_287_blank_288 = rail.IfOperator(
            task_id='if_log_currentcostcentername_287_blank_288',
            test=lambda: not (rail.result('log_currentcostcentername_287')) or ((rail.result(
                'log_currentcostcentername_287')).lower() != (((rail.result('log_required_cost_center_268')).split("/"))[-1]).lower()),
            yes_task="insert_to_list_289",
            no_task="add_final_log_for_user",
        )

        insert_to_list_289 = rail.SetVariableOperator(
            task_id='insert_to_list_289',
            append=True,
            name='{{ result("declare_list_271").name }}',
            value={
                "costCenter": {
                    "parentUri": null,
                    "uri": "{{ dag_run.conf.costcenteruri }}",
                    "name": null
                },
                "effectiveDate": {
                    "year": "{{ result('invoke_custom_ruby_code_todays_date_11').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_todays_date_11').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_todays_date_11').day }}"
                }
            }
        )

        log_cost_center_schedule_290 = rail.PythonOperator(
            task_id='log_cost_center_schedule_290',
            python_callable=lambda: (json.dumps(rail.get_dag_run_var(
                'costcenterlist'))).replace('effectiveDate":{}', 'effectiveDate":null')
        )

        put_cost_center_schedule_for_user_291 = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_291',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": json.loads(rail.result('log_cost_center_schedule_290'))
            }
        )

        insert_to_list_292 = rail.SetVariableOperator(
            task_id='insert_to_list_292',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Cost center updated"
            }
        )

        def get_details_for_final_log():
            return (('Partialy updated ' + rail.smartjoin_by_delim([exception['value'] for exception in rail.get_dag_run_var(
                    'Exception')], ";")) if rail.get_dag_run_var('Exception') else ("Successfully updated" if rail.get_dag_run_var(
                    'logs') else "No change to the user record in Replicon"))

        add_final_log_for_user = rail.WriteLogOperator(
            task_id='add_final_log_for_user',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity=lambda: 'Exception' if rail.get_dag_run_var('Exception') else (
                'Success' if rail.get_dag_run_var('logs') else 'Skipped'),
            properties=lambda dag_run: {
                "loginname": dag_run.conf['employeeid'],
                "action": "Update",
                "status": 'Exception' if rail.get_dag_run_var('Exception') else ('Success' if rail.get_dag_run_var('logs') else 'Skipped'),
                "details": get_details_for_final_log(),
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        if_entry_col4_not_equals_to_nochangetotheuserrecordinreplicon_294 = rail.IfOperator(
            task_id='if_entry_col4_not_equals_to_nochangetotheuserrecordinreplicon_294',
            test=lambda: get_details_for_final_log() != "No change to the user record in Replicon",
            yes_task="trigger_timesheet_recalculation_child",
            no_task="catch_error",
        )

        trigger_timesheet_recalculation_child = rail.TriggerDagRunOperator(
            task_id='trigger_timesheet_recalculation_child',
            retries=0,
            trigger_dag_id=f'michaelkorstna_austria_user_import_timesheet_recalculation_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{dag_run.conf.callerjobid}}",
                "userloginname": "{{ dag_run.conf.employeeid }}",
                "useruri": "{{ dag_run.conf.useruri }}"
            }
        )

        wait_for_timesheet_recalculation_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_timesheet_recalculation_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_timesheet_recalculation_child") }}'
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
        )

        add_log_for_error = rail.WriteLogOperator(
            task_id='add_log_for_error',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                "loginname": "{{dag_run.conf.employeeid}}",
                "action": "Update",
                "status": "Error",
                "details": "{{get_error_message()}}",
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> declare_list_2
        declare_list_2 >> declare_list_3 >> michael_kors_gmbh_user_sync_master_mapper_austria_search_entries_5 >> if_first_id_blank_6
        if_first_id_blank_6 >> rail.Label(
            'Yes') >> michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_add_entry_297_297_7 >> catch_error
        if_first_id_blank_6 >> rail.Label(
            'No') >> bulk_get_users3_9 >> log_startdate_10 >> invoke_custom_ruby_code_todays_date_11 >> log_annual_leavetimeoff_uri_14
        log_annual_leavetimeoff_uri_14 >> if_division_displaytext_present_15
        if_division_displaytext_present_15 >> rail.Label(
            'Yes') >> if_userdetails_isenabled_is_true_16
        if_userdetails_isenabled_is_true_16 >> rail.Label('Yes') >> disable_login_17 >> log_enddate_18 >> updateloginname_19
        updateloginname_19 >> trigger_child_timesheet_recalculation
        trigger_child_timesheet_recalculation >> wait_for_child_timesheet_recalculation >> trigger_child_add_user >> wait_for_child_add_user
        wait_for_child_add_user >> log_log_22 >> catch_error
        if_userdetails_isenabled_is_true_16 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_24
        if_userdetails_isenabled_is_not_true_24 >> rail.Label(
            'Yes') >> log_enddate_25 >> updateloginname_26 >> trigger_child_user_add >> wait_for_child_user_add >> log_log_28 >> catch_error
        if_userdetails_isenabled_is_not_true_24 >> rail.Label(
            'No') >> catch_error
        if_division_displaytext_present_15 >> rail.Label(
            'No') >> if_division_displaytext_blank_30
        if_division_displaytext_blank_30 >> rail.Label(
            'Yes') >> gototask_check_user_not_enabled >> if_userdetails_isenabled_is_not_true_31
        if_userdetails_isenabled_is_not_true_31 >> rail.Label(
            'Yes') >> log_enddate_32 >> updateloginname_33 >> trigger_child_to_add_user >> wait_for_child_to_add_user >> log_log_35 >> catch_error
        if_userdetails_isenabled_is_not_true_31 >> rail.Label(
            'No') >> gototask_check_user_enabled >> if_userdetails_isenabled_is_true_37
        if_userdetails_isenabled_is_true_37 >> rail.Label(
            'Yes') >> update_country_divison_38 >> insert_to_list_39 >> if_userdetails_isenabled_is_not_true_rehire_40
        if_userdetails_isenabled_is_true_37 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_rehire_40
        if_division_displaytext_blank_30 >> rail.Label(
            'No') >> goto_task_40 >> if_userdetails_isenabled_is_not_true_rehire_40
        if_userdetails_isenabled_is_not_true_rehire_40 >> rail.Label(
            'Yes') >> if_enddate_day_blank_41
        if_enddate_day_blank_41 >> rail.Label(
            'Yes') >> michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_add_entry_297_297_42 >> catch_error
        if_enddate_day_blank_41 >> rail.Label(
            'No') >> log_enddate_44 >> updateloginname_45 >> trigger_child_for_add_user >> wait_for_child_for_add_user >> catch_error
        if_userdetails_isenabled_is_not_true_rehire_40 >> rail.Label(
            'No') >> get_current_customfield_values >> if_request_lastdayofwork_present_49
        if_request_lastdayofwork_present_49 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_last_dayof_work_50 >> update_date_value_last_dayof_work_51 >> insert_to_list_52
        insert_to_list_52 >> if_request_contractenddate_present_54
        if_request_lastdayofwork_present_49 >> rail.Label(
            'No') >> if_request_contractenddate_present_54
        if_request_contractenddate_present_54 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_contract_end_date_55 >> update_date_value_contract_end_date_56 >> insert_to_list_57
        insert_to_list_57 >> if_userdetails_isenabled_is_true_disable_58
        if_request_contractenddate_present_54 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_true_disable_58
        if_userdetails_isenabled_is_true_disable_58 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_enddate_59 >> update_end_dateon_profile_60 >> if_terminationdate_to_date_equals_to_todayto_date_61
        if_terminationdate_to_date_equals_to_todayto_date_61 >> rail.Label(
            'Yes') >> trigger_child_annual_leave_timeoff_type_termination_proration_assignment
        trigger_child_annual_leave_timeoff_type_termination_proration_assignment >> wait_for_child_annual_leave_timeoff_type_termination_proration_assignment
        wait_for_child_annual_leave_timeoff_type_termination_proration_assignment >> disable_login_63
        disable_login_63 >> trigger_dag_run_live_michaelkorstna_child_timesheet_recalculation_v1_0async_64
        if_terminationdate_to_date_equals_to_todayto_date_61 >> rail.Label(
            'No') >> trigger_dag_run_live_michaelkorstna_child_timesheet_recalculation_v1_0async_64 >> wait_for_child_timesheetrecalculation
        wait_for_child_timesheetrecalculation >> add_log_enddate_updated >> catch_error
        if_userdetails_isenabled_is_true_disable_58 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_disable_67
        if_userdetails_isenabled_is_not_true_disable_67 >> rail.Label(
            'Yes') >> michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_add_entry_297_297_68 >> catch_error
        if_userdetails_isenabled_is_not_true_disable_67 >> rail.Label(
            'No') >> if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_70
        if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_70 >> rail.Label(
            'Yes') >> update_first_name_71 >> insert_to_list_72 >> if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_73
        if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_70 >> rail.Label(
            'No') >> if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_73
        if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_73 >> rail.Label(
            'Yes') >> update_last_name_74 >> insert_to_list_75 >> if_request_workemail_present_76
        if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_73 >> rail.Label(
            'No') >> if_request_workemail_present_76
        if_request_workemail_present_76 >> rail.Label(
            'Yes') >> update_email_77 >> insert_to_list_78 >> if_request_businesstitle_present_80
        if_request_workemail_present_76 >> rail.Label(
            'No') >> if_request_businesstitle_present_80
        if_request_businesstitle_present_80 >> rail.Label(
            'Yes') >> update_text_value_business_title_81 >> insert_to_list_82 >> if_request_jobprofile_present_87
        if_request_businesstitle_present_80 >> rail.Label(
            'No') >> if_request_jobprofile_present_87
        if_request_jobprofile_present_87 >> rail.Label(
            'Yes') >> update_text_value_job_profile_88 >> insert_to_list_89 >> if_request_defaultweeklyhours_present_91
        if_request_jobprofile_present_87 >> rail.Label(
            'No') >> if_request_defaultweeklyhours_present_91
        if_request_defaultweeklyhours_present_91 >> rail.Label(
            'Yes') >> update_text_value_default_weekly_hours_92 >> insert_to_list_93 >> if_request_compensationgrade_present_95
        if_request_defaultweeklyhours_present_91 >> rail.Label(
            'No') >> if_request_compensationgrade_present_95
        if_request_compensationgrade_present_95 >> rail.Label(
            'Yes') >> update_text_value_compensation_grade_96 >> insert_to_list_97 >> if_request_jobprofilecode_present_99
        if_request_compensationgrade_present_95 >> rail.Label(
            'No') >> if_request_jobprofilecode_present_99
        if_request_jobprofilecode_present_99 >> rail.Label(
            'Yes') >> update_text_value_job_profile_code_100 >> insert_to_list_101 >> if_request_contracttype_present_103
        if_request_jobprofilecode_present_99 >> rail.Label(
            'No') >> if_request_contracttype_present_103
        if_request_contracttype_present_103 >> rail.Label(
            'Yes') >> update_text_value_contract_type_104 >> insert_to_list_105 >> if_request_collectiveagreement_present_107
        if_request_contracttype_present_103 >> rail.Label(
            'No') >> if_request_collectiveagreement_present_107
        if_request_collectiveagreement_present_107 >> rail.Label(
            'Yes') >> update_text_value_collective_agreement_108 >> insert_to_list_109 >> if_request_locationaddress_present_111
        if_request_collectiveagreement_present_107 >> rail.Label(
            'No') >> if_request_locationaddress_present_111
        if_request_locationaddress_present_111 >> rail.Label(
            'Yes') >> update_text_value_location_address_112 >> insert_to_list_113 >> if_request_scheduledweeklyhours_present_115
        if_request_locationaddress_present_111 >> rail.Label(
            'No') >> if_request_scheduledweeklyhours_present_115
        if_request_scheduledweeklyhours_present_115 >> rail.Label(
            'Yes') >> update_numeric_value_scheduled_weekly_hours_116 >> insert_to_list_117 >> trigger_child_timeoff_type_proration_assignment
        trigger_child_timeoff_type_proration_assignment >> wait_for_child_timeoff_type_proration_assignment >> insert_to_list_119
        insert_to_list_119 >> if_request_weeklyscheduleuri_present_120
        if_request_weeklyscheduleuri_present_120 >> rail.Label(
            'Yes') >> declare_list_121 >> declare_list_122 >> if_log_weeklyschedule_service_centreschedule_123_contains_urn_124
        if_log_weeklyschedule_service_centreschedule_123_contains_urn_124 >> rail.Label(
            'Yes') >> foreach_document_126 >> if_effectivedate_day_blank_127
        if_effectivedate_day_blank_127 >> rail.Label(
            'Yes') >> insert_to_list_128 >> insert_to_list_129 >> foreach_document_126_end
        if_effectivedate_day_blank_127 >> rail.Label(
            'No') >> log_effectivedate_131 >> if_to_date_less_than_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date1days_132
        if_to_date_less_than_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date1days_132 >> rail.Label(
            'Yes') >> insert_to_list_133 >> if_to_date_not_equals_to_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date_134
        if_to_date_less_than_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date1days_132 >> rail.Label(
            'No') >> if_to_date_not_equals_to_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date_134
        if_to_date_not_equals_to_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date_134 >> rail.Label(
            'Yes') >> insert_to_list_135 >> foreach_document_126_end
        if_to_date_not_equals_to_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date_134 >> rail.Label(
            'No') >> foreach_document_126_end
        foreach_document_126 >> foreach_document_126_end >> if_first_uri_present_136
        if_log_weeklyschedule_service_centreschedule_123_contains_urn_124 >> rail.Label(
            'No') >> if_first_uri_present_136
        if_first_uri_present_136 >> rail.Label(
            'Yes') >> log_max_effectivedate_137 >> log_currentweeklyscheduleservicecenteruri_138 >> if_log_currentweeklyscheduleservicecenteruri_138_blank_140
        if_first_uri_present_136 >> rail.Label(
            'No') >> if_log_currentweeklyscheduleservicecenteruri_138_blank_140
        if_log_currentweeklyscheduleservicecenteruri_138_blank_140 >> rail.Label(
            'Yes') >> date_split_weeklyscheduledate_141 >> insert_to_list_142 >> log_weekly_schedule_143
        log_weekly_schedule_143 >> put_service_center_schedule_for_user_weekly_scheduleupdate_144 >> insert_to_list_145
        insert_to_list_145 >> if_request_originalhiredate_present_147
        if_log_currentweeklyscheduleservicecenteruri_138_blank_140 >> rail.Label(
            'No') >> if_request_originalhiredate_present_147
        if_request_weeklyscheduleuri_present_120 >> rail.Label(
            'No') >> if_request_originalhiredate_present_147
        if_request_scheduledweeklyhours_present_115 >> rail.Label(
            'No') >> if_request_originalhiredate_present_147
        if_request_originalhiredate_present_147 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_original_hire_date_148 >> update_date_value_original_hire_date_149 >> insert_to_list_150
        insert_to_list_150 >> if_request_managerid_present_151
        if_request_originalhiredate_present_147 >> rail.Label(
            'No') >> if_request_managerid_present_151
        if_request_managerid_present_151 >> rail.Label(
            'Yes') >> if_request_employeeid_not_equals_to_dataworkato_servicereceive_requestrequestmanagerid_152
        if_request_employeeid_not_equals_to_dataworkato_servicereceive_requestrequestmanagerid_152 >> rail.Label(
            'Yes') >> declare_list_153 >> if_log_supervisorschedule_154_contains_urn_155
        if_log_supervisorschedule_154_contains_urn_155 >> rail.Label(
            'Yes') >> foreach_document_157 >> if_effectivedate_day_blank_158
        if_effectivedate_day_blank_158 >> rail.Label(
            'Yes') >> insert_to_list_159 >> foreach_document_157_end
        if_effectivedate_day_blank_158 >> rail.Label(
            'No') >> log_effectivedate_161 >> if_to_time_less_than_todayto_time1days_162
        if_to_time_less_than_todayto_time1days_162 >> rail.Label(
            'Yes') >> insert_to_list_163 >> foreach_document_157_end
        if_to_time_less_than_todayto_time1days_162 >> rail.Label(
            'No') >> foreach_document_157_end
        foreach_document_157 >> foreach_document_157_end >> if_first_uri_present_164
        if_log_supervisorschedule_154_contains_urn_155 >> rail.Label(
            'No') >> if_first_uri_present_164
        if_first_uri_present_164 >> rail.Label(
            'Yes') >> log_max_effectivedate_165 >> log_currentsupervisorloginname_166 >> if_log_currentsupervisorloginname_166_blank_167
        if_first_uri_present_164 >> rail.Label(
            'No') >> if_log_currentsupervisorloginname_166_blank_167
        if_log_currentsupervisorloginname_166_blank_167 >> rail.Label(
            'Yes') >> search_users_168 >> if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisnotavailable_170
        if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisnotavailable_170 >> rail.Label(
            'Yes') >> add_supervisor_assignment_to_queue
        add_supervisor_assignment_to_queue >> if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_173
        if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisnotavailable_170 >> rail.Label(
            'No') >> if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_173
        if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_173 >> rail.Label(
            'Yes') >> add_supervisorassignment_toqueue >> if_log_5_present_175
        if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_173 >> rail.Label(
            'No') >> if_log_5_present_175
        if_log_5_present_175 >> rail.Label(
            'Yes') >> log_requiredsupervisorpermissiontoassigned_176 >> get_assigned_permission_sets_for_user2_177
        get_assigned_permission_sets_for_user2_177 >> if_required_permissions_not_present
        if_required_permissions_not_present >> rail.Label(
            'Yes') >> get_all_permission_sets_183 >> foreach_document_184 >> assign_permission_set_to_user_manager_186 >> foreach_document_184_end
        foreach_document_184 >> foreach_document_184_end >> update_supervisor_assignment_schedule_over_date_range_187
        if_required_permissions_not_present >> rail.Label(
            'No') >> update_supervisor_assignment_schedule_over_date_range_187 >> insert_to_list_188
        insert_to_list_188 >> if_request_employeeid_equals_to_dataworkato_servicereceive_requestrequestmanagerid_189
        if_log_5_present_175 >> rail.Label(
            'No') >> if_request_employeeid_equals_to_dataworkato_servicereceive_requestrequestmanagerid_189
        if_log_currentsupervisorloginname_166_blank_167 >> rail.Label(
            'No') >> if_request_employeeid_equals_to_dataworkato_servicereceive_requestrequestmanagerid_189
        if_request_employeeid_not_equals_to_dataworkato_servicereceive_requestrequestmanagerid_152 >> rail.Label(
            'No') >> if_request_employeeid_equals_to_dataworkato_servicereceive_requestrequestmanagerid_189
        if_request_employeeid_equals_to_dataworkato_servicereceive_requestrequestmanagerid_189 >> rail.Label(
            'Yes') >> insert_to_list_190 >> log_required_department_name_217
        if_request_employeeid_equals_to_dataworkato_servicereceive_requestrequestmanagerid_189 >> rail.Label(
            'No') >> log_required_department_name_217
        if_request_managerid_present_151 >> rail.Label(
            'No') >> log_required_department_name_217
        log_required_department_name_217 >> if_log_required_department_name_217_present_dataworkato_servicereceive_requestrequestsupervisorssoid_218
        if_log_required_department_name_217_present_dataworkato_servicereceive_requestrequestsupervisorssoid_218 >> rail.Label(
            'Yes') >> log_required_department_nametoconsider_219 >> declare_list_220 >> declare_list_221 >> if_log_departmentschedule_222_contains_urn_223
        if_log_departmentschedule_222_contains_urn_223 >> rail.Label(
            'Yes') >> foreach_document_225 >> if_effectivedate_day_blank_226
        if_effectivedate_day_blank_226 >> rail.Label(
            'Yes') >> insert_to_list_227 >> insert_to_list_228 >> foreach_document_225_end
        if_effectivedate_day_blank_226 >> rail.Label(
            'No') >> log_effectivedate_230 >> if_to_date_less_than_todayto_date_231
        if_to_date_less_than_todayto_date_231 >> rail.Label(
            'Yes') >> insert_to_list_232 >> if_to_date_not_equals_to_todayto_date_233
        if_to_date_less_than_todayto_date_231 >> rail.Label(
            'No') >> if_to_date_not_equals_to_todayto_date_233
        if_to_date_not_equals_to_todayto_date_233 >> rail.Label(
            'Yes') >> insert_to_list_234 >> foreach_document_225_end
        if_to_date_not_equals_to_todayto_date_233 >> rail.Label(
            'No') >> foreach_document_225_end
        foreach_document_225 >> foreach_document_225_end >> if_first_uri_present_235
        if_log_departmentschedule_222_contains_urn_223 >> rail.Label(
            'No') >> if_first_uri_present_235
        if_first_uri_present_235 >> rail.Label(
            'Yes') >> log_max_effectivedate_236 >> log_currentdepartmentname_237 >> if_log_currentdepartmentname_237_blank_238
        if_first_uri_present_235 >> rail.Label(
            'No') >> if_log_currentdepartmentname_237_blank_238
        if_log_currentdepartmentname_237_blank_238 >> rail.Label(
            'Yes') >> insert_to_list_239 >> log_department_schedule_240 >> put_department_group_schedule_for_user_241 >> insert_to_list_242
        insert_to_list_242 >> log_required_location_name_243
        if_log_currentdepartmentname_237_blank_238 >> rail.Label(
            'No') >> log_required_location_name_243
        if_log_required_department_name_217_present_dataworkato_servicereceive_requestrequestsupervisorssoid_218 >> rail.Label(
            'No') >> log_required_location_name_243 >> if_log_required_location_name_243_present_dataworkato_servicereceive_requestrequestsupervisorssoid_244
        if_log_required_location_name_243_present_dataworkato_servicereceive_requestrequestsupervisorssoid_244 >> rail.Label(
            'Yes') >> declare_list_245 >> declare_list_246 >> if_log_locationschedule_247_contains_urn_248
        if_log_locationschedule_247_contains_urn_248 >> rail.Label(
            'Yes') >> foreach_document_250 >> if_effectivedate_day_blank_251
        if_effectivedate_day_blank_251 >> rail.Label(
            'Yes') >> insert_to_list_252 >> insert_to_list_253 >> foreach_document_250_end
        if_effectivedate_day_blank_251 >> rail.Label(
            'No') >> log_effectivedate_255 >> if_to_date_less_than_todayto_date_256
        if_to_date_less_than_todayto_date_256 >> rail.Label(
            'Yes') >> insert_to_list_257 >> if_to_date_not_equals_to_todayto_date_258
        if_to_date_less_than_todayto_date_256 >> rail.Label(
            'No') >> if_to_date_not_equals_to_todayto_date_258
        if_to_date_not_equals_to_todayto_date_258 >> rail.Label(
            'Yes') >> insert_to_list_259 >> foreach_document_250_end
        if_to_date_not_equals_to_todayto_date_258 >> rail.Label(
            'No') >> foreach_document_250_end
        foreach_document_250 >> foreach_document_250_end >> if_first_uri_present_260
        if_log_locationschedule_247_contains_urn_248 >> rail.Label(
            'No') >> if_first_uri_present_260
        if_first_uri_present_260 >> rail.Label(
            'Yes') >> log_max_effectivedate_261 >> log_currentlocationname_262 >> if_log_currentlocationname_262_blank_263
        if_first_uri_present_260 >> rail.Label(
            'No') >> if_log_currentlocationname_262_blank_263
        if_log_currentlocationname_262_blank_263 >> rail.Label(
            'Yes') >> insert_to_list_264 >> log_location_schedule_265 >> put_location_schedule_for_user_266 >> insert_to_list_267
        insert_to_list_267 >> log_required_cost_center_268
        if_log_currentlocationname_262_blank_263 >> rail.Label(
            'No') >> log_required_cost_center_268
        if_log_required_location_name_243_present_dataworkato_servicereceive_requestrequestsupervisorssoid_244 >> rail.Label(
            'No') >> log_required_cost_center_268 >> if_log_required_cost_center_268_present_dataworkato_servicereceive_requestrequestsupervisorssoid_269
        if_log_required_cost_center_268_present_dataworkato_servicereceive_requestrequestsupervisorssoid_269 >> rail.Label(
            'Yes') >> declare_list_270 >> declare_list_271 >> if_log_costcenterschedule_272_contains_urn_273
        if_log_costcenterschedule_272_contains_urn_273 >> rail.Label(
            'Yes') >> foreach_document_275 >> if_effectivedate_day_blank_276
        if_effectivedate_day_blank_276 >> rail.Label(
            'Yes') >> insert_to_list_277 >> insert_to_list_278 >> foreach_document_275_end
        if_effectivedate_day_blank_276 >> rail.Label(
            'No') >> log_effectivedate_280 >> if_to_date_less_than_todayto_date_281
        if_to_date_less_than_todayto_date_281 >> rail.Label(
            'Yes') >> insert_to_list_282 >> if_to_date_not_equals_to_todayto_date_283
        if_to_date_less_than_todayto_date_281 >> rail.Label(
            'No') >> if_to_date_not_equals_to_todayto_date_283
        if_to_date_not_equals_to_todayto_date_283 >> rail.Label(
            'Yes') >> insert_to_list_284 >> foreach_document_275_end
        if_to_date_not_equals_to_todayto_date_283 >> rail.Label(
            'No') >> foreach_document_275_end
        foreach_document_275 >> foreach_document_275_end >> if_first_uri_present_285
        if_log_costcenterschedule_272_contains_urn_273 >> rail.Label(
            'No') >> if_first_uri_present_285
        if_first_uri_present_285 >> rail.Label(
            'Yes') >> log_max_effectivedate_286 >> log_currentcostcentername_287 >> if_log_currentcostcentername_287_blank_288
        if_first_uri_present_285 >> rail.Label(
            'No') >> if_log_currentcostcentername_287_blank_288
        if_log_currentcostcentername_287_blank_288 >> rail.Label(
            'Yes') >> insert_to_list_289 >> log_cost_center_schedule_290 >> put_cost_center_schedule_for_user_291 >> insert_to_list_292
        insert_to_list_292 >> add_final_log_for_user
        if_log_currentcostcentername_287_blank_288 >> rail.Label(
            'No') >> add_final_log_for_user
        if_log_required_cost_center_268_present_dataworkato_servicereceive_requestrequestsupervisorssoid_269 >> rail.Label(
            'No') >> add_final_log_for_user >> if_entry_col4_not_equals_to_nochangetotheuserrecordinreplicon_294
        if_entry_col4_not_equals_to_nochangetotheuserrecordinreplicon_294 >> rail.Label(
            'Yes') >> trigger_timesheet_recalculation_child
        trigger_timesheet_recalculation_child >> wait_for_timesheet_recalculation_child >> catch_error
        if_entry_col4_not_equals_to_nochangetotheuserrecordinreplicon_294 >> rail.Label(
            'No') >> catch_error >> add_log_for_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
