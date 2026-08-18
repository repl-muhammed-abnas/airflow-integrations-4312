
from datetime import timedelta, datetime
import json
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import rail
from michaelkorstna.spain_user_import.utils import custom_methods

from michaelkorstna.spain_user_import.mappers.michael_kors_gmbh_user_sync_master_mapper_spain import michael_kors_gmbh_user_sync_master_mapper_spain
from michaelkorstna.spain_user_import.mappers.michaelkorstna_schedulemapper_spain_mapper import michaelkorstna_schedulemapper_spain

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_spain_user_update_child_{config.instance}',
        description=f'MichaelKorsTnA Spain User Update V1.0 {config.instance}',
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

        declare_variable_4=rail.SetVariableOperator(
            task_id='declare_variable_4',
            append=False,
            name='timeofftrigger',
            value=None
        )


        michael_kors_gmbh_user_sync_master_mapper_spain_search_entries_5 = rail.PythonOperator(
            task_id='michael_kors_gmbh_user_sync_master_mapper_spain_search_entries_5',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["country"] == dag_run.conf['country'], michael_kors_gmbh_user_sync_master_mapper_spain))
        )

        if_first_id_blank_6 = rail.IfOperator(
            task_id='if_first_id_blank_6',
            test=lambda: len(rail.result('michael_kors_gmbh_user_sync_master_mapper_spain_search_entries_5')) < 1,
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
            python_callable=lambda: custom_methods.get_date_string(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate'])
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

        log_holiday_timeoff_uri = rail.PythonOperator(
            task_id='log_holiday_timeoff_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_9')[0]['timeOffTypePolicySummary']['policiesByTimeOffType'], 'timeOffType.name', '[ES] Holidays', 'timeOffType.uri', '')
        )

        if_division_displaytext_present_15 = rail.IfOperator(
            task_id='if_division_displaytext_present_15',
            test=lambda: rail.result('bulk_get_users3_9') and rail.result('bulk_get_users3_9')[0]['divisionSchedule'] and rail.result(
                'bulk_get_users3_9')[0]['divisionSchedule'][0]['division'] and rail.result(
                'bulk_get_users3_9')[0]['divisionSchedule'][0]['division']['displayText'] and rail.result(
                'bulk_get_users3_9')[0]['divisionSchedule'][0]['division']['displayText'] != 'Spain',
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
            trigger_dag_id=f'michaelkorstna_spain_user_import_timesheet_recalculation_child_{config.instance}',
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
            trigger_dag_id=f'michaelkorstna_spain_user_import_add_user_child_{config.instance}',
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
                "cbauri": "{{dag_run.conf.cbauri}}",
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
            trigger_dag_id=f'michaelkorstna_spain_user_import_add_user_child_{config.instance}',
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
                "cbauri": "{{dag_run.conf.cbauri}}",
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
            trigger_dag_id=f'michaelkorstna_spain_user_import_add_user_child_{config.instance}',
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
                "cbauri": "{{dag_run.conf.cbauri}}",
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
            no_task="if_location_equals_las_plamas_or_tenerife",
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
            trigger_dag_id=f'michaelkorstna_spain_user_import_add_user_child_{config.instance}',
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
                "cbauri": "{{dag_run.conf.cbauri}}",
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

        if_location_equals_las_plamas_or_tenerife = rail.IfOperator(
            task_id = 'if_location_equals_las_plamas_or_tenerife',
            test=lambda dag_run: dag_run.conf['location'] == 'MK Las Palmas El Corte Ingles' or dag_run.conf['location'] == 'MK Tenerife El Corte Ingles',
            yes_task='if_timezone_uri_unequal_europe_london',
            no_task='get_current_customfield_values'
        )

        if_timezone_uri_unequal_europe_london = rail.IfOperator(
            task_id = 'if_timezone_uri_unequal_europe_london',
            test=lambda: 'urn:replicon:time-zone:europe-london' != (rail.result('bulk_get_users3_9')[0]['timeZone']['uri'] if rail.result(
                'bulk_get_users3_9') and rail.result('bulk_get_users3_9')[0]['timeZone'] else ''),
            yes_task='update_timezone_for_user',
            no_task='get_current_customfield_values'
        )

        update_timezone_for_user = rail.RepliconServiceOperator(
            task_id = 'update_timezone_for_user',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeZoneUri": "urn:replicon:time-zone:europe-london"
            }
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
                'cbavalue': rail.find_first_by_attr_and_get_attr(customfieldvalues, 'customField.displayText', "CBA", 'text', '')
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
            yes_task="disable_login_63",
            no_task="trigger_dag_run_live_michaelkorstna_child_timesheet_recalculation_v1_0async_64",
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
            trigger_dag_id=f'michaelkorstna_spain_user_import_timesheet_recalculation_child_{config.instance}',
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
                "effectivedate": (datetime.strptime(custom_methods.get_date_string(rail.result(
                    'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']), "%d/%m/%Y")).strftime("%Y-%m-%d"),
                "name": rail.result('foreach_document_126')['serviceCenter']['displayText']
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
            python_callable=lambda: (datetime.strptime(custom_methods.get_date_string(rail.result(
                'foreach_document_126')['effectiveDate']), '%d/%m/%Y')).strftime("%Y-%m-%d")
        )

        if_to_date_less_than_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date1days_132 = rail.IfOperator(
            task_id='if_to_date_less_than_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date1days_132',
            test=lambda dag_run: datetime.strptime(rail.result('log_effectivedate_131'), "%Y-%m-%d") < (
                datetime.strptime((dag_run.conf['weeklyscheduleeffectivedate'] if dag_run.conf[
                'weeklyscheduleeffectivedate'] else datetime.now().strftime("%Y-%m-%d")), "%Y-%m-%d") + timedelta(days=1)),
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
                "name": "{{ result('foreach_document_126').serviceCenter.displayText }}"
            }
        )

        if_to_date_not_equals_to_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date_134 = rail.IfOperator(
            task_id='if_to_date_not_equals_to_dataworkato_servicereceive_requestrequestweeklyscheduleeffectivedateto_date_134',
            test=lambda dag_run: datetime.strptime(rail.result('log_effectivedate_131'), "%Y-%m-%d") != (
                datetime.strptime((dag_run.conf['weeklyscheduleeffectivedate'] if dag_run.conf[
                    'weeklyscheduleeffectivedate'] else datetime.now().strftime('%Y-%m-%d')), "%Y-%m-%d")),
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
                "effectivedate": (datetime.strptime(custom_methods.get_date_string(rail.result(
                    'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']),"%d/%m/%Y")).strftime("%Y-%m-%d"),
                "name": rail.result('foreach_document_157')['supervisor']['displayText']
            }
        )

        log_effectivedate_161 = rail.PythonOperator(
            task_id='log_effectivedate_161',
            python_callable=lambda: (datetime.strptime(custom_methods.get_date_string(
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
            for user in users_found:
                if user['cells'][0]['textValue'] == dag_run.conf['managerid']:
                    return {
                        'uri': user['cells'][0]['uri'],
                        'status': user['cells'][1]['textValue']
                    }
            return {
                'uri': '',
                'status': ''
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
                lambda entry: entry['type'] == 'Permission' and entry['identifier___1'] == 'Supervisor',
                michael_kors_gmbh_user_sync_master_mapper_spain))], ';')
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
                "effectivedate": (datetime.strptime(custom_methods.get_date_string(rail.result(
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
            python_callable=lambda: (datetime.strptime(custom_methods.get_date_string(
                rail.result('foreach_document_225')['effectiveDate']),"%d/%m/%Y")).strftime("%Y-%m-%d")
        )

        if_to_date_less_than_todayto_date_231 = rail.IfOperator(
            task_id='if_to_date_less_than_todayto_date_231',
            test=lambda: datetime.strptime(rail.result(
                'log_effectivedate_230'), '%Y-%m-%d') < (datetime.strptime(datetime.now().strftime("%d/%m/%Y"), "%d/%m/%Y") + timedelta(days=1)),
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
                "effectivedate": (datetime.strptime(custom_methods.get_date_string(rail.result(
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
            python_callable=lambda: (datetime.strptime(custom_methods.get_date_string(
                rail.result('foreach_document_250')['effectiveDate']),"%d/%m/%Y")).strftime("%Y-%m-%d")
        )

        if_to_date_less_than_todayto_date_256 = rail.IfOperator(
            task_id='if_to_date_less_than_todayto_date_256',
            test=lambda: datetime.strptime(rail.result(
                'log_effectivedate_255'), "%Y-%m-%d") < (datetime.strptime(datetime.now().strftime("%d/%m/%Y"), "%d/%m/%Y") + timedelta(days=1)),
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
            yes_task="get_required_cba_based_on_location",
            no_task="log_required_cost_center_268",
        )

        def get_cba_based_on_location(dag_run):
            mapper_entries = rail.result('michael_kors_gmbh_user_sync_master_mapper_spain_search_entries_5')
            cbaentry = list(filter(lambda entry: entry['type'] == 'CBA' and entry['identifier___1'] == dag_run.conf['location'],mapper_entries))
            return cbaentry[0]['value'] if cbaentry else ''

        get_required_cba_based_on_location = rail.PythonOperator(
            task_id = 'get_required_cba_based_on_location',
            python_callable=get_cba_based_on_location
        )

        if_cba_value_not_available = rail.IfOperator(
            task_id = 'if_cba_value_not_available',
            test=lambda: not(rail.result('get_required_cba_based_on_location')),
            yes_task='add_exception_fields_not_updated',
            no_task='insert_to_locationlist'
        )

        add_exception_fields_not_updated=rail.SetVariableOperator(
            task_id='add_exception_fields_not_updated',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": '''Location, CBA, Payrule, Holiday calendar not updated as CBA not available in mapper for location "{{ dag_run.conf.location }}".'''
            }
        )

        insert_to_locationlist = rail.SetVariableOperator(
            task_id='insert_to_locationlist',
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

        log_location_schedule = rail.PythonOperator(
            task_id='log_location_schedule',
            python_callable=lambda: (json.dumps(rail.get_dag_run_var('locationlist'))).replace(
                'effectiveDate":{}', 'effectiveDate":null')
        )

        put_location_schedule_for_user_266 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_266',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": json.loads(rail.result('log_location_schedule'))
            }
        )

        if_locationsent_unequal_current_location = rail.IfOperator(
            task_id = 'if_locationsent_unequal_current_location',
            test=lambda dag_run: dag_run.conf['location'] != rail.result('log_currentlocationname_262'),
            yes_task='set_timeoff_trigger_yes',
            no_task='add_log_location_updated'
        )

        set_timeoff_trigger_yes = rail.SetVariableOperator(
            task_id = 'set_timeoff_trigger_yes',
            name='timeofftrigger',
            append=False,
            value='yes'
        )

        add_log_location_updated = rail.SetVariableOperator(
            task_id='add_log_location_updated',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Location updated"
            }
        )

        if_existingcbavalue_enequal_required_value = rail.IfOperator(
            task_id = 'if_existingcbavalue_enequal_required_value',
            test=lambda: not(rail.result('get_current_customfield_values')['cbavalue']) or ((rail.result(
                'get_current_customfield_values')['cbavalue']).lower() != (rail.result('get_required_cba_based_on_location')).lower()),
            yes_task='get_dropdownoptions_for_cba',
            no_task='get_required_holiday_calendar'
        )

        get_dropdownoptions_for_cba = rail.RepliconServiceOperator(
            task_id = 'get_dropdownoptions_for_cba',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.cbauri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response,'displayText',rail.result(
                'get_required_cba_based_on_location'),'uri','')
        )

        update_dropwdown_value_for_cba = rail.RepliconServiceOperator(
            task_id = 'update_dropwdown_value_for_cba',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.cbauri }}",
                "customFieldDropDownOptionUri": "{{ result('get_dropdownoptions_for_cba') }}"
            }
        )

        log_cba_updated=rail.SetVariableOperator(
            task_id='log_cba_updated',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "CBA updated"
            }
        )

        def get_full_time_hours():
            mapper_entries = rail.result('michael_kors_gmbh_user_sync_master_mapper_spain_search_entries_5')
            entry_for_required_cba = list(filter(lambda entry: entry['type'] == 'Full time' and entry['identifier___1'] == rail.result(
                'get_required_cba_based_on_location'),mapper_entries))
            entry_for_current_cba = list(filter(lambda entry: entry['type'] == 'Full time' and entry['identifier___1'] == rail.result(
                'get_current_customfield_values')['cbavalue'],mapper_entries))
            requiredcbahours = int(entry_for_required_cba[0]['value']) if entry_for_required_cba else ''
            currentcbahours = int(entry_for_current_cba[0]['value']) if entry_for_current_cba else ''
            return {
                'requiredcbahours': requiredcbahours,
                'currentcbahours': currentcbahours,
                'isunequal': requiredcbahours != currentcbahours
            }

        get_fulltimehours_for_required_and_existing_cba =rail.PythonOperator(
            task_id = 'get_fulltimehours_for_required_and_existing_cba',
            python_callable=get_full_time_hours
        )

        def get_holidaycalendar_required(dag_run):
            mapper_entries = rail.result('michael_kors_gmbh_user_sync_master_mapper_spain_search_entries_5')
            holidayentry = list(filter(lambda entry: entry['type'] == 'Holiday Calendar' and
                            entry['identifier___1'] == dag_run.conf['location'],mapper_entries))
            return holidayentry[0]['value'] if holidayentry else ''

        get_required_holiday_calendar = rail.PythonOperator(
            task_id = 'get_required_holiday_calendar',
            python_callable=get_holidaycalendar_required
        )

        if_existing_holidaycalendar_unequal_required = rail.IfOperator(
            task_id = 'if_existing_holidaycalendar_unequal_required',
            test=lambda: rail.result('get_required_holiday_calendar') != (rail.result('bulk_get_users3_9')[0]['holidayCalendar']['name'] if rail.result(
                'bulk_get_users3_9')[0]['holidayCalendar'] else ''),
            yes_task='get_all_holiday_calendars',
            no_task='declare_payrule_schedule'
        )

        get_all_holiday_calendars=rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": json.loads(rail.result('log_location_schedule'))
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response,'name',rail.result('get_required_holiday_calendar'),'uri','')
        )

        if_required_holiday_calendaruri_present=rail.IfOperator(
            task_id='if_required_holiday_calendaruri_present',
            test='''{{ result('get_all_holiday_calendars') | is_truthy }}''',
            yes_task="update_holiday_calendar_for_user",
            no_task="log_holiday_calendar_not_available",
        )

        update_holiday_calendar_for_user=rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": "{{ result('get_all_holiday_calendars') }}"
            }
        )

        add_log_holiday_calendar_updated=rail.SetVariableOperator(
            task_id='add_log_holiday_calendar_updated',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Holiday calendar updated"
            }
        )

        log_holiday_calendar_not_available=rail.SetVariableOperator(
            task_id='log_holiday_calendar_not_available',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": '''Holiday calendar "{{ result('get_required_holiday_calendar') }}" not available in Replicon'''
            }
        )

        declare_payrule_schedule=rail.SetVariableOperator(
            task_id='declare_payrule_schedule',
            append=False,
            name='payrule_schedule',
            value=[]
        )

        declare_payrule_list=rail.SetVariableOperator(
            task_id='declare_payrule_list',
            append=False,
            name='payrulelist',
            value=[]
        )

        if_urn_in_payrulescript_schedule=rail.IfOperator(
            task_id='if_urn_in_payrulescript_schedule',
            test=lambda: 'urn' in json.dumps(rail.result('bulk_get_users3_9')[0]['payRuleScriptSchedule']),
            yes_task="foreach_payrule_script_schedule",
            no_task="if_payrule_schedule_list_has_data",
        )

        foreach_payrule_script_schedule=rail.ForEachOperator(
            task_id='foreach_payrule_script_schedule',
            items=lambda: rail.result('bulk_get_users3_9')[0]['payRuleScriptSchedule'],
            start_task = 'if_effectivedate_not_present',
            end_task = 'foreach_payrule_script_schedule_end'
        )

        if_effectivedate_not_present=rail.IfOperator(
            task_id='if_effectivedate_not_present',
            test=lambda: not(rail.result('foreach_payrule_script_schedule')['effectiveDate'] and rail.result(
                'foreach_payrule_script_schedule')['effectiveDate']['day']),
            yes_task="add_item_to_payrule_schedule_list",
            no_task="get_schedule_effective_date",
        )

        add_item_to_payrule_schedule_list=rail.SetVariableOperator(
            task_id='add_item_to_payrule_schedule_list',
            append=True,
            name='{{ result("declare_payrule_schedule").name }}',
            value=lambda: {
                "uri": rail.result('foreach_payrule_script_schedule')['payRuleScript']['uri'],
                "effectivedate": (datetime.strptime(custom_methods.get_date_string(rail.result(
                    'bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']),"%d/%m/%Y")).strftime("%Y-%m-%d"),
                "name": rail.result('foreach_payrule_script_schedule')['payRuleScript']['displayText']
            }
        )

        add_item_to_payrule_list=rail.SetVariableOperator(
            task_id='add_item_to_payrule_list',
            append=True,
            name='{{ result("declare_payrule_list").name }}',
            value={
                "effectiveDate": null,
                "payRuleScript": {
                    "uri": "{{ result('foreach_payrule_script_schedule').payRuleScript.uri }}",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        get_schedule_effective_date=rail.PythonOperator(
            task_id='get_schedule_effective_date',
            python_callable= lambda: (datetime.strptime(custom_methods.get_date_string(
                rail.result('foreach_payrule_script_schedule')['effectiveDate']),"%d/%m/%Y")).strftime("%Y-%m-%d")
        )

        def get_first_day_of_next_month(datetimeobj):
            return (datetimeobj + relativedelta(months=1)).replace(day=1)

        if_effectivedate_lessthan_second_day_next_month=rail.IfOperator(
            task_id='if_effectivedate_lessthan_second_day_next_month',
            test=lambda: datetime.strptime(rail.result(
                'get_schedule_effective_date'), "%Y-%m-%d") < ( get_first_day_of_next_month(datetime.now()) + timedelta(days=1) ),
            yes_task="insert_to_payrule_schedule_list",
            no_task="if_effectivedate_unequal_firstday_next_month",
        )

        insert_to_payrule_schedule_list=rail.SetVariableOperator(
            task_id='insert_to_payrule_schedule_list',
            append=True,
            name='{{ result("declare_payrule_schedule").name }}',
            value={
                "uri": "{{ result('foreach_payrule_script_schedule').payRuleScript.uri }}",
                "effectivedate": "{{result('get_schedule_effective_date')}}",
                "name": "{{ result('foreach_payrule_script_schedule').payRuleScript.displayText }}"
            }
        )

        if_effectivedate_unequal_firstday_next_month=rail.IfOperator(
            task_id='if_effectivedate_unequal_firstday_next_month',
            test=lambda: datetime.strptime(rail.result(
                'get_schedule_effective_date'), "%Y-%m-%d") != get_first_day_of_next_month(datetime.now()),
            yes_task="insert_to_payrule_list",
            no_task="foreach_payrule_script_schedule_end",
        )

        insert_to_payrule_list=rail.SetVariableOperator(
            task_id='insert_to_payrule_list',
            append=True,
            name='{{ result("declare_payrule_list").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('foreach_payrule_script_schedule').effectiveDate.year }}",
                    "month": "{{ result('foreach_payrule_script_schedule').effectiveDate.month }}",
                    "day": "{{ result('foreach_payrule_script_schedule').effectiveDate.day }}"
                },
                "payRuleScript": {
                    "uri": "{{ result('foreach_payrule_script_schedule').payRuleScript.uri }}",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        foreach_payrule_script_schedule_end=rail.EmptyOperator(
            task_id='foreach_payrule_script_schedule_end',
        )

        if_payrule_schedule_list_has_data=rail.IfOperator(
            task_id='if_payrule_schedule_list_has_data',
            test=lambda: bool(rail.get_dag_run_var('payrule_schedule')),
            yes_task="get_schedule_with_max_effectivedate",
            no_task="get_required_payrule_namebasedon_cba",
        )

        get_schedule_with_max_effectivedate=rail.PythonOperator(
            task_id='get_schedule_with_max_effectivedate',
            python_callable= lambda: max(rail.get_dag_run_var(
                'payrule_schedule'), key=lambda x: x['effectivedate'])
        )

        get_current_payrule_name=rail.PythonOperator(
            task_id='get_current_payrule_name',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('payrule_schedule'),'effectivedate',rail.result(
                'get_schedule_with_max_effectivedate')['effectivedate'],'name','')
        )

        def get_requiredpayrule_basedon_cba():
            mapper_entries = rail.result('michael_kors_gmbh_user_sync_master_mapper_spain_search_entries_5')
            payruleentry = list(filter(lambda entry: entry['type'] == 'Payrule' and entry['identifier___1'] == rail.result(
                'get_required_cba_based_on_location'),mapper_entries))
            return payruleentry[0]['value'] if payruleentry else ''

        get_required_payrule_namebasedon_cba=rail.PythonOperator(
            task_id='get_required_payrule_namebasedon_cba',
            python_callable=get_requiredpayrule_basedon_cba
        )

        if_currentpayrulename_unequal_required_payrule=rail.IfOperator(
            task_id='if_currentpayrulename_unequal_required_payrule',
            test=lambda: not(rail.result('get_current_payrule_name')) or ((rail.result('get_current_payrule_name')).lower() != (rail.result(
                'get_required_payrule_namebasedon_cba')).lower()),
            yes_task="get_all_scriptsforpayrule",
            no_task="log_required_cost_center_268",
        )

        get_all_scriptsforpayrule=rail.RepliconServiceOperator(
            task_id='get_all_scriptsforpayrule',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response,'displayText',rail.result(
                'get_required_payrule_namebasedon_cba'),'uri','')
        )

        if_required_payrule_script_uri_not_present=rail.IfOperator(
            task_id='if_required_payrule_script_uri_not_present',
            test='''{{ result('get_all_scriptsforpayrule') | is_falsy }}''',
            yes_task="add_log_payrule_not_available",
            no_task="add_schedule_to_payrule_list",
        )

        add_log_payrule_not_available=rail.SetVariableOperator(
            task_id='add_log_payrule_not_available',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": '''Payrule "{{ result('get_required_payrule_namebasedon_cba') }}" not available in Replicon'''
            }
        )

        add_schedule_to_payrule_list=rail.SetVariableOperator(
            task_id='add_schedule_to_payrule_list',
            append=True,
            name='{{ result("declare_payrule_list").name }}',
            value=lambda: {
    "effectiveDate": {
        "year": get_first_day_of_next_month(datetime.now()).year,
        "month": get_first_day_of_next_month(datetime.now()).month,
        "day": get_first_day_of_next_month(datetime.now()).day
    },
    "payRuleScript": {
        "uri": rail.result('get_all_scriptsforpayrule'),
        "parentUri": null,
        "name": null
    }
}
        )

        get_final_payrule_schedule=rail.PythonOperator(
            task_id='get_final_payrule_schedule',
            python_callable= lambda: json.dumps(rail.get_dag_run_var('payrulelist')).replace('effectiveDate":{}', 'effectiveDate":null')
        )

        put_pay_rule_script_assignment_schedule_for_user=rail.RepliconServiceOperator(
            task_id='put_pay_rule_script_assignment_schedule_for_user',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run:{
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": json.loads(rail.result('get_final_payrule_schedule'))
            }
        )

        add_log_payrule_updated=rail.SetVariableOperator(
            task_id='add_log_payrule_updated',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Payrule updated"
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
            no_task="if_scheduledweeklyhours_and_location_present",
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
                "effectivedate": (datetime.strptime(custom_methods.get_date_string(rail.result(
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
            python_callable=lambda:  (datetime.strptime(custom_methods.get_date_string(
                rail.result('foreach_document_275')['effectiveDate']),"%d/%m/%Y")).strftime("%Y-%m-%d")
        )

        if_to_date_less_than_todayto_date_281 = rail.IfOperator(
            task_id='if_to_date_less_than_todayto_date_281',
            test=lambda: datetime.strptime(rail.result(
                'log_effectivedate_280'), "%Y-%m-%d") < (datetime.strptime(datetime.now().strftime("%d/%m/%Y"), "%d/%m/%Y") + timedelta(days=1)),
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
            no_task="if_scheduledweeklyhours_and_location_present",
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

        log_cost_center_updated = rail.SetVariableOperator(
            task_id='log_cost_center_updated',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Cost center updated"
            }
        )

        if_scheduledweeklyhours_and_location_present = rail.IfOperator(
            task_id = 'if_scheduledweeklyhours_and_location_present',
            test=lambda dag_run: dag_run.conf['scheduledweeklyhours'] and dag_run.conf['location'],
            yes_task='if_scheduled_weekly_hours_or_location_has_changed',
            no_task='if_timeofftrigger_equal_yes'
        )

        if_scheduled_weekly_hours_or_location_has_changed = rail.IfOperator(
            task_id = 'if_scheduled_weekly_hours_or_location_has_changed',
            test=lambda dag_run: dag_run.conf['scheduledweeklyhours'] != rail.result(
                'get_current_customfield_values')['scheduledweeklyhours'] or dag_run.conf['location'] != rail.result('log_currentlocationname_262'),
            yes_task='search_officeschedule_in_mapper',
            no_task='if_timeofftrigger_equal_yes'
        )

        def get_officeschedule_from_mapper(dag_run):
            officescheduleentry = list(filter(lambda entry: entry['weekly_schedule'] == dag_run.conf['scheduledweeklyhours'] and
                                    entry['location'] == dag_run.conf['location'],michaelkorstna_schedulemapper_spain))
            return officescheduleentry[0]['office_schedule_name'] if officescheduleentry else ''

        search_officeschedule_in_mapper = rail.PythonOperator(
            task_id = 'search_officeschedule_in_mapper',
            python_callable=get_officeschedule_from_mapper
        )

        if_required_schedule_unequal_current = rail.IfOperator(
            task_id = 'if_required_schedule_unequal_current',
            test=lambda dag_run: dag_run.conf['currentschedule'] != rail.result('search_officeschedule_in_mapper'),
            yes_task='update_schedule_assignment',
            no_task='if_timeofftrigger_equal_yes'
        )

        update_schedule_assignment = rail.RepliconServiceOperator(
            task_id = 'update_schedule_assignment',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "schedulePolicyToApply": {
                    "userSchedulePolicyScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementSchedule": [],
                    "updateScheduleOverDateRange": {
                        "replacementScheduleEntries": [
                        {
                            "schedulePolicy": {
                            "officeScheduleUri": null,
                            "name": "{{ result('search_officeschedule_in_mapper') }}",
                            "officeSchedule": {
                                "officeScheduleUri": null,
                                "name": "{{ result('search_officeschedule_in_mapper') }}"
                            },
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": {
                            "year": "{{ result('invoke_custom_ruby_code_todays_date_11').year }}",
                            "month": "{{ result('invoke_custom_ruby_code_todays_date_11').month }}",
                            "day": "{{ result('invoke_custom_ruby_code_todays_date_11').day }}"
                            }
                        }
                        ],
                        "endDate": null
                    }
                    },
                    "locationScheduleToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_timeofftrigger_equal_yes = rail.IfOperator(
            task_id = 'if_timeofftrigger_equal_yes',
            test=lambda: rail.get_dag_run_var('timeofftrigger') == 'yes',
            yes_task='get_required_cba_value',
            no_task='add_final_log_for_user'
        )

        def get_cba_based_on_location_with_fulltimehours_and_accrual_amount(dag_run):
            cbavalue = get_cba_based_on_location(dag_run)
            mapper_entries = rail.result('michael_kors_gmbh_user_sync_master_mapper_spain_search_entries_5')
            fulltimehoursentry = list(filter(lambda entry: entry['type'] == 'Full time' and
                                    entry['identifier___1'] == cbavalue,mapper_entries)) if cbavalue else ''
            accrualamountentry = list(filter(lambda entry: entry['type'] == 'Accrual Amount for Holiday Leave' and
                                    entry['identifier___1'] == dag_run.conf['location'],mapper_entries))
            return {
                'cba': cbavalue,
                'fulltimehours': int(fulltimehoursentry[0]['value']) if fulltimehoursentry else '',
                'annualaccrualamount': int(accrualamountentry[0]['value']) if accrualamountentry else ''
            }

        get_required_cba_value = rail.PythonOperator(
            task_id = 'get_required_cba_value',
            python_callable=get_cba_based_on_location_with_fulltimehours_and_accrual_amount
        )

        if_required_cba_value_present = rail.IfOperator(
            task_id = 'if_required_cba_value_present',
            test=lambda: bool(rail.result('get_required_cba_value')['cba']),
            yes_task='trigger_child_timeoff_type_proration_assignment',
            no_task='log_holiday_timofftype_policy_not_updated'
        )

        trigger_child_timeoff_type_proration_assignment = rail.TriggerDagRunOperator(
            task_id='trigger_child_timeoff_type_proration_assignment',
            retries=0,
            trigger_dag_id=f'michaelkorstna_spain_user_import_timeoff_type_proration_assignment_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "callerjobid": dag_run.conf['callerjobid'],
                "userloginname": dag_run.conf['employeeid'],
                "useruri": dag_run.conf['useruri'],
                "startdate": datetime.now().strftime("%d/%m/%Y"),
                "type": "Update",
                "timeoffuri": rail.result('log_holiday_timeoff_uri'),
                "actualstartdate": rail.result('log_startdate_10'),
                "scheduledweeklyhours": ( float(rail.result('get_required_cba_value')['fulltimehours']) if float(
                    dag_run.conf['scheduledweeklyhours']) >= float(rail.result(
                    'get_required_cba_value')['fulltimehours']) else float(dag_run.conf['scheduledweeklyhours'])) if dag_run.conf[
                    'scheduledweeklyhours'] else float(rail.result('get_required_cba_value')['fulltimehours']),
                "fullpart": ( 'Full Time' if float(dag_run.conf['scheduledweeklyhours']) >= float(rail.result(
                    'get_required_cba_value')['fulltimehours']) else 'Part Time' ) if dag_run.conf['scheduledweeklyhours'] else 'Full Time',
                "timeofftype": "[ES] Holidays",
                "cbabasedhours": rail.result('get_required_cba_value')['fulltimehours'],
                "accrualdays": rail.result('get_required_cba_value')['annualaccrualamount']
            }
        )

        wait_for_child_timeoff_type_proration_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_timeoff_type_proration_assignment',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_timeoff_type_proration_assignment") }}'
        )

        log_holiday_timofftype_policy_not_updated = rail.SetVariableOperator(
            task_id = 'log_holiday_timofftype_policy_not_updated',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": '''Holiday timeoff type policy not updated as CBA is not available for location {{dag_run.conf.locations}}'''
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
            trigger_dag_id=f'michaelkorstna_spain_user_import_timesheet_recalculation_child_{config.instance}',
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
        declare_list_2 >> declare_list_3 >> declare_variable_4 >> michael_kors_gmbh_user_sync_master_mapper_spain_search_entries_5 >> if_first_id_blank_6
        if_first_id_blank_6 >> rail.Label(
            'Yes') >> michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_add_entry_297_297_7 >> catch_error
        if_first_id_blank_6 >> rail.Label(
            'No') >> bulk_get_users3_9 >> log_startdate_10 >> invoke_custom_ruby_code_todays_date_11 >> log_holiday_timeoff_uri
        log_holiday_timeoff_uri >> if_division_displaytext_present_15
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
            'No') >> if_location_equals_las_plamas_or_tenerife
        if_location_equals_las_plamas_or_tenerife >> rail.Label('Yes') >> if_timezone_uri_unequal_europe_london
        if_timezone_uri_unequal_europe_london >> rail.Label('Yes') >> update_timezone_for_user >> get_current_customfield_values
        if_timezone_uri_unequal_europe_london >> rail.Label('No') >> get_current_customfield_values
        if_location_equals_las_plamas_or_tenerife >> rail.Label('No') >> get_current_customfield_values
        get_current_customfield_values >> if_request_lastdayofwork_present_49
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
            'Yes') >> disable_login_63
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
            'Yes') >> update_numeric_value_scheduled_weekly_hours_116 >> insert_to_list_117 >> if_request_weeklyscheduleuri_present_120
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
            'Yes') >> get_required_cba_based_on_location >> if_cba_value_not_available
        if_cba_value_not_available >> rail.Label('Yes') >> add_exception_fields_not_updated >> log_required_cost_center_268
        if_cba_value_not_available >> rail.Label('No') >> insert_to_locationlist
        insert_to_locationlist >> log_location_schedule >> put_location_schedule_for_user_266 >> if_locationsent_unequal_current_location
        if_locationsent_unequal_current_location >> rail.Label('Yes') >> set_timeoff_trigger_yes >> add_log_location_updated
        if_locationsent_unequal_current_location >> rail.Label('No') >> add_log_location_updated >> if_existingcbavalue_enequal_required_value
        if_existingcbavalue_enequal_required_value >> rail.Label('Yes') >> get_dropdownoptions_for_cba >> update_dropwdown_value_for_cba >> log_cba_updated
        log_cba_updated >> get_fulltimehours_for_required_and_existing_cba >> get_required_holiday_calendar
        if_existingcbavalue_enequal_required_value >> rail.Label('No') >> get_required_holiday_calendar >> if_existing_holidaycalendar_unequal_required
        if_existing_holidaycalendar_unequal_required >> rail.Label('Yes') >> get_all_holiday_calendars >> if_required_holiday_calendaruri_present
        if_required_holiday_calendaruri_present >> rail.Label('Yes') >> update_holiday_calendar_for_user >> add_log_holiday_calendar_updated
        add_log_holiday_calendar_updated >> declare_payrule_schedule
        if_required_holiday_calendaruri_present >> rail.Label('No') >> log_holiday_calendar_not_available >> declare_payrule_schedule
        if_existing_holidaycalendar_unequal_required >> rail.Label('No') >> declare_payrule_schedule >> declare_payrule_list
        declare_payrule_list >> if_urn_in_payrulescript_schedule
        if_urn_in_payrulescript_schedule >> rail.Label('Yes') >> foreach_payrule_script_schedule >> if_effectivedate_not_present
        if_effectivedate_not_present >> rail.Label('Yes') >> add_item_to_payrule_schedule_list >> add_item_to_payrule_list
        add_item_to_payrule_list >> foreach_payrule_script_schedule_end
        if_effectivedate_not_present >> rail.Label('No') >> get_schedule_effective_date >> if_effectivedate_lessthan_second_day_next_month
        if_effectivedate_lessthan_second_day_next_month >> rail.Label('Yes') >> insert_to_payrule_schedule_list >> if_effectivedate_unequal_firstday_next_month
        if_effectivedate_lessthan_second_day_next_month >> rail.Label('No') >> if_effectivedate_unequal_firstday_next_month
        if_effectivedate_unequal_firstday_next_month >> rail.Label('Yes') >> insert_to_payrule_list >> foreach_payrule_script_schedule_end
        if_effectivedate_unequal_firstday_next_month >> rail.Label('No') >> foreach_payrule_script_schedule_end
        if_urn_in_payrulescript_schedule >> rail.Label('No') >> if_payrule_schedule_list_has_data
        foreach_payrule_script_schedule >> foreach_payrule_script_schedule_end >> if_payrule_schedule_list_has_data
        if_payrule_schedule_list_has_data >> rail.Label('Yes') >> get_schedule_with_max_effectivedate >> get_current_payrule_name
        get_current_payrule_name >> get_required_payrule_namebasedon_cba
        if_payrule_schedule_list_has_data >> rail.Label('No') >> get_required_payrule_namebasedon_cba >> if_currentpayrulename_unequal_required_payrule
        if_currentpayrulename_unequal_required_payrule >> rail.Label('Yes') >> get_all_scriptsforpayrule >> if_required_payrule_script_uri_not_present
        if_required_payrule_script_uri_not_present >> rail.Label('Yes') >> add_log_payrule_not_available >> log_required_cost_center_268
        if_required_payrule_script_uri_not_present >> rail.Label('No') >> add_schedule_to_payrule_list >> get_final_payrule_schedule
        get_final_payrule_schedule >> put_pay_rule_script_assignment_schedule_for_user >> add_log_payrule_updated >> log_required_cost_center_268
        if_currentpayrulename_unequal_required_payrule >> rail.Label('No') >> log_required_cost_center_268
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
            'Yes') >> insert_to_list_289 >> log_cost_center_schedule_290 >> put_cost_center_schedule_for_user_291 >> log_cost_center_updated
        log_cost_center_updated >> if_scheduledweeklyhours_and_location_present
        if_scheduledweeklyhours_and_location_present >> rail.Label('Yes') >> if_scheduled_weekly_hours_or_location_has_changed
        if_scheduled_weekly_hours_or_location_has_changed >> rail.Label('Yes') >> search_officeschedule_in_mapper >> if_required_schedule_unequal_current
        if_required_schedule_unequal_current >> rail.Label('Yes') >> update_schedule_assignment >> if_timeofftrigger_equal_yes
        if_required_schedule_unequal_current >> rail.Label('No') >> if_timeofftrigger_equal_yes
        if_scheduled_weekly_hours_or_location_has_changed >> rail.Label('No') >> if_timeofftrigger_equal_yes
        if_scheduledweeklyhours_and_location_present >> rail.Label('No') >> if_timeofftrigger_equal_yes
        if_timeofftrigger_equal_yes >> rail.Label('Yes') >> get_required_cba_value >> if_required_cba_value_present
        if_required_cba_value_present >> rail.Label('Yes') >> trigger_child_timeoff_type_proration_assignment
        trigger_child_timeoff_type_proration_assignment >> wait_for_child_timeoff_type_proration_assignment >> add_final_log_for_user
        if_required_cba_value_present >> rail.Label('No') >> log_holiday_timofftype_policy_not_updated >> add_final_log_for_user
        if_timeofftrigger_equal_yes >> rail.Label('No') >> add_final_log_for_user
        if_log_currentcostcentername_287_blank_288 >> rail.Label(
            'No') >> if_scheduledweeklyhours_and_location_present
        if_log_required_cost_center_268_present_dataworkato_servicereceive_requestrequestsupervisorssoid_269 >> rail.Label(
            'No') >> if_scheduledweeklyhours_and_location_present
        add_final_log_for_user >> if_entry_col4_not_equals_to_nochangetotheuserrecordinreplicon_294
        if_entry_col4_not_equals_to_nochangetotheuserrecordinreplicon_294 >> rail.Label(
            'Yes') >> trigger_timesheet_recalculation_child
        trigger_timesheet_recalculation_child >> wait_for_timesheet_recalculation_child >> catch_error
        if_entry_col4_not_equals_to_nochangetotheuserrecordinreplicon_294 >> rail.Label(
            'No') >> catch_error >> add_log_for_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
