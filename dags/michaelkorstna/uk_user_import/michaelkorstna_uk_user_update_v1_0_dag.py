
from datetime import timedelta, datetime
import json
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import rail


null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_uk_user_update_child_{config.instance}',
        description=f'MichaelKorsTnA UK User Update V1.0 {config.instance}',
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

        michael_kors_gmbh_user_sync_master_mapper_uk_search_entries_5 = rail.PythonOperator(
            task_id='michael_kors_gmbh_user_sync_master_mapper_uk_search_entries_5',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["country"] == dag_run.conf['country'], config.michael_kors_gmbh_user_sync_master_mapper_uk))
        )

        def get_required_values_for_fields(dag_run):
            mapper_entries = rail.result('michael_kors_gmbh_user_sync_master_mapper_uk_search_entries_5')
            requiredsubstituteuserdraft = list(filter(lambda entry: entry['type'] == 'Substitute Exception' and entry['identifier__1'] == dag_run.conf[
                'managerid'],mapper_entries))
            requiredsubstituteuser = requiredsubstituteuserdraft[0]['value'] if requiredsubstituteuserdraft else ''
            requiredmanager = requiredsubstituteuser if requiredsubstituteuser else dag_run.conf['managerid']
            return {
                'timezone': rail.smartjoin_by_delim([entry['default__uri'] for entry in list(filter(lambda x: x['type'] == 'Timezone',mapper_entries))],';'),
                'manager': requiredmanager

            }

        get_required_fields_from_mapper = rail.PythonOperator(
            task_id = 'get_required_fields_from_mapper',
            python_callable=get_required_values_for_fields
        )

        if_first_id_blank_6 = rail.IfOperator(
            task_id='if_first_id_blank_6',
            test=lambda: len(rail.result('michael_kors_gmbh_user_sync_master_mapper_uk_search_entries_5')) < 1,
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

        get_effective_user_group_membership_10=rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership_10',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": null
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

        get_time_off_type_assignments_for_user_15=rail.RepliconServiceOperator(
            task_id='get_time_off_type_assignments_for_user_15',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response,'name','[ES] Holidays','uri','')
        )

        log_holiday_leavetimeoff_uri_14 = rail.PythonOperator(
            task_id='log_holiday_leavetimeoff_uri_14',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_9')[0]['timeOffTypePolicySummary']['policiesByTimeOffType'], 'timeOffType.name', '[UK] Holiday leave', 'timeOffType.uri', '')
        )

        get_enddate_for_updating_loginname = rail.PythonOperator(
            task_id = 'get_enddate_for_updating_loginname',
            python_callable=lambda: datetime.now().strftime("%m%d%y")
        )

        if_division_displaytext_present_15 = rail.IfOperator(
            task_id='if_division_displaytext_present_15',
            test=lambda: rail.result('bulk_get_users3_9') and rail.result('bulk_get_users3_9')[0]['divisionSchedule'] and rail.result(
                'bulk_get_users3_9')[0]['divisionSchedule'][0]['division'] and rail.result(
                'bulk_get_users3_9')[0]['divisionSchedule'][0]['division']['displayText'] and rail.result(
                'bulk_get_users3_9')[0]['divisionSchedule'][0]['division']['displayText'] != 'United Kingdom',
            yes_task="if_userdetails_isenabled_is_true_16",
            no_task="if_division_displaytext_blank_30",
        )

        if_userdetails_isenabled_is_true_16 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_16',
            test=lambda: (rail.result('bulk_get_users3_9')[
                0]['userDetails']['isEnabled']),
            yes_task="trigger_child_holiday_timeofftype_termination_proration_assignment",
            no_task="if_userdetails_isenabled_is_not_true_24",
        )

        trigger_child_holiday_timeofftype_termination_proration_assignment = rail.TriggerDagRunOperator(
            task_id = 'trigger_child_holiday_timeofftype_termination_proration_assignment',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_user_import_holiday_timeoff_type_termination_proration_assignment_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{dag_run.conf.callerjobid}}",
                "userloginname": "{{ dag_run.conf.employeeid }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "timeoffuri": "{{ result('log_holiday_leavetimeoff_uri_14') }}",
                "timeofftype": "[UK] Holiday leave",
                "disabledate": "{{ current_time('%d/%m/%Y') }}"
            }
        )

        wait_for_holiday_timeoff_type_termination_proration_assignment = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_holiday_timeoff_type_termination_proration_assignment',
            execution_timeout = timedelta(config.execution_timeout_days),
            dag_runs="{{result('trigger_child_holiday_timeofftype_termination_proration_assignment')}}"
        )

        trigger_child_uk_holiday_timeofftype_termination_proration_assignment = rail.TriggerDagRunOperator(
            task_id = 'trigger_child_uk_holiday_timeofftype_termination_proration_assignment',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_user_import_holiday_timeoff_type_termination_proration_assignment_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{dag_run.conf.callerjobid}}",
                "userloginname": "{{ dag_run.conf.employeeid }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "timeoffuri": "{{ result('get_time_off_type_assignments_for_user_15') }}",
                "timeofftype": "[ES] Holidays",
                "disabledate": "{{ current_time('%d/%m/%Y') }}"
            }
        )

        wait_for_uk_holiday_timeoff_type_termination_proration_assignment = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_uk_holiday_timeoff_type_termination_proration_assignment',
            execution_timeout = timedelta(config.execution_timeout_days),
            dag_runs="{{result('trigger_child_uk_holiday_timeofftype_termination_proration_assignment')}}"
        )




        disable_login_17 = rail.RepliconServiceOperator(
            task_id='disable_login_17',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        updateloginname_19 = rail.RepliconServiceOperator(
            task_id='updateloginname_19',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ result('bulk_get_users3_9')[0].securityConfiguration.loginName }}{{ result('get_enddate_for_updating_loginname') }}",
                "password": "Replicon@12#",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        trigger_child_timesheet_recalculation = rail.TriggerDagRunOperator(
            task_id='trigger_child_timesheet_recalculation',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_user_import_timesheet_recalculation_child_{config.instance}',
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
            trigger_dag_id=f'michaelkorstna_uk_user_import_add_user_child_{config.instance}',
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
                "yearlyentitlementuri": "{{dag_run.conf.yearlyentitlementuri}}",
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
            yes_task="updateloginname_26",
            no_task="catch_error",
        )

        updateloginname_26 = rail.RepliconServiceOperator(
            task_id='updateloginname_26',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ result('bulk_get_users3_9')[0].securityConfiguration.loginName }}{{ result('get_enddate_for_updating_loginname') }}",
                "password": "Replicon@12#",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        trigger_child_user_add = rail.TriggerDagRunOperator(
            task_id='trigger_child_user_add',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_user_import_add_user_child_{config.instance}',
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
                "yearlyentitlementuri": "{{dag_run.conf.yearlyentitlementuri}}",
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
            yes_task="updateloginname_33",
            no_task="gototask_check_user_enabled",
        )

        gototask_check_user_enabled = rail.EmptyOperator(
            task_id = 'gototask_check_user_enabled'
        )

        updateloginname_33 = rail.RepliconServiceOperator(
            task_id='updateloginname_33',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ result('bulk_get_users3_9')[0].securityConfiguration.loginName }}{{ result('get_enddate_for_updating_loginname') }}",
                "password": "Replicon@12#",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        trigger_child_to_add_user = rail.TriggerDagRunOperator(
            task_id='trigger_child_to_add_user',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_user_import_add_user_child_{config.instance}',
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
                "yearlyentitlementuri": "{{dag_run.conf.yearlyentitlementuri}}",
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
            trigger_dag_id=f'michaelkorstna_uk_user_import_add_user_child_{config.instance}',
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
                "yearlyentitlementuri": "{{dag_run.conf.yearlyentitlementuri}}",
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
            trigger_dag_id=f'michaelkorstna_uk_user_import_timesheet_recalculation_child_{config.instance}',
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
            no_task="if_timezone_unequal_current_value",
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

        if_timezone_unequal_current_value=rail.IfOperator(
            task_id='if_timezone_unequal_current_value',
            test=lambda: rail.result('get_required_fields_from_mapper')['timezone'] and rail.result(
                'get_required_fields_from_mapper')['timezone'] != ( rail.result('bulk_get_users3_9')[0]['timeZone']['uri'] if rail.result(
                    'bulk_get_users3_9')[0]['timeZone'] else ''),
            yes_task="update_time_zone_for_user_84",
            no_task="if_request_businesstitle_present_80",
        )

        update_time_zone_for_user_84=rail.RepliconServiceOperator(
            task_id='update_time_zone_for_user_84',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeZoneUri": rail.result('get_required_fields_from_mapper')['timezone']
            }
        )

        insert_to_list_85=rail.SetVariableOperator(
            task_id='insert_to_list_85',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "TimeZone updated"
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
                "effectivedate": (datetime.strptime(get_date_string(rail.result(
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
            python_callable= lambda: (datetime.strptime(get_date_string(
                rail.result('foreach_payrule_script_schedule')['effectiveDate']),"%d/%m/%Y")).strftime("%Y-%m-%d")
        )

        def get_first_day_of_next_month(datetimeobj):
            return (datetimeobj + relativedelta(months=1)).replace(day=1)

        if_effectivedate_lessthan_first_day_next_month=rail.IfOperator(
            task_id='if_effectivedate_lessthan_first_day_next_month',
            test=lambda: datetime.strptime(rail.result(
                'get_schedule_effective_date'), "%Y-%m-%d") < ( get_first_day_of_next_month(datetime.now()) ),
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
            no_task="get_required_payrule_namebasedon_jobcode",
        )

        get_schedule_with_max_effectivedate=rail.PythonOperator(
            task_id='get_schedule_with_max_effectivedate',
            python_callable= lambda: max(rail.get_dag_run_var(
                'payrule_schedule'), key=lambda x: x['effectivedate'])
        )

        get_current_payrule_name=rail.PythonOperator(
            task_id='get_current_payrule_name',
            python_callable= lambda: (rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('payrule_schedule'),'effectivedate',rail.result(
                'get_schedule_with_max_effectivedate')['effectivedate'],'name','')).lower()
        )

        def get_requiredpayrule_basedon_jobcode(dag_run):
            mapper_entries = rail.result('michael_kors_gmbh_user_sync_master_mapper_uk_search_entries_5')
            payrulebasedonjobprofile = list(filter(lambda entry: entry['type'] == 'Payrule' and entry['identifier__1'] == dag_run.conf[
                'jobprofile'],mapper_entries))
            payrulebasedonlocation = list(filter(lambda entry: entry['type'] == 'Payrule' and entry['identifier__1'] == ( (
                "MK London Office" if "MK London Office" in dag_run.conf['location'] else 'Default') if dag_run.conf[
                'location'] else 'Default'),mapper_entries))
            return payrulebasedonjobprofile[0]['value'] if payrulebasedonjobprofile else (payrulebasedonlocation[0]['value'] if payrulebasedonlocation else '')

        get_required_payrule_namebasedon_jobcode=rail.PythonOperator(
            task_id='get_required_payrule_namebasedon_jobcode',
            python_callable=get_requiredpayrule_basedon_jobcode
        )

        if_currentpayrulename_unequal_required_payrule=rail.IfOperator(
            task_id='if_currentpayrulename_unequal_required_payrule',
            test=lambda: not(rail.result('get_current_payrule_name')) or ((rail.result('get_current_payrule_name')).lower() != (rail.result(
                'get_required_payrule_namebasedon_jobcode')).lower()),
            yes_task="get_all_scriptsforpayrule",
            no_task="if_request_defaultweeklyhours_present_91",
        )

        get_all_scriptsforpayrule=rail.RepliconServiceOperator(
            task_id='get_all_scriptsforpayrule',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response,'displayText',rail.result(
                'get_required_payrule_namebasedon_jobcode'),'uri','')
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
                "value": '''Payrule "{{ result('get_required_payrule_namebasedon_jobcode') }}" not available in Replicon'''
            }
        )

        def get_value_tobe_inserted_in_payrulelist():
            firstday_ofnextmonth = get_first_day_of_next_month(datetime.now())
            return {
                "effectiveDate": {
                    "year": firstday_ofnextmonth.year,
                    "month": firstday_ofnextmonth.month,
                    "day": firstday_ofnextmonth.day
                },
                "payRuleScript": {
                    "uri": rail.result('get_all_scriptsforpayrule'),
                    "parentUri": null,
                    "name": null
                }
            }

        add_schedule_to_payrule_list=rail.SetVariableOperator(
            task_id='add_schedule_to_payrule_list',
            append=True,
            name='{{ result("declare_payrule_list").name }}',
            value=get_value_tobe_inserted_in_payrulelist
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

        trigger_child_holiday_timeoff_type_proration_assignment = rail.TriggerDagRunOperator(
            task_id='trigger_child_holiday_timeoff_type_proration_assignment',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_user_import_timeoff_type_uk_holiday_proration_assignment_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "callerjobid": dag_run.conf['callerjobid'],
                "userloginname": dag_run.conf['employeeid'],
                "useruri": dag_run.conf['useruri'],
                "startdate": datetime.now().strftime("%d/%m/%Y"),
                "type": "Update",
                "timeoffuri": rail.result('log_holiday_leavetimeoff_uri_14'),
                "scheduledweeklyhours": (40 if float(dag_run.conf['scheduledweeklyhours']) > 40 else dag_run.conf[
                    'scheduledweeklyhours']) if dag_run.conf['scheduledweeklyhours'] else 40,
                "fullpart": ("Full Time" if float(dag_run.conf['scheduledweeklyhours']) > 40 else 'Part Time') if dag_run.conf[
                    'scheduledweeklyhours'] else 'Full Time',
                "timeofftype": "[UK] Holiday leave",
                "actualstartdate": rail.result('log_startdate_10'),
                "yearlyentitlement": dag_run.conf['yearlyentitlementuri']
            }
        )

        wait_for_child_holiday_timeoff_type_proration_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_holiday_timeoff_type_proration_assignment',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_holiday_timeoff_type_proration_assignment") }}'
        )

        add_log_holiday_leave_policy_updated = rail.SetVariableOperator(
            task_id='add_log_holiday_leave_policy_updated',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "[UK] Holiday leave policy updated"
            }
        )

        log_sick_leave_timeoff_uri = rail.PythonOperator(
            task_id = 'log_sick_leave_timeoff_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_9')[0]['timeOffTypePolicySummary']['policiesByTimeOffType'], 'timeOffType.name', '[UK] Sick Leave', 'timeOffType.uri', '')
        )

        trigger_child_timeoff_type_proration_assignment = rail.TriggerDagRunOperator(
            task_id='trigger_child_timeoff_type_proration_assignment',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_user_import_timeoff_type_proration_assignment_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "callerjobid": dag_run.conf['callerjobid'],
                "userloginname": dag_run.conf['employeeid'],
                "useruri": dag_run.conf['useruri'],
                "startdate": datetime.now().strftime("%d/%m/%Y"),
                "type": "Update",
                "timeoffuri": rail.result('log_sick_leave_timeoff_uri'),
                "scheduledweeklyhours": (40 if float(dag_run.conf['scheduledweeklyhours']) > 40 else dag_run.conf[
                    'scheduledweeklyhours']) if dag_run.conf['scheduledweeklyhours'] else 40,
                "fullpart": ("Full Time" if float(dag_run.conf['scheduledweeklyhours']) > 40 else 'Part Time') if dag_run.conf[
                    'scheduledweeklyhours'] else 'Full Time',
                "timeofftype": "[UK] Sick Leave",
                "actualstartdate": rail.result('log_startdate_10')
            }
        )

        wait_for_child_timeoff_type_proration_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_timeoff_type_proration_assignment',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_timeoff_type_proration_assignment") }}'
        )

        add_log_sick_leave_policy_updated=rail.SetVariableOperator(
            task_id='add_log_sick_leave_policy_updated',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "[UK] Sick Leave policy updated"
            }
        )

        if_request_weeklyscheduleuri_present_120 = rail.IfOperator(
            task_id='if_request_weeklyscheduleuri_present_120',
            test=lambda dag_run: dag_run.conf['weeklyscheduleuri'] and dag_run.conf['weeklyscheduleuri'] != (rail.result(
                'get_effective_user_group_membership_10')['serviceCenters'][0]['serviceCenter']['serviceCenter']['uri'] if rail.result(
                'get_effective_user_group_membership_10')['serviceCenters'] and rail.result(
                'get_effective_user_group_membership_10')['serviceCenters'][0]['serviceCenter'] and rail.result(
                'get_effective_user_group_membership_10')['serviceCenters'][0]['serviceCenter']['serviceCenter'] else ''),
            yes_task="get_weeklyscheduleeffectivedate_object",
            no_task="if_request_originalhiredate_present_147",
        )

        get_weeklyscheduleeffectivedate_object = rail.PythonOperator(
            task_id = 'get_weeklyscheduleeffectivedate_object',
            python_callable=lambda dag_run: get_date_object(dag_run.conf['weeklyscheduleeffectivedate'])
        )

        apply_user_modifications2_updatelocation_149=rail.RepliconServiceOperator(
            task_id='apply_user_modifications2_updatelocation_149',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "serviceCenterScheduleToApply": {
                    "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementServiceCenterSchedule": [],
                    "updateServiceCenterScheduleOverDateRange": {
                        "replacementServiceCenterScheduleEntries": [
                            {
                                "serviceCenter": {
                                "uri": "{{ dag_run.conf.weeklyscheduleuri }}",
                                "parentUri": null,
                                "name": null
                                },
                                "effectiveDate": {
                                "year": "{{result('get_weeklyscheduleeffectivedate_object').year}}",
                                "month": "{{result('get_weeklyscheduleeffectivedate_object').month}}",
                                "day": "{{result('get_weeklyscheduleeffectivedate_object').day}}",
                                }
                            }
                        ],
                        "endDate": null
                    }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
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
            no_task="if_department_uri_unequal_current",
        )

        if_request_employeeid_not_equals_to_dataworkato_servicereceive_requestrequestmanagerid_152 = rail.IfOperator(
            task_id='if_request_employeeid_not_equals_to_dataworkato_servicereceive_requestrequestmanagerid_152',
            test='''{{ dag_run.conf.employeeid != result('get_required_fields_from_mapper').manager }}''',
            yes_task="declare_list_153",
            no_task="if_request_employeeid_equals_to_dataworkato_servicereceive_requestrequestmanagerid_189",
        )

        declare_list_153 = rail.SetVariableOperator(
            task_id='declare_list_153',
            append=False,
            name='supervisorschedule',
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
            test=lambda: bool(rail.get_dag_run_var('supervisorschedule')),
            yes_task="log_max_effectivedate_165",
            no_task="if_log_currentsupervisorloginname_166_blank_167",
        )

        log_max_effectivedate_165 = rail.PythonOperator(
            task_id='log_max_effectivedate_165',
            python_callable=lambda: max(rail.get_dag_run_var(
                'supervisorschedule'), key=lambda x: x['effectivedate'])
        )

        log_currentsupervisorloginname_166 = rail.PythonOperator(
            task_id='log_currentsupervisorloginname_166',
            python_callable=lambda: (rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var(
                'supervisorschedule'), 'effectivedate', rail.result('log_max_effectivedate_165')['effectivedate'], 'loginname', '')).lower()
        )

        if_log_currentsupervisorloginname_166_blank_167 = rail.IfOperator(
            task_id='if_log_currentsupervisorloginname_166_blank_167',
            #pylint: disable = line-too-long
            test="{{ result('log_currentsupervisorloginname_166') | is_falsy  or result('log_currentsupervisorloginname_166') != result('get_required_fields_from_mapper').manager }}",
            yes_task="search_users_168",
            no_task="if_request_employeeid_equals_to_dataworkato_servicereceive_requestrequestmanagerid_189",
        )

        def get_supervisor_uri_and_status(response):
            users_found = response['rows']
            supervisor = {}
            for user in users_found:
                if user['cells'][0]['textValue'] == rail.result('get_required_fields_from_mapper')['manager']:
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
                            "text": "{{ result('get_required_fields_from_mapper').manager }}",
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
                "supervisorloginname": "{{ result('get_required_fields_from_mapper').manager }}",
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
                "supervisorloginname": "{{ result('get_required_fields_from_mapper').manager }}",
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
                rail.result('michael_kors_gmbh_user_sync_master_mapper_uk_search_entries_5')))], ';')
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
            test='''{{ dag_run.conf.employeeid == result('get_required_fields_from_mapper').manager }}''',
            yes_task="insert_to_list_190",
            no_task="if_department_uri_unequal_current",
        )

        insert_to_list_190 = rail.SetVariableOperator(
            task_id='insert_to_list_190',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": "Supervisor not assigned/updated since the user and manager IDs are same"
            }
        )

        if_department_uri_unequal_current = rail.IfOperator(
            task_id='if_department_uri_unequal_current',
            test=lambda dag_run: dag_run.conf['departmenturi'] and dag_run.conf['departmenturi'] != (rail.result(
                'get_effective_user_group_membership_10')['departments'][0]['department']['department']['uri'] if rail.result(
                'get_effective_user_group_membership_10')['departments'] and rail.result(
                'get_effective_user_group_membership_10')['departments'][0]['department'] and rail.result(
                'get_effective_user_group_membership_10')['departments'][0]['department']['department'] else ''),
            yes_task="apply_user_modifications2_updatedepartment_197",
            no_task="if_locationuri_unequal_current",
        )

        apply_user_modifications2_updatedepartment_197=rail.RepliconServiceOperator(
            task_id='apply_user_modifications2_updatedepartment_197',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "departmentGroupScheduleToApply": {
                    "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementDepartmentGroupSchedule": [],
                    "updateDepartmentGroupScheduleOverDateRange": {
                        "replacementDepartmentGroupScheduleEntries": [
                            {
                                "departmentGroup": {
                                "uri": "{{ dag_run.conf.departmenturi }}",
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
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
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_locationuri_unequal_current = rail.IfOperator(
            task_id='if_locationuri_unequal_current',
            test=lambda dag_run: dag_run.conf['locationuri'] and dag_run.conf['locationuri'] != (rail.result(
                'get_effective_user_group_membership_10')['locations'][0]['location']['location']['uri'] if rail.result(
                'get_effective_user_group_membership_10')['locations'] and rail.result(
                'get_effective_user_group_membership_10')['locations'][0]['location'] and rail.result(
                'get_effective_user_group_membership_10')['locations'][0]['location']['location'] else ''),
            yes_task="apply_user_modifications2_updatelocation_199",
            no_task="if_costcenteruri_unequal_current",
        )

        apply_user_modifications2_updatelocation_199=rail.RepliconServiceOperator(
            task_id='apply_user_modifications2_updatelocation_199',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "locationScheduleToApply": {
                    "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                    "replacementLocationSchedule": [],
                    "updateLocationScheduleOverDateRange": {
                        "replacementLocationScheduleEntries": [
                        {
                            "location": {
                            "uri": "{{ dag_run.conf.locationuri }}",
                            "parentUri": null,
                            "name": null
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
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_costcenteruri_unequal_current = rail.IfOperator(
            task_id='if_costcenteruri_unequal_current',
            test=lambda dag_run: dag_run.conf['costcenteruri'] and dag_run.conf['costcenteruri'] != (rail.result(
                'get_effective_user_group_membership_10')['costCenters'][0]['costCenter']['costCenter']['uri'] if rail.result(
                'get_effective_user_group_membership_10')['costCenters'] and rail.result(
                'get_effective_user_group_membership_10')['costCenters'][0]['costCenter'] and rail.result(
                'get_effective_user_group_membership_10')['costCenters'][0]['costCenter']['costCenter'] else ''),
            yes_task="apply_user_modifications2_updatecostcenter_201",
            no_task="add_final_log_for_user",
        )

        apply_user_modifications2_updatecostcenter_201=rail.RepliconServiceOperator(
            task_id='apply_user_modifications2_updatecostcenter_201',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
            "user": {
                "uri": "{{ dag_run.conf.useruri }}",
                "loginName": null,
                "parameterCorrelationId": null
            },
            "modifications": {
                "costCenterScheduleToApply": {
                "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                "replacementCostCenterSchedule": [],
                "updateCostCenterScheduleOverDateRange": {
                    "replacementCostCenterScheduleEntries": [
                    {
                        "costCenter": {
                        "uri": "{{ dag_run.conf.costcenteruri }}",
                        "parentUri": null,
                        "name": null
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
                }
            },
            "userModificationOptionUri": "urn:replicon:user-modification-option:save"
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
            trigger_dag_id=f'michaelkorstna_uk_user_import_timesheet_recalculation_child_{config.instance}',
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
        declare_list_2 >> declare_list_3 >> michael_kors_gmbh_user_sync_master_mapper_uk_search_entries_5 >> if_first_id_blank_6
        if_first_id_blank_6 >> rail.Label(
            'Yes') >> michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_michael_kors_gmbh_user_sync_logs_add_entry_297_297_7 >> catch_error
        if_first_id_blank_6 >> rail.Label(
            'No') >> bulk_get_users3_9 >> get_required_fields_from_mapper >> get_effective_user_group_membership_10 >> log_startdate_10
        log_startdate_10 >> invoke_custom_ruby_code_todays_date_11
        invoke_custom_ruby_code_todays_date_11 >> get_time_off_type_assignments_for_user_15 >> log_holiday_leavetimeoff_uri_14
        log_holiday_leavetimeoff_uri_14 >> get_enddate_for_updating_loginname >> if_division_displaytext_present_15
        if_division_displaytext_present_15 >> rail.Label(
            'Yes') >> if_userdetails_isenabled_is_true_16
        if_userdetails_isenabled_is_true_16 >> rail.Label('Yes') >> trigger_child_holiday_timeofftype_termination_proration_assignment
        trigger_child_holiday_timeofftype_termination_proration_assignment >> wait_for_holiday_timeoff_type_termination_proration_assignment
        wait_for_holiday_timeoff_type_termination_proration_assignment >> trigger_child_uk_holiday_timeofftype_termination_proration_assignment
        trigger_child_uk_holiday_timeofftype_termination_proration_assignment >> wait_for_uk_holiday_timeoff_type_termination_proration_assignment
        wait_for_uk_holiday_timeoff_type_termination_proration_assignment >> disable_login_17 >> updateloginname_19
        updateloginname_19 >> trigger_child_timesheet_recalculation
        trigger_child_timesheet_recalculation >> wait_for_child_timesheet_recalculation >> trigger_child_add_user >> wait_for_child_add_user
        wait_for_child_add_user >> log_log_22 >> catch_error
        if_userdetails_isenabled_is_true_16 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_24
        if_userdetails_isenabled_is_not_true_24 >> rail.Label(
            'Yes') >> updateloginname_26 >> trigger_child_user_add >> wait_for_child_user_add >> log_log_28 >> catch_error
        if_userdetails_isenabled_is_not_true_24 >> rail.Label(
            'No') >> catch_error
        if_division_displaytext_present_15 >> rail.Label(
            'No') >> if_division_displaytext_blank_30
        if_division_displaytext_blank_30 >> rail.Label(
            'Yes') >> gototask_check_user_not_enabled >> if_userdetails_isenabled_is_not_true_31
        if_userdetails_isenabled_is_not_true_31 >> rail.Label(
            'Yes') >> updateloginname_33 >> trigger_child_to_add_user >> wait_for_child_to_add_user >> log_log_35 >> catch_error
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
            'Yes') >> update_email_77 >> insert_to_list_78 >> if_timezone_unequal_current_value
        if_request_workemail_present_76 >> rail.Label(
            'No') >> if_timezone_unequal_current_value
        if_timezone_unequal_current_value >> rail.Label('Yes') >> update_time_zone_for_user_84 >> insert_to_list_85 >> if_request_businesstitle_present_80
        if_timezone_unequal_current_value >> rail.Label('No') >> if_request_businesstitle_present_80
        if_request_businesstitle_present_80 >> rail.Label(
            'Yes') >> update_text_value_business_title_81 >> insert_to_list_82 >> if_request_jobprofile_present_87
        if_request_businesstitle_present_80 >> rail.Label(
            'No') >> if_request_jobprofile_present_87
        if_request_jobprofile_present_87 >> rail.Label(
            'Yes') >> update_text_value_job_profile_88 >> insert_to_list_89 >> declare_payrule_schedule >> declare_payrule_list
        declare_payrule_list >> if_urn_in_payrulescript_schedule
        if_urn_in_payrulescript_schedule >> rail.Label('Yes') >> foreach_payrule_script_schedule >> if_effectivedate_not_present
        if_effectivedate_not_present >> rail.Label('Yes') >> add_item_to_payrule_schedule_list >> add_item_to_payrule_list
        add_item_to_payrule_list >> foreach_payrule_script_schedule_end
        if_effectivedate_not_present >> rail.Label('No') >> get_schedule_effective_date >> if_effectivedate_lessthan_first_day_next_month
        if_effectivedate_lessthan_first_day_next_month >> rail.Label('Yes') >> insert_to_payrule_schedule_list >> if_effectivedate_unequal_firstday_next_month
        if_effectivedate_lessthan_first_day_next_month >> rail.Label('No') >> if_effectivedate_unequal_firstday_next_month
        if_effectivedate_unequal_firstday_next_month >> rail.Label('Yes') >> insert_to_payrule_list >> foreach_payrule_script_schedule_end
        if_effectivedate_unequal_firstday_next_month >> rail.Label('No') >> foreach_payrule_script_schedule_end
        if_urn_in_payrulescript_schedule >> rail.Label('No') >> if_payrule_schedule_list_has_data
        foreach_payrule_script_schedule >> foreach_payrule_script_schedule_end >> if_payrule_schedule_list_has_data
        if_payrule_schedule_list_has_data >> rail.Label('Yes') >> get_schedule_with_max_effectivedate >> get_current_payrule_name
        get_current_payrule_name >> get_required_payrule_namebasedon_jobcode
        if_payrule_schedule_list_has_data >> rail.Label('No') >> get_required_payrule_namebasedon_jobcode >> if_currentpayrulename_unequal_required_payrule
        if_currentpayrulename_unequal_required_payrule >> rail.Label('Yes') >> get_all_scriptsforpayrule >> if_required_payrule_script_uri_not_present
        if_required_payrule_script_uri_not_present >> rail.Label('Yes') >> add_log_payrule_not_available >> if_request_defaultweeklyhours_present_91
        if_required_payrule_script_uri_not_present >> rail.Label('No') >> add_schedule_to_payrule_list >> get_final_payrule_schedule
        get_final_payrule_schedule >> put_pay_rule_script_assignment_schedule_for_user >> add_log_payrule_updated >> if_request_defaultweeklyhours_present_91
        if_currentpayrulename_unequal_required_payrule >> rail.Label('No') >> if_request_defaultweeklyhours_present_91
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
            'Yes') >> update_numeric_value_scheduled_weekly_hours_116 >> insert_to_list_117 >> trigger_child_holiday_timeoff_type_proration_assignment
        trigger_child_holiday_timeoff_type_proration_assignment >> wait_for_child_holiday_timeoff_type_proration_assignment
        wait_for_child_holiday_timeoff_type_proration_assignment >> add_log_holiday_leave_policy_updated
        add_log_holiday_leave_policy_updated >> log_sick_leave_timeoff_uri >> trigger_child_timeoff_type_proration_assignment
        trigger_child_timeoff_type_proration_assignment >> wait_for_child_timeoff_type_proration_assignment >> add_log_sick_leave_policy_updated
        add_log_sick_leave_policy_updated >> if_request_weeklyscheduleuri_present_120
        if_request_weeklyscheduleuri_present_120 >> rail.Label(
            'Yes') >> get_weeklyscheduleeffectivedate_object >> apply_user_modifications2_updatelocation_149 >> if_request_originalhiredate_present_147
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
            'Yes') >> insert_to_list_190 >> if_department_uri_unequal_current
        if_request_employeeid_equals_to_dataworkato_servicereceive_requestrequestmanagerid_189 >> rail.Label(
            'No') >> if_department_uri_unequal_current
        if_request_managerid_present_151 >> rail.Label(
            'No') >> if_department_uri_unequal_current
        if_department_uri_unequal_current
        if_department_uri_unequal_current >> rail.Label(
            'Yes') >> apply_user_modifications2_updatedepartment_197 >> if_locationuri_unequal_current
        if_department_uri_unequal_current >> rail.Label(
            'No') >> if_locationuri_unequal_current
        if_locationuri_unequal_current >> rail.Label(
            'Yes') >> apply_user_modifications2_updatelocation_199 >> if_costcenteruri_unequal_current
        if_locationuri_unequal_current >> rail.Label(
            'No') >> if_costcenteruri_unequal_current
        if_costcenteruri_unequal_current >> rail.Label(
            'Yes') >> apply_user_modifications2_updatecostcenter_201 >> add_final_log_for_user
        if_costcenteruri_unequal_current >> rail.Label(
            'No') >> add_final_log_for_user >> if_entry_col4_not_equals_to_nochangetotheuserrecordinreplicon_294
        if_entry_col4_not_equals_to_nochangetotheuserrecordinreplicon_294 >> rail.Label(
            'Yes') >> trigger_timesheet_recalculation_child
        trigger_timesheet_recalculation_child >> wait_for_timesheet_recalculation_child >> catch_error
        if_entry_col4_not_equals_to_nochangetotheuserrecordinreplicon_294 >> rail.Label(
            'No') >> catch_error >> add_log_for_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
