
from datetime import timedelta, datetime
import itertools
import pendulum
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
from ge.user_sync_portugal.portugal_master_mapper import portugal_master_mapper
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_user_sync_portugal_user_update_v1_0_{config.instance}',
        description=f'GE_portugal User Update V1.0 {config.instance}',
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
            no_task='declare_list_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_2',
            end_task='log_to_sumo',
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

        bulk_get_users3_5 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_5',
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

        log_startdate_6 = rail.PythonOperator(
            task_id='log_startdate_6',
            python_callable=lambda: rail.render_template(
                "{{ result('bulk_get_users3_5')[0].userDetails.employmentDateRange.startDate.day }}/{{ result('bulk_get_users3_5')[0].userDetails.employmentDateRange.startDate.month }}/{{ result('bulk_get_users3_5')[0].userDetails.employmentDateRange.startDate.year }}")
        )

        ge_portugal_user_sync_master_mapper_search_entries_7 = rail.PythonOperator(
            task_id='ge_portugal_user_sync_master_mapper_search_entries_7',
            python_callable=lambda dag_run:  list(
                filter(lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'], portugal_master_mapper))
        )

        if_request_legalentity_present_dataworkato_servicereceive_requestrequestsupervisorssoid_8 = rail.IfOperator(
            task_id='if_request_legalentity_present_dataworkato_servicereceive_requestrequestsupervisorssoid_8',
            test='''{{ dag_run.conf.LegalEntity | is_truthy }}''',
            yes_task="declare_list_9",
            no_task="if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_34",
        )

        declare_list_9 = rail.SetVariableOperator(
            task_id='declare_list_9',
            append=False,
            name='legal entiry schedule',
            value=[]
        )

        declare_list_10 = rail.SetVariableOperator(
            task_id='declare_list_10',
            append=False,
            name='costcenterlist',
            value=[]
        )

        def get_datetime_obj(effectiveDate):
            year = effectiveDate['year']
            month = effectiveDate['month']
            day = effectiveDate['day']
            return datetime.strptime(f"{year}/{month}/{day}", '%Y/%m/%d')

        def costcenter_schedule_data(dag_run):
            derived_costcenter_schedules = []
            derived_legal_entity_schedules = []
            costcenter_schedules = rail.result('bulk_get_users3_5')[
                0]['costCenterSchedule']
            for costcenter_schedule in costcenter_schedules:
                if costcenter_schedule['effectiveDate']:
                    effective_date = get_datetime_obj(
                        costcenter_schedule['effectiveDate'])
                    current_date = pendulum.now(config.pacific_timezone).date(
                    ) if dag_run.conf['HireEffectiveDate'] else datetime.strptime(dag_run.conf['HireEffectiveDate'], '%d/%m/%Y')
                    if effective_date.date() < current_date:
                        derived_legal_entity_schedules.append({
                            "uri": costcenter_schedule['costCenter']['uri'],
                            "name": costcenter_schedule['costCenter']['displayText'],
                            "date": effective_date.strftime('%d/%m/%Y')
                        })
                    elif effective_date.date() != current_date:
                        derived_costcenter_schedules.append({
                            "costCenter": {
                                "uri": costcenter_schedule['costCenter']['uri'],
                                "parentUri": None,
                                "name": None
                            },
                            "effectiveDate": {
                                "year": effective_date.year,
                                "month": effective_date.month,
                                "day": effective_date.day
                            }
                        })
                else:
                    employment_start_date = get_datetime_obj(rail.result('bulk_get_users3_5')[
                        0]['userDetails']['employmentDateRange']['startDate'])
                    derived_legal_entity_schedules.append({
                        "uri": costcenter_schedule['costCenter']['uri'],
                        "name": costcenter_schedule['costCenter']['displayText'],
                        "date": employment_start_date.strftime('%d/%m/%Y')
                    })

                    derived_costcenter_schedules.append({
                        "costCenter": {
                            "uri": costcenter_schedule['costCenter']['uri'],
                            "parentUri": None,
                            "name": None
                        },
                        "effectiveDate": None
                    })

            return {
                "costcenter_schedules": derived_costcenter_schedules,
                "legal_entity_schedules": derived_legal_entity_schedules
            }

        log_legal_entity_cost_centerschedule_11 = rail.PythonOperator(
            task_id='log_legal_entity_cost_centerschedule_11',
            python_callable=costcenter_schedule_data
        )

        if_first_uri_present_24 = rail.IfOperator(
            task_id='if_first_uri_present_24',
            test='''{{ result('log_legal_entity_cost_centerschedule_11') | is_truthy and result('log_legal_entity_cost_centerschedule_11').legal_entity_schedules | length > 0 }}''',
            yes_task="log_max_effectivedate_25",
            no_task="log_legal_entitycostcenternameaspermapper_27",
        )

        log_max_effectivedate_25 = rail.PythonOperator(
            task_id='log_max_effectivedate_25',
            python_callable=lambda:  (max(
                datetime.strptime(x['date'], '%d/%m/%Y') for x in rail.result('log_legal_entity_cost_centerschedule_11')['legal_entity_schedules'])).strftime('%d/%m/%Y')
            if rail.result('log_legal_entity_cost_centerschedule_11')['legal_entity_schedules'] else None
        )

        log_current_legal_entitycostcentername_26 = rail.PythonOperator(
            task_id='log_current_legal_entitycostcentername_26',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'log_legal_entity_cost_centerschedule_11')['legal_entity_schedules'], 'date', rail.result('log_max_effectivedate_25'), 'name', "")
        )

        def get_entity_mapper(entity_type, legal_entity):
            entity_types = list(filter(
                lambda x: x['legal_entity'] == entity_type
                and x['type'] == legal_entity, portugal_master_mapper))
            return entity_types[0]['value'] if entity_types else None

        log_legal_entitycostcenternameaspermapper_27 = rail.PythonOperator(
            task_id='log_legal_entitycostcenternameaspermapper_27',
            python_callable=lambda dag_run:  get_entity_mapper(
                dag_run.conf['LegalEntity'], 'Legal Entity')
        )

        if_log_current_legal_entitycostcentername_26_blank_28 = rail.IfOperator(
            task_id='if_log_current_legal_entitycostcentername_26_blank_28',
            test='''{{ result('log_current_legal_entitycostcentername_26') | is_falsy or result('log_current_legal_entitycostcentername_26') | lower != result('log_legal_entitycostcenternameaspermapper_27') | lower }}''',
            yes_task="disable_login_29",
            no_task="if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_34",
        )

        disable_login_29 = rail.RepliconServiceOperator(
            task_id='disable_login_29',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_datetobeused_30 = rail.PythonOperator(
            task_id='log_datetobeused_30',
            python_callable=lambda: pendulum.now(
                config.pacific_timezone).strftime('%m%d%Y')
        )

        updateloginname_31 = rail.RepliconServiceOperator(
            task_id='updateloginname_31',
            endpoint="/services/securityservice1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ result('bulk_get_users3_5')[0].securityConfiguration.loginName }}{{ result('log_datetobeused_30') }}"
            }
        )

        trigger_dag_run_ge_user_sync_portugal_ge_portugal_add_v1_0async_callrecipeforlegalentitychange_32 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_portugal_ge_portugal_add_v1_0async_callrecipeforlegalentitychange_32',
            retries=0,
            items=[1],
            trigger_dag_id=f'ge_portugal_add_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "EmployeeFirstName": dag_run.conf['FirstName'],
                "EmployeeLastName": dag_run.conf['LastName'],
                "EmployeeEmailAddress": dag_run.conf['Email'],
                "OHRID": dag_run.conf['OHRID'],
                "LegalEntityHireDate": dag_run.conf['HireEffectiveDate'],
                "LegacyPayrollID": dag_run.conf['LegacyPayrollID'],
                "EmployeeGender": dag_run.conf['EmployeeGender'],
                "MaritalStatus": dag_run.conf['MaritalStatus'],
                "JobPositionTitle": dag_run.conf['JobPositionTitle'],
                "SupervisorSSOID": dag_run.conf['SupervisorSSOID'],
                "SupervisorName": dag_run.conf['SupervisorName'],
                "AssignmentCategory": dag_run.conf['AssignmentCategory'],
                "DWSStartDate": dag_run.conf['DWSStartDate'],
                "DWSEndDate": dag_run.conf['DWSEndDate'],
                "DWSMonday": dag_run.conf['DWSMonday'],
                "DWSTuesday": dag_run.conf['DWSTuesday'],
                "DWSWednesday": dag_run.conf['DWSWednesday'],
                "DWSThursday": dag_run.conf['DWSThursday'],
                "DWSFriday": dag_run.conf['DWSFriday'],
                "DWSSaturday": dag_run.conf['DWSSaturday'],
                "DWSSunday": dag_run.conf['DWSSunday'],
                "TerminationEffectiveDate": dag_run.conf['TerminationEffectiveDate'],
                "IndustryFocusGroup": dag_run.conf['IndustryFocusGroup'],
                "LegalEntity": dag_run.conf['LegalEntity'],
                "ContractID": dag_run.conf['ContractID'],
                "RadiationFlag": dag_run.conf['RadiationFlag'],
                "PositionCapacity": dag_run.conf['PositionCapacity'],
                "EducationPeriods_StartDate": dag_run.conf['EducationPeriods_StartDate'],
                "EducationPeriods_EndDate": dag_run.conf['EducationPeriods_EndDate'],
                "PreviousEmploymentsPeriods_StartDate": dag_run.conf['PreviousEmploymentsPeriods_StartDate'],
                "PreviousEmploymentsPeriodsEndDate": dag_run.conf['PreviousEmploymentsPeriodsEndDate'],
                "Department_Alstom": dag_run.conf['Department_Alstom'],
                "Salary_Basis": dag_run.conf['Salary_Basis'],
                "OvertimeEligibility": dag_run.conf['OvertimeEligibility'],
                "SuspendAssignmentCategory": dag_run.conf['SuspendAssignmentCategory'],
                "DateofBirth": dag_run.conf['DateofBirth'],
                "Payroll": dag_run.conf['Payroll'],
                "HealthcareProductLineEIT": dag_run.conf['HealthcareProductLineEIT'],
                "JobType": dag_run.conf['JobType'],
                "CareerBand": dag_run.conf['CareerBand'],
                "AdjustedServiceDate": dag_run.conf['AdjustedServiceDate'],
                "Work": dag_run.conf['Work'],
                "HRMSSOID": dag_run.conf['HRMSSOID'],
                "HRMName": dag_run.conf['HRMName'],
                "SpecialWorkSchedule": dag_run.conf['SpecialWorkSchedule'],
                "EducationLevel": dag_run.conf['EducationLevel'],
                "WorktimeSystem": dag_run.conf['WorktimeSystem'],
                "Sub_Biz": dag_run.conf['Sub_Biz'],
                "ContractattributeAnnualvacationeligibility": dag_run.conf['ContractattributeAnnualvacationeligibility'],
                "LocationName": dag_run.conf['LocationName'],
                "AssignmentEffectiveDate": dag_run.conf['AssignmentEffectiveDate'],
                "HireEffectiveDate": dag_run.conf['HireEffectiveDate'],
                "RevTermEffectiveDate": dag_run.conf['RevTermEffectiveDate'],
                "type": "Add",
                "DepartmentUri": dag_run.conf['DepartmentUri'],
                "supervisor_processing_log": dag_run.conf['supervisor_processing_log']
            }
        )

        wait_for_completion_trigger_dag_run_ge_portugal_add_v1_0async_callrecipeforlegalentitychange_32 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_portugal_add_v1_0async_callrecipeforlegalentitychange_32',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_portugal_ge_portugal_add_v1_0async_callrecipeforlegalentitychange_32") }}'
        )

        dummy_operator_1 = rail.EmptyOperator(
            task_id="dummy_operator_1"
        )

        def is_temination_date_reached(dag_run):
            if dag_run.conf['TerminationEffectiveDate'] and dag_run.conf['RevTermEffectiveDate'] is None \
                    and rail.result('bulk_get_users3_5')[0]['userDetails']['isEnabled'] is True:
                temination_date = datetime.strptime(
                    dag_run.conf['TerminationEffectiveDate'], '%d/%m/%Y')
                start_date = datetime.strptime(rail.result(
                    'log_startdate_6'), '%d/%m/%Y')
                if temination_date > start_date + timedelta(days=-1):
                    return True
            return False

        if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_34 = rail.IfOperator(
            task_id='if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_34',
            test=is_temination_date_reached,
            yes_task="update_enddate_36",
            no_task="if_request_terminationeffectivedate_present_skip_disable_39",
        )

        update_enddate_36 = rail.RepliconServiceOperator(
            task_id='update_enddate_36',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": rail.result('bulk_get_users3_5')[0]['userDetails']['employmentDateRange']['startDate']['year'],
                        "month": rail.result('bulk_get_users3_5')[0]['userDetails']['employmentDateRange']['startDate']['month'],
                        "day": rail.result('bulk_get_users3_5')[0]['userDetails']['employmentDateRange']['startDate']['day']
                    },
                    "endDate": {
                        "year": datetime.strptime(dag_run.conf['TerminationEffectiveDate'], '%d/%m/%Y').year,
                        "month": datetime.strptime(dag_run.conf['TerminationEffectiveDate'], '%d/%m/%Y').month,
                        "day": datetime.strptime(dag_run.conf['TerminationEffectiveDate'], '%d/%m/%Y').day
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_37 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_37',
            message="na",
            severity="Success",
            properties={
                "action": "Disable",
                "status": "Success",
                "child_job_id": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}",
                "details": "End Date Updated",
                "OHRID": "{{ dag_run.conf.OHRID }}",
            }
        )

        def is_temination_date_not_reached(dag_run):
            if dag_run.conf['TerminationEffectiveDate'] and dag_run.conf['RevTermEffectiveDate'] is None \
                    and rail.result('bulk_get_users3_5')[0]['userDetails']['isEnabled'] is True:
                temination_date = datetime.strptime(
                    dag_run.conf['TerminationEffectiveDate'], '%d/%m/%Y')
                start_date = datetime.strptime(rail.result(
                    'log_startdate_6'), '%d/%m/%Y')
                if temination_date < start_date:
                    return True
            return False

        if_request_terminationeffectivedate_present_skip_disable_39 = rail.IfOperator(
            task_id='if_request_terminationeffectivedate_present_skip_disable_39',
            test=is_temination_date_not_reached,
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_40",
            no_task="if_userdetails_isenabled_is_not_true_rehire_42",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_40 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_40',
            message="na",
            severity="Skipped",
            properties={
                "action": "Disable",
                "status": "Skipped",
                "child_job_id": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}",
                "details": "End Date not Updated as termination date is prior to start date",
                "OHRID": "{{ dag_run.conf.OHRID }}",
            }
        )

        if_userdetails_isenabled_is_not_true_rehire_42 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_rehire_42',
            test='''{{ result('bulk_get_users3_5')[0].userDetails.isEnabled | is_falsy and dag_run.conf.RevTermEffectiveDate | is_falsy }}''',
            yes_task="if_request_hireeffectivedate_blank_43",
            no_task="if_enddate_day_present_reverse_termination_53",
        )

        if_request_hireeffectivedate_blank_43 = rail.IfOperator(
            task_id='if_request_hireeffectivedate_blank_43',
            test='''{{ dag_run.conf.HireEffectiveDate | is_falsy }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_44",
            no_task="if_enddate_day_blank_46",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_44 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_44',
            message="na",
            severity="Skipped",
            properties={
                "action": "Rehire",
                "status": "Skipped",
                "child_job_id": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}",
                "details": "Hire effective date not available",
                "OHRID": "{{ dag_run.conf.OHRID }}",
            }
        )

        if_enddate_day_blank_46 = rail.IfOperator(
            task_id='if_enddate_day_blank_46',
            test='''{{ result('bulk_get_users3_5')[0].userDetails.employmentDateRange.endDate | is_falsy and result('bulk_get_users3_5')[0].userDetails.employmentDateRange.endDate.day | is_falsy }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_47",
            no_task="log_enddate_49",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_47 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_47',
            message="na",
            severity="Skipped",
            properties={
                "action": "Rehire",
                "status": "Skipped",
                "child_job_id": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}",
                "details": "The existing profile doesn't have an end date in Replicon",
                "OHRID": "{{ dag_run.conf.OHRID }}",
            }
        )

        log_enddate_49 = rail.PythonOperator(
            task_id='log_enddate_49',
            python_callable=lambda:  rail.result('bulk_get_users3_5')[
                0]['userDetails']['employmentDateRange']['endDate']['year']
        )

        updateloginname_50 = rail.RepliconServiceOperator(
            task_id='updateloginname_50',
            endpoint="/services/securityservice1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ result('bulk_get_users3_5')[0].securityConfiguration.loginName }}{{ result('bulk_get_users3_5')[0].userDetails.employmentDateRange.endDate.month }}{{ result('bulk_get_users3_5')[0].userDetails.employmentDateRange.endDate.day }}{{ result('log_enddate_49') }}"
            }
        )

        trigger_dag_run_ge_user_sync_portugal_ge_portugal_add_v1_0async_callrecipeforrehire_51 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_portugal_ge_portugal_add_v1_0async_callrecipeforrehire_51',
            retries=0,
            items=[1],
            trigger_dag_id=f'ge_portugal_add_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "EmployeeFirstName": dag_run.conf['FirstName'],
                "EmployeeLastName": dag_run.conf['LastName'],
                "EmployeeEmailAddress": dag_run.conf['Email'],
                "OHRID": dag_run.conf['OHRID'],
                "LegalEntityHireDate": dag_run.conf['HireEffectiveDate'],
                "LegacyPayrollID": dag_run.conf['LegacyPayrollID'],
                "EmployeeGender": dag_run.conf['EmployeeGender'],
                "MaritalStatus": dag_run.conf['MaritalStatus'],
                "JobPositionTitle": dag_run.conf['JobPositionTitle'],
                "SupervisorSSOID": dag_run.conf['SupervisorSSOID'],
                "SupervisorName": dag_run.conf['SupervisorName'],
                "AssignmentCategory": dag_run.conf['AssignmentCategory'],
                "DWSStartDate": dag_run.conf['DWSStartDate'],
                "DWSEndDate": dag_run.conf['DWSEndDate'],
                "DWSMonday": dag_run.conf['DWSMonday'],
                "DWSTuesday": dag_run.conf['DWSTuesday'],
                "DWSWednesday": dag_run.conf['DWSWednesday'],
                "DWSThursday": dag_run.conf['DWSThursday'],
                "DWSFriday": dag_run.conf['DWSFriday'],
                "DWSSaturday": dag_run.conf['DWSSaturday'],
                "DWSSunday": dag_run.conf['DWSSunday'],
                "TerminationEffectiveDate": dag_run.conf['TerminationEffectiveDate'],
                "IndustryFocusGroup": dag_run.conf['IndustryFocusGroup'],
                "LegalEntity": dag_run.conf['LegalEntity'],
                "ContractID": dag_run.conf['ContractID'],
                "RadiationFlag": dag_run.conf['RadiationFlag'],
                "PositionCapacity": dag_run.conf['PositionCapacity'],
                "EducationPeriods_StartDate": dag_run.conf['EducationPeriods_StartDate'],
                "EducationPeriods_EndDate": dag_run.conf['EducationPeriods_EndDate'],
                "PreviousEmploymentsPeriods_StartDate": dag_run.conf['PreviousEmploymentsPeriods_StartDate'],
                "PreviousEmploymentsPeriodsEndDate": dag_run.conf['PreviousEmploymentsPeriodsEndDate'],
                "Department_Alstom": dag_run.conf['Department_Alstom'],
                "Salary_Basis": dag_run.conf['Salary_Basis'],
                "OvertimeEligibility": dag_run.conf['OvertimeEligibility'],
                "SuspendAssignmentCategory": dag_run.conf['SuspendAssignmentCategory'],
                "DateofBirth": dag_run.conf['DateofBirth'],
                "Payroll": dag_run.conf['Payroll'],
                "HealthcareProductLineEIT": dag_run.conf['HealthcareProductLineEIT'],
                "JobType": dag_run.conf['JobType'],
                "CareerBand": dag_run.conf['CareerBand'],
                "AdjustedServiceDate": dag_run.conf['AdjustedServiceDate'],
                "Work": dag_run.conf['Work'],
                "HRMSSOID": dag_run.conf['HRMSSOID'],
                "HRMName": dag_run.conf['HRMName'],
                "SpecialWorkSchedule": dag_run.conf['SpecialWorkSchedule'],
                "EducationLevel": dag_run.conf['EducationLevel'],
                "WorktimeSystem": dag_run.conf['WorktimeSystem'],
                "Sub_Biz": dag_run.conf['Sub_Biz'],
                "ContractattributeAnnualvacationeligibility": dag_run.conf['ContractattributeAnnualvacationeligibility'],
                "LocationName": dag_run.conf['LocationName'],
                "AssignmentEffectiveDate": dag_run.conf['AssignmentEffectiveDate'],
                "HireEffectiveDate": dag_run.conf['HireEffectiveDate'],
                "RevTermEffectiveDate": dag_run.conf['RevTermEffectiveDate'],
                "type": "Rehire",
                "DepartmentUri": dag_run.conf['DepartmentUri'],
                "supervisor_processing_log": dag_run.conf['supervisor_processing_log']
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_portugal_ge_portugal_add_v1_0async_callrecipeforrehire_51 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_portugal_ge_portugal_add_v1_0async_callrecipeforrehire_51',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_portugal_ge_portugal_add_v1_0async_callrecipeforrehire_51") }}'
        )

        dummy_operator_2 = rail.EmptyOperator(
            task_id="dummy_operator_2"
        )

        if_enddate_day_present_reverse_termination_53 = rail.IfOperator(
            task_id='if_enddate_day_present_reverse_termination_53',
            test='''{{ result('bulk_get_users3_5')[0].userDetails.employmentDateRange.endDate | is_truthy and result('bulk_get_users3_5')[0].userDetails.employmentDateRange.endDate.day | is_truthy }}''',
            yes_task="log_enddate_54",
            no_task="if_request_employeefirstname_present_servicereceive_requestrequestemployeefirstname_61",
        )

        log_enddate_54 = rail.PythonOperator(
            task_id='log_enddate_54',
            python_callable=lambda: rail.render_template(
                "{{ result('bulk_get_users3_5')[0].userDetails.employmentDateRange.endDate.day }}/{{ result('bulk_get_users3_5')[0].userDetails.employmentDateRange.endDate.month }}/{{ result('bulk_get_users3_5')[0].userDetails.employmentDateRange.endDate.year }}")
        )

        def reversetermination_validation(dag_run):
            if dag_run.conf['RevTermEffectiveDate']:
                revtermeffectivedate = datetime.strptime(
                    dag_run.conf['RevTermEffectiveDate'], '%d/%m/%Y')
                employement_enddate = get_datetime_obj(rail.result(
                    'bulk_get_users3_5')[0]['userDetails']['employmentDateRange']['endDate'])
                employment_startdate = get_datetime_obj(rail.result(
                    'bulk_get_users3_5')[0]['userDetails']['employmentDateRange']['startDate'])
                if employment_startdate < revtermeffectivedate < employement_enddate:
                    return True
            return False

        if_request_revtermeffectivedate_present_55 = rail.IfOperator(
            task_id='if_request_revtermeffectivedate_present_55',
            test=reversetermination_validation,
            yes_task="if_userdetails_isenabled_is_not_true_56",
            no_task="if_request_employeefirstname_present_servicereceive_requestrequestemployeefirstname_61",
        )

        if_userdetails_isenabled_is_not_true_56 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_56',
            test='''{{ result('bulk_get_users3_5')[0].userDetails.isEnabled | is_falsy }}''',
            yes_task="enable_login_57",
            no_task="remove_enddate_59",
        )

        enable_login_57 = rail.RepliconServiceOperator(
            task_id='enable_login_57',
            endpoint="/services/securityservice1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        insert_to_list_58 = rail.SetVariableOperator(
            task_id='insert_to_list_58',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "User profile re-enabled, reverse termination date older than end date and newer than start date."
            }
        )

        remove_enddate_59 = rail.RepliconServiceOperator(
            task_id='remove_enddate_59',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": '''{{ result('bulk_get_users3_5')[0].userDetails.employmentDateRange.startDate.year }}''',
                        "month": '''{{ result('bulk_get_users3_5')[0].userDetails.employmentDateRange.startDate.month}}''',
                        "day": '''{{result('bulk_get_users3_5')[0].userDetails.employmentDateRange.startDate.day}}'''
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        insert_to_list_60 = rail.SetVariableOperator(
            task_id='insert_to_list_60',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "End date removed, reverse termination date older than end date and newer than start date."
            }
        )

        if_request_employeefirstname_present_servicereceive_requestrequestemployeefirstname_61 = rail.IfOperator(
            task_id='if_request_employeefirstname_present_servicereceive_requestrequestemployeefirstname_61',
            test='''{{ dag_run.conf.FirstName | is_truthy and dag_run.conf.FirstName | lower != result('bulk_get_users3_5')[0].userDetails.firstName | lower }}''',
            yes_task="update_first_name_62",
            no_task="if_request_employeelastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_64",
        )

        update_first_name_62 = rail.RepliconServiceOperator(
            task_id='update_first_name_62',
            endpoint="/services/userService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.FirstName }}"
            }
        )

        insert_to_list_63 = rail.SetVariableOperator(
            task_id='insert_to_list_63',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "First name updated"
            }
        )

        if_request_employeelastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_64 = rail.IfOperator(
            task_id='if_request_employeelastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_64',
            test='''{{ dag_run.conf.LastName | is_truthy and dag_run.conf.LastName | lower != result('bulk_get_users3_5')[0].userDetails.lastName | lower }}''',
            yes_task="update_last_name_65",
            no_task="if_request_employeeemailaddress_present_67",
        )

        update_last_name_65 = rail.RepliconServiceOperator(
            task_id='update_last_name_65',
            endpoint="/services/userService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.LastName }}"
            }
        )

        insert_to_list_66 = rail.SetVariableOperator(
            task_id='insert_to_list_66',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Last name updated"
            }
        )

        if_request_employeeemailaddress_present_67 = rail.IfOperator(
            task_id='if_request_employeeemailaddress_present_67',
            test='''{{ dag_run.conf.Email | is_truthy and dag_run.conf.Email | lower != result('bulk_get_users3_5')[0].userDetails.emailAddress | lower }}''',
            yes_task="update_email_68",
            no_task="log_valuefor_job_position_title_79",
        )

        update_email_68 = rail.RepliconServiceOperator(
            task_id='update_email_68',
            endpoint="/services/userService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.Email }}"
            }
        )

        insert_to_list_69 = rail.SetVariableOperator(
            task_id='insert_to_list_69',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Email updated"
            }
        )

        def get_custom_text(custom_field_name):
            existing_custom_fields = rail.result('bulk_get_users3_5')[0][
                'userDetails']['customFieldValues']
            custom_infos = list(filter(
                lambda x: x['customField']['displayText'] == custom_field_name, existing_custom_fields))
            return custom_infos[0]['text'] if custom_infos else None

        log_valuefor_job_position_title_79 = rail.PythonOperator(
            task_id='log_valuefor_job_position_title_79',
            python_callable=lambda: get_custom_text('Job/Position Title')
        )

        if_request_jobpositiontitle_present_80 = rail.IfOperator(
            task_id='if_request_jobpositiontitle_present_80',
            test='''{{ dag_run.conf.JobPositionTitle | is_truthy and result('log_valuefor_job_position_title_79') | lower != dag_run.conf.JobPositionTitle | lower }}''',
            yes_task="log_urifor_job_position_title_81",
            no_task="log_valuefor_h_r_m_s_s_o_i_d_84",
        )

        def get_custom_uri(custom_field_name):
            existing_custom_fields = rail.result('bulk_get_users3_5')[0][
                'userDetails']['customFieldValues']
            custom_infos = list(filter(
                lambda x: x['customField']['displayText'] == custom_field_name, existing_custom_fields))
            return custom_infos[0]['customField']['uri'] if custom_infos else None

        log_urifor_job_position_title_81 = rail.PythonOperator(
            task_id='log_urifor_job_position_title_81',
            python_callable=lambda: get_custom_uri('Job/Position Title')
        )

        update_text_value_customfield_82 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_82',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_urifor_job_position_title_81') }}",
                "value": "{{ dag_run.conf.JobPositionTitle }}"
            }
        )

        insert_to_list_83 = rail.SetVariableOperator(
            task_id='insert_to_list_83',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Job/Position Title updated"
            }
        )

        log_valuefor_h_r_m_s_s_o_i_d_84 = rail.PythonOperator(
            task_id='log_valuefor_h_r_m_s_s_o_i_d_84',
            python_callable=lambda: get_custom_text('HRM SSO ID')
        )

        if_request_hrmssoid_present_85 = rail.IfOperator(
            task_id='if_request_hrmssoid_present_85',
            test='''{{ dag_run.conf.HRMSSOID | is_truthy and result('log_valuefor_h_r_m_s_s_o_i_d_84') | lower != dag_run.conf.HRMSSOID | lower }}''',
            yes_task="log_urifor_h_r_m_s_s_o_i_d_86",
            no_task="log_valuefor_h_r_m_name_89",
        )

        log_urifor_h_r_m_s_s_o_i_d_86 = rail.PythonOperator(
            task_id='log_urifor_h_r_m_s_s_o_i_d_86',
            python_callable=lambda: get_custom_uri('HRM SSO ID')
        )

        update_text_value_customfield_87 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_87',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_urifor_h_r_m_s_s_o_i_d_86') }}",
                "value": "{{ dag_run.conf.HRMSSOID }}"
            }
        )

        insert_to_list_88 = rail.SetVariableOperator(
            task_id='insert_to_list_88',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "HRMSSOID updated"
            }
        )

        log_valuefor_h_r_m_name_89 = rail.PythonOperator(
            task_id='log_valuefor_h_r_m_name_89',
            python_callable=lambda: get_custom_text('HRM Name')
        )

        if_request_hrmname_present_90 = rail.IfOperator(
            task_id='if_request_hrmname_present_90',
            test='''{{ dag_run.conf.HRMName | is_truthy and result('log_valuefor_h_r_m_name_89') | lower != dag_run.conf.HRMName.lower }}''',
            yes_task="log_urifor_h_r_m_name_91",
            no_task="log_valuefor_suspend_assignment_category_94",
        )

        log_urifor_h_r_m_name_91 = rail.PythonOperator(
            task_id='log_urifor_h_r_m_name_91',
            python_callable=lambda: get_custom_uri('HRM Name')
        )

        update_text_value_customfield_92 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_92',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_urifor_h_r_m_name_91') }}",
                "value": "{{ dag_run.conf.HRMName }}"
            }
        )

        insert_to_list_93 = rail.SetVariableOperator(
            task_id='insert_to_list_93',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "HRM Name updated"
            }
        )

        log_valuefor_suspend_assignment_category_94 = rail.PythonOperator(
            task_id='log_valuefor_suspend_assignment_category_94',
            python_callable=lambda: get_custom_text(
                'Suspend Assignment Category')
        )

        if_request_suspendassignmentcategory_present_95 = rail.IfOperator(
            task_id='if_request_suspendassignmentcategory_present_95',
            test='''{{ dag_run.conf.SuspendAssignmentCategory | is_truthy and result('log_valuefor_suspend_assignment_category_94') | lower != dag_run.conf.SuspendAssignmentCategory | lower }}''',
            yes_task="log_urifor_suspend_assignment_category_96",
            no_task="if_request_supervisorssoid_present_101",
        )

        log_urifor_suspend_assignment_category_96 = rail.PythonOperator(
            task_id='log_urifor_suspend_assignment_category_96',
            python_callable=lambda: get_custom_uri(
                'Suspend Assignment Category')
        )

        get_all_custom_field_drop_down_options_97 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_97',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_urifor_suspend_assignment_category_96') }}"
            }
        )

        log_uriforsuspendassignmentcategory_98 = rail.PythonOperator(
            task_id='log_uriforsuspendassignmentcategory_98',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_custom_field_drop_down_options_97'), 'displayText', dag_run.conf['SuspendAssignmentCategory'], 'uri')
        )

        update_dropdown_value_customfield_99 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_customfield_99',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_urifor_suspend_assignment_category_96') }}",
                "customFieldDropDownOptionUri": "{{ result('log_uriforsuspendassignmentcategory_98') }}"
            }
        )

        insert_to_list_100 = rail.SetVariableOperator(
            task_id='insert_to_list_100',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Suspend Assignment Category updated"
            }
        )

        if_request_supervisorssoid_present_101 = rail.IfOperator(
            task_id='if_request_supervisorssoid_present_101',
            test='''{{ dag_run.conf.SupervisorSSOID | is_truthy }}''',
            yes_task="if_request_ohrid_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_102",
            no_task="invoke_custom_ruby_code_141",
        )

        if_request_ohrid_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_102 = rail.IfOperator(
            task_id='if_request_ohrid_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_102',
            test='''{{ dag_run.conf.OHRID != dag_run.conf.SupervisorSSOID }}''',
            yes_task="log_supervisorschedule_104",
            no_task="if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_139",
        )

        def get_supervisor_schedules():
            supervisor_schedules = []
            currentsupervisorschedules = rail.result('bulk_get_users3_5')[
                0]['supervisorAssignmentSchedule']
            for super_schedule in currentsupervisorschedules:
                if super_schedule['effectiveDate']:
                    effective_date = get_datetime_obj(
                        super_schedule['effectiveDate'])
                    if effective_date.date() < pendulum.now(config.pacific_timezone).date():
                        supervisor_schedules.append({
                            "loginname": super_schedule['supervisor']['user']['loginName'],
                            "uri": super_schedule['supervisor']['uri'],
                            "effectivedate": effective_date.strftime('%d/%m/%Y'),
                            "name": super_schedule['supervisor']['displayText'],
                        })
                else:
                    effective_date = get_datetime_obj(rail.result('bulk_get_users3_5')[
                        0]['userDetails']['employmentDateRange']['startDate'])
                    supervisor_schedules.append({
                        "loginname": super_schedule['supervisor']['user']['loginName'],
                        "uri": super_schedule['supervisor']['uri'],
                        "effectivedate": effective_date.strftime('%d/%m/%Y'),
                        "name": super_schedule['supervisor']['displayText'],
                    })
            return supervisor_schedules

        log_supervisorschedule_104 = rail.PythonOperator(
            task_id='log_supervisorschedule_104',
            python_callable=get_supervisor_schedules
        )

        if_first_uri_present_114 = rail.IfOperator(
            task_id='if_first_uri_present_114',
            test='''{{ result('log_supervisorschedule_104') | length > 0 }}''',
            yes_task="log_max_effectivedate_115",
            no_task="if_log_currentsupervisorloginname_116_blank_117",
        )

        log_max_effectivedate_115 = rail.PythonOperator(
            task_id='log_max_effectivedate_115',
            python_callable=lambda: (max(
                datetime.strptime(x['effectivedate'], '%d/%m/%Y') for x in rail.result('log_supervisorschedule_104'))).strftime('%d/%m/%Y') if rail.result('log_supervisorschedule_104') else None
        )

        log_currentsupervisorloginname_116 = rail.PythonOperator(
            task_id='log_currentsupervisorloginname_116',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'log_supervisorschedule_104'), 'effectivedate', rail.result('log_max_effectivedate_115'), 'loginname', "")
        )

        if_log_currentsupervisorloginname_116_blank_117 = rail.IfOperator(
            task_id='if_log_currentsupervisorloginname_116_blank_117',
            test='''{{ result('log_currentsupervisorloginname_116') | is_falsy  or result('log_currentsupervisorloginname_116') != dag_run.conf.SupervisorSSOID }}''',
            yes_task="search_users_118",
            no_task="if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_139",
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def compose_user_details(response, loginname):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            users_info = list(filter(lambda x: x['loginname'] == loginname, map(lambda row: {
                'loginname': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))
            return users_info[0] if users_info else None

        search_users_118 = rail.RepliconServicePageOperator(
            task_id='search_users_118',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                "sort": [],
                "filterExpression": {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['SupervisorSSOID'],
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response, dag_run: compose_user_details(
                response, dag_run.conf['SupervisorSSOID'])
        )

        if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisnotavailable_120 = rail.IfOperator(
            task_id='if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisnotavailable_120',
            test='''{{ result('search_users_118') | is_falsy  or result('search_users_118').useruri | is_falsy }}''',
            yes_task="ge_supervisor_assignment_user_import_logs_add_entry_121",
            no_task="if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_123",
        )

        ge_supervisor_assignment_user_import_logs_add_entry_121 = rail.WriteLogOperator(
            task_id='ge_supervisor_assignment_user_import_logs_add_entry_121',
            message="na",
            severity="queued",
            log="{{ dag_run.conf.supervisor_processing_log }}",
            properties=lambda dag_run: {
                "username": dag_run.conf['OHRID'],
                "useruri": dag_run.conf['useruri'],
                "supervisorloginname": dag_run.conf['SupervisorSSOID'],
                "action": "update",
                "childjobid": get_dagrun_ecid(dag_run),
                "supervisoreffectivedate": dag_run.conf['AssignmentEffectiveDate'] if dag_run.conf['AssignmentEffectiveDate'] else
                    pendulum.now(config.pacific_timezone).strftime('%d/%m/%Y'),
                "status": "queued",
                "supervisorusername": dag_run.conf['SupervisorName']
            }
        )

        if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_123 = rail.IfOperator(
            task_id='if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_123',
            test='''{{ result('search_users_118') | is_truthy and result('search_users_118').status == 'False' }}''',
            yes_task="ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_124",
            no_task="if_log_5_present_125",
        )

        ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_124 = rail.WriteLogOperator(
            task_id='ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_124',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="na",
            severity="queued",
            properties=lambda dag_run: {
                "username": dag_run.conf['OHRID'],
                "useruri": dag_run.conf['useruri'],
                "supervisorloginname": dag_run.conf['SupervisorSSOID'],
                "action": "update",
                "childjobid": get_dagrun_ecid(dag_run),
                "supervisoreffectivedate": dag_run.conf['AssignmentEffectiveDate'] if dag_run.conf['AssignmentEffectiveDate'] else
                    pendulum.now(config.pacific_timezone).strftime('%d/%m/%Y'),
                "status": "queued",
                "supervisorusername": dag_run.conf['SupervisorName']
            }
        )

        if_log_5_present_125 = rail.IfOperator(
            task_id='if_log_5_present_125',
            test='''{{ result('search_users_118') | is_truthy and result('search_users_118').useruri and result('search_users_118').status | lower == 'true' }}''',
            yes_task="log_requiredsupervisorpermissiontoassigned_126",
            no_task="if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_139",
        )

        def get_entity_types_from_mapper(dag_run, entity_type, identifier1):
            entity_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity']
                and x['type'] == entity_type
                and x['identifier_1_(_legal_entity_code/_type/_timeoff_type)'] == identifier1, portugal_master_mapper))
            entity_values = [entity['value'] for entity in entity_types]
            return rail.smartjoin_by_delim(entity_values, ';')

        log_requiredsupervisorpermissiontoassigned_126 = rail.PythonOperator(
            task_id='log_requiredsupervisorpermissiontoassigned_126',
            python_callable=lambda dag_run: get_entity_types_from_mapper(
                dag_run, "Permission", "Supervisor")
        )

        get_assigned_permission_sets_for_user2_127 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_127',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_118').useruri }}"
            }
        )

        def get_permission_type(permission_uri):
            permissionset = rail.find_first_by_attr_and_get_attr(rail.result(
                'get_assigned_permission_sets_for_user2_127'), 'policyUri', permission_uri, 'permissionSet')
            return permissionset['name'] if permissionset else None

        log_checkif_manager_permissionsetisassigned_128 = rail.PythonOperator(
            task_id='log_checkif_manager_permissionsetisassigned_128',
            python_callable=lambda: get_permission_type(
                'urn:replicon:policy:supervision')
        )

        log_checkif_end_user_manager_permissionsetisassigned_129 = rail.PythonOperator(
            task_id='log_checkif_end_user_manager_permissionsetisassigned_129',
            python_callable=lambda:  get_permission_type(
                'urn:replicon:policy:user')
        )

        def is_valid_permission():
            if rail.result('log_checkif_manager_permissionsetisassigned_128') is None or \
                rail.result('log_checkif_end_user_manager_permissionsetisassigned_129') is None or \
                    rail.result('log_checkif_manager_permissionsetisassigned_128') not in rail.result('log_requiredsupervisorpermissiontoassigned_126') or \
                    rail.result('log_checkif_end_user_manager_permissionsetisassigned_129') not in rail.result('log_requiredsupervisorpermissiontoassigned_126'):
                return True
            return False

        if_log_supervisorpermissiontoassigned_126_not_contains_manager_permissionsetisassigned_130 = rail.IfOperator(
            task_id='if_log_supervisorpermissiontoassigned_126_not_contains_manager_permissionsetisassigned_130',
            test=is_valid_permission,
            yes_task="get_all_permission_sets_132",
            no_task="invoke_custom_ruby_code_136",
        )

        def get_super_user_permissions(dag_run, entity_type_1, entity_type_2):
            super_permissions = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity']
                and x['type'] == entity_type_1
                and x['identifier_1_(_legal_entity_code/_type/_timeoff_type)'] == entity_type_2, portugal_master_mapper))
            return [permission['value'] for permission in super_permissions] if super_permissions else []

        def get_super_permissions(response, dag_run):
            permissions_to_add = []
            mapper_permissions = get_super_user_permissions(
                dag_run, 'Permission', 'Supervsior')
            if response and mapper_permissions:
                for permission in mapper_permissions:
                    permission_uri = rail.find_first_by_attr_and_get_attr(
                        response, 'name', permission, 'uri')
                    if permission_uri:
                        permissions_to_add.append(permission_uri)
            return permissions_to_add

        get_all_permission_sets_132 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_132',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            log_response=True,
            data_handler=get_super_permissions
        )

        assign_permission_set_to_user_manager_135 = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_permission_set_to_user_manager_135',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda: rail.result('get_all_permission_sets_132'),
            execution_timeout=timedelta(days=14),
            data={
                'userUri': "{{ result('search_users_118').useruri }}",
                'permissionSetUri': '{{ item }}'
            }
        )

        def get_active_date(dag_run, field_name):
            if dag_run.conf[field_name]:
                assigment_eff_date = datetime.strptime(
                    dag_run.conf[field_name], '%d/%m/%Y')
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

        invoke_custom_ruby_code_136 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_136',
            python_callable=lambda dag_run: get_active_date(
                dag_run, "AssignmentEffectiveDate")
        )

        update_supervisor_assignment_schedule_over_date_range_137 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_137',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('search_users_118').useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": '''{{ result('invoke_custom_ruby_code_136').year }}''',
                        "month": '''{{ result('invoke_custom_ruby_code_136').month }}''',
                        "day": '''{{ result('invoke_custom_ruby_code_136').day }}'''
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        insert_to_list_138 = rail.SetVariableOperator(
            task_id='insert_to_list_138',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Supervisor updated"
            }
        )

        if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_139 = rail.IfOperator(
            task_id='if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_139',
            test='''{{ dag_run.conf.OHRID == dag_run.conf.SupervisorSSOID }}''',
            yes_task="insert_to_list_140",
            no_task="invoke_custom_ruby_code_141",
        )

        insert_to_list_140 = rail.SetVariableOperator(
            task_id='insert_to_list_140',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": "Supervisor not assigned/updated since the user and supervisor SSO ID are same"
            }
        )

        invoke_custom_ruby_code_141 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_141',
            python_callable=lambda dag_run: get_active_date(
                dag_run, "HireEffectiveDate")
        )

        if_request_industryfocusgroup_present_dataworkato_servicereceive_requestrequestsupervisorssoid_142 = rail.IfOperator(
            task_id='if_request_industryfocusgroup_present_dataworkato_servicereceive_requestrequestsupervisorssoid_142',
            test='''{{ dag_run.conf.IndustryFocusGroup | is_truthy }}''',
            yes_task="log_industry_focus_group_divisionschedule_145",
            no_task="ey_user_import_logs_add_entry_171",
        )

        # declare_list_143 = rail.SetVariableOperator(
        #     task_id='declare_list_143',
        #     append=False,
        #     name='industry focus schedule',
        #     value=[]
        # )

        # declare_list_144 = rail.SetVariableOperator(
        #     task_id='declare_list_144',
        #     append=False,
        #     name='divisionlist',
        #     value=[]
        # )

        def get_division_cost_schedules(dag_run):
            derived_industryfocus_schedules = []
            derived_division_schedules = []
            division_schedules = rail.result('bulk_get_users3_5')[
                0]['divisionSchedule']
            for division_schedule in division_schedules:
                if division_schedule['effectiveDate']:
                    effective_date = get_datetime_obj(
                        division_schedule['effectiveDate'])
                    current_date = datetime.strptime(dag_run.conf['HireEffectiveDate'], '%d/%m/%Y') \
                        if dag_run.conf['HireEffectiveDate'] else pendulum.now(config.pacific_timezone)
                    if effective_date.date() < current_date.date():
                        derived_industryfocus_schedules.append({
                            "uri": division_schedule['division']['uri'],
                            "name": division_schedule['division']['displayText'],
                            "date": effective_date.strftime('%d/%m/%Y')
                        })
                    elif effective_date.date() != current_date.date():
                        derived_division_schedules.append({
                            "division": {
                                "uri": division_schedule['division']['uri'],
                                "parentUri": None,
                                "name": None
                            },
                            "effectiveDate": {
                                "year": effective_date.year,
                                "month": effective_date.month,
                                "day": effective_date.day
                            }
                        })
                else:
                    employment_start_date = get_datetime_obj(rail.result('bulk_get_users3_5')[
                        0]['userDetails']['employmentDateRange']['startDate'])
                    derived_industryfocus_schedules.append({
                        "uri": division_schedule['division']['uri'],
                        "name": division_schedule['division']['displayText'],
                        "date": employment_start_date.strftime('%d/%m/%Y')
                    })

                    derived_division_schedules.append({
                        "division": {
                            "uri": division_schedule['division']['uri'],
                            "parentUri": None,
                            "name": None
                        },
                        "effectiveDate": None
                    })

            return {
                "division_schedules": derived_division_schedules,
                "industry_focus_schedules": derived_industryfocus_schedules
            }

        log_industry_focus_group_divisionschedule_145 = rail.PythonOperator(
            task_id='log_industry_focus_group_divisionschedule_145',
            python_callable=get_division_cost_schedules
        )

        if_first_uri_present_158 = rail.IfOperator(
            task_id='if_first_uri_present_158',
            test='''{{ result('log_industry_focus_group_divisionschedule_145').industry_focus_schedules | length > 0 }}''',
            yes_task="log_max_effectivedate_159",
            no_task="if_log_current_industry_focus_groupdivisionname_160_blank_161",
        )

        log_max_effectivedate_159 = rail.PythonOperator(
            task_id='log_max_effectivedate_159',
            python_callable=lambda: (max(
                datetime.strptime(x['date'], '%d/%m/%Y') for x in rail.result('log_industry_focus_group_divisionschedule_145')['industry_focus_schedules'])).strftime('%d/%m/%Y') if rail.result('log_industry_focus_group_divisionschedule_145')['industry_focus_schedules'] else None
        )

        log_current_industry_focus_groupdivisionname_160 = rail.PythonOperator(
            task_id='log_current_industry_focus_groupdivisionname_160',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'log_industry_focus_group_divisionschedule_145')['industry_focus_schedules'], 'date', rail.result('log_max_effectivedate_159'), 'name', "")
        )

        if_log_current_industry_focus_groupdivisionname_160_blank_161 = rail.IfOperator(
            task_id='if_log_current_industry_focus_groupdivisionname_160_blank_161',
            test='''{{ result('log_current_industry_focus_groupdivisionname_160') | is_falsy or result('log_current_industry_focus_groupdivisionname_160') | lower != dag_run.conf.IndustryFocusGroup | lower }}''',
            yes_task="get_all_divisions_162",
            no_task="ey_user_import_logs_add_entry_171",
        )

        get_all_divisions_162 = rail.RepliconServiceOperator(
            task_id='get_all_divisions_162',
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
        )

        log_industry_focus_groupdivision_uri_163 = rail.PythonOperator(
            task_id='log_industry_focus_groupdivision_uri_163',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_divisions_162'), 'displayText', dag_run.conf['IndustryFocusGroup'], 'uri')
        )

        if_log_industry_focus_groupdivision_uri_163_present_164 = rail.IfOperator(
            task_id='if_log_industry_focus_groupdivision_uri_163_present_164',
            test='''{{ result('log_industry_focus_groupdivision_uri_163') | is_truthy }}''',
            yes_task="put_division_schedule_for_user_industry_focus_group_update_167",
            no_task="insert_to_list_170",
        )

        def get_division_schedules(dag_run):
            current_date = datetime.strptime(dag_run.conf['HireEffectiveDate'], '%d/%m/%Y') \
                if dag_run.conf['HireEffectiveDate'] else pendulum.now(config.pacific_timezone).date()
            division_schedules = rail.result('log_industry_focus_group_divisionschedule_145')[
                'division_schedules']
            division_schedules.append({
                "division": {
                    "uri": rail.result('log_industry_focus_groupdivision_uri_163'),
                    "parentUri": None,
                    "name": None
                },
                "effectiveDate": {
                    "year": current_date.year,
                    "month": current_date.month,
                    "day": current_date.day
                }
            })
            return division_schedules

        put_division_schedule_for_user_industry_focus_group_update_167 = rail.RepliconServiceOperator(
            task_id='put_division_schedule_for_user_industry_focus_group_update_167',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": get_division_schedules(dag_run)
            }
        )

        insert_to_list_168 = rail.SetVariableOperator(
            task_id='insert_to_list_168',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Industry focus group updated"
            }
        )

        insert_to_list_170 = rail.SetVariableOperator(
            task_id='insert_to_list_170',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": '''Industry focus group not updated since the "{{dag_run.conf.IndustryFocusGroup}}" is not available in Replicon'''
            }
        )

        def get_status():
            status = "Skipped"
            exception_info = rail.get_dag_run_var(
                rail.result('declare_list_3')['name'])
            success_info = rail.get_dag_run_var(
                rail.result('declare_list_2')['name'])
            if exception_info:
                status = "Exception"
            if success_info:
                status = "Success"
            return status

        def get_details():
            validation_details = rail.get_dag_run_var(
                rail.result('declare_list_3')['name'])
            success_info = rail.get_dag_run_var(
                rail.result('declare_list_2')['name'])
            exception_info = "No change to the user record in Replicon"
            if validation_details:
                validations = [val['value'] for val in validation_details]
                exception_info = rail.smartjoin_by_delim(
                    validations, ";")
            if success_info:
                exception_info = "Successfully updated"
            return exception_info

        ey_user_import_logs_add_entry_171 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_add_entry_171',
            message="na",
            severity="",
            properties=lambda dag_run: {
                "action": "Update",
                "status": get_status(),
                "child_job_id": get_dagrun_ecid(dag_run),
                "username": dag_run.conf['FirstName']+" "+dag_run.conf['LastName'],
                "details": get_details(),
                "OHRID": dag_run.conf['OHRID'],
            }
        )

        ey_user_import_logs_ey_add_entry_173 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_add_entry_173',
            message="na",
            trigger_rule='one_failed',
            severity="Error",
            properties={
                "action": "Update",
                "status": "Error",
                "child_job_id": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}",
                "details": "{{ get_error_message() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> declare_list_2
        declare_list_2 >> declare_list_3 >> bulk_get_users3_5 >> log_startdate_6 >> ge_portugal_user_sync_master_mapper_search_entries_7 >> if_request_legalentity_present_dataworkato_servicereceive_requestrequestsupervisorssoid_8
        if_request_legalentity_present_dataworkato_servicereceive_requestrequestsupervisorssoid_8 >> rail.Label(
            'Yes') >> declare_list_9 >> declare_list_10 >> log_legal_entity_cost_centerschedule_11 >> if_first_uri_present_24
        if_first_uri_present_24 >> rail.Label(
            'Yes') >> log_max_effectivedate_25 >> log_current_legal_entitycostcentername_26 >> log_legal_entitycostcenternameaspermapper_27
        if_first_uri_present_24 >> rail.Label(
            'No') >> log_legal_entitycostcenternameaspermapper_27 >> if_log_current_legal_entitycostcentername_26_blank_28
        if_log_current_legal_entitycostcentername_26_blank_28 >> rail.Label(
            'Yes') >> disable_login_29 >> log_datetobeused_30 >> updateloginname_31 >> \
            trigger_dag_run_ge_user_sync_portugal_ge_portugal_add_v1_0async_callrecipeforlegalentitychange_32 >> \
            wait_for_completion_trigger_dag_run_ge_portugal_add_v1_0async_callrecipeforlegalentitychange_32 >> \
            dummy_operator_1 >> ey_user_import_logs_ey_add_entry_173
        if_log_current_legal_entitycostcentername_26_blank_28 >> rail.Label(
            'No') >> if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_34
        if_request_legalentity_present_dataworkato_servicereceive_requestrequestsupervisorssoid_8 >> rail.Label(
            'No') >> if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_34
        if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_34 >> rail.Label(
            'Yes') >> update_enddate_36 >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_37 >> \
            ey_user_import_logs_ey_add_entry_173
        if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_34 >> rail.Label(
            'No') >> if_request_terminationeffectivedate_present_skip_disable_39
        if_request_terminationeffectivedate_present_skip_disable_39 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_40 >> \
            ey_user_import_logs_ey_add_entry_173
        if_request_terminationeffectivedate_present_skip_disable_39 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_rehire_42
        if_userdetails_isenabled_is_not_true_rehire_42 >> rail.Label(
            'Yes') >> if_request_hireeffectivedate_blank_43
        if_request_hireeffectivedate_blank_43 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_44 >> \
            ey_user_import_logs_ey_add_entry_173
        if_request_hireeffectivedate_blank_43 >> rail.Label(
            'No') >> if_enddate_day_blank_46
        if_enddate_day_blank_46 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_47 >> \
            ey_user_import_logs_ey_add_entry_173
        if_enddate_day_blank_46 >> rail.Label(
            'No') >> log_enddate_49 >> updateloginname_50 >> trigger_dag_run_ge_user_sync_portugal_ge_portugal_add_v1_0async_callrecipeforrehire_51 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_portugal_ge_portugal_add_v1_0async_callrecipeforrehire_51 >> \
            dummy_operator_2 >> ey_user_import_logs_ey_add_entry_173
        if_userdetails_isenabled_is_not_true_rehire_42 >> rail.Label(
            'No') >> if_enddate_day_present_reverse_termination_53
        if_enddate_day_present_reverse_termination_53 >> rail.Label(
            'Yes') >> log_enddate_54 >> if_request_revtermeffectivedate_present_55
        if_request_revtermeffectivedate_present_55 >> rail.Label(
            'Yes') >> if_userdetails_isenabled_is_not_true_56
        if_userdetails_isenabled_is_not_true_56 >> rail.Label(
            'Yes') >> enable_login_57 >> insert_to_list_58 >> remove_enddate_59
        if_userdetails_isenabled_is_not_true_56 >> rail.Label(
            'No') >> remove_enddate_59 >> insert_to_list_60 >> if_request_employeefirstname_present_servicereceive_requestrequestemployeefirstname_61
        if_request_revtermeffectivedate_present_55 >> rail.Label(
            'No') >> if_request_employeefirstname_present_servicereceive_requestrequestemployeefirstname_61
        if_enddate_day_present_reverse_termination_53 >> rail.Label(
            'No') >> if_request_employeefirstname_present_servicereceive_requestrequestemployeefirstname_61
        if_request_employeefirstname_present_servicereceive_requestrequestemployeefirstname_61 >> rail.Label(
            'Yes') >> update_first_name_62 >> insert_to_list_63 >> \
            if_request_employeelastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_64
        if_request_employeefirstname_present_servicereceive_requestrequestemployeefirstname_61 >> rail.Label(
            'No') >> if_request_employeelastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_64
        if_request_employeelastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_64 >> rail.Label(
            'Yes') >> update_last_name_65 >> insert_to_list_66 >> if_request_employeeemailaddress_present_67
        if_request_employeelastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_64 >> rail.Label(
            'No') >> if_request_employeeemailaddress_present_67
        if_request_employeeemailaddress_present_67 >> rail.Label(
            'No') >> log_valuefor_job_position_title_79
        if_request_employeeemailaddress_present_67 >> rail.Label(
            'Yes') >> update_email_68 >> insert_to_list_69 >> log_valuefor_job_position_title_79 >> if_request_jobpositiontitle_present_80
        if_request_jobpositiontitle_present_80 >> rail.Label(
            'Yes') >> log_urifor_job_position_title_81 >> update_text_value_customfield_82 >> \
            insert_to_list_83 >> log_valuefor_h_r_m_s_s_o_i_d_84
        if_request_jobpositiontitle_present_80 >> rail.Label(
            'No') >> log_valuefor_h_r_m_s_s_o_i_d_84 >> if_request_hrmssoid_present_85
        if_request_hrmssoid_present_85 >> rail.Label(
            'Yes') >> log_urifor_h_r_m_s_s_o_i_d_86 >> update_text_value_customfield_87 >> insert_to_list_88 >> log_valuefor_h_r_m_name_89
        if_request_hrmssoid_present_85 >> rail.Label(
            'No') >> log_valuefor_h_r_m_name_89 >> if_request_hrmname_present_90
        if_request_hrmname_present_90 >> rail.Label(
            'Yes') >> log_urifor_h_r_m_name_91 >> update_text_value_customfield_92 >> \
            insert_to_list_93 >> log_valuefor_suspend_assignment_category_94
        if_request_hrmname_present_90 >> rail.Label(
            'No') >> log_valuefor_suspend_assignment_category_94 >> if_request_suspendassignmentcategory_present_95
        if_request_suspendassignmentcategory_present_95 >> rail.Label(
            'Yes') >> log_urifor_suspend_assignment_category_96 >> get_all_custom_field_drop_down_options_97 >> \
            log_uriforsuspendassignmentcategory_98 >> update_dropdown_value_customfield_99 >> insert_to_list_100 >> if_request_supervisorssoid_present_101
        if_request_suspendassignmentcategory_present_95 >> rail.Label(
            'No') >> if_request_supervisorssoid_present_101
        if_request_supervisorssoid_present_101 >> rail.Label(
            'No') >> invoke_custom_ruby_code_141
        if_request_supervisorssoid_present_101 >> rail.Label(
            'Yes') >> if_request_ohrid_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_102
        if_request_ohrid_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_102 >> rail.Label(
            'Yes') >> log_supervisorschedule_104 >> if_first_uri_present_114
        if_first_uri_present_114 >> rail.Label(
            'Yes') >> log_max_effectivedate_115 >> log_currentsupervisorloginname_116 >> if_log_currentsupervisorloginname_116_blank_117
        if_first_uri_present_114 >> rail.Label(
            'No') >> if_log_currentsupervisorloginname_116_blank_117
        if_log_currentsupervisorloginname_116_blank_117 >> rail.Label(
            'Yes') >> search_users_118 >> if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisnotavailable_120
        if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisnotavailable_120 >> rail.Label(
            'Yes') >> ge_supervisor_assignment_user_import_logs_add_entry_121 >> \
            if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_123
        if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisnotavailable_120 >> rail.Label(
            'No') >> if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_123
        if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_123 >> rail.Label(
            'Yes') >> ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_173_173_124 >> if_log_5_present_125
        if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_123 >> rail.Label(
            'No') >> if_log_5_present_125
        if_log_5_present_125 >> rail.Label('Yes') >> log_requiredsupervisorpermissiontoassigned_126 >> \
            get_assigned_permission_sets_for_user2_127 >> log_checkif_manager_permissionsetisassigned_128 >> \
            log_checkif_end_user_manager_permissionsetisassigned_129 >> \
            if_log_supervisorpermissiontoassigned_126_not_contains_manager_permissionsetisassigned_130
        if_log_supervisorpermissiontoassigned_126_not_contains_manager_permissionsetisassigned_130 >> rail.Label(
            'Yes') >> get_all_permission_sets_132 >> assign_permission_set_to_user_manager_135 >> invoke_custom_ruby_code_136
        if_log_supervisorpermissiontoassigned_126_not_contains_manager_permissionsetisassigned_130 >> rail.Label(
            'No') >> invoke_custom_ruby_code_136 >> update_supervisor_assignment_schedule_over_date_range_137 >> insert_to_list_138 >> \
            if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_139
        if_log_5_present_125 >> rail.Label('No') >> \
            if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_139
        if_log_currentsupervisorloginname_116_blank_117 >> rail.Label(
            'No') >> if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_139
        if_request_ohrid_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_102 >> rail.Label(
            'No') >> if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_139
        if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_139 >> rail.Label(
            'Yes') >> insert_to_list_140 >> invoke_custom_ruby_code_141
        if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_139 >> rail.Label(
            'No') >> invoke_custom_ruby_code_141 >> if_request_industryfocusgroup_present_dataworkato_servicereceive_requestrequestsupervisorssoid_142
        if_request_industryfocusgroup_present_dataworkato_servicereceive_requestrequestsupervisorssoid_142 >> rail.Label(
            'No') >> ey_user_import_logs_add_entry_171
        if_request_industryfocusgroup_present_dataworkato_servicereceive_requestrequestsupervisorssoid_142 >> rail.Label(
            'Yes') >> log_industry_focus_group_divisionschedule_145 >> if_first_uri_present_158
        if_first_uri_present_158 >> rail.Label(
            'Yes') >> log_max_effectivedate_159 >> log_current_industry_focus_groupdivisionname_160 >> if_log_current_industry_focus_groupdivisionname_160_blank_161
        if_first_uri_present_158 >> rail.Label(
            'No') >> if_log_current_industry_focus_groupdivisionname_160_blank_161
        if_log_current_industry_focus_groupdivisionname_160_blank_161 >> rail.Label(
            'Yes') >> get_all_divisions_162 >> log_industry_focus_groupdivision_uri_163 >> if_log_industry_focus_groupdivision_uri_163_present_164
        if_log_industry_focus_groupdivision_uri_163_present_164 >> rail.Label(
            'Yes') >> put_division_schedule_for_user_industry_focus_group_update_167 >> insert_to_list_168 >> \
            ey_user_import_logs_add_entry_171
        if_log_industry_focus_groupdivision_uri_163_present_164 >> rail.Label(
            'No') >> insert_to_list_170 >> ey_user_import_logs_add_entry_171
        if_log_current_industry_focus_groupdivisionname_160_blank_161 >> rail.Label(
            'No') >> ey_user_import_logs_add_entry_171
        if_request_industryfocusgroup_present_dataworkato_servicereceive_requestrequestsupervisorssoid_142 >> rail.Label(
            'No') >> ey_user_import_logs_add_entry_171 >> \
            ey_user_import_logs_ey_add_entry_173 >> \
            log_to_sumo

    return dag


rail.for_each_instance(create_dag)
