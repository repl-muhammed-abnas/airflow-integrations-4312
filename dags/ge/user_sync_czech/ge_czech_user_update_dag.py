
from datetime import timedelta, datetime
import itertools
import pendulum
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
from ge.user_sync_czech.czech_master_mapper import czech_master_mapper
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_czech_user_update_{config.instance}',
        description=f'GE Czech User Update {config.instance}',
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
            end_task='ey_user_import_logs_add_entry_210',
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

        declare_variable_4 = rail.SetVariableOperator(
            task_id='declare_variable_4',
            append=False,
            name='time off trigger',
            value=None
        )

        bulk_get_users3_6 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_6',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.UserURI }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        log_startdate_7 = rail.PythonOperator(
            task_id='log_startdate_7',
            python_callable=lambda: rail.render_template(
                "{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.day }}/{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.month }}/{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.year }}")
        )

        czech_master_mapper_search_entries_8 = rail.PythonOperator(
            task_id='czech_master_mapper_search_entries_8',
            python_callable=lambda dag_run: list(
                filter(lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'], czech_master_mapper))
        )

        if_entry_col5_blank_9 = rail.IfOperator(
            task_id='if_entry_col5_blank_9',
            test='''{{ result('czech_master_mapper_search_entries_8') | length == 0 }}''',
            yes_task="if_userdetails_isenabled_is_true_10",
            no_task="dummy_operator_1",
        )

        dummy_operator_1 = rail.EmptyOperator(
            task_id="dummy_operator_1"
        )

        if_userdetails_isenabled_is_true_10 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_10',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.isEnabled == True }}''',
            yes_task="disable_login_11",
            no_task="if_userdetails_isenabled_is_not_true_14",
        )

        disable_login_11 = rail.RepliconServiceOperator(
            task_id='disable_login_11',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.UserURI }}"
            }
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_12 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_12',
            message="na",
            severity="Success",
            properties={
                "action": "Disable",
                "status": "Success",
                "jobid": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "details": "User not in allowed list of Legal Entities, profile disabled",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}"
            }
        )

        if_userdetails_isenabled_is_not_true_14 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_14',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.isEnabled == False }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_15",
            no_task="if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_17",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_15 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_15',
            message="na",
            severity="Skipped",
            properties={
                "action": "Update",
                "status": "Skipped",
                "child_job_id": "{{ dag_run_ecid() }}",
                "details": "User not in allowed list of Legal Entities, profile is already disabled",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}"
            }
        )

        def is_temination_date_reached(dag_run):
            if dag_run.conf['terminationeffectivedate'] and dag_run.conf['reverseterminationeffectivedate'] is None \
                    and rail.result('bulk_get_users3_6')[0]['userDetails']['isEnabled'] is True:
                temination_date = datetime.strptime(
                    dag_run.conf['terminationeffectivedate'], '%d/%m/%Y')
                start_date = datetime.strptime(rail.result(
                    'log_startdate_7'), '%d/%m/%Y')
                if temination_date > start_date + timedelta(days=-1):
                    return True
            return False

        if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_17 = rail.IfOperator(
            task_id='if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_17',
            test=is_temination_date_reached,
            yes_task="update_enddate_19",
            no_task="if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_22",
        )

        update_enddate_19 = rail.RepliconServiceOperator(
            task_id='update_enddate_19',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['UserURI'],
                "dateRange": {
                    "startDate": {
                        "year": rail.result('bulk_get_users3_6')[0]['userDetails']['employmentDateRange']['startDate']['year'],
                        "month": rail.result('bulk_get_users3_6')[0]['userDetails']['employmentDateRange']['startDate']['month'],
                        "day": rail.result('bulk_get_users3_6')[0]['userDetails']['employmentDateRange']['startDate']['day'],
                    },
                    "endDate": {
                        "year": datetime.strptime(dag_run.conf['terminationeffectivedate'], '%d/%m/%Y').year,
                        "month": datetime.strptime(dag_run.conf['terminationeffectivedate'], '%d/%m/%Y').month,
                        "day": datetime.strptime(dag_run.conf['terminationeffectivedate'], '%d/%m/%Y').day
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_20 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_20',
            message="na",
            severity="Success",
            properties={
                "action": "Disable",
                "status": "Success",
                "child_job_id": "{{ dag_run_ecid() }}",
                "details": "End Date Updated",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}"
            }
        )

        def is_temination_date_not_reached(dag_run):
            if dag_run.conf['terminationeffectivedate'] and \
                dag_run.conf['reverseterminationeffectivedate'] is None and \
                    rail.result('bulk_get_users3_6')[0]['userDetails']['isEnabled'] is True:
                temination_date = datetime.strptime(
                    dag_run.conf['terminationeffectivedate'], '%d/%m/%Y')
                start_date = datetime.strptime(rail.result(
                    'log_startdate_7'), '%d/%m/%Y')
                if temination_date < start_date:
                    return True
            return False

        if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_22 = rail.IfOperator(
            task_id='if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_22',
            test=is_temination_date_not_reached,
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_23",
            no_task="if_userdetails_isenabled_is_not_true_rehire_25",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_23 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_23',
            message="na",
            severity="Skipped",
            properties={
                "action": "Disable",
                "status": "Skipped",
                "child_job_id": "{{ dag_run_ecid() }}",
                "details": "End Date not Updated as termination date is prior to start date",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}"
            }
        )

        if_userdetails_isenabled_is_not_true_rehire_25 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_rehire_25',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.isEnabled == False and dag_run.conf.reverseterminationeffectivedate | is_falsy }}''',
            yes_task="if_request_hireeffectivedate_blank_26",
            no_task="if_userdetails_isenabled_is_true_transfer_36",
        )

        if_request_hireeffectivedate_blank_26 = rail.IfOperator(
            task_id='if_request_hireeffectivedate_blank_26',
            test='''{{ dag_run.conf.Hireeffectivedate | is_falsy }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_27",
            no_task="if_enddate_year_present_29",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_27 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_27',
            message="na",
            severity="Skipped",
            properties={
                "action": "Rehire",
                "status": "Skipped",
                "child_job_id": "{{ dag_run_ecid() }}",
                "details": "Hire effective date not available",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}"
            }
        )

        if_enddate_year_present_29 = rail.IfOperator(
            task_id='if_enddate_year_present_29',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate | is_truthy and result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate.year | is_truthy }}''',
            yes_task="log_enddate_30",
            no_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_34",
        )

        log_enddate_30 = rail.PythonOperator(
            task_id='log_enddate_30',
            python_callable=lambda:  rail.result('bulk_get_users3_6')[
                0]['userDetails']['employmentDateRange']['endDate']['year']
        )

        updateloginname_31 = rail.RepliconServiceOperator(
            task_id='updateloginname_31',
            endpoint="/services/securityservice1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.UserURI }}",
                "loginName": "{{ result('bulk_get_users3_6')[0].securityConfiguration.loginName }}{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate.day }}{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate.month }}{{ result('log_enddate_30') }}"
            }
        )

        trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_callrecipeforrehire_32 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_callrecipeforrehire_32',
            retries=0,
            items=[1],
            trigger_dag_id=f'ge_czech_add_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "EmployeeFirstName": dag_run.conf['FirstName'],
                "EmployeeLastName": dag_run.conf['LastName'],
                "EmployeeEmailAddress": dag_run.conf['Email'],
                "OHRID": dag_run.conf['OHRID'],
                "LegalEntityHireDate": dag_run.conf['Hireeffectivedate'],
                "LegacyPayrollID": dag_run.conf['legacypayrollid'],
                "EmployeeGender": dag_run.conf['employeegender'],
                "MaritalStatus": dag_run.conf['Maritalstatus'],
                "JobPositionTitle": dag_run.conf['JobPositionTitle'],
                "SupervisorSSOID": dag_run.conf['SupervisorSSOID'],
                "SupervisorName": dag_run.conf['SupervisorName'],
                "AssignmentCategory": dag_run.conf['AssignmentCategoryEmployeeType'],
                "SuspendAssignmentCategory": None,
                "Locationname": None,
                "Contractattributeannualvacationeligibility": None,
                "Subbiz": None,
                "Worktimesystem": None,
                "Educationlevel": None,
                "Specialworkschedule": None,
                "Work": None,
                "Adjustedservicedate": None,
                "Jobtype": None,
                "HealthcareproductlineEIT": None,
                "Payroll": None,
                "Dateofbirth": None,
                "Overtimeeligibility": None,
                "Salarybasis": None,
                "Departmentalstom": None,
                "Previousemploymentsperiodsenddate": None,
                "DWSstartdate": None,
                "DWSEndDate": "na",
                "DWSMonday": dag_run.conf['DWSMonday'],
                "DWSTuesday": dag_run.conf['DWSTuesday'],
                "DWSWednesday": dag_run.conf['DWSWednesday'],
                "DWSThursday": dag_run.conf['DWSThursday'],
                "DWSFriday": dag_run.conf['DWSFriday'],
                "DWSSaturday": dag_run.conf['DWSSaturday'],
                "DWSSunday": dag_run.conf['DWSSunday'],
                "TerminationEffectiveDate": dag_run.conf['terminationeffectivedate'],
                "IndustryFocusGroup": dag_run.conf['industryfocusgroup'],
                "LegalEntity": dag_run.conf['LegalEntity'],
                "ContractID": dag_run.conf['contractid'],
                "RadiationFlag": dag_run.conf['radiationflag'],
                "PositionCapacity": dag_run.conf['positioncapacity'],
                "EducationPeriods_StartDate": "na",
                "EducationPeriods_EndDate": "na",
                "PreviousEmploymentsPeriods_StartDate": "na",
                "HRMSSOID": dag_run.conf['HRMSSOID'],
                "HRMName": dag_run.conf['HRMName'],
                "AssignmentEffectiveDate": dag_run.conf['Assignmenteffectivedate'],
                "Hireeffectivedate": dag_run.conf['Hireeffectivedate'],
                "revtermeffectivedate": dag_run.conf['reverseterminationeffectivedate'],
                "type": "Rehire",
                "CareerBand": "NA",
                "supervisor_processing_log": dag_run.conf['supervisor_processing_log']
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_callrecipeforrehire_32 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_callrecipeforrehire_32',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_callrecipeforrehire_32") }}'
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_34 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_34',
            message="na",
            severity="Skipped",
            properties={
                "action": "Rehire",
                "status": "Skipped",
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "details": "User not rehired since the existing profile doesn't have an end date",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}"
            }
        )

        if_userdetails_isenabled_is_true_transfer_36 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_transfer_36',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.isEnabled == True }}''',
            yes_task="log_costcenterschedule_38",
            no_task="dummy_operator_2",
        )

        dummy_operator_2 = rail.EmptyOperator(
            task_id="dummy_operator_2"
        )

        def get_datetime_obj(effectiveDate):
            year = effectiveDate['year']
            month = effectiveDate['month']
            day = effectiveDate['day']
            return datetime.strptime(f"{year}/{month}/{day}", '%Y/%m/%d')

        def costcenter_schedule_data():
            derived_costcenter_schedules = []
            costcenter_schedules = rail.result('bulk_get_users3_6')[
                0]['costCenterSchedule']
            for costcenter_schedule in costcenter_schedules:
                if costcenter_schedule['effectiveDate']:
                    effective_date = get_datetime_obj(
                        costcenter_schedule['effectiveDate'])
                    if effective_date.date() > pendulum.now(config.pacific_timezone).date():
                        derived_costcenter_schedules.append({
                            "uri": costcenter_schedule['costCenter']['uri'],
                            "name": costcenter_schedule['costCenter']['displayText'],
                            "date": effective_date.strftime('%d/%m/%Y')
                        })
                else:
                    derived_costcenter_schedules.append({
                        "uri": costcenter_schedule['costCenter']['uri'],
                        "name": costcenter_schedule['costCenter']['displayText'],
                        "date": pendulum.now(config.pacific_timezone).strftime('%d/%m/%Y')
                    })

            return derived_costcenter_schedules

        log_costcenterschedule_38 = rail.PythonOperator(
            task_id='log_costcenterschedule_38',
            python_callable=costcenter_schedule_data
        )

        log_latesteffectivedate_49 = rail.PythonOperator(
            task_id='log_latesteffectivedate_49',
            python_callable=lambda: (max(
                datetime.strptime(x['date'], '%d/%m/%Y') for x in rail.result('log_costcenterschedule_38'))).strftime('%d/%m/%Y') if rail.result('log_costcenterschedule_38') else None
        )

        def get_entity_mapper(dag_run, entity_type):
            entity_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == entity_type, czech_master_mapper))
            entity_values = [entity['value'] for entity in entity_types]
            return rail.smartjoin_by_delim(entity_values, '')

        log_latesteffective_legal_entity_name_50 = rail.PythonOperator(
            task_id='log_latesteffective_legal_entity_name_50',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'log_costcenterschedule_38'), 'date', rail.result('log_latesteffectivedate_49'), 'name', "")
        )

        log_legal_entitynamefrommapper_51 = rail.PythonOperator(
            task_id='log_legal_entitynamefrommapper_51',
            python_callable=lambda dag_run: get_entity_mapper(dag_run,
                                                              "Legal Entity"),
        )

        if_log_latesteffective_legal_entity_name_50_blank_52 = rail.IfOperator(
            task_id='if_log_latesteffective_legal_entity_name_50_blank_52',
            test='''{{ result('log_latesteffective_legal_entity_name_50') | is_falsy or result('log_latesteffective_legal_entity_name_50') | lower != result('log_legal_entitynamefrommapper_51') | lower }}''',
            yes_task="log_enddate_53",
            no_task="if_enddate_day_present_reverse_termination_63",
        )

        log_enddate_53 = rail.PythonOperator(
            task_id='log_enddate_53',
            python_callable=lambda:  pendulum.now(
                config.pacific_timezone).strftime('%m%d%Y')
        )

        updateloginname_56 = rail.RepliconServiceOperator(
            task_id='updateloginname_56',
            endpoint="/services/securityservice1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.UserURI }}",
                "loginName": "{{ result('bulk_get_users3_6')[0].securityConfiguration.loginName }}{{ result('log_enddate_53') }}"
            }
        )

        disable_login_57 = rail.RepliconServiceOperator(
            task_id='disable_login_57',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.UserURI }}"
            }
        )

        if_request_hireeffectivedate_blank_58 = rail.IfOperator(
            task_id='if_request_hireeffectivedate_blank_58',
            test='''{{ dag_run.conf.Hireeffectivedate | is_falsy }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_59",
            no_task="trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_callrecipefor_transfer_61",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_59 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_59',
            message="na",
            severity="Exception",
            properties={
                "action": "Transfer",
                "status": "Exception",
                "child_job_id": "{{ dag_run_ecid() }}",
                "details": "User not trasnferred since Hire Effective Date is not present",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}"
            }
        )

        trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_callrecipefor_transfer_61 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_callrecipefor_transfer_61',
            retries=0,
            items=[1],
            trigger_dag_id=f'ge_czech_add_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "EmployeeFirstName": dag_run.conf['FirstName'],
                "EmployeeLastName": dag_run.conf['LastName'],
                "EmployeeEmailAddress": dag_run.conf['Email'],
                "OHRID": dag_run.conf['OHRID'],
                "LegalEntityHireDate": dag_run.conf['Hireeffectivedate'],
                "LegacyPayrollID": dag_run.conf['legacypayrollid'],
                "EmployeeGender": dag_run.conf['employeegender'],
                "MaritalStatus": dag_run.conf['Maritalstatus'],
                "JobPositionTitle": dag_run.conf['JobPositionTitle'],
                "SupervisorSSOID": dag_run.conf['SupervisorSSOID'],
                "SupervisorName": dag_run.conf['SupervisorName'],
                "AssignmentCategory": dag_run.conf['AssignmentCategoryEmployeeType'],
                "DWSStartDate": None,
                "DWSEndDate": "na",
                "DWSMonday": dag_run.conf['DWSMonday'],
                "DWSTuesday": dag_run.conf['DWSTuesday'],
                "DWSWednesday": dag_run.conf['DWSWednesday'],
                "DWSThursday": dag_run.conf['DWSThursday'],
                "DWSFriday": dag_run.conf['DWSFriday'],
                "DWSSaturday": dag_run.conf['DWSSaturday'],
                "DWSSunday": dag_run.conf['DWSSunday'],
                "TerminationEffectiveDate": dag_run.conf['terminationeffectivedate'],
                "IndustryFocusGroup": dag_run.conf['industryfocusgroup'],
                "LegalEntity": dag_run.conf['LegalEntity'],
                "ContractID": dag_run.conf['contractid'],
                "RadiationFlag": dag_run.conf['radiationflag'],
                "PositionCapacity": dag_run.conf['positioncapacity'],
                "EducationPeriods_StartDate": "na",
                "EducationPeriods_EndDate": "na",
                "PreviousEmploymentsPeriods_StartDate": "na",
                "Previousemploymentsperiodsenddate": None,
                "Departmentalstom": None,
                "Salarybasis": None,
                "Overtimeeligibility": None,
                "SuspendAssignmentCategory": None,
                "Dateofbirth": None,
                "Payroll": None,
                "HealthcareproductlineEIT": None,
                "Jobtype": None,
                "CareerBand": "NA",
                "Adjustedservicedate": None,
                "Work": None,
                "HRMSSOID": dag_run.conf['HRMSSOID'],
                "HRMName": dag_run.conf['HRMName'],
                "Specialworkschedule": None,
                "Educationlevel": None,
                "Worktimesystem": None,
                "Subbiz": None,
                "Contractattributeannualvacationeligibility": None,
                "Locationname": None,
                "AssignmentEffectiveDate": dag_run.conf['Assignmenteffectivedate'],
                "Hireeffectivedate": dag_run.conf['Hireeffectivedate'],
                "revtermeffectivedate": dag_run.conf['reverseterminationeffectivedate'],
                "type": "Transfer",
                "supervisor_processing_log": dag_run.conf['supervisor_processing_log']
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_callrecipefor_transfer_61 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_callrecipefor_transfer_61',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_callrecipefor_transfer_61") }}'
        )

        if_enddate_day_present_reverse_termination_63 = rail.IfOperator(
            task_id='if_enddate_day_present_reverse_termination_63',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate | is_truthy and result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate.day | is_truthy }}''',
            yes_task="log_enddate_64",
            no_task="if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_71",
        )

        log_enddate_64 = rail.PythonOperator(
            task_id='log_enddate_64',
            python_callable=lambda: rail.render_template(
                "{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate.day }}/{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate.month }}/{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate.year }}")
        )

        def revers_eff_date_in_user_date_range(dag_run):
            if dag_run.conf['reverseterminationeffectivedate']:
                revers_eff_date = datetime.strptime(
                    dag_run.conf['reverseterminationeffectivedate'], '%d/%m/%Y')
                user_start_date = datetime.strptime(
                    rail.result('log_startdate_7'), '%d/%m/%Y')
                user_end_date = datetime.strptime(
                    rail.result('log_enddate_64'), '%d/%m/%Y')
                if user_start_date < revers_eff_date < user_end_date:
                    return True
            return False

        if_request_reverseterminationeffectivedate_present_65 = rail.IfOperator(
            task_id='if_request_reverseterminationeffectivedate_present_65',
            test=revers_eff_date_in_user_date_range,
            yes_task="if_userdetails_isenabled_is_not_true_66",
            no_task="if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_71",
        )

        if_userdetails_isenabled_is_not_true_66 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_66',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.isEnabled == False }}''',
            yes_task="enable_login_67",
            no_task="remove_enddate_69",
        )

        enable_login_67 = rail.RepliconServiceOperator(
            task_id='enable_login_67',
            endpoint="/services/securityservice1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.UserURI }}"
            }
        )

        insert_to_list_68 = rail.SetVariableOperator(
            task_id='insert_to_list_68',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "User profile re-enabled, reverse termination date older than end date and newer than start date."
            }
        )

        remove_enddate_69 = rail.RepliconServiceOperator(
            task_id='remove_enddate_69',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.UserURI }}",
                "dateRange": {
                    "startDate": {
                        "year": '''{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.year }}''',
                        "month": '''{{result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.month}}''',
                        "day": '''{{result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.day}}'''
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        insert_to_list_70 = rail.SetVariableOperator(
            task_id='insert_to_list_70',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "End date removed, reverse termination date older than end date and newer than start date."
            }
        )

        if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_71 = rail.IfOperator(
            task_id='if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_71',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.firstName | lower != dag_run.conf.FirstName | lower }}''',
            yes_task="update_first_name_72",
            no_task="if_lastname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestlastnamedowncase_74",
        )

        update_first_name_72 = rail.RepliconServiceOperator(
            task_id='update_first_name_72',
            endpoint="/services/userService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.UserURI }}",
                "firstname": "{{ dag_run.conf.FirstName }}"
            }
        )

        insert_to_list_73 = rail.SetVariableOperator(
            task_id='insert_to_list_73',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "First name updated"
            }
        )

        if_lastname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestlastnamedowncase_74 = rail.IfOperator(
            task_id='if_lastname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestlastnamedowncase_74',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.lastName | lower != dag_run.conf.LastName | lower }}''',
            yes_task="update_last_name_75",
            no_task="if_request_email_present_77",
        )

        update_last_name_75 = rail.RepliconServiceOperator(
            task_id='update_last_name_75',
            endpoint="/services/userService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.UserURI }}",
                "lastname": "{{ dag_run.conf.LastName }}"
            }
        )

        insert_to_list_76 = rail.SetVariableOperator(
            task_id='insert_to_list_76',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Last name updated"
            }
        )

        def email_validation(dag_run):
            existing_email = rail.result('bulk_get_users3_6')[0]['userDetails']['emailAddress'].lower(
            ) if rail.result('bulk_get_users3_6')[0]['userDetails']['emailAddress'] else None
            if dag_run.conf['Email']:
                if dag_run.conf['Email'].lower() != existing_email:
                    return True
            return False

        if_request_email_present_77 = rail.IfOperator(
            task_id='if_request_email_present_77',
            test=email_validation,
            yes_task="update_email_78",
            no_task="log_valuefor_job_position_title_80",
        )

        update_email_78 = rail.RepliconServiceOperator(
            task_id='update_email_78',
            endpoint="/services/userService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.UserURI }}",
                "email": "{{ dag_run.conf.Email }}"
            }
        )

        insert_to_list_79 = rail.SetVariableOperator(
            task_id='insert_to_list_79',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Email updated"
            }
        )

        def get_custom_value(custom_field_name):
            existing_custom_fields = rail.result('bulk_get_users3_6')[0][
                'userDetails']['customFieldValues']
            custom_infos = list(filter(
                lambda x: x['customField']['displayText'] == custom_field_name, existing_custom_fields))
            return custom_infos[0]['text'] if custom_infos else None

        log_valuefor_job_position_title_80 = rail.PythonOperator(
            task_id='log_valuefor_job_position_title_80',
            python_callable=lambda: get_custom_value("Job/Position Title")
        )

        def get_custom_uri(custom_field_name):
            existing_custom_fields = rail.result('bulk_get_users3_6')[0][
                'userDetails']['customFieldValues']
            custom_infos = list(filter(
                lambda x: x['customField'] and x['customField']['displayText'].lower() == custom_field_name.lower(), existing_custom_fields))
            return custom_infos[0]['customField']['uri'] if custom_infos else None

        log_urifor_job_position_title_81 = rail.PythonOperator(
            task_id='log_urifor_job_position_title_81',
            python_callable=lambda: get_custom_uri('Job/Position Title')
        )

        if_request_jobpositiontitle_present_82 = rail.IfOperator(
            task_id='if_request_jobpositiontitle_present_82',
            test='''{{ dag_run.conf.JobPositionTitle | is_truthy and result('log_valuefor_job_position_title_80') | lower != dag_run.conf.JobPositionTitle | lower }}''',
            yes_task="update_text_value_customfield_83",
            no_task="log_valuefor_h_r_m_s_s_o_i_d_86",
        )

        update_text_value_customfield_83 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_83',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.UserURI }}",
                "customFieldUri": "{{ result('log_urifor_job_position_title_81') }}",
                "value": "{{ dag_run.conf.JobPositionTitle }}"
            }
        )

        insert_to_list_84 = rail.SetVariableOperator(
            task_id='insert_to_list_84',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Job/Position Title updated"
            }
        )

        update_variable_85 = rail.SetVariableOperator(
            task_id='update_variable_85',
            append=False,
            name='{{ result("declare_variable_4").name }}',
            value="yes"
        )

        log_valuefor_h_r_m_s_s_o_i_d_86 = rail.PythonOperator(
            task_id='log_valuefor_h_r_m_s_s_o_i_d_86',
            python_callable=lambda: get_custom_value("HRM SSO ID")
        )

        log_urifor_h_r_m_s_s_o_i_d_87 = rail.PythonOperator(
            task_id='log_urifor_h_r_m_s_s_o_i_d_87',
            python_callable=lambda: get_custom_uri('HRM SSO ID')
        )

        if_request_hrmssoid_present_88 = rail.IfOperator(
            task_id='if_request_hrmssoid_present_88',
            test='''{{ dag_run.conf.HRMSSOID | is_truthy and result('log_valuefor_h_r_m_s_s_o_i_d_86') | lower != dag_run.conf.HRMSSOID | lower }}''',
            yes_task="update_text_value_customfield_89",
            no_task="log_valuefor_h_r_m_name_91",
        )

        update_text_value_customfield_89 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_89',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.UserURI }}",
                "customFieldUri": "{{ result('log_urifor_h_r_m_s_s_o_i_d_87') }}",
                "value": "{{ dag_run.conf.HRMSSOID }}"
            }
        )

        insert_to_list_90 = rail.SetVariableOperator(
            task_id='insert_to_list_90',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "HRMSSOID updated"
            }
        )

        log_valuefor_h_r_m_name_91 = rail.PythonOperator(
            task_id='log_valuefor_h_r_m_name_91',
            python_callable=lambda: get_custom_value("HRM Name")
        )

        log_urifor_h_r_m_name_92 = rail.PythonOperator(
            task_id='log_urifor_h_r_m_name_92',
            python_callable=lambda: get_custom_uri('HRM Name')
        )

        if_request_hrmname_present_93 = rail.IfOperator(
            task_id='if_request_hrmname_present_93',
            test='''{{ dag_run.conf.HRMName | is_truthy and result('log_valuefor_h_r_m_name_91') | lower != dag_run.conf.HRMSSOID | lower}}''',
            yes_task="update_text_value_customfield_94",
            no_task="log_valuefor_suspend_assignment_category_96",
        )

        update_text_value_customfield_94 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_94',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.UserURI }}",
                "customFieldUri": "{{ result('log_urifor_h_r_m_name_92') }}",
                "value": "{{ dag_run.conf.HRMName }}"
            }
        )

        insert_to_list_95 = rail.SetVariableOperator(
            task_id='insert_to_list_95',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "HRM Name updated"
            }
        )

        log_valuefor_suspend_assignment_category_96 = rail.PythonOperator(
            task_id='log_valuefor_suspend_assignment_category_96',
            python_callable=lambda: get_custom_value(
                "Suspend Assignment Category")
        )

        log_urifor_suspend_assignment_category_97 = rail.PythonOperator(
            task_id='log_urifor_suspend_assignment_category_97',
            python_callable=lambda: get_custom_uri(
                'Suspend Assignment Category')
        )

        if_request_suspendassignmentcategory_present_98 = rail.IfOperator(
            task_id='if_request_suspendassignmentcategory_present_98',
            test='''{{ dag_run.conf.SuspendAssignmentCategory | is_truthy and result('log_valuefor_suspend_assignment_category_96') | lower != dag_run.conf.SuspendAssignmentCategory | lower }}''',
            yes_task="get_all_custom_field_drop_down_options_99",
            no_task="if_request_supervisorssoid_present_103",
        )

        get_all_custom_field_drop_down_options_99 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_99',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_urifor_suspend_assignment_category_97') }}"
            }
        )

        log_uriforsuspendassignmentcategory_100 = rail.PythonOperator(
            task_id='log_uriforsuspendassignmentcategory_100',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_custom_field_drop_down_options_99'), 'displayText', dag_run.conf['SuspendAssignmentCategory'], 'uri')
        )

        update_dropdown_value_customfield_101 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_customfield_101',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.UserURI }}",
                "customFieldUri": "{{ result('log_urifor_suspend_assignment_category_97') }}",
                "customFieldDropDownOptionUri": "{{ result('log_uriforsuspendassignmentcategory_100') }}"
            }
        )

        insert_to_list_102 = rail.SetVariableOperator(
            task_id='insert_to_list_102',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Suspend Assignment Category updated"
            }
        )

        if_request_supervisorssoid_present_103 = rail.IfOperator(
            task_id='if_request_supervisorssoid_present_103',
            test='''{{ dag_run.conf.SupervisorSSOID | is_truthy }}''',
            yes_task="if_request_ohrid_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_104",
            no_task="if_declare_variable_4_value_equals_to_yes_152",
        )

        if_request_ohrid_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_104 = rail.IfOperator(
            task_id='if_request_ohrid_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_104',
            test='''{{ dag_run.conf.OHRID != dag_run.conf.SupervisorSSOID }}''',
            yes_task="log_supervisorschedule_106",
            no_task="if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150",
        )

        def get_supervisor_schedules():
            supervisor_schedules = []
            currentsupervisorschedules = rail.result('bulk_get_users3_6')[
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
                    effective_date = get_datetime_obj(rail.result('bulk_get_users3_6')[
                        0]['userDetails']['employmentDateRange']['startDate'])
                    supervisor_schedules.append({
                        "loginname": super_schedule['supervisor']['user']['loginName'],
                        "uri": super_schedule['supervisor']['uri'],
                        "effectivedate": effective_date.strftime('%d/%m/%Y'),
                        "name": super_schedule['supervisor']['displayText'],
                    })
            return supervisor_schedules

        log_supervisorschedule_106 = rail.PythonOperator(
            task_id='log_supervisorschedule_106',
            python_callable=get_supervisor_schedules
        )

        if_first_uri_present_116 = rail.IfOperator(
            task_id='if_first_uri_present_116',
            test='''{{ result('log_supervisorschedule_106') | length > 0 }}''',
            yes_task="log_max_effectivedate_117",
            no_task="if_log_currentsupervisorloginname_118_blank_119",
        )

        log_max_effectivedate_117 = rail.PythonOperator(
            task_id='log_max_effectivedate_117',
            python_callable=lambda: (max(
                datetime.strptime(x['effectivedate'], '%d/%m/%Y') for x in rail.result('log_supervisorschedule_106'))).strftime('%d/%m/%Y') if rail.result('log_supervisorschedule_106') else None
        )

        log_currentsupervisorloginname_118 = rail.PythonOperator(
            task_id='log_currentsupervisorloginname_118',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'log_supervisorschedule_106'), 'effectivedate', rail.result('log_max_effectivedate_117'), 'loginname')
        )

        if_log_currentsupervisorloginname_118_blank_119 = rail.IfOperator(
            task_id='if_log_currentsupervisorloginname_118_blank_119',
            test='''{{ result('log_currentsupervisorloginname_118') | is_falsy  or result('log_currentsupervisorloginname_118') != dag_run.conf.SupervisorSSOID }}''',
            yes_task="search_users_120",
            no_task="if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146",
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

        search_users_120 = rail.RepliconServicePageOperator(
            task_id='search_users_120',
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

        if_log_5_present_123 = rail.IfOperator(
            task_id='if_log_5_present_123',
            test='''{{ result('search_users_120') | is_truthy  and result('search_users_120').status == 'True' }}''',
            yes_task="get_assigned_permission_sets_for_user2_124",
            no_task="if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146",
        )

        def get_permission(permission_name):
            permissionset_details = list(filter(
                lambda x: x['permissionSet']['name'] == permission_name, rail.result('get_assigned_permission_sets_for_user2_124')))
            permissions = [permissionset['permissionSet']['name']
                           for permissionset in permissionset_details]
            return rail.smartjoin_by_delim(permissions, '') if permissions else []

        get_assigned_permission_sets_for_user2_124 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_124',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_120').useruri }}"
            }
        )

        log_checkif_manager_permissionsetisassigned_125 = rail.PythonOperator(
            task_id='log_checkif_manager_permissionsetisassigned_125',
            python_callable=lambda: get_permission('Manager')
        )

        log_checkif_end_user_manager_permissionsetisassigned_126 = rail.PythonOperator(
            task_id='log_checkif_end_user_manager_permissionsetisassigned_126',
            python_callable=lambda: get_permission('End User - Manager')
        )

        def permission_validation():
            manager_permission_name = rail.result(
                'log_checkif_manager_permissionsetisassigned_125')
            end_manager_permission_name = rail.result(
                'log_checkif_end_user_manager_permissionsetisassigned_126')
            if "Manager" in manager_permission_name and "End User - Manager" in end_manager_permission_name:
                return True
            return False

        if_log_checkif_manager_permissionsetisassigned_125_contains_approver_127 = rail.IfOperator(
            task_id='if_log_checkif_manager_permissionsetisassigned_125_contains_approver_127',
            test=permission_validation,
            yes_task="update_supervisor_assignment_schedule_over_date_range_131",
            no_task="if_log_checkif_manager_permissionsetisassigned_125_not_contains_approver_133",
        )

        def get_assignment_date(dag_run):
            if dag_run.conf['Assignmenteffectivedate']:
                assigment_eff_date = datetime.strptime(
                    dag_run.conf['Assignmenteffectivedate'], '%d/%m/%Y')
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

        update_supervisor_assignment_schedule_over_date_range_131 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_131',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['UserURI'],
                "supervisorUri": rail.result('search_users_120')['useruri'],
                "dateRange": {
                    "startDate": get_assignment_date(dag_run),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        insert_to_list_132 = rail.SetVariableOperator(
            task_id='insert_to_list_132',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Supervisor updated"
            }
        )

        def permission_manager_validation():
            manager_permission_name = rail.result(
                'log_checkif_manager_permissionsetisassigned_125')
            end_manager_permission_name = rail.result(
                'log_checkif_end_user_manager_permissionsetisassigned_126')
            if "Manager" not in manager_permission_name or "End User - Manager" not in end_manager_permission_name:
                return True
            return False

        if_log_checkif_manager_permissionsetisassigned_125_not_contains_approver_133 = rail.IfOperator(
            task_id='if_log_checkif_manager_permissionsetisassigned_125_not_contains_approver_133',
            test=permission_manager_validation,
            yes_task="get_all_permission_sets_134",
            no_task="if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146",
        )

        get_all_permission_sets_134 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_134',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        log_permissionsetfor_approver_135 = rail.PythonOperator(
            task_id='log_permissionsetfor_approver_135',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permission_sets_134'), 'displayText', "Manager", 'uri')
        )

        log_permissionsetfor_end_user_manager_136 = rail.PythonOperator(
            task_id='log_permissionsetfor_end_user_manager_136',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_permission_sets_134'), 'displayText', "End User - Manager", 'uri')
        )

        if_log_checkif_manager_permissionsetisassigned_125_blank_137 = rail.IfOperator(
            task_id='if_log_checkif_manager_permissionsetisassigned_125_blank_137',
            test='''{{ result('log_checkif_manager_permissionsetisassigned_125') | is_falsy }}''',
            yes_task="assign_permission_set_to_user_approver_138",
            no_task="if_log_checkif_end_user_manager_permissionsetisassigned_126_blank_139",
        )

        assign_permission_set_to_user_approver_138 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_approver_138',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users_120').useruri }}",
                "permissionSetUri": "{{ result('log_permissionsetfor_approver_135') }}"
            }
        )

        if_log_checkif_end_user_manager_permissionsetisassigned_126_blank_139 = rail.IfOperator(
            task_id='if_log_checkif_end_user_manager_permissionsetisassigned_126_blank_139',
            test='''{{ result('log_checkif_end_user_manager_permissionsetisassigned_126') | is_falsy }}''',
            yes_task="assign_permission_set_to_user_enduser_manager_140",
            no_task="update_supervisor_assignment_schedule_over_date_range_144",
        )

        assign_permission_set_to_user_enduser_manager_140 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_enduser_manager_140',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users_120').useruri }}",
                "permissionSetUri": "{{ result('log_permissionsetfor_end_user_manager_136') }}"
            }
        )

        update_supervisor_assignment_schedule_over_date_range_144 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_144',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['UserURI'],
                "supervisorUri": rail.result('search_users_120')['useruri'],
                "dateRange": {
                    "startDate": get_assignment_date(dag_run),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        insert_to_list_145 = rail.SetVariableOperator(
            task_id='insert_to_list_145',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Supervisor updated"
            }
        )

        if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146 = rail.IfOperator(
            task_id='if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146',
            test='''{{ result('search_users_120') | is_truthy and result('search_users_120').status | matches('False') }}''',
            yes_task="ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_147",
            no_task="if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisdisableddonotupdatesupervisor_148",
        )

        ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_147 = rail.WriteLogOperator(
            task_id='ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_147',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="na",
            severity="queued",
            properties=lambda dag_run: {
                "username": dag_run.conf['OHRID'],
                "useruri": dag_run.conf['UserURI'],
                "supervisorloginname": dag_run.conf['SupervisorSSOID'],
                "action": "update",
                "childjobid": get_dagrun_ecid(dag_run),
                "supervisoreffectivedate": dag_run.conf['Assignmenteffectivedate'] if dag_run.conf['Assignmenteffectivedate'] else
                    pendulum.now(config.pacific_timezone).strftime('%d/%m/%Y'),
                "status": "queued",
                "supervisorusername": dag_run.conf['SupervisorName']
            }
        )

        if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisdisableddonotupdatesupervisor_148 = rail.IfOperator(
            task_id='if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisdisableddonotupdatesupervisor_148',
            test='''{{ result('search_users_120') | is_falsy }}''',
            yes_task="ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_149",
            no_task="if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150",
        )

        ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_149 = rail.WriteLogOperator(
            task_id='ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_149',
            log="{{ dag_run.conf.supervisor_processing_log }}",
            message="na",
            severity="queued",
            properties=lambda dag_run: {
                "username": dag_run.conf['OHRID'],
                "useruri": dag_run.conf['UserURI'],
                "supervisorloginname": dag_run.conf['SupervisorSSOID'],
                "action": "update",
                "childjobid": get_dagrun_ecid(dag_run),
                "supervisoreffectivedate": dag_run.conf['Assignmenteffectivedate'] if dag_run.conf['Assignmenteffectivedate'] else
                    pendulum.now(config.pacific_timezone).strftime('%d/%m/%Y'),
                "status": "queued",
                "supervisorusername": dag_run.conf['SupervisorName']
            }
        )

        if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150 = rail.IfOperator(
            task_id='if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150',
            test='''{{ dag_run.conf.OHRID == dag_run.conf.SupervisorSSOID }}''',
            yes_task="insert_to_list_151",
            no_task="if_declare_variable_4_value_equals_to_yes_152",
        )

        insert_to_list_151 = rail.SetVariableOperator(
            task_id='insert_to_list_151',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": "Supervisor not assigned/updated since the user and supervisor SSO ID are same"
            }
        )

        def timeoff_triggered():
            timeoff_trigger = rail.get_dag_run_var(
                rail.result('declare_variable_4')['name'])
            return bool(timeoff_trigger == 'yes')

        if_declare_variable_4_value_equals_to_yes_152 = rail.IfOperator(
            task_id='if_declare_variable_4_value_equals_to_yes_152',
            test=timeoff_triggered,
            yes_task="log_jobpositiontitle_valuetocompareinthelookuptable_153",
            no_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208",
        )

        log_jobpositiontitle_valuetocompareinthelookuptable_153 = rail.PythonOperator(
            task_id='log_jobpositiontitle_valuetocompareinthelookuptable_153',
            python_callable=lambda dag_run: dag_run.conf['JobPositionTitle'] if dag_run.conf['JobPositionTitle'] in [
                'Field Engineer 2', 'Engineer - Remote Technical Support',
                'Junior FSE', 'Qualified FSE', 'Senior FSE',
                'Area Service Leader', 'Technical Support', 'RSL', 'Senior RSL'] else "NA"
        )

        def get_values_from_mapper(dag_run, entity_type1, entity_type2):
            entity_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity']
                and x['type'] == entity_type1
                and x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == entity_type2, czech_master_mapper))
            entity_values = [entity['value'] for entity in entity_types]
            return rail.smartjoin_by_delim(entity_values, '')

        log_timesheet_templatenamefrommapper_154 = rail.PythonOperator(
            task_id='log_timesheet_templatenamefrommapper_154',
            python_callable=lambda dag_run: get_values_from_mapper(
                dag_run, "Timesheet Template", rail.result('log_jobpositiontitle_valuetocompareinthelookuptable_153'))
        )

        if_log_timesheet_templatenamefrommapper_154_not_equals_to_datarestbulk_get_users3_6responsedfirsttimesheettemplatedisplaytext_155 = rail.IfOperator(
            task_id='if_log_timesheet_templatenamefrommapper_154_not_equals_to_datarestbulk_get_users3_6responsedfirsttimesheettemplatedisplaytext_155',
            test='''{{ result('log_timesheet_templatenamefrommapper_154') != result('bulk_get_users3_6')[0].timesheetTemplate.displayText }}''',
            yes_task="get_all_policy_sets_156",
            no_task="log_payrulenamefrommapper_163",
        )

        get_all_policy_sets_156 = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets_156',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets"
        )

        log_timesheet_templateuri_157 = rail.PythonOperator(
            task_id='log_timesheet_templateuri_157',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_policy_sets_156'), 'displayText', rail.result('log_timesheet_templatenamefrommapper_154'), 'uri')
        )

        if_log_timesheet_templateuri_157_present_158 = rail.IfOperator(
            task_id='if_log_timesheet_templateuri_157_present_158',
            test='''{{ result('log_timesheet_templateuri_157') | is_truthy }}''',
            yes_task="update_timesheet_template_159",
            no_task="insert_to_list_162",
        )

        update_timesheet_template_159 = rail.RepliconServiceOperator(
            task_id='update_timesheet_template_159',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.UserURI }}",
                "policySetUri": "{{ result('log_timesheet_templateuri_157') }}"
            }
        )

        insert_to_list_160 = rail.SetVariableOperator(
            task_id='insert_to_list_160',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Timesheet Tempalte updated"
            }
        )

        insert_to_list_162 = rail.SetVariableOperator(
            task_id='insert_to_list_162',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": "Timesheet Tempalte not updated since the template  - {{ result('log_timesheet_templatenamefrommapper_154') }} was not found in Replicon"
            }
        )

        def get_value_from_mapper(dag_run, entity_type, identifier):
            entity_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == entity_type and x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == identifier, czech_master_mapper))
            entity_values = [entity['value'] for entity in entity_types]
            return rail.smartjoin_by_delim(entity_values, '')

        log_payrulenamefrommapper_163 = rail.PythonOperator(
            task_id='log_payrulenamefrommapper_163',
            python_callable=lambda dag_run: get_value_from_mapper(dag_run, "Payrule", rail.result(
                'log_jobpositiontitle_valuetocompareinthelookuptable_153'))
        )

        def payrule_script_data():
            pay_schedules = []
            payrule_schedules = rail.result('bulk_get_users3_6')[
                0]['payRuleScriptSchedule']
            for payrule_schedule in payrule_schedules:
                if payrule_schedule['effectiveDate']:
                    effective_date = get_datetime_obj(
                        payrule_schedule['effectiveDate'])
                    if effective_date.date() > pendulum.now(config.pacific_timezone).date():
                        pay_schedules.append({
                            "uri": payrule_schedule['payRuleScript']['uri'],
                            "name": payrule_schedule['payRuleScript']['displayText'],
                            "date": effective_date.strftime('%d/%m/%Y')
                        })
                else:
                    pay_schedules.append({
                        "uri": payrule_schedule['payRuleScript']['uri'],
                        "name": payrule_schedule['payRuleScript']['displayText'],
                        "date": pendulum.now(config.pacific_timezone).strftime('%d/%m/%Y')
                    })

            return pay_schedules

        declare_list_164 = rail.SetVariableOperator(
            task_id='declare_list_164',
            append=False,
            name='Pay Rule schedule',
            value=[]
        )

        log_payruleschedule_165 = rail.PythonOperator(
            task_id='log_payruleschedule_165',
            python_callable=payrule_script_data
        )

        log_maxeffectivedate_174 = rail.PythonOperator(
            task_id='log_maxeffectivedate_174',
            python_callable=lambda: (max(
                datetime.strptime(x['date'], '%d/%m/%Y') for x in rail.result('log_payruleschedule_165'))).strftime('%d/%m/%Y') if rail.result('log_payruleschedule_165') else None
        )

        log_current_payrule_175 = rail.PythonOperator(
            task_id='log_current_payrule_175',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'log_payruleschedule_165'), 'date', rail.result('log_maxeffectivedate_174'), 'name')
        )

        if_log_payrulenamefrommapper_163_not_equals_to_dataloggerlog_current_payrule_175message_176 = rail.IfOperator(
            task_id='if_log_payrulenamefrommapper_163_not_equals_to_dataloggerlog_current_payrule_175message_176',
            test='''{{ result('log_payrulenamefrommapper_163') != result('log_current_payrule_175') }}''',
            yes_task="getallpayrulescripts_177",
            no_task="if_request_jobpositiontitle_equals_to_engineerremotetechnicalsupport_195",
        )

        getallpayrulescripts_177 = rail.RepliconServiceOperator(
            task_id='getallpayrulescripts_177',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts"
        )

        def payrule_script_schedule_list(dag_run):
            pay_schedules = []
            payrule_uri = rail.result('log_payruleuri_186')
            payrule_schedules = rail.result('bulk_get_users3_6')[
                0]['payRuleScriptSchedule']
            for payrule_schedule in payrule_schedules:
                if payrule_schedule['effectiveDate']:
                    effective_date = get_datetime_obj(
                        payrule_schedule['effectiveDate'])
                    if effective_date.date() != pendulum.now(config.pacific_timezone).date():
                        pay_schedules.append({
                            "payRuleScript": {
                                "uri": payrule_schedule['payRuleScript']['uri'],
                                "name": null
                            },
                            "effectiveDate": payrule_schedule['effectiveDate']
                        })
                else:
                    pay_schedules.append({
                        "payRuleScript": {
                            "uri": payrule_schedule['payRuleScript']['uri'],
                            "name": null
                        },
                        "effectiveDate": null
                    })

            eff_assignment_date = get_assignment_date(dag_run)
            pay_schedules.append({
                "payRuleScript": {
                    "uri": payrule_uri,
                    "name": null
                },
                "effectiveDate": eff_assignment_date
            })

            return pay_schedules

        log_payruleuri_186 = rail.PythonOperator(
            task_id='log_payruleuri_186',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getallpayrulescripts_177'), 'displayText', rail.result('log_payrulenamefrommapper_163'), 'uri', '')
        )

        if_log_payruleuri_186_present_187 = rail.IfOperator(
            task_id='if_log_payruleuri_186_present_187',
            test='''{{ result('log_payruleuri_186') | is_truthy }}''',
            yes_task="log_payruleschedule_188",
            no_task="insert_to_list_194",
        )

        log_payruleschedule_188 = rail.PythonOperator(
            task_id='log_payruleschedule_188',
            python_callable=payrule_script_schedule_list
        )

        put_pay_rule_script_assignment_schedule_for_user_191 = rail.RepliconServiceOperator(
            task_id='put_pay_rule_script_assignment_schedule_for_user_191',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['UserURI'],
                "scheduleEntries": rail.result('log_payruleschedule_188'),
            }
        )

        insert_to_list_192 = rail.SetVariableOperator(
            task_id='insert_to_list_192',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Payrule updated"
            }
        )

        insert_to_list_194 = rail.SetVariableOperator(
            task_id='insert_to_list_194',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": "Payrule not updated, since Payrule - {{ result('log_payrulenamefrommapper_163') }} is not available in Replicon"
            }
        )

        if_request_jobpositiontitle_equals_to_engineerremotetechnicalsupport_195 = rail.IfOperator(
            task_id='if_request_jobpositiontitle_equals_to_engineerremotetechnicalsupport_195',
            test='''{{ dag_run.conf.JobPositionTitle == 'Engineer - Remote Technical Support'  or dag_run.conf.JobPositionTitle == 'Field Engineer 2' }}''',
            yes_task="log_activitylist_196",
            no_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208",
        )

        def get_entity_from_mapper(dag_run, activity, job_position):
            entity_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity']
                and x['type'] == activity
                and x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == job_position, czech_master_mapper))
            return [entity['value'] for entity in entity_types]

        log_activitylist_196 = rail.PythonOperator(
            task_id='log_activitylist_196',
            python_callable=lambda dag_run: get_entity_from_mapper(dag_run, 'Activity', rail.result(
                'log_jobpositiontitle_valuetocompareinthelookuptable_153'))
        )

        get_all_activities_199 = rail.RepliconServiceOperator(
            task_id='get_all_activities_199',
            endpoint="/services/ActivityService1.svc/GetAllActivities"
        )

        def get_activity_list():
            activity_list = []
            for activity in rail.result('log_activitylist_196'):
                activity_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_activities_199'), 'displayText', activity, 'uri', '')
                if activity_uri:
                    activity_list.append(activity_uri)
            return activity_list

        log_activity_uris_203 = rail.PythonOperator(
            task_id='log_activity_uris_203',
            python_callable=get_activity_list
        )

        if_declare_list_198_list_items_greater_than_0_202 = rail.IfOperator(
            task_id='if_declare_list_198_list_items_greater_than_0_202',
            test='''{{ result('log_activity_uris_203') | length > 0 }}''',
            yes_task="put_activity_assignments_for_user_204",
            no_task="get_all_time_off_types_206",
        )

        put_activity_assignments_for_user_204 = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_204',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['UserURI'],
                "activityUris": rail.result('log_activity_uris_203')
            }
        )

        insert_to_list_205 = rail.SetVariableOperator(
            task_id='insert_to_list_205',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Activity assignment updated"
            }
        )

        get_all_time_off_types_206 = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types_206',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        trigger_dag_run_czech_time_off_policy_update_cz_compensation_207 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_czech_time_off_policy_update_cz_compensation_207',
            retries=0,
            items=[1],
            trigger_dag_id=f'ge_czech_time_off_policy_update_cz_compensation_time_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "useruri": dag_run.conf['UserURI'],
                "username": dag_run.conf['FirstName'] + " " + dag_run.conf['LastName'],
                "OHRID": dag_run.conf['OHRID'],
                "timeoffuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types_206'), 'displayText', "CZ_Compensation Time", 'uri')
            }
        )

        wait_for_completion_trigger_dag_run_czech_time_off_policy_update_cz_compensation_207 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_czech_time_off_policy_update_cz_compensation_207',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_czech_time_off_policy_update_cz_compensation_207") }}'
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

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208',
            message="na",
            # pylint: disable=unnecessary-lambda
            severity=lambda: get_status(),
            properties=lambda dag_run: {
                "action": "Update",
                "status": get_status(),
                "details": get_details(),
                "OHRID": dag_run.conf['OHRID'],
                "child_job_id": get_dagrun_ecid(dag_run),
                "username": dag_run.conf['FirstName'] + " " + dag_run.conf['LastName']
            }
        )

        ey_user_import_logs_add_entry_210 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_add_entry_210',
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "action": "Update",
                "status": "Error",
                "child_job_id": "{{ dag_run_ecid() }}",
                "details": "{{ get_error_message() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> ey_user_import_logs_add_entry_210
        can_run_batch_task >> rail.Label('No') >> declare_list_2
        declare_list_2 >> declare_list_3 >> declare_variable_4 >> bulk_get_users3_6 >> log_startdate_7 >> czech_master_mapper_search_entries_8 >> if_entry_col5_blank_9
        if_entry_col5_blank_9 >> rail.Label(
            'Yes') >> if_userdetails_isenabled_is_true_10
        if_entry_col5_blank_9 >> rail.Label(
            'No') >> dummy_operator_1 >> if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_17
        if_userdetails_isenabled_is_true_10 >> rail.Label(
            'Yes') >> disable_login_11 >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_12 >> \
            ey_user_import_logs_add_entry_210
        if_userdetails_isenabled_is_true_10 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_14
        if_userdetails_isenabled_is_not_true_14 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_15 >> ey_user_import_logs_add_entry_210
        if_userdetails_isenabled_is_not_true_14 >> rail.Label(
            'No') >> if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_17
        if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_17 >> rail.Label(
            'Yes') >> update_enddate_19 >> \
            ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_20 >> ey_user_import_logs_add_entry_210
        if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_17 >> rail.Label(
            'No') >> if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_22
        if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_22 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_23 >> \
            ey_user_import_logs_add_entry_210
        if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_22 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_rehire_25
        if_userdetails_isenabled_is_not_true_rehire_25 >> rail.Label(
            'Yes') >> if_request_hireeffectivedate_blank_26
        if_request_hireeffectivedate_blank_26 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_27 >> \
            ey_user_import_logs_add_entry_210
        if_request_hireeffectivedate_blank_26 >> rail.Label(
            'No') >> if_enddate_year_present_29
        if_enddate_year_present_29 >> rail.Label(
            'Yes') >> log_enddate_30 >> updateloginname_31 >> trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_callrecipeforrehire_32 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_callrecipeforrehire_32 >> ey_user_import_logs_add_entry_210
        if_enddate_year_present_29 >> rail.Label(
            'No') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_34 >> ey_user_import_logs_add_entry_210
        if_userdetails_isenabled_is_not_true_rehire_25 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_true_transfer_36
        if_userdetails_isenabled_is_true_transfer_36 >> rail.Label(
            'Yes') >> log_costcenterschedule_38 >> log_latesteffectivedate_49 >> log_latesteffective_legal_entity_name_50 >> \
            log_legal_entitynamefrommapper_51 >> if_log_latesteffective_legal_entity_name_50_blank_52
        if_log_latesteffective_legal_entity_name_50_blank_52 >> rail.Label(
            'Yes') >> log_enddate_53 >> updateloginname_56 >> disable_login_57 >> if_request_hireeffectivedate_blank_58
        if_request_hireeffectivedate_blank_58 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_59 >> ey_user_import_logs_add_entry_210
        if_request_hireeffectivedate_blank_58 >> rail.Label(
            'No') >> trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_callrecipefor_transfer_61 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_czech_ge_czech_add_v1_0async_callrecipefor_transfer_61 >> ey_user_import_logs_add_entry_210
        if_log_latesteffective_legal_entity_name_50_blank_52 >> rail.Label(
            'No') >> if_enddate_day_present_reverse_termination_63
        if_userdetails_isenabled_is_true_transfer_36 >> rail.Label(
            'No') >> dummy_operator_2 >> if_enddate_day_present_reverse_termination_63
        if_enddate_day_present_reverse_termination_63 >> rail.Label(
            'Yes') >> log_enddate_64 >> if_request_reverseterminationeffectivedate_present_65
        if_request_reverseterminationeffectivedate_present_65 >> rail.Label(
            'Yes') >> if_userdetails_isenabled_is_not_true_66
        if_userdetails_isenabled_is_not_true_66 >> rail.Label(
            'Yes') >> enable_login_67 >> insert_to_list_68 >> remove_enddate_69
        if_userdetails_isenabled_is_not_true_66 >> rail.Label(
            'No') >> remove_enddate_69 >> insert_to_list_70 >> \
            if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_71
        if_request_reverseterminationeffectivedate_present_65 >> rail.Label(
            'No') >> if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_71
        if_enddate_day_present_reverse_termination_63 >> rail.Label(
            'No') >> if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_71
        if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_71 >> rail.Label(
            'Yes') >> update_first_name_72 >> insert_to_list_73 >> \
            if_lastname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestlastnamedowncase_74
        if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_71 >> rail.Label(
            'No') >> if_lastname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestlastnamedowncase_74
        if_lastname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestlastnamedowncase_74 >> rail.Label(
            'Yes') >> update_last_name_75 >> insert_to_list_76 >> if_request_email_present_77
        if_lastname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestlastnamedowncase_74 >> rail.Label(
            'No') >> if_request_email_present_77
        if_request_email_present_77 >> rail.Label(
            'Yes') >> update_email_78 >> insert_to_list_79 >> log_valuefor_job_position_title_80
        if_request_email_present_77 >> rail.Label(
            'No') >> log_valuefor_job_position_title_80 >> log_urifor_job_position_title_81 >> if_request_jobpositiontitle_present_82
        if_request_jobpositiontitle_present_82 >> rail.Label(
            'Yes') >> update_text_value_customfield_83 >> insert_to_list_84 >> update_variable_85 >> log_valuefor_h_r_m_s_s_o_i_d_86
        if_request_jobpositiontitle_present_82 >> rail.Label(
            'No') >> log_valuefor_h_r_m_s_s_o_i_d_86 >> log_urifor_h_r_m_s_s_o_i_d_87 >> if_request_hrmssoid_present_88
        if_request_hrmssoid_present_88 >> rail.Label(
            'Yes') >> update_text_value_customfield_89 >> insert_to_list_90 >> log_valuefor_h_r_m_name_91
        if_request_hrmssoid_present_88 >> rail.Label(
            'No') >> log_valuefor_h_r_m_name_91 >> log_urifor_h_r_m_name_92 >> if_request_hrmname_present_93
        if_request_hrmname_present_93 >> rail.Label(
            'Yes') >> update_text_value_customfield_94 >> insert_to_list_95 >> log_valuefor_suspend_assignment_category_96
        if_request_hrmname_present_93 >> rail.Label(
            'No') >> log_valuefor_suspend_assignment_category_96 >> \
            log_urifor_suspend_assignment_category_97 >> if_request_suspendassignmentcategory_present_98
        if_request_suspendassignmentcategory_present_98 >> rail.Label(
            'Yes') >> get_all_custom_field_drop_down_options_99 >> log_uriforsuspendassignmentcategory_100 >> \
            update_dropdown_value_customfield_101 >> insert_to_list_102 >> if_request_supervisorssoid_present_103
        if_request_suspendassignmentcategory_present_98 >> rail.Label(
            'No') >> if_request_supervisorssoid_present_103
        if_request_supervisorssoid_present_103 >> rail.Label(
            'Yes') >> if_request_ohrid_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_104
        if_request_ohrid_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_104 >> rail.Label(
            'Yes') >> log_supervisorschedule_106 >> if_first_uri_present_116
        if_first_uri_present_116 >> rail.Label(
            'Yes') >> log_max_effectivedate_117 >> log_currentsupervisorloginname_118 >> if_log_currentsupervisorloginname_118_blank_119
        if_first_uri_present_116 >> rail.Label(
            'No') >> if_log_currentsupervisorloginname_118_blank_119
        if_log_currentsupervisorloginname_118_blank_119 >> rail.Label(
            'Yes') >> search_users_120 >> if_log_5_present_123
        if_log_5_present_123 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user2_124 >> log_checkif_manager_permissionsetisassigned_125 >> \
            log_checkif_end_user_manager_permissionsetisassigned_126 >> \
            if_log_checkif_manager_permissionsetisassigned_125_contains_approver_127
        if_log_checkif_manager_permissionsetisassigned_125_contains_approver_127 >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_131 >> insert_to_list_132 >> \
            if_log_checkif_manager_permissionsetisassigned_125_not_contains_approver_133
        if_log_checkif_manager_permissionsetisassigned_125_contains_approver_127 >> rail.Label(
            'No') >> if_log_checkif_manager_permissionsetisassigned_125_not_contains_approver_133
        if_log_checkif_manager_permissionsetisassigned_125_not_contains_approver_133 >> rail.Label(
            'Yes') >> get_all_permission_sets_134 >> log_permissionsetfor_approver_135 >> log_permissionsetfor_end_user_manager_136 >> \
            if_log_checkif_manager_permissionsetisassigned_125_blank_137
        if_log_checkif_manager_permissionsetisassigned_125_blank_137 >> rail.Label(
            'Yes') >> assign_permission_set_to_user_approver_138 >> if_log_checkif_end_user_manager_permissionsetisassigned_126_blank_139
        if_log_checkif_manager_permissionsetisassigned_125_blank_137 >> rail.Label(
            'No') >> if_log_checkif_end_user_manager_permissionsetisassigned_126_blank_139
        if_log_checkif_end_user_manager_permissionsetisassigned_126_blank_139 >> rail.Label(
            'Yes') >> assign_permission_set_to_user_enduser_manager_140 >> \
            update_supervisor_assignment_schedule_over_date_range_144
        if_log_checkif_end_user_manager_permissionsetisassigned_126_blank_139 >> rail.Label(
            'No') >> update_supervisor_assignment_schedule_over_date_range_144 >> insert_to_list_145 >> \
            if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146
        if_log_checkif_manager_permissionsetisassigned_125_not_contains_approver_133 >> rail.Label(
            'No') >> if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146
        if_log_5_present_123 >> rail.Label(
            'No') >> if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146
        if_log_currentsupervisorloginname_118_blank_119 >> rail.Label(
            'No') >> if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146
        if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146 >> rail.Label(
            'Yes') >> ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_147 >> \
            if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisdisableddonotupdatesupervisor_148
        if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146 >> rail.Label(
            'No') >> if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisdisableddonotupdatesupervisor_148
        if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisdisableddonotupdatesupervisor_148 >> rail.Label(
            'Yes') >> ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_149 >> \
            if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150
        if_log_5_blank_false_if_log_5_blank_false_supervisorprofileisdisableddonotupdatesupervisor_148 >> rail.Label(
            'No') >> if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150
        if_request_ohrid_not_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_104 >> rail.Label(
            'No') >> if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150
        if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150 >> rail.Label(
            'Yes') >> insert_to_list_151 >> if_declare_variable_4_value_equals_to_yes_152
        if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150 >> rail.Label(
            'No') >> if_declare_variable_4_value_equals_to_yes_152
        if_request_supervisorssoid_present_103 >> rail.Label(
            'No') >> if_declare_variable_4_value_equals_to_yes_152
        if_declare_variable_4_value_equals_to_yes_152 >> rail.Label(
            'Yes') >> log_jobpositiontitle_valuetocompareinthelookuptable_153 >> log_timesheet_templatenamefrommapper_154 >> \
            if_log_timesheet_templatenamefrommapper_154_not_equals_to_datarestbulk_get_users3_6responsedfirsttimesheettemplatedisplaytext_155
        if_log_timesheet_templatenamefrommapper_154_not_equals_to_datarestbulk_get_users3_6responsedfirsttimesheettemplatedisplaytext_155 >> rail.Label(
            'Yes') >> get_all_policy_sets_156 >> log_timesheet_templateuri_157 >> if_log_timesheet_templateuri_157_present_158
        if_log_timesheet_templateuri_157_present_158 >> rail.Label(
            'Yes') >> update_timesheet_template_159 >> insert_to_list_160 >> log_payrulenamefrommapper_163
        if_log_timesheet_templateuri_157_present_158 >> rail.Label(
            'No') >> insert_to_list_162 >> log_payrulenamefrommapper_163
        if_log_timesheet_templatenamefrommapper_154_not_equals_to_datarestbulk_get_users3_6responsedfirsttimesheettemplatedisplaytext_155 >> rail.Label(
            'No') >> log_payrulenamefrommapper_163 >> declare_list_164 >> log_payruleschedule_165 >> log_maxeffectivedate_174 >> \
            log_current_payrule_175 >> if_log_payrulenamefrommapper_163_not_equals_to_dataloggerlog_current_payrule_175message_176
        if_log_payrulenamefrommapper_163_not_equals_to_dataloggerlog_current_payrule_175message_176 >> rail.Label(
            'Yes') >> getallpayrulescripts_177 >> log_payruleuri_186 >> if_log_payruleuri_186_present_187
        if_log_payruleuri_186_present_187 >> rail.Label(
            'Yes') >> log_payruleschedule_188 >> put_pay_rule_script_assignment_schedule_for_user_191 >> insert_to_list_192 >> \
            if_request_jobpositiontitle_equals_to_engineerremotetechnicalsupport_195
        if_log_payruleuri_186_present_187 >> rail.Label(
            'No') >> insert_to_list_194 >> if_request_jobpositiontitle_equals_to_engineerremotetechnicalsupport_195
        if_log_payrulenamefrommapper_163_not_equals_to_dataloggerlog_current_payrule_175message_176 >> rail.Label(
            'No') >> if_request_jobpositiontitle_equals_to_engineerremotetechnicalsupport_195
        if_request_jobpositiontitle_equals_to_engineerremotetechnicalsupport_195 >> rail.Label(
            'Yes') >> log_activitylist_196 >> get_all_activities_199 >> log_activity_uris_203 >> if_declare_list_198_list_items_greater_than_0_202

        if_declare_list_198_list_items_greater_than_0_202 >> rail.Label(
            'Yes') >> put_activity_assignments_for_user_204 >> insert_to_list_205 >> get_all_time_off_types_206
        if_declare_list_198_list_items_greater_than_0_202 >> rail.Label(
            'No') >> get_all_time_off_types_206 >> \
            trigger_dag_run_czech_time_off_policy_update_cz_compensation_207 >> \
            wait_for_completion_trigger_dag_run_czech_time_off_policy_update_cz_compensation_207 >> \
            ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208
        if_request_jobpositiontitle_equals_to_engineerremotetechnicalsupport_195 >> rail.Label(
            'No') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208
        if_declare_variable_4_value_equals_to_yes_152 >> rail.Label(
            'No') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208 >> ey_user_import_logs_add_entry_210 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
