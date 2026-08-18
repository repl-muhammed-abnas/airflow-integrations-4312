
from datetime import timedelta, datetime
import itertools
import pendulum
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
from ge_healthcare.user_sync_netherlands.netherlands_master_mapper import netherlands_master_mapper
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'gehealthcare_netherlands_user_update_{config.instance}',
        description=f'GE netherlands User Update {config.instance}',
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
            name='Update Timesheet Template?',
            value="no"
        )

        declare_variable_5 = rail.SetVariableOperator(
            task_id='declare_variable_5',
            append=False,
            name='Update Payrule?',
            value="no"
        )

        def get_datetime_obj(effectiveDate):
            year = effectiveDate['year']
            month = effectiveDate['month']
            day = effectiveDate['day']
            return datetime.strptime(f"{day}/{month}/{year}", '%d/%m/%Y')

        bulk_get_users3_6 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_6',
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

        log_startdate_7 = rail.PythonOperator(
            task_id='log_startdate_7',
            python_callable=lambda: rail.render_template(
                "{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.day }}/{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.month }}/{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.year }}")
        )

        netherlands_master_mapper_search_entries_8 = rail.PythonOperator(
            task_id='netherlands_master_mapper_search_entries_8',
            python_callable=lambda dag_run: list(
                filter(lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'], netherlands_master_mapper))
        )

        if_entry_col5_blank_8 = rail.IfOperator(
            task_id='if_entry_col5_blank_8',
            test='''{{ result('netherlands_master_mapper_search_entries_8') | length == 0 }}''',
            yes_task="if_userdetails_isenabled_is_true_9",
            no_task="dummy_operator_1",
        )

        dummy_operator_1 = rail.EmptyOperator(
            task_id="dummy_operator_1"
        )

        if_userdetails_isenabled_is_true_9 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_9',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.isEnabled == True }}''',
            yes_task="disable_login_10",
            no_task="if_userdetails_isenabled_is_not_true_13",
        )

        disable_login_10 = rail.RepliconServiceOperator(
            task_id='disable_login_10',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_11 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_11',
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

        if_userdetails_isenabled_is_not_true_13 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_13',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.isEnabled == False }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_14",
            no_task="if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_16",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_14 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_14',
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
            if dag_run.conf['TerminationEffectiveDate'] and dag_run.conf['RevTermEffectiveDate'] is None \
                    and rail.result('bulk_get_users3_6')[0]['userDetails']['isEnabled'] is True:
                temination_date = datetime.strptime(
                    dag_run.conf['TerminationEffectiveDate'], '%d/%m/%Y')
                start_date = datetime.strptime(rail.result(
                    'log_startdate_7'), '%d/%m/%Y')
                if temination_date > start_date + timedelta(days=-1):
                    return True
            return False

        if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_16 = rail.IfOperator(
            task_id='if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_16',
            test=is_temination_date_reached,
            yes_task="update_enddate_18",
            no_task="if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_21",
        )

        update_enddate_18 = rail.RepliconServiceOperator(
            task_id='update_enddate_18',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": rail.result('bulk_get_users3_6')[0]['userDetails']['employmentDateRange']['startDate']['year'],
                        "month": rail.result('bulk_get_users3_6')[0]['userDetails']['employmentDateRange']['startDate']['month'],
                        "day": rail.result('bulk_get_users3_6')[0]['userDetails']['employmentDateRange']['startDate']['day'],
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

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_19 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_19',
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
            if dag_run.conf['TerminationEffectiveDate'] and \
                dag_run.conf['RevTermEffectiveDate'] is None and \
                    rail.result('bulk_get_users3_6')[0]['userDetails']['isEnabled'] is True:
                temination_date = datetime.strptime(
                    dag_run.conf['TerminationEffectiveDate'], '%d/%m/%Y')
                start_date = datetime.strptime(rail.result(
                    'log_startdate_7'), '%d/%m/%Y')
                if temination_date < start_date:
                    return True
            return False

        if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_21 = rail.IfOperator(
            task_id='if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_21',
            test=is_temination_date_not_reached,
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_22",
            no_task="if_userdetails_isenabled_is_not_true_rehire_24",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_22 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_22',
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

        if_userdetails_isenabled_is_not_true_rehire_24 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_rehire_24',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.isEnabled == False and dag_run.conf.RevTermEffectiveDate | is_falsy }}''',
            yes_task="if_request_hireeffectivedate_blank_25",
            no_task="if_userdetails_isenabled_is_true_transfer_36",
        )

        if_request_hireeffectivedate_blank_25 = rail.IfOperator(
            task_id='if_request_hireeffectivedate_blank_25',
            test='''{{ dag_run.conf.HireEffectiveDate | is_falsy }}''',
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
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ result('bulk_get_users3_6')[0].securityConfiguration.loginName }}{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate.month }}{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate.day }}{{ result('log_enddate_30') }}"
            }
        )

        trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_add_v1_0async_callrecipeforrehire_32 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_add_v1_0async_callrecipeforrehire_32',
            retries=0,
            items=[1],
            trigger_dag_id=f'gehealthcare_netherlands_add_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "EmployeeFirstName": dag_run.conf['FirstName'],
                "EmployeeLastName": dag_run.conf['LastName'],
                "EmployeeEmailAddress": dag_run.conf['Email'],
                "OHRID": dag_run.conf['OHRID'],
                "LegalEntityHireDate": dag_run.conf['LegalEntityHireDate'],
                "LegacyPayrollID": dag_run.conf['LegacyPayrollID'],
                "EmployeeGender": dag_run.conf['Employeegender'],
                "MaritalStatus": dag_run.conf['MaritalStatus'],
                "JobPositionTitle": dag_run.conf['JobPositionTitle'],
                "SupervisorSSOID": dag_run.conf['SupervisorSSOID'],
                "SupervisorName": dag_run.conf['SupervisorName'],
                "AssignmentCategory": dag_run.conf['AssignmentCategory'],
                "SuspendAssignmentCategory": dag_run.conf['SuspendAssignmentCategory'],
                "LocationName": None,
                "Contractattributeannualvacationeligibility": None,
                "Subbiz": None,
                "Worktimesystem": None,
                "Educationlevel": None,
                "Specialworkschedule": None,
                "WorkLocation": dag_run.conf['WorkLocation'],
                "AdjustedServiceDate": None,
                "JobType": dag_run.conf['JobType'],
                "HealthcareProductLineEIT": dag_run.conf['HealthcareProductLineEIT'],
                "Payroll": dag_run.conf['Payroll'],
                "Dateofbirth": None,
                "OvertimeEligibility": dag_run.conf['OvertimeEligibility'],
                "Salarybasis": None,
                "Departmentalstom": None,
                "Previousemploymentsperiodsenddate": None,
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
                "Educationperiodsstartdate": null,
                "Educationperiodsenddate": null,
                "Previousemploymentsperiodsstartdate": "na",
                "HRMSSOID": dag_run.conf['HRMSSOID'],
                "HRMName": dag_run.conf['HRMName'],
                "AssignmentEffectiveDate": dag_run.conf['AssignmentEffectiveDate'],
                "HireEffectiveDate": dag_run.conf['HireEffectiveDate'],
                "RevTermEffectiveDate": dag_run.conf['RevTermEffectiveDate'],
                "type": "Rehire",
                "CareerBand": dag_run.conf['CareerBand'],
                "supervisor_processing_log": dag_run.conf['supervisor_processing_log'],
                "Departmenturi": dag_run.conf['Departmenturi']
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_add_v1_0async_callrecipeforrehire_32 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_add_v1_0async_callrecipeforrehire_32',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_add_v1_0async_callrecipeforrehire_32") }}'
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
                "details": "The existing profile doesn't have an end date in Replicon",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}"
            }
        )

        if_userdetails_isenabled_is_true_transfer_36 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_transfer_36',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate | is_truthy }}''',
            yes_task="log_enddate_37",
            no_task="dummy_operator_2",
        )

        log_enddate_37 = rail.PythonOperator(
            task_id='log_enddate_37',
            python_callable=lambda: rail.render_template(
                "{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate.day }}/{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate.month }}/{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate.year }}")
        )

        def revers_eff_date_in_user_date_range(dag_run):
            user_end_date = rail.result('bulk_get_users3_6')[
                0]['userDetails']['employmentDateRange']['endDate']
            if dag_run.conf['RevTermEffectiveDate'] and user_end_date and rail.result('log_startdate_7'):
                revers_eff_date = datetime.strptime(
                    dag_run.conf['RevTermEffectiveDate'], '%d/%m/%Y')
                user_start_date = datetime.strptime(
                    rail.result('log_startdate_7'), '%d/%m/%Y')
                user_end_date = get_datetime_obj(user_end_date)
                if user_start_date < revers_eff_date < user_end_date:
                    return True
            return False

        if_request_reverseterminationeffectivedate_present_38 = rail.IfOperator(
            task_id='if_request_reverseterminationeffectivedate_present_38',
            test=revers_eff_date_in_user_date_range,
            yes_task="if_userdetails_isenabled_is_not_true_39",
            no_task="if_request_legalentity_present_changein_legal_entity_transfer_39",
        )

        if_userdetails_isenabled_is_not_true_39 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_39',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.isEnabled == True }}''',
            yes_task="enable_login_40",
            no_task="remove_enddate_42",
        )

        enable_login_40 = rail.RepliconServiceOperator(
            task_id='enable_login_40',
            endpoint="/services/securityservice1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        insert_to_list_41 = rail.SetVariableOperator(
            task_id='insert_to_list_41',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "User profile re-enabled, reverse termination date older than end date and newer than start date."
            }
        )

        remove_enddate_42 = rail.RepliconServiceOperator(
            task_id='remove_enddate_42',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
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

        insert_to_list_43 = rail.SetVariableOperator(
            task_id='insert_to_list_43',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "End date removed, reverse termination date older than end date and newer than start date."
            }
        )

        def is_legal_entity_changes(dag_run):
            if dag_run.conf['LegalEntity']:
                mapper_legalentity_info = list(filter(lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['legacy_payroll_id']
                                                      == dag_run.conf['LegacyPayrollID'] and x['type'] == "legalcombination", netherlands_master_mapper))
                mapper_legalentity = mapper_legalentity_info[0][
                    'value'] if mapper_legalentity_info else "No Assignment"
                existing_legal_entity = rail.result('bulk_get_users3_6')[
                    0]['costCenterSchedule'][0]['costCenter']['displayText']
                return bool(mapper_legalentity != existing_legal_entity)
            return True

        if_request_legalentity_present_changein_legal_entity_transfer_39 = rail.IfOperator(
            task_id='if_request_legalentity_present_changein_legal_entity_transfer_39',
            test=is_legal_entity_changes,
            yes_task="if_request_hireeffectivedate_blank_40",
            no_task="if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_44",
        )

        if_request_hireeffectivedate_blank_40 = rail.IfOperator(
            task_id='if_request_hireeffectivedate_blank_40',
            test='''{{ dag_run.conf.HireEffectiveDate | is_falsy }}''',
            yes_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_41",
            no_task="update_enddate_44",
        )

        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_41 = rail.WriteLogOperator(
            task_id='ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_41',
            message="na",
            severity="Skipped",
            properties={
                "action": "Rehire",
                "status": "Skipped",
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "details": "Hire effective date not available",
                "username": "{{ dag_run.conf.FirstName }} {{ dag_run.conf.LastName }}"
            }
        )

        update_enddate_44 = rail.RepliconServiceOperator(
            task_id='update_enddate_44',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": rail.result('bulk_get_users3_6')[0]['userDetails']['employmentDateRange']['startDate']['year'],
                        "month": rail.result('bulk_get_users3_6')[0]['userDetails']['employmentDateRange']['startDate']['month'],
                        "day": rail.result('bulk_get_users3_6')[0]['userDetails']['employmentDateRange']['startDate']['day'],
                    },
                    "endDate": {
                        "year": pendulum.now(config.pacific_timezone).year,
                        "month": pendulum.now(config.pacific_timezone).month,
                        "day": pendulum.now(config.pacific_timezone).day,
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        disable_login_45 = rail.RepliconServiceOperator(
            task_id='disable_login_45',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        updateloginname_46 = rail.RepliconServiceOperator(
            task_id='updateloginname_46',
            endpoint="/services/securityservice1.svc/SetSSOAuthenticationForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "loginName": rail.result('bulk_get_users3_6')[0]['securityConfiguration']['loginName'] + pendulum.now(config.pacific_timezone).strftime('%d%m%Y')
            }
        )

        trigger_dag_run_ge_user_sync_netherlands_add_v1_0_47 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_netherlands_add_v1_0_47',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_netherlands_add_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "EmployeeFirstName": dag_run.conf['FirstName'],
                "EmployeeLastName": dag_run.conf['LastName'],
                "EmployeeEmailAddress": dag_run.conf['Email'],
                "OHRID": dag_run.conf['OHRID'],
                "LegalEntityHireDate": dag_run.conf['LegalEntityHireDate'],
                "LegacyPayrollID": dag_run.conf['LegacyPayrollID'],
                "EmployeeGender": dag_run.conf['Employeegender'],
                "MaritalStatus": dag_run.conf['MaritalStatus'],
                "JobPositionTitle": dag_run.conf['JobPositionTitle'],
                "SupervisorSSOID": dag_run.conf['SupervisorSSOID'],
                "SupervisorName": dag_run.conf['SupervisorName'],
                "AssignmentCategory": dag_run.conf['AssignmentCategory'],
                "SuspendAssignmentCategory": dag_run.conf['SuspendAssignmentCategory'],
                "LocationName": None,
                "Contractattributeannualvacationeligibility": None,
                "Subbiz": None,
                "Worktimesystem": None,
                "Educationlevel": None,
                "Specialworkschedule": None,
                "WorkLocation": None,
                "AdjustedServiceDate": None,
                "JobType": dag_run.conf['JobType'],
                "HealthcareProductLineEIT": dag_run.conf['HealthcareProductLineEIT'],
                "Payroll": dag_run.conf['Payroll'],
                "Dateofbirth": None,
                "OvertimeEligibility": dag_run.conf['OvertimeEligibility'],
                "Salarybasis": None,
                "Departmentalstom": None,
                "Previousemploymentsperiodsenddate": None,
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
                "Educationperiodsstartdate": null,
                "Educationperiodsenddate": null,
                "Previousemploymentsperiodsstartdate": "na",
                "HRMSSOID": dag_run.conf['HRMSSOID'],
                "HRMName": dag_run.conf['HRMName'],
                "AssignmentEffectiveDate": dag_run.conf['AssignmentEffectiveDate'],
                "HireEffectiveDate": dag_run.conf['HireEffectiveDate'],
                "RevTermEffectiveDate": dag_run.conf['RevTermEffectiveDate'],
                "type": "Rehire",
                "CareerBand": dag_run.conf['CareerBand'],
                "supervisor_processing_log": dag_run.conf['supervisor_processing_log'],
                "Departmenturi": dag_run.conf['Departmenturi']
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_add_v1_0_47 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_add_v1_0_47',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_netherlands_add_v1_0_47") }}'
        )

        if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_44 = rail.IfOperator(
            task_id='if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_44',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.firstName | lower != dag_run.conf.FirstName | lower }}''',
            yes_task="update_first_name_45",
            no_task="if_lastname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestlastnamedowncase_47",
        )

        update_first_name_45 = rail.RepliconServiceOperator(
            task_id='update_first_name_45',
            endpoint="/services/userService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.FirstName }}"
            }
        )

        insert_to_list_46 = rail.SetVariableOperator(
            task_id='insert_to_list_46',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "First name updated"
            }
        )

        if_lastname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestlastnamedowncase_47 = rail.IfOperator(
            task_id='if_lastname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestlastnamedowncase_47',
            test='''{{ result('bulk_get_users3_6')[0].userDetails.lastName | lower != dag_run.conf.LastName | lower }}''',
            yes_task="update_last_name_48",
            no_task="if_request_email_present_50",
        )

        update_last_name_48 = rail.RepliconServiceOperator(
            task_id='update_last_name_48',
            endpoint="/services/userService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.LastName }}"
            }
        )

        insert_to_list_49 = rail.SetVariableOperator(
            task_id='insert_to_list_49',
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

        if_request_email_present_50 = rail.IfOperator(
            task_id='if_request_email_present_50',
            test=email_validation,
            yes_task="update_email_51",
            no_task="insert_to_list_52",
        )

        update_email_51 = rail.RepliconServiceOperator(
            task_id='update_email_51',
            endpoint="/services/userService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.Email }}"
            }
        )

        insert_to_list_52 = rail.SetVariableOperator(
            task_id='insert_to_list_52',
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
            return custom_infos[0]['text'] if custom_infos else ""

        log_valuefor_job_position_title_52 = rail.PythonOperator(
            task_id='log_valuefor_job_position_title_52',
            python_callable=lambda: get_custom_value("Job/Position Title")
        )

        def get_custom_uri(custom_field_name):
            existing_custom_fields = rail.result('bulk_get_users3_6')[0][
                'userDetails']['customFieldValues']
            custom_infos = list(filter(
                lambda x: x['customField'] and x['customField']['displayText'].lower() == custom_field_name.lower(), existing_custom_fields))
            return custom_infos[0]['customField']['uri'] if custom_infos else None

        log_urifor_job_position_title_53 = rail.PythonOperator(
            task_id='log_urifor_job_position_title_53',
            python_callable=lambda: get_custom_uri('Job/Position Title')
        )

        if_request_jobpositiontitle_present_54 = rail.IfOperator(
            task_id='if_request_jobpositiontitle_present_54',
            test='''{{ dag_run.conf.JobPositionTitle | is_truthy and result('log_valuefor_job_position_title_52') | lower != dag_run.conf.JobPositionTitle | lower }}''',
            yes_task="update_text_value_customfield_56",
            no_task="log_valuefor_h_r_m_s_s_o_i_d_58",
        )

        update_text_value_customfield_56 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_56',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_urifor_job_position_title_53') }}",
                "value": "{{ dag_run.conf.JobPositionTitle }}"
            }
        )

        insert_to_list_57 = rail.SetVariableOperator(
            task_id='insert_to_list_57',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Job/Position Title updated"
            }
        )

        log_valuefor_h_r_m_s_s_o_i_d_58 = rail.PythonOperator(
            task_id='log_valuefor_h_r_m_s_s_o_i_d_58',
            python_callable=lambda: get_custom_value("HRM SSO ID")
        )

        log_urifor_h_r_m_s_s_o_i_d_58 = rail.PythonOperator(
            task_id='log_urifor_h_r_m_s_s_o_i_d_58',
            python_callable=lambda: get_custom_uri('HRM SSO ID')
        )

        if_request_hrmssoid_present_59 = rail.IfOperator(
            task_id='if_request_hrmssoid_present_59',
            test='''{{ dag_run.conf.HRMSSOID | is_truthy and result('log_valuefor_h_r_m_s_s_o_i_d_58') | lower != dag_run.conf.HRMSSOID | lower }}''',
            yes_task="update_text_value_customfield_61",
            no_task="log_valuefor_h_r_m_name_63",
        )

        update_text_value_customfield_61 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_61',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_urifor_h_r_m_s_s_o_i_d_58') }}",
                "value": "{{ dag_run.conf.HRMSSOID }}"
            }
        )

        insert_to_list_62 = rail.SetVariableOperator(
            task_id='insert_to_list_62',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "HRMSSOID updated"
            }
        )

        log_valuefor_h_r_m_name_63 = rail.PythonOperator(
            task_id='log_valuefor_h_r_m_name_63',
            python_callable=lambda: get_custom_value("HRM Name")
        )

        log_urifor_h_r_m_name_63 = rail.PythonOperator(
            task_id='log_urifor_h_r_m_name_63',
            python_callable=lambda: get_custom_uri('HRM Name')
        )

        if_request_hrmname_present_64 = rail.IfOperator(
            task_id='if_request_hrmname_present_64',
            test='''{{ dag_run.conf.HRMName | is_truthy and result('log_valuefor_h_r_m_name_63') | lower != dag_run.conf.HRMSSOID | lower}}''',
            yes_task="update_text_value_customfield_66",
            no_task="if_request_ohrid_not_equals_to_requestrequestsupervisorssoid_104",
        )

        update_text_value_customfield_66 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_66',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_urifor_h_r_m_name_63') }}",
                "value": "{{ dag_run.conf.HRMName }}"
            }
        )

        insert_to_list_67 = rail.SetVariableOperator(
            task_id='insert_to_list_67',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "HRM Name updated"
            }
        )

        log_existing_valuefor_payroll_81 = rail.PythonOperator(
            task_id='log_existing_valuefor_payroll_81',
            python_callable=lambda: get_custom_value(
                "Payroll")
        )

        if_request_payroll_present_82 = rail.IfOperator(
            task_id='if_request_payroll_present_82',
            test='''{{ dag_run.conf.Payroll | is_truthy and dag_run.conf.Payroll | lower != result('log_existing_valuefor_payroll_81') | lower }}''',
            yes_task="log_urifor_payroll_83",
            no_task="log_existing_valuefor_healthcare_product_lineeit_86",
        )

        log_urifor_payroll_83 = rail.PythonOperator(
            task_id='log_urifor_payroll_83',
            python_callable=lambda: get_custom_uri("Payroll")
        )

        update_text_value_customfield_84 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_84',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_urifor_payroll_83') }}",
                "value": "{{ dag_run.conf.Payroll }}"
            }
        )

        insert_to_list_85 = rail.SetVariableOperator(
            task_id='insert_to_list_85',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Payroll updated"
            }
        )

        log_existing_valuefor_healthcare_product_lineeit_86 = rail.PythonOperator(
            task_id='log_existing_valuefor_healthcare_product_lineeit_86',
            python_callable=lambda: get_custom_value(
                "Healthcare Product Line EIT")
        )

        if_request_healthcareproductlineeit_present_87 = rail.IfOperator(
            task_id='if_request_healthcareproductlineeit_present_87',
            test='''{{ dag_run.conf.HealthcareProductLineEIT | is_truthy and dag_run.conf.HealthcareProductLineEIT | lower != result('log_existing_valuefor_healthcare_product_lineeit_86') | lower }}''',
            yes_task="log_urifor_healthcare_product_lineeit_88",
            no_task="log_existing_valuefor_job_type_93",
        )

        log_urifor_healthcare_product_lineeit_88 = rail.PythonOperator(
            task_id='log_urifor_healthcare_product_lineeit_88',
            python_callable=lambda: get_custom_uri(
                "Healthcare Product Line EIT")
        )

        update_text_value_customfield_89 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_89',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_urifor_healthcare_product_lineeit_88') }}",
                "value": "{{ dag_run.conf.HealthcareProductLineEIT }}"
            }
        )

        insert_to_list_90 = rail.SetVariableOperator(
            task_id='insert_to_list_90',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Healthcare Product Line EIT updated"
            }
        )

        log_existing_valuefor_job_type_93 = rail.PythonOperator(
            task_id='log_existing_valuefor_job_type_93',
            python_callable=lambda: get_custom_value(
                "Job Type")
        )

        if_request_jobtype_present_94 = rail.IfOperator(
            task_id='if_request_jobtype_present_94',
            test='''{{ dag_run.conf.JobType | is_truthy and dag_run.conf.JobType | lower != result('log_existing_valuefor_job_type_93') | lower() }}''',
            yes_task="log_urifor_job_type_95",
            no_task="log_existing_valuefor_career_band_107",
        )

        log_urifor_job_type_95 = rail.PythonOperator(
            task_id='log_urifor_job_type_95',
            python_callable=lambda: get_custom_uri(
                "Job Type")
        )

        update_text_value_customfield_96 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_96',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_urifor_job_type_95') }}",
                "value": "{{ dag_run.conf.JobType }}"
            }
        )

        insert_to_list_97 = rail.SetVariableOperator(
            task_id='insert_to_list_97',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Job Type updated"
            }
        )

        update_variable_98 = rail.SetVariableOperator(
            task_id='update_variable_98',
            append=False,
            name='{{ result("declare_variable_4").name }}',
            value="yes"
        )

        update_variable_99 = rail.SetVariableOperator(
            task_id='update_variable_99',
            append=False,
            name='{{ result("declare_variable_5").name }}',
            value="yes"
        )

        if_log_existing_valuefor_job_type_93_present_100 = rail.IfOperator(
            task_id='if_log_existing_valuefor_job_type_93_present_100',
            test='''{{ result('log_existing_valuefor_job_type_93') | is_truthy }}''',
            yes_task="ge_netherlands_user_sync_master_mapper_search_entries_101",
            no_task="log_existing_valuefor_career_band_107",
        )

        ge_netherlands_user_sync_master_mapper_search_entries_101 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_101',
            python_callable=lambda: list(filter(lambda x: x['type'] == "Restricted Timeoff Type assignment" and x['jobtype'] == rail.result(
                'log_existing_valuefor_job_type_93'), netherlands_master_mapper))
        )

        log_existing_timeoff_assignment_102 = rail.PythonOperator(
            task_id='log_existing_timeoff_assignment_102',
            python_callable=lambda:  "Restricted Timeoff Type assignment" if len(rail.result(
                'ge_netherlands_user_sync_master_mapper_search_entries_101')) > 0 else "Non Restricted Timeoff Type assignment"
        )

        ge_netherlands_user_sync_master_mapper_search_entries_103 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_103',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x['type'] == "Restricted Timeoff Type assignment" and x['jobtype'] == dag_run.conf['JobType'], netherlands_master_mapper))
        )

        log_new_timeoff_assignment_104 = rail.PythonOperator(
            task_id='log_new_timeoff_assignment_104',
            python_callable=lambda:  "Restricted Timeoff Type assignment" if len(rail.result(
                'ge_netherlands_user_sync_master_mapper_search_entries_103')) > 0 else "Non Restricted Timeoff Type assignment"
        )

        if_log_new_timeoff_assignment_104_not_equals_to_existing_timeoff_assignment_105 = rail.IfOperator(
            task_id='if_log_new_timeoff_assignment_104_not_equals_to_existing_timeoff_assignment_105',
            test='''{{ result('log_new_timeoff_assignment_104') != result('log_existing_timeoff_assignment_102') }}''',
            yes_task="log_job_type_changedand_timeoffneedstobeupdated_106",
            no_task="log_existing_valuefor_career_band_107",
        )

        log_job_type_changedand_timeoffneedstobeupdated_106 = rail.PythonOperator(
            task_id='log_job_type_changedand_timeoffneedstobeupdated_106',
            python_callable=lambda:  "Job Type Changed and Timeoff needs to be updated"
        )

        log_existing_valuefor_career_band_107 = rail.PythonOperator(
            task_id='log_existing_valuefor_career_band_107',
            python_callable=lambda: get_custom_value(
                "Career Band")
        )

        if_request_careerband_present_108 = rail.IfOperator(
            task_id='if_request_careerband_present_108',
            test='''{{ dag_run.conf.CareerBand | is_truthy and dag_run.conf.CareerBand | lower != result('log_existing_valuefor_career_band_107') | lower }}''',
            yes_task="log_urifor_career_band_109",
            no_task="log_existing_valuefor_work_112",
        )

        log_urifor_career_band_109 = rail.PythonOperator(
            task_id='log_urifor_career_band_109',
            python_callable=lambda: get_custom_uri(
                "Career Band")
        )

        update_text_value_customfield_110 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_110',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_urifor_career_band_109') }}",
                "value": "{{ dag_run.conf.CareerBand }}"
            }
        )

        insert_to_list_111 = rail.SetVariableOperator(
            task_id='insert_to_list_111',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Career Band updated"
            }
        )

        log_existing_valuefor_work_112 = rail.PythonOperator(
            task_id='log_existing_valuefor_work_112',
            python_callable=lambda: get_custom_value(
                "Work")
        )

        if_request_work_present_113 = rail.IfOperator(
            task_id='if_request_work_present_113',
            test='''{{ dag_run.conf.WorkLocation | is_truthy and dag_run.conf.WorkLocation | lower != result('log_existing_valuefor_work_112') }}''',
            yes_task="log_urifor_work_114",
            no_task="log_existing_valuefor_work_location_117",
        )

        log_urifor_work_114 = rail.PythonOperator(
            task_id='log_urifor_work_114',
            python_callable=lambda: get_custom_uri(
                "Work")
        )

        update_text_value_customfield_115 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_115',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_urifor_work_114') }}",
                "value": "{{ dag_run.conf.WorkLocation }}"
            }
        )

        insert_to_list_116 = rail.SetVariableOperator(
            task_id='insert_to_list_116',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Work updated"
            }
        )

        log_existing_valuefor_work_location_117 = rail.PythonOperator(
            task_id='log_existing_valuefor_work_location_117',
            python_callable=lambda: get_custom_value(
                "Work Location")
        )

        if_request_locationname_present_118 = rail.IfOperator(
            task_id='if_request_locationname_present_118',
            test='''{{ dag_run.conf.LocationName | is_truthy and dag_run.conf.LocationName | lower != result('log_existing_valuefor_work_location_117') }}''',
            yes_task="log_urifor_work_location_119",
            no_task="log_existing_valuefor_suspend_assignment_category_122",
        )

        log_urifor_work_location_119 = rail.PythonOperator(
            task_id='log_urifor_work_location_119',
            python_callable=lambda: get_custom_uri(
                "Work Location")
        )

        update_text_value_customfield_120 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_120',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_urifor_work_location_119') }}",
                "value": "{{ dag_run.conf.LocationName }}"
            }
        )

        insert_to_list_121 = rail.SetVariableOperator(
            task_id='insert_to_list_121',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Work Location updated"
            }
        )

        log_existing_valuefor_suspend_assignment_category_122 = rail.PythonOperator(
            task_id='log_existing_valuefor_suspend_assignment_category_122',
            python_callable=lambda: get_custom_value(
                "Suspend Assignment Category")
        )

        if_request_suspendassignmentcategory_present_123 = rail.IfOperator(
            task_id='if_request_suspendassignmentcategory_present_123',
            test='''{{ dag_run.conf.SuspendAssignmentCategory | is_truthy and dag_run.conf.SuspendAssignmentCategory | lower != result('log_existing_valuefor_suspend_assignment_category_122') }}''',
            yes_task="log_urifor_suspend_assignment_category_124",
            no_task="if_request_supervisorssoid_present_129",
        )

        log_urifor_suspend_assignment_category_124 = rail.PythonOperator(
            task_id='log_urifor_suspend_assignment_category_124',
            python_callable=lambda: get_custom_uri(
                "Suspend Assignment Category")
        )

        get_all_custom_field_drop_down_options_125 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_125',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_urifor_suspend_assignment_category_124') }}"
            }
        )

        log_uriforsuspendassignmentcategory_126 = rail.PythonOperator(
            task_id='log_uriforsuspendassignmentcategory_126',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_custom_field_drop_down_options_125'), 'displayText', dag_run.conf['SuspendAssignmentCategory'], 'uri')
        )

        update_dropdown_value_customfield_73 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_customfield_73',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_urifor_suspend_assignment_category_124') }}",
                "customFieldDropDownOptionUri": "{{ result('log_uriforsuspendassignmentcategory_126') }}"
            }
        )

        insert_to_list_128 = rail.SetVariableOperator(
            task_id='insert_to_list_128',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Suspend Assignment Category updated"
            }
        )

        if_request_supervisorssoid_present_129 = rail.IfOperator(
            task_id='if_request_supervisorssoid_present_129',
            test='''{{ dag_run.conf.SupervisorSSOID | is_truthy }}''',
            yes_task="if_request_ohrid_not_equals_to_requestrequestsupervisorssoid_104",
            no_task="log_existing_valuefor_timesheet_template_171",
        )

        if_request_ohrid_not_equals_to_requestrequestsupervisorssoid_104 = rail.IfOperator(
            task_id='if_request_ohrid_not_equals_to_requestrequestsupervisorssoid_104',
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
            no_task="if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146",
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
            no_task="if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146",
        )

        def get_permission_type(permission_uri):
            permissionset = rail.find_first_by_attr_and_get_attr(rail.result(
                'get_assigned_permission_sets_for_user2_124'), 'policyUri', permission_uri, 'permissionSet')
            return permissionset['name'] if permissionset else None

        get_assigned_permission_sets_for_user2_124 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_124',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_120').useruri }}"
            }
        )

        log_checkif_manager_permissionsetisassigned_125 = rail.PythonOperator(
            task_id='log_checkif_manager_permissionsetisassigned_125',
            python_callable=lambda: get_permission_type(
                'urn:replicon:policy:supervision')
        )

        log_checkif_end_user_manager_permissionsetisassigned_126 = rail.PythonOperator(
            task_id='log_checkif_end_user_manager_permissionsetisassigned_126',
            python_callable=lambda: get_permission_type(
                'urn:replicon:policy:user')
        )

        def get_super_user_permissions(dag_run, entity_type_1, entity_type_2):
            emp_types = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity']
                and x['legacy_payroll_id'] == dag_run.conf['LegacyPayrollID']
                and x['type'] == entity_type_1
                and x['supervisor'] == entity_type_2, netherlands_master_mapper))
            return [emp_type['value'] for emp_type in emp_types] if emp_types else []

        log_required_supervisor_permission_127 = rail.PythonOperator(
            task_id='log_required_supervisor_permission_127',
            python_callable=lambda dag_run: get_super_user_permissions(
                dag_run, 'Permission', 'Supervisor')
        )

        def is_valid_permission():
            if rail.result('log_checkif_manager_permissionsetisassigned_125') is None or rail.result('log_checkif_end_user_manager_permissionsetisassigned_126') is None \
                    or rail.result('log_checkif_manager_permissionsetisassigned_125') not in rail.result('log_required_supervisor_permission_127') \
                    or rail.result('log_checkif_end_user_manager_permissionsetisassigned_126') not in rail.result('log_required_supervisor_permission_127'):
                return True
            return False

        if_log_checkif_manager_permissionsetisassigned_125_contains_approver_127 = rail.IfOperator(
            task_id='if_log_checkif_manager_permissionsetisassigned_125_contains_approver_127',
            test=is_valid_permission,
            yes_task="get_all_permission_sets_134",
            no_task="update_supervisor_assignment_schedule_over_date_range_144",
        )

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

        def get_super_permissions(response, dag_run):
            permissions_to_add = []
            mapper_permissions = get_super_user_permissions(
                dag_run, 'Permission', 'Supervisor')
            if response and mapper_permissions:
                for permission in mapper_permissions:
                    permission_uri = rail.find_first_by_attr_and_get_attr(
                        response, 'name', permission, 'uri')
                    if permission_uri:
                        permissions_to_add.append(permission_uri)
            return permissions_to_add

        get_all_permission_sets_134 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_134',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            log_response=True,
            data_handler=get_super_permissions
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_all_permission_sets_134') | length > 0 }}",
            yes_task='add_supervisor_permissions',
            no_task='update_supervisor_assignment_schedule_over_date_range_144'
        )

        add_supervisor_permissions = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda: rail.result('get_all_permission_sets_134'),
            execution_timeout=timedelta(days=14),
            data={
                'userUri': "{{ result('search_users_120').useruri }}",
                'permissionSetUri': '{{ item }}'
            }
        )

        update_supervisor_assignment_schedule_over_date_range_144 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_144',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
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

        if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146 = rail.IfOperator(
            task_id='if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146',
            test='''{{ result('search_users_120') | is_truthy and result('search_users_120').status | matches('False') }}''',
            yes_task="ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_147",
            no_task="if_log_5_blank_false_supervisorprofileisdisableddonotupdatesupervisor_148",
        )

        ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_147 = rail.WriteLogOperator(
            task_id='ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_147',
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

        if_log_5_blank_false_supervisorprofileisdisableddonotupdatesupervisor_148 = rail.IfOperator(
            task_id='if_log_5_blank_false_supervisorprofileisdisableddonotupdatesupervisor_148',
            test='''{{ result('search_users_120') | is_falsy }}''',
            yes_task="ge_supervisor_assignment_table_ey_user_import_logs_add_entry_210_210_149",
            no_task="if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150",
        )

        ge_supervisor_assignment_table_ey_user_import_logs_add_entry_210_210_149 = rail.WriteLogOperator(
            task_id='ge_supervisor_assignment_table_ey_user_import_logs_add_entry_210_210_149',
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

        if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150 = rail.IfOperator(
            task_id='if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150',
            test='''{{ dag_run.conf.OHRID == dag_run.conf.SupervisorSSOID }}''',
            yes_task="insert_to_list_170",
            no_task="log_existing_valuefor_timesheet_template_171",
        )

        dummy_operator_2 = rail.EmptyOperator(
            task_id="dummy_operator_2"
        )

        update_variable_167 = rail.SetVariableOperator(
            task_id='update_variable_167',
            append=False,
            name='{{ result("declare_variable_4").name }}',
            value="yes"
        )

        update_variable_168 = rail.SetVariableOperator(
            task_id='update_variable_168',
            append=False,
            name='{{ result("declare_variable_5").name }}',
            value="yes"
        )

        insert_to_list_170 = rail.SetVariableOperator(
            task_id='insert_to_list_170',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": "Supervisor not assigned/updated since the user and supervisor SSO ID are same"
            }
        )

        log_existing_valuefor_timesheet_template_171 = rail.PythonOperator(
            task_id='log_existing_valuefor_timesheet_template_171',
            python_callable=lambda:  rail.result('bulk_get_users3_6')[
                0]['timesheetTemplate']['name'] if rail.result('bulk_get_users3_6')[
                0]['timesheetTemplate'] else None
        )

        log_existing_valuefor_timesheet_template_uri_172 = rail.PythonOperator(
            task_id='log_existing_valuefor_timesheet_template_uri_172',
            python_callable=lambda:  rail.result('bulk_get_users3_6')[
                0]['timesheetTemplate']['uri'] if rail.result('bulk_get_users3_6')[
                0]['timesheetTemplate'] else None
        )

        log_existing_valuefor_overtime_eligibility_173 = rail.PythonOperator(
            task_id='log_existing_valuefor_overtime_eligibility_173',
            python_callable=lambda: get_custom_value("Overtime Eligibility")
        )

        log_urifor_overtime_eligibility_174 = rail.PythonOperator(
            task_id='log_urifor_overtime_eligibility_174',
            python_callable=lambda: get_custom_uri(
                "Suspend Assignment Category")
        )

        if_request_overtimeeligibility_not_equals_to_overtime_eligibility_175 = rail.IfOperator(
            task_id='if_request_overtimeeligibility_not_equals_to_overtime_eligibility_175',
            test='''{{ dag_run.conf.OvertimeEligibility != result('log_existing_valuefor_overtime_eligibility_173') }}''',
            yes_task="if_request_overtimeeligibility_present_176",
            no_task="if_declare_variable_4_value_equals_to_yes_200",
        )

        if_request_overtimeeligibility_present_176 = rail.IfOperator(
            task_id='if_request_overtimeeligibility_present_176',
            test='''{{ dag_run.conf.OvertimeEligibility | is_truthy }}''',
            yes_task="update_variable_177",
            no_task="update_variable_194",
        )

        update_variable_177 = rail.SetVariableOperator(
            task_id='update_variable_177',
            append=False,
            name='{{ result("declare_variable_5").name }}',
            value="yes"
        )

        get_all_custom_field_drop_down_options_178 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_178',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_urifor_overtime_eligibility_174') }}"
            }
        )

        log_urifor_overtime_eligibilityoption_179 = rail.PythonOperator(
            task_id='log_urifor_overtime_eligibilityoption_179',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_custom_field_drop_down_options_178'), 'displayText', dag_run.conf['OvertimeEligibility'], 'uri')
        )

        if_log_urifor_overtime_eligibilityoption_179_present_180 = rail.IfOperator(
            task_id='if_log_urifor_overtime_eligibilityoption_179_present_180',
            test='''{{ result('log_urifor_overtime_eligibilityoption_179') | is_truthy }}''',
            yes_task="update_dropdown_value_customfield_181",
            no_task="insert_to_list_184",
        )

        update_dropdown_value_customfield_181 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_customfield_181',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_urifor_overtime_eligibility_174') }}",
                "customFieldDropDownOptionUri": "{{ result('log_urifor_overtime_eligibilityoption_179') }}"
            }
        )

        insert_to_list_182 = rail.SetVariableOperator(
            task_id='insert_to_list_182',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": '''Overtime Eligibility updated to "{{ dag_run.conf.OvertimeEligibility }}'''
            }
        )

        insert_to_list_184 = rail.SetVariableOperator(
            task_id='insert_to_list_184',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": '''Overtime eligibility option "{{ dag_run.conf.OvertimeEligibility }}" not found in Replicon'''
            }
        )

        if_overtimeeligibility_downcase_contains_no_185 = rail.IfOperator(
            task_id='if_overtimeeligibility_downcase_contains_no_185',
            test='''{{ dag_run.conf.OvertimeEligibility | lower | matches('no') }}''',
            yes_task="update_variable_186",
            no_task="update_variable_192",
        )

        update_variable_186 = rail.SetVariableOperator(
            task_id='update_variable_186',
            append=False,
            name='{{ result("declare_variable_5").name }}',
            value="no"
        )

        update_variable_187 = rail.SetVariableOperator(
            task_id='update_variable_187',
            append=False,
            name='{{ result("declare_variable_4").name }}',
            value="no"
        )

        if_log_existing_valuefor_timesheet_template_uri_172_present_188 = rail.IfOperator(
            task_id='if_log_existing_valuefor_timesheet_template_uri_172_present_188',
            test='''{{ result('log_existing_valuefor_timesheet_template_uri_172') | is_truthy }}''',
            yes_task="remove_timesheet_template_189",
            no_task="if_declare_variable_4_value_equals_to_yes_200",
        )

        remove_timesheet_template_189 = rail.RepliconServiceOperator(
            task_id='remove_timesheet_template_189',
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('log_existing_valuefor_timesheet_template_uri_172') }}"
            }
        )

        insert_to_list_190 = rail.SetVariableOperator(
            task_id='insert_to_list_190',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": '''Removed Timesheet template "{{ result('log_existing_valuefor_timesheet_template_171') }}" as Overtime Eligibility was "{{ dag_run.conf.OvertimeEligibility }}'''
            }
        )

        update_variable_192 = rail.SetVariableOperator(
            task_id='update_variable_192',
            append=False,
            name='{{ result("declare_variable_4").name }}',
            value="yes"
        )

        update_variable_194 = rail.SetVariableOperator(
            task_id='update_variable_194',
            append=False,
            name='{{ result("declare_variable_5").name }}',
            value="no"
        )

        update_dropdown_value_customfield_195 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_customfield_195',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                    "objectUri": "{{ dag_run.conf.useruri }}",
                    "customFieldUri": "{{ result('log_urifor_overtime_eligibility_174') }}",
                    "customFieldDropDownOptionUri": null
            }
        )

        if_log_existing_valuefor_timesheet_template_uri_172_present_196 = rail.IfOperator(
            task_id='if_log_existing_valuefor_timesheet_template_uri_172_present_196',
            test='''{{ result('log_existing_valuefor_timesheet_template_uri_172') | is_truthy }}''',
            yes_task="remove_timesheet_template_197",
            no_task="if_declare_variable_4_value_equals_to_yes_200",
        )

        remove_timesheet_template_197 = rail.RepliconServiceOperator(
            task_id='remove_timesheet_template_197',
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('log_existing_valuefor_timesheet_template_uri_172') }}"
            }
        )

        insert_to_list_198 = rail.SetVariableOperator(
            task_id='insert_to_list_198',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": '''Removed Timesheet template "{{ result('log_existing_valuefor_timesheet_template_171') }}" as Overtime Eligibility was blank'''
            }
        )

        update_variable_199 = rail.SetVariableOperator(
            task_id='update_variable_199',
            append=False,
            name='{{ result("declare_variable_4").name }}',
            value="no"
        )

        def timesheet_validation(dag_run):
            timsheettemplate = rail.get_dag_run_var(
                'Update Timesheet Template?')
            overtimeeligibility = dag_run.conf['OvertimeEligibility']
            if timsheettemplate == 'yes' and overtimeeligibility and overtimeeligibility.lower() != 'no':
                return True
            return False

        if_declare_variable_4_value_equals_to_yes_200 = rail.IfOperator(
            task_id='if_declare_variable_4_value_equals_to_yes_200',
            test=timesheet_validation,
            # '''{{ result('declare_variable_4').value == 'yes'  and dag_run.conf.OvertimeEligibility | is_truthy and dag_run.conf.OvertimeEligibility | lower !='no' }}''',
            yes_task="trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0201",
            no_task="if_declare_variable_5_value_equals_to_yes_202",
        )

        trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0201 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0201',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_user_sync_netherlands_payrule_assignment_add_update_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "useruri": dag_run.conf['useruri'],
                "EmployeeLastName": dag_run.conf['LastName'],
                "EmployeeFirstName": dag_run.conf['FirstName'],
                "EmployeeEmailAddress": dag_run.conf['Email'],
                "OHRID": dag_run.conf['OHRID'],
                "LegalEntityHireDate": dag_run.conf['LegalEntityHireDate'],
                "LegacyPayrollID": dag_run.conf['LegacyPayrollID'],
                "JobPositionTitle": dag_run.conf['JobPositionTitle'],
                "SupervisorSSOID": dag_run.conf['SupervisorSSOID'],
                "SupervisorName": dag_run.conf['SupervisorName'],
                "LegalEntity": dag_run.conf['LegalEntity'],
                "OvertimeEligibility": dag_run.conf['OvertimeEligibility'],
                "HealthcareProductLineEIT": dag_run.conf['HealthcareProductLineEIT'],
                "jobtype": dag_run.conf['JobType'],
                "AssignmentEffectiveDate": dag_run.conf['AssignmentEffectiveDate'],
                "type": "timesheet",
                "action": "update",
                "currenttimesheettemplate": rail.result('log_existing_valuefor_timesheet_template_171'),
                "currentpayrulename": "",
                "userstartdate": rail.render_template(
                    "{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.day }}/{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.month }}/{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.year }}")
            }
        )

        wait_for_completion_trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0201 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0201',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0201") }}'
        )

        def payrule_validation(dag_run):
            payruleupdate = rail.get_dag_run_var(
                'Update Payrule?')
            overtimeeligibility = dag_run.conf['OvertimeEligibility']
            if payruleupdate == 'yes' and overtimeeligibility and overtimeeligibility.lower() != 'no':
                return True
            return False

        if_declare_variable_5_value_equals_to_yes_202 = rail.IfOperator(
            task_id='if_declare_variable_5_value_equals_to_yes_202',
            test=payrule_validation,
            # '''{{ result('declare_variable_5').value == 'yes' and dag_run.conf.OvertimeEligibility | is_truthy and dag_run.conf.OvertimeEligibility | lower !='no' }}''',
            yes_task="trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0203",
            no_task="invoke_custom_ruby_code_204",
        )

        trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0203 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0203',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_user_sync_netherlands_payrule_assignment_add_update_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "useruri": dag_run.conf['useruri'],
                "EmployeeLastName": dag_run.conf['LastName'],
                "EmployeeFirstName": dag_run.conf['FirstName'],
                "EmployeeEmailAddress": dag_run.conf['Email'],
                "OHRID": dag_run.conf['OHRID'],
                "LegalEntityHireDate": dag_run.conf['LegalEntityHireDate'],
                "LegacyPayrollID": dag_run.conf['LegacyPayrollID'],
                "JobPositionTitle": dag_run.conf['JobPositionTitle'],
                "SupervisorSSOID": dag_run.conf['SupervisorSSOID'],
                "SupervisorName": dag_run.conf['SupervisorName'],
                "LegalEntity": dag_run.conf['LegalEntity'],
                "OvertimeEligibility": dag_run.conf['OvertimeEligibility'],
                "HealthcareProductLineEIT": dag_run.conf['HealthcareProductLineEIT'],
                "jobtype": dag_run.conf['JobType'],
                "AssignmentEffectiveDate": dag_run.conf['AssignmentEffectiveDate'],
                "type": "timesheet",
                "action": "update",
                "currenttimesheettemplate": "",
                "currentpayrulename": "",
                "userstartdate": rail.render_template(
                    "{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.day }}/{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.month }}/{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.startDate.year }}")
            }
        )

        wait_for_completion_trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0203 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0203',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0203") }}'
        )

        invoke_custom_ruby_code_204 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_204',
            python_callable=lambda dag_run: dag_run.conf['HireEffectiveDate'] or pendulum.now(
                config.pacific_timezone).strftime('%d/%m/%Y')
        )

        # pylint: disable=too-many-boolean-expressions
        def get_schedule_name_based_on_input(dag_run):
            schedule_name_based_on_input = ""
            if dag_run.conf['DWSMonday'] and dag_run.conf['DWSTuesday']\
                and dag_run.conf['DWSWednesday'] and dag_run.conf['DWSThursday'] \
                    and dag_run.conf['DWSFriday'] and dag_run.conf['DWSSaturday'] \
            or dag_run.conf['DWSSunday']:
                schedule_name_based_on_input = dag_run.conf['DWSMonday'] + "|" + dag_run.conf['DWSTuesday'] + "|" + dag_run.conf['DWSWednesday'] + "|" + \
                    dag_run.conf['DWSThursday'] + "|" + dag_run.conf['DWSFriday'] + \
                    "|" + dag_run.conf['DWSSaturday'] + \
                    "|" + dag_run.conf['DWSSunday']
            return schedule_name_based_on_input

        log_office_schedulename_205 = rail.PythonOperator(
            task_id='log_office_schedulename_205',
            python_callable=get_schedule_name_based_on_input
        )

        # pylint: disable=too-many-boolean-expressions
        def get_schedule_name(dag_run):
            present_empty_schedule = False
            present_zero_hours_schedule = False
            if dag_run.conf['DWSMonday'] == "0" and dag_run.conf['DWSTuesday'] == "0" \
                and dag_run.conf['DWSWednesday'] == "0" and dag_run.conf['DWSThursday'] == "0" \
                    and dag_run.conf['DWSFriday'] == "0" and dag_run.conf['DWSSaturday'] == "0" \
            and dag_run.conf['DWSSunday'] == "0":
                present_zero_hours_schedule = True
            if dag_run.conf['DWSMonday'] is None or dag_run.conf['DWSTuesday'] is None \
                or dag_run.conf['DWSWednesday'] is None or dag_run.conf['DWSThursday'] is None \
                    or dag_run.conf['DWSFriday'] is None or dag_run.conf['DWSSaturday'] is None \
            or dag_run.conf['DWSSunday'] is None:
                present_empty_schedule = True
            if present_zero_hours_schedule or present_empty_schedule:
                return False
            return True

        if_log_no_changein_schedule_226_blank_229 = rail.IfOperator(
            task_id='if_log_no_changein_schedule_226_blank_229',
            test=get_schedule_name,
            yes_task="log_office_scedules_236",
            no_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208",
        )

        def get_office_schedules(dag_run):
            derived_office_schedules = []
            derived_office_schedules_request = []
            office_schedule_policies = rail.result('bulk_get_users3_6')[
                0]['schedulePolicies']
            for office_schedule_policy in office_schedule_policies:
                if office_schedule_policy['effectiveDate']:
                    effective_date = get_datetime_obj(
                        office_schedule_policy['effectiveDate'])
                    current_date = datetime.strptime(
                        dag_run.conf['DWSStartDate'], '%d/%m/%Y') if dag_run.conf['DWSStartDate'] else pendulum.now(config.pacific_timezone)
                    next_day = current_date + timedelta(days=1)
                    if effective_date.date() < next_day.date():
                        derived_office_schedules.append({
                            "uri": office_schedule_policy['officeSchedule']['uri'],
                            "name": office_schedule_policy['officeSchedule']['displayText'],
                            "date": effective_date.strftime('%d/%m/%Y')
                        })
                    if effective_date.date() != current_date.date():
                        derived_office_schedules_request.append({
                            "schedulePolicy": {
                                "officeScheduleUri": office_schedule_policy['officeSchedule']['uri'],
                                "name": null,
                                "officeSchedule": {
                                    "officeScheduleUri": office_schedule_policy['officeSchedule']['uri'],
                                    "name": null
                                },
                                "scheduleTypeUri": office_schedule_policy["scheduleTypeUri"]
                            },
                            "effectiveDate": {
                                "year": effective_date.year,
                                "month": effective_date.month,
                                "day": effective_date.day,
                            },
                        })
                else:
                    employment_start_date = get_datetime_obj(rail.result('bulk_get_users3_6')[
                        0]['userDetails']['employmentDateRange']['startDate'])
                    derived_office_schedules.append({
                        "uri": office_schedule_policy['officeSchedule']['uri'],
                        "name": office_schedule_policy['officeSchedule']['displayText'],
                        "date": employment_start_date.strftime('%d/%m/%Y')
                    })

                    derived_office_schedules_request.append({
                        "schedulePolicy": {
                            "officeScheduleUri": office_schedule_policy['officeSchedule']['uri'],
                            "name": null,
                            "officeSchedule": {
                                "officeScheduleUri": office_schedule_policy['officeSchedule']['uri'],
                                "name": null
                            },
                            "scheduleTypeUri": office_schedule_policy["scheduleTypeUri"]
                        },
                        "effectiveDate": None
                    })

            return {
                "office_schedules": derived_office_schedules,
                "office_schedules_request": derived_office_schedules_request
            }

        log_office_scedules_236 = rail.PythonOperator(
            task_id='log_office_scedules_236',
            python_callable=get_office_schedules
        )

        if_first_uri_present_246 = rail.IfOperator(
            task_id='if_first_uri_present_246',
            test='''{{ result('log_office_scedules_236').office_schedules_request | length > 0 }}''',
            yes_task="log_max_effectivedate_247",
            no_task="if_log_currentschedulename_248_blank_258",
        )

        log_max_effectivedate_247 = rail.PythonOperator(
            task_id='log_max_effectivedate_247',
            python_callable=lambda: (max(
                datetime.strptime(x['date'], '%d/%m/%Y') for x in rail.result('log_office_scedules_236')['office_schedules'])).strftime('%d/%m/%Y')
            if rail.result('log_office_scedules_236')['office_schedules'] else None
        )

        log_currentschedulename_248 = rail.PythonOperator(
            task_id='log_currentschedulename_248',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'log_office_scedules_236')['office_schedules'], 'date', rail.result('log_max_effectivedate_247'), 'name', "")
        )

        log_currentscheduleuri_249 = rail.PythonOperator(
            task_id='log_currentscheduleuri_249',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'log_office_scedules_236')['office_schedules'], 'date', rail.result('log_max_effectivedate_247'), 'uri', "")
        )

        if_log_currentschedulenuri_blank_250 = rail.IfOperator(
            task_id='if_log_currentschedulenuri_blank_250',
            test='''{{ result('log_currentscheduleuri_249') | is_truthy}}''',
            yes_task="get_office_schedule_details_251",
            no_task="if_log_currentschedulename_248_blank_258",
        )

        get_office_schedule_details_251 = rail.RepliconServiceOperator(
            task_id='get_office_schedule_details_251',
            endpoint="/services/OfficeScheduleService1.svc/GetOfficeScheduleDetails",
            data={
                "officeScheduleUri": "{{ result('log_currentscheduleuri_249') }}"
            }
        )

        def get_number_of_working_day():
            sch_recurring_pattern = rail.result(
                'get_office_schedule_details_251')
            recurringPattern_arr = sch_recurring_pattern['recurringPattern'][
                'patternEntries'] if sch_recurring_pattern and sch_recurring_pattern['recurringPattern'] else []
            return recurringPattern_arr[-1]['patternDay'] if recurringPattern_arr else 0

        log_numberofdaysinschedule_252 = rail.PythonOperator(
            task_id='log_numberofdaysinschedule_252',
            python_callable=get_number_of_working_day
        )

        def get_number_of_working_hours():
            number_of_working_hours = 0.0
            sch_recurring_pattern = rail.result(
                'get_office_schedule_details_251')
            recurringPattern_arr = sch_recurring_pattern['recurringPattern'][
                'patternEntries'] if sch_recurring_pattern and sch_recurring_pattern['recurringPattern'] else []
            for recurringPattern in recurringPattern_arr:
                if recurringPattern['workDuration'] and (recurringPattern['workDuration']['hours'] > 0 or recurringPattern['workDuration']['minutes'] > 0 or recurringPattern['workDuration']['seconds']):
                    hrs = float(recurringPattern['workDuration']['hours'])
                    mins = float(
                        float(recurringPattern['workDuration']['minutes']) / 60.00)
                    totalhours = hrs + mins
                    number_of_working_hours += totalhours
            return number_of_working_hours

        get_numberof_working_hours_253 = rail.PythonOperator(
            task_id='get_numberof_working_hours_253',
            python_callable=get_number_of_working_hours
        )

        if_log_currentschedulename_248_blank_258 = rail.IfOperator(
            task_id='if_log_currentschedulename_248_blank_258',
            test='''{{ result('log_currentschedulename_248') | is_falsy  or result('log_currentschedulename_248') | lower != result('log_office_schedulename_205') | lower }}''',
            yes_task="if_workingday_less_than_8_current_scheduleisnotgreaterthan7days_259",
            no_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208",
        )

        if_workingday_less_than_8_current_scheduleisnotgreaterthan7days_259 = rail.IfOperator(
            task_id='if_workingday_less_than_8_current_scheduleisnotgreaterthan7days_259',
            test='''{{ result('log_numberofdaysinschedule_252') | is_falsy or result('log_numberofdaysinschedule_252') < 8 }}''',
            yes_task="get_all_office_schedules_260",
            no_task="check_timeoff_assignment_triggered_273",
        )

        get_all_office_schedules_260 = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules_260',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        log_gettherequiredofficeschedule_uri_261 = rail.PythonOperator(
            task_id='log_gettherequiredofficeschedule_uri_261',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_office_schedules_260'), 'displayText', rail.result('log_office_schedulename_205'), 'uri', '')
        )

        if_pluckuri_first_present_262 = rail.IfOperator(
            task_id='if_pluckuri_first_present_262',
            test='''{{ result('log_gettherequiredofficeschedule_uri_261') | is_truthy }}''',
            yes_task="log_office_schedule_252",
            no_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208",
        )

        def get_office_schedule_request(dag_run):
            existing_schedules = rail.result('log_office_scedules_236')[
                'office_schedules_request']
            effective_date = datetime.strptime(
                dag_run.conf['DWSStartDate'], '%d/%m/%Y') if dag_run.conf['DWSStartDate'] else pendulum.now(config.pacific_timezone)
            existing_schedules.append({
                "schedulePolicy": {
                    "officeScheduleUri": rail.result('log_gettherequiredofficeschedule_uri_261'),
                    "name": null,
                    "officeSchedule": {
                        "officeScheduleUri": rail.result('log_gettherequiredofficeschedule_uri_261'),
                        "name": null
                    },
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                },
                "effectiveDate": {
                    "year": effective_date.year,
                    "month": effective_date.month,
                    "day": effective_date.day,
                }
            })

            return existing_schedules

        log_office_schedule_252 = rail.PythonOperator(
            task_id='log_office_schedule_252',
            python_callable=get_office_schedule_request
        )

        put_schedule_policy_schedule_for_user_253 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user_253',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('log_office_schedule_252')
            }
        )

        insert_to_list_254 = rail.SetVariableOperator(
            task_id='insert_to_list_254',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Office schedule updated"
            }
        )

        # pylint: disable=too-many-boolean-expressions
        def get_new_numberof_working_hours(dag_run):
            numberofworkinghour = 0
            schedule_info = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Default Schedule", netherlands_master_mapper))
            mapper_schedule_name = schedule_info[0]['value'] if schedule_info else None
            schedule_to_assign = rail.result('log_office_schedulename_205')
            if mapper_schedule_name == schedule_to_assign:
                numberofworkinghour = 40
            else:
                if dag_run.conf['DWSMonday'] and float(dag_run.conf['DWSMonday']) > 0:
                    numberofworkinghour += float(dag_run.conf['DWSMonday'])
                if dag_run.conf['DWSTuesday'] and float(dag_run.conf['DWSTuesday']) > 0:
                    numberofworkinghour += float(dag_run.conf['DWSTuesday'])
                if dag_run.conf['DWSWednesday'] and float(dag_run.conf['DWSWednesday']) > 0:
                    numberofworkinghour += float(dag_run.conf['DWSWednesday'])
                if dag_run.conf['DWSThursday'] and float(dag_run.conf['DWSThursday']) > 0:
                    numberofworkinghour += float(dag_run.conf['DWSThursday'])
                if dag_run.conf['DWSFriday'] and float(dag_run.conf['DWSFriday']) > 0:
                    numberofworkinghour += float(dag_run.conf['DWSFriday'])
                if dag_run.conf['DWSSaturday'] and float(dag_run.conf['DWSSaturday']) > 0:
                    numberofworkinghour += float(dag_run.conf['DWSSaturday'])
                if dag_run.conf['DWSSunday'] and float(dag_run.conf['DWSSunday']) > 0:
                    numberofworkinghour += float(dag_run.conf['DWSSaturday'])
            return numberofworkinghour

        def is_schedule_name_present(dag_run):
            existing_numberoff_days = rail.result('get_numberof_working_hours_253') if rail.result(
                'get_numberof_working_hours_253') else 0
            new_numberof_days = get_new_numberof_working_hours(dag_run)
            return bool(existing_numberoff_days != new_numberof_days)

        if_numberof_days_comparison_267 = rail.IfOperator(
            task_id='if_numberof_days_comparison_267',
            test=is_schedule_name_present,
            yes_task="trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_0284",
            no_task="check_timeoff_assignment_triggered_273",
        )

        trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_0284 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_0284',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_netherlands_child_workflow_to_add_timeoff_type_for_update_user_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['OHRID'],
                "useruri": dag_run.conf['useruri'],
                "legalentity": dag_run.conf['LegalEntity'],
                "startdate": pendulum.now(config.pacific_timezone).strftime('%d/%m/%Y'),
                "type": "Job Type Update" if rail.result('log_job_type_changedand_timeoffneedstobeupdated_106') else "Update",
                "fullpart": "Full Time" if get_new_numberof_working_hours(dag_run) > 39 else "Part Time",
                "legacypayrollid": dag_run.conf['LegacyPayrollID'],
                "jobtype": dag_run.conf['JobType'],
                "payrule": dag_run.conf['Payroll'],
                "scheduledweeklyhours": 40 if get_new_numberof_working_hours(dag_run) > 40 else get_new_numberof_working_hours(dag_run)
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_0284 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_0284',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_0284") }}'
        )

        to_assignment_changes_269 = rail.PythonOperator(
            task_id='to_assignment_changes_269',
            python_callable=lambda:  "Timeoff Assignment Done"
        )

        check_timeoff_assignment_triggered_273 = rail.IfOperator(
            task_id='check_timeoff_assignment_triggered_273',
            test='''{{ result('log_job_type_changedand_timeoffneedstobeupdated_106') | is_truthy and result('to_assignment_changes_269') | is_falsy }}''',
            yes_task="trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_274",
            no_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208",
        )

        trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_274 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_274',
            retries=0,
            items=[-1],
            trigger_dag_id=f'gehealthcare_netherlands_child_workflow_to_add_timeoff_type_for_update_user_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['OHRID'],
                "useruri": dag_run.conf['useruri'],
                "legalentity": dag_run.conf['LegalEntity'],
                "startdate": pendulum.now(config.pacific_timezone).strftime('%d/%m/%Y'),
                "type": "Job Type Update",
                "fullpart": "Full Time" if get_new_numberof_working_hours(dag_run) > 39 else "Part Time",
                "legacypayrollid": dag_run.conf['LegacyPayrollID'],
                "jobtype": dag_run.conf['JobType'],
                "payrule": dag_run.conf['Payroll'],
                "scheduledweeklyhours": 40 if get_new_numberof_working_hours(dag_run) > 40 else get_new_numberof_working_hours(dag_run)
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_274 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_274',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_274") }}'
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
        declare_list_2 >> declare_list_3 >> declare_variable_4 >> declare_variable_5 >> bulk_get_users3_6 >> log_startdate_7 >> \
            netherlands_master_mapper_search_entries_8 >> if_entry_col5_blank_8
        if_entry_col5_blank_8 >> rail.Label(
            'Yes') >> if_userdetails_isenabled_is_true_9
        if_entry_col5_blank_8 >> rail.Label(
            'No') >> dummy_operator_1 >> if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_16
        if_userdetails_isenabled_is_true_9 >> rail.Label(
            'Yes') >> disable_login_10 >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_11 >> \
            ey_user_import_logs_add_entry_210
        if_userdetails_isenabled_is_true_9 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_13
        if_userdetails_isenabled_is_not_true_13 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_14 >> ey_user_import_logs_add_entry_210
        if_userdetails_isenabled_is_not_true_13 >> rail.Label(
            'No') >> if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_16
        if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_16 >> rail.Label(
            'Yes') >> update_enddate_18 >> \
            ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_19 >> ey_user_import_logs_add_entry_210
        if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_16 >> rail.Label(
            'No') >> if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_21
        if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_21 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_22 >> \
            ey_user_import_logs_add_entry_210
        if_request_terminationeffectivedate_present_disableonlyupdatestheenddate_21 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_rehire_24
        if_userdetails_isenabled_is_not_true_rehire_24 >> rail.Label(
            'Yes') >> if_request_hireeffectivedate_blank_25
        if_request_hireeffectivedate_blank_25 >> rail.Label(
            'Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_27 >> \
            ey_user_import_logs_add_entry_210
        if_request_hireeffectivedate_blank_25 >> rail.Label(
            'No') >> if_enddate_year_present_29
        if_enddate_year_present_29 >> rail.Label(
            'Yes') >> log_enddate_30 >> updateloginname_31 >> trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_add_v1_0async_callrecipeforrehire_32 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_ge_netherlands_add_v1_0async_callrecipeforrehire_32 >> ey_user_import_logs_add_entry_210
        if_enddate_year_present_29 >> rail.Label(
            'No') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_34 >> ey_user_import_logs_add_entry_210
        if_userdetails_isenabled_is_not_true_rehire_24 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_true_transfer_36
        if_userdetails_isenabled_is_true_transfer_36 >> rail.Label(
            'No') >> dummy_operator_2 >> if_request_legalentity_present_changein_legal_entity_transfer_39
        if_userdetails_isenabled_is_true_transfer_36 >> rail.Label(
            'Yes') >> log_enddate_37 >> if_request_reverseterminationeffectivedate_present_38
        if_request_reverseterminationeffectivedate_present_38 >> rail.Label(
            'No') >> if_request_legalentity_present_changein_legal_entity_transfer_39
        if_request_reverseterminationeffectivedate_present_38 >> rail.Label(
            'Yes') >> if_userdetails_isenabled_is_not_true_39
        if_userdetails_isenabled_is_not_true_39 >> rail.Label(
            'No') >> remove_enddate_42
        if_userdetails_isenabled_is_not_true_39 >> rail.Label('Yes') >> enable_login_40 >> insert_to_list_41 >> remove_enddate_42 >> \
            insert_to_list_43 >> if_request_legalentity_present_changein_legal_entity_transfer_39
        if_request_legalentity_present_changein_legal_entity_transfer_39 >> rail.Label(
            'No') >> if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_44
        if_request_legalentity_present_changein_legal_entity_transfer_39 >> rail.Label(
            'Yes') >> if_request_hireeffectivedate_blank_40
        if_request_hireeffectivedate_blank_40 >> rail.Label('No') >> update_enddate_44 >> disable_login_45 >> updateloginname_46 >> \
            trigger_dag_run_ge_user_sync_netherlands_add_v1_0_47 >> wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_add_v1_0_47 >> \
            ey_user_import_logs_add_entry_210
        if_request_hireeffectivedate_blank_40 >> rail.Label('Yes') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_41 >> \
            ey_user_import_logs_add_entry_210
        if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_44 >> rail.Label('No') >> \
            if_lastname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestlastnamedowncase_47
        if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_44 >> rail.Label('Yes') >> \
            update_first_name_45 >> insert_to_list_46 >> if_lastname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestlastnamedowncase_47
        if_lastname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestlastnamedowncase_47 >> rail.Label('No') >> \
            if_request_email_present_50
        if_lastname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestlastnamedowncase_47 >> rail.Label('Yes') >> \
            update_last_name_48 >> insert_to_list_49 >> if_request_email_present_50
        if_request_email_present_50 >> rail.Label('No') >> insert_to_list_52
        if_request_email_present_50 >> rail.Label('Yes') >> update_email_51 >> insert_to_list_52 >> \
            log_valuefor_job_position_title_52 >> log_urifor_job_position_title_53 >> if_request_jobpositiontitle_present_54
        if_request_jobpositiontitle_present_54 >> rail.Label(
            'No') >> log_valuefor_h_r_m_s_s_o_i_d_58
        if_request_jobpositiontitle_present_54 >> rail.Label('Yes') >> update_text_value_customfield_56 >> insert_to_list_57 >> \
            log_valuefor_h_r_m_s_s_o_i_d_58 >> log_urifor_h_r_m_s_s_o_i_d_58 >> if_request_hrmssoid_present_59
        if_request_hrmssoid_present_59 >> rail.Label(
            'No') >> log_valuefor_h_r_m_name_63
        if_request_hrmssoid_present_59 >> rail.Label('Yes') >> update_text_value_customfield_61 >> \
            insert_to_list_62 >> log_valuefor_h_r_m_name_63 >> log_urifor_h_r_m_name_63 >> if_request_hrmname_present_64
        if_request_hrmname_present_64 >> rail.Label(
            'Yes') >> update_text_value_customfield_66 >> insert_to_list_67 >> log_existing_valuefor_payroll_81 >> if_request_payroll_present_82
        if_request_payroll_present_82 >> rail.Label('Yes') >> log_urifor_payroll_83 >> update_text_value_customfield_84 >> insert_to_list_85 >> \
            log_existing_valuefor_healthcare_product_lineeit_86 >> if_request_healthcareproductlineeit_present_87
        if_request_payroll_present_82 >> rail.Label(
            'No') >> log_existing_valuefor_healthcare_product_lineeit_86
        if_request_healthcareproductlineeit_present_87 >> rail.Label('Yes') >> log_urifor_healthcare_product_lineeit_88 >> update_text_value_customfield_89 >> \
            insert_to_list_90 >> log_existing_valuefor_job_type_93
        if_request_healthcareproductlineeit_present_87 >> rail.Label(
            'No') >> log_existing_valuefor_job_type_93 >> if_request_jobtype_present_94
        if_request_jobtype_present_94 >> rail.Label('Yes') >> log_urifor_job_type_95 >> update_text_value_customfield_96 >> insert_to_list_97 >> \
            update_variable_98 >> update_variable_99 >> if_log_existing_valuefor_job_type_93_present_100
        if_log_existing_valuefor_job_type_93_present_100 >> rail.Label(
            'No') >> log_existing_valuefor_career_band_107
        if_log_existing_valuefor_job_type_93_present_100 >> rail.Label('Yes') >> ge_netherlands_user_sync_master_mapper_search_entries_101 >> log_existing_timeoff_assignment_102 >> \
            ge_netherlands_user_sync_master_mapper_search_entries_103 >> log_new_timeoff_assignment_104 >> if_log_new_timeoff_assignment_104_not_equals_to_existing_timeoff_assignment_105
        if_log_new_timeoff_assignment_104_not_equals_to_existing_timeoff_assignment_105 >> rail.Label('Yes') >> log_job_type_changedand_timeoffneedstobeupdated_106 >> \
            log_existing_valuefor_career_band_107
        if_log_new_timeoff_assignment_104_not_equals_to_existing_timeoff_assignment_105 >> rail.Label(
            'No') >> log_existing_valuefor_career_band_107
        if_request_jobtype_present_94 >> rail.Label(
            'No') >> log_existing_valuefor_career_band_107 >> if_request_careerband_present_108
        if_request_careerband_present_108 >> rail.Label(
            'No') >> log_existing_valuefor_work_112
        if_request_careerband_present_108 >> rail.Label('Yes') >> log_urifor_career_band_109 >> update_text_value_customfield_110 >> insert_to_list_111 >> \
            log_existing_valuefor_work_112 >> if_request_work_present_113
        if_request_work_present_113 >> rail.Label('Yes') >> log_urifor_work_114 >> update_text_value_customfield_115 >> insert_to_list_116 >> \
            log_existing_valuefor_work_location_117
        if_request_work_present_113 >> rail.Label(
            'No') >> log_existing_valuefor_work_location_117 >> if_request_locationname_present_118
        if_request_locationname_present_118 >> rail.Label('Yes') >> log_urifor_work_location_119 >> update_text_value_customfield_120 >> insert_to_list_121 >> \
            log_existing_valuefor_suspend_assignment_category_122
        if_request_locationname_present_118 >> rail.Label(
            'No') >> log_existing_valuefor_suspend_assignment_category_122 >> if_request_suspendassignmentcategory_present_123
        if_request_suspendassignmentcategory_present_123 >> rail.Label(
            'No') >> if_request_supervisorssoid_present_129
        if_request_suspendassignmentcategory_present_123 >> rail.Label('Yes') >> log_urifor_suspend_assignment_category_124 >> get_all_custom_field_drop_down_options_125 >> \
            log_uriforsuspendassignmentcategory_126 >> update_dropdown_value_customfield_73 >> insert_to_list_128 >> if_request_supervisorssoid_present_129
        if_request_supervisorssoid_present_129 >> rail.Label(
            'No') >> log_existing_valuefor_timesheet_template_171
        if_request_supervisorssoid_present_129 >> rail.Label(
            'Yes') >> if_request_ohrid_not_equals_to_requestrequestsupervisorssoid_104
        if_request_hrmname_present_64 >> rail.Label(
            'No') >> if_request_ohrid_not_equals_to_requestrequestsupervisorssoid_104
        if_request_ohrid_not_equals_to_requestrequestsupervisorssoid_104 >> rail.Label(
            'Yes') >> log_supervisorschedule_106 >> if_first_uri_present_116
        if_first_uri_present_116 >> rail.Label(
            'Yes') >> log_max_effectivedate_117 >> log_currentsupervisorloginname_118 >> if_log_currentsupervisorloginname_118_blank_119
        if_first_uri_present_116 >> rail.Label(
            'No') >> if_log_currentsupervisorloginname_118_blank_119
        if_log_currentsupervisorloginname_118_blank_119 >> rail.Label(
            'Yes') >> search_users_120 >> if_log_5_present_123
        if_log_5_present_123 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user2_124 >> log_checkif_manager_permissionsetisassigned_125 >> \
            log_checkif_end_user_manager_permissionsetisassigned_126 >> log_required_supervisor_permission_127 >> \
            if_log_checkif_manager_permissionsetisassigned_125_contains_approver_127
        if_log_checkif_manager_permissionsetisassigned_125_contains_approver_127 >> rail.Label(
            'No') >> update_supervisor_assignment_schedule_over_date_range_144
        if_log_checkif_manager_permissionsetisassigned_125_contains_approver_127 >> rail.Label('Yes') >> \
            get_all_permission_sets_134 >> should_add_missing_permissions
        should_add_missing_permissions >> rail.Label(
            'Yes') >> update_supervisor_assignment_schedule_over_date_range_144
        should_add_missing_permissions >> rail.Label('No') >> add_supervisor_permissions >> \
            update_supervisor_assignment_schedule_over_date_range_144 >> insert_to_list_145 >> \
            update_variable_167 >> update_variable_168 >> if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146
        if_log_5_present_123 >> rail.Label(
            'No') >> if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146
        if_log_currentsupervisorloginname_118_blank_119 >> rail.Label(
            'No') >> if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146
        if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146 >> rail.Label(
            'Yes') >> ge_supervisor_assignment_table_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_147 >> \
            if_log_5_blank_false_supervisorprofileisdisableddonotupdatesupervisor_148
        if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146 >> rail.Label(
            'No') >> if_log_5_blank_false_supervisorprofileisdisableddonotupdatesupervisor_148
        if_log_5_blank_false_supervisorprofileisdisableddonotupdatesupervisor_148 >> rail.Label(
            'Yes') >> ge_supervisor_assignment_table_ey_user_import_logs_add_entry_210_210_149 >> \
            if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150
        if_log_5_blank_false_supervisorprofileisdisableddonotupdatesupervisor_148 >> rail.Label(
            'No') >> if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150
        if_request_ohrid_not_equals_to_requestrequestsupervisorssoid_104 >> rail.Label(
            'No') >> if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150
        if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150 >> rail.Label(
            'No') >> log_existing_valuefor_timesheet_template_171
        if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150 >> rail.Label(
            'Yes') >> insert_to_list_170 >> log_existing_valuefor_timesheet_template_171 >> \
            log_existing_valuefor_timesheet_template_uri_172 >> log_existing_valuefor_overtime_eligibility_173 >> \
            log_urifor_overtime_eligibility_174 >> if_request_overtimeeligibility_not_equals_to_overtime_eligibility_175
        if_request_overtimeeligibility_not_equals_to_overtime_eligibility_175 >> rail.Label(
            'Yes') >> if_request_overtimeeligibility_present_176
        if_request_overtimeeligibility_present_176 >> rail.Label(
            'Yes') >> update_variable_177 >> get_all_custom_field_drop_down_options_178 >> log_urifor_overtime_eligibilityoption_179 >> \
            if_log_urifor_overtime_eligibilityoption_179_present_180
        if_log_urifor_overtime_eligibilityoption_179_present_180 >> rail.Label(
            'No') >> insert_to_list_184 >> if_overtimeeligibility_downcase_contains_no_185
        if_log_urifor_overtime_eligibilityoption_179_present_180 >> rail.Label('Yes') >> update_dropdown_value_customfield_181 >> insert_to_list_182 >> \
            if_overtimeeligibility_downcase_contains_no_185
        if_overtimeeligibility_downcase_contains_no_185 >> rail.Label(
            'No') >> update_variable_192 >> if_declare_variable_4_value_equals_to_yes_200
        if_overtimeeligibility_downcase_contains_no_185 >> rail.Label(
            'Yes') >> update_variable_186 >> update_variable_187 >> if_log_existing_valuefor_timesheet_template_uri_172_present_188
        if_log_existing_valuefor_timesheet_template_uri_172_present_188 >> rail.Label(
            'No') >> if_declare_variable_4_value_equals_to_yes_200
        if_log_existing_valuefor_timesheet_template_uri_172_present_188 >> rail.Label(
            'Yes') >> remove_timesheet_template_189 >> insert_to_list_190 >> if_declare_variable_4_value_equals_to_yes_200
        if_request_overtimeeligibility_present_176 >> rail.Label('No') >> update_variable_194 >> update_dropdown_value_customfield_195 >> \
            if_log_existing_valuefor_timesheet_template_uri_172_present_196
        if_log_existing_valuefor_timesheet_template_uri_172_present_196 >> rail.Label(
            'No') >> if_declare_variable_4_value_equals_to_yes_200
        if_log_existing_valuefor_timesheet_template_uri_172_present_196 >> rail.Label('Yes') >> remove_timesheet_template_197 >> insert_to_list_198 >> \
            update_variable_199 >> if_declare_variable_4_value_equals_to_yes_200
        if_request_overtimeeligibility_not_equals_to_overtime_eligibility_175 >> rail.Label(
            'No') >> if_declare_variable_4_value_equals_to_yes_200
        if_declare_variable_4_value_equals_to_yes_200 >> rail.Label(
            'No') >> if_declare_variable_5_value_equals_to_yes_202
        if_declare_variable_4_value_equals_to_yes_200 >> rail.Label(
            'Yes') >> trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0201 >> \
            wait_for_completion_trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0201 >> \
            if_declare_variable_5_value_equals_to_yes_202
        if_declare_variable_5_value_equals_to_yes_202 >> rail.Label(
            'No') >> invoke_custom_ruby_code_204
        if_declare_variable_5_value_equals_to_yes_202 >> rail.Label('Yes') >> trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0203 >> \
            wait_for_completion_trigger_dag_run_ge_netherlands_timesheet_payrule_assignment_add_update_v1_0203 >> invoke_custom_ruby_code_204 >> \
            log_office_schedulename_205 >> if_log_no_changein_schedule_226_blank_229
        if_log_no_changein_schedule_226_blank_229 >> rail.Label(
            'Yes') >> log_office_scedules_236 >> if_first_uri_present_246
        if_first_uri_present_246 >> rail.Label(
            'Yes') >> log_max_effectivedate_247 >> log_currentschedulename_248 >> log_currentscheduleuri_249 >> if_log_currentschedulenuri_blank_250
        if_log_currentschedulenuri_blank_250 >> rail.Label(
            'No') >> if_log_currentschedulename_248_blank_258
        if_log_currentschedulenuri_blank_250 >> rail.Label('Yes') >> get_office_schedule_details_251 >> \
            log_numberofdaysinschedule_252 >> get_numberof_working_hours_253 >> if_log_currentschedulename_248_blank_258
        if_log_currentschedulename_248_blank_258 >> rail.Label(
            'Yes') >> if_workingday_less_than_8_current_scheduleisnotgreaterthan7days_259
        if_workingday_less_than_8_current_scheduleisnotgreaterthan7days_259 >> rail.Label(
            'No') >> check_timeoff_assignment_triggered_273
        if_workingday_less_than_8_current_scheduleisnotgreaterthan7days_259 >> rail.Label('Yes') >> \
            get_all_office_schedules_260 >> log_gettherequiredofficeschedule_uri_261 >> if_pluckuri_first_present_262
        if_pluckuri_first_present_262 >> rail.Label('Yes') >> log_office_schedule_252 >> put_schedule_policy_schedule_for_user_253 >> insert_to_list_254 >> \
            if_numberof_days_comparison_267
        if_numberof_days_comparison_267 >> rail.Label('Yes') >> trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_0284 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_0284 >> to_assignment_changes_269 >> \
            check_timeoff_assignment_triggered_273
        check_timeoff_assignment_triggered_273 >> rail.Label(
            'No') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208
        check_timeoff_assignment_triggered_273 >> rail.Label('Yes') >> trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_274 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_netherlands_child_update_to_v1_274 >> \
            ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208
        if_numberof_days_comparison_267 >> rail.Label(
            'No') >> check_timeoff_assignment_triggered_273
        if_pluckuri_first_present_262 >> rail.Label(
            'No') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208
        if_log_currentschedulename_248_blank_258 >> rail.Label(
            'No') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208
        if_first_uri_present_246 >> rail.Label(
            'No') >> if_log_currentschedulename_248_blank_258
        if_log_no_changein_schedule_226_blank_229 >> rail.Label(
            'No') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208 >> \
            ey_user_import_logs_add_entry_210 >> log_to_sumo
    return dag


rail.for_each_instance(create_dag)
