
from datetime import timedelta, datetime
import itertools
import pendulum
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
from ge.user_sync_denmark.denmark_master_mapper import denmark_master_mapper
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_denmark_user_update_{config.instance}',
        description=f'GE denmark User Update {config.instance}',
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

        denmark_master_mapper_search_entries_8 = rail.PythonOperator(
            task_id='denmark_master_mapper_search_entries_8',
            python_callable=lambda dag_run: list(
                filter(lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'], denmark_master_mapper))
        )

        if_entry_col5_blank_8 = rail.IfOperator(
            task_id='if_entry_col5_blank_8',
            test='''{{ result('denmark_master_mapper_search_entries_8') | length == 0 }}''',
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
                "userUri": "{{ dag_run.conf.UserURI }}"
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
                "userUri": dag_run.conf['UserURI'],
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
                "userUri": "{{ dag_run.conf.UserURI }}",
                "loginName": "{{ result('bulk_get_users3_6')[0].securityConfiguration.loginName }}{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate.month }}{{ result('bulk_get_users3_6')[0].userDetails.employmentDateRange.endDate.day }}{{ result('log_enddate_30') }}"
            }
        )

        trigger_dag_run_ge_user_sync_denmark_ge_denmark_add_v1_0async_callrecipeforrehire_32 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_denmark_ge_denmark_add_v1_0async_callrecipeforrehire_32',
            retries=0,
            items=[1],
            trigger_dag_id=f'ge_denmark_add_v1_0_{config.instance}',
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
                "Locationname": None,
                "Contractattributeannualvacationeligibility": None,
                "Subbiz": None,
                "Worktimesystem": None,
                "Educationlevel": None,
                "Specialworkschedule": None,
                "Work": None,
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
                "AssignmentEffectiveDate": dag_run.conf['Assignmenteffectivedate'],
                "HireEffectiveDate": dag_run.conf['HireEffectiveDate'],
                "RevTermEffectiveDate": dag_run.conf['RevTermEffectiveDate'],
                "type": "Rehire",
                "CareerBand": dag_run.conf['CareerBand'],
                "supervisor_processing_log": dag_run.conf['supervisor_processing_log'],
                "Departmenturi": dag_run.conf['Departmenturi']
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_denmark_ge_denmark_add_v1_0async_callrecipeforrehire_32 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_denmark_ge_denmark_add_v1_0async_callrecipeforrehire_32',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_denmark_ge_denmark_add_v1_0async_callrecipeforrehire_32") }}'
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
            no_task="if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_44",
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
                "userUri": "{{ dag_run.conf.UserURI }}"
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

        insert_to_list_43 = rail.SetVariableOperator(
            task_id='insert_to_list_43',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "End date removed, reverse termination date older than end date and newer than start date."
            }
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
                "userUri": "{{ dag_run.conf.UserURI }}",
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
                "userUri": "{{ dag_run.conf.UserURI }}",
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
                "userUri": "{{ dag_run.conf.UserURI }}",
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
            return custom_infos[0]['text'] if custom_infos else None

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
                "objectUri": "{{ dag_run.conf.UserURI }}",
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
                "objectUri": "{{ dag_run.conf.UserURI }}",
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
            no_task="log_valuefor_suspend_assignment_category_68",
        )

        update_text_value_customfield_66 = rail.RepliconServiceOperator(
            task_id='update_text_value_customfield_66',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.UserURI }}",
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

        log_valuefor_suspend_assignment_category_68 = rail.PythonOperator(
            task_id='log_valuefor_suspend_assignment_category_68',
            python_callable=lambda: get_custom_value(
                "Suspend Assignment Category")
        )

        log_urifor_suspend_assignment_category_68 = rail.PythonOperator(
            task_id='log_urifor_suspend_assignment_category_68',
            python_callable=lambda: get_custom_uri(
                'Suspend Assignment Category')
        )

        if_request_suspendassignmentcategory_present_69 = rail.IfOperator(
            task_id='if_request_suspendassignmentcategory_present_69',
            test='''{{ dag_run.conf.SuspendAssignmentCategory | is_truthy and result('log_valuefor_suspend_assignment_category_68') | lower != dag_run.conf.SuspendAssignmentCategory | lower }}''',
            yes_task="get_all_custom_field_drop_down_options_70",
            no_task="if_request_supervisorssoid_present_103",
        )

        get_all_custom_field_drop_down_options_70 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_70',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_urifor_suspend_assignment_category_68') }}"
            }
        )

        log_uriforsuspendassignmentcategory_72 = rail.PythonOperator(
            task_id='log_uriforsuspendassignmentcategory_72',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_custom_field_drop_down_options_70'), 'displayText', dag_run.conf['SuspendAssignmentCategory'], 'uri')
        )

        update_dropdown_value_customfield_73 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_customfield_73',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.UserURI }}",
                "customFieldUri": "{{ result('log_urifor_suspend_assignment_category_68') }}",
                "customFieldDropDownOptionUri": "{{ result('log_uriforsuspendassignmentcategory_72') }}"
            }
        )

        insert_to_list_74 = rail.SetVariableOperator(
            task_id='insert_to_list_74',
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
            no_task="if_request_legalentity_present_116",
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
                and x['type'] == entity_type_1
                and x['identifier__1__(_legal_entity_code/_type/_timeoff_type)'] == entity_type_2, denmark_master_mapper))
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
            no_task="if_request_legalentity_present_116",
        )

        insert_to_list_151 = rail.SetVariableOperator(
            task_id='insert_to_list_151',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": "Supervisor not assigned/updated since the user and supervisor SSO ID are same"
            }
        )

        dummy_operator_2 = rail.EmptyOperator(
            task_id="dummy_operator_2"
        )

        if_request_legalentity_present_116 = rail.IfOperator(
            task_id='if_request_legalentity_present_116',
            test='''{{ dag_run.conf.LegalEntity | is_truthy }}''',
            yes_task="log_costcenterschedule_131",
            no_task="if_request_payroll_present_164",
        )

        def costcenter_schedule_data(dag_run):
            derived_costcenter_schedules = []
            derived_legal_entity_schedules = []
            costcenter_schedules = rail.result('bulk_get_users3_6')[
                0]['costCenterSchedule']
            for costcenter_schedule in costcenter_schedules:
                if costcenter_schedule['effectiveDate']:
                    effective_date = get_datetime_obj(
                        costcenter_schedule['effectiveDate'])
                    current_date = datetime.strptime(
                        dag_run.conf['HireEffectiveDate'], '%d/%m/%Y') if dag_run.conf['HireEffectiveDate'] else pendulum.now(config.pacific_timezone)
                    if effective_date.date() <= current_date.date():
                        # Only include rows with a valid effectiveDate in legal_entity_schedules
                        # This ensures rows with null dates are excluded from current LE selection
                        derived_legal_entity_schedules.append({
                            "uri": costcenter_schedule['costCenter']['uri'],
                            "name": costcenter_schedule['costCenter']['displayText'],
                            "date": effective_date.strftime('%d/%m/%Y')
                        })
                    if effective_date.date() > current_date.date():
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
                    # Rows with null effectiveDate: use employment start date as fallback
                    # This ensures we have a date for comparison, but the max date selection
                    # will prefer any actual effective date over the fallback date
                    employment_start_date = get_datetime_obj(rail.result('bulk_get_users3_6')[
                        0]['userDetails']['employmentDateRange']['startDate'])
                    
                    # Add to legal_entity_schedules with fallback date
                    # The max date selection will naturally prefer valid dates over this fallback
                    derived_legal_entity_schedules.append({
                        "uri": costcenter_schedule['costCenter']['uri'],
                        "name": costcenter_schedule['costCenter']['displayText'],
                        "date": employment_start_date.strftime('%d/%m/%Y')
                    })
                    
                    # Also add to costcenter_schedules for future scheduling
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

        log_costcenterschedule_131 = rail.PythonOperator(
            task_id='log_costcenterschedule_131',
            python_callable=costcenter_schedule_data
        )

        if_first_uri_present_132 = rail.IfOperator(
            task_id='if_first_uri_present_132',
            test='''{{ result('log_costcenterschedule_131') | is_truthy and result('log_costcenterschedule_131').legal_entity_schedules | length > 0 }}''',
            yes_task="log_max_effectivedate_133",
            no_task="if_log_latesteffective_legal_entity_name_blank_136",
        )

        log_max_effectivedate_133 = rail.PythonOperator(
            task_id='log_max_effectivedate_133',
            python_callable=lambda:  (max(
                datetime.strptime(x['date'], '%d/%m/%Y') for x in rail.result('log_costcenterschedule_131')['legal_entity_schedules'])).strftime('%d/%m/%Y')
            if rail.result('log_costcenterschedule_131')['legal_entity_schedules'] else None
        )

        log_current_legal_entitycostcentername_134 = rail.PythonOperator(
            task_id='log_current_legal_entitycostcentername_134',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'log_costcenterschedule_131')['legal_entity_schedules'], 'date', rail.result('log_max_effectivedate_133'), 'name', "")
        )

        def get_entity_mapper(entity_type, legal_entity):
            entity_types = list(filter(
                lambda x: x['legal_entity'] == entity_type
                and x['type'] == legal_entity, denmark_master_mapper))
            return entity_types[0]['value'] if entity_types else None

        log_legal_entitycostcenternameaspermapper_134 = rail.PythonOperator(
            task_id='log_legal_entitycostcenternameaspermapper_134',
            python_callable=lambda dag_run:  get_entity_mapper(
                dag_run.conf['LegalEntity'], 'Legal Entity')
        )

        if_log_latesteffective_legal_entity_name_blank_136 = rail.IfOperator(
            task_id='if_log_latesteffective_legal_entity_name_blank_136',
            test='''{{ result('log_legal_entitycostcenternameaspermapper_134') | is_falsy or result('log_current_legal_entitycostcentername_134') | lower != result('log_legal_entitycostcenternameaspermapper_134') | lower }}''',
            yes_task="get_all_cost_centers_137",
            no_task="if_request_payroll_present_164",
        )

        get_all_cost_centers_137 = rail.RepliconServiceOperator(
            task_id='get_all_cost_centers_137',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_legal_entitycostcenternameaspermapper_134'), 'uri', '')
        )

        if_pluckuri_first_present_139 = rail.IfOperator(
            task_id='if_pluckuri_first_present_139',
            test='''{{ result('get_all_cost_centers_137') | is_truthy }}''',
            yes_task="put_cost_center_schedule_for_user_legal_entity_update_142",
            no_task="insert_to_list_163",
        )

        def get_cost_center_list(dag_run):
            # Prepare updated cost center schedule list with new legal entity
            costcenter_schedule_list = rail.result('bulk_get_users3_6')[
                0]['costCenterSchedule']
            current_date = datetime.strptime(
                dag_run.conf['HireEffectiveDate'], '%d/%m/%Y') if dag_run.conf['HireEffectiveDate'] else pendulum.now(config.pacific_timezone)
            new_effective_date = {
                "year": current_date.year,
                "month": current_date.month,
                "day": current_date.day
            }
            # Remove any existing entries with the same effective date
            costcenter_schedule_list = [
                item for item in costcenter_schedule_list 
                if item.get('effectiveDate') != new_effective_date
            ]
            costcenter_schedule_list.append({
                "costCenter": {
                    "uri": rail.result('get_all_cost_centers_137'),
                    "parentUri": None,
                    "name": None
                },
                "effectiveDate": new_effective_date
            })

            return costcenter_schedule_list

        put_cost_center_schedule_for_user_legal_entity_update_142 = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_legal_entity_update_142',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['UserURI'],
                "scheduleEntries": get_cost_center_list(dag_run)
            }
        )

        insert_to_list_143 = rail.SetVariableOperator(
            task_id='insert_to_list_143',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Legal entity updated"
            }
        )

        def get_timesheet_template(dag_run):
            timesheettemplate = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Timesheet Template", denmark_master_mapper))
            if timesheettemplate:
                timesheettemplate_ot = list(filter(
                    lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Timesheet Template"
                    and x['overtime_eligibility'] == dag_run.conf['OvertimeEligibility'], denmark_master_mapper))
                timesheettemplate = timesheettemplate_ot if timesheettemplate_ot else timesheettemplate
            return timesheettemplate[0]['value'] if timesheettemplate else None

        log_timesheet_templatenamefrommapper_144 = rail.PythonOperator(
            task_id='log_timesheet_templatenamefrommapper_144',
            python_callable=get_timesheet_template,
        )

        if_first_presence_blank_145 = rail.IfOperator(
            task_id='if_first_presence_blank_145',
            test='''{{ result('log_timesheet_templatenamefrommapper_144') | is_falsy}}''',
            yes_task="insert_to_list_146",
            no_task="get_all_policy_sets_148",
        )

        insert_to_list_146 = rail.SetVariableOperator(
            task_id='insert_to_list_146',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": '''timesheet template not updated since the "{{result('log_timesheet_templatenamefrommapper_144')}}" is not available in Replicon'''
            }
        )

        get_all_policy_sets_148 = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets_148',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        def get_tstemplate_uri():
            current_template = list(filter(lambda x: x['name'] and x['name'].lower() == rail.result(
                'log_timesheet_templatenamefrommapper_144').lower(), rail.result('get_all_policy_sets_148')))
            return current_template[0]['uri'] if current_template else None

        log_required_timesheet_template_uri_149 = rail.PythonOperator(
            task_id='log_required_timesheet_template_uri_149',
            python_callable=get_tstemplate_uri
        )

        if_pluckuri_smart_joinnil_present_150 = rail.IfOperator(
            task_id='if_pluckuri_smart_joinnil_present_150',
            test='''{{ result('log_required_timesheet_template_uri_149') | is_truthy }}''',
            yes_task="assign_policy_set_to_user_151",
            no_task="insert_to_list_154",
        )

        assign_policy_set_to_user_151 = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user_151',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.UserURI }}",
                "policySetUri": "{{ result('log_required_timesheet_template_uri_149') }}"
            }
        )

        insert_to_list_152 = rail.SetVariableOperator(
            task_id='insert_to_list_152',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Timesheet Tempalte updated"
            }
        )

        insert_to_list_154 = rail.SetVariableOperator(
            task_id='insert_to_list_154',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": '''Timesheet Tempalte "{{ result('log_timesheet_templatenamefrommapper_144') }}" not available in Replicon'''
            }
        )

        if_request_legalentity_equals_to_re1014_155 = rail.IfOperator(
            task_id='if_request_legalentity_equals_to_re1014_155',
            test='''{{ dag_run.conf.LegalEntity == 'RE1014'  or dag_run.conf.LegalEntity == 'RE1018' }}''',
            yes_task="if_request_overtimeeligibility_present_156",
            no_task="trigger_dag_run_ge_user_sync_denmark_workflow_to_add_timeoff_type_for_lm_wind_users_161",
        )

        if_request_overtimeeligibility_present_156 = rail.IfOperator(
            task_id='if_request_overtimeeligibility_present_156',
            test='''{{ dag_run.conf.OvertimeEligibility | is_truthy }}''',
            yes_task="trigger_dag_run_ge_user_sync_denmark_to_add_timeoff_type_for_lm_wind_users_157",
            no_task="insert_to_list_159",
        )

        trigger_dag_run_ge_user_sync_denmark_to_add_timeoff_type_for_lm_wind_users_157 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_denmark_to_add_timeoff_type_for_lm_wind_users_157',
            retries=0,
            items=[-1],
            trigger_dag_id=f'ge_user_sync_denmark_ge_denmark_child_workflow_to_add_timeoff_type_for_lm_wind_users_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "userloginname": "{{ dag_run.conf.OHRID }}",
                "useruri": "{{ dag_run.conf.UserURI }}",
                "LegalEntity": "{{ dag_run.conf.LegalEntity }}",
                "startdate": null,
                "type": "update",
                "numberofworkingdays": null,
                "fullpart": null,
                "OvertimeEligibility": "{{ dag_run.conf.OvertimeEligibility }}"
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_denmark_to_add_timeoff_type_for_lm_wind_users_157 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_denmark_to_add_timeoff_type_for_lm_wind_users_157',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_denmark_to_add_timeoff_type_for_lm_wind_users_157") }}'
        )

        insert_to_list_159 = rail.SetVariableOperator(
            task_id='insert_to_list_159',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": '''for legal entity {{ dag_run.conf.LegalEntity }} , overtime eligebility is mandatory to assign time off types'''
            }
        )

        trigger_dag_run_ge_user_sync_denmark_workflow_to_add_timeoff_type_for_lm_wind_users_161 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_denmark_workflow_to_add_timeoff_type_for_lm_wind_users_161',
            retries=0,
            items=[-1],
            trigger_dag_id=f'ge_user_sync_denmark_ge_denmark_child_workflow_to_add_timeoff_type_for_lm_wind_users_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "userloginname": "{{ dag_run.conf.OHRID }}",
                "useruri": "{{ dag_run.conf.UserURI }}",
                "LegalEntity": "{{ dag_run.conf.LegalEntity }}",
                "startdate": null,
                "type": "update",
                "numberofworkingdays": null,
                "fullpart": null,
                "OvertimeEligibility": null
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_denmark_workflow_to_add_timeoff_type_for_lm_wind_users_161 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_denmark_workflow_to_add_timeoff_type_for_lm_wind_users_161',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_denmark_workflow_to_add_timeoff_type_for_lm_wind_users_161") }}'
        )

        insert_to_list_163 = rail.SetVariableOperator(
            task_id='insert_to_list_163',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": '''Legal entity not updated since the '' is not available in Replicon'''
            }
        )

        if_request_payroll_present_164 = rail.IfOperator(
            task_id='if_request_payroll_present_164',
            test='''{{ dag_run.conf.Payroll | is_truthy }}''',
            yes_task="if_request_legalentity_equals_to_re1018_165",
            no_task="if_request_industryfocusgroup_present_requestrequestsupervisorssoid_177",
        )

        if_request_legalentity_equals_to_re1018_165 = rail.IfOperator(
            task_id='if_request_legalentity_equals_to_re1018_165',
            test='''{{ dag_run.conf.LegalEntity == 'RE1018'  or dag_run.conf.LegalEntity == 'RE1014' }}''',
            yes_task="if_payrulescriptschedule_to_json_contains_urn_166",
            no_task="if_request_industryfocusgroup_present_requestrequestsupervisorssoid_177",
        )

        if_payrulescriptschedule_to_json_contains_urn_166 = rail.IfOperator(
            task_id='if_payrulescriptschedule_to_json_contains_urn_166',
            test='''{{ result('bulk_get_users3_6')[0].payRuleScriptSchedule | length > 0 }}''',
            yes_task="parse_json_payrule_schedule_167",
            no_task="if_first_present_present_169",
        )

        parse_json_payrule_schedule_167 = rail.PythonOperator(
            task_id='parse_json_payrule_schedule_167',
            python_callable=lambda: rail.result('bulk_get_users3_6')[
                0]['payRuleScriptSchedule']
        )

        def get_current_schedule(data):
            if not data and len(data) == 0:
                return None
            current_schedule = list(filter(lambda x: datetime(
                **x['effectiveDate']) if x['effectiveDate'] else datetime.min.date() <= pendulum.now(config.pacific_timezone).date(), data))
            return None if len(current_schedule) == 0 else current_schedule[-1]

        get_current_schedule_168 = rail.PythonOperator(
            task_id='get_current_schedule_168',
            python_callable=lambda: get_current_schedule(
                rail.result('parse_json_payrule_schedule_167'))
        )

        get_payrule_mapper_168 = rail.PythonOperator(
            task_id='get_payrule_mapper_168',
            python_callable=lambda dag_run: get_entity_mapper(
                dag_run.conf['LegalEntity'], 'Payrule')
        )

        if_first_present_present_169 = rail.IfOperator(
            task_id='if_first_present_present_169',
            test='''{{ result('get_payrule_mapper_168') | is_truthy }}''',
            yes_task="if_first_presence_not_equals_to_schedulepoliciesdisplaytext_170",
            no_task="if_request_industryfocusgroup_present_requestrequestsupervisorssoid_177",
        )

        if_first_presence_not_equals_to_schedulepoliciesdisplaytext_170 = rail.IfOperator(
            task_id='if_first_presence_not_equals_to_schedulepoliciesdisplaytext_170',
            test='''{{ result('get_current_schedule_168') | is_falsy or result('get_current_schedule_168').payRuleScript.displayText | is_falsy or result('get_payrule_mapper_168') != result('get_current_schedule_168').payRuleScript.displayText }}''',
            yes_task="get_all_scripts_171",
            no_task="if_request_industryfocusgroup_present_requestrequestsupervisorssoid_177",
        )

        get_all_scripts_171 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_171',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
        )

        log_payruleuri_171 = rail.PythonOperator(
            task_id='log_payruleuri_171',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_scripts_171'), 'displayText', rail.result('get_payrule_mapper_168'), 'uri', '')
        )

        if_pluckuri_first_blank_172 = rail.IfOperator(
            task_id='if_pluckuri_first_blank_172',
            test='''{{ result('log_payruleuri_171') | is_falsy }}''',
            yes_task="insert_to_list_173",
            no_task="put_pay_rule_script_assignment_schedule_for_user_175",
        )

        insert_to_list_173 = rail.SetVariableOperator(
            task_id='insert_to_list_173',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": '''payrule not updated since the {{ result('get_payrule_mapper_168') }} is not available in Replicon'''
            }
        )

        put_pay_rule_script_assignment_schedule_for_user_175 = rail.RepliconServiceOperator(
            task_id='put_pay_rule_script_assignment_schedule_for_user_175',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['UserURI'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "payRulesScheduleModifications": {
                        "scheduleEntries": [
                            {
                                "payRuleScript": {
                                    "uri": rail.result('log_payruleuri_171'),
                                    "name": null
                                },
                                "effectiveDate": {
                                    "year": pendulum.now(config.pacific_timezone).year,
                                    "month": pendulum.now(config.pacific_timezone).month,
                                    "day": pendulum.now(config.pacific_timezone).day
                                }
                            }
                        ]
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_list_176 = rail.SetVariableOperator(
            task_id='insert_to_list_176',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "payrule updated"
            }
        )

        if_request_industryfocusgroup_present_requestrequestsupervisorssoid_177 = rail.IfOperator(
            task_id='if_request_industryfocusgroup_present_requestrequestsupervisorssoid_177',
            test='''{{ dag_run.conf.IndustryFocusGroup | is_truthy }}''',
            yes_task="log_industry_focus_group_divisionschedule_181",
            no_task="if_request_legalentity_equals_to_206",
        )

        def get_division_cost_schedules(dag_run):
            derived_industryfocus_schedules = []
            derived_division_schedules = []
            division_schedules = rail.result('bulk_get_users3_6')[
                0]['divisionSchedule']
            for division_schedule in division_schedules:
                if division_schedule['effectiveDate']:
                    effective_date = get_datetime_obj(
                        division_schedule['effectiveDate'])
                    current_date = datetime.strptime(
                        dag_run.conf['HireEffectiveDate'], '%d/%m/%Y') if dag_run.conf['HireEffectiveDate'] else pendulum.now(config.pacific_timezone)
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
                    employment_start_date = get_datetime_obj(rail.result('bulk_get_users3_6')[
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

        log_industry_focus_group_divisionschedule_181 = rail.PythonOperator(
            task_id='log_industry_focus_group_divisionschedule_181',
            python_callable=get_division_cost_schedules
        )

        if_first_uri_present_193 = rail.IfOperator(
            task_id='if_first_uri_present_193',
            test='''{{ result('log_industry_focus_group_divisionschedule_181').industry_focus_schedules | length > 0 }}''',
            yes_task="log_max_effectivedate_194",
            no_task="if_log_current_industry_focus_groupdivisionname_196",
        )

        log_max_effectivedate_194 = rail.PythonOperator(
            task_id='log_max_effectivedate_194',
            python_callable=lambda: (max(
                datetime.strptime(x['date'], '%d/%m/%Y') for x in rail.result('log_industry_focus_group_divisionschedule_181')['industry_focus_schedules'])).strftime('%d/%m/%Y')
            if rail.result('log_industry_focus_group_divisionschedule_181')['industry_focus_schedules'] else None
        )

        log_current_industry_focus_groupdivisionname_195 = rail.PythonOperator(
            task_id='log_current_industry_focus_groupdivisionname_195',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'log_industry_focus_group_divisionschedule_181')['industry_focus_schedules'], 'date', rail.result('log_max_effectivedate_194'), 'name', "")
        )

        if_log_current_industry_focus_groupdivisionname_196 = rail.IfOperator(
            task_id='if_log_current_industry_focus_groupdivisionname_196',
            test='''{{ result('log_industry_focus_group_divisionschedule_181') | is_falsy or result('log_current_industry_focus_groupdivisionname_195') | lower != dag_run.conf.IndustryFocusGroup | lower }}''',
            yes_task="get_all_divisions_197",
            no_task="if_request_legalentity_equals_to_206",
        )

        get_all_divisions_197 = rail.RepliconServiceOperator(
            task_id='get_all_divisions_197',
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
        )

        log_industry_focus_groupdivision_uri_198 = rail.PythonOperator(
            task_id='log_industry_focus_groupdivision_uri_198',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_divisions_197'), 'displayText', dag_run.conf['IndustryFocusGroup'], 'uri')
        )

        if_log_industry_focus_groupdivision_uri_present_199 = rail.IfOperator(
            task_id='if_log_industry_focus_groupdivision_uri_present_199',
            test='''{{ result('log_industry_focus_groupdivision_uri_198') | is_truthy }}''',
            yes_task="put_division_schedule_for_user_industry_focus_group_update_202",
            no_task="insert_to_list_205",
        )

        def get_division_schedules(dag_run):
            current_date = datetime.strptime(dag_run.conf['HireEffectiveDate'], '%d/%m/%Y') \
                if dag_run.conf['HireEffectiveDate'] else pendulum.now(config.pacific_timezone).date()
            division_schedules = rail.result('log_industry_focus_group_divisionschedule_181')[
                'division_schedules']
            division_schedules.append({
                "division": {
                    "uri": rail.result('log_industry_focus_groupdivision_uri_198'),
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

        put_division_schedule_for_user_industry_focus_group_update_202 = rail.RepliconServiceOperator(
            task_id='put_division_schedule_for_user_industry_focus_group_update_202',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['UserURI'],
                "scheduleEntries": get_division_schedules(dag_run)
            }
        )

        insert_to_list_203 = rail.SetVariableOperator(
            task_id='insert_to_list_203',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Industry focus group updated"
            }
        )

        insert_to_list_205 = rail.SetVariableOperator(
            task_id='insert_to_list_205',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": '''Industry focus group not updated since the "{{dag_run.conf.IndustryFocusGroup}}" is not available in Replicon'''
            }
        )

        if_request_legalentity_equals_to_206 = rail.IfOperator(
            task_id='if_request_legalentity_equals_to_206',
            test='''{{ dag_run.conf.LegalEntity == 'RE1014'  or dag_run.conf.LegalEntity == 'RE1018' }}''',
            yes_task="log_valuefor_ot_eligibility_title_207",
            no_task="log_office_schedulename_223",
        )

        log_valuefor_ot_eligibility_title_207 = rail.PythonOperator(
            task_id='log_valuefor_ot_eligibility_title_207',
            python_callable=lambda: get_custom_value("Overtime Eligibility")
        )

        if_request_overtimeeligibility_present_207 = rail.IfOperator(
            task_id='if_request_overtimeeligibility_present_207',
            test='''{{ dag_run.conf.OvertimeEligibility | is_truthy and result('log_valuefor_ot_eligibility_title_207') | is_truthy and result('log_valuefor_ot_eligibility_title_207') | lower != dag_run.conf.OvertimeEligibility | lower}}''',
            yes_task="get_all_custom_field_drop_down_options_208",
            no_task="log_office_schedulename_223",
        )

        get_all_custom_field_drop_down_options_208 = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_208',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": get_custom_uri('Overtime Eligibility')
            }
        )

        update_dropdown_value_customfield_209 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_customfield_209',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['UserURI'],
                "customFieldUri": get_custom_uri('Overtime Eligibility'),
                "customFieldDropDownOptionUri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_custom_field_drop_down_options_208'), 'displayText', dag_run.conf['OvertimeEligibility'], 'uri')
            }
        )

        get_all_employee_type_details_210 = rail.RepliconServiceOperator(
            task_id='get_all_employee_type_details_210',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data={}
        )

        def get_employee_type_mapper(dag_run):
            ot_eligibility_input = dag_run.conf['OvertimeEligibility'] if dag_run.conf['OvertimeEligibility'] else ""
            employeetype = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Employee Type", denmark_master_mapper))
            if employeetype:
                employeetype_ot = list(filter(
                    lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Employee Type"
                    and x['overtime_eligibility'] == ot_eligibility_input, denmark_master_mapper))
                employeetype = employeetype_ot if employeetype_ot else employeetype
            return employeetype[0]['value'] if employeetype else None

        def get_employee_type_uri(dag_run):
            mapper_employee_type = get_employee_type_mapper(dag_run)
            current_employeetype = list(filter(lambda x: x['name'] and x['name'].lower(
            ) == mapper_employee_type.lower(), rail.result('get_all_employee_type_details_210')))
            return current_employeetype[0]['uri'] if current_employeetype else None

        update_employee_type_for_user_212 = rail.RepliconServiceOperator(
            task_id='update_employee_type_for_user_212',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['UserURI'],
                "employeeTypeUri": get_employee_type_uri(dag_run)
            }
        )

        trigger_dag_run_ge_user_sync_denmark_to_add_timeoff_type_for_lm_wind_users_213 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_denmark_to_add_timeoff_type_for_lm_wind_users_213',
            retries=0,
            items=[-1],
            trigger_dag_id=f'ge_user_sync_denmark_ge_denmark_child_workflow_to_add_timeoff_type_for_lm_wind_users_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "userloginname": "{{ dag_run.conf.OHRID }}",
                "useruri": "{{ dag_run.conf.UserURI }}",
                "LegalEntity": "{{ dag_run.conf.LegalEntity }}",
                "startdate": null,
                "type": "update",
                "numberofworkingdays": null,
                "fullpart": null,
                "OvertimeEligibility": "{{ dag_run.conf.OvertimeEligibility }}"
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_denmark_to_add_timeoff_type_for_lm_wind_users_213 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_denmark_to_add_timeoff_type_for_lm_wind_users_213',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_denmark_to_add_timeoff_type_for_lm_wind_users_213") }}'
        )

        log_timesheet_templatenamefrommapper_214 = rail.PythonOperator(
            task_id='log_timesheet_templatenamefrommapper_214',
            python_callable=get_timesheet_template
        )

        if_log_timesheet_templatenamefrommapper_214_present_215 = rail.IfOperator(
            task_id='if_log_timesheet_templatenamefrommapper_214_present_215',
            test='''{{ result('log_timesheet_templatenamefrommapper_214') | is_truthy }}''',
            yes_task="get_all_policy_sets_216",
            no_task="log_office_schedulename_223",
        )

        get_all_policy_sets_216 = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets_216',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        def get_tstemplate_uri_216():
            current_template = list(filter(lambda x: x['name'] and x['name'].lower() == rail.result(
                'log_timesheet_templatenamefrommapper_214').lower(), rail.result('get_all_policy_sets_216')))
            return current_template[0]['uri'] if current_template else None

        log_required_timesheet_template_uri_216 = rail.PythonOperator(
            task_id='log_required_timesheet_template_uri_216',
            python_callable=get_tstemplate_uri_216
        )

        if_pluckuri_smart_joinnil_present_217 = rail.IfOperator(
            task_id='if_pluckuri_smart_joinnil_present_217',
            test='''{{ result('log_required_timesheet_template_uri_216') | is_truthy }}''',
            yes_task="update_timesheet_template_218",
            no_task="log_office_schedulename_223",
        )

        update_timesheet_template_218 = rail.RepliconServiceOperator(
            task_id='update_timesheet_template_218',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": '''{{ dag_run.conf.UserURI }}''',
                "policySetUri": "{{ result('log_required_timesheet_template_uri_216')}}"
            }
        )

        insert_to_list_221 = rail.SetVariableOperator(
            task_id='insert_to_list_221',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "value": '''Timesheet Tempalte {{ result('log_timesheet_templatenamefrommapper_214') }} not available in Replicon'''
            }
        )

        # pylint: disable=too-many-boolean-expressions
        def get_schedule_name_based_on_input(dag_run):
            schedule_name_based_on_input = None
            if dag_run.conf['DWSMonday'] and dag_run.conf['DWSTuesday']\
                and dag_run.conf['DWSWednesday'] and dag_run.conf['DWSThursday'] \
                    and dag_run.conf['DWSFriday'] and dag_run.conf['DWSSaturday'] \
                or dag_run.conf['DWSSunday']:
                schedule_name_based_on_input = dag_run.conf['DWSMonday'] + "|" + dag_run.conf['DWSTuesday'] + "|" + dag_run.conf['DWSWednesday'] + "|" + \
                    dag_run.conf['DWSThursday'] + "|" + dag_run.conf['DWSFriday'] + \
                    "|" + dag_run.conf['DWSSaturday'] + \
                    "|" + dag_run.conf['DWSSunday']
            return schedule_name_based_on_input

        log_office_schedulename_223 = rail.PythonOperator(
            task_id='log_office_schedulename_223',
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

        if_log_no_changein_schedule_226_blank_227 = rail.IfOperator(
            task_id='if_log_no_changein_schedule_226_blank_227',
            test=get_schedule_name,
            yes_task="log_office_scedules_232",
            no_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208",
        )

        def get_office_schedules(dag_run):
            derived_office_schedules = []
            derived_office_schedules_request = []
            # division_schedules = rail.result('bulk_get_users3_6')[
            #     0]['divisionSchedule']
            office_schedule_policies = rail.result('bulk_get_users3_6')[
                0]['schedulePolicies']
            for office_schedule_policy in office_schedule_policies:
                if office_schedule_policy['effectiveDate']:
                    effective_date = get_datetime_obj(
                        office_schedule_policy['effectiveDate'])
                    current_date = datetime.strptime(
                        dag_run.conf['DWSStartDate'], '%d/%m/%Y') if dag_run.conf['DWSStartDate'] else pendulum.now(config.pacific_timezone)
                    if effective_date.date() < current_date.date():
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

        log_office_scedules_232 = rail.PythonOperator(
            task_id='log_office_scedules_232',
            python_callable=get_office_schedules
        )

        if_first_uri_present_244 = rail.IfOperator(
            task_id='if_first_uri_present_244',
            test='''{{ result('log_office_scedules_232').office_schedules_request | length > 0 }}''',
            yes_task="log_max_effectivedate_245",
            no_task="if_log_currentschedulename_246_blank_247",
        )

        log_max_effectivedate_245 = rail.PythonOperator(
            task_id='log_max_effectivedate_245',
            python_callable=lambda: (max(
                datetime.strptime(x['date'], '%d/%m/%Y') for x in rail.result('log_office_scedules_232')['office_schedules'])).strftime('%d/%m/%Y')
            if rail.result('log_office_scedules_232')['office_schedules'] else None
        )

        log_currentschedulename_246 = rail.PythonOperator(
            task_id='log_currentschedulename_246',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'log_office_scedules_232')['office_schedules'], 'date', rail.result('log_max_effectivedate_245'), 'name', "")
        )

        if_log_currentschedulename_246_blank_247 = rail.IfOperator(
            task_id='if_log_currentschedulename_246_blank_247',
            test='''{{ result('log_currentschedulename_246') | is_falsy  or result('log_currentschedulename_246') != result('log_office_schedulename_223') }}''',
            yes_task="get_all_office_schedules_248",
            no_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208",
        )

        get_all_office_schedules_248 = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules_248',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        log_gettherequiredofficeschedule_uri_248 = rail.PythonOperator(
            task_id='log_gettherequiredofficeschedule_uri_248',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_office_schedules_248'), 'displayText', rail.result('log_office_schedulename_223'), 'uri')
        )

        if_pluckuri_first_present_250 = rail.IfOperator(
            task_id='if_pluckuri_first_present_250',
            test='''{{ result('log_gettherequiredofficeschedule_uri_248') | is_truthy }}''',
            yes_task="log_office_schedule_252",
            no_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208",
        )

        def get_office_schedule_request(dag_run):
            existing_schedules = rail.result('log_office_scedules_232')[
                'office_schedules_request']
            effective_date = datetime.strptime(
                dag_run.conf['DWSStartDate'], '%d/%m/%Y') if dag_run.conf['DWSStartDate'] else pendulum.now(config.pacific_timezone)
            existing_schedules.append({
                "schedulePolicy": {
                    "officeScheduleUri": rail.result('log_gettherequiredofficeschedule_uri_248'),
                    "name": null,
                    "officeSchedule": {
                        "officeScheduleUri": rail.result('log_gettherequiredofficeschedule_uri_248'),
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
                "userUri": dag_run.conf['UserURI'],
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

        if_log_currentschedulename_246_present_256 = rail.IfOperator(
            task_id='if_log_currentschedulename_246_present_256',
            test='''{{ result('log_currentschedulename_246') | is_truthy }}''',
            yes_task="get_office_schedule_details_258",
            no_task="if_numberof_days_comparison_280",
        )

        get_office_schedule_details_258 = rail.RepliconServiceOperator(
            task_id='get_office_schedule_details_258',
            endpoint="/services/OfficeScheduleService1.svc/GetOfficeScheduleDetails",
            data=lambda: {
                "officeScheduleUri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'log_office_scedules_232')['office_schedules'], 'date', rail.result('log_max_effectivedate_245'), 'uri')
            }
        )

        def get_existing_number_of_working_days():
            number_of_working_days = 0
            sch_recurring_pattern = rail.result(
                'get_office_schedule_details_258')
            recurringPattern_arr = sch_recurring_pattern['recurringPattern'][
                'patternEntries'] if sch_recurring_pattern and sch_recurring_pattern['recurringPattern'] else []
            for recurringPattern in recurringPattern_arr:
                if recurringPattern['workDuration'] and recurringPattern['workDuration']['hours'] > 0:
                    number_of_working_days += 1
            return number_of_working_days

        # pylint: disable=too-many-boolean-expressions
        def get_new_numberof_days(dag_run):
            numberofdays = 0
            schedule_info = list(filter(
                lambda x: x['legal_entity'] == dag_run.conf['LegalEntity'] and x['type'] == "Default Schedule", denmark_master_mapper))
            mapper_schedule_name = schedule_info[0]['value'] if schedule_info else None
            schedule_to_assign = rail.result('log_office_schedulename_223')
            if mapper_schedule_name == schedule_to_assign:
                numberofdays = 5
            else:
                if dag_run.conf['DWSMonday'] and float(dag_run.conf['DWSMonday']) > 0:
                    numberofdays += 1
                if dag_run.conf['DWSTuesday'] and float(dag_run.conf['DWSTuesday']) > 0:
                    numberofdays += 1
                if dag_run.conf['DWSWednesday'] and float(dag_run.conf['DWSWednesday']) > 0:
                    numberofdays += 1
                if dag_run.conf['DWSThursday'] and float(dag_run.conf['DWSThursday']) > 0:
                    numberofdays += 1
                if dag_run.conf['DWSFriday'] and float(dag_run.conf['DWSFriday']) > 0:
                    numberofdays += 1
                if dag_run.conf['DWSSaturday'] and float(dag_run.conf['DWSSaturday']) > 0:
                    numberofdays += 1
                if dag_run.conf['DWSSunday'] and float(dag_run.conf['DWSSunday']) > 0:
                    numberofdays += 1
            return numberofdays

        def is_schedule_name_present(dag_run):
            existing_numberoff_days = get_existing_number_of_working_days()
            new_numberof_days = get_new_numberof_days(dag_run)
            return bool(existing_numberoff_days != new_numberof_days)

        if_numberof_days_comparison_280 = rail.IfOperator(
            task_id='if_numberof_days_comparison_280',
            test=is_schedule_name_present,
            yes_task="parse_json_282",
            no_task="ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208",
        )

        parse_json_282 = rail.PythonOperator(
            task_id='parse_json_282',
            python_callable=lambda: rail.result('bulk_get_users3_6')[
                0]['timeOffTypePolicySummary']['policiesByTimeOffType']
        )

        def get_timeoff_uri(to_name):
            existing_timeoffs = rail.result('parse_json_282')
            return rail.find_first_by_attr_and_get_attr(existing_timeoffs, 'timeOffType.name', to_name, 'timeOffType.uri')

        trigger_dag_run_ge_user_sync_denmark_ge_denmark_child_vacation_parttime_v1_0284 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_denmark_ge_denmark_child_vacation_parttime_v1_0284',
            retries=0,
            items=[-1],
            trigger_dag_id=f'ge_user_sync_denmark_ge_denmark_child_vacation_parttime_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda dag_run: {
                "userloginname": dag_run.conf['OHRID'],
                "useruri": dag_run.conf['UserURI'],
                "startdate": dag_run.conf['DWSStartDate'] if dag_run.conf['DWSStartDate'] else pendulum.now(config.pacific_timezone).strftime('%d/%m/%Y'),
                "type": "Update",
                "numberofworkingdays": get_new_numberof_days(dag_run),
                "timeoffuri": get_timeoff_uri('01. DK_Vacation'),
                "actualstartdate": rail.result('log_startdate_7')
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_denmark_ge_denmark_child_vacation_parttime_v1_0284 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_denmark_ge_denmark_child_vacation_parttime_v1_0284',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_denmark_ge_denmark_child_vacation_parttime_v1_0284") }}'
        )

        insert_to_list_285 = rail.SetVariableOperator(
            task_id='insert_to_list_285',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "01. DK_Vacation policy updated"
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
        declare_list_2 >> declare_list_3 >> declare_variable_4 >> bulk_get_users3_6 >> log_startdate_7 >> denmark_master_mapper_search_entries_8 >> if_entry_col5_blank_8
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
            'Yes') >> log_enddate_30 >> updateloginname_31 >> trigger_dag_run_ge_user_sync_denmark_ge_denmark_add_v1_0async_callrecipeforrehire_32 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_denmark_ge_denmark_add_v1_0async_callrecipeforrehire_32 >> ey_user_import_logs_add_entry_210
        if_enddate_year_present_29 >> rail.Label(
            'No') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_34 >> ey_user_import_logs_add_entry_210
        if_userdetails_isenabled_is_not_true_rehire_24 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_true_transfer_36
        if_userdetails_isenabled_is_true_transfer_36 >> rail.Label(
            'No') >> dummy_operator_2 >> if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_44
        if_userdetails_isenabled_is_true_transfer_36 >> rail.Label(
            'Yes') >> log_enddate_37 >> if_request_reverseterminationeffectivedate_present_38
        if_request_reverseterminationeffectivedate_present_38 >> rail.Label(
            'No') >> if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_44
        if_request_reverseterminationeffectivedate_present_38 >> rail.Label(
            'Yes') >> if_userdetails_isenabled_is_not_true_39
        if_userdetails_isenabled_is_not_true_39 >> rail.Label(
            'No') >> remove_enddate_42
        if_userdetails_isenabled_is_not_true_39 >> rail.Label('Yes') >> enable_login_40 >> insert_to_list_41 >> remove_enddate_42 >> \
            insert_to_list_43 >> if_firstname_downcase_not_equals_to_dataworkato_servicereceive_requestrequestfirstnamedowncase_44
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
            'No') >> log_valuefor_suspend_assignment_category_68
        if_request_hrmname_present_64 >> rail.Label('Yes') >> update_text_value_customfield_66 >> \
            insert_to_list_67 >> log_valuefor_suspend_assignment_category_68 >> log_urifor_suspend_assignment_category_68 >> \
            if_request_suspendassignmentcategory_present_69
        if_request_suspendassignmentcategory_present_69 >> rail.Label(
            'No') >> if_request_supervisorssoid_present_103
        if_request_suspendassignmentcategory_present_69 >> rail.Label('No') >> get_all_custom_field_drop_down_options_70 >> \
            log_uriforsuspendassignmentcategory_72 >> update_dropdown_value_customfield_73 >> \
            insert_to_list_74 >> if_request_supervisorssoid_present_103
        if_request_supervisorssoid_present_103 >> rail.Label(
            'No') >> if_request_legalentity_present_116
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
            log_checkif_end_user_manager_permissionsetisassigned_126 >> log_required_supervisor_permission_127 >> \
            if_log_checkif_manager_permissionsetisassigned_125_contains_approver_127
        if_log_checkif_manager_permissionsetisassigned_125_contains_approver_127 >> rail.Label(
            'No') >> update_supervisor_assignment_schedule_over_date_range_144 >> insert_to_list_145 >> \
            if_log_6_contains_false_if_log_6_contains_false_supervisorprofileisdisableddonotupdatesupervisor_146
        if_log_checkif_manager_permissionsetisassigned_125_contains_approver_127 >> rail.Label(
            'Yes') >> get_all_permission_sets_134 >> should_add_missing_permissions
        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_supervisor_permissions >> update_supervisor_assignment_schedule_over_date_range_144
        should_add_missing_permissions >> rail.Label(
            'No') >> update_supervisor_assignment_schedule_over_date_range_144
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
            'Yes') >> insert_to_list_151 >> if_request_legalentity_present_116
        if_request_legalentity_present_116 >> rail.Label(
            'No') >> if_request_payroll_present_164
        if_request_legalentity_present_116 >> rail.Label(
            'Yes') >> log_costcenterschedule_131 >> if_first_uri_present_132
        if_first_uri_present_132 >> rail.Label(
            'No') >> if_log_latesteffective_legal_entity_name_blank_136
        if_first_uri_present_132 >> rail.Label('Yes') >> log_max_effectivedate_133 >> log_current_legal_entitycostcentername_134 >> \
            log_legal_entitycostcenternameaspermapper_134 >> if_log_latesteffective_legal_entity_name_blank_136
        if_log_latesteffective_legal_entity_name_blank_136 >> rail.Label(
            'No') >> if_request_payroll_present_164
        if_request_payroll_present_164 >> rail.Label(
            'No') >> if_request_industryfocusgroup_present_requestrequestsupervisorssoid_177
        if_request_payroll_present_164 >> rail.Label(
            'Yes') >> if_request_legalentity_equals_to_re1018_165
        if_request_legalentity_equals_to_re1018_165 >> rail.Label(
            'No') >> if_request_industryfocusgroup_present_requestrequestsupervisorssoid_177
        if_request_legalentity_equals_to_re1018_165 >> rail.Label(
            'Yes') >> if_payrulescriptschedule_to_json_contains_urn_166
        if_payrulescriptschedule_to_json_contains_urn_166 >> rail.Label(
            'No') >> if_first_present_present_169
        if_payrulescriptschedule_to_json_contains_urn_166 >> rail.Label('Yes') >> parse_json_payrule_schedule_167 >> get_current_schedule_168 >> \
            get_payrule_mapper_168 >> if_first_present_present_169
        if_first_present_present_169 >> rail.Label(
            'No') >> if_request_industryfocusgroup_present_requestrequestsupervisorssoid_177
        if_first_present_present_169 >> rail.Label(
            'Yes') >> if_first_presence_not_equals_to_schedulepoliciesdisplaytext_170
        if_first_presence_not_equals_to_schedulepoliciesdisplaytext_170 >> rail.Label('Yes') >> get_all_scripts_171 >> log_payruleuri_171 >> \
            if_pluckuri_first_blank_172
        if_pluckuri_first_blank_172 >> rail.Label(
            'No') >> insert_to_list_173 >> if_request_industryfocusgroup_present_requestrequestsupervisorssoid_177
        if_pluckuri_first_blank_172 >> rail.Label('Yes') >> put_pay_rule_script_assignment_schedule_for_user_175 >> \
            insert_to_list_176 >> if_request_industryfocusgroup_present_requestrequestsupervisorssoid_177
        if_first_presence_not_equals_to_schedulepoliciesdisplaytext_170 >> rail.Label(
            'No') >> if_request_industryfocusgroup_present_requestrequestsupervisorssoid_177
        if_request_industryfocusgroup_present_requestrequestsupervisorssoid_177 >> rail.Label(
            'No') >> if_request_legalentity_equals_to_206
        if_request_industryfocusgroup_present_requestrequestsupervisorssoid_177 >> rail.Label('Yes') >> \
            log_industry_focus_group_divisionschedule_181 >> if_first_uri_present_193
        if_first_uri_present_193 >> rail.Label(
            'No') >> if_log_current_industry_focus_groupdivisionname_196
        if_first_uri_present_193 >> rail.Label('Yes') >> log_max_effectivedate_194 >> log_current_industry_focus_groupdivisionname_195 >> \
            if_log_current_industry_focus_groupdivisionname_196
        if_log_current_industry_focus_groupdivisionname_196 >> rail.Label(
            'No') >> if_request_legalentity_equals_to_206
        if_log_current_industry_focus_groupdivisionname_196 >> rail.Label('Yes') >> get_all_divisions_197 >> log_industry_focus_groupdivision_uri_198 >> \
            if_log_industry_focus_groupdivision_uri_present_199
        if_log_industry_focus_groupdivision_uri_present_199 >> rail.Label(
            'No') >> insert_to_list_205 >> if_request_legalentity_equals_to_206
        if_log_industry_focus_groupdivision_uri_present_199 >> rail.Label('Yes') >> put_division_schedule_for_user_industry_focus_group_update_202 >> \
            insert_to_list_203 >> if_request_legalentity_equals_to_206
        if_request_legalentity_equals_to_206 >> rail.Label(
            'No') >> log_office_schedulename_223
        if_request_legalentity_equals_to_206 >> rail.Label(
            'Yes') >> log_valuefor_ot_eligibility_title_207 >> if_request_overtimeeligibility_present_207
        if_request_overtimeeligibility_present_207 >> rail.Label(
            'No') >> log_office_schedulename_223
        if_request_overtimeeligibility_present_207 >> rail.Label('Yes') >> get_all_custom_field_drop_down_options_208 >> update_dropdown_value_customfield_209 >> \
            get_all_employee_type_details_210 >> update_employee_type_for_user_212 >> trigger_dag_run_ge_user_sync_denmark_to_add_timeoff_type_for_lm_wind_users_213 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_denmark_to_add_timeoff_type_for_lm_wind_users_213 >> log_timesheet_templatenamefrommapper_214 >> \
            if_log_timesheet_templatenamefrommapper_214_present_215
        if_log_timesheet_templatenamefrommapper_214_present_215 >> rail.Label(
            'No') >> log_office_schedulename_223
        if_log_timesheet_templatenamefrommapper_214_present_215 >> rail.Label('Yes') >> get_all_policy_sets_216 >> log_required_timesheet_template_uri_216 >> \
            if_pluckuri_smart_joinnil_present_217
        if_pluckuri_smart_joinnil_present_217 >> rail.Label(
            'No') >> log_office_schedulename_223
        if_pluckuri_smart_joinnil_present_217 >> rail.Label('Yes') >> update_timesheet_template_218 >> insert_to_list_221 >> \
            log_office_schedulename_223 >> if_log_no_changein_schedule_226_blank_227
        if_log_no_changein_schedule_226_blank_227 >> rail.Label(
            'No') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208
        if_log_no_changein_schedule_226_blank_227 >> rail.Label(
            'Yes') >> log_office_scedules_232 >> if_first_uri_present_244
        if_first_uri_present_244 >> rail.Label(
            'No') >> if_log_currentschedulename_246_blank_247
        if_first_uri_present_244 >> rail.Label(
            'Yes') >> log_max_effectivedate_245 >> log_currentschedulename_246 >> if_log_currentschedulename_246_blank_247
        if_log_currentschedulename_246_blank_247 >> rail.Label(
            'No') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208
        if_log_currentschedulename_246_blank_247 >> rail.Label('Yes') >> get_all_office_schedules_248 >> \
            log_gettherequiredofficeschedule_uri_248 >> if_pluckuri_first_present_250
        if_pluckuri_first_present_250 >> rail.Label(
            'No') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208
        if_pluckuri_first_present_250 >> rail.Label('Yes') >> log_office_schedule_252 >> put_schedule_policy_schedule_for_user_253 >> insert_to_list_254 >> \
            if_log_currentschedulename_246_present_256
        if_log_currentschedulename_246_present_256 >> rail.Label(
            'No') >> if_numberof_days_comparison_280
        if_log_currentschedulename_246_present_256 >> rail.Label(
            'No') >> get_office_schedule_details_258 >> if_numberof_days_comparison_280
        if_numberof_days_comparison_280 >> rail.Label(
            'No') >> ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208
        if_numberof_days_comparison_280 >> rail.Label('Yes') >> parse_json_282 >> trigger_dag_run_ge_user_sync_denmark_ge_denmark_child_vacation_parttime_v1_0284 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_denmark_ge_denmark_child_vacation_parttime_v1_0284 >> insert_to_list_285 >> \
            ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208
        if_log_latesteffective_legal_entity_name_blank_136 >> rail.Label(
            'Yes') >> get_all_cost_centers_137 >> if_pluckuri_first_present_139
        if_pluckuri_first_present_139 >> rail.Label(
            'No') >> insert_to_list_163 >> if_request_payroll_present_164
        if_pluckuri_first_present_139 >> rail.Label('Yes') >> put_cost_center_schedule_for_user_legal_entity_update_142 >> \
            insert_to_list_143 >> log_timesheet_templatenamefrommapper_144 >> if_first_presence_blank_145
        if_first_presence_blank_145 >> rail.Label(
            'No') >> get_all_policy_sets_148
        if_first_presence_blank_145 >> rail.Label('Yes') >> insert_to_list_146 >> get_all_policy_sets_148 >> \
            log_required_timesheet_template_uri_149 >> if_pluckuri_smart_joinnil_present_150
        if_pluckuri_smart_joinnil_present_150 >> rail.Label(
            'No') >> insert_to_list_154
        if_pluckuri_smart_joinnil_present_150 >> rail.Label('Yes') >> assign_policy_set_to_user_151 >> insert_to_list_152 >> insert_to_list_154 >> \
            if_request_legalentity_equals_to_re1014_155
        if_request_legalentity_equals_to_re1014_155 >> rail.Label('No') >> trigger_dag_run_ge_user_sync_denmark_workflow_to_add_timeoff_type_for_lm_wind_users_161 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_denmark_workflow_to_add_timeoff_type_for_lm_wind_users_161 >> if_request_payroll_present_164
        if_request_legalentity_equals_to_re1014_155 >> rail.Label(
            'Yes') >> if_request_overtimeeligibility_present_156
        if_request_overtimeeligibility_present_156 >> rail.Label(
            'No') >> insert_to_list_159 >> if_request_payroll_present_164
        if_request_overtimeeligibility_present_156 >> rail.Label('Yes') >> trigger_dag_run_ge_user_sync_denmark_to_add_timeoff_type_for_lm_wind_users_157 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_denmark_to_add_timeoff_type_for_lm_wind_users_157 >> insert_to_list_159
        if_request_ohrid_equals_to_dataworkato_servicereceive_requestrequestsupervisorssoid_150 >> rail.Label(
            'No') >> if_request_legalentity_present_116
        ey_user_import_logs_ey_user_import_logs_ey_user_import_logs_add_entry_210_210_208 >> ey_user_import_logs_add_entry_210 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
