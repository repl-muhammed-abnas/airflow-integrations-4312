
from datetime import timedelta, datetime, date
import json
from airflow.models import Variable
import rail
from velaw.user_import.user_import_mapper import velaw_user_import_mapper

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'velaw_user_import_velawg3_user_update_v2_0_{config.instance}',
        description=f'VelawG3_User Update V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
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
            end_task='velaw_user_import_logs_add_entry_185',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_2 = rail.SetVariableOperator(
            task_id='declare_list_2',
            append=False,
            name='Exception',
            value=[]
        )

        declare_variable_3 = rail.SetVariableOperator(
            task_id='declare_variable_3',
            append=False,
            name='timeoffprocess',
            value=None
        )

        declare_variable_4 = rail.SetVariableOperator(
            task_id='declare_variable_4',
            append=False,
            name='scheduleupdate',
            value=None
        )

        declare_variable_5 = rail.SetVariableOperator(
            task_id='declare_variable_5',
            append=False,
            name='timezoneandholidaycalendarupdate',
            value=None
        )

        declare_variable_6 = rail.SetVariableOperator(
            task_id='declare_variable_6',
            append=False,
            name='timeoffapprovalpathupdate',
            value=None
        )

        declare_variable_7 = rail.SetVariableOperator(
            task_id='declare_variable_7',
            append=False,
            name='timesheetapprovalpathupdate',
            value=None
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

        date_split_todays_date_10 = rail.EmptyOperator(
            task_id='date_split_todays_date_10',
        )

        velaw_check_user_update_logs = rail.CreateLogOperator(
            task_id='velaw_check_user_update_logs'
        )

        velaw_supervisor_check_user_update_logs = rail.CreateLogOperator(
            task_id='velaw_supervisor_check_user_update_logs'
        )

        if_userdetails_isenabled_is_not_true_11 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_11',
            test=lambda dag_run: not rail.result('bulk_get_users3_9')[
                0]['userDetails']['isEnabled'] and dag_run.conf['enabled'] == 'False',
            yes_task="velaw_user_import_logs_add_entry_12",
            no_task="velaw_user_import_mapper_search_entries_time_off_approval_path_14",
        )

        velaw_user_import_logs_add_entry_12 = rail.WriteLogOperator(
            task_id='velaw_user_import_logs_add_entry_12',
            log="{{ result('velaw_check_user_update_logs') }}",
            message="na",
            severity="Skipped",
            properties={
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "importaction": "update",
                "status": "Skipped",
                "details": "User already disabled in Replicon",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        def get_time_off_approval_path(dag_run):
            timeoff_approval_path_list = list(filter(
                lambda x: x['mapper'] == 'Yes' and x["type"] == "TimeOff Approval Path", velaw_user_import_mapper))

            def get_location():
                departments = ("Vinson & Elkins|Vinson & Elkins LLP|Global Document Specialists",
                               "Vinson & Elkins|Vinson & Elkins LLP|Billing Coordinators")
                if dag_run.conf['countryisocode'] == "US":
                    if (dag_run.conf['department'] in departments and dag_run.conf['jobfamilies'] == "Paralegal") or (dag_run.conf['flsastatus'] == "Exempt" and dag_run.conf['persontype'] == "Administrative"):
                        return "All"
                return dag_run.conf['location']

            return next(iter(filter(lambda x: x["mapper"] == "Yes" and x["type"] == "TimeOff Approval Path"
                                    and x["employee_type"] == (dag_run.conf['department'] if (dag_run.conf['countryisocode'] == "US" and dag_run.conf['department'] in ("Vinson & Elkins|Vinson & Elkins LLP|Global Document Specialists", "Vinson & Elkins|Vinson & Elkins LLP|Billing Coordinators")) else ("All" if dag_run.conf['countryisocode'] == "US" else "All"))
                                    and x["country_code"] == dag_run.conf['countryisocode']
                                    and x["location"] == get_location()
                                    and x["person_type"] == dag_run.conf['persontype']
                                    and x["flsa"] == ("All" if (dag_run.conf['countryisocode'] == "US" and dag_run.conf['department'] in ("Vinson & Elkins|Vinson & Elkins LLP|Global Document Specialists", "Vinson & Elkins|Vinson & Elkins LLP|Billing Coordinators")) else ("All" if dag_run.conf['jobfamilies'] == "Paralegal" else dag_run.conf['flsastatus']) if dag_run.conf['countryisocode'] == "US" else dag_run.conf['flsastatus'])
                                    and x["job_code"] == ("Paralegal" if (dag_run.conf['countryisocode'] == "US" and dag_run.conf['jobfamilies'] == "Paralegal") else "All excluding Paralegal" if dag_run.conf['countryisocode'] == "US" else "All"),
                                    timeoff_approval_path_list)), {})

        velaw_user_import_mapper_search_entries_time_off_approval_path_14 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_time_off_approval_path_14',
            python_callable=get_time_off_approval_path
        )

        def get_timesheet_approval_path(dag_run):
            timesheet_approval_path_list = list(filter(
                lambda x: x['mapper'] == 'Yes' and x["type"] == "Timesheet Approval Path", velaw_user_import_mapper))

            def get_location():
                departments = ("Vinson & Elkins|Vinson & Elkins LLP|Global Document Specialists",
                               "Vinson & Elkins|Vinson & Elkins LLP|Billing Coordinators")
                if dag_run.conf['countryisocode'] == "US":
                    if (dag_run.conf['department'] in departments and dag_run.conf['jobfamilies'] == "Paralegal") or (dag_run.conf['flsastatus'] == "Exempt" and dag_run.conf['persontype'] == "Administrative"):
                        return "All"
                return dag_run.conf['location']

            return next(iter(filter(lambda x: x["mapper"] == "Yes" and x["type"] == "Timesheet Approval Path"
                                    and x["employee_type"] == ("All" if dag_run.conf['countryisocode'] == "US" else ("All" if dag_run.conf['department'] not in ("Vinson & Elkins|Vinson & Elkins LLP|Global Document Specialists", "Vinson & Elkins|Vinson & Elkins LLP|Billing Coordinators") else ("All" if dag_run.conf['jobfamilies'] == "Paralegal" else dag_run.conf['department'])))
                                    and x["country_code"] == dag_run.conf['countryisocode']
                                    and x["location"] == get_location()
                                    and x["flsa"] == dag_run.conf['flsastatus']
                                    and x["job_code"] == ("Paralegal" if dag_run.conf['jobfamilies'] == "Paralegal" else "All excluding Paralegal"), timesheet_approval_path_list)), {})

        velaw_user_import_mapper_search_entries_timesheet_approval_path_15 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timesheet_approval_path_15',
            python_callable=get_timesheet_approval_path
        )

        if_userdetails_isenabled_is_not_true_rehire_16 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_rehire_16',
            test=lambda dag_run: rail.result('bulk_get_users3_9') and not rail.result('bulk_get_users3_9')[
                0]['userDetails']['isEnabled'] and dag_run.conf['enabled'] == 'True',
            yes_task="enable_login_17",
            no_task="if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_21",
        )

        enable_login_17 = rail.RepliconServiceOperator(
            task_id='enable_login_17',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        date_split_start_date_18 = rail.EmptyOperator(
            task_id='date_split_start_date_18',
        )

        update_employment_date_range_19 = rail.RepliconServiceOperator(
            task_id='update_employment_date_range_19',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": dag_run.conf['startdate'].split('/')[2],
                        "month": dag_run.conf['startdate'].split('/')[0],
                        "day":  dag_run.conf['startdate'].split('/')[1]
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        update_variable_20 = rail.SetVariableOperator(
            task_id='update_variable_20',
            append=False,
            name='{{ result("declare_variable_4").name }}',
            value="Yes"
        )

        if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_21 = rail.IfOperator(
            task_id='if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_21',
            test=lambda dag_run: dag_run.conf['firstname'] and rail.result('bulk_get_users3_9') and rail.result(
                'bulk_get_users3_9')[0]['userDetails']['firstName'].lower() != dag_run.conf['firstname'].lower(),
            yes_task="update_first_name_22",
            no_task="if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_23",
        )

        update_first_name_22 = rail.RepliconServiceOperator(
            task_id='update_first_name_22',
            endpoint="/services/userService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_23 = rail.IfOperator(
            task_id='if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_23',
            test=lambda dag_run: dag_run.conf['lastname'] and rail.result('bulk_get_users3_9') and rail.result(
                'bulk_get_users3_9')[0]['userDetails']['lastName'].lower() != dag_run.conf['lastname'].lower(),
            yes_task="update_last_name_24",
            no_task="if_request_employeeid_present_dataworkato_servicereceive_requestrequestlastnamedowncase_25",
        )

        update_last_name_24 = rail.RepliconServiceOperator(
            task_id='update_last_name_24',
            endpoint="/services/userService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        if_request_employeeid_present_dataworkato_servicereceive_requestrequestlastnamedowncase_25 = rail.IfOperator(
            task_id='if_request_employeeid_present_dataworkato_servicereceive_requestrequestlastnamedowncase_25',
            test=lambda dag_run: dag_run.conf['employeeid'] and rail.result('bulk_get_users3_9') and rail.result(
                'bulk_get_users3_9')[0]['userDetails']['employeeId'] != dag_run.conf['employeeid'],
            yes_task="update_employee_i_d_26",
            no_task="if_request_email_present_27",
        )

        update_employee_i_d_26 = rail.RepliconServiceOperator(
            task_id='update_employee_i_d_26',
            endpoint="/services/userService1.svc/UpdateEmployeeId",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeId": "{{ dag_run.conf.employeeid }}"
            }
        )

        if_request_email_present_27 = rail.IfOperator(
            task_id='if_request_email_present_27',
            test=lambda dag_run: dag_run.conf['email'] and dag_run.conf['email'].lower(
            ) != rail.result('bulk_get_users3_9')[0]['userDetails']['emailAddress'].lower(),
            yes_task="update_email_28",
            no_task="invoke_custom_ruby_code_29",
        )

        update_email_28 = rail.RepliconServiceOperator(
            task_id='update_email_28',
            endpoint="/services/userService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.email }}"
            }
        )

        invoke_custom_ruby_code_29 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_29',
            python_callable=lambda: {
                "jobcode": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_9')[0]['userDetails']['customFieldValues'], 'customField.displayText', "Job Code", 'text'),
                "jobtitle": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_9')[0]['userDetails']['customFieldValues'], 'customField.displayText', "Job Title", 'text'),
                "flsastatus": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_9')[0]['userDetails']['customFieldValues'], 'customField.displayText', "FLSA Status", 'text'),
                "assignmentcategory": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_9')[0]['userDetails']['customFieldValues'], 'customField.displayText', "Assignment Category", 'text'),
                "countryisocode": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_9')[0]['userDetails']['customFieldValues'], 'customField.displayText', "Country ISO Code", 'text'),
                "persontype": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_9')[0]['userDetails']['customFieldValues'], 'customField.displayText', "Person Type", 'text'),
                "legalemployer": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_9')[0]['userDetails']['customFieldValues'], 'customField.displayText', "Legal Employer", 'text')
            }
        )

        declare_list_30 = rail.SetVariableOperator(
            task_id='declare_list_30',
            append=False,
            name='customfieldvalues',
            value=[]
        )

        if_request_jobcode_present_31 = rail.IfOperator(
            task_id='if_request_jobcode_present_31',
            test=lambda dag_run: dag_run.conf['jobcode'] and rail.result('invoke_custom_ruby_code_29') and rail.result('invoke_custom_ruby_code_29')[
                'jobcode'] and dag_run.conf['jobcode'].lower() != rail.result('invoke_custom_ruby_code_29')['jobcode'].lower(),
            yes_task="insert_to_list_32",
            no_task="if_request_jobtitle_present_34",
        )

        insert_to_list_32 = rail.SetVariableOperator(
            task_id='insert_to_list_32',
            append=True,
            name='{{ result("declare_list_30").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.jobcodeudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": null,
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.jobcodeudfvalueuri }}",
                    "name": null
                }
            }
        )

        update_variable_33 = rail.SetVariableOperator(
            task_id='update_variable_33',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value="Yes"
        )

        if_request_jobtitle_present_34 = rail.IfOperator(
            task_id='if_request_jobtitle_present_34',
            test=lambda dag_run: dag_run.conf['jobtitle'] and rail.result('invoke_custom_ruby_code_29') and rail.result('invoke_custom_ruby_code_29')[
                'jobtitle'] and dag_run.conf['jobtitle'].lower() != rail.result('invoke_custom_ruby_code_29')['jobtitle'].lower(),
            yes_task="insert_to_list_35",
            no_task="if_request_flsastatus_present_36",
        )

        insert_to_list_35 = rail.SetVariableOperator(
            task_id='insert_to_list_35',
            append=True,
            name='{{ result("declare_list_30").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.jobtitleudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": null,
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.jobtitleudfvalueuri }}",
                    "name": null
                }
            }
        )

        if_request_flsastatus_present_36 = rail.IfOperator(
            task_id='if_request_flsastatus_present_36',
            test=lambda dag_run: dag_run.conf['flsastatus'] and rail.result('invoke_custom_ruby_code_29') and rail.result('invoke_custom_ruby_code_29')[
                'flsastatus'] and dag_run.conf['flsastatus'].lower() != rail.result('invoke_custom_ruby_code_29')['flsastatus'].lower(),
            yes_task="insert_to_list_37",
            no_task="if_request_assignmentcategory_present_42",
        )

        insert_to_list_37 = rail.SetVariableOperator(
            task_id='insert_to_list_37',
            append=True,
            name='{{ result("declare_list_30").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.flsastatusudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": null,
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.flsastatusudfvalueuri }}",
                    "name": null
                }
            }
        )

        update_variable_38 = rail.SetVariableOperator(
            task_id='update_variable_38',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value="Yes"
        )

        update_variable_39 = rail.SetVariableOperator(
            task_id='update_variable_39',
            append=False,
            name='{{ result("declare_variable_4").name }}',
            value="Yes"
        )

        update_variable_40 = rail.SetVariableOperator(
            task_id='update_variable_40',
            append=False,
            name='{{ result("declare_variable_7").name }}',
            value="Yes"
        )

        update_variable_41 = rail.SetVariableOperator(
            task_id='update_variable_41',
            append=False,
            name='{{ result("declare_variable_6").name }}',
            value="Yes"
        )

        if_request_assignmentcategory_present_42 = rail.IfOperator(
            task_id='if_request_assignmentcategory_present_42',
            test=lambda dag_run: dag_run.conf['assignmentcategory'] and rail.result('invoke_custom_ruby_code_29') and rail.result('invoke_custom_ruby_code_29')[
                'assignmentcategory'] and dag_run.conf['assignmentcategory'].lower() != rail.result('invoke_custom_ruby_code_29')['assignmentcategory'].lower(),
            yes_task="insert_to_list_43",
            no_task="if_request_countryisocode_present_45",
        )

        insert_to_list_43 = rail.SetVariableOperator(
            task_id='insert_to_list_43',
            append=True,
            name='{{ result("declare_list_30").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.assignmentcategoryudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": null,
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.assignmentcategoryudfvalueuri }}",
                    "name": null
                }
            }
        )

        update_variable_44 = rail.SetVariableOperator(
            task_id='update_variable_44',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value="Yes"
        )

        if_request_countryisocode_present_45 = rail.IfOperator(
            task_id='if_request_countryisocode_present_45',
            test=lambda dag_run: dag_run.conf['countryisocode'] and rail.result('invoke_custom_ruby_code_29') and rail.result('invoke_custom_ruby_code_29')[
                'countryisocode'] and dag_run.conf['countryisocode'].lower() != rail.result('invoke_custom_ruby_code_29')['countryisocode'].lower(),
            yes_task="insert_to_list_46",
            no_task="if_request_persontype_present_52",
        )

        insert_to_list_46 = rail.SetVariableOperator(
            task_id='insert_to_list_46',
            append=True,
            name='{{ result("declare_list_30").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.countryisocodeudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": null,
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.countryisocodeudfvalueuri }}",
                    "name": null
                }
            }
        )

        update_variable_47 = rail.SetVariableOperator(
            task_id='update_variable_47',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value="Yes"
        )

        update_variable_48 = rail.SetVariableOperator(
            task_id='update_variable_48',
            append=False,
            name='{{ result("declare_variable_4").name }}',
            value="Yes"
        )

        update_variable_49 = rail.SetVariableOperator(
            task_id='update_variable_49',
            append=False,
            name='{{ result("declare_variable_5").name }}',
            value="Yes"
        )

        update_variable_50 = rail.SetVariableOperator(
            task_id='update_variable_50',
            append=False,
            name='{{ result("declare_variable_6").name }}',
            value="Yes"
        )

        update_variable_51 = rail.SetVariableOperator(
            task_id='update_variable_51',
            append=False,
            name='{{ result("declare_variable_7").name }}',
            value="Yes"
        )

        if_request_persontype_present_52 = rail.IfOperator(
            task_id='if_request_persontype_present_52',
            test=lambda dag_run: dag_run.conf['persontype'] and rail.result('invoke_custom_ruby_code_29') and rail.result('invoke_custom_ruby_code_29')[
                'persontype'] and dag_run.conf['persontype'].lower() != rail.result('invoke_custom_ruby_code_29')['persontype'].lower(),
            yes_task="insert_to_list_53",
            no_task="if_request_legalemployer_present_56",
        )

        insert_to_list_53 = rail.SetVariableOperator(
            task_id='insert_to_list_53',
            append=True,
            name='{{ result("declare_list_30").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.persontypeudfuri }}",
                    "name": null,
                    "groupUri": null
                },
                "date": null,
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.persontypeudfvalueuri }}",
                    "name": null
                }
            }
        )

        update_variable_54 = rail.SetVariableOperator(
            task_id='update_variable_54',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value="Yes"
        )

        update_variable_55 = rail.SetVariableOperator(
            task_id='update_variable_55',
            append=False,
            name='{{ result("declare_variable_6").name }}',
            value="Yes"
        )

        if_request_legalemployer_present_56 = rail.IfOperator(
            task_id='if_request_legalemployer_present_56',
            test=lambda dag_run: dag_run.conf['legalemployer'] and rail.result('invoke_custom_ruby_code_29') and rail.result('invoke_custom_ruby_code_29')[
                'legalemployer'] and dag_run.conf['legalemployer'].lower() != rail.result('invoke_custom_ruby_code_29')['legalemployer'].lower(),
            yes_task="insert_to_list_57",
            no_task="get_customfield_values",
        )

        insert_to_list_57 = rail.SetVariableOperator(
            task_id='insert_to_list_57',
            append=True,
            name='{{ result("declare_list_30").name }}',
            value={
                "customField": {
                    "uri": "{{ dag_run.conf.legalemployerudfvalue }}",
                    "name": null,
                    "groupUri": null
                },
                "date": null,
                "dropDownOption": {
                    "uri": "{{ dag_run.conf.legalemployerudfvalueuri }}",
                    "name": null
                }
            }
        )

        get_customfield_values = rail.GetVariableOperator(
            task_id='get_customfield_values',
            name='customfieldvalues'
        )

        log_custom_fieldto_apply_58 = rail.PythonOperator(
            task_id='log_custom_fieldto_apply_58',
            python_callable=lambda: json.loads(json.dumps(rail.result('get_customfield_values')['value'], ensure_ascii=False)
                                               .replace('"date":{}', '"date":null')
                                               .replace('{"year":null,"month":null,"day":null}', '{}')) if rail.result('get_customfield_values')['value'] else null
        )

        if_log_custom_fieldto_apply_58_present_59 = rail.IfOperator(
            task_id='if_log_custom_fieldto_apply_58_present_59',
            test=lambda: rail.result('log_custom_fieldto_apply_58'),
            yes_task="update_custom_fields_60",
            no_task="if_request_payratesamount_present_61",
        )

        update_custom_fields_60 = rail.RepliconServiceOperator(
            task_id='update_custom_fields_60',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "customFieldValuesToApply": rail.result('log_custom_fieldto_apply_58'),
                    "projectRolesToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_payratesamount_present_61 = rail.IfOperator(
            task_id='if_request_payratesamount_present_61',
            test='''{{ dag_run.conf.payratesamount | is_truthy }}''',
            yes_task="if_payrollrateschedule_to_json_contains_urn_62",
            no_task="if_request_hourlycostamount_present_67",
        )

        if_payrollrateschedule_to_json_contains_urn_62 = rail.IfOperator(
            task_id='if_payrollrateschedule_to_json_contains_urn_62',
            test=lambda: rail.result('bulk_get_users3_9') and rail.result(
                'bulk_get_users3_9')[0]['payrollRateSchedule'][0]['uri'],
            yes_task="invoke_custom_ruby_code_64",
            no_task="if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_65",
        )

        def get_day_diff(effectivedate):
            day_diff = (datetime(
                effectivedate['year'], effectivedate['month'], effectivedate['day'])-datetime.today()).days
            return day_diff if day_diff > 0 else 0

        invoke_custom_ruby_code_64 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_64',
            python_callable=lambda: list(map(lambda item: {
                "effectiveDate": (str(item['effectiveDate']['day']) + "/" + str(item['effectiveDate']['month']) + "/" + str(item['effectiveDate']['year'])) if item['effectiveDate'] else (str(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']['day']) + "/" + str(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']['month']) + "/" + str(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']['year'])),
                "displayText": item['hourlyRate']['amount'],
                "daydiff": get_day_diff(item['effectiveDate']) if item['effectiveDate'] else get_day_diff(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate'])
            }, rail.result('bulk_get_users3_9')[0]['payrollRateSchedule']))
        )

        if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_65 = rail.IfOperator(
            task_id='if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_65',
            test=lambda dag_run: rail.result('invoke_custom_ruby_code_64') and not rail.result('invoke_custom_ruby_code_64')[0]['displayText'] or float(
                rail.result('invoke_custom_ruby_code_64')[0]['displayText']) != float(dag_run.conf['payratesamount']),
            yes_task="update_user_payroll_rate_schedule_over_date_range_66",
            no_task="if_request_hourlycostamount_present_67",
        )

        update_user_payroll_rate_schedule_over_date_range_66 = rail.RepliconServiceOperator(
            task_id='update_user_payroll_rate_schedule_over_date_range_66',
            endpoint="/services/PayrollService1.svc/UpdateUserPayrollRateScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "hourlyRate": {
                    "amount": "{{ dag_run.conf.payratesamount }}",
                    "currencyUri": "{{ dag_run.conf.payratescurrencyuri }}"
                },
                "dateRange": {
                    "startDate": {
                        "year": date.today().year,
                        "month":  date.today().month,
                        "day": date.today().day
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_hourlycostamount_present_67 = rail.IfOperator(
            task_id='if_request_hourlycostamount_present_67',
            test='''{{ dag_run.conf.hourlycostamount | is_truthy }}''',
            yes_task="if_costrateschedule_to_json_contains_urn_68",
            no_task="if_request_defaultbillingrateamount_present_73",
        )

        if_costrateschedule_to_json_contains_urn_68 = rail.IfOperator(
            task_id='if_costrateschedule_to_json_contains_urn_68',
            test=lambda: rail.result('bulk_get_users3_9') and rail.result(
                'bulk_get_users3_9')[0]['costRateSchedule'][0]['uri'],
            yes_task="parse_json_costrate_schedule_69",
            no_task="if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_71",
        )

        parse_json_costrate_schedule_69 = rail.PythonOperator(
            task_id='parse_json_costrate_schedule_69',
            python_callable=lambda: rail.result('bulk_get_users3_9')[
                0]['costRateSchedule']
        )

        invoke_custom_ruby_code_70 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_70',
            python_callable=lambda: list(map(lambda item: {
                "effectiveDate": (str(item['effectiveDate']['day']) + "/" + str(item['effectiveDate']['month']) + "/" + str(item['effectiveDate']['year'])) if item['effectiveDate'] else (str(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']['day']) + "/" + str(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']['month']) + "/" + str(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']['year'])),
                "displayText": item['hourlyRate']['amount'],
                "daydiff": get_day_diff(item['effectiveDate']) if item['effectiveDate'] else get_day_diff(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate'])
            }, rail.result('parse_json_costrate_schedule_69')))
        )

        if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_71 = rail.IfOperator(
            task_id='if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_71',
            test=lambda dag_run: rail.result('invoke_custom_ruby_code_70') and not rail.result('invoke_custom_ruby_code_70')[0]['displayText'] or float(
                rail.result('invoke_custom_ruby_code_70')[0]['displayText']) != float(dag_run.conf['hourlycostamount']),
            yes_task="update_user_cost_rate_schedule_over_date_range_72",
            no_task="if_request_defaultbillingrateamount_present_73",
        )

        update_user_cost_rate_schedule_over_date_range_72 = rail.RepliconServiceOperator(
            task_id='update_user_cost_rate_schedule_over_date_range_72',
            endpoint="/services/ResourceService1.svc/UpdateUserCostRateScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "hourlyRate": {
                    "amount": "{{ dag_run.conf.hourlycostamount }}",
                    "currencyUri": "{{ dag_run.conf.hourlycostcurrencyuri }}"
                },
                "dateRange": {
                    "startDate": {
                        "year": date.today().year,
                        "month":  date.today().month,
                        "day": date.today().day
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_defaultbillingrateamount_present_73 = rail.IfOperator(
            task_id='if_request_defaultbillingrateamount_present_73',
            test=lambda dag_run: dag_run.conf['defaultbillingrateamount'] and float(dag_run.conf['defaultbillingrateamount']) != float(
                rail.result('bulk_get_users3_9')[0]['defaultBillingRate']['effectiveBillingRate']['value']['amount']),
            yes_task="update_user_specific_billing_rate_amount_74",
            no_task="get_scheduleupdate",
        )

        update_user_specific_billing_rate_amount_74 = rail.RepliconServiceOperator(
            task_id='update_user_specific_billing_rate_amount_74',
            endpoint="/services/Billing/BillingRateService1.svc/UpdateUserSpecificBillingRateAmount",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "rate": {
                    "amount": "{{ dag_run.conf.defaultbillingrateamount }}",
                    "currencyUri": "{{ dag_run.conf.defaultbillingratecurrencyuri }}"
                }
            }
        )

        get_scheduleupdate = rail.GetVariableOperator(
            task_id='get_scheduleupdate',
            name='scheduleupdate',
        )

        if_declare_variable_4_value_equals_to_yes_75 = rail.IfOperator(
            task_id='if_declare_variable_4_value_equals_to_yes_75',
            test=lambda: rail.result('get_scheduleupdate') and rail.result(
                'get_scheduleupdate')['value'] == 'Yes',
            yes_task="if_schedulepolicies_to_json_contains_urn_76",
            no_task="get_effective_user_group_membership_81",
        )

        if_schedulepolicies_to_json_contains_urn_76 = rail.IfOperator(
            task_id='if_schedulepolicies_to_json_contains_urn_76',
            test=lambda: rail.result('bulk_get_users3_9') and rail.result('bulk_get_users3_9')[0]['schedulePolicies'] and rail.result(
                'bulk_get_users3_9')[0]['schedulePolicies'][0]['uri'],
            yes_task="parse_json_office_schedule_77",
            no_task="if_schedulepolicies_uri_blank_dataworkato_servicereceive_requestrequestinitialschedulename_79",
        )

        parse_json_office_schedule_77 = rail.PythonOperator(
            task_id='parse_json_office_schedule_77',
            python_callable=lambda: rail.result('bulk_get_users3_9') and rail.result('bulk_get_users3_9')[
                0]['schedulePolicies']
        )

        def get_invoke_custom_ruby_code_78():
            current_office_schedule = list(map(lambda item: {
                "effectiveDate": (str(item['effectiveDate']['day']) + "/" + str(item['effectiveDate']['month']) + "/" + str(item['effectiveDate']['year'])) if item['effectiveDate'] else (str(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']['day']) + "/" + str(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']['month']) + "/" + str(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']['year'])),
                "displayText": item['officeSchedule']['displayText'],
                "uri": item['officeSchedule']['uri'],
                "scheduleTypeUri": item['scheduleTypeUri'],
                "daydiff": get_day_diff(item['effectiveDate']) if item['effectiveDate'] else get_day_diff(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate'])
            }, rail.result('parse_json_office_schedule_77')))
            current_office_schedule = sorted(current_office_schedule, key=lambda x:x['daydiff'])[0] if current_office_schedule else []
            return current_office_schedule

        invoke_custom_ruby_code_78 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_78',
            python_callable=get_invoke_custom_ruby_code_78
        )

        if_schedulepolicies_uri_blank_dataworkato_servicereceive_requestrequestinitialschedulename_79 = rail.IfOperator(
            task_id='if_schedulepolicies_uri_blank_dataworkato_servicereceive_requestrequestinitialschedulename_79',
            test=lambda dag_run: bool(rail.result('invoke_custom_ruby_code_78') and not rail.result('invoke_custom_ruby_code_78')[
                'uri'] or dag_run.conf['officescheduleuri'] != rail.result('invoke_custom_ruby_code_78')['uri']),
            yes_task="updateofficeschedule_80",
            no_task="get_effective_user_group_membership_81",
        )

        updateofficeschedule_80 = rail.RepliconServiceOperator(
            task_id='updateofficeschedule_80',
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
                                        "officeScheduleUri": "{{ dag_run.conf.officescheduleuri }}",
                                        "name": null,
                                        "officeSchedule": {
                                            "officeScheduleUri": "{{ dag_run.conf.officescheduleuri }}",
                                            "name": null
                                        },
                                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                                    },
                                    "effectiveDate": {
                                        "year": date.today().year,
                                        "month":  date.today().month,
                                        "day": date.today().day
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "projectRolesToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        get_effective_user_group_membership_81 = rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership_81',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": null
            }
        )

        if_request_department_present_82 = rail.IfOperator(
            task_id='if_request_department_present_82',
            test=lambda dag_run: dag_run.conf['department'] and
            (not rail.result('get_effective_user_group_membership_81')['departments'] or dag_run.conf['departmenturi'] != rail.result(
                'get_effective_user_group_membership_81')['departments'][0]['department']['department']['uri']),
            yes_task="update_department_group_83",
            no_task="if_request_location_present_86",
        )

        update_department_group_83 = rail.RepliconServiceOperator(
            task_id='update_department_group_83',
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
                                        "year": date.today().year,
                                        "month":  date.today().month,
                                        "day": date.today().day
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "projectRolesToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        update_variable_84 = rail.SetVariableOperator(
            task_id='update_variable_84',
            append=False,
            name='{{ result("declare_variable_7").name }}',
            value="Yes"
        )

        update_variable_85 = rail.SetVariableOperator(
            task_id='update_variable_85',
            append=False,
            name='{{ result("declare_variable_6").name }}',
            value="Yes"
        )

        if_request_location_present_86 = rail.IfOperator(
            task_id='if_request_location_present_86',
            test=lambda dag_run: dag_run.conf['location'] and rail.result('get_effective_user_group_membership_81') and rail.result('get_effective_user_group_membership_81')[
                'locations'] and dag_run.conf['locationuri'] != rail.result('get_effective_user_group_membership_81')['locations'][0]['location']['location']['uri'],
            yes_task="update_location_group_87",
            no_task="if_request_jobfamilies_present_92",
        )

        update_location_group_87 = rail.RepliconServiceOperator(
            task_id='update_location_group_87',
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
                                        "year": date.today().year,
                                        "month":  date.today().month,
                                        "day": date.today().day
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "projectRolesToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        update_variable_88 = rail.SetVariableOperator(
            task_id='update_variable_88',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value="Yes"
        )

        update_variable_89 = rail.SetVariableOperator(
            task_id='update_variable_89',
            append=False,
            name='{{ result("declare_variable_5").name }}',
            value="Yes"
        )

        update_variable_90 = rail.SetVariableOperator(
            task_id='update_variable_90',
            append=False,
            name='{{ result("declare_variable_6").name }}',
            value="Yes"
        )

        update_variable_91 = rail.SetVariableOperator(
            task_id='update_variable_91',
            append=False,
            name='{{ result("declare_variable_7").name }}',
            value="Yes"
        )

        if_request_jobfamilies_present_92 = rail.IfOperator(
            task_id='if_request_jobfamilies_present_92',
            test=lambda dag_run: dag_run.conf['jobfamilies'] and rail.result('get_effective_user_group_membership_81')[
                'costCenters'] and dag_run.conf['jobfamiliesuri'] != rail.result('get_effective_user_group_membership_81')['costCenters'][0]['costCenter']['costCenter']['uri'],
            yes_task="update_cost_center_group_93",
            no_task="if_request_paytype_present_96",
        )

        update_cost_center_group_93 = rail.RepliconServiceOperator(
            task_id='update_cost_center_group_93',
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
                                        "uri": "{{ dag_run.conf.jobfamiliesuri }}",
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year": date.today().year,
                                        "month":  date.today().month,
                                        "day": date.today().day
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "projectRolesToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        update_variable_94 = rail.SetVariableOperator(
            task_id='update_variable_94',
            append=False,
            name='{{ result("declare_variable_7").name }}',
            value="Yes"
        )

        update_variable_95 = rail.SetVariableOperator(
            task_id='update_variable_95',
            append=False,
            name='{{ result("declare_variable_6").name }}',
            value="Yes"
        )

        if_request_paytype_present_96 = rail.IfOperator(
            task_id='if_request_paytype_present_96',
            test=lambda dag_run: dag_run.conf['paytype'] and rail.result('get_effective_user_group_membership_81')[
                'divisions'] and dag_run.conf['paytypeuri'] != rail.result('get_effective_user_group_membership_81')['divisions'][0]['division']['division']['uri'],
            yes_task="update_division_group_97",
            no_task="if_request_employeetype_present_98",
        )

        update_division_group_97 = rail.RepliconServiceOperator(
            task_id='update_division_group_97',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "divisionScheduleToApply": {
                        "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementDivisionSchedule": [],
                        "updateDivisionScheduleOverDateRange": {
                            "replacementDivisionScheduleEntries": [
                                {
                                    "division": {
                                        "uri": "{{ dag_run.conf.paytypeuri }}",
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year": date.today().year,
                                        "month":  date.today().month,
                                        "day": date.today().day
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "projectRolesToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_employeetype_present_98 = rail.IfOperator(
            task_id='if_request_employeetype_present_98',
            test=lambda dag_run: dag_run.conf['employeetype'] and rail.result('get_effective_user_group_membership_81')['employeeTypes'] and dag_run.conf['employeetypeuri'] != rail.result(
                'get_effective_user_group_membership_81')['employeeTypes'][0]['employeeType']['employeeType']['uri'],
            yes_task="update_employeetype_group_99",
            no_task="if_request_supervisorloginname_present_131",
        )

        update_employeetype_group_99 = rail.RepliconServiceOperator(
            task_id='update_employeetype_group_99',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "employeeTypeGroupScheduleToApply": {
                        "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementEmployeeTypeGroupSchedule": [],
                        "updateEmployeeTypeGroupScheduleOverDateRange": {
                            "replacementEmployeeTypeGroupScheduleEntries": [
                                {
                                    "employeeTypeGroup": {
                                        "uri": "{{ dag_run.conf.employeetypeuri }}",
                                        "parent": null,
                                        "name": null,
                                        "parameterCorrelationId": null
                                    },
                                    "effectiveDate": {
                                        "year": date.today().year,
                                        "month":  date.today().month,
                                        "day": date.today().day
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "projectRolesToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_payrule_blank_100 = rail.IfOperator(
            task_id='if_request_payrule_blank_100',
            test='''{{ dag_run.conf.payrule | is_falsy }}''',
            yes_task="insert_to_list_101",
            no_task="if_request_payrule_present_102",
        )

        insert_to_list_101 = rail.SetVariableOperator(
            task_id='insert_to_list_101',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Payrule not defined for employee type {{dag_run.conf.employeetype}} in mapper "
            }
        )

        if_request_payrule_present_102 = rail.IfOperator(
            task_id='if_request_payrule_present_102',
            test='''{{ dag_run.conf.payrule | is_truthy }}''',
            yes_task="if_payrulescriptschedule_to_json_contains_urn_103",
            no_task="if_request_timesheettemplate_not_equals_to_donotassign_120",
        )

        if_payrulescriptschedule_to_json_contains_urn_103 = rail.IfOperator(
            task_id='if_payrulescriptschedule_to_json_contains_urn_103',
            test=lambda: rail.result('bulk_get_users3_9') and rail.result('bulk_get_users3_9')[0]['payRuleScriptSchedule'] and rail.result(
                'bulk_get_users3_9')[0]['payRuleScriptSchedule'][0]['payRuleScript']['uri'],
            yes_task="parse_json_payrule_schedule_104",
            no_task="if_schedulepolicies_uri_blank_106",
        )

        parse_json_payrule_schedule_104 = rail.PythonOperator(
            task_id='parse_json_payrule_schedule_104',
            python_callable=lambda: rail.result('bulk_get_users3_9')[
                0]['payRuleScriptSchedule']
        )

        invoke_custom_ruby_code_105 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_105',
            python_callable=lambda: list(map(lambda item: {
                "effectiveDate": (str(item['effectiveDate']['day']) + "/" + str(item['effectiveDate']['month']) + "/" + str(item['effectiveDate']['year'])) if item['effectiveDate'] else (str(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']['day']) + "/" + str(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']['month']) + "/" + str(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate']['year'])),
                "displayText": item['payRuleScript']['displayText'],
                "uri": item['payRuleScript']['uri'],
                "scheduleTypeUri": "NA",
                "daydiff": get_day_diff(item['effectiveDate']) if item['effectiveDate'] else get_day_diff(rail.result('bulk_get_users3_9')[0]['userDetails']['employmentDateRange']['startDate'])
            }, rail.result('parse_json_payrule_schedule_104')))
        )

        if_schedulepolicies_uri_blank_106 = rail.IfOperator(
            task_id='if_schedulepolicies_uri_blank_106',
            test=lambda dag_run: rail.result('invoke_custom_ruby_code_105') and (not rail.result('invoke_custom_ruby_code_105')[
                0]['uri'] or rail.result('invoke_custom_ruby_code_105')[0]['uri'] != dag_run.conf['payruleuri']),
            yes_task="if_request_payruleuri_blank_107",
            no_task="if_request_timesheettemplate_not_equals_to_donotassign_120",
        )

        if_request_payruleuri_blank_107 = rail.IfOperator(
            task_id='if_request_payruleuri_blank_107',
            test='''{{ dag_run.conf.payruleuri | is_falsy }}''',
            yes_task="insert_to_list_108",
            no_task="get_timesheet_for_date2_111",
        )

        insert_to_list_108 = rail.SetVariableOperator(
            task_id='insert_to_list_108',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Payrule {{ dag_run.conf.payrule }} not available in Replicon"
            }
        )

        get_timesheet_for_date2_111 = rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date2_111',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "date": {
                    "year": date.today().year,
                    "month":  date.today().month,
                    "day": date.today().day
                },
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        if_timesheet_uri_present_112 = rail.IfOperator(
            task_id='if_timesheet_uri_present_112',
            test=lambda: rail.result('get_timesheet_for_date2_111')[
                'timesheet']['uri'],
            yes_task="get_timesheet_details_113",
            no_task="if_timesheet_uri_blank_115",
        )

        get_timesheet_details_113 = rail.RepliconServiceOperator(
            task_id='get_timesheet_details_113',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
                "timesheetUri": "{{ result('get_timesheet_for_date2_111').timesheet.uri }}"
            }
        )

        if_timesheet_uri_blank_115 = rail.IfOperator(
            task_id='if_timesheet_uri_blank_115',
            test=lambda: not rail.result('get_timesheet_for_date2_111')[
                'timesheet']['uri'],
            yes_task="update_payrulewith_today_116",
            no_task="update_payrule_119",
        )

        update_payrulewith_today_116 = rail.RepliconServiceOperator(
            task_id='update_payrulewith_today_116',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "payRulesScheduleModifications": {
                        "scheduleEntries": [
                            {
                                "payRuleScript": {
                                    "uri": "{{ dag_run.conf.payruleuri }}",
                                    "name": null
                                },
                                "effectiveDate": {
                                    "year": date.today().year,
                                    "month":  date.today().month,
                                    "day": date.today().day
                                }
                            }
                        ]
                    },
                    "projectRolesToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_list_117 = rail.SetVariableOperator(
            task_id='insert_to_list_117',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Timesheet not generated hence payrule updated with run date"
            }
        )

        update_payrule_119 = rail.RepliconServiceOperator(
            task_id='update_payrule_119',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "payRulesScheduleModifications": {
                        "scheduleEntries": [
                            {
                                "payRuleScript": {
                                    "uri": "{{ dag_run.conf.payruleuri }}",
                                    "name": null
                                },
                                "effectiveDate": {
                                    "year": "{{ result('get_timesheet_details_113').dateRange.startDate.year }}",
                                    "month": "{{ result('get_timesheet_details_113').dateRange.startDate.month }}",
                                    "day": "{{ result('get_timesheet_details_113').dateRange.startDate.day }}"
                                }
                            }
                        ]
                    },
                    "projectRolesToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_timesheettemplate_not_equals_to_donotassign_120 = rail.IfOperator(
            task_id='if_request_timesheettemplate_not_equals_to_donotassign_120',
            test=lambda dag_run: dag_run.conf['timesheettemplate'] != 'Do Not Assign',
            yes_task="if_request_timesheettemplate_blank_121",
            no_task="if_request_timesheettemplate_equals_to_donotassign_129",
        )

        if_request_timesheettemplate_blank_121 = rail.IfOperator(
            task_id='if_request_timesheettemplate_blank_121',
            test=lambda dag_run: not dag_run.conf['timesheettemplate'],
            yes_task="insert_to_list_122",
            no_task="if_request_timesheettemplate_present_123"
        )

        insert_to_list_122 = rail.SetVariableOperator(
            task_id='insert_to_list_122',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Timesheet template not defined for employee type {{ dag_run.conf.employeetype }} in mapper "
            }
        )

        if_request_timesheettemplate_present_123 = rail.IfOperator(
            task_id='if_request_timesheettemplate_present_123',
            test=lambda dag_run: dag_run.conf['timesheettemplate'] and dag_run.conf['timesheettemplate'] != rail.result(
                'bulk_get_users3_9')[0]['timesheetTemplate']['name'],
            yes_task="if_request_timesheettemplateuri_present_124",
            no_task="if_request_timesheettemplate_equals_to_donotassign_129",
        )

        if_request_timesheettemplateuri_present_124 = rail.IfOperator(
            task_id='if_request_timesheettemplateuri_present_124',
            test=lambda dag_run: dag_run.conf['timesheettemplateuri'],
            yes_task="assign_policy_set_to_user_timesheettemplate_125",
            no_task="insert_to_list_127",
        )

        assign_policy_set_to_user_timesheettemplate_125 = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user_timesheettemplate_125',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ dag_run.conf.timesheettemplateuri }}"
            }
        )

        insert_to_list_127 = rail.SetVariableOperator(
            task_id='insert_to_list_127',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Timesheet template {{ dag_run.conf.timesheettemplate }} not available in Replicon"
            }
        )

        if_request_timesheettemplate_equals_to_donotassign_129 = rail.IfOperator(
            task_id='if_request_timesheettemplate_equals_to_donotassign_129',
            test=lambda dag_run: dag_run.conf['timesheettemplate'] == 'Do Not Assign' and rail.result(
                'bulk_get_users3_9') and rail.result('bulk_get_users3_9')[0]['timesheetTemplate']['uri'],
            yes_task="remove_policy_set_assignment_from_user_timesheettemplate_130",
            no_task="if_request_supervisorloginname_present_131",
        )

        remove_policy_set_assignment_from_user_timesheettemplate_130 = rail.RepliconServiceOperator(
            task_id='remove_policy_set_assignment_from_user_timesheettemplate_130',
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('bulk_get_users3_9')[0].timesheetTemplate.uri }}"
            }
        )

        if_request_supervisorloginname_present_131 = rail.IfOperator(
            task_id='if_request_supervisorloginname_present_131',
            test=lambda dag_run: dag_run.conf['supervisorloginname'],
            yes_task="if_request_supervisorloginname_equals_to_dataworkato_servicereceive_requestrequestloginname_132",
            no_task="get_timeoffapprovalpathupdate",
        )

        if_request_supervisorloginname_equals_to_dataworkato_servicereceive_requestrequestloginname_132 = rail.IfOperator(
            task_id='if_request_supervisorloginname_equals_to_dataworkato_servicereceive_requestrequestloginname_132',
            test=lambda dag_run: dag_run.conf['supervisorloginname'] == dag_run.conf['loginname'],
            yes_task="insert_to_list_133",
            no_task="get_supervisor_assignment_detailsforuser_135",
        )

        insert_to_list_133 = rail.SetVariableOperator(
            task_id='insert_to_list_133',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Supervisor not updated  - Supervisor login name is same as User login name"
            }
        )

        get_supervisor_assignment_detailsforuser_135 = rail.RepliconServiceOperator(
            task_id='get_supervisor_assignment_detailsforuser_135',
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "asOfDate": {
                    "year": date.today().year,
                    "month":  date.today().month,
                    "day": date.today().day
                }
            }
        )

        if_user_loginname_blank_136 = rail.IfOperator(
            task_id='if_user_loginname_blank_136',
            test=lambda dag_run: rail.result('get_supervisor_assignment_detailsforuser_135') and
            (not rail.result('get_supervisor_assignment_detailsforuser_135')['supervisor']['user']['loginName'] or rail.result(
                'get_supervisor_assignment_detailsforuser_135')['supervisor']['user']['loginName'].lower() != dag_run.conf['supervisorloginname'].lower()),
            yes_task="if_request_supervisoruri_blank_137",
            no_task="get_timeoffapprovalpathupdate"
        )

        if_request_supervisoruri_blank_137 = rail.IfOperator(
            task_id='if_request_supervisoruri_blank_137',
            test=lambda dag_run: not dag_run.conf['supervisoruri'],
            yes_task="velaw_supervisor_check_add_entry_138",
            no_task="get_assigned_permission_sets_for_userfor_supervisor_140",
        )

        velaw_supervisor_check_add_entry_138 = rail.WriteLogOperator(
            task_id='velaw_supervisor_check_add_entry_138',
            log="{{ result('velaw_supervisor_check_user_update_logs') }}",
            message="na",
            severity="pending",
            properties={
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "user_uri": "{{ dag_run.conf.useruri }}",
                "supervisorloginname": "{{ dag_run.conf.supervisorloginname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "importaction": "Update",
                "status": "pending",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        get_assigned_permission_sets_for_userfor_supervisor_140 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_userfor_supervisor_140',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ dag_run.conf.supervisoruri }}"
            }
        )

        if_request_supervisorstatus_equals_to_enabled_141 = rail.IfOperator(
            task_id='if_request_supervisorstatus_equals_to_enabled_141',
            test='''{{ dag_run.conf.supervisorstatus == 'Enabled' }}''',
            yes_task="invoke_custom_ruby_code_142",
            no_task="velaw_supervisor_check_add_entry_150",
        )

        invoke_custom_ruby_code_142 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_142',
            python_callable=lambda: {
                "supervisor": rail.find_first_by_attr_and_get_attr(rail.result('get_assigned_permission_sets_for_userfor_supervisor_140'), 'displayText', "*Gen3 - Supervisor", 'permissionSet'),
                "enduser": rail.find_first_by_attr_and_get_attr(rail.result('get_assigned_permission_sets_for_userfor_supervisor_140'), 'displayText', "*Gen3 - Project Resource with reports & Substitute User", 'permissionSet')
            }
        )

        if_output_supervisor_blank_143 = rail.IfOperator(
            task_id='if_output_supervisor_blank_143',
            test=lambda: rail.result('invoke_custom_ruby_code_142') and not rail.result(
                'invoke_custom_ruby_code_142')['supervisor'],
            yes_task="assign_supervsior_permission_set_to_user_gen3_supervisor_144",
            no_task="if_output_enduser_blank_145",
        )

        assign_supervsior_permission_set_to_user_gen3_supervisor_144 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_gen3_supervisor_144',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ dag_run.conf.supervisoruri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        if_output_enduser_blank_145 = rail.IfOperator(
            task_id='if_output_enduser_blank_145',
            test=lambda: rail.result('invoke_custom_ruby_code_142') and not rail.result(
                'invoke_custom_ruby_code_142')['enduser'],
            yes_task="assign_supervsior_permission_set_to_user_gen3_project_resourcewithreports_substitute_user_146",
            no_task="date_split_supervisor_effective_date_147",
        )

        assign_supervsior_permission_set_to_user_gen3_project_resourcewithreports_substitute_user_146 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_gen3_project_resourcewithreports_substitute_user_146',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ dag_run.conf.supervisoruri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorendusepermissionuri }}"
            }
        )

        def get_supervisor_date():
            today = datetime.now()
            if today.weekday() == 0:
                datestr = (today - timedelta(days=1)).strftime("%d/%m/%Y")
            elif today.weekday() == 1:
                datestr = (today - timedelta(days=2)).strftime("%d/%m/%Y")
            elif today.weekday() == 2:
                datestr = (today - timedelta(days=3)).strftime("%d/%m/%Y")
            elif today.weekday() == 3:
                datestr = (today - timedelta(days=4)).strftime("%d/%m/%Y")
            elif today.weekday() == 4:
                datestr = (today - timedelta(days=5)).strftime("%d/%m/%Y")
            elif today.weekday() == 5:
                datestr = (today - timedelta(days=6)).strftime("%d/%m/%Y")
            else:
                datestr = today.strftime("%d/%m/%Y")
            return datestr
        date_split_supervisor_effective_date_147 = rail.PythonOperator(
            task_id='date_split_supervisor_effective_date_147',
            python_callable=get_supervisor_date
        )

        update_supervisor_assignment_schedule_over_date_range_148 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_148',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": dag_run.conf['supervisoruri'],
                "dateRange": {
                    "startDate": {
                        "year": rail.result('date_split_supervisor_effective_date_147').split('/')[2],
                        "month": rail.result('date_split_supervisor_effective_date_147').split('/')[1],
                        "day": rail.result('date_split_supervisor_effective_date_147').split('/')[0]
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        velaw_supervisor_check_add_entry_150 = rail.WriteLogOperator(
            task_id='velaw_supervisor_check_add_entry_150',
            log="{{ result('velaw_supervisor_check_user_update_logs') }}",
            message="na",
            severity="pending",
            properties={
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "user_uri": "{{ dag_run.conf.useruri }}",
                "supervisorloginname": "{{ dag_run.conf.supervisorloginname }}",
                "importaction": "Update",
                "status": "pending",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        get_timeoffapprovalpathupdate = rail.GetVariableOperator(
            task_id='get_timeoffapprovalpathupdate',
            name='timeoffapprovalpathupdate'
        )

        if_declare_variable_6_value_equals_to_yes_151 = rail.IfOperator(
            task_id='if_declare_variable_6_value_equals_to_yes_151',
            test=lambda: rail.result('get_timeoffapprovalpathupdate') and rail.result(
                'get_timeoffapprovalpathupdate')['value'] == 'Yes',
            yes_task="if_entry_col10_blank_152",
            no_task="get_timesheetapprovalpathupdate"
        )

        if_entry_col10_blank_152 = rail.IfOperator(
            task_id='if_entry_col10_blank_152',
            test=lambda: rail.result('velaw_user_import_mapper_search_entries_time_off_approval_path_14') and not rail.result(
                'velaw_user_import_mapper_search_entries_time_off_approval_path_14')['value_|_default_uri'],
            yes_task="insert_to_list_153",
            no_task="if_entry_col10_present_154"
        )

        insert_to_list_153 = rail.SetVariableOperator(
            task_id='insert_to_list_153',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Timeoff approval path not defined in mapper"
            }
        )

        if_entry_col10_present_154 = rail.IfOperator(
            task_id='if_entry_col10_present_154',
            test=lambda: rail.result('velaw_user_import_mapper_search_entries_time_off_approval_path_14') and rail.result('bulk_get_users3_9') and rail.result(
                'velaw_user_import_mapper_search_entries_time_off_approval_path_14')['value_|_default_uri'] and rail.result('velaw_user_import_mapper_search_entries_time_off_approval_path_14')['value_|_default_uri'] != rail.result('bulk_get_users3_9')[0]['timeOffApprovalPath']['displayText'],
            yes_task="update_approval_path_for_user_timeoff_155",
            no_task="get_timesheetapprovalpathupdate"
        )

        update_approval_path_for_user_timeoff_155 = rail.RepliconServiceOperator(
            task_id='update_approval_path_for_user_timeoff_155',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timeOffApprovalPathToApply": {
                        "uri": null,
                        "name": rail.result('velaw_user_import_mapper_search_entries_time_off_approval_path_14')['value_|_default_uri']
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        get_timesheetapprovalpathupdate = rail.GetVariableOperator(
            task_id='get_timesheetapprovalpathupdate',
            name='timesheetapprovalpathupdate'
        )

        if_declare_variable_7_value_equals_to_yes_157 = rail.IfOperator(
            task_id='if_declare_variable_7_value_equals_to_yes_157',
            test=lambda: rail.result('get_timesheetapprovalpathupdate') and rail.result(
                'get_timesheetapprovalpathupdate')['value'] == 'Yes',
            yes_task="if_entry_col10_blank_158",
            no_task="get_timezoneandholidaycalendarupdate"
        )

        if_entry_col10_blank_158 = rail.IfOperator(
            task_id='if_entry_col10_blank_158',
            test=lambda: rail.result('velaw_user_import_mapper_search_entries_timesheet_approval_path_15') and not rail.result(
                'velaw_user_import_mapper_search_entries_timesheet_approval_path_15')['value_|_default_uri'],
            yes_task="insert_to_list_159",
            no_task="if_entry_col10_present_160"
        )

        insert_to_list_159 = rail.SetVariableOperator(
            task_id='insert_to_list_159',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Timesheet approval path not defined in mapper"
            }
        )

        if_entry_col10_present_160 = rail.IfOperator(
            task_id='if_entry_col10_present_160',
            test=lambda: rail.result('velaw_user_import_mapper_search_entries_timesheet_approval_path_15') and rail.result('bulk_get_users3_9') and rail.result('velaw_user_import_mapper_search_entries_timesheet_approval_path_15')[
                'value_|_default_uri'] and rail.result('velaw_user_import_mapper_search_entries_timesheet_approval_path_15')['value_|_default_uri'] != rail.result('bulk_get_users3_9')[0]['timesheetApprovalPath']['displayText'],
            yes_task="update_approval_path_for_user_timesheet_161",
            no_task="get_timezoneandholidaycalendarupdate",
        )

        update_approval_path_for_user_timesheet_161 = rail.RepliconServiceOperator(
            task_id='update_approval_path_for_user_timesheet_161',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timesheetApprovalPathToApply": {
                        "uri": null,
                        "name": rail.result('velaw_user_import_mapper_search_entries_timesheet_approval_path_15')['value_|_default_uri']
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        get_timezoneandholidaycalendarupdate = rail.GetVariableOperator(
            task_id='get_timezoneandholidaycalendarupdate',
            name='timezoneandholidaycalendarupdate',
        )

        if_declare_variable_5_value_equals_to_yes_163 = rail.IfOperator(
            task_id='if_declare_variable_5_value_equals_to_yes_163',
            test=lambda: rail.result('get_timezoneandholidaycalendarupdate') and rail.result(
                'get_timezoneandholidaycalendarupdate')['value'] == 'Yes',
            yes_task="if_request_timezone_present_164",
            no_task="get_exceptions",
        )

        if_request_timezone_present_164 = rail.IfOperator(
            task_id='if_request_timezone_present_164',
            test=lambda dag_run: dag_run.conf['timezone'],
            yes_task="if_request_timezoneuri_present_165",
            no_task="insert_to_list_172",
        )

        if_request_timezoneuri_present_165 = rail.IfOperator(
            task_id='if_request_timezoneuri_present_165',
            test=lambda dag_run: dag_run.conf['timezoneuri'],
            yes_task="if_request_timezoneuri_not_equals_to_datarestbulk_get_users3_9responsedfirsttimezoneuri_166",
            no_task="insert_to_list_172",
        )

        if_request_timezoneuri_not_equals_to_datarestbulk_get_users3_9responsedfirsttimezoneuri_166 = rail.IfOperator(
            task_id='if_request_timezoneuri_not_equals_to_datarestbulk_get_users3_9responsedfirsttimezoneuri_166',
            test=lambda dag_run: rail.result('bulk_get_users3_9') and (
                dag_run.conf['timezoneuri'] != rail.result('bulk_get_users3_9')[0]['timeZone']['uri']),
            yes_task="update_time_zone_for_user_167",
            no_task="insert_to_list_170",
        )

        update_time_zone_for_user_167 = rail.RepliconServiceOperator(
            task_id='update_time_zone_for_user_167',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeZoneUri": "{{ dag_run.conf.timezoneuri }}"
            }
        )

        insert_to_list_170 = rail.SetVariableOperator(
            task_id='insert_to_list_170',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Time zone {{ dag_run.conf.timezone }} not available in Replicon"
            }
        )

        insert_to_list_172 = rail.SetVariableOperator(
            task_id='insert_to_list_172',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Timezone not defined in mapper for Country ISO Code {{ dag_run.conf.countryisocode }} and Location {{ dag_run.conf.location }}"
            }
        )

        if_request_holicaycalendar_present_173 = rail.IfOperator(
            task_id='if_request_holicaycalendar_present_173',
            test=lambda dag_run: dag_run.conf['holicaycalendar'],
            yes_task="if_holidaycalendar_displaytext_not_equals_to_dataworkato_servicereceive_requestrequestholicaycalendar_174",
            no_task="insert_to_list_180",
        )

        if_holidaycalendar_displaytext_not_equals_to_dataworkato_servicereceive_requestrequestholicaycalendar_174 = rail.IfOperator(
            task_id='if_holidaycalendar_displaytext_not_equals_to_dataworkato_servicereceive_requestrequestholicaycalendar_174',
            test=lambda dag_run: rail.result('bulk_get_users3_9') and (
                dag_run.conf['holicaycalendar'] != rail.result('bulk_get_users3_9')[0]['holidayCalendar']['displayText']),
            yes_task="if_request_holicaycalendaruri_present_175",
            no_task="get_exceptions",
        )

        if_request_holicaycalendaruri_present_175 = rail.IfOperator(
            task_id='if_request_holicaycalendaruri_present_175',
            test=lambda dag_run: dag_run.conf['holicaycalendaruri'],
            yes_task="update_holiday_calendar_for_user_176",
            no_task="insert_to_list_178",
        )

        update_holiday_calendar_for_user_176 = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user_176',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": "{{ dag_run.conf.holicaycalendaruri }}"
            }
        )

        insert_to_list_178 = rail.SetVariableOperator(
            task_id='insert_to_list_178',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Holiday calendar {{ dag_run.conf.holicaycalendar }} not available in Replicon"
            }
        )

        insert_to_list_180 = rail.SetVariableOperator(
            task_id='insert_to_list_180',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Holiday Calendar not defined in mapper for Country ISO Code {{ dag_run.conf.countryisocode }} and Location {{ dag_run.conf.location }}"
            }
        )

        get_exceptions = rail.GetVariableOperator(
            task_id='get_exceptions',
            name='Exception'
        )

        get_timeoff_process = rail.GetVariableOperator(
            task_id='get_timeoff_process',
            name='timeoffprocess',
        )

        if_declare_variable_3_value_equals_to_yes_181 = rail.IfOperator(
            task_id='if_declare_variable_3_value_equals_to_yes_181',
            test=lambda: rail.result('get_timeoff_process') and rail.result(
                'get_timeoff_process')['value'] == 'Yes',
            yes_task="trigger_dag_run_velaw_user_import_velawg3_child_timeoff_assignment_for_update_users_v2_0182",
            no_task="velaw_user_import_logs_add_entry_183",
        )

        trigger_dag_run_velaw_user_import_velawg3_child_timeoff_assignment_for_update_users_v2_0182 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_timeoff_assignment_for_update_users_v2_0182',
            retries=0,
            items=[0],
            trigger_dag_id=f'velaw_user_import_velawg3_child_timeoff_assignment_for_update_users_v2_0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "useruri": dag_run.conf['useruri'],
                "loginname": dag_run.conf['loginname'],
                "countryisocode": dag_run.conf['countryisocode'],
                "location": dag_run.conf['location'],
                "persontype": dag_run.conf['persontype'],
                "assignmentcategory": dag_run.conf['assignmentcategory'],
                "flsastatus": dag_run.conf['flsastatus'],
                "jobcode": dag_run.conf['jobcode'],
                "startdate": dag_run.conf['startdate'] if dag_run.conf['startdate'] else date.today().strftime("%m/%d/%Y"),
                "startingbalancesettouri": dag_run.conf['startingbalancesettouri'],
                "preventbalanceoverdrawuri": dag_run.conf['preventbalanceoverdrawuri']
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_timeoff_assignment_for_update_users_v2_0182 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_timeoff_assignment_for_update_users_v2_0182',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_timeoff_assignment_for_update_users_v2_0182") }}'
        )

        velaw_user_import_logs_add_entry_183 = rail.WriteLogOperator(
            task_id='velaw_user_import_logs_add_entry_183',
            log="{{ result('velaw_check_user_update_logs') }}",
            message="na",
            severity=lambda: "Exception" if rail.result('get_exceptions') and rail.result(
                'get_exceptions')['value'] else "Success",
            properties=lambda dag_run: {
                "username": dag_run.conf['firstname'] + ' ' + dag_run.conf['lastname'],
                "loginname": dag_run.conf['loginname'],
                "employeeid": dag_run.conf['employeeid'],
                "importaction": "update",
                "status": "Exception" if rail.result('get_exceptions') and rail.result('get_exceptions')['value'] else "Success",
                "details": "Partialy updated - " + str(rail.result('get_exceptions')['value']) if rail.result('get_exceptions') and rail.result('get_exceptions')['value'] else "Updated successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        velaw_user_import_logs_add_entry_185 = rail.WriteLogOperator(
            task_id='velaw_user_import_logs_add_entry_185',
            log="{{ result('velaw_check_user_update_logs') }}",
            message="na",
            severity="Error",
            trigger_rule='one_failed',
            properties={
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "importaction": "update",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> velaw_user_import_logs_add_entry_185
        can_run_batch_task >> rail.Label('No') >> declare_list_2
        declare_list_2 >> declare_variable_3 >> declare_variable_4 >> declare_variable_5 >> declare_variable_6 >> declare_variable_7 \
            >> bulk_get_users3_9 >> date_split_todays_date_10 >> velaw_check_user_update_logs >> velaw_supervisor_check_user_update_logs \
            >> if_userdetails_isenabled_is_not_true_11
        if_userdetails_isenabled_is_not_true_11 >> rail.Label(
            'Yes') >> velaw_user_import_logs_add_entry_12 >> velaw_user_import_logs_add_entry_185
        if_userdetails_isenabled_is_not_true_11 >> rail.Label(
            'No') >> velaw_user_import_mapper_search_entries_time_off_approval_path_14 >> velaw_user_import_mapper_search_entries_timesheet_approval_path_15 >> if_userdetails_isenabled_is_not_true_rehire_16
        if_userdetails_isenabled_is_not_true_rehire_16 >> rail.Label(
            'Yes') >> enable_login_17 >> date_split_start_date_18 >> update_employment_date_range_19 >> update_variable_20 \
            >> if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_21
        if_userdetails_isenabled_is_not_true_rehire_16 >> rail.Label(
            'No') >> if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_21
        if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_21 >> rail.Label(
            'Yes') >> update_first_name_22 >> if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_23
        if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_21 >> rail.Label(
            'No') >> if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_23
        if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_23 >> rail.Label(
            'Yes') >> update_last_name_24 >> if_request_employeeid_present_dataworkato_servicereceive_requestrequestlastnamedowncase_25
        if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_23 >> rail.Label(
            'No') >> if_request_employeeid_present_dataworkato_servicereceive_requestrequestlastnamedowncase_25
        if_request_employeeid_present_dataworkato_servicereceive_requestrequestlastnamedowncase_25 >> rail.Label(
            'Yes') >> update_employee_i_d_26 >> if_request_email_present_27
        if_request_employeeid_present_dataworkato_servicereceive_requestrequestlastnamedowncase_25 >> rail.Label(
            'No') >> if_request_email_present_27
        if_request_email_present_27 >> rail.Label(
            'Yes') >> update_email_28 >> invoke_custom_ruby_code_29
        if_request_email_present_27 >> rail.Label(
            'No') >> invoke_custom_ruby_code_29 >> declare_list_30 >> if_request_jobcode_present_31
        if_request_jobcode_present_31 >> rail.Label(
            'Yes') >> insert_to_list_32 >> update_variable_33 >> if_request_jobtitle_present_34
        if_request_jobcode_present_31 >> rail.Label(
            'No') >> if_request_jobtitle_present_34
        if_request_jobtitle_present_34 >> rail.Label(
            'Yes') >> insert_to_list_35 >> if_request_flsastatus_present_36
        if_request_jobtitle_present_34 >> rail.Label(
            'No') >> if_request_flsastatus_present_36
        if_request_flsastatus_present_36 >> rail.Label(
            'Yes') >> insert_to_list_37 >> update_variable_38 >> update_variable_39 >> update_variable_40 >> update_variable_41 >> if_request_assignmentcategory_present_42
        if_request_flsastatus_present_36 >> rail.Label(
            'No') >> if_request_assignmentcategory_present_42
        if_request_assignmentcategory_present_42 >> rail.Label(
            'Yes') >> insert_to_list_43 >> update_variable_44 >> if_request_countryisocode_present_45
        if_request_assignmentcategory_present_42 >> rail.Label(
            'No') >> if_request_countryisocode_present_45
        if_request_countryisocode_present_45 >> rail.Label(
            'Yes') >> insert_to_list_46 >> update_variable_47 >> update_variable_48 >> update_variable_49 >> update_variable_50 >> update_variable_51 >> if_request_persontype_present_52
        if_request_countryisocode_present_45 >> rail.Label(
            'No') >> if_request_persontype_present_52
        if_request_persontype_present_52 >> rail.Label(
            'Yes') >> insert_to_list_53 >> update_variable_54 >> update_variable_55 >> if_request_legalemployer_present_56
        if_request_persontype_present_52 >> rail.Label(
            'No') >> if_request_legalemployer_present_56
        if_request_legalemployer_present_56 >> rail.Label(
            'Yes') >> insert_to_list_57 >> get_customfield_values
        if_request_legalemployer_present_56 >> rail.Label(
            'No') >> get_customfield_values >> log_custom_fieldto_apply_58 >> if_log_custom_fieldto_apply_58_present_59
        if_log_custom_fieldto_apply_58_present_59 >> rail.Label(
            'Yes') >> update_custom_fields_60 >> if_request_payratesamount_present_61
        if_log_custom_fieldto_apply_58_present_59 >> rail.Label(
            'No') >> if_request_payratesamount_present_61
        if_request_payratesamount_present_61 >> rail.Label(
            'Yes') >> if_payrollrateschedule_to_json_contains_urn_62
        if_payrollrateschedule_to_json_contains_urn_62 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_64 >> if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_65
        if_payrollrateschedule_to_json_contains_urn_62 >> rail.Label(
            'No') >> if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_65
        if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_65 >> rail.Label(
            'Yes') >> update_user_payroll_rate_schedule_over_date_range_66 >> if_request_hourlycostamount_present_67
        if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_65 >> rail.Label(
            'No') >> if_request_hourlycostamount_present_67
        if_request_payratesamount_present_61 >> rail.Label(
            'No') >> if_request_hourlycostamount_present_67
        if_request_hourlycostamount_present_67 >> rail.Label(
            'Yes') >> if_costrateschedule_to_json_contains_urn_68
        if_costrateschedule_to_json_contains_urn_68 >> rail.Label(
            'Yes') >> parse_json_costrate_schedule_69 >> invoke_custom_ruby_code_70 >> if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_71
        if_costrateschedule_to_json_contains_urn_68 >> rail.Label(
            'No') >> if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_71
        if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_71 >> rail.Label(
            'Yes') >> update_user_cost_rate_schedule_over_date_range_72 >> if_request_defaultbillingrateamount_present_73
        if_schedulepolicies_displaytext_blank_dataworkato_servicereceive_requestrequestinitialschedulename_71 >> rail.Label(
            'No') >> if_request_defaultbillingrateamount_present_73
        if_request_hourlycostamount_present_67 >> rail.Label(
            'No') >> if_request_defaultbillingrateamount_present_73
        if_request_defaultbillingrateamount_present_73 >> rail.Label(
            'Yes') >> update_user_specific_billing_rate_amount_74 >> get_scheduleupdate
        if_request_defaultbillingrateamount_present_73 >> rail.Label(
            'No') >> get_scheduleupdate >> if_declare_variable_4_value_equals_to_yes_75
        if_declare_variable_4_value_equals_to_yes_75 >> rail.Label(
            'Yes') >> if_schedulepolicies_to_json_contains_urn_76
        if_schedulepolicies_to_json_contains_urn_76 >> rail.Label(
            'Yes') >> parse_json_office_schedule_77 >> invoke_custom_ruby_code_78 >> if_schedulepolicies_uri_blank_dataworkato_servicereceive_requestrequestinitialschedulename_79
        if_schedulepolicies_to_json_contains_urn_76 >> rail.Label(
            'No') >> if_schedulepolicies_uri_blank_dataworkato_servicereceive_requestrequestinitialschedulename_79
        if_schedulepolicies_uri_blank_dataworkato_servicereceive_requestrequestinitialschedulename_79 >> rail.Label(
            'Yes') >> updateofficeschedule_80 >> get_effective_user_group_membership_81
        if_schedulepolicies_uri_blank_dataworkato_servicereceive_requestrequestinitialschedulename_79 >> rail.Label(
            'No') >> get_effective_user_group_membership_81
        if_declare_variable_4_value_equals_to_yes_75 >> rail.Label(
            'No') >> get_effective_user_group_membership_81 >> if_request_department_present_82
        if_request_department_present_82 >> rail.Label(
            'Yes') >> update_department_group_83 >> update_variable_84 >> update_variable_85 >> if_request_location_present_86
        if_request_department_present_82 >> rail.Label(
            'No') >> if_request_location_present_86
        if_request_location_present_86 >> rail.Label(
            'Yes') >> update_location_group_87 >> update_variable_88 >> update_variable_89 >> update_variable_90 >> update_variable_91 >> if_request_jobfamilies_present_92
        if_request_location_present_86 >> rail.Label(
            'No') >> if_request_jobfamilies_present_92
        if_request_jobfamilies_present_92 >> rail.Label(
            'Yes') >> update_cost_center_group_93 >> update_variable_94 >> update_variable_95 >> if_request_paytype_present_96
        if_request_jobfamilies_present_92 >> rail.Label(
            'No') >> if_request_paytype_present_96
        if_request_paytype_present_96 >> rail.Label(
            'Yes') >> update_division_group_97 >> if_request_employeetype_present_98
        if_request_paytype_present_96 >> rail.Label(
            'No') >> if_request_employeetype_present_98
        if_request_employeetype_present_98 >> rail.Label(
            'Yes') >> update_employeetype_group_99 >> if_request_payrule_blank_100
        if_request_payrule_blank_100 >> rail.Label(
            'Yes') >> insert_to_list_101 >> if_request_payrule_present_102
        if_request_payrule_blank_100 >> rail.Label(
            'No') >> if_request_payrule_present_102
        if_request_payrule_present_102 >> rail.Label(
            'Yes') >> if_payrulescriptschedule_to_json_contains_urn_103
        if_payrulescriptschedule_to_json_contains_urn_103 >> rail.Label(
            'Yes') >> parse_json_payrule_schedule_104 >> invoke_custom_ruby_code_105 >> if_schedulepolicies_uri_blank_106
        if_payrulescriptschedule_to_json_contains_urn_103 >> rail.Label(
            'No') >> if_schedulepolicies_uri_blank_106
        if_schedulepolicies_uri_blank_106 >> rail.Label(
            'Yes') >> if_request_payruleuri_blank_107
        if_request_payruleuri_blank_107 >> rail.Label(
            'Yes') >> insert_to_list_108 >> if_request_timesheettemplate_not_equals_to_donotassign_120
        if_request_payruleuri_blank_107 >> rail.Label(
            'No') >> get_timesheet_for_date2_111 >> if_timesheet_uri_present_112
        if_timesheet_uri_present_112 >> rail.Label(
            'Yes') >> get_timesheet_details_113 >> if_timesheet_uri_blank_115
        if_timesheet_uri_present_112 >> rail.Label(
            'No') >> if_timesheet_uri_blank_115
        if_timesheet_uri_blank_115 >> rail.Label(
            'Yes') >> update_payrulewith_today_116 >> insert_to_list_117 >> if_request_timesheettemplate_not_equals_to_donotassign_120
        if_timesheet_uri_blank_115 >> rail.Label(
            'No') >> update_payrule_119 >> if_request_timesheettemplate_not_equals_to_donotassign_120
        if_schedulepolicies_uri_blank_106 >> rail.Label(
            'No') >> if_request_timesheettemplate_not_equals_to_donotassign_120
        if_request_payrule_present_102 >> rail.Label(
            'No') >> if_request_timesheettemplate_not_equals_to_donotassign_120
        if_request_timesheettemplate_not_equals_to_donotassign_120 >> rail.Label(
            'Yes') >> if_request_timesheettemplate_blank_121
        if_request_timesheettemplate_blank_121 >> rail.Label(
            'Yes') >> insert_to_list_122 >> if_request_timesheettemplate_present_123
        if_request_timesheettemplate_blank_121 >> rail.Label(
            'No') >> if_request_timesheettemplate_present_123
        if_request_timesheettemplate_present_123 >> rail.Label(
            'Yes') >> if_request_timesheettemplateuri_present_124
        if_request_timesheettemplate_present_123 >> rail.Label(
            'No') >> if_request_timesheettemplate_equals_to_donotassign_129
        if_request_timesheettemplateuri_present_124 >> rail.Label(
            'Yes') >> assign_policy_set_to_user_timesheettemplate_125 >> if_request_timesheettemplate_equals_to_donotassign_129
        if_request_timesheettemplateuri_present_124 >> rail.Label(
            'No') >> insert_to_list_127 >> if_request_timesheettemplate_equals_to_donotassign_129
        if_request_timesheettemplate_not_equals_to_donotassign_120 >> rail.Label(
            'No') >> if_request_timesheettemplate_equals_to_donotassign_129
        if_request_timesheettemplate_equals_to_donotassign_129 >> rail.Label(
            'Yes') >> remove_policy_set_assignment_from_user_timesheettemplate_130 >> if_request_supervisorloginname_present_131
        if_request_timesheettemplate_equals_to_donotassign_129 >> rail.Label(
            'No') >> if_request_supervisorloginname_present_131
        if_request_employeetype_present_98 >> rail.Label(
            'No') >> if_request_supervisorloginname_present_131
        if_request_supervisorloginname_present_131 >> rail.Label(
            'Yes') >> if_request_supervisorloginname_equals_to_dataworkato_servicereceive_requestrequestloginname_132
        if_request_supervisorloginname_equals_to_dataworkato_servicereceive_requestrequestloginname_132 >> rail.Label(
            'Yes') >> insert_to_list_133 >> get_timeoffapprovalpathupdate >> if_declare_variable_6_value_equals_to_yes_151
        if_request_supervisorloginname_equals_to_dataworkato_servicereceive_requestrequestloginname_132 >> rail.Label(
            'No') >> get_supervisor_assignment_detailsforuser_135 >> if_user_loginname_blank_136
        if_user_loginname_blank_136 >> rail.Label(
            'Yes') >> if_request_supervisoruri_blank_137
        if_request_supervisoruri_blank_137 >> rail.Label(
            'Yes') >> velaw_supervisor_check_add_entry_138 >> get_timeoffapprovalpathupdate
        if_request_supervisoruri_blank_137 >> rail.Label(
            'No') >> get_assigned_permission_sets_for_userfor_supervisor_140 >> if_request_supervisorstatus_equals_to_enabled_141
        if_request_supervisorstatus_equals_to_enabled_141 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_142 >> if_output_supervisor_blank_143
        if_request_supervisorstatus_equals_to_enabled_141 >> rail.Label(
            'No') >> velaw_supervisor_check_add_entry_150 >> get_timeoffapprovalpathupdate
        if_output_supervisor_blank_143 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_gen3_supervisor_144 >> if_output_enduser_blank_145
        if_output_supervisor_blank_143 >> rail.Label(
            'No') >> if_output_enduser_blank_145
        if_output_enduser_blank_145 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_gen3_project_resourcewithreports_substitute_user_146 >> date_split_supervisor_effective_date_147
        if_output_enduser_blank_145 >> rail.Label(
            'No') >> date_split_supervisor_effective_date_147 >> update_supervisor_assignment_schedule_over_date_range_148 >> get_timeoffapprovalpathupdate
        if_user_loginname_blank_136 >> rail.Label(
            'No') >> get_timeoffapprovalpathupdate
        if_request_supervisorloginname_present_131 >> rail.Label(
            'No') >> get_timeoffapprovalpathupdate
        if_declare_variable_6_value_equals_to_yes_151 >> rail.Label(
            'Yes') >> if_entry_col10_blank_152
        if_entry_col10_blank_152 >> rail.Label(
            'Yes') >> insert_to_list_153 >> if_entry_col10_present_154
        if_entry_col10_blank_152 >> rail.Label(
            'No') >> if_entry_col10_present_154
        if_entry_col10_present_154 >> rail.Label(
            'Yes') >> update_approval_path_for_user_timeoff_155 >> get_timesheetapprovalpathupdate >> if_declare_variable_7_value_equals_to_yes_157
        if_entry_col10_present_154 >> rail.Label(
            'No') >> get_timesheetapprovalpathupdate
        if_declare_variable_6_value_equals_to_yes_151 >> rail.Label(
            'No') >> get_timesheetapprovalpathupdate
        if_declare_variable_7_value_equals_to_yes_157 >> rail.Label(
            'Yes') >> if_entry_col10_blank_158
        if_entry_col10_blank_158 >> rail.Label(
            'Yes') >> insert_to_list_159 >> if_entry_col10_present_160
        if_entry_col10_blank_158 >> rail.Label(
            'No') >> if_entry_col10_present_160
        if_entry_col10_present_160 >> rail.Label(
            'Yes') >> update_approval_path_for_user_timesheet_161 >> get_timezoneandholidaycalendarupdate >> if_declare_variable_5_value_equals_to_yes_163
        if_entry_col10_present_160 >> rail.Label(
            'No') >> get_timezoneandholidaycalendarupdate
        if_declare_variable_7_value_equals_to_yes_157 >> rail.Label(
            'No') >> get_timezoneandholidaycalendarupdate
        if_declare_variable_5_value_equals_to_yes_163 >> rail.Label(
            'Yes') >> if_request_timezone_present_164
        if_request_timezone_present_164 >> rail.Label(
            'Yes') >> if_request_timezoneuri_present_165
        if_request_timezone_present_164 >> rail.Label(
            'No') >> insert_to_list_172
        if_request_timezoneuri_present_165 >> rail.Label(
            'Yes') >> if_request_timezoneuri_not_equals_to_datarestbulk_get_users3_9responsedfirsttimezoneuri_166
        if_request_timezoneuri_not_equals_to_datarestbulk_get_users3_9responsedfirsttimezoneuri_166 >> rail.Label(
            'Yes') >> update_time_zone_for_user_167 >> insert_to_list_172
        if_request_timezoneuri_not_equals_to_datarestbulk_get_users3_9responsedfirsttimezoneuri_166 >> rail.Label(
            'No') >> insert_to_list_170 >> if_request_holicaycalendar_present_173
        if_request_timezoneuri_present_165 >> rail.Label(
            'No') >> insert_to_list_172 >> if_request_holicaycalendar_present_173
        if_request_holicaycalendar_present_173 >> rail.Label(
            'Yes') >> if_holidaycalendar_displaytext_not_equals_to_dataworkato_servicereceive_requestrequestholicaycalendar_174
        if_holidaycalendar_displaytext_not_equals_to_dataworkato_servicereceive_requestrequestholicaycalendar_174 >> rail.Label(
            'Yes') >> if_request_holicaycalendaruri_present_175
        if_request_holicaycalendaruri_present_175 >> rail.Label(
            'Yes') >> update_holiday_calendar_for_user_176 >> get_exceptions >> get_timeoff_process >> if_declare_variable_3_value_equals_to_yes_181
        if_request_holicaycalendaruri_present_175 >> rail.Label(
            'No') >> insert_to_list_178 >> get_exceptions
        if_holidaycalendar_displaytext_not_equals_to_dataworkato_servicereceive_requestrequestholicaycalendar_174 >> rail.Label(
            'No') >> get_exceptions
        if_request_holicaycalendar_present_173 >> rail.Label(
            'No') >> insert_to_list_180 >> get_exceptions
        if_declare_variable_5_value_equals_to_yes_163 >> rail.Label(
            'No') >> get_exceptions
        if_declare_variable_3_value_equals_to_yes_181 >> rail.Label(
            'Yes') >> trigger_dag_run_velaw_user_import_velawg3_child_timeoff_assignment_for_update_users_v2_0182 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_timeoff_assignment_for_update_users_v2_0182 >> velaw_user_import_logs_add_entry_183
        if_declare_variable_3_value_equals_to_yes_181 >> rail.Label(
            'No') >> velaw_user_import_logs_add_entry_183 >> velaw_user_import_logs_add_entry_185 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
