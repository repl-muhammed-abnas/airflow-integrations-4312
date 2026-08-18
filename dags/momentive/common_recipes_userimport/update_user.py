# pylint: disable=too-many-statements
from datetime import timedelta, datetime
from airflow.models import Variable
import rail
from momentive.common_recipes_userimport.utils import request_payload, python_callable
from momentive.common_recipes_userimport.utils.python_callable import get_current_data

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.momentive_othercountries_user_sync_update_user_child_dag_id,
        description=f'momentive_othercountries_user_sync_update_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_workertype_change'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_workertype_change',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_workertype_change=rail.SetVariableOperator(
            task_id='create_workertype_change',
            append=False,
            name='workertype_change',
            value='false'
        )

        create_workshift_change=rail.SetVariableOperator(
            task_id='create_workshift_change',
            append=False,
            name='workshift_change',
            value='false'
        )

        create_Exemptionstatus_change=rail.SetVariableOperator(
            task_id='create_Exemptionstatus_change',
            append=False,
            name='Exemptionstatus_change',
            value='false'
        )

        create_location_change=rail.SetVariableOperator(
            task_id='create_location_change',
            append=False,
            name='location_change',
            value='false'
        )

        create_workersubtype_change=rail.SetVariableOperator(
            task_id='create_workersubtype_change',
            append=False,
            name='workersubtype_change',
            value='false'
        )

        create_timeofftrigger=rail.SetVariableOperator(
            task_id='create_timeofftrigger',
            append=False,
            name='timeofftrigger',
            value='false'
        )

        exception_log = rail.CreateLogOperator(
            task_id = "exception_log"
        )

        log_entries = rail.CreateLogOperator(
            task_id = "log_entries"
        )

        get_input_validation_log = rail.PythonOperator(
            task_id = "get_input_validation_log",
            python_callable=python_callable.get_input_validationlog
        )

        if_input_validation_log_present = rail.IfOperator(
            task_id='if_input_validation_log_present',
            test="{{ result('get_input_validation_log').exc_present | is_truthy }}",
            yes_task="log_user_import_not_created",
            no_task="get_user_data",
        )

        log_user_import_not_created = rail.WriteLogOperator(
            task_id="log_user_import_not_created",
            log = '{{ dag_run.conf.logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda dag_run: {
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "action": "Update",
                "status": "Exception",
                # non-existent task 'get_exception_log'
                'details': "User not updated," + rail.result('get_input_validation_log')['exc_value'],
                'country':'South Korea' if "Korea, Republic of" in dag_run.conf['country'] else \
                    'UAE' if "United Arab Emirates" in dag_run.conf['country'] else \
                        "Belgium" if "Belgium" in dag_run.conf['country'] else ""
            }
        )

        get_user_data = rail.RepliconServiceOperator(
            task_id='get_user_data',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.useruri }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_regireupdate_equals_rehire = rail.IfOperator(
            task_id='if_regireupdate_equals_rehire',
            test="{{ dag_run.conf.rehireupdate == 'rehire' }}",
            yes_task="update_timeofftrigger_17",
            no_task="validate_hiredate_and_startdate",
        )

        update_timeofftrigger_17 = rail.SetVariableOperator(
            task_id='update_timeofftrigger_17',
            append=False,
            name='{{ result("create_timeofftrigger").name }}',
            value='true'
        )

        validate_hiredate_and_startdate = rail.PythonOperator(
            task_id= "validate_hiredate_and_startdate",
            python_callable=python_callable.validate_hiredate_startdate
        )

        if_validate_hiredate_and_startdate_is_false_20 = rail.IfOperator(
            task_id='if_validate_hiredate_and_startdate_is_false_20',
            test="{{ result('validate_hiredate_and_startdate') | is_falsy }}",
            yes_task="remove_end_date_and_update_rehire_date",
            no_task="if_firstname_mismatch",
        )

        remove_end_date_and_update_rehire_date = rail.RepliconServiceOperator(
            task_id='remove_end_date_and_update_rehire_date',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": request_payload.get_datetime_obj(dag_run.conf['hiredate'])
                }
            }
        )

        enable_userprofile = rail.RepliconServiceOperator(
            task_id='enable_userprofile',
            endpoint="/services/securityservice1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_user_enabled = rail.WriteLogOperator(
            task_id='log_user_enabled',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "User enabled in Replicon and end date removed"
            }
        )

        if_firstname_mismatch = rail.IfOperator(
            task_id="if_firstname_mismatch",
            test="{{ (result('get_user_data')[0].userDetails.firstName | is_falsy or \
                result('get_user_data')[0].userDetails.firstName.lower() != dag_run.conf.firstname.lower()) and \
                dag_run.conf.firstname | is_truthy }}",
            yes_task="update_firstname",
            no_task="if_lastname_mismatch"
        )

        update_firstname = rail.RepliconServiceOperator(
            task_id = "update_firstname",
            endpoint = "/services/UserService1.svc/UpdateFirstName",
            data ={
                "userUri" : "{{ dag_run.conf.useruri }}",
                "firstname" : "{{ dag_run.conf.firstname }}"
            }
        )

        log_first_name_updated = rail.WriteLogOperator(
            task_id='log_first_name_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "First name updated"
            }
        )

        if_lastname_mismatch = rail.IfOperator(
            task_id="if_lastname_mismatch",
            test="{{ (result('get_user_data')[0].userDetails.lastName | is_falsy or \
                result('get_user_data')[0].userDetails.lastName.lower() != dag_run.conf.lastname.lower()) and \
                dag_run.conf.lastname | is_truthy }}",
            yes_task="update_lastname",
            no_task="if_email_mismatch"
        )

        update_lastname = rail.RepliconServiceOperator(
            task_id = "update_lastname",
            endpoint = "/services/UserService1.svc/UpdateLastName",
            data ={
                "userUri" : "{{ dag_run.conf.useruri }}",
                "lastname" : "{{ dag_run.conf.lastname }}"
            }
        )

        log_last_name_updated = rail.WriteLogOperator(
            task_id='log_last_name_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Last name updated"
            }
        )

        if_email_mismatch = rail.IfOperator(
            task_id="if_email_mismatch",
            test="{{ (result('get_user_data')[0].userDetails.emailAddress | is_falsy or \
                result('get_user_data')[0].userDetails.emailAddress.lower() != dag_run.conf.emailaddress.lower()) and \
                dag_run.conf.emailaddress | is_truthy }}",
            yes_task="update_email_address",
            no_task="if_regireupdate_notequals_rehire"
        )

        update_email_address = rail.RepliconServiceOperator(
            task_id = "update_email_address",
            endpoint = "/services/UserService1.svc/UpdateEmail",
            data ={
                "userUri" : "{{ dag_run.conf.useruri }}",
                "email" : "{{ dag_run.conf.emailaddress }}"
            }
        )

        log_email_updated = rail.WriteLogOperator(
            task_id='log_email_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Email address updated"
            }
        )

        if_regireupdate_notequals_rehire = rail.IfOperator(
            task_id='if_regireupdate_notequals_rehire',
            test="{{ dag_run.conf.rehireupdate != 'rehire' }}",
            yes_task="validate_termination_date_and_enddate",
            no_task="get_required_user_customfields",
        )

        validate_termination_date_and_enddate = rail.PythonOperator(
            task_id= "validate_termination_date_and_enddate",
            python_callable=python_callable.validate_terminationdate_enddate
        )

        if_terminationdate_present_and_not_equal_enddate = rail.IfOperator(
            task_id='if_terminationdate_present_and_not_equal_enddate',
            test="{{ dag_run.conf.terminationdate | is_truthy and \
                result('validate_termination_date_and_enddate') | is_falsy }}",
            yes_task="update_end_date_39",
            no_task="get_required_user_customfields",
        )

        update_end_date_39 = rail.RepliconServiceOperator(
            task_id='update_end_date_39',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": request_payload.get_datetime_obj(dag_run.conf['hiredate']),
                    "endDate": request_payload.get_datetime_obj(dag_run.conf['terminationdate'])
                }
            }
        )

        log_termination_date_updated = rail.WriteLogOperator(
            task_id='log_termination_date_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Termination date updated"
            }
        )

        get_required_user_customfields = rail.RepliconServiceOperator(
            task_id='get_required_user_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': "urn:replicon:object-type:user"
            }
        )

        get_user_udf_values = rail.PythonOperator(
            task_id = "get_user_udf_values",
            python_callable=python_callable.get_udf_values_from_userdetails
        )

        if_CF_Date_of_Birth_MM_DD_YYYY_present = rail.IfOperator(
            task_id='if_CF_Date_of_Birth_MM_DD_YYYY_present',
            test="{{ dag_run.conf.date_of_birth | is_truthy }}",
            yes_task="validate_CF_Date_of_Birth_MM_DD_YYYY",
            no_task="if_businesstitle_present_and_mismatch",
        )

        validate_CF_Date_of_Birth_MM_DD_YYYY = rail.IfOperator(
            task_id='validate_CF_Date_of_Birth_MM_DD_YYYY',
            test=lambda dag_run: bool('-' in dag_run.conf['date_of_birth']),
            yes_task="check_dob_mismatch",
            no_task="log_birthdate_invalid",
        )

        log_birthdate_invalid = rail.WriteLogOperator(
            task_id='log_birthdate_invalid',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Birthdate not in predefined date format"
            }
        )

        check_dob_mismatch = rail.IfOperator(
            task_id='check_dob_mismatch',
            # Blank stored DOB = mismatch -> write it (recipe node 49); see is_dob_mismatch.
            test=lambda dag_run: bool(python_callable.is_dob_mismatch(dag_run)),
            yes_task="update_dob_udf",
            no_task="if_businesstitle_present_and_mismatch",
        )

        update_dob_udf = rail.RepliconServiceOperator(
            task_id='update_dob_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": rail.result('get_user_udf_values')['dob_uri'],
                "value": request_payload.get_datetime_obj(dag_run.conf['date_of_birth'])
            }
        )

        log_dob_updated = rail.WriteLogOperator(
            task_id='log_dob_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Birthdate field updated"
            }
        )

        if_businesstitle_present_and_mismatch = rail.IfOperator(
            task_id='if_businesstitle_present_and_mismatch',
            test="{{ dag_run.conf.businesstitle | is_truthy and \
                dag_run.conf.businesstitle.lower() != result('get_user_udf_values').title.lower() }}",
            yes_task="update_title_udf",
            no_task="if_yearofservice_present_and_mismatch",
        )

        update_title_udf = rail.RepliconServiceOperator(
            task_id='update_title_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{result('get_user_udf_values').title_uri }}",
                "value": "{{ dag_run.conf.businesstitle }}"
            }
        )

        log_title_updated = rail.WriteLogOperator(
            task_id='log_title_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Business title field updated"
            }
        )

        if_yearofservice_present_and_mismatch = rail.IfOperator(
            task_id='if_yearofservice_present_and_mismatch',
            test="{{ dag_run.conf.year_of_service | is_truthy and \
                dag_run.conf.year_of_service != result('get_user_udf_values').yearsofservice }}",
            yes_task="update_yearsofservice_udf",
            no_task="if_fieldhr_present_and_mismatch",
        )

        update_yearsofservice_udf = rail.RepliconServiceOperator(
            task_id='update_yearsofservice_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{result('get_user_udf_values').yearsofservice_uri }}",
                "value": "{{ dag_run.conf.year_of_service }}"
            }
        )

        log_yearsofservice_updated = rail.WriteLogOperator(
            task_id='log_yearsofservice_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Years of Service field updated"
            }
        )

        if_fieldhr_present_and_mismatch = rail.IfOperator(
            task_id='if_fieldhr_present_and_mismatch',
            test="{{ dag_run.conf.fieldhr | is_truthy and \
                dag_run.conf.fieldhr.lower() != result('get_user_udf_values').hrm.lower() }}",
            yes_task="update_fieldhr_udf",
            no_task="if_contsrvcdate_present_and_mismatch",
        )

        update_fieldhr_udf = rail.RepliconServiceOperator(
            task_id='update_fieldhr_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{result('get_user_udf_values').hrm_uri }}",
                "value": "{{ dag_run.conf.fieldhr }}"
            }
        )

        log_fieldhr_updated = rail.WriteLogOperator(
            task_id='log_fieldhr_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "HRM field updated"
            }
        )

        if_contsrvcdate_present_and_mismatch = rail.IfOperator(
            task_id='if_contsrvcdate_present_and_mismatch',
            test="{{ dag_run.conf.continous_service_date | is_truthy and \
                dag_run.conf.continous_service_date != result('get_user_udf_values').cont_yearsofservice }}",
            yes_task="update_contsrvcdate_udf",
            no_task="if_timeoffservdate_present_and_mismatch",
        )

        update_contsrvcdate_udf = rail.RepliconServiceOperator(
            task_id='update_contsrvcdate_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{result('get_user_udf_values').cont_yearsofservice_uri }}",
                "value": "{{ dag_run.conf.continous_service_date }}"
            }
        )

        log_contsrvcdate_updated = rail.WriteLogOperator(
            task_id='log_contsrvcdate_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Continuous Years of Service - YOS field updated"
            }
        )

        if_timeoffservdate_present_and_mismatch = rail.IfOperator(
            task_id='if_timeoffservdate_present_and_mismatch',
            test="{{ dag_run.conf.timeoff_service_date | is_truthy and \
                dag_run.conf.timeoff_service_date != result('get_user_udf_values').timeoffservcdate }}",
            yes_task="update_timeoffservdate_udf",
            no_task="if_gender_present_and_mismatch",
        )

        update_timeoffservdate_udf = rail.RepliconServiceOperator(
            task_id='update_timeoffservdate_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{result('get_user_udf_values').timeoffservcdate_uri }}",
                "value": "{{ dag_run.conf.timeoff_service_date }}"
            }
        )

        log_timeoffservdate_updated = rail.WriteLogOperator(
            task_id='log_timeoffservdate_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Time off Service Date - YOSS field updated"
            }
        )

        if_gender_present_and_mismatch = rail.IfOperator(
            task_id='if_gender_present_and_mismatch',
            test="{{ dag_run.conf.gender | is_truthy and \
                dag_run.conf.gender.lower() != result('get_user_udf_values').gender.lower() }}",
            yes_task="update_gender_udf",
            no_task="if_function_present_and_mismatch",
        )

        update_gender_udf = rail.RepliconServiceOperator(
            task_id='update_gender_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{result('get_user_udf_values').gender_uri }}",
                "value": "{{ dag_run.conf.gender }}"
            }
        )

        if_function_present_and_mismatch = rail.IfOperator(
            task_id='if_function_present_and_mismatch',
            test="{{ dag_run.conf.function | is_truthy and \
                dag_run.conf.function.lower() != result('get_user_udf_values').function.lower() }}",
            yes_task="update_function_udf",
            no_task="if_workersubtype_present_and_mismatch",
        )

        update_function_udf = rail.RepliconServiceOperator(
            task_id='update_function_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{result('get_user_udf_values').function_uri }}",
                "value": "{{ dag_run.conf.function }}"
            }
        )

        if_workersubtype_present_and_mismatch = rail.IfOperator(
            task_id='if_workersubtype_present_and_mismatch',
            test="{{ dag_run.conf.worker_subType | is_truthy and result('get_user_udf_values').worker_subType | is_truthy and \
                dag_run.conf.worker_subType.lower() != result('get_user_udf_values').worker_subType.lower() }}",
            yes_task="get_workersubtype_dropdowns",
            no_task="create_changein_shift",
        )

        get_workersubtype_dropdowns = rail.RepliconServiceOperator(
            task_id='get_workersubtype_dropdowns',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_user_udf_values').workersubtype_uri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['worker_subType'], 'uri', '')
        )

        if_get_workersubtype_dropdowns_uri_present = rail.IfOperator(
            task_id='if_get_workersubtype_dropdowns_uri_present',
            test="{{ result('get_workersubtype_dropdowns') | is_truthy }}",
            yes_task="update_worker_subtype_udf",
            no_task="create_changein_shift",
        )

        update_worker_subtype_udf = rail.RepliconServiceOperator(
            task_id='update_worker_subtype_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_user_udf_values').workersubtype_uri }}",
                "customFieldDropDownOptionUri": "{{ result('get_workersubtype_dropdowns') }}"
            }
        )

        log_workersubtype_updated = rail.WriteLogOperator(
            task_id='log_workersubtype_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "worker_subType field updated"
            }
        )

        create_changein_shift=rail.SetVariableOperator(
            task_id='create_changein_shift',
            append=False,
            name='changeinshift',
            value='no'
        )

        if_workshift_present_and_mismatch = rail.IfOperator(
            task_id='if_workshift_present_and_mismatch',
            test="{{ dag_run.conf.work_shift | is_truthy and \
                dag_run.conf.work_shift.lower() != result('get_user_udf_values').work_shift.lower() }}",
            yes_task="get_workshift_dropdowns",
            no_task="getdata_sup_emp_grp_dept_grp",
        )

        get_workshift_dropdowns = rail.RepliconServiceOperator(
            task_id='get_workshift_dropdowns',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_user_udf_values').workshift_uri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['work_shift'], 'uri', '')
        )

        if_get_workershift_dropdowns_uri_present = rail.IfOperator(
            task_id='if_get_workershift_dropdowns_uri_present',
            test="{{ result('get_workshift_dropdowns') | is_truthy }}",
            yes_task="update_workshift_udf",
            no_task="getdata_sup_emp_grp_dept_grp",
        )

        update_workshift_udf = rail.RepliconServiceOperator(
            task_id='update_workshift_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{result('get_user_udf_values').workshift_uri }}",
                "customFieldDropDownOptionUri": "{{ result('get_workshift_dropdowns') }}"
            }
        )

        if_user_workshift_rota_mismatch = rail.IfOperator(
            task_id='if_user_workshift_rota_mismatch',
            test=lambda dag_run: bool('Rota' not in dag_run.conf['work_shift'] and \
                                    'Rota' in rail.result('get_user_udf_values')['work_shift']),
            yes_task="update_changeinshift_variable_112",
            no_task="getdata_sup_emp_grp_dept_grp",
        )

        update_changeinshift_variable_112 = rail.SetVariableOperator(
            task_id='update_changeinshift_variable_112',
            append=False,
            name='{{ result("create_changein_shift").name }}',
            value='yes'
        )

        getdata_sup_emp_grp_dept_grp = rail.RepliconServiceOperator(
            task_id="getdata_sup_emp_grp_dept_grp",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_data_sup_emp_grp_dept_grp
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: {
                'basic_user_with_report_uri': rail.find_first_by_attr_and_get_attr(
                    response,'name',"Basic User with Reports",'uri'),
                'supervisor': rail.find_first_by_attr_and_get_attr(
                    response,'name',"Supervisor - Edit",'uri')
            }
        )

        if_manager_id_present = rail.IfOperator(
            task_id='if_manager_id_present',
            test="{{ dag_run.conf.managerid | is_truthy }}",
            yes_task="if_managerid_equals_workrefempid",
            no_task="compare_to_today",
        )

        if_managerid_equals_workrefempid = rail.IfOperator(
            task_id='if_managerid_equals_workrefempid',
            test="{{ dag_run.conf.managerid == dag_run.conf.workerreferenceemployeeid }}",
            yes_task="log_supervisor_sameas_user",
            no_task="search_for_user_with_empid",
        )

        log_supervisor_sameas_user = rail.WriteLogOperator(
            task_id='log_supervisor_sameas_user',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Supervsior not updated for since user's  supervsior can not be same as the user"
            }
        )

        search_for_user_with_empid = rail.RepliconServiceOperator(
            task_id='search_for_user_with_empid',
            endpoint="/services/UserListService1.svc/GetData",
            data = request_payload.search_supervisor_payload,
            data_handler=python_callable.get_userdata_list_for_managerid
        )

        check_if_multiple_manageruseruri_present = rail.IfOperator(
            task_id='check_if_multiple_manageruseruri_present',
            test=lambda: bool(len(rail.result('search_for_user_with_empid')) > 1 ),
            yes_task="log_multiple_user_for_same_managerid",
            no_task="check_if_single_manageruseruri_present",
        )

        log_multiple_user_for_same_managerid = rail.WriteLogOperator(
            task_id='log_multiple_user_for_same_managerid',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Supervisor not assigned for user {{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }} as \
                    multiple users have same Employee ID:{{ dag_run.conf.managerid }} ."
            }
        )

        check_if_single_manageruseruri_present = rail.IfOperator(
            task_id='check_if_single_manageruseruri_present',
            test=lambda: bool(len(rail.result('search_for_user_with_empid')) == 1 ),
            yes_task="get_manager_details",
            no_task="log_supervisor_assignment",
        )

        get_manager_details = rail.RepliconServiceOperator(
            task_id='get_manager_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data = request_payload.get_manager_details_payload
        )

        if_manager_details_present_and_enabled = rail.IfOperator(
            task_id='if_manager_details_present_and_enabled',
            test="{{ result('get_manager_details') | is_truthy and result('get_manager_details')[0]['userDetails']['isEnabled'] | is_truthy }}",
            yes_task="get_assigned_permissionset_foruser",
            no_task="log_supervisor_assignment",
        )

        get_assigned_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionset_foruser',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data = {
                "userUri": "{{ result('search_for_user_with_empid')[0].uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'user.uri', '')
        )

        if_supervisor_permission_not_assigned = rail.IfOperator(
            task_id='if_supervisor_permission_not_assigned',
            test="{{ result('get_assigned_permissionset_foruser') | is_falsy }}",
            yes_task="add_missing_supervisor_permission",
            no_task="if_search_user_supervisor_null_139",
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=request_payload.add_missing_supervisor_permission_payload
        )

        if_search_user_supervisor_null_139 = rail.IfOperator(
            task_id='if_search_user_supervisor_null_139',
            test="{{ result('getdata_sup_emp_grp_dept_grp').rows[0].cells[2].dataType | is_truthy and \
                result('getdata_sup_emp_grp_dept_grp').rows[0].cells[2].dataType == 'urn:replicon:list-type:null' }}",
            yes_task="update_initial_supervisor",
            no_task="if_search_user_supervisor_not_null_142",
        )

        update_initial_supervisor = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "initialSupervisorUri": "{{ result('search_for_user_with_empid')[0].uri }}",
                "scheduleEntries": []
            }
        )

        log_initial_supervisor_added = rail.WriteLogOperator(
            task_id='log_initial_supervisor_added',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Initial supervisor added"
            }
        )

        if_search_user_supervisor_not_null_142 = rail.IfOperator(
            task_id='if_search_user_supervisor_not_null_142',
            test="{{ result('getdata_sup_emp_grp_dept_grp').rows[0].cells[2].dataType | is_truthy and \
                result('getdata_sup_emp_grp_dept_grp').rows[0].cells[2].dataType != 'urn:replicon:list-type:null' }}",
            yes_task="get_current_supervisor_empid",
            no_task="compare_to_today",
        )

        get_current_supervisor_empid = rail.RepliconServiceOperator(
            task_id='get_current_supervisor_empid',
            endpoint="/services/UserListService1.svc/GetData",
            data = request_payload.get_current_supervisorempid,
            data_handler=lambda response: response['rows'][0]['cells'][0]['textValue']
        )

        if_current_supervisor_empid_mismatch_managerid = rail.IfOperator(
            task_id='if_current_supervisor_empid_mismatch_managerid',
            test="{{ dag_run.conf.managerid | is_truthy and \
                result('get_current_supervisor_empid') != dag_run.conf.managerid }}",
            yes_task="update_supervisor_assignment_over_daterange",
            no_task="compare_to_today",
        )

        update_supervisor_assignment_over_daterange = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_over_daterange',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data = lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_for_user_with_empid')[0]['uri'],
                "dateRange":{
                    "startDate": request_payload.get_datetime_obj(
                        dag_run.conf['effective_date_of_manager_change'] if dag_run.conf['effective_date_of_manager_change'] else str(
                            datetime.strftime(datetime.now().date(), '%Y-%m-%d')))
                }
            }
        )

        log_supervisor_updated = rail.WriteLogOperator(
            task_id='log_supervisor_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Supervsior updated"
            }
        )

        log_supervisor_assignment = rail.WriteLogOperator(
            task_id="log_supervisor_assignment",
            log = '{{ dag_run.conf.supervisor_logger}}',
            message="Exception",
            severity="Exception",
            properties=request_payload.supervisor_assignment_log_payload
        )

        compare_to_today = rail.PythonOperator(
            task_id = "compare_to_today",
            python_callable=python_callable.compare_dates_to_today
        )

        validate_exemptioneff_date_155 = rail.IfOperator(
            task_id='validate_exemptioneff_date_155',
            test="{{ result('compare_to_today').exemption_eff_date | is_truthy }}",
            yes_task="update_exemption_change_variable_156",
            no_task="validate_workshift_change_effect_date_157",
        )

        update_exemption_change_variable_156 = rail.SetVariableOperator(
            task_id='update_exemption_change_variable_156',
            append=False,
            name='{{ result("create_Exemptionstatus_change").name }}',
            value='true'
        )

        validate_workshift_change_effect_date_157 = rail.IfOperator(
            task_id='validate_workshift_change_effect_date_157',
            test="{{ result('compare_to_today').workshift_change_effective_date | is_truthy }}",
            yes_task="update_workshift_change_variable_158",
            no_task="validate_effect_date_of_workertype_159",
        )

        update_workshift_change_variable_158 = rail.SetVariableOperator(
            task_id='update_workshift_change_variable_158',
            append=False,
            name='{{ result("create_workshift_change").name }}',
            value='true'
        )

        validate_effect_date_of_workertype_159 = rail.IfOperator(
            task_id='validate_effect_date_of_workertype_159',
            test="{{ result('compare_to_today').effective_date_of_workertype | is_truthy }}",
            yes_task="update_workertype_change_variable_160",
            no_task="if_location_present_and_validate_cflrv_loc_changedate",
        )

        update_workertype_change_variable_160 = rail.SetVariableOperator(
            task_id='update_workertype_change_variable_160',
            append=False,
            name='{{ result("create_workertype_change").name }}',
            value='true'
        )

        if_location_present_and_validate_cflrv_loc_changedate = rail.IfOperator(
            task_id='if_location_present_and_validate_cflrv_loc_changedate',
            test="{{ dag_run.conf.location | is_truthy and \
                result('compare_to_today').cf_lrv_location_change_effective_date | is_truthy }}",
            yes_task="update_location_change_variable_163",
            no_task="create_location_lookup",
        )

        update_location_change_variable_163 = rail.SetVariableOperator(
            task_id='update_location_change_variable_163',
            append=False,
            name='{{ result("create_location_change").name }}',
            value='true'
        )

        create_location_lookup=rail.SetVariableOperator(
            task_id='create_location_lookup',
            append=False,
            name='location_lookup',
            value='Any'
        )

        create_country_lookup=rail.SetVariableOperator(
            task_id='create_country_lookup',
            append=False,
            name='country_lookup',
            value=python_callable.get_iniial_country_lookup_value
        )

        create_shift_lookup=rail.SetVariableOperator(
            task_id='create_shift_lookup',
            append=False,
            name='shift_lookup',
            value='Any'
        )

        if_workshift_equals_prod_rota_or_nonrota = rail.IfOperator(
            task_id='if_workshift_equals_prod_rota_or_nonrota',
            test="{{ dag_run.conf.work_shift == 'PRODUCTION Rota' or dag_run.conf.work_shift == 'PRODUCTION Non Rota' }}",
            yes_task="update_shift_lookup_to_workshift",
            no_task="update_shift_lookup_to_any",
        )

        update_shift_lookup_to_workshift = rail.SetVariableOperator(
            task_id='update_shift_lookup_to_workshift',
            append=False,
            name='{{ result("create_shift_lookup").name }}',
            value=lambda dag_run: dag_run.conf['work_shift']
        )

        update_shift_lookup_to_any = rail.SetVariableOperator(
            task_id='update_shift_lookup_to_any',
            append=False,
            name='{{ result("create_shift_lookup").name }}',
            value='Any'
        )

        create_workersubshift_lookup=rail.SetVariableOperator(
            task_id='create_workersubshift_lookup',
            append=False,
            name='workersubshift_lookup',
            value=lambda dag_run: dag_run.conf['legalentity']
        )

        validate_hiredate_to_today = rail.IfOperator(
            task_id='validate_hiredate_to_today',
            # FIX vs SK: SK tested bool(validate_hiredate) - the function OBJECT (always
            # truthy), so the gate always took Yes; the function must be called.
            test=lambda dag_run: bool(python_callable.validate_hiredate(dag_run)),
            yes_task="get_timesheet_for_date2",
            no_task="get_country_lookup_variable",
        )

        get_timesheet_for_date2 = rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date2',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=request_payload.get_timesheet_for_date2_payload
        )

        if_get_timesheet_for_date2_uri_present = rail.IfOperator(
            task_id='if_get_timesheet_for_date2_uri_present',
            test="{{ result('get_timesheet_for_date2') | is_truthy and \
                result('get_timesheet_for_date2').timesheet | is_truthy and \
                result('get_timesheet_for_date2').timesheet.uri | is_truthy }}",
            yes_task="get_timesheet_details",
            no_task="get_country_lookup_variable",
        )

        get_timesheet_details = rail.RepliconServiceOperator(
            task_id='get_timesheet_details',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
                "timesheetUri": "{{ result('get_timesheet_for_date2').timesheet.uri }}"
            }
        )

        get_startdate_of_next_timesheet = rail.PythonOperator(
            task_id = "get_startdate_of_next_timesheet",
            python_callable=python_callable.get_startday_of_nexttimesheet
        )

        get_country_lookup_variable = rail.GetVariableOperator(
            task_id='get_country_lookup_variable',
            name="{{ result('create_country_lookup').name }}"
        )

        get_location_lookup_variable = rail.GetVariableOperator(
            task_id='get_location_lookup_variable',
            name="{{ result('create_location_lookup').name }}"
        )

        get_workersubshift_lookup_variable = rail.GetVariableOperator(
            task_id='get_workersubshift_lookup_variable',
            name="{{ result('create_workersubshift_lookup').name }}"
        )

        search_momentive_mapper_values = rail.PythonOperator(
            task_id = "search_momentive_mapper_values",
            python_callable=python_callable.search_momentivemapper_workertype_country
        )

        search_entry_in_mapper_for_employeetype = rail.PythonOperator(
            task_id = "search_entry_in_mapper_for_employeetype",
            python_callable=python_callable.search_in_mapper_for_employeetype
        )

        get_effectiveusergroupmembership = rail.RepliconServiceOperator(
            task_id = "get_effectiveusergroupmembership",
            endpoint = "/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        validate_employeetype = rail.IfOperator(
            task_id='validate_employeetype',
            test="{{ result('get_effectiveusergroupmembership').employeeTypes | is_truthy and \
                result('get_effectiveusergroupmembership').employeeTypes[0].employeeType.employeeType | is_truthy and \
                result('get_effectiveusergroupmembership').employeeTypes[0].employeeType.employeeType.displayText | is_truthy and \
                    result('get_effectiveusergroupmembership').employeeTypes[0].employeeType.employeeType.displayText != \
                        result('search_entry_in_mapper_for_employeetype')['value'] }}",
            yes_task="get_all_employee_type",
            no_task="get_shift_lookup_variable",
        )

        get_all_employee_type = rail.RepliconServiceOperator(
            task_id="get_all_employee_type",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response,'displayText', rail.result(
                    'search_entry_in_mapper_for_employeetype')['value'], 'uri', '')
        )

        if_employeetype_present = rail.IfOperator(
            task_id='if_employeetype_present',
            test="{{ result('get_all_employee_type') | is_truthy }}",
            yes_task="update_employeetype_group",
            no_task="log_employeetype_not_updated",
        )

        log_employeetype_not_updated = rail.WriteLogOperator(
            task_id='log_employeetype_not_updated',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : """Employee type was not updated as the employee type 
                '{{ result('search_entry_in_mapper_for_employeetype').value }}' not found in Replicon """
            }
        )

        update_employeetype_group = rail.RepliconServiceOperator(
            task_id='update_employeetype_group',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.update_employeetypegrp_payload
        )

        update_workertype_change_variable_194 = rail.SetVariableOperator(
            task_id='update_workertype_change_variable_194',
            append=False,
            name='{{ result("create_workertype_change").name }}',
            value='true'
        )

        log_employeetype_updated = rail.WriteLogOperator(
            task_id='log_employeetype_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Employee type updated"
            }
        )

        get_shift_lookup_variable = rail.GetVariableOperator(
            task_id='get_shift_lookup_variable',
            name="{{ result('create_shift_lookup').name }}"
        )

        usermappings_mapper = rail.PythonOperator(
            task_id = "usermappings_mapper",
            python_callable=python_callable.user_mappings_mapper,
            op_args=['{{ dag_run.conf.workertype }}','{{ dag_run.conf.exemptionstatus }}','{{ dag_run.conf.gender }}','update']
        )

        if_payrule_present_in_usermapping = rail.IfOperator(
            task_id='if_payrule_present_in_usermapping',
            test="{{ result('usermappings_mapper').payrule | is_truthy }}",
            yes_task="check_payrulescriptschedule_present_for_user",
            no_task="if_language_present_in_usermappings",
        )

        check_payrulescriptschedule_present_for_user = rail.IfOperator(
            task_id="check_payrulescriptschedule_present_for_user",
            test="{{ result('get_user_data')[0].payRuleScriptSchedule | length > 0 }}",
            yes_task="get_current_payrule_uri",
            no_task="if_language_present_in_usermappings"
        )

        get_current_payrule_uri = rail.PythonOperator(
            task_id = "get_current_payrule_uri",
            python_callable=lambda: get_current_data('payRuleScriptSchedule','payRuleScript')
        )

        if_payrule_uri_mismatch = rail.IfOperator(
            task_id="if_payrule_uri_mismatch",
            test="{{ result('get_current_payrule_uri').uri | is_falsy or \
                result('get_current_payrule_uri').text != result('usermappings_mapper').payrule }}",
            yes_task="get_req_payrule_script",
            no_task="if_language_present_in_usermappings"
        )

        get_req_payrule_script = rail.RepliconServiceOperator(
            task_id='get_req_payrule_script',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('usermappings_mapper')['payrule'], 'uri', '')
        )

        if_get_req_payrule_script_present = rail.IfOperator(
            task_id="if_get_req_payrule_script_present",
            test="{{ result('get_req_payrule_script') | is_truthy }}",
            yes_task="update_payrule",
            no_task="if_language_present_in_usermappings"
        )

        update_payrule = rail.RepliconServiceOperator(
            task_id = "update_payrule",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.payrule_update_payload
        )

        log_payrule_updated = rail.WriteLogOperator(
            task_id='log_payrule_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Pay rule updated"
            }
        )

        if_language_present_in_usermappings = rail.IfOperator(
            task_id='if_language_present_in_usermappings',
            test="{{ result('usermappings_mapper').language | is_truthy }}",
            yes_task="get_language_for_user",
            no_task="if_timezone_present_in_usermappings",
        )

        get_language_for_user = rail.RepliconServiceOperator(
            task_id="get_language_for_user",
            endpoint="/services/InternationalizationService1.svc/GetLanguageForUser",
            data = {
                'userUri': "{{ dag_run.conf.useruri}}"
            }
        )

        if_language_mismatch = rail.IfOperator(
            task_id='if_language_mismatch',
            test="{{ result('usermappings_mapper').language != result('get_language_for_user').uri }}",
            yes_task="update_langauge_for_user",
            no_task="if_timezone_present_in_usermappings",
        )

        update_langauge_for_user = rail.RepliconServiceOperator(
            task_id='update_langauge_for_user',
            endpoint="/services/InternationalizationService1.svc/UpdateLanguageForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "languageUri": "{{ result('usermappings_mapper').language }}"
            }
        )

        if_timezone_present_in_usermappings = rail.IfOperator(
            task_id='if_timezone_present_in_usermappings',
            test="{{ result('usermappings_mapper').timezone | is_truthy }}",
            yes_task="get_all_timezones",
            no_task="if_timesheet_approvalpath_mismatch",
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id="get_all_timezones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'ianaName', rail.result('usermappings_mapper')['timezone'], 'uri', '')
        )

        if_timezone_mismatch = rail.IfOperator(
            task_id='if_timezone_mismatch',
            test="{{ result('usermappings_mapper').timezone != result('get_user_data')[0].timeZone.ianaName and \
                result('get_all_timezones') | is_truthy }}",
            yes_task="update_timezone_user",
            no_task="if_timesheet_approvalpath_mismatch",
        )

        update_timezone_user = rail.RepliconServiceOperator(
            task_id='update_timezone_user',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                "timeZoneUri": "{{ result('get_all_timezones') }}"
            }
        )

        if_timesheet_approvalpath_mismatch = rail.IfOperator(
            task_id='if_timesheet_approvalpath_mismatch',
            test="{{ result('usermappings_mapper').timesheetapprovalpath | is_truthy and \
                (result('get_user_data')[0].timesheetApprovalPath | is_falsy or \
                    result('usermappings_mapper').timesheetapprovalpath != result('get_user_data')[0].timesheetApprovalPath.displayText) }}",
            yes_task="get_timesheet_approvalpathuri",
            no_task="if_holidaycalendar_mismatch",
        )

        get_timesheet_approvalpathuri = rail.RepliconServiceOperator(
            task_id='get_timesheet_approvalpathuri',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('usermappings_mapper')['timesheetapprovalpath'], 'uri', '')
        )

        update_timesheet_approvalpath_user = rail.RepliconServiceOperator(
            task_id='update_timesheet_approvalpath_user',
            endpoint='/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser',
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                'approvalPathUri': "{{ result('get_timesheet_approvalpathuri') }}"
            }
        )

        log_timesheet_approvalpath_updated = rail.WriteLogOperator(
            task_id='log_timesheet_approvalpath_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Timesheet approval path updated"
            }
        )

        if_holidaycalendar_mismatch = rail.IfOperator(
            task_id='if_holidaycalendar_mismatch',
            test="{{ result('usermappings_mapper').holidaycalendar | is_truthy and \
                (result('get_user_data')[0].holidayCalendar | is_falsy or \
                    result('usermappings_mapper').holidaycalendar != result('get_user_data')[0].holidayCalendar.displayText) }}",
            yes_task="get_required_holidaycalendar_uri",
            no_task="get_policysets",
        )

        get_required_holidaycalendar_uri = rail.RepliconServiceOperator(
            task_id='get_required_holidaycalendar_uri',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', rail.result('usermappings_mapper')['holidaycalendar'], 'uri', '')
        )

        if_holidaycalendar_uri_present = rail.IfOperator(
            task_id='if_holidaycalendar_uri_present',
            test="{{ result('get_required_holidaycalendar_uri') | is_truthy }}",
            yes_task="update_holidaycalendar",
            no_task="get_policysets",
        )

        update_holidaycalendar = rail.RepliconServiceOperator(
            task_id='update_holidaycalendar',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": "{{ result('get_required_holidaycalendar_uri') }}"
            }
        )

        log_holidaycalendar_updated = rail.WriteLogOperator(
            task_id='log_holidaycalendar_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Holiday calendar updated"
            }
        )

        get_policysets = rail.RepliconServiceOperator(
            task_id='get_policysets',
            endpoint='/services/PolicySetService1.svc/GetAllPolicySets',
            data_handler=lambda response: {
                'existing_timesheettemplate' : rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('get_user_data')[0]['timesheetTemplate'], 'uri', ''),
                'existing_punchentry': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'UK Punch Policy', 'uri', ''),
                'timeoff': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Time Off', 'uri', ''),
                'timesheet': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('usermappings_mapper')['timesheet'], 'uri', ''),
                'punchentry': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('usermappings_mapper')['punchentrypolicy'], 'uri', '')
            }
        )

        get_changeinshift_variable = rail.GetVariableOperator(
            task_id='get_changeinshift_variable',
            name="{{ result('create_changein_shift').name }}"
        )

        if_changeinshift_yes_and_legalentity_mom_perf_mat_ltd = rail.IfOperator(
            task_id='if_changeinshift_yes_and_legalentity_mom_perf_mat_ltd',
            test="{{ result('get_changeinshift_variable').value == 'yes' and \
                dag_run.conf.legalentity == 'MOMENTIVE PERFORMANCE MATERIALS LTD' }}",
            yes_task="remove_timesheet_template",
            no_task="if_legalentity_is_mom_perf_mat_kor",
        )

        remove_timesheet_template = rail.RepliconServiceOperator(
            task_id = "remove_timesheet_template",
            endpoint = "/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data = {
                "userUri":"{{ dag_run.conf.useruri}}",
                "policySetUri": "{{ result('get_policysets').existing_timesheettemplate }}" 
                }
        )

        remove_punchentry_template = rail.RepliconServiceOperator(
            task_id = "remove_punchentry_template",
            endpoint = "/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data = {
                "userUri":"{{ dag_run.conf.useruri}}",
                "policySetUri": "{{ result('get_policysets').existing_punchentry }}" 
                }
        )

        if_legalentity_is_mom_perf_mat_kor = rail.IfOperator(
            task_id='if_legalentity_is_mom_perf_mat_kor',
            test="{{ dag_run.conf.legalentity == 'MOMENTIVE PERFORMANCE MATERIALS KOREA CO., LTD.' }}",
            yes_task="get_workertype_change_variable",
            no_task="if_paygroup_mismatch",
        )

        get_workertype_change_variable = rail.GetVariableOperator(
            task_id='get_workertype_change_variable',
            name="{{ result('create_workertype_change').name }}"
        )

        if_worktypechange_is_true = rail.IfOperator(
            task_id='if_worktypechange_is_true',
            test="{{ result('get_workertype_change_variable').value == 'true' }}",
            yes_task="if_worker_type_is_not_contingentworker",
            no_task="if_timesheet_mismatch",
        )

        if_worker_type_is_not_contingentworker = rail.IfOperator(
            task_id='if_worker_type_is_not_contingentworker',
            test="{{ dag_run.conf.workertype != 'Contingent Worker' }}",
            yes_task="update_timeoff_template",
            no_task="remove_timeoff_template",
        )

        update_timeoff_template = rail.RepliconServiceOperator(
            task_id='update_timeoff_template',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('get_policysets').timeoff }}"
            }
        )

        assign_policyDataAccessScopes_timeoff = rail.RepliconServiceOperator(
            task_id='assign_policyDataAccessScopes_timeoff',
            endpoint='/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser',
            data=request_payload.assign_policydataaccessscope_department
        )

        log_timeofftemplate_updated = rail.WriteLogOperator(
            task_id='log_timeofftemplate_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Time off template was  updated"
            }
        )

        remove_timeoff_template = rail.RepliconServiceOperator(
            task_id = "remove_timeoff_template",
            endpoint = "/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data = {
                "userUri":"{{ dag_run.conf.useruri}}",
                "policySetUri": "{{ result('get_policysets').timeoff }}" 
                }
        )

        log_timeofftemplate_removed = rail.WriteLogOperator(
            task_id='log_timeofftemplate_removed',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Time off template was  removed"
            }
        )

        if_timesheet_mismatch = rail.IfOperator(
            task_id='if_timesheet_mismatch',
            test="{{ result('usermappings_mapper').timesheet | is_truthy and \
                (result('get_user_data')[0].timesheetTemplate | is_falsy or \
                    result('usermappings_mapper').timesheet != result('get_user_data')[0].timesheetTemplate.displayText) }}",
            yes_task="if_timesheet_templateuri_present",
            no_task="if_paygroup_mismatch",
        )

        if_timesheet_templateuri_present = rail.IfOperator(
            task_id='if_timesheet_templateuri_present',
            test="{{ result('get_policysets').timesheet | is_truthy }}",
            yes_task="update_timesheet_template",
            no_task="log_timesheet_execption",
        )

        log_timesheet_execption = rail.WriteLogOperator(
            task_id='log_timesheet_execption',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Timesheet template not updated since '{{ result('usermappings_mapper').timesheet }} not found or is disabled in Replicon"
            }
        )

        update_timesheet_template = rail.RepliconServiceOperator(
            task_id='update_timesheet_template',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('get_policysets').timesheet }}"
            }
        )

        log_timesheettemplate_updated = rail.WriteLogOperator(
            task_id='log_timesheettemplate_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Timesheettemplate was  updated"
            }
        )

        if_punchentry_policy_present = rail.IfOperator(
            task_id='if_punchentry_policy_present',
            test="{{ result('usermappings_mapper').punchentrypolicy | is_truthy }}",
            yes_task="get_users_current_timesheet_end_date",
            no_task="if_paygroup_mismatch",
        )

        get_users_current_timesheet_end_date = rail.RepliconServiceOperator(
            task_id="get_users_current_timesheet_end_date",
            endpoint="/services/TimesheetService1.svc/GetNextTimesheetDueDate",
            data=lambda dag_run :{
                "userUri": dag_run.conf['useruri'],
                "asOfDate": request_payload.effective_dateformat_payload(datetime.now())
            }
        )

        if_paygroup_mismatch = rail.IfOperator(
            task_id='if_paygroup_mismatch',
            test="{{ dag_run.conf.paygroup | is_truthy and \
                (result('get_effectiveusergroupmembership').serviceCenters | is_falsy or \
                    (result('get_effectiveusergroupmembership').serviceCenters | is_truthy and \
                        dag_run.conf.paygroup != result('get_effectiveusergroupmembership').serviceCenters[0].serviceCenter.serviceCenter.displayText)) }}",
            yes_task="validate_paygroupuri",
            no_task="if_costcenter_mismatch",
        )

        validate_paygroupuri = rail.IfOperator(
            task_id='validate_paygroupuri',
            test=lambda dag_run: bool(dag_run.conf['paygroupuri'] and 'urn' in dag_run.conf['paygroupuri']),
            yes_task="update_servicecenter_group",
            no_task="if_costcenter_mismatch",
        )

        update_servicecenter_group = rail.RepliconServiceOperator(
            task_id='update_servicecenter_group',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.update_servicecenter_payload
        )

        log_paygroup_updated = rail.WriteLogOperator(
            task_id='log_paygroup_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Paygroup updated"
            }
        )

        if_costcenter_mismatch = rail.IfOperator(
            task_id='if_costcenter_mismatch',
            test="{{ dag_run.conf.cost_center | is_truthy and \
                (result('get_effectiveusergroupmembership').costCenters | is_falsy or \
                    (result('get_effectiveusergroupmembership').costCenters | is_truthy and \
                        dag_run.conf.cost_center != result('get_effectiveusergroupmembership').costCenters[0].costCenter.costCenter.displayText)) }}",
            yes_task="if_costcenter_uri_present",
            no_task="if_legalentity_mismatch",
        )

        if_costcenter_uri_present = rail.IfOperator(
            task_id='if_costcenter_uri_present',
            test="{{ dag_run.conf.costcenteruri | is_truthy }}",
            yes_task="update_costcentergroup",
            no_task="log_costcenter_exception",
        )

        update_costcentergroup = rail.RepliconServiceOperator(
            task_id='update_costcentergroup',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.get_update_costcenter_param
        )

        log_costcenter_updated = rail.WriteLogOperator(
            task_id='log_costcenter_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Cost center updated"
            }
        )

        log_costcenter_exception = rail.WriteLogOperator(
            task_id='log_costcenter_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Cost center was not updated as the Cost center '{{ dag_run.conf.cost_center }} not found in Replicon"
            }
        )

        if_legalentity_mismatch = rail.IfOperator(
            task_id='if_legalentity_mismatch',
            test="{{ dag_run.conf.legalentity | is_truthy and \
                (result('get_effectiveusergroupmembership').divisions | is_falsy or \
                    (result('get_effectiveusergroupmembership').divisions | is_truthy and \
                        dag_run.conf.legalentity != result('get_effectiveusergroupmembership').divisions[0].division.division.displayText)) }}",
            yes_task="if_legalentity_uri_present",
            no_task="if_location_mismatch",
        )

        if_legalentity_uri_present = rail.IfOperator(
            task_id='if_legalentity_uri_present',
            test="{{ dag_run.conf.legalentityuri | is_truthy }}",
            yes_task="update_divisiongroup",
            no_task="log_legal_enity_exception",
        )

        update_divisiongroup = rail.RepliconServiceOperator(
            task_id='update_divisiongroup',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.apply_user_modifications_division
        )

        log_legal_enity_updated = rail.WriteLogOperator(
            task_id='log_legal_enity_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Legal entity updated"
            }
        )

        log_legal_enity_exception = rail.WriteLogOperator(
            task_id='log_legal_enity_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Legal entity  was not updated as the Legal entity (division) '{{ dag_run.conf.legalentity }} not found in Replicon"
            }
        )

        if_location_mismatch = rail.IfOperator(
            task_id='if_location_mismatch',
            test="{{ dag_run.conf.location | is_truthy and \
                (result('get_effectiveusergroupmembership').departments | is_falsy or \
                    (result('get_effectiveusergroupmembership').departments | is_truthy and \
                        dag_run.conf.location != result('get_effectiveusergroupmembership').departments[0].department.department.displayText)) }}",
            yes_task="if_departmentgroupuri_present",
            no_task="get_location_change_variable",
        )

        if_departmentgroupuri_present = rail.IfOperator(
            task_id='if_departmentgroupuri_present',
            test="{{ dag_run.conf.departmentgroupuri | is_truthy }}",
            yes_task="update_departmentgroup",
            no_task="log_dept_grp_exception",
        )

        update_departmentgroup = rail.RepliconServiceOperator(
            task_id='update_departmentgroup',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.department_update_payload
        )

        assign_policyDataAccessScopes_department = rail.RepliconServiceOperator(
            task_id='assign_policyDataAccessScopes_department',
            endpoint='/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser',
            data=request_payload.assign_policydataaccessscope_department
        )

        log_dept_grp_updated = rail.WriteLogOperator(
            task_id='log_dept_grp_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Department group updated"
            }
        )

        update_location_change_variable_282 = rail.SetVariableOperator(
            task_id='update_location_change_variable_282',
            append=False,
            name='{{ result("create_location_change").name }}',
            value='true'
        )

        log_dept_grp_exception = rail.WriteLogOperator(
            task_id='log_dept_grp_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Department group was not updated as the department(location) '{{ dag_run.conf.location }} not found in Replicon"
            }
        )

        get_location_change_variable = rail.GetVariableOperator(
            task_id='get_location_change_variable',
            name="{{ result('create_location_change').name }}"
        )

        get_Exemptionstatus_change_variable = rail.GetVariableOperator(
            task_id='get_Exemptionstatus_change_variable',
            name="{{ result('create_Exemptionstatus_change').name }}"
        )

        get_workertype_change_variable_285 = rail.GetVariableOperator(
            task_id='get_workertype_change_variable_285',
            name="{{ result('create_workertype_change').name }}"
        )

        if_location_exemption_workertype_is_true = rail.IfOperator(
            task_id='if_location_exemption_workertype_is_true',
            test="{{ result('get_location_change_variable').value == 'true' or \
                result('get_Exemptionstatus_change_variable').value == 'true' or \
                    result('get_workertype_change_variable_285').value == 'true' }}",
            yes_task="update_timeofftrigger_286",
            no_task="if_schedule_present_in_usermapping",
        )

        update_timeofftrigger_286 = rail.SetVariableOperator(
            task_id='update_timeofftrigger_286',
            append=False,
            name='{{ result("create_timeofftrigger").name }}',
            value='true'
        )

        if_schedule_present_in_usermapping = rail.IfOperator(
            task_id='if_schedule_present_in_usermapping',
            test="{{ result('usermappings_mapper').schedule | is_truthy }}",
            yes_task="check_schedulepolicies_present_for_user",
            no_task="if_activities_not_present",
        )

        check_schedulepolicies_present_for_user = rail.IfOperator(
            task_id="check_schedulepolicies_present_for_user",
            test="{{ result('get_user_data')[0].schedulePolicies | length > 0 }}",
            yes_task="get_current_schedulepolicy_uri",
            no_task="if_activities_not_present"
        )

        get_current_schedulepolicy_uri = rail.PythonOperator(
            task_id = "get_current_schedulepolicy_uri",
            python_callable=lambda: get_current_data('schedulePolicies','officeSchedule')
        )

        if_schedule_uri_mismatch = rail.IfOperator(
            task_id="if_schedule_uri_mismatch",
            test="{{ result('get_current_schedulepolicy_uri').uri | is_falsy or \
                result('get_current_schedulepolicy_uri').text != result('usermappings_mapper').schedule }}",
            yes_task="get_req_schedule_script",
            no_task="if_activities_not_present"
        )

        get_req_schedule_script = rail.RepliconServiceOperator(
            task_id='get_req_schedule_script',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('usermappings_mapper')['schedule'], 'uri', '')
        )

        if_get_req_schedule_script_present = rail.IfOperator(
            task_id="if_get_req_schedule_script_present",
            test="{{ result('get_req_schedule_script') | is_truthy }}",
            yes_task="update_schedule_policy",
            no_task="if_schedule_equals_shift"
        )

        update_schedule_policy = rail.RepliconServiceOperator(
            task_id = "update_schedule_policy",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.schedule_update_payload
        )

        if_schedule_equals_shift = rail.IfOperator(
            task_id="if_schedule_equals_shift",
            test="{{ result('usermappings_mapper').schedule == 'Shift' }}",
            yes_task="update_schedule_policy_300",
            no_task="if_activities_not_present"
        )

        update_schedule_policy_300 = rail.RepliconServiceOperator(
            task_id = "update_schedule_policy_300",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.schedule_update_payload
        )

        log_schedule_updated = rail.WriteLogOperator(
            task_id='log_schedule_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Schedule updated"
            }
        )

        if_activities_not_present = rail.IfOperator(
            task_id="if_activities_not_present",
            test="{{ result('usermappings_mapper').activities | is_falsy }}",
            yes_task="put_activity_assignments_for_user_303",
            no_task="get_enabled_activities"
        )

        put_activity_assignments_for_user_303 = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_303',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "activityUris": []
            }
        )

        get_enabled_activities = rail.RepliconServiceOperator(
            task_id='get_enabled_activities',
            endpoint="/services/ActivityService1.svc/GetEnabledActivities",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('usermappings_mapper')['activities'], 'uri', '')
        )

        put_activity_assignments_for_user_307 = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_307',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "activityUris": [rail.result('get_enabled_activities')]
            }
        )

        log_activity_updated = rail.WriteLogOperator(
            task_id='log_activity_updated',
            log = "{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value" : "Activity updated"
            }
        )

        get_timeofftrigger_variable = rail.GetVariableOperator(
            task_id='get_timeofftrigger_variable',
            name="{{ result('create_timeofftrigger').name }}"
        )

        if_timeoff_trigger_true_and_timeoff_present = rail.IfOperator(
            task_id="if_timeoff_trigger_true_and_timeoff_present",
            test="{{ result('usermappings_mapper').timeoffs | is_truthy and \
                result('get_timeofftrigger_variable').value == 'true' }}",
            yes_task="trigger_update_user_timeoff",
            no_task="write_log_entry"
        )

        trigger_update_user_timeoff = rail.TriggerDagRunOperator(
            task_id='trigger_update_user_timeoff',
            trigger_dag_id=config.momentive_othercountries_user_sync_update_user_timeoff_assign_child_dag_id,
            conf=request_payload.trigger_updateuser_timeoff,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_update_user_timeoff = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_user_timeoff',
            dag_runs='{{ result("trigger_update_user_timeoff") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        write_log_entry = rail.WriteCSVFileOperator(
            task_id='write_log_entry',
            source="{{ result('log_entries') }}",
            header=['value'],
            row=lambda item: [
                item['properties']['value']
            ]
        )

        write_log_exception = rail.WriteCSVFileOperator(
            task_id='write_log_exception',
            source="{{ result('exception_log') }}",
            header=['value'],
            row=lambda item: [
                item['properties']['value']
            ]
        )

        log_user_import = rail.WriteLogOperator(
            task_id='log_user_import',
            log = "{{ dag_run.conf.logger }}",
            message="na",
            severity="Skipped",
            properties=python_callable.get_status_and_details_for_update
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ dag_run.conf.logger}}',
            trigger_rule='one_failed',
            message="Error",
            severity="Error",
            properties=lambda dag_run: {
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "action": "Update",
                "status": "Error",
                'details': "User partially updated," + "{{ get_error_message() }}",
                'country': rail.result('get_country_lookup_variable')['value']
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> create_workertype_change

        create_workertype_change >> create_workshift_change >> create_Exemptionstatus_change >> create_location_change >> create_workersubtype_change >> \
            create_timeofftrigger >> exception_log >> log_entries >> get_input_validation_log >> if_input_validation_log_present

        if_input_validation_log_present >> rail.Label('Yes') >> log_user_import_not_created >> write_log_entry
        if_input_validation_log_present >> rail.Label('No') >> get_user_data >> if_regireupdate_equals_rehire

        if_regireupdate_equals_rehire >> rail.Label('Yes') >> update_timeofftrigger_17 >> validate_hiredate_and_startdate >> \
            if_validate_hiredate_and_startdate_is_false_20
        if_regireupdate_equals_rehire >> rail.Label('No') >> validate_hiredate_and_startdate >> if_validate_hiredate_and_startdate_is_false_20

        if_validate_hiredate_and_startdate_is_false_20 >> rail.Label('Yes') >> remove_end_date_and_update_rehire_date >> enable_userprofile >> \
            log_user_enabled >> if_firstname_mismatch
        if_validate_hiredate_and_startdate_is_false_20 >> rail.Label('No') >> if_firstname_mismatch

        if_firstname_mismatch >> rail.Label('Yes') >> update_firstname >> log_first_name_updated >> if_lastname_mismatch
        if_firstname_mismatch >> rail.Label('No') >> if_lastname_mismatch

        if_lastname_mismatch >> rail.Label('Yes') >> update_lastname >> log_last_name_updated >> if_email_mismatch
        if_lastname_mismatch >> rail.Label('No') >> if_email_mismatch

        if_email_mismatch >> rail.Label('Yes') >> update_email_address >> log_email_updated >> if_regireupdate_notequals_rehire
        if_email_mismatch >> rail.Label('No') >> if_regireupdate_notequals_rehire

        if_regireupdate_notequals_rehire >> rail.Label('Yes') >> validate_termination_date_and_enddate >> if_terminationdate_present_and_not_equal_enddate
        if_regireupdate_notequals_rehire >> rail.Label('No') >> get_required_user_customfields

        if_terminationdate_present_and_not_equal_enddate >> rail.Label('Yes') >> update_end_date_39 >> log_termination_date_updated >> \
            get_required_user_customfields
        if_terminationdate_present_and_not_equal_enddate >> rail.Label('No') >> get_required_user_customfields

        get_required_user_customfields >> get_user_udf_values >> if_CF_Date_of_Birth_MM_DD_YYYY_present

        if_CF_Date_of_Birth_MM_DD_YYYY_present >> rail.Label('Yes') >> validate_CF_Date_of_Birth_MM_DD_YYYY
        if_CF_Date_of_Birth_MM_DD_YYYY_present >> rail.Label('No') >> if_businesstitle_present_and_mismatch

        validate_CF_Date_of_Birth_MM_DD_YYYY >> rail.Label('Yes') >> check_dob_mismatch
        validate_CF_Date_of_Birth_MM_DD_YYYY >> rail.Label('No') >> log_birthdate_invalid >> if_businesstitle_present_and_mismatch

        check_dob_mismatch >> rail.Label('Yes') >> update_dob_udf >> log_dob_updated >> if_businesstitle_present_and_mismatch
        check_dob_mismatch >> rail.Label('No') >> if_businesstitle_present_and_mismatch

        if_businesstitle_present_and_mismatch >> rail.Label('Yes') >> update_title_udf >> log_title_updated >> if_yearofservice_present_and_mismatch
        if_businesstitle_present_and_mismatch >> rail.Label('No') >> if_yearofservice_present_and_mismatch

        if_yearofservice_present_and_mismatch >> rail.Label('Yes') >> update_yearsofservice_udf >> log_yearsofservice_updated >> if_fieldhr_present_and_mismatch
        if_yearofservice_present_and_mismatch >> rail.Label('No') >> if_fieldhr_present_and_mismatch

        if_fieldhr_present_and_mismatch >> rail.Label('Yes') >> update_fieldhr_udf >> log_fieldhr_updated >> if_contsrvcdate_present_and_mismatch
        if_fieldhr_present_and_mismatch >> rail.Label('No') >> if_contsrvcdate_present_and_mismatch

        if_contsrvcdate_present_and_mismatch >> rail.Label('Yes') >> update_contsrvcdate_udf >> log_contsrvcdate_updated >> \
            if_timeoffservdate_present_and_mismatch
        if_contsrvcdate_present_and_mismatch >> rail.Label('No') >> if_timeoffservdate_present_and_mismatch

        if_timeoffservdate_present_and_mismatch >> rail.Label('Yes') >> update_timeoffservdate_udf >> log_timeoffservdate_updated >> \
            if_gender_present_and_mismatch
        if_timeoffservdate_present_and_mismatch >> rail.Label('No') >> if_gender_present_and_mismatch

        if_gender_present_and_mismatch >> rail.Label('Yes') >> update_gender_udf >> if_function_present_and_mismatch
        if_gender_present_and_mismatch >> rail.Label('No') >> if_function_present_and_mismatch

        if_function_present_and_mismatch >> rail.Label('Yes') >> update_function_udf >> if_workersubtype_present_and_mismatch
        if_function_present_and_mismatch >> rail.Label('No') >> if_workersubtype_present_and_mismatch

        if_workersubtype_present_and_mismatch >> rail.Label('Yes') >> get_workersubtype_dropdowns >> if_get_workersubtype_dropdowns_uri_present
        if_workersubtype_present_and_mismatch >> rail.Label('No') >> create_changein_shift

        if_get_workersubtype_dropdowns_uri_present >> rail.Label('Yes') >> update_worker_subtype_udf >> log_workersubtype_updated >> create_changein_shift
        if_get_workersubtype_dropdowns_uri_present >> rail.Label('No') >> create_changein_shift

        create_changein_shift >> if_workshift_present_and_mismatch

        if_workshift_present_and_mismatch >> rail.Label('Yes') >> get_workshift_dropdowns >> if_get_workershift_dropdowns_uri_present
        if_workshift_present_and_mismatch >> rail.Label('No') >> getdata_sup_emp_grp_dept_grp

        if_get_workershift_dropdowns_uri_present >> rail.Label('Yes') >> update_workshift_udf >> if_user_workshift_rota_mismatch
        if_get_workershift_dropdowns_uri_present >> rail.Label('No') >> getdata_sup_emp_grp_dept_grp

        if_user_workshift_rota_mismatch >> rail.Label('Yes') >> update_changeinshift_variable_112 >> getdata_sup_emp_grp_dept_grp
        if_user_workshift_rota_mismatch >> rail.Label('No') >> getdata_sup_emp_grp_dept_grp

        getdata_sup_emp_grp_dept_grp >> get_all_permissionsets >> if_manager_id_present

        if_manager_id_present >> rail.Label('Yes') >> if_managerid_equals_workrefempid
        if_manager_id_present >> rail.Label('No') >> compare_to_today

        if_managerid_equals_workrefempid >> rail.Label('Yes') >> log_supervisor_sameas_user >> compare_to_today
        if_managerid_equals_workrefempid >> rail.Label('No') >> search_for_user_with_empid >> check_if_multiple_manageruseruri_present

        check_if_multiple_manageruseruri_present >> rail.Label('Yes') >> log_multiple_user_for_same_managerid >> compare_to_today
        check_if_multiple_manageruseruri_present >> rail.Label('No') >> check_if_single_manageruseruri_present

        check_if_single_manageruseruri_present >> rail.Label('Yes') >> get_manager_details >> if_manager_details_present_and_enabled
        check_if_single_manageruseruri_present >> rail.Label('No') >> log_supervisor_assignment >> compare_to_today

        if_manager_details_present_and_enabled >> rail.Label('Yes') >> get_assigned_permissionset_foruser >> if_supervisor_permission_not_assigned
        if_manager_details_present_and_enabled >> rail.Label('No') >> log_supervisor_assignment >> compare_to_today

        if_supervisor_permission_not_assigned >> rail.Label('Yes') >> add_missing_supervisor_permission >> if_search_user_supervisor_null_139
        if_supervisor_permission_not_assigned >> rail.Label('No') >> if_search_user_supervisor_null_139

        if_search_user_supervisor_null_139 >> rail.Label('Yes') >> update_initial_supervisor >> log_initial_supervisor_added >> \
            if_search_user_supervisor_not_null_142
        if_search_user_supervisor_null_139 >> rail.Label('No') >> if_search_user_supervisor_not_null_142

        if_search_user_supervisor_not_null_142 >> rail.Label('Yes') >> get_current_supervisor_empid >> if_current_supervisor_empid_mismatch_managerid
        if_search_user_supervisor_not_null_142 >> rail.Label('No') >> compare_to_today

        if_current_supervisor_empid_mismatch_managerid >> rail.Label('Yes') >> update_supervisor_assignment_over_daterange >> log_supervisor_updated >> \
            compare_to_today
        if_current_supervisor_empid_mismatch_managerid >> rail.Label('No') >> compare_to_today

        compare_to_today >> validate_exemptioneff_date_155

        validate_exemptioneff_date_155 >> rail.Label('Yes') >> update_exemption_change_variable_156 >> validate_workshift_change_effect_date_157
        validate_exemptioneff_date_155 >> rail.Label('No') >> validate_workshift_change_effect_date_157

        validate_workshift_change_effect_date_157 >> rail.Label('Yes') >> update_workshift_change_variable_158 >> validate_effect_date_of_workertype_159
        validate_workshift_change_effect_date_157 >> rail.Label('No') >> validate_effect_date_of_workertype_159

        validate_effect_date_of_workertype_159 >> rail.Label('Yes') >> update_workertype_change_variable_160 >> \
            if_location_present_and_validate_cflrv_loc_changedate
        validate_effect_date_of_workertype_159 >> rail.Label('No') >> if_location_present_and_validate_cflrv_loc_changedate

        if_location_present_and_validate_cflrv_loc_changedate >> rail.Label('Yes') >> update_location_change_variable_163 >> create_location_lookup
        if_location_present_and_validate_cflrv_loc_changedate >> rail.Label('No') >> create_location_lookup

        create_location_lookup >> create_country_lookup >> create_shift_lookup >> if_workshift_equals_prod_rota_or_nonrota

        if_workshift_equals_prod_rota_or_nonrota >> rail.Label('Yes') >> update_shift_lookup_to_workshift >> create_workersubshift_lookup
        if_workshift_equals_prod_rota_or_nonrota >> rail.Label('No') >> update_shift_lookup_to_any >> create_workersubshift_lookup

        create_workersubshift_lookup >> validate_hiredate_to_today

        validate_hiredate_to_today >> rail.Label('Yes') >> get_timesheet_for_date2 >> if_get_timesheet_for_date2_uri_present
        validate_hiredate_to_today >> rail.Label('No') >> get_country_lookup_variable

        if_get_timesheet_for_date2_uri_present >> rail.Label('Yes') >> get_timesheet_details >> get_startdate_of_next_timesheet >> get_country_lookup_variable
        if_get_timesheet_for_date2_uri_present >> rail.Label('No') >> get_country_lookup_variable

        get_country_lookup_variable >> get_location_lookup_variable >> get_workersubshift_lookup_variable >> search_momentive_mapper_values >> \
            search_entry_in_mapper_for_employeetype >> get_effectiveusergroupmembership >> validate_employeetype

        validate_employeetype >> rail.Label('Yes') >> get_all_employee_type >> if_employeetype_present
        validate_employeetype >> rail.Label('No') >> get_shift_lookup_variable

        if_employeetype_present >> rail.Label('Yes') >> update_employeetype_group >> update_workertype_change_variable_194 >> log_employeetype_updated >> \
            get_shift_lookup_variable
        if_employeetype_present >> rail.Label('No') >> log_employeetype_not_updated >> get_shift_lookup_variable

        get_shift_lookup_variable >> usermappings_mapper >> if_payrule_present_in_usermapping

        if_payrule_present_in_usermapping >> rail.Label('Yes') >> check_payrulescriptschedule_present_for_user
        if_payrule_present_in_usermapping >> rail.Label('No') >> if_language_present_in_usermappings

        check_payrulescriptschedule_present_for_user >> rail.Label('Yes') >> get_current_payrule_uri >> if_payrule_uri_mismatch
        check_payrulescriptschedule_present_for_user >> rail.Label('No') >> if_language_present_in_usermappings

        if_payrule_uri_mismatch >> rail.Label('Yes') >> get_req_payrule_script >> if_get_req_payrule_script_present
        if_payrule_uri_mismatch >> rail.Label('No') >> if_language_present_in_usermappings

        if_get_req_payrule_script_present >> rail.Label('Yes') >> update_payrule >> log_payrule_updated >> if_language_present_in_usermappings
        if_get_req_payrule_script_present >> rail.Label('No') >> if_language_present_in_usermappings

        if_language_present_in_usermappings >> rail.Label('Yes') >> get_language_for_user >> if_language_mismatch
        if_language_present_in_usermappings >> rail.Label('No') >> if_timezone_present_in_usermappings

        if_language_mismatch >> rail.Label('Yes') >> update_langauge_for_user >> if_timezone_present_in_usermappings
        if_language_mismatch >> rail.Label('No') >> if_timezone_present_in_usermappings

        if_timezone_present_in_usermappings >> rail.Label('Yes') >> get_all_timezones >> if_timezone_mismatch
        if_timezone_present_in_usermappings >> rail.Label('No') >> if_timesheet_approvalpath_mismatch

        if_timezone_mismatch >> rail.Label('Yes') >> update_timezone_user >> if_timesheet_approvalpath_mismatch
        if_timezone_mismatch >> rail.Label('No') >> if_timesheet_approvalpath_mismatch

        if_timesheet_approvalpath_mismatch >> rail.Label('Yes') >> get_timesheet_approvalpathuri >> update_timesheet_approvalpath_user >> \
            log_timesheet_approvalpath_updated >>if_holidaycalendar_mismatch
        if_timesheet_approvalpath_mismatch >> rail.Label('No') >> if_holidaycalendar_mismatch

        if_holidaycalendar_mismatch >> rail.Label('Yes') >> get_required_holidaycalendar_uri >> if_holidaycalendar_uri_present
        if_holidaycalendar_mismatch >> rail.Label('No') >> get_policysets

        if_holidaycalendar_uri_present >> rail.Label('Yes') >> update_holidaycalendar >> log_holidaycalendar_updated >> get_policysets
        if_holidaycalendar_uri_present >> rail.Label('No') >> get_policysets

        get_policysets >> get_changeinshift_variable >> if_changeinshift_yes_and_legalentity_mom_perf_mat_ltd

        if_changeinshift_yes_and_legalentity_mom_perf_mat_ltd >> rail.Label('Yes') >> remove_timesheet_template >> remove_punchentry_template >> \
            if_legalentity_is_mom_perf_mat_kor
        if_changeinshift_yes_and_legalentity_mom_perf_mat_ltd >> rail.Label('No') >> if_legalentity_is_mom_perf_mat_kor

        if_legalentity_is_mom_perf_mat_kor >> rail.Label('Yes') >> get_workertype_change_variable >> if_worktypechange_is_true
        if_legalentity_is_mom_perf_mat_kor >> rail.Label('No') >> if_paygroup_mismatch

        if_worktypechange_is_true >> rail.Label('Yes') >> if_worker_type_is_not_contingentworker
        if_worktypechange_is_true >> rail.Label('No') >> if_timesheet_mismatch

        if_worker_type_is_not_contingentworker >> rail.Label('Yes') >> update_timeoff_template >> assign_policyDataAccessScopes_timeoff >> \
            log_timeofftemplate_updated >> if_timesheet_mismatch
        if_worker_type_is_not_contingentworker >> rail.Label('No') >> remove_timeoff_template >> log_timeofftemplate_removed >> if_timesheet_mismatch

        if_timesheet_mismatch >> rail.Label('Yes') >> if_timesheet_templateuri_present
        if_timesheet_mismatch >> rail.Label('No') >> if_paygroup_mismatch

        if_timesheet_templateuri_present >> rail.Label('Yes') >> update_timesheet_template >> log_timesheettemplate_updated >> if_punchentry_policy_present
        if_timesheet_templateuri_present >> rail.Label('No') >> log_timesheet_execption >> if_paygroup_mismatch

        if_punchentry_policy_present >> rail.Label('Yes') >> get_users_current_timesheet_end_date >> if_paygroup_mismatch
        if_punchentry_policy_present >> rail.Label('No') >> if_paygroup_mismatch

        if_paygroup_mismatch >> rail.Label('Yes') >> validate_paygroupuri
        if_paygroup_mismatch >> rail.Label('No') >> if_costcenter_mismatch

        validate_paygroupuri >> rail.Label('Yes') >> update_servicecenter_group >> log_paygroup_updated >> if_costcenter_mismatch
        validate_paygroupuri >> rail.Label('No') >> if_costcenter_mismatch

        if_costcenter_mismatch >> rail.Label('Yes') >> if_costcenter_uri_present
        if_costcenter_mismatch >> rail.Label('No') >> if_legalentity_mismatch

        if_costcenter_uri_present >> rail.Label('Yes') >> update_costcentergroup >> log_costcenter_updated >> if_legalentity_mismatch
        if_costcenter_uri_present >> rail.Label('No') >> log_costcenter_exception >> if_legalentity_mismatch

        if_legalentity_mismatch >> rail.Label('Yes') >> if_legalentity_uri_present
        if_legalentity_mismatch >> rail.Label('No') >> if_location_mismatch

        if_legalentity_uri_present >> rail.Label('Yes') >> update_divisiongroup >> log_legal_enity_updated >> if_location_mismatch
        if_legalentity_uri_present >> rail.Label('No') >> log_legal_enity_exception >> if_location_mismatch

        if_location_mismatch >> rail.Label('Yes') >> if_departmentgroupuri_present
        if_location_mismatch >> rail.Label('No') >> get_location_change_variable

        if_departmentgroupuri_present >> rail.Label('Yes') >> update_departmentgroup >> assign_policyDataAccessScopes_department >> log_dept_grp_updated >> \
            update_location_change_variable_282 >> get_location_change_variable
        if_departmentgroupuri_present >> rail.Label('No') >> log_dept_grp_exception >> get_location_change_variable

        get_location_change_variable >> get_Exemptionstatus_change_variable >> get_workertype_change_variable_285 >> if_location_exemption_workertype_is_true

        if_location_exemption_workertype_is_true >> rail.Label('Yes') >> update_timeofftrigger_286 >> if_schedule_present_in_usermapping
        if_location_exemption_workertype_is_true >> rail.Label('No') >> if_schedule_present_in_usermapping

        if_schedule_present_in_usermapping >> rail.Label('Yes') >> check_schedulepolicies_present_for_user
        if_schedule_present_in_usermapping >> rail.Label('No') >> if_activities_not_present

        check_schedulepolicies_present_for_user >> rail.Label('Yes') >> get_current_schedulepolicy_uri >> if_schedule_uri_mismatch
        check_schedulepolicies_present_for_user >> rail.Label('No') >> if_activities_not_present

        if_schedule_uri_mismatch >> rail.Label('Yes') >> get_req_schedule_script >> if_get_req_schedule_script_present
        if_schedule_uri_mismatch >> rail.Label('No') >> if_activities_not_present

        if_get_req_schedule_script_present >> rail.Label('Yes') >> update_schedule_policy >> log_schedule_updated >> if_activities_not_present
        if_get_req_schedule_script_present >> rail.Label('No') >> if_schedule_equals_shift

        if_schedule_equals_shift >> rail.Label('Yes') >> update_schedule_policy_300 >> log_schedule_updated >> if_activities_not_present
        if_schedule_equals_shift >> rail.Label('No') >> if_activities_not_present

        if_activities_not_present >> rail.Label('Yes') >> put_activity_assignments_for_user_303 >> get_timeofftrigger_variable
        if_activities_not_present >> rail.Label('No') >> get_enabled_activities >> put_activity_assignments_for_user_307 >> log_activity_updated >> \
            get_timeofftrigger_variable

        get_timeofftrigger_variable >> if_timeoff_trigger_true_and_timeoff_present

        if_timeoff_trigger_true_and_timeoff_present >> rail.Label('Yes') >> trigger_update_user_timeoff >> wait_for_update_user_timeoff >> write_log_entry
        if_timeoff_trigger_true_and_timeoff_present >> rail.Label('No') >> write_log_entry

        write_log_entry >> write_log_exception >> log_user_import >> catch_and_log_error

        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
