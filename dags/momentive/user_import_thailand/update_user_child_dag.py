# pylint: disable=line-too-long
from datetime import timedelta, datetime
from airflow.models import Variable
import json
import rail
from rail.lib.ecid import get_dagrun_ecid
from momentive.user_import_thailand.utils import python_callable, request_payload
from momentive.user_import_thailand.mappers.momentive_thailand_mapper import momentive_thailand_mapper, mapper_value

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.momentive_thailand_user_sync_child_update_user_dag_id,
        description=f'Momentive_thailand_user_sync_update_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            # Recipe catch [489] retries the try body 3x @ 10s on transient failure.
            'retries': 3,
            'retry_delay': timedelta(seconds=10),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

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

        create_workertype_change = rail.SetVariableOperator(
            task_id='create_workertype_change',
            append=False,
            name='workertype_change',
            value='false'
        )

        create_workshift_change = rail.SetVariableOperator(
            task_id='create_workshift_change',
            append=False,
            name='workshift_change',
            value='false'
        )

        create_Exemptionstatus_change = rail.SetVariableOperator(
            task_id='create_Exemptionstatus_change',
            append=False,
            name='Exemptionstatus_change',
            value='false'
        )

        create_location_change = rail.SetVariableOperator(
            task_id='create_location_change',
            append=False,
            name='location_change',
            value='false'
        )

        create_costcenter_change = rail.SetVariableOperator(
            task_id='create_costcenter_change',
            append=False,
            name='costcenter_change',
            value='false'
        )

        create_timeofftrigger = rail.SetVariableOperator(
            task_id='create_timeofftrigger',
            append=False,
            name='timeofftrigger',
            value='false'
        )

        exception_log = rail.CreateLogOperator(
            task_id="exception_log"
        )

        # Recipe [486]: status escalates to "Exception" for ONLY 4 loggers
        # (DOB-invalid, supervisor-multiple, emp-type-not-found, dept-group-not-found).
        # These write here; the other "not found" loggers (timesheet-template,
        # paygroup, cost-center, legal-entity) stay in exception_log (details only).
        status_exception_log = rail.CreateLogOperator(
            task_id="status_exception_log"
        )

        log_entries = rail.CreateLogOperator(
            task_id="log_entries"
        )

        punch_entry_policy_ref_log = rail.CreateLogOperator(
            task_id="punch_entry_policy_ref_log"
        )

        get_input_validation_log = rail.PythonOperator(
            task_id="get_input_validation_log",
            python_callable=python_callable.get_input_validationlog
        )

        if_input_validation_log_present = rail.IfOperator(
            task_id='if_input_validation_log_present',
            test="{{ result('get_input_validation_log').exc_present | is_truthy }}",
            yes_task="log_user_import_not_created",
            no_task="get_user_data_14",
        )

        log_user_import_not_created = rail.WriteLogOperator(
            task_id="log_user_import_not_created",
            log='{{ dag_run.conf.user_import_logs}}',
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['User_ID'],
                "username": dag_run.conf['First_Name'] + " " + dag_run.conf['Last_Name'],
                "action": "Update",
                "status": "Exception",
                "details": "User not updated, " + rail.result('get_input_validation_log')['exc_value'],
                "childjobid": get_dagrun_ecid(dag_run),
            }
        )

        get_user_data_14 = rail.RepliconServiceOperator(
            task_id='get_user_data_14',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_all_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            }
        )

        if_rehireupdate_equals_rehire_15 = rail.IfOperator(
            task_id='if_rehireupdate_equals_rehire_15',
            test="{{ dag_run.conf.rehire_update == 'rehire' }}",
            yes_task="update_timeofftrigger_16",
            no_task="validate_hiredate_and_startdate",
        )

        update_timeofftrigger_16 = rail.SetVariableOperator(
            task_id='update_timeofftrigger_16',
            append=False,
            name='{{ result("create_timeofftrigger").name }}',
            value='true'
        )

        validate_hiredate_and_startdate = rail.PythonOperator(
            task_id="validate_hiredate_and_startdate",
            python_callable=python_callable.validate_hiredate_startdate
        )

        if_validate_hiredate_and_startdate_is_false_19 = rail.IfOperator(
            task_id='if_validate_hiredate_and_startdate_is_false_19',
            test="{{ result('validate_hiredate_and_startdate') | is_falsy }}",
            yes_task="remove_end_date_and_update_rehire_date_20",
            no_task="if_firstname_mismatch_24",
        )

        remove_end_date_and_update_rehire_date_20 = rail.RepliconServiceOperator(
            task_id='remove_end_date_and_update_rehire_date_20',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": python_callable.split_date_string(dag_run.conf['Hire_Date'], 'datetime'),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        enable_userprofile = rail.RepliconServiceOperator(
            task_id='enable_userprofile',
            endpoint="/services/securityservice1.svc/EnableLogin",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri']
            }
        )

        log_user_enabled = rail.WriteLogOperator(
            task_id='log_user_enabled',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "User enabled in Replicon and end date removed"
            }
        )

        if_firstname_mismatch_24 = rail.IfOperator(
            task_id="if_firstname_mismatch_24",
            test="{{ (result('get_user_data_14')[0].userDetails.firstName | is_falsy or \
                result('get_user_data_14')[0].userDetails.firstName.lower() != dag_run.conf.First_Name.lower()) and \
                dag_run.conf.First_Name | is_truthy }}",
            yes_task="update_firstname",
            no_task="if_lastname_mismatch_27"
        )

        update_firstname = rail.RepliconServiceOperator(
            task_id="update_firstname",
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "firstname": dag_run.conf['First_Name']
            }
        )

        log_first_name_updated = rail.WriteLogOperator(
            task_id='log_first_name_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "First name updated"
            }
        )

        if_lastname_mismatch_27 = rail.IfOperator(
            task_id="if_lastname_mismatch_27",
            test="{{ (result('get_user_data_14')[0].userDetails.lastName | is_falsy or \
                result('get_user_data_14')[0].userDetails.lastName.lower() != dag_run.conf.Last_Name.lower()) and \
                dag_run.conf.Last_Name | is_truthy }}",
            yes_task="update_lastname",
            no_task="if_email_mismatch_30"
        )

        update_lastname = rail.RepliconServiceOperator(
            task_id="update_lastname",
            endpoint="/services/UserService1.svc/UpdateLastName",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "lastname": dag_run.conf['Last_Name']
            }
        )

        log_last_name_updated = rail.WriteLogOperator(
            task_id='log_last_name_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Last name updated"
            }
        )

        if_email_mismatch_30 = rail.IfOperator(
            task_id="if_email_mismatch_30",
            test="{{ (result('get_user_data_14')[0].userDetails.emailAddress | is_falsy or \
                result('get_user_data_14')[0].userDetails.emailAddress.lower() != dag_run.conf.Email_Address.lower()) and \
                dag_run.conf.Email_Address | is_truthy }}",
            yes_task="update_email_address",
            no_task="if_rehireupdate_not_equals_rehire_35"
        )

        update_email_address = rail.RepliconServiceOperator(
            task_id="update_email_address",
            endpoint="/services/UserService1.svc/UpdateEmail",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "email": dag_run.conf['Email_Address']
            }
        )

        log_email_updated = rail.WriteLogOperator(
            task_id='log_email_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Email address updated"
            }
        )

        if_rehireupdate_not_equals_rehire_35 = rail.IfOperator(
            task_id='if_rehireupdate_not_equals_rehire_35',
            test="{{ dag_run.conf.rehire_update != 'rehire' }}",
            yes_task="if_terminationdate_present_and_not_equal_enddate",
            no_task="get_user_udf_values",
        )

        if_terminationdate_present_and_not_equal_enddate = rail.IfOperator(
            task_id='if_terminationdate_present_and_not_equal_enddate',
            test=python_callable.validate_terminationdate_enddate,
            yes_task="update_end_date",
            no_task="get_user_udf_values",
        )

        update_end_date = rail.RepliconServiceOperator(
            task_id='update_end_date',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": python_callable.split_date_string(dag_run.conf['Hire_Date'], 'datetime'),
                    "endDate": python_callable.split_date_string(dag_run.conf['Termination_Date'], 'datetime'),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_termination_date_updated = rail.WriteLogOperator(
            task_id='log_termination_date_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Termination date updated"
            }
        )

        get_user_udf_values = rail.PythonOperator(
            task_id="get_user_udf_values",
            python_callable=python_callable.get_udf_values_from_userdetails
        )

        if_CF_Date_of_Birth_MM_DD_YYYY_present_41 = rail.IfOperator(
            task_id='if_CF_Date_of_Birth_MM_DD_YYYY_present_41',
            test="{{ dag_run.conf.CF_Date_of_Birth_MM_DD_YYYY | is_truthy }}",
            yes_task="validate_CF_Date_of_Birth_MM_DD_YYYY",
            no_task="if_businesstitle_present_and_mismatch",
        )

        validate_CF_Date_of_Birth_MM_DD_YYYY = rail.IfOperator(
            task_id='validate_CF_Date_of_Birth_MM_DD_YYYY',
            test=lambda dag_run: bool(
                '-' in dag_run.conf['CF_Date_of_Birth_MM_DD_YYYY']),
            yes_task="check_dob_mismatch",
            no_task="log_birthdate_invalid",
        )

        log_birthdate_invalid = rail.WriteLogOperator(
            task_id='log_birthdate_invalid',
            log="{{ result('status_exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value": "Birthdate not in predefined date format"
            }
        )

        check_dob_mismatch = rail.IfOperator(
            task_id='check_dob_mismatch',
            test=lambda dag_run: not (rail.result('get_user_udf_values')['dob']) or bool(datetime.strptime(dag_run.conf['CF_Date_of_Birth_MM_DD_YYYY'], '%Y-%m-%d') !=
                                                                                         datetime.strptime(rail.result('get_user_udf_values')['dob'], '%Y/%m/%d')),
            yes_task="update_dob_udf",
            no_task="if_businesstitle_present_and_mismatch",
        )

        update_dob_udf = rail.RepliconServiceOperator(
            task_id='update_dob_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_custom_fields'), 'displayText', 'Date of Birth', 'uri', ''),
                "value": rail.parse_date(dag_run.conf['CF_Date_of_Birth_MM_DD_YYYY'], '%Y-%m-%d')
            }
        )

        log_dob_updated = rail.WriteLogOperator(
            task_id='log_dob_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Birthdate field updated"
            }
        )

        if_businesstitle_present_and_mismatch = rail.IfOperator(
            task_id='if_businesstitle_present_and_mismatch',
            test="{{ dag_run.conf.Business_Title | is_truthy and \
                dag_run.conf.Business_Title.lower() != result('get_user_udf_values').title }}",
            yes_task="update_title_udf",
            no_task="if_yearsofservice_present_and_mismatch",
        )

        update_title_udf = rail.RepliconServiceOperator(
            task_id='update_title_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_custom_fields'), 'displayText', 'Title', 'uri', ''),
                "value":  dag_run.conf['Business_Title']
            }
        )

        log_title_updated = rail.WriteLogOperator(
            task_id='log_title_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Business title field updated"
            }
        )

        if_yearsofservice_present_and_mismatch = rail.IfOperator(
            task_id='if_yearsofservice_present_and_mismatch',
            test="{{ dag_run.conf.Year_of_service | is_truthy and \
                dag_run.conf.Year_of_service.lower() != result('get_user_udf_values').yearsofservice }}",
            yes_task="update_yearsofservice_udf",
            no_task="if_fieldhr_present_and_mismatch",
        )

        update_yearsofservice_udf = rail.RepliconServiceOperator(
            task_id='update_yearsofservice_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_custom_fields'), 'displayText', 'Years of Service', 'uri', ''),
                "value": dag_run.conf['Year_of_service']
            }
        )

        log_yearsofservice_updated = rail.WriteLogOperator(
            task_id='log_yearsofservice_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Years of Service field updated"
            }
        )

        if_fieldhr_present_and_mismatch = rail.IfOperator(
            task_id='if_fieldhr_present_and_mismatch',
            test="{{ dag_run.conf.Field_HR | is_truthy and \
                dag_run.conf.Field_HR.lower() != result('get_user_udf_values').hrm }}",
            yes_task="update_fieldhr_udf",
            no_task="if_gender_present_and_mismatch",
        )

        update_fieldhr_udf = rail.RepliconServiceOperator(
            task_id='update_fieldhr_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_custom_fields'), 'displayText', 'HRM', 'uri', ''),
                "value": dag_run.conf['Field_HR']
            }
        )

        log_fieldhr_updated = rail.WriteLogOperator(
            task_id='log_fieldhr_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "HRM field updated"
            }
        )

        if_gender_present_and_mismatch = rail.IfOperator(
            task_id='if_gender_present_and_mismatch',
            test="{{ dag_run.conf.Gender | is_truthy and \
                dag_run.conf.Gender != result('get_user_udf_values').gender }}",
            yes_task="update_gender_udf",
            no_task="getdata_sup_emp_grp_dept_grp",
        )

        update_gender_udf = rail.RepliconServiceOperator(
            task_id='update_gender_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_custom_fields'), 'displayText', 'Gender', 'uri', ''),
                "value": dag_run.conf['Gender']
            }
        )

        getdata_sup_emp_grp_dept_grp = rail.RepliconServiceOperator(
            task_id="getdata_sup_emp_grp_dept_grp",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: request_payload.get_data_sup_emp_grp_dept_grp(dag_run)
        )

        if_manager_id_present_with_effective_date = rail.IfOperator(
            task_id='if_manager_id_present_with_effective_date',
            test="{{ dag_run.conf.Manager_ID | is_truthy and dag_run.conf.Effective_Date_of_Manager_Change | is_truthy }}",
            yes_task="if_managerid_equals_workrefempid",
            no_task="compare_to_today",
        )

        if_managerid_equals_workrefempid = rail.IfOperator(
            task_id='if_managerid_equals_workrefempid',
            test="{{ dag_run.conf.Manager_ID == dag_run.conf.Worker_Reference_Employee_ID }}",
            yes_task="log_supervisor_sameas_user",
            no_task="search_for_user_with_empid",
        )

        log_supervisor_sameas_user = rail.WriteLogOperator(
            task_id='log_supervisor_sameas_user',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Supervsior not updated for since user's  supervsior can not be same as the user"
            }
        )

        search_for_user_with_empid = rail.RepliconServiceOperator(
            task_id='search_for_user_with_empid',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.search_supervisor_payload,
            data_handler=python_callable.get_userdata_list_for_managerid
        )

        check_if_multiple_manageruseruri_present = rail.IfOperator(
            task_id='check_if_multiple_manageruseruri_present',
            test=lambda: bool(
                len(rail.result('search_for_user_with_empid')) > 1),
            yes_task="log_multiple_user_for_same_managerid",
            no_task="if_supervisor_present",
        )

        log_multiple_user_for_same_managerid = rail.WriteLogOperator(
            task_id='log_multiple_user_for_same_managerid',
            log="{{ result('status_exception_log') }}",
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "value": f"Supervisor not assigned for user {dag_run.conf['First_Name']} {dag_run.conf['Last_Name']} as \
                    multiple users have same Employee ID:{dag_run.conf['Manager_ID']} ."
            }
        )

        # Recipe #84: only fetch manager details when a supervisor was actually found.
        # Guards get_manager_details (search_for_user_with_empid[0]) from an empty match;
        # not found -> defer to the supervisor-assignment log (foreign-supervisor path).
        if_supervisor_present = rail.IfOperator(
            task_id='if_supervisor_present',
            test=lambda: bool(rail.result('search_for_user_with_empid')),
            yes_task="get_manager_details",
            no_task="log_supervisor_assignment",
        )

        get_manager_details = rail.RepliconServiceOperator(
            task_id='get_manager_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_manager_details_payload
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
            data=lambda dag_run: {
                "userUri": rail.result('search_for_user_with_empid')[0]['uri']
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.uri')
        )

        if_supervisor_permission_not_assigned = rail.IfOperator(
            task_id='if_supervisor_permission_not_assigned',
            test="{{ result('get_assigned_permissionset_foruser') | is_falsy }}",
            yes_task="get_all_permission_sets",
            no_task="if_search_user_supervisor_null",
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=request_payload.add_missing_supervisor_permission_payload_2
        )

        if_search_user_supervisor_null = rail.IfOperator(
            task_id='if_search_user_supervisor_null',
            test="{{ result('getdata_sup_emp_grp_dept_grp').rows | is_truthy and result('getdata_sup_emp_grp_dept_grp').rows[0].cells[2] | is_truthy and \
                result('getdata_sup_emp_grp_dept_grp').rows[0].cells[2].dataType == 'urn:replicon:list-type:null' }}",
            yes_task="update_initial_supervisor",
            no_task="if_search_user_supervisor_not_null",
        )

        update_initial_supervisor = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "initialSupervisorUri": rail.result('search_for_user_with_empid')[0]['uri'],
                "scheduleEntries": []
            }
        )

        log_initial_supervisor_added = rail.WriteLogOperator(
            task_id='log_initial_supervisor_added',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "value": "Initial supervisor added"
            }
        )

        if_search_user_supervisor_not_null = rail.IfOperator(
            task_id='if_search_user_supervisor_not_null',
            test="{{ result('getdata_sup_emp_grp_dept_grp').rows | is_truthy and result('getdata_sup_emp_grp_dept_grp').rows[0].cells[2].dataType | is_truthy and \
                result('getdata_sup_emp_grp_dept_grp').rows[0].cells[2].dataType != 'urn:replicon:list-type:null' }}",
            yes_task="get_current_supervisor_uri",
            no_task="compare_to_today",
        )

        get_current_supervisor_uri = rail.RepliconServiceOperator(
            task_id="get_current_supervisor_uri",
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "asOfDate": python_callable.split_date_string(
                    dag_run.conf['Effective_Date_of_Manager_Change'] if dag_run.conf['Effective_Date_of_Manager_Change'] else str(
                        datetime.strftime(datetime.now().date(), '%Y-%m-%d')), 'datetime')
            }
        )

        if_current_supervisor_uri_mismatch_manager_uri = rail.IfOperator(
            task_id='if_current_supervisor_uri_mismatch_manager_uri',
            test=lambda: rail.result('get_manager_details') and rail.result(
                'get_current_supervisor_uri')['supervisor']['user']['uri'] != rail.result('get_manager_details')[0]['userDetails']['uri'],
            yes_task="update_supervisor_assignment_over_daterange",
            no_task="compare_to_today",
        )

        update_supervisor_assignment_over_daterange = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_over_daterange',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "supervisorUri": rail.result('search_for_user_with_empid')[0]['uri'],
                "dateRange": {
                    "startDate": python_callable.split_date_string(
                        dag_run.conf['Effective_Date_of_Manager_Change'] if dag_run.conf['Effective_Date_of_Manager_Change'] else str(
                            datetime.strftime(datetime.now().date(), '%Y-%m-%d')), 'datetime')
                }
            }
        )

        log_supervisor_updated = rail.WriteLogOperator(
            task_id='log_supervisor_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Supervsior updated"
            }
        )

        log_supervisor_assignment = rail.WriteLogOperator(
            task_id="log_supervisor_assignment",
            log='{{ dag_run.conf.supervisor_assignment_logs}}',
            message="Exception",
            severity="Exception",
            properties=request_payload.supervisor_assignment_log_payload
        )

        # Recipe: the change-effective-date validations ([124]/[128]/[130]/[133])
        # run UNCONDITIONALLY and compare each eff-date to today. The recipe's only
        # gate here is [114] Location != "TH Bangkok" wrapping the timesheet-fetch
        # block [115-119], which is skip:true (dead) — so no timesheet fetch and no
        # hire-date gate. See compare_dates_to_today.
        compare_to_today = rail.PythonOperator(
            task_id="compare_to_today",
            python_callable=python_callable.compare_dates_to_today
        )

        # Recipe: the timesheet-fetch block [115-119] is dead (skip:true); the effective
        # date it would have produced falls back to today. get_startday_of_nexttimesheet
        # returns today, consumed by the employee-type and pay-rule update payloads.
        get_startdate_of_next_timesheet = rail.PythonOperator(
            task_id="get_startdate_of_next_timesheet",
            python_callable=python_callable.get_startday_of_nexttimesheet
        )

        validate_exemptioneff_date_160 = rail.IfOperator(
            task_id='validate_exemptioneff_date_160',
            test="{{ result('compare_to_today').exemption_eff_date | is_truthy }}",
            yes_task="update_exemption_change_variable_161",
            no_task="validate_workshift_change_effect_date_162",
        )

        update_exemption_change_variable_161 = rail.SetVariableOperator(
            task_id='update_exemption_change_variable_161',
            append=False,
            name='{{ result("create_Exemptionstatus_change").name }}',
            value='true'
        )

        validate_workshift_change_effect_date_162 = rail.IfOperator(
            task_id='validate_workshift_change_effect_date_162',
            test="{{ result('compare_to_today').work_shift_change_effective_date | is_truthy }}",
            yes_task="update_workshift_change_variable_163",
            no_task="validate_effect_date_of_workertype_164",
        )

        update_workshift_change_variable_163 = rail.SetVariableOperator(
            task_id='update_workshift_change_variable_163',
            append=False,
            name='{{ result("create_workshift_change").name }}',
            value='true'
        )

        validate_effect_date_of_workertype_164 = rail.IfOperator(
            task_id='validate_effect_date_of_workertype_164',
            test="{{ result('compare_to_today').effective_date_of_workertype | is_truthy }}",
            yes_task="update_workertype_change_variable_165",
            no_task="if_location_present_and_validate_cflrv_loc_changedate",
        )

        update_workertype_change_variable_165 = rail.SetVariableOperator(
            task_id='update_workertype_change_variable_165',
            append=False,
            name='{{ result("create_workertype_change").name }}',
            value='true'
        )

        if_location_present_and_validate_cflrv_loc_changedate = rail.IfOperator(
            task_id='if_location_present_and_validate_cflrv_loc_changedate',
            test="{{ dag_run.conf.Location | is_truthy and \
                result('compare_to_today').cf_lrv_location_change_effective_date | is_truthy }}",
            yes_task="update_location_change_variable_168",
            no_task="compute_mapper_keys",
        )

        update_location_change_variable_168 = rail.SetVariableOperator(
            task_id='update_location_change_variable_168',
            append=False,
            name='{{ result("create_location_change").name }}',
            value='true'
        )

        compute_mapper_keys = rail.PythonOperator(
            task_id='compute_mapper_keys',
            python_callable=python_callable.mapper_keys
        )

        log_employeetypetobeassigned = rail.PythonOperator(
            task_id='log_employeetypetobeassigned',
            python_callable=lambda dag_run: mapper_value(
                'Employee Type', workertype=dag_run.conf['Worker_Type'], location='Null',
                exemptstatus=rail.result('compute_mapper_keys')['exemptstatus'],
                shift=rail.result('compute_mapper_keys')['shift'], gender='Any', cost_center='Any')
        )

        get_effectiveusergroupmembership = rail.RepliconServiceOperator(
            task_id="get_effectiveusergroupmembership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": null
            }
        )

        validate_employeetype = rail.IfOperator(
            task_id='validate_employeetype',
            test=lambda: bool(not (rail.result('get_effectiveusergroupmembership').get('employeeTypes')) or
                              not (((rail.result('get_effectiveusergroupmembership')['employeeTypes'][0].get('employeeType') or {}).get('employeeType') or {}).get('uri')) or
                              (python_callable.get_current_group_display_text('employeeTypes', 'employeeType').lower() != (rail.result(
                                  'log_employeetypetobeassigned') or '').lower())),
            yes_task="get_all_employee_type_details",
            no_task="log_timesheetapprovalpathtobeassigned_198",
        )

        get_all_employee_type_details = rail.RepliconServiceOperator(
            task_id="get_all_employee_type_details",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText', rail.result(
                'log_employeetypetobeassigned'), 'uri', '')
        )

        if_employeetype_present = rail.IfOperator(
            task_id='if_employeetype_present',
            test="{{ result('get_all_employee_type_details') | is_truthy }}",
            yes_task="update_employeetype_group",
            no_task="log_employeetype_not_updated",
        )

        log_employeetype_not_updated = rail.WriteLogOperator(
            task_id='log_employeetype_not_updated',
            log="{{ result('status_exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value": """Employee type was not updated as the employee type '{{ result('log_employeetypetobeassigned') }}' not found in Replicon """
            }
        )

        update_employeetype_group = rail.RepliconServiceOperator(
            task_id='update_employeetype_group',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.update_employeetypegrp_payload
        )

        update_variable_timeofftrigger = rail.SetVariableOperator(
            task_id='update_variable_timeofftrigger',
            append=False,
            name='{{ result("create_timeofftrigger").name }}',
            value='true'
        )

        log_employeetype_updated = rail.WriteLogOperator(
            task_id='log_employeetype_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Employee type updated"
            }
        )

        log_timesheetapprovalpathtobeassigned_198 = rail.PythonOperator(
            task_id='log_timesheetapprovalpathtobeassigned_198',
            python_callable=lambda dag_run: "System Approval" if dag_run.conf.get('Exemption_Status') == "1" else "Supervisor"
        )

        if_timesheetapprovalpathtobeassigned_present_and_not_equalsto_curr_path = rail.IfOperator(
            task_id='if_timesheetapprovalpathtobeassigned_present_and_not_equalsto_curr_path',
            test=lambda: bool(rail.result('log_timesheetapprovalpathtobeassigned_198') and rail.result('log_timesheetapprovalpathtobeassigned_198') != (
                rail.result('get_user_data_14')[0].get("timesheetApprovalPath") or {}).get('displayText')),
            yes_task="get_all_approvalpaths_timesheet",
            no_task="log_holidaycalendartobeassigned_204"
        )

        get_all_approvalpaths_timesheet = rail.RepliconServiceOperator(
            task_id='get_all_approvalpaths_timesheet',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_timesheetapprovalpathtobeassigned_198'), 'uri')
        )

        update_approval_path_touser = rail.RepliconServiceOperator(
            task_id='update_approval_path_touser',
            endpoint="/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "approvalPathUri": rail.result('get_all_approvalpaths_timesheet')
            }
        )

        log_approvalpaths_timesheet_updated = rail.WriteLogOperator(
            task_id='log_approvalpaths_timesheet_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Timesheet approval path updated"
            }
        )

        log_holidaycalendartobeassigned_204 = rail.PythonOperator(
            task_id='log_holidaycalendartobeassigned_204',
            python_callable=lambda dag_run: mapper_value(
                'Holiday Calendar', workertype='Any', location=dag_run.conf['Location'],
                exemptstatus='Any', shift=rail.result('compute_mapper_keys')['shift'], gender='Any', cost_center='Any')
        )

        if_holidaycalendar_tobeassigned_present_and_not_equalto_curr_calendar = rail.IfOperator(
            task_id='if_holidaycalendar_tobeassigned_present_and_not_equalto_curr_calendar',
            test=lambda: bool(rail.result('log_holidaycalendartobeassigned_204') and rail.result('log_holidaycalendartobeassigned_204') != (
                rail.result('get_user_data_14')[0].get("holidayCalendar") or {}).get('displayText')),
            yes_task="get_all_holidaycalendars",
            no_task="log_timesheettemplatetobeassigned_211"
        )

        get_all_holidaycalendars = rail.RepliconServiceOperator(
            task_id='get_all_holidaycalendars',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', rail.result('log_holidaycalendartobeassigned_204'), 'uri')
        )

        update_holiday_calendar_foruser = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_foruser',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "holidayCalendarUri": rail.result('get_all_holidaycalendars')
            }
        )

        log_holiday_calendar_updated = rail.WriteLogOperator(
            task_id='log_holiday_calendar_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Holiday calendar updated"
            }
        )

        log_timesheettemplatetobeassigned_211 = rail.PythonOperator(
            task_id='log_timesheettemplatetobeassigned_211',
            python_callable=lambda dag_run: mapper_value(
                'Timesheet Template', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'],
                exemptstatus=rail.result('compute_mapper_keys')['exemptstatus'], shift=rail.result('compute_mapper_keys')['shift'],
                gender=rail.result('compute_mapper_keys')['gender'], cost_center=rail.result('compute_mapper_keys')['cost_center'])
        )

        get_policysets = rail.RepliconServiceOperator(
            task_id='get_policysets',
            endpoint='/services/PolicySetService1.svc/GetAllPolicySets',
            data_handler=lambda response: {
                'timeoff': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Time Off', 'uri', ''),
                'timesheet': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('log_timesheettemplatetobeassigned_211'), 'uri', ''),
                'all': response
            }
        )

        if_timesheet_mismatch = rail.IfOperator(
            task_id='if_timesheet_mismatch',
            # 'NA' is the mapper's not-applicable sentinel (e.g. Bangkok) — treat it like a
            # blank value and skip the timesheet-template change instead of looking up 'NA'.
            test=lambda: bool(rail.result('log_timesheettemplatetobeassigned_211') and rail.result('log_timesheettemplatetobeassigned_211') != 'NA' and rail.result('log_timesheettemplatetobeassigned_211') != (
                rail.result('get_user_data_14')[0].get("timesheetTemplate") or {}).get('displayText')),
            yes_task="if_timesheet_templateuri_present",
            no_task="if_paygroup_present",
        )

        if_timesheet_templateuri_present = rail.IfOperator(
            task_id='if_timesheet_templateuri_present',
            test="{{ result('get_policysets').timesheet | is_truthy }}",
            yes_task="update_timesheet_template",
            no_task="log_timesheettemplate_not_updated",
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
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Timesheet template was  updated"
            }
        )

        log_punchentrypolicy_tobeassigned_218 = rail.PythonOperator(
            task_id='log_punchentrypolicy_tobeassigned_218',
            python_callable=lambda dag_run: mapper_value(
                'Punch Entry Policy', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'],
                exemptstatus=rail.result('compute_mapper_keys')['exemptstatus'], shift=rail.result('compute_mapper_keys')['shift'],
                gender=rail.result('compute_mapper_keys')['gender'], cost_center=rail.result('compute_mapper_keys')['cost_center'])
        )

        get_punchentry_policy_uri = rail.PythonOperator(
            task_id='get_punchentry_policy_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_policysets')['all'], 'displayText', rail.result('log_punchentrypolicy_tobeassigned_218'), 'uri', '')
        )

        if_punchentrypolicy_uri_present = rail.IfOperator(
            task_id='if_punchentrypolicy_uri_present',
            test="{{ result('get_punchentry_policy_uri') | is_truthy }}",
            yes_task="get_next_timesheet_duedate",
            no_task="if_paygroup_present",
        )

        get_next_timesheet_duedate = rail.RepliconServiceOperator(
            task_id='get_next_timesheet_duedate',
            endpoint="/services/TimesheetService1.svc/GetNextTimesheetDueDate",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "asOfDate": python_callable.split_date_string(str(datetime.now().date()))
            }
        )

        if_day_is_present_in_nexttimesheet_duedate = rail.IfOperator(
            task_id='if_day_is_present_in_nexttimesheet_duedate',
            test="{{ result('get_next_timesheet_duedate') | is_truthy and result('get_next_timesheet_duedate').day | is_truthy }}",
            yes_task="punch_entry_policy_ref_log_add_entry",
            no_task="if_paygroup_present",
        )

        # NOTE: The Thailand master DAG currently has no consumer for this deferred
        # punch-entry-policy queue (unlike Japan). The write is harmless and kept for
        # parity so the wiring is not broken; wire a master-side consumer when added.
        punch_entry_policy_ref_log_add_entry = rail.WriteLogOperator(
            task_id='punch_entry_policy_ref_log_add_entry',
            log="{{ result('punch_entry_policy_ref_log') }}",
            message="na",
            properties=lambda dag_run: {
                "useruri": dag_run.conf['useruri'],
                "status": "pending",
                "policyuri": rail.result('get_punchentry_policy_uri'),
                "action": "add",
                "effectivedate": "{{ result('get_next_timesheet_duedate').day }}/{{ result('get_next_timesheet_duedate').month }}/{{ result('get_next_timesheet_duedate').year }}"
            }
        )

        log_timesheettemplate_not_updated = rail.WriteLogOperator(
            task_id='log_timesheettemplate_not_updated',
            log="{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value": """Timesheet template not updated since '{{ result('log_timesheettemplatetobeassigned_211') }}' not found or disabled in Replicon """
            }
        )

        if_paygroup_present = rail.IfOperator(
            task_id='if_paygroup_present',
            test="{{ dag_run.conf.paygroup | is_truthy }}",
            yes_task="paygroup_mismatch_present",
            no_task="if_cost_center_present"
        )

        paygroup_mismatch_present = rail.IfOperator(
            task_id="paygroup_mismatch_present",
            test=lambda dag_run: bool(not python_callable.get_current_group_display_text('serviceCenters', 'serviceCenter') or
                                      (python_callable.get_current_group_display_text('serviceCenters', 'serviceCenter') != dag_run.conf['paygroup'])),
            yes_task="get_servicecenter_group_data",
            no_task="if_cost_center_present"
        )

        get_servicecenter_group_data = rail.RepliconServiceOperator(
            task_id="get_servicecenter_group_data",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data=request_payload.get_servicecenter_group_data_payload
        )

        servicecenter_list = rail.PythonOperator(
            task_id='servicecenter_list',
            python_callable=python_callable.build_servicecenter_list
        )

        log_req_servicecenter_uri = rail.PythonOperator(
            task_id="log_req_servicecenter_uri",
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('servicecenter_list'), 'name', dag_run.conf['paygroup'], 'uri', '')
        )

        if_paygroup_present_and_contains_urn = rail.IfOperator(
            task_id="if_paygroup_present_and_contains_urn",
            test="{{ result('log_req_servicecenter_uri') | is_truthy }}",
            yes_task="update_service_center_paygroup",
            no_task="log_exception_paygroup_not_updated"
        )

        update_service_center_paygroup = rail.RepliconServiceOperator(
            task_id="update_service_center_paygroup",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.update_service_center_payload
        )

        log_paygroup_updated = rail.WriteLogOperator(
            task_id='log_paygroup_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Paygroup updated"
            }
        )

        log_exception_paygroup_not_updated = rail.WriteLogOperator(
            task_id='log_exception_paygroup_not_updated',
            log="{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value": """Paygroup was not updated as the Paygroup '{{ dag_run.conf.paygroup }}' not found in Replicon """
            }
        )

        if_cost_center_present = rail.IfOperator(
            task_id='if_cost_center_present',
            test="{{ dag_run.conf.cost_center | is_truthy }}",
            yes_task="costcenter_mismatch_present",
            no_task="if_legalentity_present"
        )

        costcenter_mismatch_present = rail.IfOperator(
            task_id="costcenter_mismatch_present",
            test=lambda dag_run: bool(not python_callable.get_current_group_display_text('costCenters', 'costCenter') or
                                      (python_callable.get_current_group_display_text('costCenters', 'costCenter') != dag_run.conf['cost_center'])),
            yes_task="get_costcenter_group_data",
            no_task="if_legalentity_present"
        )

        get_costcenter_group_data = rail.RepliconServiceOperator(
            task_id="get_costcenter_group_data",
            endpoint="/services/CostCenterListService1.svc/GetData",
            data=request_payload.get_costcenter_group_data_payload
        )

        costcenter_list = rail.PythonOperator(
            task_id='costcenter_list',
            python_callable=python_callable.build_costcenter_list
        )

        log_req_costcentergroup_uri = rail.PythonOperator(
            task_id="log_req_costcentergroup_uri",
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('costcenter_list'), 'name', dag_run.conf['cost_center'], 'uri', '')
        )

        if_costcenter_uri_present = rail.IfOperator(
            task_id="if_costcenter_uri_present",
            test="{{ result('log_req_costcentergroup_uri') | is_truthy }}",
            yes_task="log_costcenter_changeeffdate",
            no_task="log_costcenter_notupdated_exception"
        )

        log_costcenter_changeeffdate = rail.PythonOperator(
            task_id="log_costcenter_changeeffdate",
            python_callable=lambda dag_run: python_callable.split_date_string(dag_run.conf['eff_cost_center']) if dag_run.conf['eff_cost_center'] \
                else python_callable.split_date_string(datetime.now().strftime("%Y-%m-%d"))
        )

        # Recipe [271]: ApplyUserModifications2 with resource-specific member names and the
        # cost center identified by name. The upstream get_costcenter_group_data /
        # if_costcenter_uri_present guard stays -- it reproduces recipe [269]'s check that
        # the cost center exists and is enabled in Replicon before this fires.
        update_cost_center_group = rail.RepliconServiceOperator(
            task_id="update_cost_center_group",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.update_costcenter_modifications_payload
        )

        log_costcenter_updated = rail.WriteLogOperator(
            task_id='log_costcenter_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Cost center updated"
            }
        )

        log_costcenter_notupdated_exception = rail.WriteLogOperator(
            task_id='log_costcenter_notupdated_exception',
            log="{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value": """Cost center was not updated as the costcenter '{{ dag_run.conf.cost_center }}' not found in Replicon """
            }
        )

        if_legalentity_present = rail.IfOperator(
            task_id="if_legalentity_present",
            test="{{ dag_run.conf.legal_entity | is_truthy }}",
            yes_task="legalentity_mismatch_present",
            no_task="if_location_present"
        )

        legalentity_mismatch_present = rail.IfOperator(
            task_id="legalentity_mismatch_present",
            test=lambda dag_run: bool(not python_callable.get_current_group_display_text('divisions', 'division') or
                                      (python_callable.get_current_group_display_text('divisions', 'division') != dag_run.conf['legal_entity'])),
            yes_task="get_division_group_data",
            no_task="if_location_present"
        )

        get_division_group_data = rail.RepliconServiceOperator(
            task_id="get_division_group_data",
            endpoint="/services/DivisionListService1.svc/GetData",
            data=request_payload.get_division_group_data_payload
        )

        legalentity_list = rail.PythonOperator(
            task_id='legalentity_list',
            python_callable=python_callable.build_legalentity_list
        )

        log_req_legalentity_division_uri = rail.PythonOperator(
            task_id="log_req_legalentity_division_uri",
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('legalentity_list'), 'name', dag_run.conf['legal_entity'], 'uri', '')
        )

        if_legalentity_uri_present = rail.IfOperator(
            task_id="if_legalentity_uri_present",
            test="{{ result('log_req_legalentity_division_uri') | is_truthy }}",
            yes_task="update_division_group",
            no_task="log_legalentity_notupdated_exception"
        )

        update_division_group = rail.RepliconServiceOperator(
            task_id="update_division_group",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.update_division_group_payload
        )

        log_legalentity_updated = rail.WriteLogOperator(
            task_id='log_legalentity_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Legal entity updated"
            }
        )

        log_legalentity_notupdated_exception = rail.WriteLogOperator(
            task_id='log_legalentity_notupdated_exception',
            log="{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value": """Legal entity  was not updated as the Legal entity (division) '{{ dag_run.conf.legal_entity }}' not found in Replicon """
            }
        )

        if_location_present = rail.IfOperator(
            task_id="if_location_present",
            test="{{ dag_run.conf.Location | is_truthy }}",
            yes_task="location_mismatch_present",
            no_task="log_payruletobeassigned"
        )

        location_mismatch_present = rail.IfOperator(
            task_id="location_mismatch_present",
            test=lambda dag_run: bool(not python_callable.get_current_group_display_text('departments', 'department') or
                                      (python_callable.get_current_group_display_text('departments', 'department') != dag_run.conf['Location'])),
            yes_task="if_departmentgrp_uri_present",
            no_task="log_payruletobeassigned"
        )

        if_departmentgrp_uri_present = rail.IfOperator(
            task_id="if_departmentgrp_uri_present",
            test="{{ dag_run.conf.departmentgroupuri | is_truthy }}",
            yes_task="log_location_change_eff_date",
            no_task="log_department_group_notupdated_exception"
        )

        log_location_change_eff_date = rail.PythonOperator(
            task_id="log_location_change_eff_date",
            python_callable=lambda dag_run: python_callable.split_date_string(dag_run.conf['CF_LRV_Location_Change_Effective_Date']) if dag_run.conf['CF_LRV_Location_Change_Effective_Date'] \
                else python_callable.split_date_string(datetime.now().strftime("%Y-%m-%d"))
        )

        update_department_group = rail.RepliconServiceOperator(
            task_id="update_department_group",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.update_department_group_payload
        )

        put_policy_data_access_scopes_for_userdepartmentrestricted = rail.RepliconServiceOperator(
            task_id='put_policy_data_access_scopes_for_userdepartmentrestricted',
            endpoint="/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "policyDataAccessScopes": [
                    {
                        "policyUri": "urn:replicon:policy:time-off",
                        "locations": [],
                        "divisions": [],
                        "costCenters": [],
                        "serviceCenters": [],
                        "departmentGroups": [
                            {
                                "departmentGroup": {
                                    "uri": dag_run.conf['departmentgroupuri'],
                                    "parentUri": null,
                                    "name": null
                                },
                                "groupSpecificationModeUri": null,
                                "groupDescendantModeUri": null
                            }
                        ],
                        "employeeTypeGroups": []
                    }
                ]
            }
        )

        log_department_group_updated = rail.WriteLogOperator(
            task_id='log_department_group_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Department group updated"
            }
        )

        update_timeofftrigger_262 = rail.SetVariableOperator(
            task_id='update_timeofftrigger_262',
            append=False,
            name='{{ result("create_timeofftrigger").name }}',
            value='true'
        )

        log_department_group_notupdated_exception = rail.WriteLogOperator(
            task_id='log_department_group_notupdated_exception',
            log="{{ result('status_exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value": """Department group was not updated as the department(location) '{{ dag_run.conf.Location }}' not found in Replicon"""
            }
        )

        # Recipe [189] (exemption/workshift/location change -> set timeoff trigger)
        # is skip:true (dead). Only the live setters remain: rehire, employee-type
        # update, and department/location update (update_timeofftrigger_262).

        log_payruletobeassigned = rail.PythonOperator(
            task_id='log_payruletobeassigned',
            python_callable=lambda dag_run: mapper_value(
                'Payrule', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'],
                exemptstatus=rail.result('compute_mapper_keys')['exemptstatus'], shift=rail.result('compute_mapper_keys')['shift'],
                gender=rail.result('compute_mapper_keys')['gender'], cost_center=rail.result('compute_mapper_keys')['cost_center'])
        )

        if_payruletobeassigned_present = rail.IfOperator(
            task_id="if_payruletobeassigned_present",
            test="{{ result('log_payruletobeassigned') | is_truthy }}",
            yes_task="if_payrule_scriptschedule_is_present",
            no_task="log_schedule_tobeassigned"
        )

        if_payrule_scriptschedule_is_present = rail.IfOperator(
            task_id="if_payrule_scriptschedule_is_present",
            test=lambda: bool(rail.result('get_user_data_14')[0]["payRuleScriptSchedule"]),
            yes_task="latest_payrule_name",
            no_task="log_schedule_tobeassigned"
        )

        latest_payrule_name = rail.PythonOperator(
            task_id='latest_payrule_name',
            python_callable=lambda: request_payload.get_current_value_from_schedule_list_for_user(
                rail.result('get_user_data_14')[0]['payRuleScriptSchedule'], 'payRuleScript', 'displayText')
        )

        validate_payrule_name = rail.IfOperator(
            task_id="validate_payrule_name",
            test=lambda: bool(not (rail.result("latest_payrule_name")) or rail.result(
                "latest_payrule_name") != rail.result('log_payruletobeassigned')),
            yes_task="get_req_payrule_script",
            no_task="log_schedule_tobeassigned"
        )

        get_req_payrule_script = rail.RepliconServiceOperator(
            task_id='get_req_payrule_script',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_payruletobeassigned'), 'uri', '')
        )

        if_payrule_uri_present_in_scripts = rail.IfOperator(
            task_id="if_payrule_uri_present_in_scripts",
            test="{{ result('get_req_payrule_script') | is_truthy }}",
            yes_task="update_payrule_for_user",
            no_task="log_schedule_tobeassigned"
        )

        update_payrule_for_user = rail.RepliconServiceOperator(
            task_id='update_payrule_for_user',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.update_payrule_for_user_payload
        )

        log_payrule_updated = rail.WriteLogOperator(
            task_id='log_payrule_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Pay rule updated"
            }
        )

        log_schedule_tobeassigned = rail.PythonOperator(
            task_id='log_schedule_tobeassigned',
            python_callable=lambda dag_run: mapper_value(
                'Schedule', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'],
                exemptstatus=rail.result('compute_mapper_keys')['exemptstatus'], shift=rail.result('compute_mapper_keys')['shift'],
                gender=rail.result('compute_mapper_keys')['gender'], cost_center=rail.result('compute_mapper_keys')['cost_center'])
        )

        if_schedule_tobeassigned_present = rail.IfOperator(
            task_id="if_schedule_tobeassigned_present",
            test="{{ result('log_schedule_tobeassigned') | is_truthy }}",
            yes_task="if_schedule_policies_present",
            no_task="log_actvities_tobeassigned"
        )

        if_schedule_policies_present = rail.IfOperator(
            task_id="if_schedule_policies_present",
            test=lambda: bool(
                rail.result('get_user_data_14')[0].get("schedulePolicies") and
                'urn' in json.dumps(rail.result('get_user_data_14')[0]["schedulePolicies"])
            ),
            yes_task="latest_schedule_policy_name",
            no_task="log_actvities_tobeassigned"
        )

        latest_schedule_policy_name = rail.PythonOperator(
            task_id='latest_schedule_policy_name',
            python_callable=lambda: request_payload.get_current_schedule_policy_from_list(
                rail.result('get_user_data_14')[0]['schedulePolicies'])
        )

        validate_schedule_policy_name = rail.IfOperator(
            task_id="validate_schedule_policy_name",
            test=lambda: bool(
                not (rail.result("latest_schedule_policy_name")) or
                rail.result("latest_schedule_policy_name").get('displayText') != rail.result('log_schedule_tobeassigned')
            ),
            yes_task="log_schedule_policy_change_eff_date",
            no_task="log_actvities_tobeassigned"
        )

        log_schedule_policy_change_eff_date = rail.PythonOperator(
            task_id="log_schedule_policy_change_eff_date",
            python_callable=lambda dag_run: python_callable.split_date_string(dag_run.conf['Work_Shift_Change_Effective_Date']) if dag_run.conf['Work_Shift_Change_Effective_Date'] \
                else python_callable.split_date_string(datetime.now().strftime("%Y-%m-%d"))
        )

        get_req_schedule_script = rail.RepliconServiceOperator(
            task_id='get_req_schedule_script',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_schedule_tobeassigned'), 'uri', '')
        )

        if_schedule_uri_present_in_scripts = rail.IfOperator(
            task_id="if_schedule_uri_present_in_scripts",
            test="{{ result('get_req_schedule_script') | is_truthy }}",
            yes_task="build_schedule_entries",
            no_task="if_schedule_tobeassigned_equals_shift"
        )

        # Recipe [439]-[454]: rebuild the schedule-policy entries client-side, preserving
        # the user's existing entries (blank-dated kept with a null effectiveDate, dated
        # entries kept unless their date equals the change date), then append the new
        # target entry. Shift vs office-schedule is derived from the mapped target name,
        # so the same rebuilt list is correct for either branch below.
        build_schedule_entries = rail.PythonOperator(
            task_id='build_schedule_entries',
            python_callable=python_callable.build_schedule_entries
        )

        # Recipe [457]/[466]: office-schedule target.
        put_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_policy_schedule_for_user',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=request_payload.schedule_policy_payload
        )

        log_schedule_policy_updated = rail.WriteLogOperator(
            task_id='log_schedule_policy_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Schedule updated"
            }
        )

        if_schedule_tobeassigned_equals_shift = rail.IfOperator(
            task_id="if_schedule_tobeassigned_equals_shift",
            test=lambda: rail.result('log_schedule_tobeassigned') == 'Shift',
            yes_task="build_shift_schedule_entries",
            no_task="log_actvities_tobeassigned"
        )

        # Recipe [460]/[471]: Shift target — same rebuild, officeScheduleUri null.
        build_shift_schedule_entries = rail.PythonOperator(
            task_id='build_shift_schedule_entries',
            python_callable=python_callable.build_schedule_entries
        )

        update_shift_schedule_for_user = rail.RepliconServiceOperator(
            task_id='update_shift_schedule_for_user',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=request_payload.shift_schedule_policy_payload
        )

        log_schedule_shift_updated = rail.WriteLogOperator(
            task_id='log_schedule_shift_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Schedule updated"
            }
        )

        log_actvities_tobeassigned = rail.PythonOperator(
            task_id='log_actvities_tobeassigned',
            python_callable=lambda dag_run: mapper_value(
                'Activity', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'],
                exemptstatus=rail.result('compute_mapper_keys')['exemptstatus'], shift=rail.result('compute_mapper_keys')['shift'],
                gender=rail.result('compute_mapper_keys')['gender'], cost_center=rail.result('compute_mapper_keys')['cost_center'])
        )

        if_activities_tobeassigned_not_present = rail.IfOperator(
            task_id='if_activities_tobeassigned_not_present',
            test="{{ result('log_actvities_tobeassigned') | is_falsy }}",
            yes_task="put_activityassignments_for_user",
            no_task="create_activity_list"
        )

        put_activityassignments_for_user = rail.RepliconServiceOperator(
            task_id='put_activityassignments_for_user',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "activityUris": []
            }
        )

        create_activity_list = rail.SetVariableOperator(
            task_id='create_activity_list',
            append=True,
            name='activity_list',
            value=[]
        )

        split_activities_bydelim = rail.PythonOperator(
            task_id='split_activities_bydelim',
            python_callable=lambda: [activity.strip() for activity in rail.result('log_actvities_tobeassigned').split("|")]
        )

        get_all_enabled_activities = rail.RepliconServiceOperator(
            task_id='get_all_enabled_activities',
            endpoint="/services/ActivityService1.svc/GetEnabledActivities"
        )

        update_activity_list = rail.SetVariableOperator(
            task_id='update_activity_list',
            append=False,
            name='{{ result("create_activity_list").name }}',
            value=lambda: [uri for uri in [rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_activities'), 'displayText', activity, 'uri') for activity in rail.result('split_activities_bydelim') or []] if uri]
        )

        if_activity_uris_present = rail.IfOperator(
            task_id='if_activity_uris_present',
            test=lambda: len(rail.get_dag_run_var("activity_list") or []) > 0,
            yes_task="assign_activities_to_user",
            no_task="if_timeofftrigger_var_equals_true"
        )

        assign_activities_to_user = rail.RepliconServiceOperator(
            task_id='assign_activities_to_user',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "activityUris": rail.get_dag_run_var("activity_list")
            }
        )

        log_activity_updated = rail.WriteLogOperator(
            task_id='log_activity_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Activity updated"
            }
        )

        if_timeofftrigger_var_equals_true = rail.IfOperator(
            task_id="if_timeofftrigger_var_equals_true",
            test=lambda: rail.get_dag_run_var("timeofftrigger") == 'true',
            yes_task="log_timeofftypes_tobeassigned",
            no_task="log_user_import"
        )

        log_timeofftypes_tobeassigned = rail.PythonOperator(
            task_id='log_timeofftypes_tobeassigned',
            python_callable=lambda dag_run: mapper_value(
                'Time off types', workertype=dag_run.conf['Worker_Type'], location=dag_run.conf['Location'],
                exemptstatus=rail.result('compute_mapper_keys')['exemptstatus'], shift=rail.result('compute_mapper_keys')['shift'],
                gender=rail.result('compute_mapper_keys')['gender'], cost_center=rail.result('compute_mapper_keys')['cost_center'])
        )

        trigger_update_user_timeoff = rail.TriggerDagRunOperator(
            task_id='trigger_update_user_timeoff',
            trigger_dag_id=config.momentive_thailand_user_sync_child_update_user_timeoff_assign_id,
            conf=request_payload.trigger_updateuser_timeoff,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            # Never retry a trigger -- a retry would spawn a duplicate grandchild run.
            retries=0,
        )

        wait_for_update_user_timeoff = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_user_timeoff',
            dag_runs='{{ result("trigger_update_user_timeoff") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_result_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_result_from_child',
            dag_runs='''{{result('trigger_update_user_timeoff')}}''',
            dagrun_task_id='final_response_from_dag',
            target='result'
        )

        if_error_in_gather_result_from_child = rail.IfOperator(
            task_id='if_error_in_gather_result_from_child',
            test=lambda: bool(rail.result("gather_result_from_child")) and "Error" in json.dumps(rail.result(
                "gather_result_from_child")[0]),
            yes_task='stop_processing_due_to_error_in_child',
            no_task='log_user_import'
        )

        stop_processing_due_to_error_in_child = rail.FailOperator(
            task_id='stop_processing_due_to_error_in_child',
            message='''Error in updating timeoff type for a user'''
        )

        log_user_import = rail.WriteLogOperator(
            task_id='log_user_import',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity=lambda: "Exception" if len(rail.load_all_records(
                rail.result("status_exception_log"))) > 0 else "Success",
            properties=python_callable.get_status_and_details_for_update
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log='{{ dag_run.conf.user_import_logs}}',
            trigger_rule='one_failed',
            message="na",
            severity=lambda: "Exception" if "Timesheet date is prior to the first timesheet period setting in the system." in rail.render_template(
                "{{ get_error_message() }}") else "Error",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['User_ID'],
                "username": f"{dag_run.conf['First_Name']} {dag_run.conf['Last_Name']}",
                "action": "Update",
                "status": "Exception" if "Timesheet date is prior to the first timesheet period setting in the system." in rail.render_template(
                    "{{ get_error_message() }}") else "Error",
                'details': "User partially updated, " + "{{ get_error_message() }}",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> create_workertype_change

        create_workertype_change >> create_workshift_change >> create_Exemptionstatus_change >> create_location_change >> \
            create_costcenter_change >> create_timeofftrigger >> exception_log >> status_exception_log >> log_entries >> punch_entry_policy_ref_log >> get_input_validation_log >> if_input_validation_log_present

        if_input_validation_log_present >> rail.Label(
            'Yes') >> log_user_import_not_created >> catch_and_log_error
        if_input_validation_log_present >> rail.Label(
            'No') >> get_user_data_14 >> get_all_custom_fields >> if_rehireupdate_equals_rehire_15 >> rail.Label('Yes') >> update_timeofftrigger_16 >> validate_hiredate_and_startdate

        if_rehireupdate_equals_rehire_15 >> rail.Label('No') >> validate_hiredate_and_startdate >> if_validate_hiredate_and_startdate_is_false_19

        if_validate_hiredate_and_startdate_is_false_19 >> rail.Label('Yes') >> remove_end_date_and_update_rehire_date_20 >> enable_userprofile >> log_user_enabled >> if_firstname_mismatch_24
        if_validate_hiredate_and_startdate_is_false_19 >> rail.Label('No') >> if_firstname_mismatch_24 >> rail.Label('Yes') >> update_firstname >> log_first_name_updated >> if_lastname_mismatch_27

        if_firstname_mismatch_24 >> rail.Label('No') >> if_lastname_mismatch_27 >> rail.Label('Yes') >> update_lastname >> log_last_name_updated >> if_email_mismatch_30
        if_lastname_mismatch_27 >> rail.Label('No') >> if_email_mismatch_30 >> rail.Label('Yes') >> update_email_address >> log_email_updated >> if_rehireupdate_not_equals_rehire_35

        if_email_mismatch_30 >> rail.Label('No') >> if_rehireupdate_not_equals_rehire_35 >> rail.Label('Yes') >> if_terminationdate_present_and_not_equal_enddate
        if_rehireupdate_not_equals_rehire_35 >> rail.Label('No') >> get_user_udf_values

        if_terminationdate_present_and_not_equal_enddate >> rail.Label('Yes') >> update_end_date >> log_termination_date_updated >> get_user_udf_values
        if_terminationdate_present_and_not_equal_enddate >> rail.Label('No') >> get_user_udf_values >> if_CF_Date_of_Birth_MM_DD_YYYY_present_41

        if_CF_Date_of_Birth_MM_DD_YYYY_present_41 >> rail.Label('Yes') >> validate_CF_Date_of_Birth_MM_DD_YYYY >> rail.Label('Yes') >> check_dob_mismatch
        validate_CF_Date_of_Birth_MM_DD_YYYY >> rail.Label('No') >> log_birthdate_invalid >> if_businesstitle_present_and_mismatch
        check_dob_mismatch >> rail.Label('Yes') >> update_dob_udf >> log_dob_updated >> if_businesstitle_present_and_mismatch
        check_dob_mismatch >> rail.Label('No') >> if_businesstitle_present_and_mismatch
        if_CF_Date_of_Birth_MM_DD_YYYY_present_41 >> rail.Label('No') >> if_businesstitle_present_and_mismatch

        if_businesstitle_present_and_mismatch >> rail.Label('Yes') >> update_title_udf >> log_title_updated >> if_yearsofservice_present_and_mismatch
        if_businesstitle_present_and_mismatch >> rail.Label('No') >> if_yearsofservice_present_and_mismatch

        if_yearsofservice_present_and_mismatch >> rail.Label('Yes') >> update_yearsofservice_udf >> log_yearsofservice_updated >> if_fieldhr_present_and_mismatch
        if_yearsofservice_present_and_mismatch >> rail.Label('No') >> if_fieldhr_present_and_mismatch

        if_fieldhr_present_and_mismatch >> rail.Label('Yes') >> update_fieldhr_udf >> log_fieldhr_updated >> if_gender_present_and_mismatch
        if_fieldhr_present_and_mismatch >> rail.Label('No') >> if_gender_present_and_mismatch

        if_gender_present_and_mismatch >> rail.Label('Yes') >> update_gender_udf >> getdata_sup_emp_grp_dept_grp
        if_gender_present_and_mismatch >> rail.Label('No') >> getdata_sup_emp_grp_dept_grp

        getdata_sup_emp_grp_dept_grp >> if_manager_id_present_with_effective_date >> rail.Label('Yes') >> if_managerid_equals_workrefempid >> rail.Label('Yes') >> log_supervisor_sameas_user >> compare_to_today
        if_managerid_equals_workrefempid >> rail.Label('No') >> search_for_user_with_empid >> check_if_multiple_manageruseruri_present >> rail.Label('Yes') >> log_multiple_user_for_same_managerid >> compare_to_today
        check_if_multiple_manageruseruri_present >> rail.Label('No') >> if_supervisor_present
        if_supervisor_present >> rail.Label('No') >> log_supervisor_assignment
        if_supervisor_present >> rail.Label('Yes') >> get_manager_details >> if_manager_details_present_and_enabled >> rail.Label('Yes') >> get_assigned_permissionset_foruser >> if_supervisor_permission_not_assigned >> rail.Label('Yes') >> get_all_permission_sets >> add_missing_supervisor_permission >> if_search_user_supervisor_null

        if_supervisor_permission_not_assigned >> rail.Label('No') >> if_search_user_supervisor_null

        if_search_user_supervisor_null >> rail.Label('Yes') >> update_initial_supervisor >> log_initial_supervisor_added >> if_search_user_supervisor_not_null
        if_search_user_supervisor_null >> rail.Label('No') >> if_search_user_supervisor_not_null

        if_search_user_supervisor_not_null >> rail.Label('Yes') >> get_current_supervisor_uri >> if_current_supervisor_uri_mismatch_manager_uri

        if_current_supervisor_uri_mismatch_manager_uri >> rail.Label('Yes') >> update_supervisor_assignment_over_daterange >> log_supervisor_updated >> compare_to_today
        if_current_supervisor_uri_mismatch_manager_uri >> rail.Label('No') >> compare_to_today

        if_search_user_supervisor_not_null >> rail.Label('No') >> compare_to_today

        if_manager_details_present_and_enabled >> rail.Label('No') >> log_supervisor_assignment >> compare_to_today
        if_manager_id_present_with_effective_date >> rail.Label('No') >> compare_to_today

        compare_to_today >> get_startdate_of_next_timesheet >> validate_exemptioneff_date_160 >> rail.Label('Yes') >> update_exemption_change_variable_161 >> validate_workshift_change_effect_date_162
        validate_exemptioneff_date_160 >> rail.Label('No') >> validate_workshift_change_effect_date_162 >> rail.Label('Yes') >> update_workshift_change_variable_163 >> validate_effect_date_of_workertype_164
        validate_workshift_change_effect_date_162 >> rail.Label('No') >> validate_effect_date_of_workertype_164 >> rail.Label('Yes') >> update_workertype_change_variable_165 >> if_location_present_and_validate_cflrv_loc_changedate
        validate_effect_date_of_workertype_164 >> rail.Label('No') >> if_location_present_and_validate_cflrv_loc_changedate

        if_location_present_and_validate_cflrv_loc_changedate >> rail.Label('Yes') >> update_location_change_variable_168 >> compute_mapper_keys
        if_location_present_and_validate_cflrv_loc_changedate >> rail.Label('No') >> compute_mapper_keys

        compute_mapper_keys >> log_employeetypetobeassigned >> get_effectiveusergroupmembership >> validate_employeetype >> rail.Label('Yes') >> get_all_employee_type_details >> \
            if_employeetype_present >> rail.Label('Yes') >> update_employeetype_group >> update_variable_timeofftrigger >> log_employeetype_updated >> log_timesheetapprovalpathtobeassigned_198
        if_employeetype_present >> rail.Label('No') >> log_employeetype_not_updated >> log_timesheetapprovalpathtobeassigned_198
        validate_employeetype >> rail.Label('No') >> log_timesheetapprovalpathtobeassigned_198

        log_timesheetapprovalpathtobeassigned_198 >> if_timesheetapprovalpathtobeassigned_present_and_not_equalsto_curr_path >> rail.Label('Yes') >> get_all_approvalpaths_timesheet >> update_approval_path_touser >> log_approvalpaths_timesheet_updated >> log_holidaycalendartobeassigned_204
        if_timesheetapprovalpathtobeassigned_present_and_not_equalsto_curr_path >> rail.Label('No') >> log_holidaycalendartobeassigned_204

        log_holidaycalendartobeassigned_204 >> if_holidaycalendar_tobeassigned_present_and_not_equalto_curr_calendar >> rail.Label('Yes') >> get_all_holidaycalendars >> update_holiday_calendar_foruser >> log_holiday_calendar_updated >> log_timesheettemplatetobeassigned_211
        if_holidaycalendar_tobeassigned_present_and_not_equalto_curr_calendar >> rail.Label('No') >> log_timesheettemplatetobeassigned_211

        log_timesheettemplatetobeassigned_211 >> get_policysets >> if_timesheet_mismatch >> rail.Label('Yes') >> if_timesheet_templateuri_present >> rail.Label('Yes') >> update_timesheet_template \
            >> log_timesheettemplate_updated >> log_punchentrypolicy_tobeassigned_218 >> get_punchentry_policy_uri >> if_punchentrypolicy_uri_present >> rail.Label('Yes') >> get_next_timesheet_duedate >> if_day_is_present_in_nexttimesheet_duedate >> rail.Label('Yes') >> punch_entry_policy_ref_log_add_entry >> if_paygroup_present
        if_punchentrypolicy_uri_present >> rail.Label('No') >> if_paygroup_present
        if_day_is_present_in_nexttimesheet_duedate >> rail.Label('No') >> if_paygroup_present
        # Recipe nests the punch-entry block [218]-[226] inside [215] (timesheet-template
        # uri present) with no else, so a not-found template skips punch entirely and
        # continues at the next domain. Only the exception detail is recorded here.
        if_timesheet_templateuri_present >> rail.Label('No') >> log_timesheettemplate_not_updated >> if_paygroup_present
        if_timesheet_mismatch >> rail.Label('No') >> if_paygroup_present

        if_paygroup_present >> rail.Label('Yes') >> paygroup_mismatch_present >> rail.Label('Yes') >> get_servicecenter_group_data >> servicecenter_list >> log_req_servicecenter_uri >> if_paygroup_present_and_contains_urn >> rail.Label('Yes') >> update_service_center_paygroup >> log_paygroup_updated >> if_cost_center_present
        if_paygroup_present >> rail.Label('No') >> if_cost_center_present
        paygroup_mismatch_present >> rail.Label('No') >> if_cost_center_present
        if_paygroup_present_and_contains_urn >> rail.Label('No') >> log_exception_paygroup_not_updated >> if_cost_center_present

        if_cost_center_present >> rail.Label('Yes') >> costcenter_mismatch_present >> rail.Label('Yes') >> get_costcenter_group_data >> costcenter_list >> log_req_costcentergroup_uri >> if_costcenter_uri_present >> rail.Label('Yes') >> log_costcenter_changeeffdate \
            >> update_cost_center_group >> log_costcenter_updated >> if_legalentity_present

        if_cost_center_present >> rail.Label('No') >> if_legalentity_present
        costcenter_mismatch_present >> rail.Label('No') >> if_legalentity_present
        if_costcenter_uri_present >> rail.Label('No') >> log_costcenter_notupdated_exception >> if_legalentity_present

        if_legalentity_present >> rail.Label('Yes') >> legalentity_mismatch_present >> rail.Label('Yes') >> get_division_group_data >> legalentity_list >> \
            log_req_legalentity_division_uri >> if_legalentity_uri_present >> rail.Label('Yes') >> update_division_group >> log_legalentity_updated >> if_location_present

        if_legalentity_present >> rail.Label('No') >> if_location_present
        legalentity_mismatch_present >> rail.Label('No') >> if_location_present
        if_legalentity_uri_present >> rail.Label('No') >> log_legalentity_notupdated_exception >> if_location_present

        if_location_present >> rail.Label('Yes') >> location_mismatch_present >> rail.Label('Yes') >> if_departmentgrp_uri_present >> rail.Label('Yes') >> log_location_change_eff_date >>\
            update_department_group >> put_policy_data_access_scopes_for_userdepartmentrestricted >> log_department_group_updated >> update_timeofftrigger_262 >> log_payruletobeassigned

        if_location_present >> rail.Label('No') >> log_payruletobeassigned
        location_mismatch_present >> rail.Label('No') >> log_payruletobeassigned
        if_departmentgrp_uri_present >> rail.Label('No') >> log_department_group_notupdated_exception >> log_payruletobeassigned

        log_payruletobeassigned >> if_payruletobeassigned_present >> rail.Label('Yes') >> if_payrule_scriptschedule_is_present >> rail.Label('Yes') >> latest_payrule_name >>\
            validate_payrule_name >> rail.Label('Yes') >> get_req_payrule_script >> if_payrule_uri_present_in_scripts >> rail.Label('Yes') >> update_payrule_for_user >> log_payrule_updated >> log_schedule_tobeassigned

        if_payruletobeassigned_present >> rail.Label('No') >> log_schedule_tobeassigned
        if_payrule_scriptschedule_is_present >> rail.Label('No') >> log_schedule_tobeassigned
        validate_payrule_name >> rail.Label('No') >> log_schedule_tobeassigned
        if_payrule_uri_present_in_scripts >> rail.Label('No') >> log_schedule_tobeassigned

        log_schedule_tobeassigned >> if_schedule_tobeassigned_present >> rail.Label('Yes') >> if_schedule_policies_present >> rail.Label('Yes') >> latest_schedule_policy_name >>\
            validate_schedule_policy_name >> rail.Label('Yes') >> log_schedule_policy_change_eff_date >> get_req_schedule_script >> if_schedule_uri_present_in_scripts >> rail.Label('Yes') >>\
            build_schedule_entries >> put_policy_schedule_for_user >> log_schedule_policy_updated >> log_actvities_tobeassigned

        if_schedule_tobeassigned_present >> rail.Label('No') >> log_actvities_tobeassigned
        if_schedule_policies_present >> rail.Label('No') >> log_actvities_tobeassigned
        validate_schedule_policy_name >> rail.Label('No') >> log_actvities_tobeassigned
        if_schedule_uri_present_in_scripts >> rail.Label('No') >> if_schedule_tobeassigned_equals_shift >> rail.Label('Yes') >> build_shift_schedule_entries \
            >> update_shift_schedule_for_user >> log_schedule_shift_updated >> log_actvities_tobeassigned
        if_schedule_tobeassigned_equals_shift >> rail.Label('No') >> log_actvities_tobeassigned

        log_actvities_tobeassigned >> if_activities_tobeassigned_not_present >> rail.Label('Yes') >> put_activityassignments_for_user >> create_activity_list
        if_activities_tobeassigned_not_present >> rail.Label('No') >> create_activity_list >> split_activities_bydelim >> get_all_enabled_activities >> update_activity_list >>\
            if_activity_uris_present >> rail.Label('Yes') >> assign_activities_to_user >> log_activity_updated >> if_timeofftrigger_var_equals_true
        if_activity_uris_present >> rail.Label('No') >> if_timeofftrigger_var_equals_true

        if_timeofftrigger_var_equals_true >> rail.Label('Yes') >> log_timeofftypes_tobeassigned >> trigger_update_user_timeoff >> wait_for_update_user_timeoff >> gather_result_from_child \
            >> if_error_in_gather_result_from_child >> rail.Label('Yes') >> stop_processing_due_to_error_in_child >> log_user_import
        if_error_in_gather_result_from_child >> rail.Label('No') >> log_user_import

        if_timeofftrigger_var_equals_true >> rail.Label('No') >> log_user_import

        log_user_import >> catch_and_log_error


        return dag


rail.for_each_instance(create_dag)
