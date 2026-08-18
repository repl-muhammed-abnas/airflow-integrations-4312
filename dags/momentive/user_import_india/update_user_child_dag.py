# pylint: disable=too-many-statements
from datetime import timedelta, datetime
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from momentive.user_import_india.utils import python_callable, request_payload
from momentive.user_import_india.mappers.momentive_user_import_mapper import momentive_userimport_mapper

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.momentive_india_user_sync_child_update_user_dag_id,
        description=f'Momentive_india_user_sync_update_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
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

        create_businesstitle_change = rail.SetVariableOperator(
            task_id='create_businesstitle_change',
            append=False,
            name='businesstitle_change',
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

        create_timeofftrigger = rail.SetVariableOperator(
            task_id='create_timeofftrigger',
            append=False,
            name='timeofftrigger',
            value='false'
        )

        exception_log = rail.CreateLogOperator(
            task_id="exception_log"
        )

        log_entries = rail.CreateLogOperator(
            task_id="log_entries"
        )

        get_input_validation_log = rail.PythonOperator(
            task_id="get_input_validation_log",
            python_callable=python_callable.get_input_validationlog
        )

        if_input_validation_log_present = rail.IfOperator(
            task_id='if_input_validation_log_present',
            test="{{ result('get_input_validation_log').exc_present | is_truthy }}",
            yes_task="log_user_import_not_created",
            no_task="if_workertype_not_contingentworker_and_businesstitle_not_present",
        )

        log_user_import_not_created = rail.WriteLogOperator(
            task_id="log_user_import_not_created",
            log='{{ dag_run.conf.user_import_logs}}',
            message="na",
            severity="Exception",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + "|" + dag_run.conf['lastname'],
                "action": "Update",
                "status": "Exception",
                "details": "User not updated," + rail.result('get_input_validation_log')['exc_value'] + "| NA",
                "childjobid": get_dagrun_ecid(dag_run),
            }
        )

        if_workertype_not_contingentworker_and_businesstitle_not_present = rail.IfOperator(
            task_id='if_workertype_not_contingentworker_and_businesstitle_not_present',
            test=lambda dag_run: dag_run.conf['workertype'] == 'Contingent Worker' and dag_run.conf['businesstitle'].startswith(
                "ext"),
            yes_task="log_user_import_not_created_contingent_non_alp",
            no_task="get_user_data",
        )

        log_user_import_not_created_contingent_non_alp = rail.WriteLogOperator(
            task_id="log_user_import_not_created_contingent_non_alp",
            log='{{ dag_run.conf.user_import_logs}}',
            message="na",
            severity="Exception",
            properties={
                "jobid": "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.userid }}",
                "username": "{{ dag_run.conf.firstname }}" + "|" + "{{ dag_run.conf.lastname }}",
                "action": "Update",
                "status": "Exception",
                "details": "User update skipped since woker belongs to Contingent worker non ALP group",
                "childjobid": "{{ dag_run_ecid() }}",
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

        if_rehireupdate_equals_rehire = rail.IfOperator(
            task_id='if_rehireupdate_equals_rehire',
            test="{{ dag_run.conf.rehire_update == 'rehire' }}",
            yes_task="update_timeofftrigger_17",
            no_task="if_active_1_and_isEnabled_falsy_20",
        )

        update_timeofftrigger_17 = rail.SetVariableOperator(
            task_id='update_timeofftrigger_17',
            append=False,
            name='{{ result("create_timeofftrigger").name }}',
            value='true'
        )

        if_active_1_and_isEnabled_falsy_20 = rail.IfOperator(
            task_id='if_active_1_and_isEnabled_falsy_20',
            test=lambda dag_run: bool(dag_run.conf['active'] == 1 and rail.result(
                'get_user_data')[0]['userDetails']['isEnabled'] == False),
            yes_task='validate_hiredate_and_startdate',
            no_task='get_all_permissionsets'
        )

        validate_hiredate_and_startdate = rail.PythonOperator(
            task_id="validate_hiredate_and_startdate",
            python_callable=python_callable.validate_hiredate_startdate
        )

        if_validate_hiredate_and_startdate_is_false_22 = rail.IfOperator(
            task_id='if_validate_hiredate_and_startdate_is_false_22',
            test="{{ result('validate_hiredate_and_startdate') | is_falsy }}",
            yes_task="remove_end_date_and_update_rehire_date",
            no_task="enable_userprofile",
        )

        remove_end_date_and_update_rehire_date = rail.RepliconServiceOperator(
            task_id='remove_end_date_and_update_rehire_date',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": python_callable.split_date_string(dag_run.conf['hiredate'], 'datetime')
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
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "User enabled in Replicon and end date removed"
            }
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: {
                'basic_user_with_report_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'name', "Basic User with Reports", 'uri'),
                'supervisor': rail.find_first_by_attr_and_get_attr(
                    response, 'name', "Supervisor - Edit", 'uri')
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
            task_id="update_firstname",
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
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

        if_lastname_mismatch = rail.IfOperator(
            task_id="if_lastname_mismatch",
            test="{{ (result('get_user_data')[0].userDetails.lastName | is_falsy or \
                result('get_user_data')[0].userDetails.lastName.lower() != dag_run.conf.lastname.lower()) and \
                dag_run.conf.lastname | is_truthy }}",
            yes_task="update_lastname",
            no_task="if_email_mismatch"
        )

        update_lastname = rail.RepliconServiceOperator(
            task_id="update_lastname",
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
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

        if_email_mismatch = rail.IfOperator(
            task_id="if_email_mismatch",
            test="{{ (result('get_user_data')[0].userDetails.emailAddress | is_falsy or \
                result('get_user_data')[0].userDetails.emailAddress.lower() != dag_run.conf.emailaddress.lower()) and \
                dag_run.conf.emailaddress | is_truthy }}",
            yes_task="update_email_address",
            no_task="if_rehireupdate_not_equals_rehire"
        )

        update_email_address = rail.RepliconServiceOperator(
            task_id="update_email_address",
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.emailaddress }}"
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

        if_rehireupdate_not_equals_rehire = rail.IfOperator(
            task_id='if_rehireupdate_not_equals_rehire',
            test="{{ dag_run.conf.rehire_update != 'rehire' }}",
            yes_task="if_terminationdate_present_and_not_equal_enddate",
            no_task="get_required_user_customfields",
        )

        if_terminationdate_present_and_not_equal_enddate = rail.IfOperator(
            task_id='if_terminationdate_present_and_not_equal_enddate',
            test=python_callable.validate_terminationdate_enddate,
            yes_task="update_end_date_39",
            no_task="get_required_user_customfields",
        )

        update_end_date_39 = rail.RepliconServiceOperator(
            task_id='update_end_date_39',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": python_callable.split_date_string(dag_run.conf['hiredate'], 'datetime'),
                    "endDate": python_callable.split_date_string(dag_run.conf['terminationdate'], 'datetime')
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

        get_required_user_customfields = rail.RepliconServiceOperator(
            task_id='get_required_user_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': "urn:replicon:object-type:user"
            }
        )

        get_user_udf_values = rail.PythonOperator(
            task_id="get_user_udf_values",
            python_callable=python_callable.get_udf_values_from_userdetails
        )

        if_CF_Date_of_Birth_MM_DD_YYYY_present = rail.IfOperator(
            task_id='if_CF_Date_of_Birth_MM_DD_YYYY_present',
            test="{{ dag_run.conf.CF_Date_of_Birth_MM_DD_YYYY | is_truthy }}",
            yes_task="validate_CF_Date_of_Birth_MM_DD_YYYY",
            no_task="if_gender_present_and_mismatch",
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
            log="{{ result('exception_log') }}",
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
            no_task="if_gender_present_and_mismatch",
        )

        update_dob_udf = rail.RepliconServiceOperator(
            task_id='update_dob_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": rail.result('get_user_udf_values')['dob_uri'],
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

        if_gender_present_and_mismatch = rail.IfOperator(
            task_id='if_gender_present_and_mismatch',
            test="{{ dag_run.conf.gender | is_truthy and \
                dag_run.conf.gender.lower() != result('get_user_udf_values').gender }}",
            yes_task="update_gender_udf",
            no_task="if_fieldhr_present_and_mismatch",
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

        if_fieldhr_present_and_mismatch = rail.IfOperator(
            task_id='if_fieldhr_present_and_mismatch',
            test="{{ dag_run.conf.fieldhr | is_truthy and \
                dag_run.conf.fieldhr.lower() != result('get_user_udf_values').hrm }}",
            yes_task="update_fieldhr_udf",
            no_task="if_businesstitle_present_and_mismatch",
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
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "HRM field updated"
            }
        )

        if_businesstitle_present_and_mismatch = rail.IfOperator(
            task_id='if_businesstitle_present_and_mismatch',
            test="{{ dag_run.conf.businesstitle | is_truthy and \
                dag_run.conf.businesstitle.lower() != result('get_user_udf_values').title }}",
            yes_task="update_title_udf",
            no_task="getdata_sup_emp_grp_dept_grp",
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
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Business title field updated"
            }
        )

        getdata_sup_emp_grp_dept_grp = rail.RepliconServiceOperator(
            task_id="getdata_sup_emp_grp_dept_grp",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_data_sup_emp_grp_dept_grp
        )

        if_manager_id_present_with_effective_date = rail.IfOperator(
            task_id='if_manager_id_present_with_effective_date',
            test="{{ dag_run.conf.managerid | is_truthy and dag_run.conf.effective_date_of_manager_change | is_truthy }}",
            yes_task="if_managerid_equals_workrefempid",
            no_task="compare_to_today",
        )

        if_managerid_equals_workrefempid = rail.IfOperator(
            task_id='if_managerid_equals_workrefempid',
            test="{{ dag_run.conf.managerid == dag_run.conf.Worker_Reference_Employee_ID }}",
            yes_task="log_supervisor_sameas_user",
            no_task="search_for_user_with_empid",
        )

        log_supervisor_sameas_user = rail.WriteLogOperator(
            task_id='log_supervisor_sameas_user',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Supervsior not updated for since user's supervsior can not be same as the user"
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
            no_task="get_manager_details",
        )

        log_multiple_user_for_same_managerid = rail.WriteLogOperator(
            task_id='log_multiple_user_for_same_managerid',
            log="{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value": "Supervisor not assigned for user {{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }} as \
                    multiple users have same Employee ID:{{ dag_run.conf.managerid }} ."
            }
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
            data={
                "userUri": "{{ result('search_for_user_with_empid')[0].uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'user.uri', '')
        )

        if_supervisor_permission_not_assigned = rail.IfOperator(
            task_id='if_supervisor_permission_not_assigned',
            test="{{ result('get_assigned_permissionset_foruser') | is_falsy }}",
            yes_task="add_missing_supervisor_permission",
            no_task="if_search_user_supervisor_null_96",
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=request_payload.add_missing_supervisor_permission_payload
        )

        if_search_user_supervisor_null_96 = rail.IfOperator(
            task_id='if_search_user_supervisor_null_96',
            test="{{ result('getdata_sup_emp_grp_dept_grp').rows | is_truthy and result('getdata_sup_emp_grp_dept_grp').rows[0].cells[2] | is_truthy and \
                result('getdata_sup_emp_grp_dept_grp').rows[0].cells[2].dataType == 'urn:replicon:list-type:null' }}",
            yes_task="update_initial_supervisor",
            no_task="if_search_user_supervisor_not_null_99",
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
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Initial supervisor added"
            }
        )

        if_search_user_supervisor_not_null_99 = rail.IfOperator(
            task_id='if_search_user_supervisor_not_null_99',
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
                    dag_run.conf['effective_date_of_manager_change'] if dag_run.conf['effective_date_of_manager_change'] else str(
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
                        dag_run.conf['effective_date_of_manager_change'] if dag_run.conf['effective_date_of_manager_change'] else str(
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

        compare_to_today = rail.PythonOperator(
            task_id="compare_to_today",
            python_callable=python_callable.compare_dates_to_today
        )

        validate_exemptioneff_date_112 = rail.IfOperator(
            task_id='validate_exemptioneff_date_112',
            test="{{ result('compare_to_today').exemption_eff_date | is_truthy }}",
            yes_task="update_exemption_change_variable_113",
            no_task="validate_businesstitle_change_effect_date_114",
        )

        update_exemption_change_variable_113 = rail.SetVariableOperator(
            task_id='update_exemption_change_variable_113',
            append=False,
            name='{{ result("create_Exemptionstatus_change").name }}',
            value='true'
        )

        validate_businesstitle_change_effect_date_114 = rail.IfOperator(
            task_id='validate_businesstitle_change_effect_date_114',
            test="{{ result('compare_to_today').cf_lrv_businesstitle_change_effective_date | is_truthy }}",
            yes_task="update_businesstitle_change_variable_115",
            no_task="validate_effect_date_of_workertype_116",
        )

        update_businesstitle_change_variable_115 = rail.SetVariableOperator(
            task_id='update_businesstitle_change_variable_115',
            append=False,
            name='{{ result("create_businesstitle_change").name }}',
            value='true'
        )

        validate_effect_date_of_workertype_116 = rail.IfOperator(
            task_id='validate_effect_date_of_workertype_116',
            test="{{ result('compare_to_today').effective_date_of_workertype | is_truthy }}",
            yes_task="update_workertype_change_variable_117",
            no_task="if_location_present_and_validate_cflrv_loc_changedate",
        )

        update_workertype_change_variable_117 = rail.SetVariableOperator(
            task_id='update_workertype_change_variable_117',
            append=False,
            name='{{ result("create_workertype_change").name }}',
            value='true'
        )

        if_location_present_and_validate_cflrv_loc_changedate = rail.IfOperator(
            task_id='if_location_present_and_validate_cflrv_loc_changedate',
            test="{{ dag_run.conf.location | is_truthy and \
                result('compare_to_today').cf_lrv_location_change_effective_date | is_truthy }}",
            yes_task="update_location_change_variable_120",
            no_task="if_request_location_equals_to_inchennai_123",
        )

        update_location_change_variable_120 = rail.SetVariableOperator(
            task_id='update_location_change_variable_120',
            append=False,
            name='{{ result("create_location_change").name }}',
            value='true'
        )

        if_request_location_equals_to_inchennai_123 = rail.IfOperator(
            task_id='if_request_location_equals_to_inchennai_123',
            test='''{{ dag_run.conf.location == 'IN Chennai' }}''',
            yes_task="log_businesstitle_124",
            no_task="log_businesstitle_126",
        )

        log_businesstitle_124 = rail.PythonOperator(
            task_id='log_businesstitle_124',
            python_callable=lambda dag_run: "Trainee" if dag_run.conf['businesstitle'].lower().startswith("trainee") else (
                "EXT" if dag_run.conf['businesstitle'].lower().startswith("ext") else (
                    "DCS" if dag_run.conf['businesstitle'].lower().startswith("dcs") else "Any"))
        )

        log_businesstitle_126 = rail.PythonOperator(
            task_id='log_businesstitle_126',
            python_callable=lambda dag_run:  "Any" if dag_run.conf['businesstitle'].lower().startswith("trainee") else (
                "EXT" if dag_run.conf['businesstitle'].lower().startswith("ext") else (
                    "Any" if dag_run.conf['businesstitle'].lower().startswith("dcs") else "Any"))
        )

        log_businesstitle_127 = rail.PythonOperator(
            task_id='log_businesstitle_127',
            python_callable=lambda:  rail.result(
                'log_businesstitle_124') or rail.result('log_businesstitle_126')
        )

        momentive_userimport_mapper_search_entries_128 = rail.PythonOperator(
            task_id='momentive_userimport_mapper_search_entries_128',
            python_callable=lambda dag_run:  list(filter(lambda x: x["type"] == "Employee Type" and x["workertype"] == dag_run.conf['workertype'] and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and (
                x['businesstitle'] == rail.result('log_businesstitle_127') if rail.result('log_businesstitle_127') else x['businesstitle']), momentive_userimport_mapper))
        )

        momentive_userimport_mapper_search_entries_129 = rail.PythonOperator(
            task_id='momentive_userimport_mapper_search_entries_129',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["location"] == dag_run.conf['location'], momentive_userimport_mapper))
        )

        get_timesheet_for_date2 = rail.RepliconServiceOperator(
            task_id='get_timesheet_for_date2',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=request_payload.get_timesheet_for_date2_payload
        )

        if_get_timesheet_for_date2_uri_present = rail.IfOperator(
            task_id='if_get_timesheet_for_date2_uri_present',
            test="{{ result('get_timesheet_for_date2').timesheet.uri | is_truthy }}",
            yes_task="get_timesheet_details",
            no_task="get_effectiveusergroupmembership",
        )

        get_timesheet_details = rail.RepliconServiceOperator(
            task_id='get_timesheet_details',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
                "timesheetUri": "{{ result('get_timesheet_for_date2').timesheet.uri }}"
            }
        )

        get_startdate_of_next_timesheet = rail.PythonOperator(
            task_id="get_startdate_of_next_timesheet",
            python_callable=python_callable.get_startday_of_nexttimesheet
        )

        get_effectiveusergroupmembership = rail.RepliconServiceOperator(
            task_id="get_effectiveusergroupmembership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        search_entry_in_mapper_for_employeetype = rail.PythonOperator(
            task_id='search_entry_in_mapper_for_employeetype',
            python_callable=lambda dag_run:  list(filter(lambda x: x["type"] == "Employee Type" and x["workertype"] == dag_run.conf['workertype'] and x["exemptstatus"] ==
                                                  dag_run.conf['exemptionstatus'] and x['businesstitle'] == rail.result('log_businesstitle_127'), rail.result("momentive_userimport_mapper_search_entries_128")))[0]['value']
        )

        validate_employeetype = rail.IfOperator(
            task_id='validate_employeetype',
            test=lambda: bool(not (rail.result('get_effectiveusergroupmembership')['employeeTypes']) or
                              not (rail.result('get_effectiveusergroupmembership')['employeeTypes'][0]['employeeType']['employeeType']['uri']) or
                              (rail.result('get_effectiveusergroupmembership')['employeeTypes'][0]['employeeType']['employeeType']['displayText'] != rail.result(
                                  'search_entry_in_mapper_for_employeetype'))),
            yes_task="get_all_employee_type",
            no_task="search_entry_in_mapper_for_timesheet_template",
        )

        get_all_employee_type = rail.RepliconServiceOperator(
            task_id="get_all_employee_type",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText', rail.result(
                    'search_entry_in_mapper_for_employeetype'), 'uri', '')
        )

        if_employeetype_present = rail.IfOperator(
            task_id='if_employeetype_present',
            test="{{ result('get_all_employee_type') | is_truthy }}",
            yes_task="update_employeetype_group",
            no_task="log_employeetype_not_updated",
        )

        log_employeetype_not_updated = rail.WriteLogOperator(
            task_id='log_employeetype_not_updated',
            log="{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value": """Employee type was not updated as the employee type 
                '{{ result('search_entry_in_mapper_for_employeetype').value }}' not found in Replicon """
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

        search_entry_in_mapper_for_timesheet_template = rail.PythonOperator(
            task_id='search_entry_in_mapper_for_timesheet_template',
            python_callable=lambda dag_run:  list(filter(lambda x: x["type"] == "Timesheet Template" and x["workertype"] == dag_run.conf['workertype'] and x["location"] == dag_run.conf['location'] and x[
                                                  "exemptstatus"] == dag_run.conf['exemptionstatus'] and x['businesstitle'] == rail.result('log_businesstitle_127'), rail.result("momentive_userimport_mapper_search_entries_129")))[0]['value']
        )

        mapper_search_punch_entry_policy = rail.PythonOperator(
            task_id='mapper_search_punch_entry_policy',
            python_callable=lambda dag_run:  list(filter(lambda x: x["type"] == "Punch Entry Policy" and x["workertype"] == dag_run.conf['workertype'] and x["location"] == dag_run.conf['location'] and x[
                                                  "exemptstatus"] == dag_run.conf['exemptionstatus'] and x['businesstitle'] == rail.result('log_businesstitle_127'), rail.result("momentive_userimport_mapper_search_entries_129")))[0]['value']
        )

        get_policysets = rail.RepliconServiceOperator(
            task_id='get_policysets',
            endpoint='/services/PolicySetService1.svc/GetAllPolicySets',
            data_handler=lambda response: {
                'existing_timesheettemplate': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('get_user_data')[0]['timesheetTemplate'], 'uri', ''),
                'timeoff': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Time Off', 'uri', ''),
                'timesheet': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('search_entry_in_mapper_for_timesheet_template'), 'uri', ''),
                'punchentry': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('mapper_search_punch_entry_policy'), 'uri', '')
            }
        )

        if_timesheet_mismatch = rail.IfOperator(
            task_id='if_timesheet_mismatch',
            test="{{ result('search_entry_in_mapper_for_timesheet_template') | is_truthy and \
                result('search_entry_in_mapper_for_timesheet_template') != result('get_user_data')[0].timesheetTemplate.displayText }}",
            yes_task="if_timesheet_templateuri_present",
            no_task="search_entry_in_mapper_for_holiday_calendar",
        )

        if_timesheet_templateuri_present = rail.IfOperator(
            task_id='if_timesheet_templateuri_present',
            test="{{ result('get_policysets').timesheet | is_truthy }}",
            yes_task="update_timesheet_template",
            no_task="get_timesheet_period_schedule_for_user",
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
                "value": "Timesheettemplate was  updated"
            }
        )

        get_timesheet_period_schedule_for_user = rail.RepliconServiceOperator(
            task_id='get_timesheet_period_schedule_for_user',
            endpoint="/services/TimesheetPeriodService2.svc/GetTimesheetPeriodScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        check_if_timesheet_period_schedule_list_size_less_than_0 = rail.IfOperator(
            task_id="check_if_timesheet_period_schedule_list_size_less_than_0",
            test=lambda: bool(
                len(rail.result("get_timesheet_period_schedule_for_user")) < 0),
            yes_task="put_timesheet_period_schedule_for_user",
            no_task="search_entry_in_mapper_for_holiday_calendar"
        )

        put_timesheet_period_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_timesheet_period_schedule_for_user',
            endpoint="/services/TimesheetPeriodService2.svc/PutTimesheetPeriodScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "scheduleEntries": [
                    {
                        "timesheetPeriod": {
                            "name": "Monthly"
                        }
                    }
                ]
            }
        )

        search_entry_in_mapper_for_holiday_calendar = rail.PythonOperator(
            task_id='search_entry_in_mapper_for_holiday_calendar',
            python_callable=lambda dag_run:  list(filter(lambda x: x["type"] == "Holiday Calendar" and x["workertype"] == dag_run.conf['workertype'] and x["location"] == dag_run.conf['location'] and x[
                                                  "exemptstatus"] == dag_run.conf['exemptionstatus'] and x['businesstitle'] == rail.result('log_businesstitle_127'), rail.result("momentive_userimport_mapper_search_entries_129")))[0]['value']
        )

        if_holidaycalendar_mismatch = rail.IfOperator(
            task_id='if_holidaycalendar_mismatch',
            test="{{ result('search_entry_in_mapper_for_holiday_calendar') | is_truthy and \
                result('search_entry_in_mapper_for_holiday_calendar') != result('get_user_data')[0].holidayCalendar.displayText }}",
            yes_task="get_required_holidaycalendar_uri",
            no_task="validate_department",
        )

        get_required_holidaycalendar_uri = rail.RepliconServiceOperator(
            task_id='get_required_holidaycalendar_uri',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', rail.result('search_entry_in_mapper_for_holiday_calendar'), 'uri', '')
        )

        if_holidaycalendar_uri_present = rail.IfOperator(
            task_id='if_holidaycalendar_uri_present',
            test="{{ result('get_required_holidaycalendar_uri') | is_truthy }}",
            yes_task="update_holidaycalendar",
            no_task="validate_department",
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
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Holiday calendar updated"
            }
        )

        validate_department = rail.IfOperator(
            task_id='validate_department',
            test=lambda dag_run: bool(not (rail.result('get_effectiveusergroupmembership')['departments']) or not (rail.result(
                'get_effectiveusergroupmembership')['departments'][0]['department']['department']['displayText']) or rail.result(
                'get_effectiveusergroupmembership')['departments'][0]['department']['department']['displayText'] != dag_run.conf['location']),
            yes_task="get_department_group_data",
            no_task="if_location_change_is_true",
        )

        get_department_group_data = rail.RepliconServiceOperator(
            task_id="get_department_group_data",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.search_location_department_group_payload,
            data_handler=lambda response: [{
                "fullpath":  rail.smartjoin_by_delim([x['textValue'] for x in item['cells'][-1]['cellCollection'] if x['textValue']], ' / '),
                #    item['cells'][-1]['cellCollection'][0]['textValue'] + '/' + item['cells'][-1]['cellCollection'][1]['textValue'] + item['cells'][-1]['cellCollection'][2]['textValue'],
                "name": item['cells'][0]['textValue'] if item['cells'][0]['dataType'] == "urn:replicon:list-type:object" else '',
                "uri": item['cells'][0]['uri'] if item['cells'][0]['dataType'] == "urn:replicon:list-type:object" else ''
            } for item in response['rows']] if response['rows'] else ''
        )

        get_required_department_group_uri = rail.PythonOperator(
            task_id="get_required_department_group_uri",
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_department_group_data'), 'fullpath', "Momentive / India / " + dag_run.conf['location'], "uri", '')
        )

        if_required_department_group_uri_present = rail.IfOperator(
            task_id="if_required_department_group_uri_present",
            test=lambda: bool(rail.result(
                'get_required_department_group_uri')),
            yes_task="update_departmentgroup",
            no_task="log_dept_grp_exception"
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
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Department group updated"
            }
        )

        log_dept_grp_exception = rail.WriteLogOperator(
            task_id='log_dept_grp_exception',
            log="{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value": "Department group was not updated as the department(location) '{{ dag_run.conf.location }} not found in Replicon"
            }
        )

        if_location_change_is_true = rail.IfOperator(
            task_id='if_location_change_is_true',
            test=lambda: bool(rail.get_dag_run_var('location_change')),
            yes_task='update_timeofftrigger_197',
            no_task="create_payrule_variable",
        )

        update_timeofftrigger_197 = rail.SetVariableOperator(
            task_id='update_timeofftrigger_197',
            append=False,
            name='{{ result("create_timeofftrigger").name }}',
            value='true'
        )

        get_assigned_time_punch_policy_for_user = rail.RepliconServiceOperator(
            task_id='get_assigned_time_punch_policy_for_user',
            endpoint="/services/PolicySetService1.svc/GetAssignedPolicySetsForUser",
            data={
                'userUri': "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', "urn:replicon:policy:time-punch", 'policySet.displayText', '')
        )

        if_existing_punchpolicy_not_equals_to_be_assigned = rail.IfOperator(
            task_id="if_existing_punchpolicy_not_equals_to_be_assigned",
            test=lambda: bool(rail.result("get_assigned_time_punch_policy_for_user") != rail.result(
                "mapper_search_punch_entry_policy")),
            yes_task="if_punch_entry_policy_uri_exists",
            no_task="create_payrule_variable"
        )

        if_punch_entry_policy_uri_exists = rail.IfOperator(
            task_id="if_punch_entry_policy_uri_exists",
            test=lambda: bool(rail.result("get_policysets")['punchentry']),
            yes_task='assign_punch_entry_policy_set_to_user',
            no_task='log_punch_entry_policy_exception'
        )

        assign_punch_entry_policy_set_to_user = rail.RepliconServiceOperator(
            task_id='assign_punch_entry_policy_set_to_user',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('get_policysets').punchentry }}"
            }
        )

        log_punch_entry_policy_updated = rail.WriteLogOperator(
            task_id='log_punch_entry_policy_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Punch entry policy was updated"
            }
        )

        log_punch_entry_policy_exception = rail.WriteLogOperator(
            task_id='log_punch_entry_policy_exception',
            log="{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value": '''Punch entry policy was not updated as the Punch entry policy "{{result('mapper_search_punch_entry_policy'}}" not found in Replicon'''
            }
        )

        create_payrule_variable = rail.SetVariableOperator(
            task_id='create_payrule_variable',
            append=False,
            name='payrule',
            value=''
        )

        if_request_location_equals_to_inbangalorembs_210 = rail.IfOperator(
            task_id='if_request_location_equals_to_inbangalorembs_210',
            test='''{{ dag_run.conf.location == 'IN Bangalore MBS' }}''',
            yes_task="if_request_india_spec_schedule_indicator_equals_to_yes_211",
            no_task="update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_mbs_216",
        )

        if_request_india_spec_schedule_indicator_equals_to_yes_211 = rail.IfOperator(
            task_id='if_request_india_spec_schedule_indicator_equals_to_yes_211',
            test='''{{ dag_run.conf.India_Spec_schedule_Indicator == 'Yes' }}''',
            yes_task="update_variable_when_india_spec_schedule_indicatorisyesandlocationis_mbs_212",
            no_task="update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_mbs_214",
        )

        update_variable_when_india_spec_schedule_indicatorisyesandlocationis_mbs_212 = rail.SetVariableOperator(
            task_id='update_variable_when_india_spec_schedule_indicatorisyesandlocationis_mbs_212',
            append=False,
            name='{{ result("create_payrule_variable").name }}',
            value=lambda dag_run: list(filter(lambda x: x["type"] == "Payrule" and x["workertype"] == dag_run.conf['workertype'] and x["location"] == dag_run.conf['location'] and x["exemptstatus"] == dag_run.conf['exemptionstatus']
                                       and x['businesstitle'] == "Premium", rail.result('momentive_userimport_mapper_search_entries_129')))[0]['value'].strip() if rail.result('momentive_userimport_mapper_search_entries_129') else ''
        )

        update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_mbs_214 = rail.SetVariableOperator(
            task_id='update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_mbs_214',
            append=False,
            name='{{ result("create_payrule_variable").name }}',
            value=lambda dag_run: list(filter(lambda x: x["type"] == "Payrule" and x["workertype"] == dag_run.conf['workertype'] and
                                              x["location"] == dag_run.conf['location'] and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and
                                              x['businesstitle'] == rail.result('log_businesstitle_127'), rail.result(
                'momentive_userimport_mapper_search_entries_129')))[0]['value'].strip() if rail.result(
                'momentive_userimport_mapper_search_entries_129') else ''
        )

        update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_mbs_216 = rail.SetVariableOperator(
            task_id='update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_mbs_216',
            append=False,
            name='{{ result("create_payrule_variable").name }}',
            value=lambda dag_run: list(filter(lambda x: x["type"] == "Payrule" and x["workertype"] == dag_run.conf['workertype'] and x["location"] == dag_run.conf['location'] and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and x['businesstitle'] == rail.result(
                'log_businesstitle_127'), rail.result('momentive_userimport_mapper_search_entries_129')))[0]['value'].strip() if rail.result('momentive_userimport_mapper_search_entries_129') else ''
        )

        create_payrule_list = rail.SetVariableOperator(
            task_id='create_payrule_list',
            append=True,
            name='payrule_list',
            value=[]
        )

        if_payrule_script_schedule_list_size_greater_than_0 = rail.IfOperator(
            task_id="if_payrule_script_schedule_list_size_greater_than_0",
            test=lambda: bool(len(rail.result('get_user_data')[
                              0]['payRuleScriptSchedule']) > 0),
            yes_task="latest_payrule_name",
            no_task="validate_payrule_name"
        )

        latest_payrule_name = rail.PythonOperator(
            task_id='latest_payrule_name',
            python_callable=lambda: request_payload.get_current_value_from_schedule_list_for_user(
                rail.result('get_user_data')[0]['payRuleScriptSchedule'], 'payRuleScript', 'displayText')
        )

        validate_payrule_name = rail.IfOperator(
            task_id="validate_payrule_name",
            test=lambda: bool(not (rail.result("latest_payrule_name")) or rail.result(
                "latest_payrule_name") != rail.get_dag_run_var('payrule')),
            yes_task="get_req_payrule_script",
            no_task="search_entry_in_mapper_for_schedule"
        )

        get_req_payrule_script = rail.RepliconServiceOperator(
            task_id='get_req_payrule_script',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.get_dag_run_var('payrule'), 'uri', '')
        )

        get_payrule_script_assignment_schedule_for_user = rail.RepliconServiceOperator(
            task_id='get_payrule_script_assignment_schedule_for_user',
            endpoint="/services/PayRuleScriptService2.svc/GetPayRuleScriptAssignmentScheduleForUser",
            data={
                'userUri': "{{ dag_run.conf.useruri }}"
            }
        )

        def get_latest_payrule_list():
            final_payrule_list_new = []
            user_details_start_date = rail.result(
                'get_user_data')[0]['userDetails']['employmentDateRange']['startDate']
            for item in rail.result('get_user_data')[0]['payRuleScriptSchedule']:
                if (not (item['effectiveDate'])):
                    final_payrule_list_new.append({
                        "payRuleScript": {
                            "uri": item['payRuleScript']['uri'],
                            "name": item['payRuleScript']['displayText'],
                        }
                    })
                elif (item['effectiveDate']):
                    if (datetime.strptime(str(item['effectiveDate']['day']) + "/" + str(
                            item['effectiveDate']['month']) + "/" + str(item['effectiveDate']['year']), "%d/%m/%Y") < datetime.strptime(
                                rail.result('get_startdate_of_next_timesheet'), "%Y-%m-%d")):
                        final_payrule_list_new.append({
                            "payRuleScript": {
                                "uri": item['payRuleScript']['uri'],
                                "name": item['payRuleScript']['displayText'],
                            },
                            "effectiveDate": {
                                "year": int(user_details_start_date['year']),
                                "month": int(user_details_start_date['month']),
                                "day": int(user_details_start_date['day'])
                            }
                        })

            final_payrule_list_new.append({
                "payRuleScript": {
                    "uri": rail.result('get_req_payrule_script'),
                    "name": "",
                },
                "effectiveDate": {
                    "year": rail.result('get_startdate_of_next_timesheet').split("-")[0],
                    "month": rail.result('get_startdate_of_next_timesheet').split("-")[1],
                    "day": rail.result('get_startdate_of_next_timesheet').split("-")[2]
                }
            })

            return final_payrule_list_new

        final_payrule_list = rail.PythonOperator(
            task_id='final_payrule_list',
            python_callable=get_latest_payrule_list
        )

        if_final_payrule_list_new_exists = rail.IfOperator(
            task_id="if_final_payrule_list_new_exists",
            test=lambda: bool(rail.result("final_payrule_list")),
            yes_task="update_payrule_script_assignment_schedule_for_user",
            no_task="search_entry_in_mapper_for_schedule"
        )

        update_payrule_script_assignment_schedule_for_user = rail.RepliconServiceOperator(
            task_id='update_payrule_script_assignment_schedule_for_user',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                "scheduleEntries": rail.result("final_payrule_list")
            }
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

        search_entry_in_mapper_for_schedule = rail.PythonOperator(
            task_id='search_entry_in_mapper_for_schedule',
            python_callable=lambda dag_run:  list(filter(lambda x: x["type"] == "Schedule" and x["workertype"] == dag_run.conf['workertype'] and x["location"] == dag_run.conf['location'] and x["exemptstatus"]
                                                  == dag_run.conf['exemptionstatus'] and x['businesstitle'] == rail.result('log_businesstitle_127'), rail.result("momentive_userimport_mapper_search_entries_129")))[0]['value']
        )

        if_schedule_mapper_search_true = rail.IfOperator(
            task_id="if_schedule_mapper_search_true",
            test=lambda: bool(rail.result(
                "search_entry_in_mapper_for_schedule")),
            yes_task="get_current_office_schedule_name",
            no_task="if_request_location_equals_to_IN_bangalore_mbs_and_indicator_yes_299"
        )

        get_current_office_schedule_name = rail.PythonOperator(
            task_id="get_current_office_schedule_name",
            python_callable=lambda: request_payload.get_current_value_from_schedule_list_for_user(
                rail.result('get_user_data')[0]['schedulePolicies'], 'officeSchedule', 'displayText')
        )

        if_current_office_schedule_not_equals_search_entry_in_mapper_for_schedule = rail.IfOperator(
            task_id="if_current_office_schedule_not_equals_search_entry_in_mapper_for_schedule",
            test=lambda: bool(rail.result("get_current_office_schedule_name") != rail.result(
                "search_entry_in_mapper_for_schedule")),
            yes_task="get_req_schedule_script",
            no_task="if_request_location_equals_to_IN_bangalore_mbs_and_indicator_yes_299"
        )

        get_req_schedule_script = rail.RepliconServiceOperator(
            task_id='get_req_schedule_script',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('search_entry_in_mapper_for_schedule'), 'uri', '')
        )

        if_get_req_schedule_script_present = rail.IfOperator(
            task_id="if_get_req_schedule_script_present",
            test="{{ result('get_req_schedule_script') | is_truthy }}",
            yes_task="update_schedule_policy",
            no_task="if_schedule_equals_shift"
        )

        update_schedule_policy = rail.RepliconServiceOperator(
            task_id="update_schedule_policy",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.schedule_update_payload
        )

        if_schedule_equals_shift = rail.IfOperator(
            task_id="if_schedule_equals_shift",
            test="{{ result('search_entry_in_mapper_for_schedule') == 'Shift' }}",
            yes_task="update_schedule_policy_300",
            no_task="if_request_location_equals_to_IN_bangalore_mbs_and_indicator_yes_299"
        )

        update_schedule_policy_300 = rail.RepliconServiceOperator(
            task_id="update_schedule_policy_300",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.schedule_update_payload
        )

        log_schedule_updated = rail.WriteLogOperator(
            task_id='log_schedule_updated',
            log="{{ result('log_entries') }}",
            message="na",
            severity="Success",
            properties={
                "value": "Schedule updated"
            }
        )

        if_request_location_equals_to_IN_bangalore_mbs_and_indicator_yes_299 = rail.IfOperator(
            task_id='if_request_location_equals_to_IN_bangalore_mbs_and_indicator_yes_299',
            test='''{{ dag_run.conf.location == 'IN Bangalore MBS' and dag_run.conf.India_Spec_schedule_Indicator == 'Yes' }}''',
            yes_task="log_activitytobeassigned_301",
            no_task="log_activitytobeassigned_303",
        )

        log_activitytobeassigned_301 = rail.PythonOperator(
            task_id='log_activitytobeassigned_301',
            python_callable=lambda dag_run: list(filter(lambda x: x["type"] == "Activity" and x["workertype"] == dag_run.conf['workertype'] and
                                                        x["location"] == dag_run.conf['location'] and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and x['businesstitle'] == rail.result(
                'log_businesstitle_127'), rail.result("momentive_userimport_mapper_search_entries_129"))) if rail.result(
                'momentive_userimport_mapper_search_entries_129') else ''
        )

        log_activitytobeassigned_303 = rail.PythonOperator(
            task_id='log_activitytobeassigned_303',
            python_callable=lambda dag_run: list(filter(lambda x: x["type"] == "Activity" and x["workertype"] == dag_run.conf['workertype'] and
                                                        x["location"] == dag_run.conf['location'] and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and x['businesstitle'] == rail.result(
                'log_businesstitle_127'), rail.result("momentive_userimport_mapper_search_entries_129"))) if rail.result(
                'momentive_userimport_mapper_search_entries_129') else ''
        )

        log_activitytobeassigned_304 = rail.PythonOperator(
            task_id='log_activitytobeassigned_304',
            python_callable=lambda:  rail.result(
                'log_activitytobeassigned_301') or rail.result('log_activitytobeassigned_303')
        )

        if_log_activitytobeassigned_116_present_305 = rail.IfOperator(
            task_id='if_log_activitytobeassigned_116_present_305',
            test='''{{ result('log_activitytobeassigned_304') | is_truthy }}''',
            yes_task="get_enabled_activities_309",
            no_task="put_activity_assignments_for_user_306",
        )

        put_activity_assignments_for_user_306 = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_306',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "activityUris": []
            }
        )

        get_enabled_activities_309 = rail.RepliconServiceOperator(
            task_id='get_enabled_activities_309',
            endpoint="/services/ActivityService1.svc/GetEnabledActivities",
        )

        log_activtiesuri_310 = rail.PythonOperator(
            task_id='log_activtiesuri_310',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_enabled_activities_309'), 'name', rail.result('log_activitytobeassigned_304')[0]['value'].strip(), 'uri', '')
        )

        put_activity_assignments_for_user_311 = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_311',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "activityUris": ["{{ result('log_activtiesuri_310') }}"]
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

        if_timeoff_trigger_true = rail.IfOperator(
            task_id="if_timeoff_trigger_true",
            test=lambda: bool(rail.get_dag_run_var(
                'timeofftrigger') == 'true'),
            yes_task="search_mapper_for_timeoff_types",
            no_task="log_user_import"
        )

        search_mapper_for_timeoff_types = rail.PythonOperator(
            task_id='search_mapper_for_timeoff_types',
            python_callable=lambda dag_run:  list(filter(lambda x: x["type"] == "Time off Types" and x["workertype"] == dag_run.conf['workertype'] and x["location"] == dag_run.conf['location'] and x["exemptstatus"] == dag_run.conf['exemptionstatus'] and x['businesstitle'] == rail.result(
                'log_businesstitle_127'), rail.result("momentive_userimport_mapper_search_entries_129")))[0]['value']
        )

        trigger_update_user_timeoff = rail.TriggerDagRunOperator(
            task_id='trigger_update_user_timeoff',
            trigger_dag_id=config.momentive_india_user_sync_child_update_user_timeoff_assign_id,
            conf=request_payload.trigger_updateuser_timeoff,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_update_user_timeoff = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_user_timeoff',
            dag_runs='{{ result("trigger_update_user_timeoff") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        log_user_import = rail.WriteLogOperator(
            task_id='log_user_import',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity=lambda: "Exception" if len(rail.load_all_records(
                rail.result("exception_log"))) > 0 else "Success",
            properties=python_callable.get_status_and_details_for_update
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log='{{ dag_run.conf.user_import_logs}}',
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "jobid": "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.userid }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "action": "Update",
                "status": "Error",
                'details': "User partially updated," + "{{ get_error_message() }}",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> create_workertype_change

        create_workertype_change >> create_businesstitle_change >> create_Exemptionstatus_change >> create_location_change >> \
            create_timeofftrigger >> exception_log >> log_entries >> get_input_validation_log >> if_input_validation_log_present

        if_input_validation_log_present >> rail.Label(
            'Yes') >> log_user_import_not_created >> catch_and_log_error
        if_input_validation_log_present >> rail.Label(
            'No') >> if_workertype_not_contingentworker_and_businesstitle_not_present

        if_workertype_not_contingentworker_and_businesstitle_not_present >> rail.Label(
            'Yes') >> log_user_import_not_created_contingent_non_alp >> catch_and_log_error
        if_workertype_not_contingentworker_and_businesstitle_not_present >> rail.Label(
            'No') >> get_user_data

        get_user_data >> if_rehireupdate_equals_rehire

        if_rehireupdate_equals_rehire >> rail.Label(
            'Yes') >> update_timeofftrigger_17 >> if_active_1_and_isEnabled_falsy_20
        if_rehireupdate_equals_rehire >> rail.Label(
            'No') >> if_active_1_and_isEnabled_falsy_20

        if_active_1_and_isEnabled_falsy_20 >> rail.Label(
            'Yes') >> validate_hiredate_and_startdate >> if_validate_hiredate_and_startdate_is_false_22

        if_validate_hiredate_and_startdate_is_false_22 >> rail.Label(
            'Yes') >> remove_end_date_and_update_rehire_date >> enable_userprofile
        if_validate_hiredate_and_startdate_is_false_22 >> rail.Label(
            'No') >> enable_userprofile

        enable_userprofile >> log_user_enabled >> get_all_permissionsets

        if_active_1_and_isEnabled_falsy_20 >> rail.Label(
            'No') >> get_all_permissionsets >> if_firstname_mismatch

        if_firstname_mismatch >> rail.Label(
            'Yes') >> update_firstname >> log_first_name_updated >> if_lastname_mismatch
        if_firstname_mismatch >> rail.Label('No') >> if_lastname_mismatch

        if_lastname_mismatch >> rail.Label(
            'Yes') >> update_lastname >> log_last_name_updated >> if_email_mismatch
        if_lastname_mismatch >> rail.Label('No') >> if_email_mismatch

        if_email_mismatch >> rail.Label(
            'Yes') >> update_email_address >> log_email_updated >> if_rehireupdate_not_equals_rehire
        if_email_mismatch >> rail.Label(
            'No') >> if_rehireupdate_not_equals_rehire

        if_rehireupdate_not_equals_rehire >> rail.Label(
            'Yes') >> if_terminationdate_present_and_not_equal_enddate
        if_rehireupdate_not_equals_rehire >> rail.Label(
            'No') >> get_required_user_customfields

        if_terminationdate_present_and_not_equal_enddate >> rail.Label('Yes') >> update_end_date_39 >> log_termination_date_updated >> \
            get_required_user_customfields
        if_terminationdate_present_and_not_equal_enddate >> rail.Label(
            'No') >> get_required_user_customfields

        get_required_user_customfields >> get_user_udf_values >> if_CF_Date_of_Birth_MM_DD_YYYY_present

        if_CF_Date_of_Birth_MM_DD_YYYY_present >> rail.Label(
            'Yes') >> validate_CF_Date_of_Birth_MM_DD_YYYY
        if_CF_Date_of_Birth_MM_DD_YYYY_present >> rail.Label(
            'No') >> if_gender_present_and_mismatch

        validate_CF_Date_of_Birth_MM_DD_YYYY >> rail.Label(
            'Yes') >> check_dob_mismatch
        validate_CF_Date_of_Birth_MM_DD_YYYY >> rail.Label(
            'No') >> log_birthdate_invalid >> if_gender_present_and_mismatch

        check_dob_mismatch >> rail.Label(
            'Yes') >> update_dob_udf >> log_dob_updated >> if_gender_present_and_mismatch
        check_dob_mismatch >> rail.Label(
            'No') >> if_gender_present_and_mismatch

        if_gender_present_and_mismatch >> rail.Label(
            'Yes') >> update_gender_udf >> if_fieldhr_present_and_mismatch
        if_gender_present_and_mismatch >> rail.Label(
            'No') >> if_fieldhr_present_and_mismatch

        if_fieldhr_present_and_mismatch >> rail.Label(
            'Yes') >> update_fieldhr_udf >> log_fieldhr_updated >> if_businesstitle_present_and_mismatch
        if_fieldhr_present_and_mismatch >> rail.Label(
            'No') >> if_businesstitle_present_and_mismatch

        if_businesstitle_present_and_mismatch >> rail.Label(
            'Yes') >> update_title_udf >> log_title_updated >> getdata_sup_emp_grp_dept_grp
        if_businesstitle_present_and_mismatch >> rail.Label(
            'No') >> getdata_sup_emp_grp_dept_grp

        getdata_sup_emp_grp_dept_grp >> if_manager_id_present_with_effective_date

        if_manager_id_present_with_effective_date >> rail.Label(
            'No') >> compare_to_today
        if_manager_id_present_with_effective_date >> rail.Label(
            'Yes') >> if_managerid_equals_workrefempid

        if_managerid_equals_workrefempid >> rail.Label(
            'Yes') >> log_supervisor_sameas_user >> compare_to_today
        if_managerid_equals_workrefempid >> rail.Label(
            'No') >> search_for_user_with_empid >> check_if_multiple_manageruseruri_present

        check_if_multiple_manageruseruri_present >> rail.Label(
            'Yes') >> log_multiple_user_for_same_managerid >> compare_to_today
        check_if_multiple_manageruseruri_present >> rail.Label(
            'No') >> get_manager_details >> if_manager_details_present_and_enabled

        if_manager_details_present_and_enabled >> rail.Label(
            'Yes') >> get_assigned_permissionset_foruser >> if_supervisor_permission_not_assigned

        if_supervisor_permission_not_assigned >> rail.Label(
            'Yes') >> add_missing_supervisor_permission >> if_search_user_supervisor_null_96
        if_supervisor_permission_not_assigned >> rail.Label(
            'No') >> if_search_user_supervisor_null_96

        if_search_user_supervisor_null_96 >> rail.Label(
            'Yes') >> update_initial_supervisor >> log_initial_supervisor_added
        if_search_user_supervisor_null_96 >> rail.Label(
            'No') >> if_search_user_supervisor_not_null_99

        if_search_user_supervisor_not_null_99 >> rail.Label(
            'Yes') >> get_current_supervisor_uri >> if_current_supervisor_uri_mismatch_manager_uri

        if_current_supervisor_uri_mismatch_manager_uri >> rail.Label(
            'Yes') >> update_supervisor_assignment_over_daterange >> log_supervisor_updated >> compare_to_today

        if_current_supervisor_uri_mismatch_manager_uri >> rail.Label(
            'No') >> compare_to_today

        if_search_user_supervisor_not_null_99 >> rail.Label(
            'No') >> compare_to_today

        if_manager_details_present_and_enabled >> rail.Label(
            'No') >> log_supervisor_assignment >> compare_to_today

        compare_to_today >> validate_exemptioneff_date_112

        validate_exemptioneff_date_112 >> rail.Label(
            'Yes') >> update_exemption_change_variable_113 >> validate_businesstitle_change_effect_date_114
        validate_exemptioneff_date_112 >> rail.Label(
            'No') >> validate_businesstitle_change_effect_date_114

        validate_businesstitle_change_effect_date_114 >> rail.Label(
            'Yes') >> update_businesstitle_change_variable_115 >> validate_effect_date_of_workertype_116
        validate_businesstitle_change_effect_date_114 >> rail.Label(
            'No') >> validate_effect_date_of_workertype_116

        validate_effect_date_of_workertype_116 >> rail.Label(
            'Yes') >> update_workertype_change_variable_117 >> if_location_present_and_validate_cflrv_loc_changedate
        validate_effect_date_of_workertype_116 >> rail.Label(
            'No') >> if_location_present_and_validate_cflrv_loc_changedate

        if_location_present_and_validate_cflrv_loc_changedate >> rail.Label(
            'Yes') >> update_location_change_variable_120 >> if_request_location_equals_to_inchennai_123
        if_location_present_and_validate_cflrv_loc_changedate >> rail.Label(
            'No') >> if_request_location_equals_to_inchennai_123

        if_request_location_equals_to_inchennai_123 >> rail.Label(
            'Yes') >> log_businesstitle_124 >> log_businesstitle_127

        if_request_location_equals_to_inchennai_123 >> rail.Label(
            'No') >> log_businesstitle_126 >> log_businesstitle_127

        log_businesstitle_127 >> momentive_userimport_mapper_search_entries_128 >> momentive_userimport_mapper_search_entries_129

        momentive_userimport_mapper_search_entries_129 >> get_timesheet_for_date2 >> if_get_timesheet_for_date2_uri_present

        if_get_timesheet_for_date2_uri_present >> rail.Label(
            'Yes') >> get_timesheet_details >> get_startdate_of_next_timesheet >> get_effectiveusergroupmembership
        if_get_timesheet_for_date2_uri_present >> rail.Label(
            'No') >> get_effectiveusergroupmembership

        get_effectiveusergroupmembership >> search_entry_in_mapper_for_employeetype >> validate_employeetype

        validate_employeetype >> rail.Label(
            'Yes') >> get_all_employee_type >> if_employeetype_present

        if_employeetype_present >> rail.Label('Yes') >> update_employeetype_group >> update_variable_timeofftrigger \
            >> log_employeetype_updated >> search_entry_in_mapper_for_timesheet_template
        if_employeetype_present >> rail.Label(
            'No') >> log_employeetype_not_updated >> search_entry_in_mapper_for_timesheet_template

        validate_employeetype >> rail.Label(
            'No') >> search_entry_in_mapper_for_timesheet_template

        search_entry_in_mapper_for_timesheet_template >> mapper_search_punch_entry_policy >> get_policysets >> if_timesheet_mismatch

        if_timesheet_mismatch >> rail.Label(
            'Yes') >> if_timesheet_templateuri_present

        if_timesheet_templateuri_present >> rail.Label(
            'Yes') >> update_timesheet_template >> log_timesheettemplate_updated >> get_timesheet_period_schedule_for_user
        if_timesheet_templateuri_present >> rail.Label(
            'No') >> get_timesheet_period_schedule_for_user

        get_timesheet_period_schedule_for_user >> check_if_timesheet_period_schedule_list_size_less_than_0

        check_if_timesheet_period_schedule_list_size_less_than_0 >> rail.Label(
            'Yes') >> put_timesheet_period_schedule_for_user >> search_entry_in_mapper_for_holiday_calendar
        check_if_timesheet_period_schedule_list_size_less_than_0 >> rail.Label(
            'No') >> search_entry_in_mapper_for_holiday_calendar

        if_timesheet_mismatch >> rail.Label(
            'No') >> search_entry_in_mapper_for_holiday_calendar

        search_entry_in_mapper_for_holiday_calendar >> if_holidaycalendar_mismatch

        if_holidaycalendar_mismatch >> rail.Label('No') >> validate_department
        if_holidaycalendar_mismatch >> rail.Label(
            'Yes') >> get_required_holidaycalendar_uri >> if_holidaycalendar_uri_present

        if_holidaycalendar_uri_present >> rail.Label(
            'Yes') >> update_holidaycalendar >> log_holidaycalendar_updated >> validate_department
        if_holidaycalendar_uri_present >> rail.Label(
            'No') >> validate_department

        validate_department >> rail.Label(
            'Yes') >> get_department_group_data >> get_required_department_group_uri >> if_required_department_group_uri_present

        if_required_department_group_uri_present >> rail.Label(
            'Yes') >> update_departmentgroup >> assign_policyDataAccessScopes_department >> log_dept_grp_updated >> if_location_change_is_true
        if_required_department_group_uri_present >> rail.Label(
            'No') >> log_dept_grp_exception >> if_location_change_is_true

        validate_department >> rail.Label('No') >> if_location_change_is_true

        if_location_change_is_true >> rail.Label(
            'No') >> create_payrule_variable

        if_location_change_is_true >> rail.Label(
            'Yes') >> update_timeofftrigger_197 >> get_assigned_time_punch_policy_for_user >> if_existing_punchpolicy_not_equals_to_be_assigned

        if_existing_punchpolicy_not_equals_to_be_assigned >> rail.Label(
            'Yes') >> if_punch_entry_policy_uri_exists

        if_punch_entry_policy_uri_exists >> rail.Label(
            'Yes') >> assign_punch_entry_policy_set_to_user >> log_punch_entry_policy_updated >> create_payrule_variable
        if_punch_entry_policy_uri_exists >> rail.Label(
            'No') >> log_punch_entry_policy_exception >> create_payrule_variable

        if_existing_punchpolicy_not_equals_to_be_assigned >> rail.Label(
            'No') >> create_payrule_variable

        create_payrule_variable >> if_request_location_equals_to_inbangalorembs_210

        if_request_location_equals_to_inbangalorembs_210 >> rail.Label(
            'Yes') >> if_request_india_spec_schedule_indicator_equals_to_yes_211

        if_request_india_spec_schedule_indicator_equals_to_yes_211 >> rail.Label(
            'Yes') >> update_variable_when_india_spec_schedule_indicatorisyesandlocationis_mbs_212 >> create_payrule_list
        if_request_india_spec_schedule_indicator_equals_to_yes_211 >> rail.Label(
            'No') >> update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_mbs_214 >> create_payrule_list

        if_request_location_equals_to_inbangalorembs_210 >> rail.Label(
            'No') >> update_variable_when_india_spec_schedule_indicatoris_noandlocationisnot_mbs_216 >> create_payrule_list

        create_payrule_list >> if_payrule_script_schedule_list_size_greater_than_0

        if_payrule_script_schedule_list_size_greater_than_0 >> rail.Label(
            'Yes') >> latest_payrule_name >> validate_payrule_name
        if_payrule_script_schedule_list_size_greater_than_0 >> rail.Label(
            'No') >> validate_payrule_name

        validate_payrule_name >> rail.Label('Yes') >> get_req_payrule_script >> get_payrule_script_assignment_schedule_for_user \
            >> final_payrule_list >> if_final_payrule_list_new_exists

        if_final_payrule_list_new_exists >> rail.Label(
            'No') >> search_entry_in_mapper_for_schedule
        if_final_payrule_list_new_exists >> rail.Label('Yes') >> update_payrule_script_assignment_schedule_for_user \
            >> log_payrule_updated >> search_entry_in_mapper_for_schedule

        validate_payrule_name >> rail.Label(
            'No') >> search_entry_in_mapper_for_schedule

        search_entry_in_mapper_for_schedule >> if_schedule_mapper_search_true

        if_schedule_mapper_search_true >> rail.Label(
            'No') >> if_request_location_equals_to_IN_bangalore_mbs_and_indicator_yes_299
        if_schedule_mapper_search_true >> rail.Label('Yes') >> get_current_office_schedule_name \
            >> if_current_office_schedule_not_equals_search_entry_in_mapper_for_schedule

        if_current_office_schedule_not_equals_search_entry_in_mapper_for_schedule >> rail.Label(
            'No') >> if_request_location_equals_to_IN_bangalore_mbs_and_indicator_yes_299
        if_current_office_schedule_not_equals_search_entry_in_mapper_for_schedule >> rail.Label(
            'Yes') >> get_req_schedule_script >> if_get_req_schedule_script_present

        if_get_req_schedule_script_present >> rail.Label(
            'No') >> if_schedule_equals_shift
        if_get_req_schedule_script_present >> rail.Label(
            'Yes') >> update_schedule_policy >> if_schedule_equals_shift

        if_schedule_equals_shift >> rail.Label('Yes') >> update_schedule_policy_300 \
            >> log_schedule_updated >> if_request_location_equals_to_IN_bangalore_mbs_and_indicator_yes_299
        if_schedule_equals_shift >> rail.Label(
            'No') >> if_request_location_equals_to_IN_bangalore_mbs_and_indicator_yes_299

        if_request_location_equals_to_IN_bangalore_mbs_and_indicator_yes_299 >> rail.Label(
            'Yes') >> log_activitytobeassigned_301 >> log_activitytobeassigned_304
        if_request_location_equals_to_IN_bangalore_mbs_and_indicator_yes_299 >> rail.Label(
            'No') >> log_activitytobeassigned_303 >> log_activitytobeassigned_304

        log_activitytobeassigned_304 >> if_log_activitytobeassigned_116_present_305

        if_log_activitytobeassigned_116_present_305 >> rail.Label(
            'No') >> put_activity_assignments_for_user_306 >> if_timeoff_trigger_true
        if_log_activitytobeassigned_116_present_305 >> rail.Label('Yes') >> get_enabled_activities_309 \
            >> log_activtiesuri_310 >> put_activity_assignments_for_user_311 >> log_activity_updated >> if_timeoff_trigger_true

        if_timeoff_trigger_true >> rail.Label(
            'Yes') >> search_mapper_for_timeoff_types >> trigger_update_user_timeoff >> wait_for_update_user_timeoff >> log_user_import
        if_timeoff_trigger_true >> rail.Label('No') >> log_user_import

        log_user_import >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
