from datetime import timedelta
from airflow.models import Variable
import rail
from dominos.user_import.utils.request_payload import get_replicon_date, get_today_date
from dominos.user_import.utils.response_filter import get_supervisor_details, is_assign_supervisorpermission


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/dominos/user_import/config.py


# pylint: disable=too-many-statements
def create_updateuser_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dominos_userimport_child_update_user_{config.instance}',
        description=f'Dominos_Child_Update User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.updateuser_child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_report'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_report',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_report = rail.RepliconServiceOperator(
            task_id='get_user_report',
            endpoint="/services/ReportService1.svc/GenerateReport",
            data=lambda dag_run: {
                "reportUri": dag_run.conf['reporturi'],
                "filterValues": [
                    {
                        "reportFilterUri": dag_run.conf['reportfilteruri'],
                        "value": dag_run.conf['useruri'].split(':')[-1]
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('get_user_report').payload }}"
        )

        parse_csv_user_data = rail.PythonOperator(
            task_id='parse_csv_user_data',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv'))[0]
        )

        is_firstname_present = rail.IfOperator(
            task_id='is_firstname_present',
            test="{{ dag_run.conf.firstname | is_truthy and dag_run.conf.firstname != \
                result('parse_csv_user_data')['User First Name'] }}",
            yes_task="update_firstname",
            no_task="is_lastname_present",
        )

        update_firstname = rail.RepliconServiceOperator(
            task_id='update_firstname',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        is_lastname_present = rail.IfOperator(
            task_id='is_lastname_present',
            test="{{ dag_run.conf.lastname | is_truthy and \
                dag_run.conf.lastname != result('parse_csv_user_data')['User Last Name'] }}",
            yes_task="update_lastname",
            no_task="is_employeeid_present",
        )

        update_lastname = rail.RepliconServiceOperator(
            task_id='update_lastname',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        is_employeeid_present = rail.IfOperator(
            task_id='is_employeeid_present',
            test="{{ dag_run.conf.employeeid | is_truthy and \
                dag_run.conf.employeeid != result('parse_csv_user_data')['Employee ID'] }}",
            yes_task="update_employeeid",
            no_task="is_email_present",
        )

        update_employeeid = rail.RepliconServiceOperator(
            task_id='update_employeeid',
            endpoint="/services/UserService1.svc/UpdateEmployeeId",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeId": "{{ dag_run.conf.employeeid }}"
            }
        )

        is_email_present = rail.IfOperator(
            task_id='is_email_present',
            test="{{ dag_run.conf.email | is_truthy and \
                dag_run.conf.email != result('parse_csv_user_data')['User Email'] }}",
            yes_task="update_email",
            no_task="is_loginstatus_terminated",
        )

        update_email = rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.email }}"
            }
        )

        is_loginstatus_terminated = rail.IfOperator(
            task_id='is_loginstatus_terminated',
            test="{{ dag_run.conf.loginstatus == 'Terminated' \
                and result('parse_csv_user_data')['User Status'] != 'Disabled' }}",
            yes_task="is_enddate_present",
            no_task="is_loginstatus_present",
        )

        is_enddate_present = rail.IfOperator(
            task_id='is_enddate_present',
            test="{{ dag_run.conf.enddate | is_truthy }}",
            yes_task="update_employment_daterange",
            no_task="is_loginstatus_present",
        )

        update_employment_daterange = rail.RepliconServiceOperator(
            task_id='update_employment_daterange',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": get_replicon_date(rail.result('parse_csv_user_data')['User Start Date'], '%m/%d/%Y'),
                    "endDate": get_replicon_date(dag_run.conf['enddate'])
                }
            }
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        is_loginstatus_present = rail.IfOperator(
            task_id='is_loginstatus_present',
            test="{{ dag_run.conf.loginstatus | is_truthy }}",
            yes_task="is_loginstatus_equals_active",
            no_task="get_usercustom_field_uri",
        )

        is_loginstatus_equals_active = rail.IfOperator(
            task_id='is_loginstatus_equals_active',
            test="{{ dag_run.conf.loginstatus == 'Active' \
                and result('parse_csv_user_data')['User Status'] == 'Disabled' }}",
            yes_task="enable_login",
            no_task="get_usercustom_field_uri",
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        get_usercustom_field_uri = rail.RepliconServiceOperator(
            task_id='get_usercustom_field_uri',
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroup",
            data={
                "objectTypeUri": "urn:replicon:object-type:user"
            }
        )

        get_required_user_customfields = rail.RepliconServiceOperator(
            task_id='get_required_user_customfields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                'cost_center_udf': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Cost Center', 'uri', ''),
                'cost_center_name_udf': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Cost Center Name', 'uri', '')
            }
        )

        is_costcenter_udf_update = rail.IfOperator(
            task_id='is_costcenter_udf_update',
            test="{{ result('parse_csv_user_data')['Cost Center'] != dag_run.conf.departmentcode and \
                result('get_required_user_customfields').cost_center_udf | is_truthy }}",
            yes_task="update_costcenter_udf",
            no_task="is_costcentername_udf_update",
        )

        update_costcenter_udf = rail.RepliconServiceOperator(
            task_id='update_costcenter_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').cost_center_udf }}",
                "value": "{{ dag_run.conf.departmentcode }}"
            }
        )

        is_costcentername_udf_update = rail.IfOperator(
            task_id='is_costcentername_udf_update',
            test="{{ result('parse_csv_user_data')['Cost Center Name'] != dag_run.conf.department and \
                result('get_required_user_customfields').cost_center_name_udf | is_truthy }}",
            yes_task="update_costcentername_udf",
            no_task="is_employeetype_present",
        )

        update_costcentername_udf = rail.RepliconServiceOperator(
            task_id='update_costcentername_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').cost_center_name_udf }}",
                "value": "{{ dag_run.conf.department }}"
            }
        )

        is_employeetype_present = rail.IfOperator(
            task_id='is_employeetype_present',
            test="{{ dag_run.conf.employeetype | is_truthy and \
                dag_run.conf.employeetype != 'T' \
                    and result('parse_csv_user_data')['Employee Type'] != \
                        'Full-time Salaried' }}",
            yes_task="get_fulltimesalaried_employee_type",
            no_task="is_employeetype_present2",
        )

        get_fulltimesalaried_employee_type = rail.RepliconServiceOperator(
            task_id='get_fulltimesalaried_employee_type',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Full-time Salaried', 'uri', '')
        )

        is_fulltimesalaried_employeetype_present = rail.IfOperator(
            task_id='is_fulltimesalaried_employeetype_present',
            test="{{ result('get_fulltimesalaried_employee_type') | is_truthy }}",
            yes_task="update_employeetypegroup_user",
            no_task="write_fulltimesalaried_exception",
        )

        update_employeetypegroup_user = rail.RepliconServiceOperator(
            task_id='update_employeetypegroup_user',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeTypeUri": "{{ result('get_fulltimesalaried_employee_type') }}"
            }
        )

        write_fulltimesalaried_exception = rail.WriteLogOperator(
            task_id='write_fulltimesalaried_exception',
            log="{{ dag_run.conf.log }}",
            message="Employee type for user \"{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}\" is not \
                updated as employee type \"Full-time Salaried\" is not available in Replicon",
            severity="Failed",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Failed",
                "reason": "Employee type for user \"{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}\" is \
                    not updated as employee type \"Full-time Salaried\" is not available in Replicon",
            }
        )

        is_employeetype_present2 = rail.IfOperator(
            task_id='is_employeetype_present2',
            test="{{ dag_run.conf.employeetype | is_truthy and dag_run.conf.employeetype == 'T' \
                and result('parse_csv_user_data')['Employee Type'] != 'Contractor' }}",
            yes_task="get_contractor_employee_type",
            no_task="is_supervisorid_present",
        )

        get_contractor_employee_type = rail.RepliconServiceOperator(
            task_id='get_contractor_employee_type',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Contractor', 'uri', '')
        )

        is_contractor_employeetype_present = rail.IfOperator(
            task_id='is_contractor_employeetype_present',
            test='''{{ result('get_contractor_employee_type') | is_truthy }}''',
            yes_task="update_employee_type_user_2",
            no_task="write_employeetype_exception",
        )

        update_employee_type_user_2 = rail.RepliconServiceOperator(
            task_id='update_employee_type_user_2',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "employeeTypeUri": "{{ result('get_contractor_employee_type') }}"
            }
        )

        write_employeetype_exception = rail.WriteLogOperator(
            task_id='write_employeetype_exception',
            log="{{ dag_run.conf.log }}",
            message="na",
            severity="Failed",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Failed",
                "reason": "Employee type for user \"{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}\" is \
                        not updated as employee type \"Contractor\" is not available in Replicon"
            }
        )

        is_supervisorid_present = rail.IfOperator(
            task_id='is_supervisorid_present',
            test="{{ dag_run.conf.supervisorid | is_truthy }}",
            yes_task="is_supervisorid_not_same_loginname",
            no_task="catch_error",
        )

        is_supervisorid_not_same_loginname = rail.IfOperator(
            task_id='is_supervisorid_not_same_loginname',
            test="{{ dag_run.conf.supervisorid != dag_run.conf.loginname }}",
            yes_task="get_userdata_supervisor",
            no_task="write_supervisor_loginname_exception",
        )

        get_userdata_supervisor = rail.RepliconServiceOperator(
            task_id='get_userdata_supervisor',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{ dag_run.conf.supervisorid }}"
                        }
                    }
                }
            },
            data_handler=get_supervisor_details
        )

        is_supervisor_present = rail.IfOperator(
            task_id='is_supervisor_present',
            test="{{ result('get_userdata_supervisor').uri | is_truthy }}",
            yes_task="is_supervisor_different",
            no_task="write_supervisor_not_exists_exception",
        )

        is_supervisor_different = rail.IfOperator(
            task_id='is_supervisor_different',
            test="{{ result('get_userdata_supervisor').name != \
                result('parse_csv_user_data')['User Supervisor Name (Current)'] }}",
            yes_task='get_missing_supervisor_permission',
            no_task='catch_error'
        )

        get_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                'userUri': "{{ result('get_userdata_supervisor').uri }}"
            },
            data_handler=is_assign_supervisorpermission
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_missing_supervisor_permission') | is_truthy }}",
            yes_task='add_missing_supervisor_permission',
            no_task='assign_supervisor'
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': "{{ result('get_userdata_supervisor').uri }}",
                'permissionSetUri': '{{ dag_run.conf.supervisorpermissionuri }}'
            }
        )

        assign_supervisor = rail.RepliconServiceOperator(
            task_id='assign_supervisor',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'supervisorUri': rail.result('get_userdata_supervisor')['uri'],
                'dateRange': {
                    'startDate': get_today_date()
                }
            }
        )

        write_supervisor_not_exists_exception = rail.WriteLogOperator(
            task_id='write_supervisor_not_exists_exception',
            log="{{ dag_run.conf.log }}",
            message="Supervisor is not updated for user \"{{ dag_run.conf.firstname }} \
                {{ dag_run.conf.lastname }}\" as the user with login name \"{{ dag_run.conf.supervisorid }}\" is \
                    not available in Replicon.",
            severity="Failed",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Failed",
                "reason": "Supervisor is not updated for user \"{{ dag_run.conf.firstname }} \
                {{ dag_run.conf.lastname }}\" as the user with login name \"{{ dag_run.conf.supervisorid }}\" is \
                    not available in Replicon."
            }
        )

        write_supervisor_loginname_exception = rail.WriteLogOperator(
            task_id='write_supervisor_loginname_exception',
            log="{{ dag_run.conf.log }}",
            message="Supervisor is not updated for user \"{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}\" as \
                    the \"Login name\" for user and supervisor is same on the input file",
            severity="Failed",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Failed",
                "reason": "Supervisor is not updated for user \"{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}\" as \
                    the \"Login name\" for user and supervisor is same on the input file"
            }
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed'
        )

        is_email_error = rail.IfOperator(
            task_id='is_email_error',
            test="{{ get_task_state('update_email') == 'failed' }}",
            yes_task='write_email_error',
            no_task='is_disableuser_terminationdate_error'
        )

        write_email_error = rail.WriteLogOperator(
            task_id='write_email_error',
            log="{{ dag_run.conf.log }}",
            message="Email address for \"{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}\" is not updated: \
                    {{ get_error_message() }}",
            severity="Error",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                "reason": "Email address for \"{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}\" is not updated: \
                    {{ get_error_message() }}"
            }
        )

        is_disableuser_terminationdate_error = rail.IfOperator(
            task_id='is_disableuser_terminationdate_error',
            test="{{ get_task_state('update_employment_daterange') == 'failed' or \
                get_task_state('disable_login') == 'failed' }}",
            yes_task='write_disableuser_terminationdate_error',
            no_task='write_generic_log_error'
        )

        write_disableuser_terminationdate_error = rail.WriteLogOperator(
            task_id='write_disableuser_terminationdate_error',
            log="{{ dag_run.conf.log }}",
            message="Disable user and Termination date for \"{{ dag_run.conf.firstname }}\" \"{{ dag_run.conf.lastname }}\" is \
                    not updated: {{ get_error_message() }}",
            severity="Error",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                "reason": "Disable user and Termination date for \"{{ dag_run.conf.firstname }}\" \"{{ dag_run.conf.lastname }}\" is \
                    not updated: {{ get_error_message() }}"
            }
        )

        write_generic_log_error = rail.WriteLogOperator(
            task_id='write_generic_log_error',
            log="{{ dag_run.conf.log }}",
            message="All fields for user \"{{ dag_run.conf.firstname }}\" \"{{ dag_run.conf.lastname }}\" is not \
                updated: {{ get_error_message() }}",
            severity="Error",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                "reason": "All fields for user \"{{ dag_run.conf.firstname }}\" \"{{ dag_run.conf.lastname }}\" is not \
                    updated: {{ get_error_message() }}"
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label(
            'No') >> get_user_report
        get_user_report >> parse_csv >> parse_csv_user_data >> is_firstname_present
        is_firstname_present >> rail.Label(
            'Yes') >> update_firstname >> is_lastname_present
        is_firstname_present >> rail.Label(
            'No') >> is_lastname_present
        is_lastname_present >> rail.Label(
            'Yes') >> update_lastname >> is_employeeid_present
        is_lastname_present >> rail.Label(
            'No') >> is_employeeid_present
        is_employeeid_present >> rail.Label(
            'Yes') >> update_employeeid >> is_email_present
        is_employeeid_present >> rail.Label(
            'No') >> is_email_present
        is_email_present >> rail.Label(
            'Yes') >> update_email >> is_loginstatus_terminated
        is_email_present >> rail.Label(
            'No') >> is_loginstatus_terminated
        is_loginstatus_terminated >> rail.Label(
            'Yes') >> is_enddate_present
        is_enddate_present >> rail.Label(
            'Yes') >> update_employment_daterange >> disable_login >> \
            is_loginstatus_present
        is_enddate_present >> rail.Label(
            'No') >> is_loginstatus_present
        is_loginstatus_terminated >> rail.Label(
            'No') >> is_loginstatus_present
        is_loginstatus_present >> rail.Label(
            'Yes') >> is_loginstatus_equals_active
        is_loginstatus_equals_active >> rail.Label(
            'Yes') >> enable_login >> get_usercustom_field_uri
        is_loginstatus_equals_active >> rail.Label(
            'No') >> get_usercustom_field_uri
        is_loginstatus_present >> rail.Label(
            'No') >> get_usercustom_field_uri
        get_usercustom_field_uri >> get_required_user_customfields >> is_costcenter_udf_update
        is_costcenter_udf_update >> rail.Label(
            'Yes') >> update_costcenter_udf >> is_costcentername_udf_update
        is_costcenter_udf_update >> rail.Label(
            'No') >> is_costcentername_udf_update
        is_costcentername_udf_update >> rail.Label(
            'Yes') >> update_costcentername_udf >> is_employeetype_present
        is_costcentername_udf_update >> rail.Label(
            'No') >> is_employeetype_present
        is_employeetype_present >> rail.Label(
            'Yes') >> get_fulltimesalaried_employee_type >> is_fulltimesalaried_employeetype_present
        is_fulltimesalaried_employeetype_present >> rail.Label(
            'Yes') >> update_employeetypegroup_user >> is_employeetype_present2
        is_fulltimesalaried_employeetype_present >> rail.Label(
            'No') >> write_fulltimesalaried_exception >> is_employeetype_present2
        is_employeetype_present >> rail.Label(
            'No') >> is_employeetype_present2
        is_employeetype_present2 >> rail.Label(
            'Yes') >> get_contractor_employee_type >> is_contractor_employeetype_present
        is_contractor_employeetype_present >> rail.Label(
            'Yes') >> update_employee_type_user_2 >> is_supervisorid_present
        is_contractor_employeetype_present >> rail.Label(
            'No') >> write_employeetype_exception >> is_supervisorid_present
        is_employeetype_present2 >> rail.Label(
            'No') >> is_supervisorid_present
        is_supervisorid_present >> rail.Label(
            'Yes') >> is_supervisorid_not_same_loginname
        is_supervisorid_not_same_loginname >> rail.Label(
            'Yes') >> get_userdata_supervisor >> is_supervisor_present
        is_supervisor_present >> rail.Label(
            'Yes') >> is_supervisor_different
        is_supervisor_different >> rail.Label(
            'Yes') >> get_missing_supervisor_permission >> should_add_missing_permissions
        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permission >> assign_supervisor
        should_add_missing_permissions >> rail.Label(
            'No') >> assign_supervisor
        assign_supervisor >> rail.Label(
            'On Error') >> catch_error
        is_supervisor_different >> rail.Label(
            'On Error') >> catch_error
        is_supervisor_present >> rail.Label(
            'No') >> write_supervisor_not_exists_exception >> rail.Label(
                'On Error') >> catch_error
        is_supervisorid_not_same_loginname >> rail.Label(
            'No') >> write_supervisor_loginname_exception >> rail.Label(
                'On Error') >> catch_error
        is_supervisorid_present >> rail.Label(
            'On Error') >> catch_error

        catch_error >> is_email_error

        is_email_error >> rail.Label(
            'Yes') >> write_email_error >> dagrun_log_to_sumo
        is_email_error >> rail.Label(
            'No') >> is_disableuser_terminationdate_error
        is_disableuser_terminationdate_error >> rail.Label(
            'Yes') >> write_disableuser_terminationdate_error >> dagrun_log_to_sumo
        is_disableuser_terminationdate_error >> rail.Label(
            'No') >> write_generic_log_error >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_updateuser_dag)
