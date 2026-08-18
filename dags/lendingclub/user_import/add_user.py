from datetime import timedelta
from airflow.models import Variable
import rail
from lendingclub.user_import.utils import python_callable, request_payload

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'lendingclub_user_import_add_user_child_{config.instance}',
        description=f'lendingclub_user_import_add_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.add_user_child_dag_active_runs,
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
            no_task='exception_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='exception_log',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        exception_log = rail.CreateLogOperator(
            task_id = "exception_log"
        )

        failure_list = rail.PythonOperator(
            task_id = "failure_list",
            python_callable=python_callable.get_failure_list
        )

        is_failure_list_present = rail.IfOperator(
            task_id='is_failure_list_present',
            test="{{ result('failure_list').error_status | is_truthy }}",
            yes_task="log_user_with_failure_reasons",
            no_task="is_employeestatus_disabled",
        )

        log_user_with_failure_reasons = rail.WriteLogOperator(
            task_id="log_user_with_failure_reasons",
            log = '{{ dag_run.conf.logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda item, dag_run: {
                "UserID": dag_run.conf['loginname'] + "|" + dag_run.conf['empid'],
                "Action": "Adduser",
                "Status": "Exception",
                'Details': "User not created," + rail.result('failure_list')['error_value'],
            }
        )

        is_employeestatus_disabled = rail.IfOperator(
            task_id='is_employeestatus_disabled',
            test="{{ dag_run.conf.employeestatus.lower() == 'disabled' }}",
            yes_task="log_user_already_disabled",
            no_task="is_hiredate_invalid",
        )

        log_user_already_disabled = rail.WriteLogOperator(
            task_id="log_user_already_disabled",
            log = '{{ dag_run.conf.logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda item, dag_run: {
                "UserID": dag_run.conf['loginname'] + "|" + dag_run.conf['empid'],
                "Action": "Adduser",
                "Status": "Exception",
                'Details': "User not created, user status recevied as Disabled",
            }
        )

        is_hiredate_invalid = rail.IfOperator(
            task_id='is_hiredate_invalid',
            test=lambda dag_run: bool('-' not in dag_run.conf['hiredate']),
            yes_task="log_invalid_hiredate_format",
            no_task="get_user_details",
        )

        log_invalid_hiredate_format = rail.WriteLogOperator(
            task_id="log_invalid_hiredate_format",
            log = '{{ dag_run.conf.logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda item, dag_run: {
                "UserID": dag_run.conf['loginname'] + "|" + dag_run.conf['empid'],
                "Action": "Adduser",
                "Status": "Exception",
                'Details': "User not created, invalid date format",
            }
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data = {
                "users": [
                    {
                        "loginName": "{{ dag_run.conf.loginname }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_user_details_present = rail.IfOperator(
            task_id='if_user_details_present',
            test="{{ result('get_user_details') | is_truthy }}",
            yes_task="log_loginname_already_present",
            no_task="get_primary_department",
        )

        log_loginname_already_present = rail.WriteLogOperator(
            task_id="log_loginname_already_present",
            log = '{{ dag_run.conf.logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda item, dag_run: {
                "UserID": dag_run.conf['loginname'] + "|" + dag_run.conf['empid'],
                "Action": "Adduser",
                "Status": "Exception",
                'Details': "Loginname " + dag_run.conf['loginname'] + "  already present in Replicon"
            }
        )

        get_primary_department = rail.RepliconServiceOperator(
            task_id='get_primary_department',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
            data_handler=lambda response, dag_run:{
                "all_scrum_team" : rail.find_first_by_attr_and_get_attr(response, 'displayText', 'All scrum team', 'uri', ''),
                "scrum": rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf['Scrum'], 'uri', '')
            }
        )

        is_primary_department_absent = rail.IfOperator(
            task_id='is_primary_department_absent',
            test="{{ result('get_primary_department').all_scrum_team | is_falsy }}",
            yes_task="log_primary_department_not_present",
            no_task="get_employee_type_details",
        )

        log_primary_department_not_present = rail.WriteLogOperator(
            task_id="log_primary_department_not_present",
            log = '{{ dag_run.conf.logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda item, dag_run: {
                "UserID": dag_run.conf['loginname'] + "|" + dag_run.conf['empid'],
                "Action": "Adduser",
                "Status": "Exception",
                'Details': "Primary Department not available in Replicon. Department is mandatory"
            }
        )

        get_employee_type_details = rail.RepliconServiceOperator(
            task_id='get_employee_type_details',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf['employeetypename'], 'uri', '')
        )

        is_employeetype_present = rail.IfOperator(
            task_id='is_employeetype_present',
            test="{{ result('get_employee_type_details') | is_falsy }}",
            yes_task="log_invalid_employeetype",
            no_task="add_user",
        )

        log_invalid_employeetype = rail.WriteLogOperator(
            task_id="log_invalid_employeetype",
            log = '{{ dag_run.conf.logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda item, dag_run: {
                "UserID": dag_run.conf['loginname'] + "|" + dag_run.conf['empid'],
                "Action": "Adduser",
                "Status": "Exception",
                'Details': "User not created, invalid Employee type",
            }
        )

        add_user = rail.RepliconServiceOperator(
            task_id='add_user',
            endpoint="/services/importservice1.svc/PutUser3",
            data = request_payload.add_user_payload
        )

        if_employee_status_is_on_leave = rail.IfOperator(
            task_id='if_employee_status_is_on_leave',
            test="{{ dag_run.conf.employeestatus.lower() == 'on leave' }}",
            yes_task="disable_user",
            no_task="update_customfield_jobfield",
        )

        disable_user = rail.RepliconServiceOperator(
            task_id='disable_user',
            endpoint="/services/securityService1.svc/DisableLogin",
            data = {
                "userUri": "{{ result('add_user').uri }}"
                }
        )

        update_customfield_jobfield = rail.RepliconServiceOperator(
            task_id='update_customfield_jobfield',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data = {
                "objectUri": "{{ result('add_user').uri }}",
                "customFieldUri": "{{ dag_run.conf.joblevel_uri }}",
                "value": "{{ dag_run.conf.joblevel }}"
            }
        )

        if_vendoruri_present_and_employeetype_is_contrcator = rail.IfOperator(
            task_id='if_vendoruri_present_and_employeetype_is_contrcator',
            test="{{ dag_run.conf.vendor_uri | is_truthy and dag_run.conf.employeetypename.lower() == 'contractors' }}",
            yes_task="get_enabled_customfielddropdown_options",
            no_task="if_managerid_present_and_not_equal_empid",
        )

        get_enabled_customfielddropdown_options = rail.RepliconServiceOperator(
            task_id='get_enabled_customfielddropdown_options',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data = {
                "customFieldUri": "{{ dag_run.conf.vendor_uri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['vendor'], 'uri', '')
        )

        if_required_vendor_uri_present = rail.IfOperator(
            task_id='if_required_vendor_uri_present',
            test="{{ result('get_enabled_customfielddropdown_options') | is_truthy }}",
            yes_task="update_vendor_dropdown_value",
            no_task="log_vendor_not_assigned",
        )

        update_vendor_dropdown_value = rail.RepliconServiceOperator(
            task_id='update_vendor_dropdown_value',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data = {
                "objectUri": "{{ result('add_user').uri }}",
                "customFieldUri": "{{ dag_run.conf.vendor_uri }}",
                "customFieldDropDownOptionUri": "{{ result('get_enabled_customfielddropdown_options') }}"
            }
        )

        log_vendor_not_assigned = rail.WriteLogOperator(
            task_id='log_vendor_not_assigned',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Vendors field not updated, no vendor option found for {{ dag_run.conf.vendor }} in Replicon."
            }
        )

        if_managerid_present_and_not_equal_empid = rail.IfOperator(
            task_id='if_managerid_present_and_not_equal_empid',
            test="{{ dag_run.conf.managerid | is_truthy and dag_run.conf.managerid != dag_run.conf.empid }}",
            yes_task="search_for_user_with_empid",
            no_task="if_locationcode_or_location_is_present",
        )

        search_for_user_with_empid = rail.RepliconServiceOperator(
            task_id='search_for_user_with_empid',
            endpoint="/services/UserListService1.svc/GetData",
            data = {
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:login-name"
                ]
            },
            data_handler=python_callable.get_userdata_list_for_managerid
        )

        check_if_single_manageruseruri_present = rail.IfOperator(
            task_id='check_if_single_manageruseruri_present',
            test=lambda: bool(len(rail.result('search_for_user_with_empid')) == 1 ),
            yes_task="get_manager_details",
            no_task="check_if_manageruri_absent",
        )

        get_manager_details = rail.RepliconServiceOperator(
            task_id='get_manager_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data = {
                "users": [
                    {
                        "uri": "{{ result('search_for_user_with_empid')[0]['uri'] }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_manager_details_present_and_enabled = rail.IfOperator(
            task_id='if_manager_details_present_and_enabled',
            test="{{ result('get_manager_details') | is_truthy and result('get_manager_details')[0]['userDetails']['isEnabled'] | is_truthy}}",
            yes_task="get_assigned_permissionset_foruser",
            no_task="log_supervisor_assignment",
        )

        get_assigned_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionset_foruser',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data = {
                "userUri": "{{ result('search_for_user_with_empid')[0]['uri'] }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.uri', '')
        )

        if_supervisor_permission_assigned = rail.IfOperator(
            task_id='if_supervisor_permission_assigned',
            test="{{ result('get_assigned_permissionset_foruser') | is_truthy }}",
            yes_task="put_supervisor_assignment_schedule",
            no_task="log_supervisor_assignment",
        )

        put_supervisor_assignment_schedule = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment_schedule',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data = {
                "userUri": "{{ result('add_user').uri }}",
                "initialSupervisorUri": "{{ result('search_for_user_with_empid')[0]['uri'] }}"
            }
        )

        check_if_manageruri_absent = rail.IfOperator(
            task_id='check_if_manageruri_absent',
            test=lambda: bool(len(rail.result('search_for_user_with_empid')) == 0 ),
            yes_task="log_supervisor_assignment",
            no_task="if_locationcode_or_location_is_present",
        )

        log_supervisor_assignment = rail.WriteLogOperator(
            task_id="log_supervisor_assignment",
            log = '{{ dag_run.conf.supervisor_logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda item, dag_run: {
                "loginid": dag_run.conf['loginname'],
                "empid": dag_run.conf['empid'],
                "managerid": dag_run.conf['managerid'],
                "useruri": "{{ result('add_user').uri }}",
                'type': "add",
            }
        )

        if_locationcode_or_location_is_present = rail.IfOperator(
            task_id='if_locationcode_or_location_is_present',
            test="{{ dag_run.conf.locationcode | is_truthy or dag_run.conf.location | is_truthy }}",
            yes_task="search_location_by_codeandname",
            no_task="if_deptcode_or_deptname_is_present",
        )

        search_location_by_codeandname = rail.RepliconServiceOperator(
            task_id='search_location_by_codeandname',
            endpoint="/services/LocationListService1.svc/GetData",
            data = request_payload.search_location_filter,
            data_handler = python_callable.get_uri_value
        )

        if_location_uri_present = rail.IfOperator(
            task_id='if_location_uri_present',
            test="{{ result('search_location_by_codeandname') | is_truthy }}",
            yes_task="put_locationschedule_for_user",
            no_task="log_location_not_assigned",
        )

        log_location_not_assigned = rail.WriteLogOperator(
            task_id='log_location_not_assigned',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Location not assigned, no location found for {{ dag_run.conf.location }} in Replicon."
            }
        )

        put_locationschedule_for_user = rail.RepliconServiceOperator(
            task_id='put_locationschedule_for_user',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data = {
                "userUri": "{{ result('add_user').uri }}",
                "scheduleEntries": [
                    {
                        "location": {
                        "uri": "{{ result('search_location_by_codeandname') }}"
                        }
                    }
                ]
            }
        )

        if_deptcode_or_deptname_is_present = rail.IfOperator(
            task_id='if_deptcode_or_deptname_is_present',
            test="{{ dag_run.conf.departmentcode | is_truthy or dag_run.conf.department | is_truthy }}",
            yes_task="search_department_by_codeandname",
            no_task="if_permission_is_present",
        )

        search_department_by_codeandname = rail.RepliconServiceOperator(
            task_id='search_department_by_codeandname',
            endpoint="/services/DivisionListService1.svc/GetData",
            data = request_payload.search_department_filter,
            data_handler = python_callable.get_uri_value
        )

        if_dept_uri_present = rail.IfOperator(
            task_id='if_dept_uri_present',
            test="{{ result('search_department_by_codeandname') | is_truthy }}",
            yes_task="put_divisionschedule_for_user",
            no_task="log_division_not_assigned",
        )

        put_divisionschedule_for_user = rail.RepliconServiceOperator(
            task_id='put_divisionschedule_for_user',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data = {
                "userUri": "{{ result('add_user').uri }}",
                "scheduleEntries": [
                    {
                        "division": {
                        "uri": "{{ result('search_department_by_codeandname') }}"
                        }
                    }
                ]
            }
        )

        log_division_not_assigned = rail.WriteLogOperator(
            task_id='log_division_not_assigned',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Division (department) not assigned, no location found for  {{ dag_run.conf.department }} in Replicon."
            }
        )

        if_permission_is_present = rail.IfOperator(
            task_id='if_permission_is_present',
            test="{{ dag_run.conf.permission | is_truthy }}",
            yes_task="if_permission_is_resource_or_supervisor",
            no_task="if_scrum_is_present",
        )

        if_permission_is_resource_or_supervisor = rail.IfOperator(
            task_id='if_permission_is_resource_or_supervisor',
            test="{{ dag_run.conf.permission.lower() == 'resource'  or dag_run.conf.permission.lower() == 'supervisor' }}",
            yes_task="get_enabled_timeofftypes",
            no_task="if_scrum_is_present",
        )

        get_enabled_timeofftypes = rail.RepliconServiceOperator(
            task_id='get_enabled_timeofftypes',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=python_callable.get_timeofftypeuris
        )

        put_timeoffassignment_for_user = rail.RepliconServiceOperator(
            task_id='put_timeoffassignment_for_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data = lambda:{
                "userUri": rail.result('add_user')['uri'],
                "timeOffTypeUris": rail.result('get_enabled_timeofftypes')
            }
        )

        if_scrum_is_present = rail.IfOperator(
            task_id='if_scrum_is_present',
            test="{{ dag_run.conf.Scrum | is_truthy }}",
            yes_task="is_scrumdept_present",
            no_task="write_log_user_import",
        )

        is_scrumdept_present = rail.IfOperator(
            task_id='is_scrumdept_present',
            test="{{ result('get_primary_department').scrum | is_truthy }}",
            yes_task="update_dept_for_user",
            no_task="create_newdraft_dept",
        )

        update_dept_for_user = rail.RepliconServiceOperator(
            task_id='update_dept_for_user',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data = {
                "userUri": "{{ result('add_user').uri }}",
                "departmentUri": "{{ result('get_primary_department').scrum }}"
            }
        )

        create_newdraft_dept = rail.RepliconServiceOperator(
            task_id='create_newdraft_dept',
            endpoint="/services/DepartmentService1.svc/CreateNewDraft",
            data = {
                "parentDepartmentUri": "{{ result('get_primary_department').all_scrum_team }}"
            }
        )

        update_name_dept = rail.RepliconServiceOperator(
            task_id='update_name_dept',
            endpoint="/services/DepartmentService1.svc/UpdateName",
            data = {
                "departmentUri": "{{ result('create_newdraft_dept') }}",
                "name": "{{ dag_run.conf.Scrum }}"
            }
        )

        publish_draft_dept = rail.RepliconServiceOperator(
            task_id='publish_draft_dept',
            endpoint="/services/DepartmentService1.svc/PublishDraft",
            data = {
                "draftUri": "{{ result('create_newdraft_dept') }}"
            }
        )

        update_department_for_user = rail.RepliconServiceOperator(
            task_id='update_department_for_user',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data = {
                "userUri": "{{ result('add_user').uri }}",
                "departmentUri": "{{ result('publish_draft_dept') }}"
            }
        )

        write_log_user_import = rail.WriteCSVFileOperator(
            task_id='write_log_user_import',
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
            properties=python_callable.get_status_and_details_for_add
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "UserID": "{{ dag_run.conf.loginname }}" + "|" + "{{ dag_run.conf.empid }}",
                "Action": "Adduser",
                "Status": "Error",
                "Details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> exception_log

        exception_log >> failure_list >> is_failure_list_present
        is_failure_list_present >> rail.Label('Yes') >> log_user_with_failure_reasons >> catch_and_log_error
        is_failure_list_present >> rail.Label('No') >> is_employeestatus_disabled

        is_employeestatus_disabled >> rail.Label('Yes') >> log_user_already_disabled >> catch_and_log_error
        is_employeestatus_disabled >> rail.Label('No') >> is_hiredate_invalid

        is_hiredate_invalid >> rail.Label('Yes') >> log_invalid_hiredate_format >> catch_and_log_error
        is_hiredate_invalid >> rail.Label('No') >> get_user_details >> if_user_details_present

        if_user_details_present >> rail.Label('Yes') >> log_loginname_already_present >> catch_and_log_error
        if_user_details_present >> rail.Label('No') >> get_primary_department >> is_primary_department_absent

        is_primary_department_absent >> rail.Label('Yes') >> log_primary_department_not_present >> catch_and_log_error
        is_primary_department_absent >> rail.Label('No') >> get_employee_type_details >> is_employeetype_present

        is_employeetype_present >> rail.Label('Yes') >> log_invalid_employeetype >> catch_and_log_error
        is_employeetype_present >> rail.Label('No') >> add_user

        add_user >> if_employee_status_is_on_leave
        if_employee_status_is_on_leave >> rail.Label('Yes') >> disable_user >> update_customfield_jobfield >> if_vendoruri_present_and_employeetype_is_contrcator
        if_employee_status_is_on_leave >> rail.Label('No') >> update_customfield_jobfield >> if_vendoruri_present_and_employeetype_is_contrcator

        if_vendoruri_present_and_employeetype_is_contrcator >> rail.Label('Yes') >> get_enabled_customfielddropdown_options >> if_required_vendor_uri_present

        if_required_vendor_uri_present >> rail.Label('Yes') >> update_vendor_dropdown_value >> if_managerid_present_and_not_equal_empid
        if_required_vendor_uri_present >> rail.Label('No') >> log_vendor_not_assigned >> if_managerid_present_and_not_equal_empid

        if_vendoruri_present_and_employeetype_is_contrcator >> rail.Label('No') >> if_managerid_present_and_not_equal_empid

        if_managerid_present_and_not_equal_empid >> rail.Label('Yes') >> search_for_user_with_empid >> check_if_single_manageruseruri_present

        check_if_single_manageruseruri_present >> rail.Label('Yes') >> get_manager_details >> if_manager_details_present_and_enabled

        if_manager_details_present_and_enabled >> rail.Label('Yes') >> get_assigned_permissionset_foruser >> if_supervisor_permission_assigned

        if_supervisor_permission_assigned >> rail.Label('Yes') >> put_supervisor_assignment_schedule >> if_locationcode_or_location_is_present
        if_supervisor_permission_assigned >> rail.Label('No') >> log_supervisor_assignment >> if_locationcode_or_location_is_present

        if_manager_details_present_and_enabled >> rail.Label('No') >> log_supervisor_assignment >> if_locationcode_or_location_is_present

        check_if_single_manageruseruri_present >> rail.Label('No') >> check_if_manageruri_absent

        check_if_manageruri_absent >> rail.Label('Yes') >> log_supervisor_assignment >> if_locationcode_or_location_is_present
        check_if_manageruri_absent >> rail.Label('No') >> if_locationcode_or_location_is_present

        if_managerid_present_and_not_equal_empid >> rail.Label('No') >> if_locationcode_or_location_is_present

        if_locationcode_or_location_is_present >> rail.Label('Yes') >> search_location_by_codeandname >> if_location_uri_present

        if_location_uri_present >> rail.Label('Yes') >> put_locationschedule_for_user >> if_deptcode_or_deptname_is_present
        if_location_uri_present >> rail.Label('No') >> log_location_not_assigned >> if_deptcode_or_deptname_is_present
        if_locationcode_or_location_is_present >> rail.Label('No') >> if_deptcode_or_deptname_is_present

        if_deptcode_or_deptname_is_present >> rail.Label('Yes') >> search_department_by_codeandname >> if_dept_uri_present

        if_dept_uri_present >> rail.Label('Yes') >> put_divisionschedule_for_user >> if_permission_is_present
        if_dept_uri_present >> rail.Label('No') >> log_division_not_assigned >> if_permission_is_present

        if_deptcode_or_deptname_is_present >> rail.Label('No') >> if_permission_is_present

        if_permission_is_present >> rail.Label('Yes') >> if_permission_is_resource_or_supervisor
        if_permission_is_present >> rail.Label('No') >> if_scrum_is_present

        if_permission_is_resource_or_supervisor >> rail.Label('Yes') >> get_enabled_timeofftypes >> \
            put_timeoffassignment_for_user >> if_scrum_is_present
        if_permission_is_resource_or_supervisor >> rail.Label('No') >> if_scrum_is_present

        if_scrum_is_present >> rail.Label('Yes') >> is_scrumdept_present

        is_scrumdept_present >> rail.Label('Yes') >> update_dept_for_user >> write_log_user_import
        is_scrumdept_present >> rail.Label('No') >> create_newdraft_dept >> update_name_dept >> publish_draft_dept >> \
        update_department_for_user >> write_log_user_import

        if_scrum_is_present >> rail.Label('No') >> write_log_user_import

        write_log_user_import >> log_user_import >> catch_and_log_error



        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
