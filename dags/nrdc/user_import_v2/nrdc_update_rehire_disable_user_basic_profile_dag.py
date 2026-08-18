
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.nrdc_updaterehiredisableuserbasicprofile,
        description=f'Live|NRDC Update / Rehire / Disable User (basic profile) {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
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
            no_task='get_custom_fieldsforuser_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_custom_fieldsforuser_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_custom_fieldsforuser_3 = rail.RepliconServiceOperator(
            task_id='get_custom_fieldsforuser_3',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data=lambda: {
                "objectUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user:1"
            }
        )

        def get_customoef_uri(custom_field_info):
            existing_customoefs = rail.result('get_custom_fieldsforuser_3')
            input_department_info = list(filter(
                lambda item: item['displayText'] == custom_field_info, existing_customoefs))
            return input_department_info[0]['uri'] if input_department_info else None

        log_employeenumber_u_d_f_4 = rail.PythonOperator(
            task_id='log_employeenumber_u_d_f_4',
            python_callable=lambda: get_customoef_uri("Employee Number")
        )

        log_user_name_u_d_f_5 = rail.PythonOperator(
            task_id='log_user_name_u_d_f_5',
            python_callable=lambda:  get_customoef_uri("User-Name")
        )

        log_t_y_p_e_uri_6 = rail.PythonOperator(
            task_id='log_t_y_p_e_uri_6',
            python_callable=lambda:  get_customoef_uri("Type")
        )

        log_title_uri_7 = rail.PythonOperator(
            task_id='log_title_uri_7',
            python_callable=lambda:  get_customoef_uri("Title")
        )

        log_todays_year_11 = rail.PythonOperator(
            task_id='log_todays_year_11',
            python_callable=lambda:  datetime.now(timezone.utc).year
        )

        log_todays_month_12 = rail.PythonOperator(
            task_id='log_todays_month_12',
            python_callable=lambda:  datetime.now(timezone.utc).month
        )

        log_todays_day_13 = rail.PythonOperator(
            task_id='log_todays_day_13',
            python_callable=lambda:  datetime.now(timezone.utc).day
        )

        get_user_details_userreferencedetailsreport_14 = rail.RepliconServiceOperator(
            task_id='get_user_details_userreferencedetailsreport_14',
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
            },
            data_handler=lambda response: response[0] if response else None
        )

        def get_user_custom_info(task_info):
            user_custom_details = []

            user_custom_info = rail.result(task_info)[
                'userDetails']['customFieldValues']
            for user_custom in user_custom_info:
                user_custom_details.append(
                    {
                        "name": user_custom['customField']['displayText'],
                        "uri": user_custom['customField']['uri'],
                        "value": user_custom['text']
                    }
                )
            return user_custom_details

        log_customfieldsdata_15 = rail.PythonOperator(
            task_id='log_customfieldsdata_15',
            python_callable=lambda:  get_user_custom_info(
                'get_user_details_userreferencedetailsreport_14')
        )

        if_userdetails_lastname_not_equals_to_dataworkato_service3cd9c331requestlastname_19 = rail.IfOperator(
            task_id='if_userdetails_lastname_not_equals_to_dataworkato_service3cd9c331requestlastname_19',
            test=lambda dag_run: rail.result('get_user_details_userreferencedetailsreport_14')[
                'userDetails']['lastName'] != dag_run.conf['lastname'],
            yes_task="update_last_name_20",
            no_task="if_request_employeeid_present_22",
        )

        update_last_name_20 = rail.RepliconServiceOperator(
            task_id='update_last_name_20',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "timesheetPeriodScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": "{{ dag_run.conf.lastname }}",
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": null,
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        log_forlookuplogs_21 = rail.PythonOperator(
            task_id='log_forlookuplogs_21',
            python_callable=lambda dag_run:  "Lastname updated to" +
            dag_run.conf['lastname']
        )

        if_request_employeeid_present_22 = rail.IfOperator(
            task_id='if_request_employeeid_present_22',
            test='''{{ dag_run.conf.employeeid | is_truthy  and dag_run.conf.employeeid != '-' }}''',
            yes_task="if_userdetails_employeeid_not_equals_to_dataworkato_service3cd9c331requestemployeeid_23",
            no_task="if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestempnumber_26",
        )

        if_userdetails_employeeid_not_equals_to_dataworkato_service3cd9c331requestemployeeid_23 = rail.IfOperator(
            task_id='if_userdetails_employeeid_not_equals_to_dataworkato_service3cd9c331requestemployeeid_23',
            test='''{{ result('get_user_details_userreferencedetailsreport_14').userDetails.employeeId != dag_run.conf.employeeid }}''',
            yes_task="updateemployee_id_24",
            no_task="if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestempnumber_26",
        )

        updateemployee_id_24 = rail.RepliconServiceOperator(
            task_id='updateemployee_id_24',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "timesheetPeriodScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": null,
                        "employeeId": {
                            "employeeId": "{{ dag_run.conf.employeeid }}"
                        }
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        log_forlookuplogs_25 = rail.PythonOperator(
            task_id='log_forlookuplogs_25',
            python_callable=lambda dag_run:  'EmployeeId updated to' +
            dag_run.conf['employeeid']
        )

        def is_employee_data_matching(custom_value, custom_filed_info):
            custom_info = rail.find_first_by_attr_and_get_attr(rail.result(
                'log_customfieldsdata_15'), 'name', custom_filed_info, 'value')
            return bool(custom_info == custom_value)

        if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestempnumber_26 = rail.IfOperator(
            task_id='if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestempnumber_26',
            test=lambda dag_run: is_employee_data_matching(
                dag_run.conf['empnumber'], "Employee Number"),
            yes_task="updateemployee_number_u_d_f_27",
            no_task="if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestuserfullname_29",
        )

        updateemployee_number_u_d_f_27 = rail.RepliconServiceOperator(
            task_id='updateemployee_number_u_d_f_27',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_employeenumber_u_d_f_4') }}",
                "value": "{{ dag_run.conf.empnumber }}"
            }
        )

        log_forlookuplogs_28 = rail.PythonOperator(
            task_id='log_forlookuplogs_28',
            python_callable=lambda dag_run:  'Employee number updated ' +
            dag_run.conf['empnumber']
        )

        if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestuserfullname_29 = rail.IfOperator(
            task_id='if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestuserfullname_29',
            # pylint: disable=line-too-long
            test=lambda dag_run: is_employee_data_matching(
                dag_run.conf['userfullname'], "User-Name"),
            yes_task="updateusername_u_d_f_30",
            no_task="if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requesttitle_32",
        )

        updateusername_u_d_f_30 = rail.RepliconServiceOperator(
            task_id='updateusername_u_d_f_30',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_user_name_u_d_f_5') }}",
                "value": "{{ dag_run.conf.userfullname }}"
            }
        )

        log_forlookuplogs_31 = rail.PythonOperator(
            task_id='log_forlookuplogs_31',
            python_callable=lambda dag_run:  'User-name UFD updated to ' +
            dag_run.conf['userfullname']
        )

        if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requesttitle_32 = rail.IfOperator(
            task_id='if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requesttitle_32',
            test=lambda dag_run: is_employee_data_matching(
                dag_run.conf['title'], "Title"),
            yes_task="update_title_u_d_f_33",
            no_task="if_request_department_present_35",
        )

        update_title_u_d_f_33 = rail.RepliconServiceOperator(
            task_id='update_title_u_d_f_33',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_title_uri_7') }}",
                "value": "{{ dag_run.conf.title }}"
            }
        )

        log_forlookuplogs_34 = rail.PythonOperator(
            task_id='log_forlookuplogs_34',
            python_callable=lambda dag_run:  'Title UFD updated to ' +
            dag_run.conf['title']
        )

        if_request_department_present_35 = rail.IfOperator(
            task_id='if_request_department_present_35',
            test='''{{ dag_run.conf.department | is_truthy }}''',
            yes_task="if_department_name_not_equals_to_dataworkato_service3cd9c331requestdepartment_36",
            no_task="if_request_department_blank_42",
        )

        if_department_name_not_equals_to_dataworkato_service3cd9c331requestdepartment_36 = rail.IfOperator(
            task_id='if_department_name_not_equals_to_dataworkato_service3cd9c331requestdepartment_36',
            test='''{{ result('get_user_details_userreferencedetailsreport_14').userDetails.department.name != dag_run.conf.department }}''',
            yes_task="get_enableddepartments_37",
            no_task="if_request_department_blank_42",
        )

        get_enableddepartments_37 = rail.RepliconServiceOperator(
            task_id='get_enableddepartments_37',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",

        )

        log_departmenturi_38 = rail.PythonOperator(
            task_id='log_departmenturi_38',
            python_callable=lambda dag_run:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_enableddepartments_37'), 'displayText', dag_run.conf['department'], 'uri')
        )

        if_log_departmenturi_38_present_39 = rail.IfOperator(
            task_id='if_log_departmenturi_38_present_39',
            test='''{{ result('log_departmenturi_38') | is_truthy }}''',
            yes_task="update_department_40",
            no_task="if_request_department_blank_42",
        )

        update_department_40 = rail.RepliconServiceOperator(
            task_id='update_department_40',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "timesheetPeriodScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": {
                        "uri": "{{ result('log_departmenturi_38') }}",
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "employeeTypeToApply": null,
                    "userDetailsToApply": null,
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        log_forlookuplogs_41 = rail.PythonOperator(
            task_id='log_forlookuplogs_41',
            python_callable=lambda dag_run:  'Department updated to' +
            dag_run.conf['department']
        )

        if_request_department_blank_42 = rail.IfOperator(
            task_id='if_request_department_blank_42',
            test='''{{ dag_run.conf.department | is_falsy }}''',
            yes_task="nrdc_user_import_logs_add_entry_43",
            no_task="get_all_permission_sets_44",
        )

        nrdc_user_import_logs_add_entry_43 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_43',
            message="Department must be valid",
            severity="Error",
            properties={
                "user": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname | {{ dag_run.conf.emailaddress }}",
                "action": "Update  User",
                "status": "Failed",
                "details": "Department must be valid",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        get_all_permission_sets_44 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_44',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",

        )

        declare_list_45 = rail.SetVariableOperator(
            task_id='declare_list_45',
            append=False,
            name='Permissionset',
            value=[]
        )

        get_assigned_permission_sets_for_user2_46 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_46',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        def get_permission_set(permission_name, task_name):
            permission_sets = rail.result(task_name)
            permission_uri = rail.find_first_by_attr_and_get_attr(
                permission_sets, 'displayText', permission_name, 'uri')
            return permission_uri

        log_ifenduserpermissionwasassigned_47 = rail.PythonOperator(
            task_id='log_ifenduserpermissionwasassigned_47',
            python_callable=lambda: get_permission_set(
                "End User", 'get_assigned_permission_sets_for_user2_46')
        )

        if_log_ifenduserpermissionwasassigned_47_blank_48 = rail.IfOperator(
            task_id='if_log_ifenduserpermissionwasassigned_47_blank_48',
            test='''{{ result('log_ifenduserpermissionwasassigned_47') | is_falsy }}''',
            yes_task="log_enduserpermission_49",
            no_task="if_request_locationuri_present_51",
        )

        log_enduserpermission_49 = rail.PythonOperator(
            task_id='log_enduserpermission_49',
            python_callable=lambda:  get_permission_set(
                "End User", 'get_all_permission_sets_44')
        )

        assign_permission_set_to_user_50 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_50',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ result('log_enduserpermission_49') }}"
            }
        )

        if_request_locationuri_present_51 = rail.IfOperator(
            task_id='if_request_locationuri_present_51',
            test='''{{ dag_run.conf.locationuri | is_truthy  and dag_run.conf.locationuri | matches('urn') }}''',
            yes_task="log_ifreportuserpermissionwasassigned_52",
            no_task="if_userdetails_isenabled_is_not_true_disabled_61",
        )

        log_ifreportuserpermissionwasassigned_52 = rail.PythonOperator(
            task_id='log_ifreportuserpermissionwasassigned_52',
            python_callable=lambda:  get_permission_set(
                "All Timesheets", 'get_assigned_permission_sets_for_user2_46')
        )

        if_log_ifreportuserpermissionwasassigned_52_blank_53 = rail.IfOperator(
            task_id='if_log_ifreportuserpermissionwasassigned_52_blank_53',
            test='''{{ result('log_ifreportuserpermissionwasassigned_52') | is_falsy }}''',
            yes_task="log_all_timesheetspermission_54",
            no_task="put_policy_data_access_scopes_for_user_56",
        )

        log_all_timesheetspermission_54 = rail.PythonOperator(
            task_id='log_all_timesheetspermission_54',
            python_callable=lambda:  get_permission_set(
                "All Timesheets", 'get_all_permission_sets_44')
        )

        assign_permission_set_to_user_55 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_55',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ result('log_all_timesheetspermission_54') }}"
            }
        )

        put_policy_data_access_scopes_for_user_56 = rail.RepliconServiceOperator(
            task_id='put_policy_data_access_scopes_for_user_56',
            endpoint="/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policyDataAccessScopes": [
                    {
                        "policyUri": "urn:replicon:policy:payroll-management",
                        "locations": [
                            {
                                "location": {
                                    "uri": "{{ dag_run.conf.locationuri }}",
                                    "parentUri": null,
                                    "name": null
                                }
                            }
                        ],
                        "divisions": [],
                        "costCenters": [],
                        "serviceCenters": [],
                        "departmentGroups": [],
                        "employeeTypeGroups": []
                    }
                ]
            }
        )

        locationuri_not_equals_locationschedulefirstlocationuri_57 = rail.IfOperator(
            task_id='locationuri_not_equals_locationschedulefirstlocationuri_57',
            test='''{{ dag_run.conf.locationuri != result('get_user_details_userreferencedetailsreport_14').locationSchedule[0].location.uri }}''',
            yes_task="update_location_schedule_for_user_58",
            no_task="else_60",
        )

        update_location_schedule_for_user_58 = rail.RepliconServiceOperator(
            task_id='update_location_schedule_for_user_58',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
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
                                    "effectiveDate": null
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": null,
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        log_forlookuplogs_59 = rail.PythonOperator(
            task_id='log_forlookuplogs_59',
            python_callable=lambda:  '''Location updated'''
        )

        else_60 = rail.EmptyOperator(
            task_id='else_60',
        )

        if_userdetails_isenabled_is_not_true_disabled_61 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_disabled_61',
            # pylint: disable=line-too-long
            test='''{{ result('get_user_details_userreferencedetailsreport_14').userDetails.isEnabled != True and dag_run.conf.accountstatus | lower == 'enabled' }}''',
            yes_task="re_enable_userprofile_62",
            no_task="if_request_accountstatus_equals_to_disabled_65",
        )

        re_enable_userprofile_62 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_62',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        remove_user_end_date_63 = rail.RepliconServiceOperator(
            task_id='remove_user_end_date_63',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": null,
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        log_forlookuplogs_64 = rail.PythonOperator(
            task_id='log_forlookuplogs_64',
            python_callable=lambda:  '''User profile re-enabled'''
        )

        if_request_accountstatus_equals_to_disabled_65 = rail.IfOperator(
            task_id='if_request_accountstatus_equals_to_disabled_65',
            test='''{{ dag_run.conf.accountstatus | lower == 'disabled' }}''',
            yes_task="if_userdetails_isenabled_is_true_enabled_66",
            no_task="log_forlogs_73",
        )

        if_userdetails_isenabled_is_true_enabled_66 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_enabled_66',
            test='''{{ result('get_user_details_userreferencedetailsreport_14').userDetails.isEnabled == True }}''',
            yes_task="disable_userprofile_67",
            no_task="log_forlogs_73",
        )

        disable_userprofile_67 = rail.RepliconServiceOperator(
            task_id='disable_userprofile_67',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_to_date_to_time_greater_than_todayto_time_68 = rail.IfOperator(
            task_id='if_to_date_to_time_greater_than_todayto_time_68',
            test=lambda dag_run: datetime.strptime(
                dag_run.conf['whencreated'], '%Y-%m-%d %H:%M:%S') > datetime.now(),
            yes_task="update_user_end_date_69",
            no_task="log_forlookuplogs_70",
        )

        update_user_end_date_69 = rail.RepliconServiceOperator(
            task_id='update_user_end_date_69',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": "{{ result('log_todays_year_11') }}",
                                "month": "{{ result('log_todays_month_12') }}",
                                "day": "{{ result('log_todays_day_13') }}"
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        log_forlookuplogs_70 = rail.PythonOperator(
            task_id='log_forlookuplogs_70',
            python_callable=lambda:  '''User profile Disabled'''
        )

        else_71 = rail.EmptyOperator(
            task_id='else_71',
        )

        log_forlookuplogs_72 = rail.PythonOperator(
            task_id='log_forlookuplogs_72',
            python_callable=lambda:  '''User is already Disabled'''
        )

        def get_log_meesage():
            message_logs = []
            message_logs.append(rail.result('log_forlookuplogs_21'))
            message_logs.append(rail.result('log_forlookuplogs_25'))
            message_logs.append(rail.result('log_forlookuplogs_28'))
            message_logs.append(rail.result('log_forlookuplogs_31'))
            message_logs.append(rail.result('log_forlookuplogs_34'))
            message_logs.append(rail.result('log_forlookuplogs_41'))
            message_logs.append(rail.result('log_forlookuplogs_59'))
            message_logs.append(rail.result('log_forlookuplogs_64'))
            message_logs.append(rail.result('log_forlookuplogs_70'))
            message_logs.append(rail.result('log_forlookuplogs_72'))
            message_logs.append(rail.result('log_forlookuplogs_loa_disabled'))
            message_logs.append(rail.result('log_forlookuplogs_loa_enabled'))
            return rail.smartjoin_by_delim(message_logs, '|')

        log_forlogs_73 = rail.PythonOperator(
            task_id='log_forlogs_73',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda:  get_log_meesage()
        )

        log_removingblankspaces_74 = rail.PythonOperator(
            task_id='log_removingblankspaces_74',
            python_callable=lambda:  
                rail.result('log_forlogs_73')
        )

        if_log_removingblankspaces_74_present_75 = rail.IfOperator(
            task_id='if_log_removingblankspaces_74_present_75',
            test='''{{ result('log_removingblankspaces_74') | is_truthy }}''',
            yes_task="nrdc_user_import_logs_add_entry_76",
            no_task="catch_77",
        )

        nrdc_user_import_logs_add_entry_76 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_76',
            message="Success",
            severity="Success",
            properties={
                "user": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }} | {{ dag_run.conf.emailaddress }}",
                "action": "Update User",
                "status": "Success",
                "details": "{{ result('log_removingblankspaces_74') }}|{{ dag_run_ecid() }}",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        catch_77 = rail.EmptyOperator(
            task_id='catch_77',
            trigger_rule='one_failed',
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
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

        if_loa_present = rail.IfOperator(
            task_id='if_loa_present',
            test='''{{ dag_run.conf.leaveofabsence | matches('LOA') }}''',
            yes_task="get_custom_fieldsforuser",
            no_task="if_loa_not_present",
        )

        get_custom_fieldsforuser = rail.RepliconServiceOperator(
            task_id='get_custom_fieldsforuser',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_loa_u_d_f  = rail.PythonOperator(
            task_id='log_loa_u_d_f',
            python_callable=lambda:  get_customoef_uri("LOA Status")
        )

        updateemployee_number_u_d_f = rail.RepliconServiceOperator(
            task_id='updateemployee_number_u_d_f',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_loa_u_d_f') }}",
                "value": "Yes"
            }
        )

        update_user_timesheet = rail.RepliconServiceOperator(
            task_id='update_user_timesheet',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data=lambda dag_run:{
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementTimesheetPeriodSchedule": [],
                        "updateTimesheetPeriodScheduleOverDateRange": {
                            "replacementTimesheetPeriodScheduleEntries": [
                                {
                                    "timesheetPeriod": {
                                        "name": "No timesheet period"
                                    },
                                    "effectiveDate": rail.parse_date(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
                                }
                            ]
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        disable_userprofile  = rail.RepliconServiceOperator(
            task_id='disable_userprofile',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_loa_not_present = rail.IfOperator(
            task_id='if_loa_not_present',
            test='''{{ dag_run.conf.leaveofabsence | is_falsy and result('get_user_details')[0].userDetails.customFieldValues|find_first_by_attr_and_get_attr("customField.displayText","LOA Status","text") | matches('Yes')  }}''',
            yes_task="get_custom_fieldsforuser_loa",
            no_task="log_removingblankspaces_74",
        )

        get_custom_fieldsforuser_loa = rail.RepliconServiceOperator(
            task_id='get_custom_fieldsforuser_loa',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_loa_u_d_f_loa  = rail.PythonOperator(
            task_id='log_loa_u_d_f_loa',
            python_callable=lambda:  get_customoef_uri("LOA Status")
        )

        updateemployee_number_u_d_f_loa = rail.RepliconServiceOperator(
            task_id='updateemployee_number_u_d_f_loa',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_loa_u_d_f_loa') }}",
                "value": "No"
            }
        )

        update_user_timesheet_loa = rail.RepliconServiceOperator(
            task_id='update_user_timesheet_loa',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data=lambda dag_run:{
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementTimesheetPeriodSchedule": [],
                        "updateTimesheetPeriodScheduleOverDateRange": {
                            "replacementTimesheetPeriodScheduleEntries": [
                                {
                                    "timesheetPeriod": {
                                        "name": rail.result("get_user_details")[0]['timesheetPeriodSchedule'][0]['timesheetPeriod']['displayText']
                                    },
                                    "effectiveDate": rail.parse_date(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
                                }
                            ]
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        enable_user  = rail.RepliconServiceOperator(
            task_id='enable_user',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_forlookuplogs_loa_enabled = rail.PythonOperator(
            task_id='log_forlookuplogs_loa_enabled',
            python_callable=lambda:  '''User is enabled since LOA is not present'''
        )

        log_forlookuplogs_loa_disabled = rail.PythonOperator(
            task_id='log_forlookuplogs_loa_disabled',
            python_callable=lambda:  '''User is disabled since LOA is present'''
        )
        

        nrdc_user_import_logs_add_entry_78 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_78',
            message="fixme get message from prop ",
            severity="fixme get severity from prop ",
            properties={
                "user": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname | {{ dag_run.conf.emailaddress }}",
                "action": "Update User",
                "status": "Error",
                "details": "User Not Updated {{ get_error_message() }}|{{ dag_run_ecid() }}",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> \
            get_custom_fieldsforuser_3 >> log_employeenumber_u_d_f_4 >> log_user_name_u_d_f_5 >> \
            log_t_y_p_e_uri_6 >> log_title_uri_7 >> log_todays_year_11 >> log_todays_month_12 >> log_todays_day_13 >> \
            get_user_details_userreferencedetailsreport_14 >> log_customfieldsdata_15 >> \
            if_userdetails_lastname_not_equals_to_dataworkato_service3cd9c331requestlastname_19
        if_userdetails_lastname_not_equals_to_dataworkato_service3cd9c331requestlastname_19 >> rail.Label(
            'Yes') >> update_last_name_20 >> log_forlookuplogs_21 >> if_request_employeeid_present_22
        if_userdetails_lastname_not_equals_to_dataworkato_service3cd9c331requestlastname_19 >> rail.Label(
            'No') >> if_request_employeeid_present_22
        if_request_employeeid_present_22 >> rail.Label(
            'Yes') >> if_userdetails_employeeid_not_equals_to_dataworkato_service3cd9c331requestemployeeid_23
        if_userdetails_employeeid_not_equals_to_dataworkato_service3cd9c331requestemployeeid_23 >> rail.Label(
            'Yes') >> updateemployee_id_24 >> log_forlookuplogs_25 >> \
            if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestempnumber_26
        if_userdetails_employeeid_not_equals_to_dataworkato_service3cd9c331requestemployeeid_23 >> rail.Label(
            'No') >> if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestempnumber_26
        if_request_employeeid_present_22 >> rail.Label(
            'No') >> if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestempnumber_26
        if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestempnumber_26 >> rail.Label(
            'Yes') >> updateemployee_number_u_d_f_27 >> log_forlookuplogs_28 >> \
            if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestuserfullname_29
        if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestempnumber_26 >> rail.Label(
            'No') >> if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestuserfullname_29
        if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestuserfullname_29 >> rail.Label(
            'Yes') >> updateusername_u_d_f_30 >> log_forlookuplogs_31 >> \
            if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requesttitle_32
        if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requestuserfullname_29 >> rail.Label(
            'No') >> if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requesttitle_32
        if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requesttitle_32 >> rail.Label(
            'Yes') >> update_title_u_d_f_33 >> log_forlookuplogs_34 >> if_request_department_present_35
        if_pluckvalue_smart_join_not_equals_to_dataworkato_service3cd9c331requesttitle_32 >> rail.Label(
            'No') >> if_request_department_present_35
        if_request_department_present_35 >> rail.Label(
            'Yes') >> if_department_name_not_equals_to_dataworkato_service3cd9c331requestdepartment_36
        if_department_name_not_equals_to_dataworkato_service3cd9c331requestdepartment_36 >> rail.Label(
            'Yes') >> get_enableddepartments_37 >> log_departmenturi_38 >> if_log_departmenturi_38_present_39
        if_log_departmenturi_38_present_39 >> rail.Label(
            'Yes') >> update_department_40 >> log_forlookuplogs_41 >> if_request_department_blank_42
        if_log_departmenturi_38_present_39 >> rail.Label(
            'No') >> if_request_department_blank_42
        if_department_name_not_equals_to_dataworkato_service3cd9c331requestdepartment_36 >> rail.Label(
            'No') >> if_request_department_blank_42
        if_request_department_present_35 >> rail.Label(
            'No') >> if_request_department_blank_42
        if_request_department_blank_42 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_43 >> get_all_permission_sets_44
        if_request_department_blank_42 >> rail.Label(
            'No') >> get_all_permission_sets_44 >> declare_list_45 >> get_assigned_permission_sets_for_user2_46 >> \
            log_ifenduserpermissionwasassigned_47 >> if_log_ifenduserpermissionwasassigned_47_blank_48
        if_log_ifenduserpermissionwasassigned_47_blank_48 >> rail.Label(
            'Yes') >> log_enduserpermission_49 >> assign_permission_set_to_user_50 >> if_request_locationuri_present_51
        if_log_ifenduserpermissionwasassigned_47_blank_48 >> rail.Label(
            'No') >> if_request_locationuri_present_51
        if_request_locationuri_present_51 >> rail.Label(
            'Yes') >> log_ifreportuserpermissionwasassigned_52 >> if_log_ifreportuserpermissionwasassigned_52_blank_53
        if_log_ifreportuserpermissionwasassigned_52_blank_53 >> rail.Label(
            'Yes') >> log_all_timesheetspermission_54 >> assign_permission_set_to_user_55 >> if_userdetails_isenabled_is_not_true_disabled_61
        if_log_ifreportuserpermissionwasassigned_52_blank_53 >> rail.Label(
            'No') >> put_policy_data_access_scopes_for_user_56 >> \
            locationuri_not_equals_locationschedulefirstlocationuri_57
        locationuri_not_equals_locationschedulefirstlocationuri_57 >> rail.Label(
            'Yes') >> update_location_schedule_for_user_58 >> log_forlookuplogs_59 >> \
            if_userdetails_isenabled_is_not_true_disabled_61
        locationuri_not_equals_locationschedulefirstlocationuri_57 >> rail.Label(
            'No') >> else_60 >> if_userdetails_isenabled_is_not_true_disabled_61
        if_request_locationuri_present_51 >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_disabled_61
        if_userdetails_isenabled_is_not_true_disabled_61 >> rail.Label(
            'Yes') >> re_enable_userprofile_62 >> remove_user_end_date_63 >> log_forlookuplogs_64 >> \
            if_request_accountstatus_equals_to_disabled_65
        if_userdetails_isenabled_is_not_true_disabled_61 >> rail.Label(
            'No') >> if_request_accountstatus_equals_to_disabled_65
        if_request_accountstatus_equals_to_disabled_65 >> rail.Label(
            'Yes') >> if_userdetails_isenabled_is_true_enabled_66
        if_userdetails_isenabled_is_true_enabled_66 >> rail.Label(
            'Yes') >> disable_userprofile_67 >> if_to_date_to_time_greater_than_todayto_time_68
        if_to_date_to_time_greater_than_todayto_time_68 >> rail.Label(
            'Yes') >> update_user_end_date_69 >> log_forlookuplogs_70
        if_to_date_to_time_greater_than_todayto_time_68 >> rail.Label(
            'No') >> log_forlookuplogs_70 >> else_71 >> log_forlookuplogs_72 >> log_forlogs_73
        if_userdetails_isenabled_is_true_enabled_66 >> rail.Label(
            'No') >> log_forlogs_73
        if_request_accountstatus_equals_to_disabled_65 >> rail.Label(
            'No') >> log_forlogs_73 >> get_user_details >> if_loa_present >> rail.Label("Yes") >> get_custom_fieldsforuser >> log_loa_u_d_f >> updateemployee_number_u_d_f >> update_user_timesheet  >> disable_userprofile >> log_forlookuplogs_loa_disabled >> log_removingblankspaces_74 >> if_log_removingblankspaces_74_present_75
        if_loa_present >> rail.Label("No") >> if_loa_not_present >> rail.Label("Yes") >> get_custom_fieldsforuser_loa >> log_loa_u_d_f_loa >> updateemployee_number_u_d_f_loa >> update_user_timesheet_loa >> enable_user >> log_forlookuplogs_loa_enabled >> log_removingblankspaces_74
        if_loa_not_present >> rail.Label("Yes") >> log_removingblankspaces_74
        if_log_removingblankspaces_74_present_75 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_76 >> log_to_sumo
        if_log_removingblankspaces_74_present_75 >> rail.Label(
            'No') >> catch_77 >> nrdc_user_import_logs_add_entry_78 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
