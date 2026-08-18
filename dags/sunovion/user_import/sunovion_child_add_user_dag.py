
from datetime import timedelta
from airflow.models import Variable
import rail

from sunovion.user_import.mappers.sunovion_mapper_file import sunovion_mapper
from sunovion.user_import.utils import request_payload

null = None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'sunovion_user_import_add_user_child_{config.instance}',
        description=f'Sunovion_Child_Add User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_request_paygroup_present_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_request_paygroup_present_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_paygroup_present_3 = rail.IfOperator(
            task_id='if_request_paygroup_present_3',
            test='''{{ dag_run.conf.paygroup | is_truthy }}''',
            yes_task="log_todaysdate_4",
            no_task="log_user_not_added_as_paygroup_blank",
        )

        log_todaysdate_4 = rail.PythonOperator(
            task_id='log_todaysdate_4',
            python_callable=request_payload.get_todays_date
        )

        sunovion_mapper_file_search_entries_checkfordepartmentonthemapper_8 = rail.PythonOperator(
            task_id='sunovion_mapper_file_search_entries_checkfordepartmentonthemapper_8',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["type"] == "department" and x["identifier_1"] == dag_run.conf['paygroup'], sunovion_mapper))
        )

        log_pluckifthedepartmentispresent_9 = rail.PythonOperator(
            task_id='log_pluckifthedepartmentispresent_9',
            python_callable=lambda: rail.result('sunovion_mapper_file_search_entries_checkfordepartmentonthemapper_8')[0]['data_set'] if rail.result(
                'sunovion_mapper_file_search_entries_checkfordepartmentonthemapper_8') else ''  # col5name
        )

        if_log_pluckifthedepartmentispresent_9_present_10 = rail.IfOperator(
            task_id='if_log_pluckifthedepartmentispresent_9_present_10',
            test='''{{ result('log_pluckifthedepartmentispresent_9') | is_truthy }}''',
            yes_task="adhoc_http_action_11",
            no_task="log_user_not_added_as_department_unavailable",
        )

        adhoc_http_action_11 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_11',
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['paygroup'], 'uri', '')
        )

        if_log_required_paygroupuri_12_present_13 = rail.IfOperator(
            task_id='if_log_required_paygroupuri_12_present_13',
            test='''{{ result('adhoc_http_action_11') | is_truthy }}''',
            yes_task="create_user_14",
            no_task="log_user_not_added_as_paygroup_unavailable",
        )

        create_user_14 = rail.RepliconServiceOperator(
            task_id='create_user_14',
            endpoint="/services/importservice1.svc/PutUser3",
            data={
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": "{{ dag_run.conf.loginname }}",
                        "parameterCorrelationId": null
                    },
                    "firstname": "{{ dag_run.conf.firstname }}",
                    "lastname": "{{ dag_run.conf.lastname }}",
                    "emailAddress": "{{ dag_run.conf.emailaddress }}",
                    "employeeId": "{{ dag_run.conf.employeeid }}",
                    "department": {
                        "uri": null,
                        "name": "{{ result('log_pluckifthedepartmentispresent_9') }}",
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": "{{ result('log_todaysdate_4').year }}",
                            "month": "{{ result('log_todaysdate_4').month }}",
                            "day": "{{ result('log_todaysdate_4').day }}"
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:replicon"
                        ],
                        "isLoginEnabled": "true",
                        "loginName": "{{ dag_run.conf.loginname }}",
                        "password": "key-K7DHn"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": [],
                    "policySets": [],
                    "employeeType": {
                        "uri": null,
                        "name": "{{ dag_run.conf.employeetype }}"
                    },
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": [],
                    "assignedActivities": [],
                    "timeZone": null,
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [
                        {
                            "serviceCenter": {
                                "uri": "{{ result('adhoc_http_action_11') }}",
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": {
                                "year": "{{ result('log_todaysdate_4').year }}",
                                "month": "{{ result('log_todaysdate_4').month }}",
                                "day": "{{ result('log_todaysdate_4').day }}"
                            }
                        }
                    ],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        unassignalltimeoffsforuser_15 = rail.RepliconServiceOperator(
            task_id='unassignalltimeoffsforuser_15',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                "timeOffTypeUris": []
            }
        )

        if_request_startdate_present_16 = rail.IfOperator(
            task_id='if_request_startdate_present_16',
            test='''{{ dag_run.conf.startdate | is_truthy }}''',
            yes_task="get_start_date_object",
            no_task="if_request_enddate_present_21",
        )

        get_start_date_object = rail.PythonOperator(
            task_id='get_start_date_object',
            python_callable=lambda dag_run: request_payload.get_date_object(
                dag_run.conf['startdate'])
        )

        update_employment_date_range_20 = rail.RepliconServiceOperator(
            task_id='update_employment_date_range_20',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('get_start_date_object').year }}",
                        "month": "{{ result('get_start_date_object').month }}",
                        "day": "{{ result('get_start_date_object').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_enddate_present_21 = rail.IfOperator(
            task_id='if_request_enddate_present_21',
            test='''{{ dag_run.conf.enddate | is_truthy }}''',
            yes_task="log_start_dateday_22",
            no_task="if_request_paygroup_equals_el8_or_emd",
        )

        log_start_dateday_22 = rail.PythonOperator(
            task_id='log_start_dateday_22',
            python_callable=lambda dag_run: request_payload.get_date_object(
                dag_run.conf['startdate'])
        )

        log_end_dateday_25 = rail.PythonOperator(
            task_id='log_end_dateday_25',
            python_callable=lambda dag_run: request_payload.get_date_object(
                dag_run.conf['enddate'])
        )

        update_employment_date_range_28 = rail.RepliconServiceOperator(
            task_id='update_employment_date_range_28',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('log_start_dateday_22').year }}",
                        "month": "{{ result('log_start_dateday_22').month }}",
                        "day": "{{ result('log_start_dateday_22').day }}"
                    },
                    "endDate": {
                        "year": "{{ result('log_end_dateday_25').year }}",
                        "month": "{{ result('log_end_dateday_25').month }}",
                        "day": "{{ result('log_end_dateday_25').day }}"
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_request_paygroup_equals_el8_or_emd = rail.IfOperator(
            task_id='if_request_paygroup_equals_el8_or_emd',
            test='''{{ dag_run.conf.paygroup == 'EL8' or dag_run.conf.paygroup == 'EMD'}}''',
            yes_task="set_s_s_o_authentication_for_user_31",
            no_task="adhoc_http_action_32",
        )

        set_s_s_o_authentication_for_user_31 = rail.RepliconServiceOperator(
            task_id='set_s_s_o_authentication_for_user_31',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                "loginName": "{{ dag_run.conf.loginname }}"
            }
        )

        adhoc_http_action_32 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_32',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_pluckifthedepartmentispresent_9'), 'uri', '')
        )

        if_log_departmenturi_33_present_34 = rail.IfOperator(
            task_id='if_log_departmenturi_33_present_34',
            test='''{{ result('adhoc_http_action_32') | is_truthy }}''',
            yes_task="update_department_for_user_35",
            no_task="log_department_not_available",
        )

        update_department_for_user_35 = rail.RepliconServiceOperator(
            task_id='update_department_for_user_35',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                "departmentUri": "{{ result('adhoc_http_action_32') }}"
            }
        )

        log_department_not_available = rail.WriteLogOperator(
            task_id='log_department_not_available',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''Department not added for User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}". "{{ result('adhoc_http_action_32') }}" not available in Replicon, hence, SDPA Parent is added as department for the user.''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        sunovion_mapper_file_search_entries_checkfortimeoffapprovalpathonthemapper_38 = rail.PythonOperator(
            task_id='sunovion_mapper_file_search_entries_checkfortimeoffapprovalpathonthemapper_38',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["type"] == "timeoff approval path" and x["identifier_1"] == dag_run.conf['employeetype'], sunovion_mapper))
        )

        log_pluckifthetimeoffapprovalpathispresent_39 = rail.PythonOperator(
            task_id='log_pluckifthetimeoffapprovalpathispresent_39',
            python_callable=lambda: rail.result('sunovion_mapper_file_search_entries_checkfortimeoffapprovalpathonthemapper_38')[0]['data_set'] if rail.result(
                'sunovion_mapper_file_search_entries_checkfortimeoffapprovalpathonthemapper_38') else ''
        )

        if_log_pluckifthetimeoffapprovalpathispresent_39_present_40 = rail.IfOperator(
            task_id='if_log_pluckifthetimeoffapprovalpathispresent_39_present_40',
            test='''{{ result('log_pluckifthetimeoffapprovalpathispresent_39') | is_truthy }}''',
            yes_task="adhoc_http_action_41",
            no_task="sunovion_mapper_file_search_entries_checkfortimesheettemplateonthemapper_45",
        )

        adhoc_http_action_41 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_41',
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_pluckifthetimeoffapprovalpathispresent_39'), 'uri', '')
        )

        if_log_timeoffapprovalpathuri_42_present_43 = rail.IfOperator(
            task_id='if_log_timeoffapprovalpathuri_42_present_43',
            test='''{{ result('adhoc_http_action_41') | is_truthy }}''',
            yes_task="update_timeoff_approval_path_for_user_44",
            no_task="sunovion_mapper_file_search_entries_checkfortimesheettemplateonthemapper_45",
        )

        update_timeoff_approval_path_for_user_44 = rail.RepliconServiceOperator(
            task_id='update_timeoff_approval_path_for_user_44',
            endpoint="/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                "approvalPathUri": "{{ result('adhoc_http_action_41') }}"
            }
        )

        sunovion_mapper_file_search_entries_checkfortimesheettemplateonthemapper_45 = rail.PythonOperator(
            task_id='sunovion_mapper_file_search_entries_checkfortimesheettemplateonthemapper_45',
            python_callable=lambda dag_run:  list(filter(lambda x: x["type"] == "timesheet template" and x["identifier_1"]
                                                  == dag_run.conf['paygroup'] and x["identifier_2"] == dag_run.conf['employeetype'], sunovion_mapper))
        )

        log_pluckifthetimesheettemplateispresent_46 = rail.PythonOperator(
            task_id='log_pluckifthetimesheettemplateispresent_46',
            python_callable=lambda: rail.result('sunovion_mapper_file_search_entries_checkfortimesheettemplateonthemapper_45')[0]['data_set'] if rail.result(
                'sunovion_mapper_file_search_entries_checkfortimesheettemplateonthemapper_45') else ''  # col5name
        )

        if_log_pluckifthetimesheettemplateispresent_46_present_47 = rail.IfOperator(
            task_id='if_log_pluckifthetimesheettemplateispresent_46_present_47',
            test='''{{ result('log_pluckifthetimesheettemplateispresent_46') | is_truthy }}''',
            yes_task="adhoc_http_action_48",
            no_task="log_timesheettemplate_not_available_for_paygroup",
        )

        adhoc_http_action_48 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_48',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_timesheet_template_uri = rail.PythonOperator(
            task_id='get_timesheet_template_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'adhoc_http_action_48'), 'displayText', rail.result('log_pluckifthetimesheettemplateispresent_46'), 'uri', '')
        )

        if_log_timesheettemplateuri_49_present_50 = rail.IfOperator(
            task_id='if_log_timesheettemplateuri_49_present_50',
            test='''{{ result('get_timesheet_template_uri') | is_truthy }}''',
            yes_task="log_time_offtemplateuri_51",
            no_task="log_timesheettemplate_not_available",
        )

        log_time_offtemplateuri_51 = rail.PythonOperator(
            task_id='log_time_offtemplateuri_51',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('adhoc_http_action_48'), 'displayText', 'Time Off', 'uri', '')
        )

        log_policysetstobeadded_52 = rail.PythonOperator(
            task_id='log_policysetstobeadded_52',
            python_callable=lambda: [rail.result('get_timesheet_template_uri')] + [rail.result('log_time_offtemplateuri_51')]
        )

        updatetemplatesforuser_53 = rail.RepliconServiceOperator(
            task_id='updatetemplatesforuser_53',
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data=lambda:{
                "userUri": rail.result('create_user_14')['uri'],
                "policySetUris": rail.result('log_policysetstobeadded_52')
            }
        )

        log_timesheettemplate_not_available = rail.WriteLogOperator(
            task_id='log_timesheettemplate_not_available',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''Timesheet template not added for User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}". "Timesheet template" "{{ result('log_pluckifthetimesheettemplateispresent_46') }}" as per mapper file is not available in Replicon''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_timesheettemplate_not_available_for_paygroup = rail.WriteLogOperator(
            task_id='log_timesheettemplate_not_available_for_paygroup',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''Timesheet template not added for User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}". "Timesheet template" not available for paygroup "{{ dag_run.conf.paygroup }}" and Employee Type "{{ dag_run.conf.employeetype }}" in mapper file''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        sunovion_mapper_file_search_entries_checkfortimesheetapprovalpathonthemapper_58 = rail.PythonOperator(
            task_id='sunovion_mapper_file_search_entries_checkfortimesheetapprovalpathonthemapper_58',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["type"] == "timesheet approval path" and x["identifier_1"] == dag_run.conf['employeetype'], sunovion_mapper))
        )

        log_pluckifthetimesheetapprovalpathispresent_59 = rail.PythonOperator(
            task_id='log_pluckifthetimesheetapprovalpathispresent_59',
            python_callable=lambda: rail.result(
                'sunovion_mapper_file_search_entries_checkfortimesheetapprovalpathonthemapper_58')[0]['data_set'] if rail.result(
                'sunovion_mapper_file_search_entries_checkfortimesheetapprovalpathonthemapper_58') else ''
        )

        if_log_pluckifthetimesheetapprovalpathispresent_59_present_60 = rail.IfOperator(
            task_id='if_log_pluckifthetimesheetapprovalpathispresent_59_present_60',
            test='''{{ result('log_pluckifthetimesheetapprovalpathispresent_59') | is_truthy }}''',
            yes_task="adhoc_http_action_61",
            no_task="log_timesheetapprovalpath_not_available_for_employeetype",
        )

        adhoc_http_action_61 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_61',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_pluckifthetimesheetapprovalpathispresent_59'), 'uri', '')
        )

        if_log_timesheetapprovalpathuri_62_present_63 = rail.IfOperator(
            task_id='if_log_timesheetapprovalpathuri_62_present_63',
            test='''{{ result('adhoc_http_action_61') | is_truthy }}''',
            yes_task="update_timesheet_approval_path_for_user_64",
            no_task="log_timesheetapprovalpath_not_available",
        )

        update_timesheet_approval_path_for_user_64 = rail.RepliconServiceOperator(
            task_id='update_timesheet_approval_path_for_user_64',
            endpoint="/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                "approvalPathUri": "{{ result('adhoc_http_action_61') }}"
            }
        )

        log_timesheetapprovalpath_not_available = rail.WriteLogOperator(
            task_id='log_timesheetapprovalpath_not_available',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''"Timesheet Approval Path" not added for User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}". Timesheet appoval path "{{ result('log_pluckifthetimesheetapprovalpathispresent_59') }}" as per mapper file is not available in Replicon''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_timesheetapprovalpath_not_available_for_employeetype = rail.WriteLogOperator(
            task_id='log_timesheetapprovalpath_not_available_for_employeetype',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''Timesheet Approval Path not added for User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}". "Timesheet Approval Path" not available for Employee Type "{{ dag_run.conf.employeetype }}" in mapper file''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        put_user_notification_preferences_69 = rail.RepliconServiceOperator(
            task_id='put_user_notification_preferences_69',
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data={
                "user": {
                    "uri": "{{ result('create_user_14').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "preferences": {
                    "notificationDeliveryPreferences": [
                        {
                            "objectTypeUri": "urn:replicon:object-type:user",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:timesheet",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:time-off",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:holiday",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        }
                    ],
                    "sharedDeliveryPreferenceOptionUris": [
                        "urn:replicon:user-shared-delivery-preference-option:always-deliver"
                    ]
                }
            }
        )

        sunovion_mapper_file_search_entries_checkforlicensesonthemapper_70 = rail.PythonOperator(
            task_id='sunovion_mapper_file_search_entries_checkforlicensesonthemapper_70',
            python_callable=lambda:  list(filter(lambda x: x["type"] == "licenses" and x["identifier_1"] == rail.result(
                'log_pluckifthedepartmentispresent_9'), sunovion_mapper))
        )

        log_pluckifthelicensesispresent_71 = rail.PythonOperator(
            task_id='log_pluckifthelicensesispresent_71',
            python_callable=lambda: rail.result('sunovion_mapper_file_search_entries_checkforlicensesonthemapper_70')[0]['data_set'] if rail.result(
                'sunovion_mapper_file_search_entries_checkforlicensesonthemapper_70') else ''
        )

        if_log_pluckifthelicensesispresent_71_present_72 = rail.IfOperator(
            task_id='if_log_pluckifthelicensesispresent_71_present_72',
            test='''{{ result('log_pluckifthelicensesispresent_71') | is_truthy }}''',
            yes_task="adhoc_http_action_73",
            no_task="if_request_supervisorid_present_82",
        )

        adhoc_http_action_73 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_73',
            endpoint="/services/AccountManagementService1.svc/GetAllProductsAvailableForUserAssignment",
        )

        def get_required_products_uri():
            products_required = (rail.result(
                'log_pluckifthelicensesispresent_71')).split("|")
            uris = [rail.find_first_by_attr_and_get_attr(rail.result(
                'adhoc_http_action_73'), 'displayText', product, 'uri', '') for product in products_required]
            return [uri for uri in uris if uri != '']

        get_uris_of_products_tobe_assigned = rail.PythonOperator(
            task_id='get_uris_of_products_tobe_assigned',
            python_callable=get_required_products_uri
        )

        put_product_assignments_for_user_81 = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user_81',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user_14')['uri'],
                "productUris": rail.result('get_uris_of_products_tobe_assigned')
            }
        )

        if_request_supervisorid_present_82 = rail.IfOperator(
            task_id='if_request_supervisorid_present_82',
            test='''{{ dag_run.conf.supervisorid | is_truthy }}''',
            yes_task="if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_83",
            no_task="if_request_permissionsets_present_92",
        )

        if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_83 = rail.IfOperator(
            task_id='if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_83',
            test='''{{ dag_run.conf.supervisorid != dag_run.conf.loginname }}''',
            yes_task="search_users_84",
            no_task="log_supervisor_not_updated",
        )

        def get_user_uri(response, dag_run):
            users_found = response['rows']
            matching_user = list(filter(
                lambda user: user['cells'][0]['textValue'] == dag_run.conf['supervisorid'], users_found))
            return {
                'uri': matching_user[0]['cells'][0]['uri'] if matching_user else ''
            }

        search_users_84 = rail.RepliconServiceOperator(
            task_id='search_users_84',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled",
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{dag_run.conf.supervisorid}}"
                        }
                    }
                }
            },
            data_handler=get_user_uri
        )

        if_log_getsupervisor_uri_85_present_86 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_85_present_86',
            test='''{{ result('search_users_84').uri | is_truthy }}''',
            yes_task="update_initial_supervisorwithtodayaseffectivedate_87",
            no_task="if_log_getsupervisor_uri_85_blank_88",
        )

        update_initial_supervisorwithtodayaseffectivedate_87 = rail.RepliconServiceOperator(
            task_id='update_initial_supervisorwithtodayaseffectivedate_87',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                "supervisorUri": "{{ result('search_users_84').uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('log_todaysdate_4').year }}",
                        "month": "{{ result('log_todaysdate_4').month }}",
                        "day": "{{ result('log_todaysdate_4').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_log_getsupervisor_uri_85_blank_88 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_85_blank_88',
            test='''{{ result('search_users_84').uri | is_falsy }}''',
            yes_task="sunovion_user_supervisor_mapping_table_add_entry_89",
            no_task="if_request_permissionsets_present_92",
        )

        sunovion_user_supervisor_mapping_table_add_entry_89 = rail.WriteLogOperator(
            task_id='sunovion_user_supervisor_mapping_table_add_entry_89',
            log="{{ dag_run.conf.supervisorlookuptable }}",
            message="na",
            severity="Error",
            properties={
                'jobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "supervisorid": "{{dag_run.conf.supervisorid}}",
                "status": "Error",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"
            }
        )

        log_supervisor_not_updated = rail.WriteLogOperator(
            task_id='log_supervisor_not_updated',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}" is created, however supervisor is not updated as the "Login name" for user and supervisor same on the input file''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_request_permissionsets_present_92 = rail.IfOperator(
            task_id='if_request_permissionsets_present_92',
            test='''{{ dag_run.conf.permissionsets | is_truthy }}''',
            yes_task="adhoc_http_action_93",
            no_task="update_time_zone_for_user_103",
        )

        adhoc_http_action_93 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_93',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        def get_uri_of_permissions_tobe_assigned(dag_run):
            required_permissions = (dag_run.conf['permissionsets']).split(',')
            permissions_uris = [rail.find_first_by_attr_and_get_attr(rail.result(
                'adhoc_http_action_93'), 'displayText', permission, 'uri', '') for permission in required_permissions]
            return [uri for uri in permissions_uris if uri != '']

        log_getnumberofpermissionstobeassigned_94 = rail.PythonOperator(
            task_id='log_getnumberofpermissionstobeassigned_94',
            python_callable=get_uri_of_permissions_tobe_assigned
        )

        if_log_permissiontobeassigned_100_present_101 = rail.IfOperator(
            task_id='if_log_permissiontobeassigned_100_present_101',
            test='''{{ result('log_getnumberofpermissionstobeassigned_94') | is_truthy }}''',
            yes_task="put_permission_set_assignments_for_user_102",
            no_task="update_time_zone_for_user_103",
        )

        put_permission_set_assignments_for_user_102 = rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_for_user_102',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user_14')['uri'],
                "permissionSetUris": rail.result('log_getnumberofpermissionstobeassigned_94')
            }
        )

        update_time_zone_for_user_103 = rail.RepliconServiceOperator(
            task_id='update_time_zone_for_user_103',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                "timeZoneUri": "urn:replicon:time-zone:america-new-york"
            }
        )

        update_work_week_for_user_104 = rail.RepliconServiceOperator(
            task_id='update_work_week_for_user_104',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                "dayOfWeekUri": "urn:replicon:day-of-week:sunday"
            }
        )

        if_request_residentstate_present_105 = rail.IfOperator(
            task_id='if_request_residentstate_present_105',
            test='''{{ dag_run.conf.residentstate | is_truthy }}''',
            yes_task="log_required_resident_state_110",
            no_task="log_holidaycalendar_not_updated_as_residentstate_blank",
        )

        log_required_resident_state_110 = rail.PythonOperator(
            task_id='log_required_resident_state_110',
            python_callable=lambda dag_run: 'Puerto Rico' if dag_run.conf[
                'residentstate'] == 'PR' else 'US'
        )

        sunovion_mapper_file_search_entries_checkforholidaycalendaronthemapper_111 = rail.PythonOperator(
            task_id='sunovion_mapper_file_search_entries_checkforholidaycalendaronthemapper_111',
            python_callable=lambda:  list(filter(
                lambda x: x["type"] == "holiday calendar" and x["identifier_1"] == "ALL" and x["identifier_2"] == "ALL", sunovion_mapper))
        )

        if_log_pluckiftheholidaycalendarispresentonthemapper_112_present_113 = rail.IfOperator(
            task_id='if_log_pluckiftheholidaycalendarispresentonthemapper_112_present_113',
            test=lambda: len(rail.result(
                'sunovion_mapper_file_search_entries_checkforholidaycalendaronthemapper_111')) > 0,
            yes_task="adhoc_http_action_114",
            no_task="log_holidaycalendar_not_updated",
        )

        adhoc_http_action_114 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_114',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
        )

        log_holidaycalendar_uri_115 = rail.PythonOperator(
            task_id='log_holidaycalendar_uri_115',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('adhoc_http_action_114'), 'displayText', (rail.result(
                'sunovion_mapper_file_search_entries_checkforholidaycalendaronthemapper_111')[0]['data_set'] if rail.result(
                'sunovion_mapper_file_search_entries_checkforholidaycalendaronthemapper_111') else ''), 'uri', '')
        )

        update_holiday_calendar_116 = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_116',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                "holidayCalendarUri": "{{ result('log_holidaycalendar_uri_115') }}"
            }
        )

        log_holidaycalendar_not_updated = rail.WriteLogOperator(
            task_id='log_holidaycalendar_not_updated',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}" is created, however holiday calendar is not updated as "Holiday Calendar" not available for paygroup "{{ dag_run.conf.paygroup }}" and location "{{ dag_run.conf.residentstate }}" in mapper file''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_holidaycalendar_not_updated_as_residentstate_blank = rail.WriteLogOperator(
            task_id='log_holidaycalendar_not_updated_as_residentstate_blank',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}" is created, however holiday calendar is not updated as "Resident State" is blank in input file''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_request_residentstate_present_121 = rail.IfOperator(
            task_id='if_request_residentstate_present_121',
            test='''{{ dag_run.conf.residentstate | is_truthy }}''',
            yes_task="adhoc_http_action_122",
            no_task="if_request_employeetype_present_132",
        )

        adhoc_http_action_122 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_122',
            endpoint="/services/LocationService1.svc/GetAllLocations",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['residentstate'], 'uri', '')
        )

        if_log_required_residentstateuri_123_present_124 = rail.IfOperator(
            task_id='if_log_required_residentstateuri_123_present_124',
            test='''{{ result('adhoc_http_action_122') | is_truthy }}''',
            yes_task="put_location_schedule_for_user_resident_state_125",
            no_task="adhoc_http_action_127",
        )

        put_location_schedule_for_user_resident_state_125 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_resident_state_125',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                    "scheduleEntries": [
                        {
                            "location": {
                                "uri": "{{ result('adhoc_http_action_122') }}",
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": {
                                "year": "{{ result('log_todaysdate_4').year }}",
                                "month": "{{ result('log_todaysdate_4').month }}",
                                "day": "{{ result('log_todaysdate_4').day }}"
                            }
                        }
                    ]
            }
        )

        adhoc_http_action_127 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_127',
            endpoint="/services/LocationService1.svc/CreateNewDraft",
        )

        update_namefor_resident_state_128 = rail.RepliconServiceOperator(
            task_id='update_namefor_resident_state_128',
            endpoint="/services/LocationService1.svc/UpdateName",
            data={
                "locationUri": "{{result('adhoc_http_action_127')}}",
                "name": "{{ dag_run.conf.residentstate }}"
            }
        )

        update_codefor_resident_state_129 = rail.RepliconServiceOperator(
            task_id='update_codefor_resident_state_129',
            endpoint="/services/LocationService1.svc/UpdateCode",
            data={
                "locationUri": "{{result('adhoc_http_action_127')}}",
                "code": "{{ dag_run.conf.residentstate }}"
            }
        )

        adhoc_http_action_130 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_130',
            endpoint="/services/LocationService1.svc/PublishDraft",
            data={
                "draftUri": "{{result('adhoc_http_action_127')}}"
            }
        )

        put_location_schedule_for_user_resident_state_131 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_resident_state_131',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                    "scheduleEntries": [
                        {
                            "location": {
                                "uri": "{{result('adhoc_http_action_130').uri}}",
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": {
                                "year": "{{ result('log_todaysdate_4').year }}",
                                "month": "{{ result('log_todaysdate_4').month }}",
                                "day": "{{ result('log_todaysdate_4').day }}"
                            }
                        }
                    ]
            }
        )

        if_request_employeetype_present_132 = rail.IfOperator(
            task_id='if_request_employeetype_present_132',
            test='''{{ dag_run.conf.employeetype | is_truthy }}''',
            yes_task="adhoc_http_action_133",
            no_task="if_request_costcenter_present_137",
        )

        adhoc_http_action_133 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_133',
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
            data_handler=lambda response, dag_run: {
                'requiredgroupuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf['employeetype'], 'uri', ''),
                'nonexemptgroupuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Non-Exempt', 'uri', '')
            }
        )

        if_log_required_employeetype_groupuri_134_present_135 = rail.IfOperator(
            task_id='if_log_required_employeetype_groupuri_134_present_135',
            test='''{{ result('adhoc_http_action_133').requiredgroupuri | is_truthy }}''',
            yes_task="put_division_schedule_for_user_employee_type_136",
            no_task="if_employeetype_equal_ca_non_exempt",
        )

        put_division_schedule_for_user_employee_type_136 = rail.RepliconServiceOperator(
            task_id='put_division_schedule_for_user_employee_type_136',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                    "scheduleEntries": [
                        {
                            "division": {
                                "uri": "{{ result('adhoc_http_action_133').requiredgroupuri }}",
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": {
                                "year": "{{ result('log_todaysdate_4').year }}",
                                "month": "{{ result('log_todaysdate_4').month }}",
                                "day": "{{ result('log_todaysdate_4').day }}"
                            }
                        }
                    ]
            }
        )

        if_employeetype_equal_ca_non_exempt = rail.IfOperator(
            task_id='if_employeetype_equal_ca_non_exempt',
            test=lambda dag_run: dag_run.conf['employeetype'] == 'CA Non-Exempt',
            yes_task='if_non_exempt_uri_present',
            no_task='if_request_costcenter_present_137'
        )

        if_non_exempt_uri_present = rail.IfOperator(
            task_id='if_non_exempt_uri_present',
            test=lambda: bool(rail.result('adhoc_http_action_133')[
                              'nonexemptgroupuri']),
            yes_task='put_divisionschedule_for_user_employeetype',
            no_task='if_request_costcenter_present_137'
        )

        put_divisionschedule_for_user_employeetype = rail.RepliconServiceOperator(
            task_id='put_divisionschedule_for_user_employeetype',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                "scheduleEntries": [
                    {
                        "division": {
                            "uri": "{{ result('adhoc_http_action_133').nonexemptgroupuri }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": {
                            "year": "{{ result('log_todaysdate_4').year }}",
                            "month": "{{ result('log_todaysdate_4').month }}",
                            "day": "{{ result('log_todaysdate_4').day }}"
                        }
                    }
                ]
            }
        )

        if_request_costcenter_present_137 = rail.IfOperator(
            task_id='if_request_costcenter_present_137',
            test='''{{ dag_run.conf.costcenter | is_truthy }}''',
            yes_task="adhoc_http_action_138",
            no_task="adhoc_http_action_152",
        )

        adhoc_http_action_138 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_138',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data_handler=lambda response, dag_run: {
                'requiredcostcenteruri': rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf['costcenter'], 'uri', '')
            }
        )

        if_log_required_costcenteruri_139_present_140 = rail.IfOperator(
            task_id='if_log_required_costcenteruri_139_present_140',
            test='''{{ result('adhoc_http_action_138').requiredcostcenteruri | is_truthy }}''',
            yes_task="put_cost_center_schedule_for_user_141",
            no_task="if_log_check_cost_center_length_143_less_than_51_144",
        )

        put_cost_center_schedule_for_user_141 = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_141',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                    "scheduleEntries": [
                        {
                            "costCenter": {
                                "uri": "{{ result('adhoc_http_action_138').requiredcostcenteruri }}",
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": {
                                "year": "{{ result('log_todaysdate_4').year }}",
                                "month": "{{ result('log_todaysdate_4').month }}",
                                "day": "{{ result('log_todaysdate_4').day }}"
                            }
                        }
                    ]
            }
        )

        if_log_check_cost_center_length_143_less_than_51_144 = rail.IfOperator(
            task_id='if_log_check_cost_center_length_143_less_than_51_144',
            test=lambda dag_run: len(dag_run.conf['costcenter']) < 51,
            yes_task="adhoc_http_action_145",
            no_task="log_costcenter_not_added",
        )

        adhoc_http_action_145 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_145',
            endpoint="/services/CostCenterService1.svc/CreateNewDraft",
        )

        update_namefor_cost_center_146 = rail.RepliconServiceOperator(
            task_id='update_namefor_cost_center_146',
            endpoint="/services/CostCenterService1.svc/UpdateName",
            data={
                "costCenterUri": "{{result('adhoc_http_action_145')}}",
                "name": "{{ dag_run.conf.costcenter }}"
            }
        )

        update_codefor_cost_center_147 = rail.RepliconServiceOperator(
            task_id='update_codefor_cost_center_147',
            endpoint="/services/CostCenterService1.svc/UpdateCode",
            data={
                "costCenterUri": "{{result('adhoc_http_action_145')}}",
                "code": "{{ dag_run.conf.costcenter }}"
            }
        )

        adhoc_http_action_148 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_148',
            endpoint="/services/CostCenterService1.svc/PublishDraft",
            data={
                "draftUri": "{{result('adhoc_http_action_145')}}"
            }
        )

        put_cost_center_schedule_for_user_149 = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_149',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                    "scheduleEntries": [
                        {
                            "costCenter": {
                                "uri": "{{result('adhoc_http_action_148').uri}}",
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": {
                                "year": "{{ result('log_todaysdate_4').year }}",
                                "month": "{{ result('log_todaysdate_4').month }}",
                                "day": "{{ result('log_todaysdate_4').day }}"
                            }
                        }
                    ]
            }
        )

        log_costcenter_not_added = rail.WriteLogOperator(
            task_id='log_costcenter_not_added',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''Cost center is not added for user "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}" as "{{ dag_run.conf.costcenter }}" have more than 50 characters''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        adhoc_http_action_152 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_152',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda response: {
                'payrulenonexempt': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Sunovion Payrule Non-Exempt', 'uri', ''),
                'payrulecanonexempt': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Sunovion Payrule CA Non-Exempt', 'uri', ''),
                'payruleexempt': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Sunovion Payrule Exempt', 'uri', '')
            }
        )

        if_request_employeetype_equals_to_nonexempt_153 = rail.IfOperator(
            task_id='if_request_employeetype_equals_to_nonexempt_153',
            test='''{{ dag_run.conf.employeetype == 'Non-Exempt' }}''',
            yes_task="if_log_required_payruleuri_154_present_155",
            no_task="if_employeetype_equal_ca_nonexempt",
        )

        if_log_required_payruleuri_154_present_155 = rail.IfOperator(
            task_id='if_log_required_payruleuri_154_present_155',
            test='''{{ result('adhoc_http_action_152').payrulenonexempt | is_truthy }}''',
            yes_task="put_pay_rule_script_assignment_schedule_for_user_156",
            no_task="if_employeetype_equal_ca_nonexempt",
        )

        put_pay_rule_script_assignment_schedule_for_user_156 = rail.RepliconServiceOperator(
            task_id='put_pay_rule_script_assignment_schedule_for_user_156',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": "{{ result('adhoc_http_action_152').payrulenonexempt }}",
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_employeetype_equal_ca_nonexempt = rail.IfOperator(
            task_id='if_employeetype_equal_ca_nonexempt',
            test='''{{ dag_run.conf.employeetype == 'CA Non-Exempt' }}''',
            yes_task='if_canonexempturi_present',
            no_task='if_request_employeetype_equals_to_exempt_157'
        )

        if_canonexempturi_present = rail.IfOperator(
            task_id='if_canonexempturi_present',
            test='''{{ result('adhoc_http_action_152').payrulecanonexempt | is_truthy }}''',
            yes_task='put_payrule_script_assignment_schedule_foruser',
            no_task='if_request_employeetype_equals_to_exempt_157'
        )

        put_payrule_script_assignment_schedule_foruser = rail.RepliconServiceOperator(
            task_id='put_payrule_script_assignment_schedule_foruser',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                "scheduleEntries": [
                    {
                        "payRuleScript": {
                            "uri": "{{ result('adhoc_http_action_152').payrulecanonexempt }}",
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_request_employeetype_equals_to_exempt_157 = rail.IfOperator(
            task_id='if_request_employeetype_equals_to_exempt_157',
            test='''{{ dag_run.conf.employeetype == 'Exempt' }}''',
            yes_task="if_log_required_payruleuri_158_present_159",
            no_task="if_request_initialschedulename_present_exempt_161",
        )

        if_log_required_payruleuri_158_present_159 = rail.IfOperator(
            task_id='if_log_required_payruleuri_158_present_159',
            test='''{{ result('adhoc_http_action_152').payruleexempt | is_truthy }}''',
            yes_task="put_pay_rule_script_assignment_schedule_for_user_160",
            no_task="if_request_initialschedulename_present_exempt_161",
        )

        put_pay_rule_script_assignment_schedule_for_user_160 = rail.RepliconServiceOperator(
            task_id='put_pay_rule_script_assignment_schedule_for_user_160',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                    "scheduleEntries": [
                        {
                            "payRuleScript": {
                                "uri": "{{ result('adhoc_http_action_152').payruleexempt }}",
                                "name": null
                            },
                            "effectiveDate": null
                        }
                    ]
            }
        )

        if_request_initialschedulename_present_exempt_161 = rail.IfOperator(
            task_id='if_request_initialschedulename_present_exempt_161',
            test='''{{ dag_run.conf.initialschedulename | is_truthy }}''',
            yes_task="adhoc_http_action_162",
            no_task="adhoc_http_action_166",
        )

        adhoc_http_action_162 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_162',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['initialschedulename'], 'uri', '')
        )

        if_log_required_scheduleuri_163_present_164 = rail.IfOperator(
            task_id='if_log_required_scheduleuri_163_present_164',
            test='''{{ result('adhoc_http_action_162') | is_truthy }}''',
            yes_task="put_schedule_policy_schedule_for_user_165",
            no_task="adhoc_http_action_166",
        )

        put_schedule_policy_schedule_for_user_165 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user_165',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ result('create_user_14').uri }}",
                    "scheduleEntries":  [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": "{{ result('adhoc_http_action_162') }}",
                                "name": null,
                                "officeSchedule": null,
                                "scheduleTypeUri": null
                            },
                            "effectiveDate": {
                                "year": "{{ result('log_todaysdate_4').year }}",
                                "month": "{{ result('log_todaysdate_4').month }}",
                                "day": "{{ result('log_todaysdate_4').day }}"
                            }
                        }
                    ]
            }
        )

        adhoc_http_action_166 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_166',
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroups",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'User', 'uri', '')
        )

        if_log_required_customfieldgroupuri_167_present_168 = rail.IfOperator(
            task_id='if_log_required_customfieldgroupuri_167_present_168',
            test='''{{ result('adhoc_http_action_166') | is_truthy }}''',
            yes_task="adhoc_http_action_169",
            no_task="trigger_child_to_add_timeofftype_for_new_user",
        )

        adhoc_http_action_169 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_169',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{ result('adhoc_http_action_166') }}"
            },
            data_handler=lambda response: {
                'vacationaccrualdateuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Vacation Accrual Date', 'uri', ''),
                'scheduledhrsperweekuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Scheduled Hours Per Week', 'uri', ''),
                'workdayemployeetypeuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Workday Employee Type', 'uri', ''),
                'workdayexecutiveuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Workday Executive', 'uri', '')
            }
        )

        if_request_vacationaccrualdate_present_170 = rail.IfOperator(
            task_id='if_request_vacationaccrualdate_present_170',
            test='''{{ dag_run.conf.vacationaccrualdate | is_truthy }}''',
            yes_task="if_log_get_required_vacation_accrual_date_u_d_f_uri_171_present_172",
            no_task="if_request_scheduledhoursperweek_present_177",
        )

        if_log_get_required_vacation_accrual_date_u_d_f_uri_171_present_172 = rail.IfOperator(
            task_id='if_log_get_required_vacation_accrual_date_u_d_f_uri_171_present_172',
            test='''{{ result('adhoc_http_action_169').vacationaccrualdateuri | is_truthy }}''',
            yes_task="log_get_required_vacation_accrual_date_u_d_fday_173",
            no_task="if_request_scheduledhoursperweek_present_177",
        )

        log_get_required_vacation_accrual_date_u_d_fday_173 = rail.PythonOperator(
            task_id='log_get_required_vacation_accrual_date_u_d_fday_173',
            python_callable=lambda dag_run: request_payload.get_date_object(
                dag_run.conf['vacationaccrualdate'])
        )

        update_date_valuefor_vacation_accrual_date_176 = rail.RepliconServiceOperator(
            task_id='update_date_valuefor_vacation_accrual_date_176',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ result('create_user_14').uri }}",
                "customFieldUri": "{{ result('adhoc_http_action_169').vacationaccrualdateuri }}",
                "value": {
                    "year": "{{ result('log_get_required_vacation_accrual_date_u_d_fday_173').year }}",
                    "month": "{{ result('log_get_required_vacation_accrual_date_u_d_fday_173').month }}",
                    "day": "{{ result('log_get_required_vacation_accrual_date_u_d_fday_173').day }}"
                }
            }
        )

        if_request_scheduledhoursperweek_present_177 = rail.IfOperator(
            task_id='if_request_scheduledhoursperweek_present_177',
            test='''{{ dag_run.conf.scheduledhoursperweek | is_truthy }}''',
            yes_task="if_log_get_required_scheduled_hours_per_week_u_d_f_uri_178_present_179",
            no_task="if_request_workdayemployeetype_present_181",
        )

        if_log_get_required_scheduled_hours_per_week_u_d_f_uri_178_present_179 = rail.IfOperator(
            task_id='if_log_get_required_scheduled_hours_per_week_u_d_f_uri_178_present_179',
            test='''{{ result('adhoc_http_action_169').scheduledhrsperweekuri | is_truthy }}''',
            yes_task="update_text_valuefor_scheduled_hours_per_week_180",
            no_task="if_request_workdayemployeetype_present_181",
        )

        update_text_valuefor_scheduled_hours_per_week_180 = rail.RepliconServiceOperator(
            task_id='update_text_valuefor_scheduled_hours_per_week_180',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_14').uri }}",
                "customFieldUri": "{{ result('adhoc_http_action_169').scheduledhrsperweekuri }}",
                "value": "{{ dag_run.conf.scheduledhoursperweek }}"
            }
        )

        if_request_workdayemployeetype_present_181 = rail.IfOperator(
            task_id='if_request_workdayemployeetype_present_181',
            test='''{{ dag_run.conf.workdayemployeetype | is_truthy }}''',
            yes_task="if_log_get_required_workday_employee_type_u_d_f_uri_182_present_183",
            no_task="if_request_workdayexecutive_present_188",
        )

        if_log_get_required_workday_employee_type_u_d_f_uri_182_present_183 = rail.IfOperator(
            task_id='if_log_get_required_workday_employee_type_u_d_f_uri_182_present_183',
            test='''{{ result('adhoc_http_action_169').workdayemployeetypeuri | is_truthy }}''',
            yes_task="adhoc_http_action_184",
            no_task="if_request_workdayexecutive_present_188",
        )

        adhoc_http_action_184 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_184',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('adhoc_http_action_169').workdayemployeetypeuri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['workdayemployeetype'], 'uri', '')
        )

        if_log_get_required_workday_employee_typedropdown_uri_185_present_186 = rail.IfOperator(
            task_id='if_log_get_required_workday_employee_typedropdown_uri_185_present_186',
            test='''{{ result('adhoc_http_action_184') | is_truthy }}''',
            yes_task="update_dropdown_valuefor_workday_employee_type_187",
            no_task="if_request_workdayexecutive_present_188",
        )

        update_dropdown_valuefor_workday_employee_type_187 = rail.RepliconServiceOperator(
            task_id='update_dropdown_valuefor_workday_employee_type_187',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user_14').uri }}",
                "customFieldUri": "{{ result('adhoc_http_action_169').workdayemployeetypeuri }}",
                "customFieldDropDownOptionUri": "{{ result('adhoc_http_action_184') }}"
            }
        )

        if_request_workdayexecutive_present_188 = rail.IfOperator(
            task_id='if_request_workdayexecutive_present_188',
            test='''{{ dag_run.conf.workdayexecutive | is_truthy }}''',
            yes_task="if_log_get_required_workday_executive_u_d_f_uri_189_present_190",
            no_task="trigger_child_to_add_timeofftype_for_new_user",
        )

        if_log_get_required_workday_executive_u_d_f_uri_189_present_190 = rail.IfOperator(
            task_id='if_log_get_required_workday_executive_u_d_f_uri_189_present_190',
            test='''{{ result('adhoc_http_action_169').workdayexecutiveuri | is_truthy }}''',
            yes_task="adhoc_http_action_191",
            no_task="trigger_child_to_add_timeofftype_for_new_user",
        )

        adhoc_http_action_191 = rail.RepliconServiceOperator(
            task_id='adhoc_http_action_191',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('adhoc_http_action_169').workdayexecutiveuri }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['workdayexecutive'], 'uri', '')
        )

        if_log_get_required_workday_executivedropdown_uri_192_present_193 = rail.IfOperator(
            task_id='if_log_get_required_workday_executivedropdown_uri_192_present_193',
            test='''{{ result('adhoc_http_action_191') | is_truthy }}''',
            yes_task="update_dropdown_valuefor_workday_executive_194",
            no_task="trigger_child_to_add_timeofftype_for_new_user",
        )

        update_dropdown_valuefor_workday_executive_194 = rail.RepliconServiceOperator(
            task_id='update_dropdown_valuefor_workday_executive_194',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('create_user_14').uri }}",
                "customFieldUri": "{{ result('adhoc_http_action_169').workdayexecutiveuri }}",
                "customFieldDropDownOptionUri": "{{ result('adhoc_http_action_191') }}"
            }
        )

        trigger_child_to_add_timeofftype_for_new_user = rail.TriggerDagRunOperator(
            task_id='trigger_child_to_add_timeofftype_for_new_user',
            retries=0,
            trigger_dag_id=f'sunovion_user_import_workflow_to_add_timeoff_type_for_new_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "callerjobid": "{{ dag_run.conf.callerjobid }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "userloginname": "{{ dag_run.conf.loginname }}",
                "useruri": "{{ result('create_user_14').uri }}",
                "employeetype": "{{ dag_run.conf.employeetype }}",
                "location": "{{ dag_run.conf.residentstate }}",
                "workdayemployeetype": "{{ dag_run.conf.workdayemployeetype }}",
                "workdayexecutive": "{{ dag_run.conf.workdayexecutive }}"
            }
        )

        waitfor_child_to_add_timeofftype_for_new_user = rail.WaitForDagRunsSensor(
            task_id='waitfor_child_to_add_timeofftype_for_new_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_to_add_timeofftype_for_new_user") }}'
        )

        log_user_added_successfully = rail.WriteLogOperator(
            task_id='log_user_added_successfully',
            log="{{ dag_run.conf.userimportlogtable}}",
            message="na",
            severity="Success",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Success",
                "failurereason": "",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )


        log_user_not_added_as_paygroup_unavailable = rail.WriteLogOperator(
            task_id='log_user_not_added_as_paygroup_unavailable',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}" is not created as paygroup  "{{ dag_run.conf.paygroup }}" is not available in Replicon''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_user_not_added_as_department_unavailable = rail.WriteLogOperator(
            task_id='log_user_not_added_as_department_unavailable',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                #pylint: disable = line-too-long
                "failurereason": '''User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}" not created as "Department" is not available for paygroup "{{ dag_run.conf.paygroup }}" in mapper file''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_user_not_added_as_paygroup_blank = rail.WriteLogOperator(
            task_id='log_user_not_added_as_paygroup_blank',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                'parentjobid': "{{dag_run.conf.callerjobid}}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                "failurereason": '''User "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}" not created as "Paygroup" is blank on input file''',
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        def get_task_state(task_id):
            return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.userimportlogtable }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                'parentjobid': dag_run.conf['callerjobid'],
                "loginname": dag_run.conf['loginname'],
                "status": "Error",
                "failurereason": 'User"' + dag_run.conf['firstname'] + ' ' + dag_run.conf['lastname'] +
                    ('" is created, however all fields are not updated: ' if get_task_state(
                    'update_employment_date_range_28') == 'success' else '" not created:') + rail.render_template("{{get_error_message()}}"),
                "childjobid": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> if_request_paygroup_present_3
        if_request_paygroup_present_3 >> rail.Label(
            'Yes') >> log_todaysdate_4 >> sunovion_mapper_file_search_entries_checkfordepartmentonthemapper_8 >> log_pluckifthedepartmentispresent_9
        log_pluckifthedepartmentispresent_9 >> if_log_pluckifthedepartmentispresent_9_present_10
        if_log_pluckifthedepartmentispresent_9_present_10 >> rail.Label(
            'Yes') >> adhoc_http_action_11 >> if_log_required_paygroupuri_12_present_13
        if_log_required_paygroupuri_12_present_13 >> rail.Label(
            'Yes') >> create_user_14 >> unassignalltimeoffsforuser_15 >> if_request_startdate_present_16
        if_request_startdate_present_16 >> rail.Label(
            'Yes') >> get_start_date_object >> update_employment_date_range_20 >> if_request_enddate_present_21
        if_request_startdate_present_16 >> rail.Label(
            'No') >> if_request_enddate_present_21
        if_request_enddate_present_21 >> rail.Label(
            'Yes') >> log_start_dateday_22 >> log_end_dateday_25 >> update_employment_date_range_28 >> if_request_paygroup_equals_el8_or_emd
        if_request_enddate_present_21 >> rail.Label(
            'No') >> if_request_paygroup_equals_el8_or_emd
        if_request_paygroup_equals_el8_or_emd >> rail.Label(
            'Yes') >> set_s_s_o_authentication_for_user_31 >> adhoc_http_action_32
        if_request_paygroup_equals_el8_or_emd >> rail.Label(
            'No') >> adhoc_http_action_32 >> if_log_departmenturi_33_present_34
        if_log_departmenturi_33_present_34 >> rail.Label(
            'Yes') >> update_department_for_user_35 >> sunovion_mapper_file_search_entries_checkfortimeoffapprovalpathonthemapper_38
        if_log_departmenturi_33_present_34 >> rail.Label(
            'No') >> log_department_not_available >> sunovion_mapper_file_search_entries_checkfortimeoffapprovalpathonthemapper_38
        sunovion_mapper_file_search_entries_checkfortimeoffapprovalpathonthemapper_38 >> log_pluckifthetimeoffapprovalpathispresent_39
        log_pluckifthetimeoffapprovalpathispresent_39 >> if_log_pluckifthetimeoffapprovalpathispresent_39_present_40
        if_log_pluckifthetimeoffapprovalpathispresent_39_present_40 >> rail.Label(
            'Yes') >> adhoc_http_action_41 >> if_log_timeoffapprovalpathuri_42_present_43
        if_log_timeoffapprovalpathuri_42_present_43 >> rail.Label(
            'Yes') >> update_timeoff_approval_path_for_user_44 >> sunovion_mapper_file_search_entries_checkfortimesheettemplateonthemapper_45
        if_log_timeoffapprovalpathuri_42_present_43 >> rail.Label(
            'No') >> sunovion_mapper_file_search_entries_checkfortimesheettemplateonthemapper_45
        if_log_pluckifthetimeoffapprovalpathispresent_39_present_40 >> rail.Label(
            'No') >> sunovion_mapper_file_search_entries_checkfortimesheettemplateonthemapper_45 >> log_pluckifthetimesheettemplateispresent_46
        log_pluckifthetimesheettemplateispresent_46 >> if_log_pluckifthetimesheettemplateispresent_46_present_47
        if_log_pluckifthetimesheettemplateispresent_46_present_47 >> rail.Label(
            'Yes') >> adhoc_http_action_48 >> get_timesheet_template_uri >> if_log_timesheettemplateuri_49_present_50
        if_log_timesheettemplateuri_49_present_50 >> rail.Label(
            'Yes') >> log_time_offtemplateuri_51 >> log_policysetstobeadded_52 >> updatetemplatesforuser_53
        updatetemplatesforuser_53 >> sunovion_mapper_file_search_entries_checkfortimesheetapprovalpathonthemapper_58
        if_log_timesheettemplateuri_49_present_50 >> rail.Label(
            'No') >> log_timesheettemplate_not_available >> sunovion_mapper_file_search_entries_checkfortimesheetapprovalpathonthemapper_58
        if_log_pluckifthetimesheettemplateispresent_46_present_47 >> rail.Label(
            'No') >> log_timesheettemplate_not_available_for_paygroup >> sunovion_mapper_file_search_entries_checkfortimesheetapprovalpathonthemapper_58
        sunovion_mapper_file_search_entries_checkfortimesheetapprovalpathonthemapper_58 >> log_pluckifthetimesheetapprovalpathispresent_59
        log_pluckifthetimesheetapprovalpathispresent_59 >> if_log_pluckifthetimesheetapprovalpathispresent_59_present_60
        if_log_pluckifthetimesheetapprovalpathispresent_59_present_60 >> rail.Label(
            'Yes') >> adhoc_http_action_61 >> if_log_timesheetapprovalpathuri_62_present_63
        if_log_timesheetapprovalpathuri_62_present_63 >> rail.Label(
            'Yes') >> update_timesheet_approval_path_for_user_64 >> put_user_notification_preferences_69
        if_log_timesheetapprovalpathuri_62_present_63 >> rail.Label(
            'No') >> log_timesheetapprovalpath_not_available >> put_user_notification_preferences_69
        if_log_pluckifthetimesheetapprovalpathispresent_59_present_60 >> rail.Label(
            'No') >> log_timesheetapprovalpath_not_available_for_employeetype >> put_user_notification_preferences_69
        put_user_notification_preferences_69 >> sunovion_mapper_file_search_entries_checkforlicensesonthemapper_70 >> log_pluckifthelicensesispresent_71
        log_pluckifthelicensesispresent_71 >> if_log_pluckifthelicensesispresent_71_present_72
        if_log_pluckifthelicensesispresent_71_present_72 >> rail.Label(
            'Yes') >> adhoc_http_action_73 >> get_uris_of_products_tobe_assigned >> put_product_assignments_for_user_81
        put_product_assignments_for_user_81 >> if_request_supervisorid_present_82
        if_log_pluckifthelicensesispresent_71_present_72 >> rail.Label(
            'No') >> if_request_supervisorid_present_82
        if_request_supervisorid_present_82 >> rail.Label(
            'Yes') >> if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_83
        if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_83 >> rail.Label(
            'Yes') >> search_users_84 >> if_log_getsupervisor_uri_85_present_86
        if_log_getsupervisor_uri_85_present_86 >> rail.Label(
            'Yes') >> update_initial_supervisorwithtodayaseffectivedate_87 >> if_log_getsupervisor_uri_85_blank_88
        if_log_getsupervisor_uri_85_present_86 >> rail.Label(
            'No') >> if_log_getsupervisor_uri_85_blank_88
        if_log_getsupervisor_uri_85_blank_88 >> rail.Label(
            'Yes') >> sunovion_user_supervisor_mapping_table_add_entry_89 >> if_request_permissionsets_present_92
        if_log_getsupervisor_uri_85_blank_88 >> rail.Label(
            'No') >> if_request_permissionsets_present_92
        if_request_supervisorid_not_equals_to_dataworkato_service3cd9c331requestloginname_83 >> rail.Label(
            'No') >> log_supervisor_not_updated >> if_request_permissionsets_present_92
        if_request_supervisorid_present_82 >> rail.Label(
            'No') >> if_request_permissionsets_present_92
        if_request_permissionsets_present_92 >> rail.Label(
            'Yes') >> adhoc_http_action_93 >> log_getnumberofpermissionstobeassigned_94 >> if_log_permissiontobeassigned_100_present_101
        if_log_permissiontobeassigned_100_present_101 >> rail.Label(
            'Yes') >> put_permission_set_assignments_for_user_102 >> update_time_zone_for_user_103
        if_log_permissiontobeassigned_100_present_101 >> rail.Label(
            'No') >> update_time_zone_for_user_103
        if_request_permissionsets_present_92 >> rail.Label(
            'No') >> update_time_zone_for_user_103 >> update_work_week_for_user_104 >> if_request_residentstate_present_105
        if_request_residentstate_present_105 >> rail.Label(
            'Yes') >> log_required_resident_state_110
        log_required_resident_state_110 >> sunovion_mapper_file_search_entries_checkforholidaycalendaronthemapper_111
        sunovion_mapper_file_search_entries_checkforholidaycalendaronthemapper_111 >> if_log_pluckiftheholidaycalendarispresentonthemapper_112_present_113
        if_log_pluckiftheholidaycalendarispresentonthemapper_112_present_113 >> rail.Label(
            'Yes') >> adhoc_http_action_114 >> log_holidaycalendar_uri_115 >> update_holiday_calendar_116 >> if_request_residentstate_present_121
        if_log_pluckiftheholidaycalendarispresentonthemapper_112_present_113 >> rail.Label(
            'No') >> log_holidaycalendar_not_updated >> if_request_residentstate_present_121
        if_request_residentstate_present_105 >> rail.Label(
            'No') >> log_holidaycalendar_not_updated_as_residentstate_blank >> if_request_residentstate_present_121
        if_request_residentstate_present_121 >> rail.Label(
            'Yes') >> adhoc_http_action_122 >> if_log_required_residentstateuri_123_present_124
        if_log_required_residentstateuri_123_present_124 >> rail.Label(
            'Yes') >> put_location_schedule_for_user_resident_state_125 >> if_request_employeetype_present_132
        if_log_required_residentstateuri_123_present_124 >> rail.Label(
            'No') >> adhoc_http_action_127 >> update_namefor_resident_state_128 >> update_codefor_resident_state_129 >> adhoc_http_action_130
        adhoc_http_action_130 >> put_location_schedule_for_user_resident_state_131 >> if_request_employeetype_present_132
        if_request_residentstate_present_121 >> rail.Label(
            'No') >> if_request_employeetype_present_132
        if_request_employeetype_present_132 >> rail.Label(
            'Yes') >> adhoc_http_action_133 >> if_log_required_employeetype_groupuri_134_present_135
        if_log_required_employeetype_groupuri_134_present_135 >> rail.Label(
            'Yes') >> put_division_schedule_for_user_employee_type_136 >> if_employeetype_equal_ca_non_exempt
        if_log_required_employeetype_groupuri_134_present_135 >> rail.Label(
            'No') >> if_employeetype_equal_ca_non_exempt
        if_employeetype_equal_ca_non_exempt >> rail.Label(
            'Yes') >> if_non_exempt_uri_present
        if_non_exempt_uri_present >> rail.Label(
            'Yes') >> put_divisionschedule_for_user_employeetype >> if_request_costcenter_present_137
        if_non_exempt_uri_present >> rail.Label(
            'No') >> if_request_costcenter_present_137
        if_employeetype_equal_ca_non_exempt >> rail.Label(
            'No') >> if_request_costcenter_present_137
        if_request_employeetype_present_132 >> rail.Label(
            'No') >> if_request_costcenter_present_137
        if_request_costcenter_present_137 >> rail.Label(
            'Yes') >> adhoc_http_action_138 >> if_log_required_costcenteruri_139_present_140
        if_log_required_costcenteruri_139_present_140 >> rail.Label(
            'Yes') >> put_cost_center_schedule_for_user_141 >> adhoc_http_action_152
        if_log_required_costcenteruri_139_present_140 >> rail.Label(
            'No') >> if_log_check_cost_center_length_143_less_than_51_144
        if_log_check_cost_center_length_143_less_than_51_144 >> rail.Label(
            'Yes') >> adhoc_http_action_145 >> update_namefor_cost_center_146 >> update_codefor_cost_center_147 >> adhoc_http_action_148
        adhoc_http_action_148 >> put_cost_center_schedule_for_user_149 >> adhoc_http_action_152
        if_log_check_cost_center_length_143_less_than_51_144 >> rail.Label(
            'No') >> log_costcenter_not_added >> adhoc_http_action_152
        if_request_costcenter_present_137 >> rail.Label(
            'No') >> adhoc_http_action_152 >> if_request_employeetype_equals_to_nonexempt_153
        if_request_employeetype_equals_to_nonexempt_153 >> rail.Label(
            'Yes') >> if_log_required_payruleuri_154_present_155
        if_request_employeetype_equals_to_nonexempt_153 >> rail.Label(
            'No') >> if_employeetype_equal_ca_nonexempt
        if_log_required_payruleuri_154_present_155 >> rail.Label(
            'Yes') >> put_pay_rule_script_assignment_schedule_for_user_156 >> if_employeetype_equal_ca_nonexempt
        if_log_required_payruleuri_154_present_155 >> rail.Label(
            'No') >> if_employeetype_equal_ca_nonexempt
        if_employeetype_equal_ca_nonexempt >> rail.Label(
            'Yes') >> if_canonexempturi_present
        if_canonexempturi_present >> rail.Label(
            'Yes') >> put_payrule_script_assignment_schedule_foruser >> if_request_employeetype_equals_to_exempt_157
        if_canonexempturi_present >> rail.Label(
            'No') >> if_request_employeetype_equals_to_exempt_157
        if_employeetype_equal_ca_nonexempt >> rail.Label(
            'No') >> if_request_employeetype_equals_to_exempt_157
        if_request_employeetype_equals_to_exempt_157 >> rail.Label(
            'Yes') >> if_log_required_payruleuri_158_present_159
        if_log_required_payruleuri_158_present_159 >> rail.Label(
            'Yes') >> put_pay_rule_script_assignment_schedule_for_user_160 >> if_request_initialschedulename_present_exempt_161
        if_log_required_payruleuri_158_present_159 >> rail.Label(
            'No') >> if_request_initialschedulename_present_exempt_161
        if_request_employeetype_equals_to_exempt_157 >> rail.Label(
            'No') >> if_request_initialschedulename_present_exempt_161
        if_request_initialschedulename_present_exempt_161 >> rail.Label(
            'Yes') >> adhoc_http_action_162 >> if_log_required_scheduleuri_163_present_164
        if_log_required_scheduleuri_163_present_164 >> rail.Label(
            'Yes') >> put_schedule_policy_schedule_for_user_165 >> adhoc_http_action_166
        if_log_required_scheduleuri_163_present_164 >> rail.Label(
            'No') >> adhoc_http_action_166
        if_request_initialschedulename_present_exempt_161 >> rail.Label(
            'No') >> adhoc_http_action_166 >> if_log_required_customfieldgroupuri_167_present_168
        if_log_required_customfieldgroupuri_167_present_168 >> rail.Label(
            'Yes') >> adhoc_http_action_169 >> if_request_vacationaccrualdate_present_170
        if_request_vacationaccrualdate_present_170 >> rail.Label(
            'Yes') >> if_log_get_required_vacation_accrual_date_u_d_f_uri_171_present_172
        if_log_get_required_vacation_accrual_date_u_d_f_uri_171_present_172 >> rail.Label(
            'Yes') >> log_get_required_vacation_accrual_date_u_d_fday_173 >> update_date_valuefor_vacation_accrual_date_176
        update_date_valuefor_vacation_accrual_date_176 >> if_request_scheduledhoursperweek_present_177
        if_log_get_required_vacation_accrual_date_u_d_f_uri_171_present_172 >> rail.Label(
            'No') >> if_request_scheduledhoursperweek_present_177
        if_request_vacationaccrualdate_present_170 >> rail.Label(
            'No') >> if_request_scheduledhoursperweek_present_177
        if_request_scheduledhoursperweek_present_177 >> rail.Label(
            'Yes') >> if_log_get_required_scheduled_hours_per_week_u_d_f_uri_178_present_179
        if_log_get_required_scheduled_hours_per_week_u_d_f_uri_178_present_179 >> rail.Label(
            'Yes') >> update_text_valuefor_scheduled_hours_per_week_180 >> if_request_workdayemployeetype_present_181
        if_log_get_required_scheduled_hours_per_week_u_d_f_uri_178_present_179 >> rail.Label(
            'No') >> if_request_workdayemployeetype_present_181
        if_request_scheduledhoursperweek_present_177 >> rail.Label(
            'No') >> if_request_workdayemployeetype_present_181
        if_request_workdayemployeetype_present_181 >> rail.Label(
            'Yes') >> if_log_get_required_workday_employee_type_u_d_f_uri_182_present_183
        if_log_get_required_workday_employee_type_u_d_f_uri_182_present_183 >> rail.Label(
            'Yes') >> adhoc_http_action_184 >> if_log_get_required_workday_employee_typedropdown_uri_185_present_186
        if_log_get_required_workday_employee_typedropdown_uri_185_present_186 >> rail.Label(
            'Yes') >> update_dropdown_valuefor_workday_employee_type_187 >> if_request_workdayexecutive_present_188
        if_log_get_required_workday_employee_typedropdown_uri_185_present_186 >> rail.Label(
            'No') >> if_request_workdayexecutive_present_188
        if_log_get_required_workday_employee_type_u_d_f_uri_182_present_183 >> rail.Label(
            'No') >> if_request_workdayexecutive_present_188
        if_request_workdayemployeetype_present_181 >> rail.Label(
            'No') >> if_request_workdayexecutive_present_188
        if_request_workdayexecutive_present_188 >> rail.Label(
            'Yes') >> if_log_get_required_workday_executive_u_d_f_uri_189_present_190
        if_log_get_required_workday_executive_u_d_f_uri_189_present_190 >> rail.Label(
            'Yes') >> adhoc_http_action_191 >> if_log_get_required_workday_executivedropdown_uri_192_present_193
        if_log_get_required_workday_executivedropdown_uri_192_present_193 >> rail.Label(
            'Yes') >> update_dropdown_valuefor_workday_executive_194 >> trigger_child_to_add_timeofftype_for_new_user
        if_log_get_required_workday_executivedropdown_uri_192_present_193 >> rail.Label(
            'No') >> trigger_child_to_add_timeofftype_for_new_user
        if_log_get_required_workday_executive_u_d_f_uri_189_present_190 >> rail.Label(
            'No') >> trigger_child_to_add_timeofftype_for_new_user
        if_request_workdayexecutive_present_188 >> rail.Label(
            'No') >> trigger_child_to_add_timeofftype_for_new_user
        if_log_required_customfieldgroupuri_167_present_168 >> rail.Label(
            'No') >> trigger_child_to_add_timeofftype_for_new_user >> waitfor_child_to_add_timeofftype_for_new_user >> log_user_added_successfully
        log_user_added_successfully >> catch_and_log_error
        if_log_required_paygroupuri_12_present_13 >> rail.Label(
            'No') >> log_user_not_added_as_paygroup_unavailable >> catch_and_log_error
        if_log_pluckifthedepartmentispresent_9_present_10 >> rail.Label(
            'No') >> log_user_not_added_as_department_unavailable >> catch_and_log_error
        if_request_paygroup_present_3 >> rail.Label(
            'No') >> log_user_not_added_as_paygroup_blank >> catch_and_log_error >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
