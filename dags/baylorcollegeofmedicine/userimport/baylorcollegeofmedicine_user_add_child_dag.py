
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'baylorcollegeofmedicine_user_add_child_{config.instance}',
        description=f'BaylorCollegeOfMedicine User Add V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_user,
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
            no_task='declare_list_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_3 = rail.SetVariableOperator(
            task_id='declare_list_3',
            append=False,
            name='exceptionlogger',
            value=[]
        )

        if_request_places_present_4 = rail.IfOperator(
            task_id='if_request_places_present_4',
            test='''{{ dag_run.conf.places | is_truthy  and dag_run.conf.placeuri | is_falsy }}''',
            yes_task="log_place_not_available_in_replicon",
            no_task="if_split_smart_join_present_conditiontocheckifrequiredfieldsexistin_inputfile_7",
        )

        log_place_not_available_in_replicon = rail.WriteLogOperator(
            task_id='log_place_not_available_in_replicon',
            log="{{ dag_run.conf.userimportlogslookup }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "action": "Add",
                "status": "Skipped",
                "details": 'Place "{{ dag_run.conf.place }}" is not available in Replicon',
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        if_split_smart_join_present_conditiontocheckifrequiredfieldsexistin_inputfile_7 = rail.IfOperator(
            task_id='if_split_smart_join_present_conditiontocheckifrequiredfieldsexistin_inputfile_7',
            test=lambda dag_run: not bool(
                dag_run.conf['firstname'] and dag_run.conf['lastname'] and dag_run.conf['loginname']),
            yes_task="log_firstname_lastname_loginname_not_present",
            no_task="create_user_10",
        )

        log_firstname_lastname_loginname_not_present = rail.WriteLogOperator(
            task_id='log_firstname_lastname_loginname_not_present',
            log="{{ dag_run.conf.userimportlogslookup }}",
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "action": "Add",
                "status": "Skipped",
                "details": rail.smartjoin_by_delim((("" if dag_run.conf['firstname'] else "Employee First Name is not present") + ";" +
                                                    ("" if dag_run.conf['lastname'] else "Employee Last Name is not present") + ";" +
                                                    ("" if dag_run.conf['loginname'] else "Employee loginname is not present")).split(";"), ';'),
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname']
            }
        )

        create_user_10 = rail.RepliconServiceOperator(
            task_id='create_user_10',
            endpoint="/services/ImportService1.svc/PutUser3",
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
                    "department": null,
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": "{{ dag_run.conf.startdate.year }}",
                            "month": "{{ dag_run.conf.startdate.month }}",
                            "day": "{{ dag_run.conf.startdate.day }}"
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "isLoginEnabled": "true",
                        "loginName": "{{ dag_run.conf.loginname }}",
                        "SSOName": "{{ dag_run.conf.loginname }}",
                        "password": null
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": [
                        {
                            "uri": "{{ dag_run.conf.basicpermissionuri }}",
                            "name": null
                        }
                    ],
                    "policySets": [
                        {
                            "uri": "{{ dag_run.conf.timesheettemplateuri }}",
                            "name": null
                        },
                        {
                            "uri": "{{ dag_run.conf.punchentrypolicyuri }}",
                            "name": null
                        }
                    ],
                    "employeeType": null,
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
                    "serviceCenterSchedule": [],
                    "departmentGroupSchedule": [],
                    "employeeTypeGroupSchedule": [],
                    "timesheetPeriodSchedule": [
                        {
                            "timesheetPeriod": {
                                "uri": "{{ dag_run.conf.timesheetperioduri }}",
                                "name": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": [],
                    "displayNameParameter": null,
                    "decimalSeparatorUri": null,
                    "numberGroupSeparatorUri": null,
                    "extensionFieldValues": []
                }
            }
        )

        put_time_off_type_assignments_for_user_removealltimeofftypesassignment_11 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_removealltimeofftypesassignment_11',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user_10').uri }}",
                "timeOffTypeUris": []
            }
        )

        if_request_placeuri_present_12 = rail.IfOperator(
            task_id='if_request_placeuri_present_12',
            test='''{{ dag_run.conf.placeuri | is_truthy }}''',
            yes_task="_adhoc_http_action_13",
            no_task="if_request_employeetypeuri_present_14",
        )

        _adhoc_http_action_13 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_13',
            endpoint="/services/PlaceService1.svc/PutPlaceAssignmentScheduleForUser",
            data={
                "userTarget": {
                    "uri": "{{ result('create_user_10').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "scheduleEntries": [{
                    "effectiveDate": null,
                    "places": [{
                        "uri": "{{ dag_run.conf.placeuri }}",
                        "name": null
                    }]
                }]
            }
        )

        if_request_employeetypeuri_present_14 = rail.IfOperator(
            task_id='if_request_employeetypeuri_present_14',
            test='''{{ dag_run.conf.employeetypeuri | is_truthy }}''',
            yes_task="update_employee_type_group_15",
            no_task="if_request_timeapproveruri_blank_16",
        )

        update_employee_type_group_15 = rail.RepliconServiceOperator(
            task_id='update_employee_type_group_15',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ result('create_user_10').uri }}",
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
                                    "effectiveDate": null
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_timeapproveruri_blank_16 = rail.IfOperator(
            task_id='if_request_timeapproveruri_blank_16',
            test='''{{ dag_run.conf.timeapproveruri | is_falsy }}''',
            yes_task="insert_to_list_17",
            no_task="update_time_approver_assignment_19",
        )

        insert_to_list_17 = rail.SetVariableOperator(
            task_id='insert_to_list_17',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "time approver not assigned since it is not present in the feed file"
            }
        )

        update_time_approver_assignment_19 = rail.RepliconServiceOperator(
            task_id='update_time_approver_assignment_19',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ result('create_user_10').uri }}",
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
                                        "uri": "{{ dag_run.conf.timeapproveruri }}",
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": null
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

        if_request_departmentgroupuri_present_20 = rail.IfOperator(
            task_id='if_request_departmentgroupuri_present_20',
            test='''{{ dag_run.conf.departmentgroupuri | is_truthy }}''',
            yes_task="update_department_group_21",
            no_task="if_request_supervisor_present_22",
        )

        update_department_group_21 = rail.RepliconServiceOperator(
            task_id='update_department_group_21',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ result('create_user_10').uri }}",
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
                                        "uri": "{{ dag_run.conf.departmentgroupuri }}",
                                        "parent": null,
                                        "name": null,
                                        "parameterCorrelationId": null
                                    },
                                    "effectiveDate": null
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "objectExtensionFieldsToApply": []
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_supervisor_present_22 = rail.IfOperator(
            task_id='if_request_supervisor_present_22',
            test='''{{ dag_run.conf.supervisor | is_truthy }}''',
            yes_task="if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_23",
            no_task="if_request_officescheduleuri_blank_37",
        )

        if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_23 = rail.IfOperator(
            task_id='if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_23',
            test='''{{ dag_run.conf.loginname == dag_run.conf.supervisor }}''',
            yes_task="insert_to_list_24",
            no_task="search_users_26",
        )

        insert_to_list_24 = rail.SetVariableOperator(
            task_id='insert_to_list_24',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Supervisor not assigned since the user and supervisor are same"
            }
        )

        def get_supervisor_uri_and_status(response, dag_run):
            users_found = response['rows']
            supervisor = {}
            for user in users_found:
                if user['cells'][0]['textValue'] == dag_run.conf['supervisor']:
                    supervisor = user
                    break
            return {
                'uri': supervisor['cells'][0]['uri'] if supervisor else '',
                'status': supervisor['cells'][1]['textValue'] if supervisor else ''
            }

        search_users_26 = rail.RepliconServiceOperator(
            task_id='search_users_26',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": "{{ dag_run.conf.supervisor }}",
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=get_supervisor_uri_and_status
        )

        if_pluckuri_smart_joinnil_blank_27 = rail.IfOperator(
            task_id='if_pluckuri_smart_joinnil_blank_27',
            test=lambda: not bool(rail.result('search_users_26')['uri']),
            yes_task="add_to_supervisorassignment_queue",
            no_task="if_plucktextvalue_firstnil_not_equals_to_true_30",
        )

        add_to_supervisorassignment_queue = rail.WriteLogOperator(
            task_id='add_to_supervisorassignment_queue',
            log="{{ dag_run.conf.supervisorlookup }}",
            message="na",
            severity="queued",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{ dag_run.conf.loginname }}",
                "useruri": "{{ result('create_user_10').uri }}",
                "supervisorloginname": "{{ dag_run.conf.supervisor }}",
                "action": "Add",
                "childjobid": "{{ dag_run_ecid() }}",
                "status": "queued"
            }
        )

        if_plucktextvalue_firstnil_not_equals_to_true_30 = rail.IfOperator(
            task_id='if_plucktextvalue_firstnil_not_equals_to_true_30',
            test=lambda: rail.result('search_users_26')['status'] != 'True',
            yes_task="add_to_supervisor_assignmentqueue",
            no_task="get_assigned_permission_sets_for_user2_33",
        )

        add_to_supervisor_assignmentqueue = rail.WriteLogOperator(
            task_id='add_to_supervisor_assignmentqueue',
            log="{{ dag_run.conf.supervisorlookup }}",
            message="na",
            severity="queued",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{ dag_run.conf.loginname }}",
                "useruri": "{{ result('create_user_10').uri }}",
                "supervisorloginname": "{{ dag_run.conf.supervisor }}",
                "action": "Add",
                "childjobid": "{{ dag_run_ecid() }}",
                "status": "queued"
            }
        )

        get_assigned_permission_sets_for_user2_33 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_33',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{result('search_users_26').uri}}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.name', '') if response else ''
        )

        if_pluckname_firstnil_blank_34 = rail.IfOperator(
            task_id='if_pluckname_firstnil_blank_34',
            test=lambda: not bool(rail.result(
                'get_assigned_permission_sets_for_user2_33')),
            yes_task="assign_permission_set_to_user_supervisor_35",
            no_task="assign_supervisor_36",
        )

        assign_permission_set_to_user_supervisor_35 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_supervisor_35',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data={
                "userUri": "{{result('search_users_26').uri}}",
                "permissionSetUris": [
                    "{{ dag_run.conf.supervisorpermissionuri }}",
                    "{{ dag_run.conf.basicwithreportpermissionuri }}"
                ]
            }
        )

        assign_supervisor_36 = rail.RepliconServiceOperator(
            task_id='assign_supervisor_36',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user_10').uri }}",
                "supervisorUri": "{{result('search_users_26').uri}}",
                "dateRange": null
            }
        )

        if_request_officescheduleuri_blank_37 = rail.IfOperator(
            task_id='if_request_officescheduleuri_blank_37',
            test='''{{ dag_run.conf.officescheduleuri | is_falsy }}''',
            yes_task="get_default_office_schedule_uri",
            no_task="if_request_officescheduleuri_present_40",
        )

        get_default_office_schedule_uri = rail.RepliconServiceOperator(
            task_id = 'get_default_office_schedule_uri',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response,'displayText','Default Schedule','uri','')
        )

        put_default_office_schedule = rail.RepliconServiceOperator(
            task_id='put_default_office_schedule',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda: {
                "userUri": rail.result('create_user_10')['uri'],
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "officeSchedule": {
                                "officeScheduleUri": rail.result('get_default_office_schedule_uri')
                            },
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        }
                    }
                ]
            }
        )

        insert_to_list_39 = rail.SetVariableOperator(
            task_id='insert_to_list_39',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": '"Default Schedule" is assigned since the  schedule received  "{{ dag_run.conf.officeschedule }}" is not avaiilable in Replicon'
            }
        )

        if_request_officescheduleuri_present_40 = rail.IfOperator(
            task_id='if_request_officescheduleuri_present_40',
            test='''{{ dag_run.conf.officescheduleuri | is_truthy }}''',
            yes_task="put_schedule_policy_schedule_for_user_assignreceivedofficeschedule_41",
            no_task="add_final_log_for_user_created",
        )

        put_schedule_policy_schedule_for_user_assignreceivedofficeschedule_41 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user_assignreceivedofficeschedule_41',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ result('create_user_10').uri }}",
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "officeSchedule": {
                                "officeScheduleUri": "{{ dag_run.conf.officescheduleuri }}"
                            },
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        }
                    }
                ]
            }
        )

        add_final_log_for_user_created = rail.WriteLogOperator(
            task_id='add_final_log_for_user_created',
            log="{{ dag_run.conf.userimportlogslookup }}",
            message="na",
            severity=lambda: "exception" if rail.get_dag_run_var(
                'exceptionlogger') else "Success",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "action": "Add",
                "status": "exception" if rail.get_dag_run_var('exceptionlogger') else "Success",
                "details": "user created ;" + rail.get_dag_run_var('exceptionlogger')[0]['log'] if rail.get_dag_run_var(
                  'exceptionlogger') else "User successfully created",
                "jobid": dag_run.conf['callerjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.userimportlogslookup }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "action": "Add",
                "status": "Error",
                "details": "{{get_error_message()}}",
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> declare_list_3
        declare_list_3 >> if_request_places_present_4
        if_request_places_present_4 >> rail.Label(
            'Yes') >> log_place_not_available_in_replicon >> catch_and_log_error
        if_request_places_present_4 >> rail.Label(
            'No') >> if_split_smart_join_present_conditiontocheckifrequiredfieldsexistin_inputfile_7
        if_split_smart_join_present_conditiontocheckifrequiredfieldsexistin_inputfile_7 >> rail.Label(
            'Yes') >> log_firstname_lastname_loginname_not_present >> catch_and_log_error
        if_split_smart_join_present_conditiontocheckifrequiredfieldsexistin_inputfile_7 >> rail.Label(
            'No') >> create_user_10 >> put_time_off_type_assignments_for_user_removealltimeofftypesassignment_11 >> if_request_placeuri_present_12
        if_request_placeuri_present_12 >> rail.Label(
            'Yes') >> _adhoc_http_action_13 >> if_request_employeetypeuri_present_14
        if_request_placeuri_present_12 >> rail.Label(
            'No') >> if_request_employeetypeuri_present_14
        if_request_employeetypeuri_present_14 >> rail.Label(
            'Yes') >> update_employee_type_group_15 >> if_request_timeapproveruri_blank_16
        if_request_employeetypeuri_present_14 >> rail.Label(
            'No') >> if_request_timeapproveruri_blank_16
        if_request_timeapproveruri_blank_16 >> rail.Label(
            'Yes') >> insert_to_list_17 >> if_request_departmentgroupuri_present_20
        if_request_timeapproveruri_blank_16 >> rail.Label(
            'No') >> update_time_approver_assignment_19 >> if_request_departmentgroupuri_present_20
        if_request_departmentgroupuri_present_20 >> rail.Label(
            'Yes') >> update_department_group_21 >> if_request_supervisor_present_22
        if_request_departmentgroupuri_present_20 >> rail.Label(
            'No') >> if_request_supervisor_present_22
        if_request_supervisor_present_22 >> rail.Label(
            'Yes') >> if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_23
        if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_23 >> rail.Label(
            'Yes') >> insert_to_list_24 >> if_request_officescheduleuri_blank_37
        if_request_loginname_equals_to_dataworkato_servicereceive_requestrequestsupervisor_23 >> rail.Label(
            'No') >> search_users_26 >> if_pluckuri_smart_joinnil_blank_27
        if_pluckuri_smart_joinnil_blank_27 >> rail.Label(
            'Yes') >> add_to_supervisorassignment_queue >> if_request_officescheduleuri_blank_37
        if_pluckuri_smart_joinnil_blank_27 >> rail.Label(
            'No') >> if_plucktextvalue_firstnil_not_equals_to_true_30
        if_plucktextvalue_firstnil_not_equals_to_true_30 >> rail.Label(
            'Yes') >> add_to_supervisor_assignmentqueue >> if_request_officescheduleuri_blank_37
        if_plucktextvalue_firstnil_not_equals_to_true_30 >> rail.Label(
            'No') >> get_assigned_permission_sets_for_user2_33 >> if_pluckname_firstnil_blank_34
        if_pluckname_firstnil_blank_34 >> rail.Label(
            'Yes') >> assign_permission_set_to_user_supervisor_35 >> assign_supervisor_36
        if_pluckname_firstnil_blank_34 >> rail.Label(
            'No') >> assign_supervisor_36 >> if_request_officescheduleuri_blank_37
        if_request_supervisor_present_22 >> rail.Label(
            'No') >> if_request_officescheduleuri_blank_37
        if_request_officescheduleuri_blank_37 >> rail.Label(
            'Yes') >> get_default_office_schedule_uri >> put_default_office_schedule >> insert_to_list_39
        insert_to_list_39 >> if_request_officescheduleuri_present_40
        if_request_officescheduleuri_blank_37 >> rail.Label(
            'No') >> if_request_officescheduleuri_present_40
        if_request_officescheduleuri_present_40 >> rail.Label(
            'Yes') >> put_schedule_policy_schedule_for_user_assignreceivedofficeschedule_41 >> add_final_log_for_user_created
        if_request_officescheduleuri_present_40 >> rail.Label(
            'No') >> add_final_log_for_user_created >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
