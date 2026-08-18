
from datetime import timedelta, datetime
from airflow.models import Variable

from frontdoorinc.user_import.utils import custom_methods

import rail



null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'frontdoorinc_user_import_create_supervisor_child_{config.instance}',
        description=f'Frontdoorinc_user_import_create_supervisor_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
            no_task='search_supervisor_users'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_supervisor_users',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        search_supervisor_users = rail.RepliconServiceOperator(
            task_id='search_supervisor_users',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: {
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:employee-id"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
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
                            "text": dag_run.conf['employeeid'],
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        def get_supuri(dag_run):
            result = rail.result('search_supervisor_users')['rows'][0] if rail.result('search_supervisor_users') and rail.result('search_supervisor_users')['rows'] and rail.result('search_supervisor_users')['rows'][0] and rail.result(
                'search_supervisor_users')['rows'][0]['cells'] and rail.result('search_supervisor_users')['rows'][0]['cells'][0] and rail.result('search_supervisor_users')['rows'][0]['cells'][0]['uri'] else null
            if result and result['cells'][3].get('textValue') == dag_run.conf['employeeid']:
                return rail.smartjoin_by_delim(result['cells'][0]['uri'], "")
            return None
        

        log_userexist = rail.PythonOperator(
            task_id='log_userexist',
            python_callable=lambda dag_run: get_supuri(dag_run) if rail.result(
                'search_supervisor_users') and rail.result('search_supervisor_users')['rows'] and rail.result('search_supervisor_users')['rows'][0] else null
        )

        if_log_userexist_present_and_user_disabled_4 = rail.IfOperator(
            task_id='if_log_userexist_present_and_user_disabled_4',
            test=lambda: rail.result('log_userexist') and not rail.result('search_supervisor_users')['rows'][0]['cells'][2]['boolValue'] if rail.result('search_supervisor_users') and rail.result('search_supervisor_users')['rows'] and rail.result('search_supervisor_users')['rows'][0] and rail.result('search_supervisor_users')['rows'][0]['cells'] and rail.result('search_supervisor_users')['rows'][0]['cells'][2] else null,
            yes_task="log_to_sumo",
            no_task="check_if_user_exist_with_same_login_name_11"
        )

        check_if_user_exist_with_same_login_name_11 = rail.PythonOperator(
            task_id = "check_if_user_exist_with_same_login_name_11",
            python_callable =lambda dag_run: custom_methods.is_user_exist_with_same_login_name(
                rail.result('search_supervisor_users'), dag_run.conf['employeeid']
            )
        )

        if_user_exist_with_same_login_name_12 = rail.IfOperator(
            task_id='if_user_exist_with_same_login_name_12',
            test=lambda: rail.result('check_if_user_exist_with_same_login_name_11'),
            yes_task="get_supervisoruri",
            no_task="put_user2_16"
        )

        # This task is to replicate return value in step number 13 in workato
        # so that the parent has the supervisor URI when create_user_child.py and update_child_dag.py dag executes
        # gather_listdata2 and gather_listdata2 respectively
        get_supervisoruri = rail.PythonOperator(
            task_id='get_supervisoruri',
            python_callable=lambda: rail.result('log_userexist')
        )

        put_user2_16 = rail.RepliconServiceOperator(
            task_id='put_user2_16',
            endpoint="/services/ImportService1.svc/PutUser2",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['emailaddress'],
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['firstname'],
                    "lastname": dag_run.conf['lastname'],
                    "emailAddress": dag_run.conf['emailaddress'],
                    "employeeId": dag_run.conf['employeeid'],
                    "department": null,
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": "8 hours/day; Mon-Fri",
                                "officeSchedule": null,
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": null
                        }
                    ],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": {
                            "year": datetime.strptime(dag_run.conf['hiredate'], "%Y-%m-%d").year,
                            "month": datetime.strptime(dag_run.conf['hiredate'], "%Y-%m-%d").month,
                            "day": datetime.strptime(dag_run.conf['hiredate'], "%Y-%m-%d").day,
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
                        "loginName": dag_run.conf['emailaddress'],
                        "SSOName": dag_run.conf['emailaddress'],
                        "password": null
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": [
                        {
                            "uri": null,
                            "name": "Project Resource with Reports"
                        },
                        {
                            "uri": null,
                            "name": "Supervisor"
                        }
                    ],
                    "policySets": [
                        {
                            "uri": null,
                            "name": "Time distribution - FTE"
                        }
                    ],
                    "employeeType": null,
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": {
                        "uri": null,
                        "name": "Frontdoor Approval Path"
                    },
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": [
                        {
                            "customField": {
                                "uri": dag_run.conf['customfielduri_jobprofilecode'],
                                "name": null,
                                "groupUri": null
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": null,
                            "number": dag_run.conf['jobprofilecode']
                        }
                    ],
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
                                "uri": null,
                                "name": "Monthly"
                            },
                            "effectiveDate": null
                        }
                    ],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": [],
                    "displayNameParameter": null
                }
            }
        )

        get_supervisor_uri = rail.PythonOperator(
            task_id='get_supervisor_uri',
            python_callable=lambda: rail.result('put_user2_16')['uri']
        )

        if_request_departmenturi_present_17 = rail.IfOperator(
            task_id='if_request_departmenturi_present_17',
            test='''{{ dag_run.conf.departmenturi | is_truthy }}''',
            yes_task="put_department_group_schedule_for_user_18",
            no_task="if_request_employeetypeuri_present_19",
        )

        put_department_group_schedule_for_user_18 = rail.RepliconServiceOperator(
            task_id='put_department_group_schedule_for_user_18',
            endpoint="/services/DepartmentGroupService1.svc/PutDepartmentGroupScheduleForUser",
            data={
                "userUri": "{{ result('put_user2_16').uri }}",
                "scheduleEntries": [
                    {
                        "departmentGroup": {
                            "uri": "{{ dag_run.conf.departmenturi }}",
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_request_employeetypeuri_present_19 = rail.IfOperator(
            task_id='if_request_employeetypeuri_present_19',
            test='''{{ dag_run.conf.employeetypeuri | is_truthy }}''',
            yes_task="apply_user_modifications2employee_type_group_20",
            no_task="if_request_locationuri_present_21",
        )

        apply_user_modifications2employee_type_group_20 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2employee_type_group_20',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ result('put_user2_16').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications":  {
                    "employeeTypeGroupScheduleToApply": {
                        "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                        "replacementEmployeeTypeGroupSchedule": [
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
                        "updateEmployeeTypeGroupScheduleOverDateRange": null
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_locationuri_present_21 = rail.IfOperator(
            task_id='if_request_locationuri_present_21',
            test='''{{ dag_run.conf.locationuri | is_truthy }}''',
            yes_task="apply_user_modifications2location_22",
            no_task="if_request_costcenterid_present_23",
        )

        apply_user_modifications2location_22 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2location_22',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ result('put_user2_16').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications":  {
                    "locationScheduleToApply": {
                        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                        "replacementLocationSchedule": [
                            {
                                "location": {
                                    "uri": "{{ dag_run.conf.locationuri }}",
                                    "parentUri": null,
                                    "name": null
                                },
                                "effectiveDate": null
                            }
                        ],
                        "updateLocationScheduleOverDateRange": null
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_costcenterid_present_23 = rail.IfOperator(
            task_id='if_request_costcenterid_present_23',
            test='''{{ dag_run.conf.costcenterid | is_truthy }}''',
            yes_task="apply_user_modifications2service_center_schedule_24",
            no_task="if_request_jobprofilename_present_25",
        )

        apply_user_modifications2service_center_schedule_24 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2service_center_schedule_24',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ result('put_user2_16').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications":  {
                    "costCenterScheduleToApply": {
                        "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                        "replacementCostCenterSchedule": [
                            {
                                "costCenter": {
                                    "uri": "{{ dag_run.conf.costcenterid }}",
                                    "parentUri": null,
                                    "name": null
                                },
                                "effectiveDate": null
                            }
                        ],
                        "updateCostCenterScheduleOverDateRange": null
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_jobprofilename_present_25 = rail.IfOperator(
            task_id='if_request_jobprofilename_present_25',
            test='''{{ dag_run.conf.jobprofilename | is_truthy  and dag_run.conf.customfielduri_jobprofilecode | is_truthy }}''',
            yes_task="update_text_value_jobprofilename_26",
            no_task="if_request_hourlyrate_present_27",
        )

        update_text_value_jobprofilename_26 = rail.RepliconServiceOperator(
            task_id='update_text_value_jobprofilename_26',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2_16').uri }}",
                "customFieldUri": "{{ dag_run.conf.customfielduri_jobprofilename }}",
                "value": "{{ dag_run.conf.jobprofilename }}"
            }
        )

        if_request_hourlyrate_present_27 = rail.IfOperator(
            task_id='if_request_hourlyrate_present_27',
            test='''{{ dag_run.conf.hourlyrate | is_truthy }}''',
            yes_task="put_user_payroll_rate_schedule_28",
            no_task="if_request_managerid_present_29",
        )

        put_user_payroll_rate_schedule_28 = rail.RepliconServiceOperator(
            task_id='put_user_payroll_rate_schedule_28',
            endpoint="/services/ResourceService1.svc/PutUserCostRateSchedule",
            data={
                "userUri": "{{ result('put_user2_16').uri }}",
                "schedule": {
                    "initialHourlyRate": {
                        "amount": "{{ dag_run.conf.hourlyrate }}",
                        "currency": {
                            "uri": null,
                            "name": null,
                            "symbol": null
                        }
                    },
                    "scheduleEntries": []
                }
            }

        )

        if_request_managerid_present_29 = rail.IfOperator(
            task_id='if_request_managerid_present_29',
            test='''{{ dag_run.conf.managerid | is_truthy }}''',
            yes_task="search_users_30",
            no_task="add_supervisor_success_entry",
        )

        search_users_30 = rail.RepliconServiceOperator(
            task_id='search_users_30',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: {
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:employee-id"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
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
                            "text": dag_run.conf['managerid'],
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        def get_manageruri(dag_run):
            result = rail.result('search_users_30')['rows'][0] if rail.result('search_users_30') and rail.result('search_users_30')['rows'] and rail.result('search_users_30')['rows'][0] and rail.result(
                'search_users_30')['rows'][0]['cells'] and rail.result('search_users_30')['rows'][0]['cells'][0] and rail.result('search_users_30')['rows'][0]['cells'][0]['uri'] else null
            if result and result['cells'][3].get('textValue') == dag_run.conf['managerid']:
                return rail.smartjoin_by_delim(result['cells'][0]['uri'], "")
            return None

        log_checkifuserexist_31 = rail.PythonOperator(
            task_id='log_checkifuserexist_31',
            python_callable=lambda dag_run: get_manageruri(
                dag_run) if rail.result('search_users_30') and rail.result('search_users_30')['rows'] and rail.result('search_users_30')['rows'][0] else null
        )

        if_log_checkifuserexist_31_present_32 = rail.IfOperator(
            task_id='if_log_checkifuserexist_31_present_32',
            test='''{{ result('log_checkifuserexist_31') | is_truthy }}''',
            yes_task="get_assigned_permission_sets_for_user2_33",
            no_task="catch_42",
        )

        get_assigned_permission_sets_for_user2_33 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_33',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('log_checkifuserexist_31') }}"
            }
        )

        def get_permissionsets():
            record = rail.result('get_assigned_permission_sets_for_user2_33') if rail.result(
                'get_assigned_permission_sets_for_user2_33') else null
            for d in record:
                if d['permissionSet']['name'] == "Supervisor":
                    return d['permissionSet']
            return None

        log_checkifsupervisorpermissionsetisassigned_34 = rail.PythonOperator(
            task_id='log_checkifsupervisorpermissionsetisassigned_34',
            python_callable=get_permissionsets
        )

        if_log_checkifsupervisorpermissionsetisassigned_34_present_35 = rail.IfOperator(
            task_id='if_log_checkifsupervisorpermissionsetisassigned_34_present_35',
            test='''{{ result('log_checkifsupervisorpermissionsetisassigned_34') | is_truthy }}''',
            yes_task="put_supervisor_assignment_schedule_36",
            no_task="if_log_checkifsupervisorpermissionsetisassigned_34_blank_37",
        )

        put_supervisor_assignment_schedule_36 = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment_schedule_36',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ result('put_user2_16').uri }}",
                "initialSupervisorUri": "{{ result('log_checkifuserexist_31') }}",
                "scheduleEntries": []
            }
        )

        if_log_checkifsupervisorpermissionsetisassigned_34_blank_37 = rail.IfOperator(
            task_id='if_log_checkifsupervisorpermissionsetisassigned_34_blank_37',
            test='''{{ result('log_checkifsupervisorpermissionsetisassigned_34') | is_falsy }}''',
            yes_task="get_all_permission_sets_38",
            no_task="add_supervisor_success_entry",
        )

        get_all_permission_sets_38 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_38',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data=None
        )

        log_get_supervisorpermissionuri_39 = rail.PythonOperator(
            task_id='log_get_supervisorpermissionuri_39',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permission_sets_38'), 'name', 'Supervisor', 'uri', '')
        )

        assign_permission_set_to_user_40 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_40',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('log_checkifuserexist_31') }}",
                "permissionSetUri": "{{ result('log_get_supervisorpermissionuri_39') }}"
            }
        )

        put_supervisor_assignment_schedule_41 = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment_schedule_41',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": "{{ result('put_user2_16').uri }}",
                "initialSupervisorUri": "{{ result('log_checkifuserexist_31') }}",
                "scheduleEntries": []
            }
        )

        add_supervisor_success_entry = rail.WriteLogOperator(
            task_id='add_supervisor_success_entry',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="success",
            properties=lambda dag_run: {
                "username": str(dag_run.conf['firstname']) + " " + str(dag_run.conf['lastname']),
                "employeeid": dag_run.conf['employeeid'],
                "action": "add",
                "status": "success",
                "details": "Supervisor created successfully",
                "jobid": dag_run.conf['jobid'],
                "childjob": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        catch_42 = rail.EmptyOperator(
            task_id='catch_42',
            trigger_rule='one_failed',
        )

        frontdoorinc_user_import_logs_add_entry_43 = rail.WriteLogOperator(
            task_id='frontdoorinc_user_import_logs_add_entry_43',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="failed",
            properties=lambda dag_run: {
                "username": str(dag_run.conf['firstname']) + " " + str(dag_run.conf['lastname']),
                "employeeid": dag_run.conf['employeeid'],
                "action": "add",
                "status": "failed",
                "details": rail.render_template("{{ get_error_message() }}"),
                "jobid": dag_run.conf['jobid'],
                "childjob": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> search_supervisor_users
        search_supervisor_users >> log_userexist >> if_log_userexist_present_and_user_disabled_4
        if_log_userexist_present_and_user_disabled_4 >> rail.Label('Yes') >> log_to_sumo
        if_log_userexist_present_and_user_disabled_4 >> rail.Label('No') >> check_if_user_exist_with_same_login_name_11 >> if_user_exist_with_same_login_name_12

        if_user_exist_with_same_login_name_12 >> rail.Label('Yes') >> get_supervisoruri >> log_to_sumo
        if_user_exist_with_same_login_name_12 >> rail.Label('No') >> put_user2_16

        put_user2_16 >> get_supervisor_uri >> if_request_departmenturi_present_17
        if_request_departmenturi_present_17 >> rail.Label(
            'Yes') >> put_department_group_schedule_for_user_18 >> if_request_employeetypeuri_present_19
        if_request_departmenturi_present_17 >> rail.Label(
            'No') >> if_request_employeetypeuri_present_19
        if_request_employeetypeuri_present_19 >> rail.Label(
            'Yes') >> apply_user_modifications2employee_type_group_20 >> if_request_locationuri_present_21
        if_request_employeetypeuri_present_19 >> rail.Label(
            'No') >> if_request_locationuri_present_21
        if_request_locationuri_present_21 >> rail.Label(
            'Yes') >> apply_user_modifications2location_22 >> if_request_costcenterid_present_23
        if_request_locationuri_present_21 >> rail.Label(
            'No') >> if_request_costcenterid_present_23
        if_request_costcenterid_present_23 >> rail.Label(
            'Yes') >> apply_user_modifications2service_center_schedule_24 >> if_request_jobprofilename_present_25
        if_request_costcenterid_present_23 >> rail.Label(
            'No') >> if_request_jobprofilename_present_25
        if_request_jobprofilename_present_25 >> rail.Label(
            'Yes') >> update_text_value_jobprofilename_26 >> if_request_hourlyrate_present_27
        if_request_jobprofilename_present_25 >> rail.Label(
            'No') >> if_request_hourlyrate_present_27
        if_request_hourlyrate_present_27 >> rail.Label(
            'Yes') >> put_user_payroll_rate_schedule_28 >> if_request_managerid_present_29
        if_request_hourlyrate_present_27 >> rail.Label(
            'No') >> if_request_managerid_present_29
        if_request_managerid_present_29 >> rail.Label(
            'Yes') >> search_users_30 >> log_checkifuserexist_31 >> if_log_checkifuserexist_31_present_32
        if_log_checkifuserexist_31_present_32 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user2_33 >> log_checkifsupervisorpermissionsetisassigned_34
        log_checkifsupervisorpermissionsetisassigned_34 >> if_log_checkifsupervisorpermissionsetisassigned_34_present_35
        if_log_checkifsupervisorpermissionsetisassigned_34_present_35 >> rail.Label(
            'Yes') >> put_supervisor_assignment_schedule_36 >> if_log_checkifsupervisorpermissionsetisassigned_34_blank_37
        if_log_checkifsupervisorpermissionsetisassigned_34_present_35 >> rail.Label(
            'No') >> if_log_checkifsupervisorpermissionsetisassigned_34_blank_37
        if_log_checkifsupervisorpermissionsetisassigned_34_blank_37 >> rail.Label(
            'Yes') >> get_all_permission_sets_38 >> log_get_supervisorpermissionuri_39
        log_get_supervisorpermissionuri_39 >> assign_permission_set_to_user_40 >> put_supervisor_assignment_schedule_41
        put_supervisor_assignment_schedule_41 >> add_supervisor_success_entry >> catch_42
        if_log_checkifsupervisorpermissionsetisassigned_34_blank_37 >> rail.Label(
            'No') >> add_supervisor_success_entry >> catch_42
        if_log_checkifuserexist_31_present_32 >> rail.Label('No') >> catch_42
        if_request_managerid_present_29 >> rail.Label(
            'No') >> add_supervisor_success_entry >> catch_42 >> frontdoorinc_user_import_logs_add_entry_43 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
