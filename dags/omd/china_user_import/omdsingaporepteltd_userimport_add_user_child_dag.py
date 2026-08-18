
from datetime import timedelta
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'omdsingaporepteltd_china_user_import_add_user_child_{config.instance}',
        description=f'Omdsingaporepteltd UserImport Add User Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_loginname_not_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_loginname_not_present',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_loginname_not_present=rail.IfOperator(
            task_id='if_loginname_not_present',
            test=lambda dag_run: not bool( dag_run.conf['firstname'] and dag_run.conf['lastname'] and dag_run.conf['loginname'] ),
            yes_task="log_loginname_not_present",
            no_task="create_user",
        )

        log_loginname_not_present=rail.WriteLogOperator(
            task_id='log_loginname_not_present',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity="ignored",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "action": "add",
                "status": "ignored",
                "details": "loginname or firstname or lastname is not present",
                "jobid": "{{dag_run.conf.callerjobid}}",
                "childjobid": "{{dag_run_ecid()}}"
            }
        )

        create_user=rail.RepliconServiceOperator(
            task_id='create_user',
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
                "schedulePolicySchedule": [
                  {
                    "schedulePolicy": {
                      "officeScheduleUri": "{{ dag_run.conf.officescheduleuri }}",
                      "name": null,
                      "officeSchedule": null,
                      "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                  }
                ],
                "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
                "employmentDateRange": null,
                "securityConfiguration": {
                  "enabledAuthenticationTypeUris": [
                    "urn:replicon:user-authentication-type:sso"
                  ],
                  "isLoginEnabled": "true",
                  "loginName": "{{ dag_run.conf.loginname }}",
                  "SSOName": "{{ dag_run.conf.loginname }}",
                  "password": null
                },
                "holidayCalendar": {
                  "uri": null,
                  "name": "China"
                },
                "timeOffPolicy": null,
                "permissionSets": [
                  {
                    "uri": "{{ dag_run.conf.permissionuri }}",
                    "name": null
                  }
                ],
                "policySets": [
                  {
                    "uri": "{{ dag_run.conf.timesheettemplateuri }}",
                    "name": null
                  },
                  {
                    "uri": "{{ dag_run.conf.timeofftemplateuri }}",
                    "name": null
                  }
                ],
                "employeeType": null,
                "timesheetPeriodTypeUri": null,
                "costRateSchedule": null,
                "payrollRateSchedule": null,
                "defaultBillingRate": null,
                "timesheetApprovalPath": {
                  "uri": "{{ dag_run.conf.timesheetapprovalpathuri }}",
                  "name": null
                },
                "expenseApprovalPath": null,
                "timeOffApprovalPath": {
                  "uri": "{{ dag_run.conf.timeoffapprovalpathuri }}",
                  "name": null
                },
                "customFieldValues": [],
                "assignedActivities": [],
                "timeZone": {
                  "uri": "urn:replicon:time-zone:asia-shanghai",
                  "IANAName": null
                },
                "timesheetPeriodSchedule": [
                  {
                    "timesheetPeriod": {
                      "uri": "{{ dag_run.conf.timesheetperioduri }}",
                      "name": null
                    },
                    "effectiveDate": null
                  }
                ],
                "extensionFieldValues": []
              }
            }
        )

        create_exceptionlogger_list=rail.SetVariableOperator(
            task_id='create_exceptionlogger_list',
            append=False,
            name='exceptionlogger',
            value=[]
        )

        if_day_in_startdate_present=rail.IfOperator(
            task_id='if_day_in_startdate_present',
            test='''{{ dag_run.conf.startdate.day | is_truthy }}''',
            yes_task="update_start_date",
            no_task="put_timeoff_type_assignments_for_user",
        )

        update_start_date=rail.RepliconServiceOperator(
            task_id='update_start_date',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
              "userUri": "{{ result('create_user').uri }}",
              "dateRange": {
                "startDate": {
                  "year": "{{ dag_run.conf.startdate.year }}",
                  "month": "{{ dag_run.conf.startdate.month }}",
                  "day": "{{ dag_run.conf.startdate.day }}"
                },
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
              }
            }
        )

        put_timeoff_type_assignments_for_user=rail.RepliconServiceOperator(
            task_id='put_timeoff_type_assignments_for_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
              "userUri": "{{ result('create_user').uri }}",
              "timeOffTypeUris": [
                "{{ dag_run.conf.holidaytimeoffuri }}"
              ]
            }
        )

        is_employeetype_present=rail.IfOperator(
            task_id='is_employeetype_present',
            test='''{{ dag_run.conf.employeetype | is_truthy }}''',
            yes_task="is_employeetype_uri_present",
            no_task="is_department_present",
        )

        is_employeetype_uri_present=rail.IfOperator(
            task_id='is_employeetype_uri_present',
            test='''{{ dag_run.conf.employeetypeuri | is_truthy }}''',
            yes_task="update_employee_type_group",
            no_task="log_employeetype_not_available",
        )

        update_employee_type_group=rail.RepliconServiceOperator(
            task_id='update_employee_type_group',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ result('create_user').uri }}",
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

        log_employeetype_not_available=rail.SetVariableOperator(
            task_id='log_employeetype_not_available',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Employee Type {{ dag_run.conf.employeetype }} not available in Replicon"
            }
        )

        is_department_present=rail.IfOperator(
            task_id='is_department_present',
            test='''{{ dag_run.conf.department | is_truthy }}''',
            yes_task="is_department_uri_present",
            no_task="is_location_present",
        )

        is_department_uri_present=rail.IfOperator(
            task_id='is_department_uri_present',
            test='''{{ dag_run.conf.departmenturi | is_truthy }}''',
            yes_task="update_department_group",
            no_task="log_department_not_available",
        )

        update_department_group=rail.RepliconServiceOperator(
            task_id='update_department_group',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ result('create_user').uri }}",
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

        log_department_not_available=rail.SetVariableOperator(
            task_id='log_department_not_available',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Department {{ dag_run.conf.department }} not available in Replicon"
            }
        )

        is_location_present=rail.IfOperator(
            task_id='is_location_present',
            test='''{{ dag_run.conf.location | is_truthy }}''',
            yes_task="is_location_uri_present",
            no_task="is_division_present",
        )

        is_location_uri_present=rail.IfOperator(
            task_id='is_location_uri_present',
            test='''{{ dag_run.conf.locationuri | is_truthy }}''',
            yes_task="update_location_groups",
            no_task="log_location_not_available",
        )

        update_location_groups=rail.RepliconServiceOperator(
            task_id='update_location_groups',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ result('create_user').uri }}",
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

        log_location_not_available=rail.SetVariableOperator(
            task_id='log_location_not_available',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Location {{ dag_run.conf.location }} not available in Replicon"
            }
        )

        is_division_present=rail.IfOperator(
            task_id='is_division_present',
            test='''{{ dag_run.conf.division | is_truthy }}''',
            yes_task="is_division_uri_present",
            no_task="is_costcenter_present",
        )

        is_division_uri_present=rail.IfOperator(
            task_id='is_division_uri_present',
            test='''{{ dag_run.conf.divisionuri | is_truthy }}''',
            yes_task="update_division_groups",
            no_task="log_division_not_available",
        )

        update_division_groups=rail.RepliconServiceOperator(
            task_id='update_division_groups',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ result('create_user').uri }}",
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
                          "uri": "{{ dag_run.conf.divisionuri }}",
                          "parentUri": null,
                          "name": null
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

        log_division_not_available=rail.SetVariableOperator(
            task_id='log_division_not_available',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Divisiob {{ dag_run.conf.division }} not available in Replicon."
            }
        )

        is_costcenter_present=rail.IfOperator(
            task_id='is_costcenter_present',
            test='''{{ dag_run.conf.costcenter | is_truthy }}''',
            yes_task="is_costcenter_uri_present",
            no_task="is_legalentity_present",
        )

        is_costcenter_uri_present=rail.IfOperator(
            task_id='is_costcenter_uri_present',
            test='''{{ dag_run.conf.costcenteruri | is_truthy }}''',
            yes_task="update_costcenter_groups",
            no_task="log_costcenter_not_available",
        )

        update_costcenter_groups=rail.RepliconServiceOperator(
            task_id='update_costcenter_groups',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ result('create_user').uri }}",
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
                          "uri": "{{ dag_run.conf.costcenteruri }}",
                          "parentUri": null,
                          "name": null
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

        log_costcenter_not_available=rail.SetVariableOperator(
            task_id='log_costcenter_not_available',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Cost Center {{ dag_run.conf.costcenter }} not available in Replicon"
            }
        )

        is_legalentity_present=rail.IfOperator(
            task_id='is_legalentity_present',
            test='''{{ dag_run.conf.legalentity | is_truthy }}''',
            yes_task="is_legalentity_uri_present",
            no_task="is_supervisoremployeeid_present",
        )

        is_legalentity_uri_present=rail.IfOperator(
            task_id='is_legalentity_uri_present',
            test='''{{ dag_run.conf.legalentityuri | is_truthy }}''',
            yes_task="update_legalentity_groups",
            no_task="log_legalentity_not_present",
        )

        update_legalentity_groups=rail.RepliconServiceOperator(
            task_id='update_legalentity_groups',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ result('create_user').uri }}",
                "loginName": null,
                "parameterCorrelationId": null
              },
              "modifications": {
                "serviceCenterScheduleToApply": {
                  "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                  "replacementServiceCenterSchedule": [],
                  "updateServiceCenterScheduleOverDateRange": {
                    "replacementServiceCenterScheduleEntries": [
                      {
                        "serviceCenter": {
                          "uri": "{{ dag_run.conf.legalentityuri }}",
                          "parentUri": null,
                          "name": null
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

        log_legalentity_not_present=rail.SetVariableOperator(
            task_id='log_legalentity_not_present',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Legal Entity {{ dag_run.conf.legalentity }} not present in Replicon"
            }
        )

        is_supervisoremployeeid_present=rail.IfOperator(
            task_id='is_supervisoremployeeid_present',
            test='''{{ dag_run.conf.supervisoremployeeid | is_truthy }}''',
            yes_task="if_employeeid_equals_supervisoremployeeid",
            no_task="add_log_to_lookuptable",
        )

        if_employeeid_equals_supervisoremployeeid=rail.IfOperator(
            task_id='if_employeeid_equals_supervisoremployeeid',
            test='''{{ dag_run.conf.employeeid == dag_run.conf.supervisoremployeeid }}''',
            yes_task="log_supervisor_not_assigned",
            no_task="search_supervisor_user",
        )

        log_supervisor_not_assigned=rail.SetVariableOperator(
            task_id='log_supervisor_not_assigned',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Supervisor not assigned since the user and supervisor are same"
            }
        )

        search_supervisor_user=rail.RepliconServiceOperator(
            task_id='search_supervisor_user',
            endpoint="/services/UserListService1.svc/GetData",
            data={
              "page": "1",
              "pagesize": "100",
              "columnUris": [
                  "urn:replicon:user-list-column:login-name",
                  "urn:replicon:user-list-column:employee-id",
                  "urn:replicon:user-list-column:enabled"
              ],
              "sort": [],
              "filterExpression": {
                  "leftExpression": {
                      "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                  },
                  "operatorUri": "urn:replicon:filter-operator:text-search",
                  "rightExpression": {
                      "value": {
                          "text": "{{dag_run.conf.supervisoremployeeid}}"
                      }
                  }
              }
            }
        )

        if_supervisor_not_found=rail.IfOperator(
            task_id='if_supervisor_not_found',
            test=lambda: bool(len(rail.result('search_supervisor_user')['rows']) < 1),
            yes_task="log_supervisor_not_present",
            no_task="if_supervisor_found",
        )

        log_supervisor_not_present=rail.SetVariableOperator(
            task_id='log_supervisor_not_present',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "supervisor not present in Replicon"
            }
        )

        if_supervisor_found=rail.IfOperator(
            task_id='if_supervisor_found',
            test=lambda: bool(len(rail.result('search_supervisor_user')['rows']) > 0),
            yes_task="get_supervisor_uri",
            no_task="add_log_to_lookuptable",
        )

        get_supervisor_uri=rail.PythonOperator(
            task_id='get_supervisor_uri',
            python_callable= lambda: rail.result('search_supervisor_user')['rows'][0]['cells'][0]['uri']
                              if rail.result('search_supervisor_user')['rows'][0]['cells'][0]['textValue'] else null
        )

        if_supervisor_uri_not_present=rail.IfOperator(
            task_id='if_supervisor_uri_not_present',
            test="{{result('get_supervisor_uri') | is_falsy }}",
            yes_task="log_supervisor_not_available_in_replicon",
            no_task="if_supervisor_user_not_enabled",
        )

        log_supervisor_not_available_in_replicon=rail.SetVariableOperator(
            task_id='log_supervisor_not_available_in_replicon',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Supervisor not present in Replicon or does not have required permission"
            }
        )

        if_supervisor_user_not_enabled=rail.IfOperator(
            task_id='if_supervisor_user_not_enabled',
            test=lambda: bool(rail.result('search_supervisor_user')['rows'][0]['cells'][2]['textValue'] != 'True' if
                          rail.result('search_supervisor_user')['rows'][0]['cells'][0]['textValue'] else null),
            yes_task="log_supervisor_not_enabled",
            no_task="get_assigned_permission_sets_for_user",
        )

        log_supervisor_not_enabled=rail.SetVariableOperator(
            task_id='log_supervisor_not_enabled',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Supervisor profile is not enabled in Replicon"
            }
        )

        get_assigned_permission_sets_for_user=rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
              "userUri": "{{ result('get_supervisor_uri') }}"
            }
        )

        if_supervision_permission_not_present=rail.IfOperator(
            task_id='if_supervision_permission_not_present',
            test=lambda: not bool(rail.find_first_by_attr_and_get_attr(
                  rail.result('get_assigned_permission_sets_for_user'),'policyUri','urn:replicon:policy:supervision',
                  'permissionSet.name',null) if rail.result('get_assigned_permission_sets_for_user') else null),
            yes_task="log_insufficient_permission",
            no_task="assign_supervisor",
        )

        log_insufficient_permission=rail.SetVariableOperator(
            task_id='log_insufficient_permission',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Supervisor does not have required permission assigned"
            }
        )

        assign_supervisor=rail.RepliconServiceOperator(
            task_id='assign_supervisor',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
              "userUri": "{{ result('create_user').uri }}",
              "supervisorUri": "{{ result('get_supervisor_uri') }}",
              "dateRange": null
            }
        )

        add_log_to_lookuptable=rail.WriteLogOperator(
            task_id='add_log_to_lookuptable',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity=lambda: "exception" if rail.get_dag_run_var('exceptionlogger') else "success",
            properties=lambda dag_run:{
              "loginname": dag_run.conf['loginname'],
              "action": "Add",
              "status": "exception" if rail.get_dag_run_var('exceptionlogger') else "success",
              "details": ','.join([ log['log'] for log in rail.get_dag_run_var('exceptionlogger')]) if
                          rail.get_dag_run_var('exceptionlogger') else "successfully created",
              "jobid": dag_run.conf['callerjobid'],
              "childjobid": rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        catch_and_log_error=rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.lookuptable}}",
            trigger_rule='one_failed',
            message="na",
            severity="error",
            properties={
              "loginname": "{{dag_run.conf.loginname}}",
              "action": "Add",
              "status": "error",
              "details": "{{get_error_message()}}",
              "jobid": "{{dag_run.conf.callerjobid}}",
              "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info=lambda dag_run:{
              "employeeid": dag_run.conf['employeeid'],
              "status": "exception" if rail.get_dag_run_var('exceptionlogger') else "success"
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> if_loginname_not_present
        if_loginname_not_present
        if_loginname_not_present >> rail.Label('Yes')  >> log_loginname_not_present >> catch_and_log_error
        if_loginname_not_present >> rail.Label('No') >> create_user >> create_exceptionlogger_list >> if_day_in_startdate_present
        if_day_in_startdate_present >> rail.Label('Yes')  >> update_start_date >> put_timeoff_type_assignments_for_user
        if_day_in_startdate_present >> rail.Label('No') >> put_timeoff_type_assignments_for_user >> is_employeetype_present
        is_employeetype_present >> rail.Label('Yes')  >> is_employeetype_uri_present
        is_employeetype_uri_present >> rail.Label('Yes')  >> update_employee_type_group >> is_department_present
        is_employeetype_uri_present >> rail.Label('No') >> log_employeetype_not_available >> is_department_present
        is_employeetype_present >> rail.Label('No') >> is_department_present
        is_department_present >> rail.Label('Yes')  >> is_department_uri_present
        is_department_uri_present >> rail.Label('Yes')  >> update_department_group >> is_location_present
        is_department_uri_present >> rail.Label('No') >> log_department_not_available >> is_location_present
        is_department_present >> rail.Label('No') >> is_location_present
        is_location_present >> rail.Label('Yes')  >> is_location_uri_present
        is_location_uri_present >> rail.Label('Yes')  >> update_location_groups >> is_division_present
        is_location_uri_present >> rail.Label('No') >> log_location_not_available >> is_division_present
        is_location_present >> rail.Label('No') >> is_division_present
        is_division_present >> rail.Label('Yes')  >> is_division_uri_present
        is_division_uri_present >> rail.Label('Yes')  >> update_division_groups >> is_costcenter_present
        is_division_uri_present >> rail.Label('No') >> log_division_not_available >> is_costcenter_present
        is_division_present >> rail.Label('No') >> is_costcenter_present
        is_costcenter_present >> rail.Label('Yes')  >> is_costcenter_uri_present
        is_costcenter_uri_present >> rail.Label('Yes')  >> update_costcenter_groups >> is_legalentity_present
        is_costcenter_uri_present >> rail.Label('No') >> log_costcenter_not_available >> is_legalentity_present
        is_costcenter_present >> rail.Label('No') >> is_legalentity_present
        is_legalentity_present >> rail.Label('Yes')  >> is_legalentity_uri_present
        is_legalentity_uri_present >> rail.Label('Yes')  >> update_legalentity_groups >> is_supervisoremployeeid_present
        is_legalentity_uri_present >> rail.Label('No') >> log_legalentity_not_present >> is_supervisoremployeeid_present
        is_legalentity_present >> rail.Label('No') >> is_supervisoremployeeid_present
        is_supervisoremployeeid_present >> rail.Label('Yes')  >> if_employeeid_equals_supervisoremployeeid
        if_employeeid_equals_supervisoremployeeid >> rail.Label('Yes')  >> log_supervisor_not_assigned >> add_log_to_lookuptable
        if_employeeid_equals_supervisoremployeeid >> rail.Label('No') >> search_supervisor_user >> if_supervisor_not_found
        if_supervisor_not_found >> rail.Label('Yes')  >> log_supervisor_not_present >> if_supervisor_found
        if_supervisor_not_found >> rail.Label('No') >> if_supervisor_found
        if_supervisor_found >> rail.Label('Yes')  >> get_supervisor_uri >> if_supervisor_uri_not_present
        if_supervisor_uri_not_present >> rail.Label('Yes')  >> log_supervisor_not_available_in_replicon >> add_log_to_lookuptable
        if_supervisor_uri_not_present >> rail.Label('No') >> if_supervisor_user_not_enabled
        if_supervisor_user_not_enabled >> rail.Label('Yes')  >> log_supervisor_not_enabled >> add_log_to_lookuptable
        if_supervisor_user_not_enabled >> rail.Label('No') >> get_assigned_permission_sets_for_user >> if_supervision_permission_not_present
        if_supervision_permission_not_present >> rail.Label('Yes')  >> log_insufficient_permission >> add_log_to_lookuptable
        if_supervision_permission_not_present >> rail.Label('No') >> assign_supervisor >> add_log_to_lookuptable
        if_supervisor_found >> rail.Label('No') >> add_log_to_lookuptable
        is_supervisoremployeeid_present >> rail.Label('No') >> add_log_to_lookuptable >> catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
