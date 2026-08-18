
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'omdsingaporepteltd_china_user_import_update_user_{config.instance}',
        description=f'Omdsingaporepteltd User Update V1.0 {config.instance}',
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
            no_task="bulk_get_users",
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

        bulk_get_users=rail.RepliconServiceOperator(
            task_id='bulk_get_users',
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
            }
        )

        create_exceptionlogger_list=rail.SetVariableOperator(
            task_id='create_exceptionlogger_list',
            append=False,
            name='exceptionlogger',
            value=[]
        )

        def is_userstartdate_notequal_current_startdate(dag_run):
            startDate = rail.result('bulk_get_users')[0]['userDetails']['employmentDateRange']['startDate']
            startdate = str(startDate['day']) + '-' + str(startDate['month']) + '-' + str(startDate['year']) if startDate else null
            return bool(dag_run.conf['userstartdate'] and startdate and
                    datetime.strptime(startdate,"%d-%m-%Y") != datetime.strptime(dag_run.conf['userstartdate'],"%d-%m-%Y"))

        if_startdate_present_and_notequal_current=rail.IfOperator(
            task_id='if_startdate_present_and_notequal_current',
            test=is_userstartdate_notequal_current_startdate,
            yes_task="if_enddate_present_and_notequal_current",
            no_task="is_enddate_present_andnot_eqaul_current",
        )

        def is_enddate_notequal_current(dag_run):
            endDate = rail.result('bulk_get_users')[0]['userDetails']['employmentDateRange']['endDate']
            enddate = str(endDate['day']) + '-' + str(endDate['month']) + '-' + str(endDate['year']) if endDate else null
            return bool(dag_run.conf['enddate']['day'] and (
              (datetime.strptime(enddate,"%d-%m-%Y") if enddate else '') != datetime.strptime(dag_run.conf['userenddate'],"%d-%m-%Y")))

        if_enddate_present_and_notequal_current=rail.IfOperator(
            task_id='if_enddate_present_and_notequal_current',
            test=is_enddate_notequal_current,
            yes_task="update_employment_daterange_with_newenddate",
            no_task="update_employment_date_range_withnew_startdate",
        )

        update_employment_daterange_with_newenddate=rail.RepliconServiceOperator(
            task_id='update_employment_daterange_with_newenddate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run:{
              "userUri": dag_run.conf['useruri'],
              "dateRange": {
                "startDate": rail.result('bulk_get_users')[0]['userDetails']['employmentDateRange']['startDate'],
                "endDate":{
                  "year": dag_run.conf['enddate']['year'],
                  "month": dag_run.conf['enddate']['month'],
                  "day": dag_run.conf['enddate']['day']
                }
                ,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
              }
            }
        )

        update_employment_date_range_withnew_startdate=rail.RepliconServiceOperator(
            task_id='update_employment_date_range_withnew_startdate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
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

        is_enddate_present_andnot_eqaul_current=rail.IfOperator(
            task_id='is_enddate_present_andnot_eqaul_current',
            test=is_enddate_notequal_current,
            yes_task="update_employment_date_range_with_new_enddate",
            no_task="if_employeestatus_equals_no",
        )

        update_employment_date_range_with_new_enddate=rail.RepliconServiceOperator(
            task_id='update_employment_date_range_with_new_enddate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run:{
              "userUri": dag_run.conf['useruri'],
              "dateRange": {
                "startDate": rail.result('bulk_get_users')[0]['userDetails']['employmentDateRange']['startDate'],
                "endDate":{
                  "year": dag_run.conf['enddate']['year'],
                  "month": dag_run.conf['enddate']['month'],
                  "day": dag_run.conf['enddate']['day']
                }
                ,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
              }
            }
        )

        if_employeestatus_equals_no=rail.IfOperator(
            task_id='if_employeestatus_equals_no',
            test=lambda dag_run: dag_run.conf['employeestatus'] and dag_run.conf['employeestatus'].lower() == 'no',
            yes_task="if_login_enabled_equal_true",
            no_task="if_employeestatus_equals_yes",
        )

        if_login_enabled_equal_true=rail.IfOperator(
            task_id='if_login_enabled_equal_true',
            test='''{{ result('bulk_get_users')[0].securityConfiguration.isLoginEnabled | is_truthy }}''',
            yes_task="disable_login",
            no_task="if_employeestatus_equals_yes",
        )

        disable_login=rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
              "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_employeestatus_equals_yes=rail.IfOperator(
            task_id='if_employeestatus_equals_yes',
            test=lambda dag_run: dag_run.conf['employeestatus'] and dag_run.conf['employeestatus'].lower() == 'yes',
            yes_task="if_login_enabled_not_true",
            no_task="if_firstname_not_equal_current",
        )

        if_login_enabled_not_true=rail.IfOperator(
            task_id='if_login_enabled_not_true',
            test='''{{ result('bulk_get_users')[0].securityConfiguration.isLoginEnabled | is_falsy }}''',
            yes_task="renable_user",
            no_task="if_firstname_not_equal_current",
        )

        renable_user=rail.RepliconServiceOperator(
            task_id='renable_user',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
              "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_firstname_not_equal_current=rail.IfOperator(
            task_id='if_firstname_not_equal_current',
            test=lambda dag_run: bool(dag_run.conf['firstname'] and rail.result('bulk_get_users')[0]['userDetails']['firstName'] and
                                  dag_run.conf['firstname'].lower() != rail.result('bulk_get_users')[0]['userDetails']['firstName'].lower()),
            yes_task="update_first_name",
            no_task="if_lastname_not_equal_current",
        )

        update_first_name=rail.RepliconServiceOperator(
            task_id='update_first_name',
            endpoint="/services/userService1.svc/UpdateFirstName",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        if_lastname_not_equal_current=rail.IfOperator(
            task_id='if_lastname_not_equal_current',
            test=lambda dag_run: bool(dag_run.conf['lastname'] and rail.result('bulk_get_users')[0]['userDetails']['lastName'] and
                                  dag_run.conf['lastname'].lower() != rail.result('bulk_get_users')[0]['userDetails']['lastName'].lower()),
            yes_task="update_last_name",
            no_task="if_emailaddress_not_equal_current",
        )

        update_last_name=rail.RepliconServiceOperator(
            task_id='update_last_name',
            endpoint="/services/userService1.svc/UpdateLastName",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        if_emailaddress_not_equal_current=rail.IfOperator(
            task_id='if_emailaddress_not_equal_current',
            test=lambda dag_run: bool(dag_run.conf['emailaddress'] and rail.result('bulk_get_users')[0]['userDetails']['emailAddress'] and
                                  dag_run.conf['emailaddress'].lower() != rail.result('bulk_get_users')[0]['userDetails']['emailAddress'].lower()),
            yes_task="update_emailaddress",
            no_task="if_supervisoremployeeid_present",
        )

        update_emailaddress=rail.RepliconServiceOperator(
            task_id='update_emailaddress',
            endpoint="/services/userService1.svc/UpdateEmail",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        if_supervisoremployeeid_present=rail.IfOperator(
            task_id='if_supervisoremployeeid_present',
            test='''{{ dag_run.conf.supervisoremployeeid | is_truthy }}''',
            yes_task="if_supervisorid_equals_employeeid",
            no_task="get_effective_user_group_membership",
        )

        if_supervisorid_equals_employeeid=rail.IfOperator(
            task_id='if_supervisorid_equals_employeeid',
            test='''{{ dag_run.conf.supervisoremployeeid == dag_run.conf.employeeid }}''',
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

        def get_supervisor_object(response):
            return [ {
                "loginname": row['cells'][0]['textValue'],
                "uri": row['cells'][0]['uri'],
                "employeeid": row['cells'][1]['textValue'],
                "enabled": row['cells'][2]['boolValue'],
                "multicurrencyvalue": len(row['cells'][3]['moneyValue']['multiCurrencyValue']) if row['cells'][3]['moneyValue'] and
                                        row['cells'][3]['moneyValue']['multiCurrencyValue'] else 0
            } for row in response['rows']]

        search_supervisor_user=rail.RepliconServiceOperator(
            task_id='search_supervisor_user',
            endpoint="/services/UserListService1.svc/GetData",
            data={
              "page": "1",
              "pagesize": "100",
              "columnUris": [
                  "urn:replicon:user-list-column:login-name",
                  "urn:replicon:user-list-column:employee-id",
                  "urn:replicon:user-list-column:enabled",
                  "urn:replicon:user-list-column:hourly-cost"
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
            },
            data_handler=get_supervisor_object
        )

        if_supervisor_has_no_multicurrencyvalue=rail.IfOperator(
            task_id='if_supervisor_has_no_multicurrencyvalue',
            test=lambda: rail.result('search_supervisor_user')[0]['multicurrencyvalue'] < 1,
            yes_task="log_supervisor_not_present",
            no_task="if_supervisor_has_multicurrencyvalue",
        )

        log_supervisor_not_present=rail.SetVariableOperator(
            task_id='log_supervisor_not_present',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "supervisor not present in Replicon"
            }
        )

        if_supervisor_has_multicurrencyvalue=rail.IfOperator(
            task_id='if_supervisor_has_multicurrencyvalue',
            test=lambda: rail.result('search_supervisor_user')[0]['multicurrencyvalue'] > 0,
            yes_task="get_supervisor_assignment_details",
            no_task="get_effective_user_group_membership",
        )

        get_supervisor_assignment_details=rail.RepliconServiceOperator(
            task_id='get_supervisor_assignment_details',
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "asOfDate": null
            }
        )

        log_supervisoruri=rail.PythonOperator(
            task_id='log_supervisoruri',
            python_callable= lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('search_supervisor_user'),
                              'employeeid',dag_run.conf['supervisoremployeeid'],'uri','')
        )

        if_supervisor_not_equal_current=rail.IfOperator(
            task_id='if_supervisor_not_equal_current',
            test=lambda: (rail.result('get_supervisor_assignment_details')['supervisor']['uri'] != rail.result('log_supervisoruri')
                          if rail.result('get_supervisor_assignment_details') else False),
            yes_task="get_assigned_permission_sets_for_user",
            no_task="if_supervisor_assignment_details_not_present",
        )

        get_assigned_permission_sets_for_user=rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
              "userUri": "{{ result('log_supervisoruri') }}"
            }
        )

        if_supervisor_permission_present=rail.IfOperator(
            task_id='if_supervisor_permission_present',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(
                          rail.result('get_assigned_permission_sets_for_user'),'policyUri','urn:replicon:policy:supervision',
                          'permissionSet.name',null) if rail.result('get_assigned_permission_sets_for_user')[0]['policyUri'] else False),
            yes_task="update_supervisor_assignment_schedule_over_date_range",
            no_task="if_supervisor_permission_not_present",
        )

        update_supervisor_assignment_schedule_over_date_range=rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "supervisorUri": "{{ result('log_supervisoruri') }}",
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

        if_supervisor_permission_not_present=rail.IfOperator(
            task_id='if_supervisor_permission_not_present',
            test=lambda: not bool(rail.find_first_by_attr_and_get_attr(
                              rail.result('get_assigned_permission_sets_for_user'),'policyUri','urn:replicon:policy:supervision',
                              'permissionSet.name',null) if rail.result('get_assigned_permission_sets_for_user')[0]['policyUri'] else False),
            yes_task="log_insufficient_permissions",
            no_task="if_supervisor_assignment_details_not_present",
        )

        log_insufficient_permissions=rail.SetVariableOperator(
            task_id='log_insufficient_permissions',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "supervisor does not have required permissions"
            }
        )

        if_supervisor_assignment_details_not_present=rail.IfOperator(
            task_id='if_supervisor_assignment_details_not_present',
            test='''{{result('get_supervisor_assignment_details') | is_falsy }}''',
            yes_task="if_supervisoruri_not_present",
            no_task="get_effective_user_group_membership",
        )

        if_supervisoruri_not_present=rail.IfOperator(
            task_id='if_supervisoruri_not_present',
            test="{{result('log_supervisoruri') | is_falsy}}",
            yes_task="logsupervisor_notpresent",
            no_task="get_assigned_permissionsets_foruser",
        )

        logsupervisor_notpresent=rail.SetVariableOperator(
            task_id='logsupervisor_notpresent',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "supervisor not present in Replicon"
            }
        )

        get_assigned_permissionsets_foruser=rail.RepliconServiceOperator(
            task_id='get_assigned_permissionsets_foruser',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
              "userUri": "{{result('log_supervisoruri')}}" 
            }
        )

        is_supervisor_permission_present=rail.IfOperator(
            task_id='is_supervisor_permission_present',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(
                          rail.result('get_assigned_permissionsets_foruser'),'policyUri','urn:replicon:policy:supervision',
                          'permissionSet.name',null) if rail.result('get_assigned_permissionsets_foruser')[0]['policyUri'] else False),
            yes_task="update_supervisor_assignment",
            no_task="is_supervisor_permission_notpresent",
        )

        update_supervisor_assignment=rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "supervisorUri": "{{ result('log_supervisoruri') }}",
              "dateRange": {
                "startDate": null,
                "endDate": null,
                "relativeDateRangeUri": null,
                "relativeDateRangeAsOfDate": null
              }
            }
        )

        is_supervisor_permission_notpresent=rail.IfOperator(
            task_id='is_supervisor_permission_notpresent',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(
                          rail.result('get_assigned_permissionsets_foruser'),'policyUri','urn:replicon:policy:supervision',
                          'permissionSet.name',null) if rail.result('get_assigned_permissionsets_foruser')[0]['policyUri'] else False),
            yes_task="log_not_sufficient_permissions",
            no_task="get_effective_user_group_membership",
        )

        log_not_sufficient_permissions=rail.SetVariableOperator(
            task_id='log_not_sufficient_permissions',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "supervisor does not required permissions"
            }
        )

        get_effective_user_group_membership=rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
              "userUri": "{{ dag_run.conf.useruri }}",
              "dateRange": null
            }
        )

        if_employeetype_present=rail.IfOperator(
            task_id='if_employeetype_present',
            test='''{{ dag_run.conf.employeetype | is_truthy }}''',
            yes_task="if_employeetype_uri_not_present",
            no_task="if_costcenter_present",
        )

        if_employeetype_uri_not_present=rail.IfOperator(
            task_id='if_employeetype_uri_not_present',
            test='''{{ dag_run.conf.employeetypeuri | is_falsy }}''',
            yes_task="log_employeetype_not_available",
            no_task="if_employeetypeuri_not_equal_current",
        )

        log_employeetype_not_available=rail.SetVariableOperator(
            task_id='log_employeetype_not_available',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Employee Type {{ dag_run.conf.employeetype }} not available in Replicon"
            }
        )

        if_employeetypeuri_not_equal_current=rail.IfOperator(
            task_id='if_employeetypeuri_not_equal_current',
            test=lambda dag_run: bool( dag_run.conf['employeetypeuri'] != rail.result(
                                  'get_effective_user_group_membership')['employeeTypes'][0]['employeeType']['employeeType']['uri'] if
                                  rail.result('get_effective_user_group_membership') and
                                  rail.result('get_effective_user_group_membership')['employeeTypes'] and
                                  rail.result('get_effective_user_group_membership')['employeeTypes'][0]['employeeType'] and
                                  rail.result('get_effective_user_group_membership')['employeeTypes'][0]['employeeType']['employeeType']['uri'] else True),
            yes_task="update_employeetype",
            no_task="if_costcenter_present",
        )

        update_employeetype=rail.RepliconServiceOperator(
            task_id='update_employeetype',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ dag_run.conf.useruri }}",
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
                        "effectiveDate": {
                          "year": "{{ dag_run.conf.today.year }}",
                          "month": "{{ dag_run.conf.today.month }}",
                          "day": "{{ dag_run.conf.today.day }}"
                        }
                      }
                    ],
                    "endDate": null
                  }
                }
              },
              "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_costcenter_present=rail.IfOperator(
            task_id='if_costcenter_present',
            test='''{{ dag_run.conf.costcenter | is_truthy }}''',
            yes_task="if_costcenter_uri_not_present",
            no_task="if_department_present",
        )

        if_costcenter_uri_not_present=rail.IfOperator(
            task_id='if_costcenter_uri_not_present',
            test='''{{ dag_run.conf.costcenteruri | is_falsy }}''',
            yes_task="log_costcenter_not_available",
            no_task="if_costcenter_uri_not_equal_current",
        )

        log_costcenter_not_available=rail.SetVariableOperator(
            task_id='log_costcenter_not_available',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Cost Center {{ dag_run.conf.costcenter }} not available in Replicon"
            }
        )

        if_costcenter_uri_not_equal_current=rail.IfOperator(
            task_id='if_costcenter_uri_not_equal_current',
            test=lambda dag_run: bool( dag_run.conf['costcenteruri'] != rail.result(
                                  'get_effective_user_group_membership')['costCenters'][0]['costCenter']['costCenter']['uri'] if
                                  rail.result('get_effective_user_group_membership') and
                                  rail.result('get_effective_user_group_membership')['costCenters'] and
                                  rail.result('get_effective_user_group_membership')['costCenters'][0]['costCenter'] and
                                  rail.result('get_effective_user_group_membership')['costCenters'][0]['costCenter']['costCenter']['uri'] else True),
            yes_task="update_costcenter",
            no_task="if_department_present",
        )

        update_costcenter=rail.RepliconServiceOperator(
            task_id='update_costcenter',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ dag_run.conf.useruri }}",
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
                        "effectiveDate": {
                          "year": "{{ dag_run.conf.today.year }}",
                          "month": "{{ dag_run.conf.today.month }}",
                          "day": "{{ dag_run.conf.today.day }}"
                        }
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

        if_department_present=rail.IfOperator(
            task_id='if_department_present',
            test='''{{ dag_run.conf.department | is_truthy }}''',
            yes_task="if_department_uri_not_present",
            no_task="if_location_present",
        )

        if_department_uri_not_present=rail.IfOperator(
            task_id='if_department_uri_not_present',
            test='''{{ dag_run.conf.departmenturi | is_falsy }}''',
            yes_task="log_department_not_available",
            no_task="if_departmenturi_not_equal_current",
        )

        log_department_not_available=rail.SetVariableOperator(
            task_id='log_department_not_available',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Department {{ dag_run.conf.department }} not available in Replicon"
            }
        )

        if_departmenturi_not_equal_current=rail.IfOperator(
            task_id='if_departmenturi_not_equal_current',
            test=lambda dag_run: bool( dag_run.conf['departmenturi'] != rail.result(
                                  'get_effective_user_group_membership')['departments'][0]['department']['department']['uri'] if
                                  rail.result('get_effective_user_group_membership') and
                                  rail.result('get_effective_user_group_membership')['departments'] and
                                  rail.result('get_effective_user_group_membership')['departments'][0]['department'] and
                                  rail.result('get_effective_user_group_membership')['departments'][0]['department']['department']['uri'] else True),
            yes_task="update_department",
            no_task="if_location_present",
        )

        update_department=rail.RepliconServiceOperator(
            task_id='update_department',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ dag_run.conf.useruri }}",
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
                        "effectiveDate": {
                          "year": "{{ dag_run.conf.today.year }}",
                          "month": "{{ dag_run.conf.today.month }}",
                          "day": "{{ dag_run.conf.today.day }}"
                        }
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

        if_location_present=rail.IfOperator(
            task_id='if_location_present',
            test='''{{ dag_run.conf.location | is_truthy }}''',
            yes_task="if_locationuri_not_present",
            no_task="if_division_present",
        )

        if_locationuri_not_present=rail.IfOperator(
            task_id='if_locationuri_not_present',
            test='''{{ dag_run.conf.locationuri | is_falsy }}''',
            yes_task="log_location_not_present",
            no_task="if_locationuri_not_equal_current",
        )

        log_location_not_present=rail.SetVariableOperator(
            task_id='log_location_not_present',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Location {{ dag_run.conf.location }} not available in Replicon"
            }
        )

        if_locationuri_not_equal_current=rail.IfOperator(
            task_id='if_locationuri_not_equal_current',
            test=lambda dag_run: bool( dag_run.conf['locationuri'] != rail.result(
                                  'get_effective_user_group_membership')['locations'][0]['location']['location']['uri'] if
                                  rail.result('get_effective_user_group_membership') and
                                  rail.result('get_effective_user_group_membership')['locations'] and
                                  rail.result('get_effective_user_group_membership')['locations'][0]['location'] and
                                  rail.result('get_effective_user_group_membership')['locations'][0]['location']['location']['uri'] else True),
            yes_task="update_location",
            no_task="if_division_present",
        )

        update_location=rail.RepliconServiceOperator(
            task_id='update_location',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ dag_run.conf.useruri }}",
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
                        "effectiveDate": {
                          "year": "{{ dag_run.conf.today.year }}",
                          "month": "{{ dag_run.conf.today.month }}",
                          "day": "{{ dag_run.conf.today.day }}"
                        }
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

        if_division_present=rail.IfOperator(
            task_id='if_division_present',
            test='''{{ dag_run.conf.division | is_truthy }}''',
            yes_task="if_divisionuri_not_present",
            no_task="if_legalentity_present",
        )

        if_divisionuri_not_present=rail.IfOperator(
            task_id='if_divisionuri_not_present',
            test='''{{ dag_run.conf.divisionuri | is_falsy }}''',
            yes_task="log_division_not_available",
            no_task="if_dvisionuri_not_equal_current",
        )

        log_division_not_available=rail.SetVariableOperator(
            task_id='log_division_not_available',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Division {{ dag_run.conf.division }} not available in Replicon"
            }
        )

        if_dvisionuri_not_equal_current=rail.IfOperator(
            task_id='if_dvisionuri_not_equal_current',
            test=lambda dag_run: bool( dag_run.conf['divisionuri'] != rail.result(
                                  'get_effective_user_group_membership')['divisions'][0]['division']['division']['uri'] if
                                  rail.result('get_effective_user_group_membership') and
                                  rail.result('get_effective_user_group_membership')['divisions'] and
                                  rail.result('get_effective_user_group_membership')['divisions'][0]['division'] and
                                  rail.result('get_effective_user_group_membership')['divisions'][0]['division']['division']['uri'] else True),
            yes_task="update_division",
            no_task="if_legalentity_present",
        )

        update_division=rail.RepliconServiceOperator(
            task_id='update_division',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ dag_run.conf.useruri }}",
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
                        "effectiveDate": {
                          "year": "{{ dag_run.conf.today.year }}",
                          "month": "{{ dag_run.conf.today.month }}",
                          "day": "{{ dag_run.conf.today.day }}"
                        }
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

        if_legalentity_present=rail.IfOperator(
            task_id='if_legalentity_present',
            test='''{{ dag_run.conf.legalentity | is_truthy }}''',
            yes_task="if_legalentity_uri_not_present",
            no_task="add_log_to_lookuptable",
        )

        if_legalentity_uri_not_present=rail.IfOperator(
            task_id='if_legalentity_uri_not_present',
            test='''{{ dag_run.conf.divisionuri | is_falsy }}''',
            yes_task="log_legalentity_not_available",
            no_task="if_legalentity_uri_not_equal_current",
        )

        log_legalentity_not_available=rail.SetVariableOperator(
            task_id='log_legalentity_not_available',
            append=True,
            name='{{ result("create_exceptionlogger_list").name }}',
            value={
              "log": "Legal Entity{{ dag_run.conf.legalentity }} not available in Replicon"
            }
        )

        if_legalentity_uri_not_equal_current=rail.IfOperator(
            task_id='if_legalentity_uri_not_equal_current',
            test=lambda dag_run: bool( dag_run.conf['legalentityuri'] != rail.result(
                                  'get_effective_user_group_membership')['serviceCenters'][0]['serviceCenter']['serviceCenter']['uri'] if
                                  rail.result('get_effective_user_group_membership') and
                                  rail.result('get_effective_user_group_membership')['serviceCenters'] and
                                  rail.result('get_effective_user_group_membership')['serviceCenters'][0]['serviceCenter'] and
                                  rail.result('get_effective_user_group_membership')['serviceCenters'][0]['serviceCenter']['serviceCenter']['uri'] else True),
            yes_task="update_legalentity",
            no_task="add_log_to_lookuptable",
        )

        update_legalentity=rail.RepliconServiceOperator(
            task_id='update_legalentity',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
              "user": {
                "uri": "{{ dag_run.conf.useruri }}",
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
                        "effectiveDate": {
                          "year": "{{ dag_run.conf.today.year }}",
                          "month": "{{ dag_run.conf.today.month }}",
                          "day": "{{ dag_run.conf.today.day }}"
                        }
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

        add_log_to_lookuptable=rail.WriteLogOperator(
            task_id='add_log_to_lookuptable',
            log="{{ dag_run.conf.lookuptable }}",
            message="na",
            severity=lambda: "exception" if rail.get_dag_run_var('exceptionlogger') else "success",
            properties=lambda dag_run:{
              "loginname": dag_run.conf['loginname'],
              "action": "Update",
              "status": "exception" if rail.get_dag_run_var('exceptionlogger') else "success",
              "details": ','.join([ log['log'] for log in rail.get_dag_run_var('exceptionlogger')]) if
                          rail.get_dag_run_var('exceptionlogger') else "updated successfully",
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
              "action": "Update",
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
              "loginname": dag_run.conf['loginname'],
              "status": "exception" if rail.get_dag_run_var('exceptionlogger') else "success",
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> if_loginname_not_present
        if_loginname_not_present >> rail.Label('Yes') >> log_loginname_not_present >> catch_and_log_error
        if_loginname_not_present >> rail.Label('No') >> bulk_get_users >> create_exceptionlogger_list >> if_startdate_present_and_notequal_current
        if_startdate_present_and_notequal_current >> rail.Label('Yes')  >> if_enddate_present_and_notequal_current
        if_enddate_present_and_notequal_current >> rail.Label('Yes')  >> update_employment_daterange_with_newenddate >> if_employeestatus_equals_no
        if_enddate_present_and_notequal_current >> rail.Label('No') >> update_employment_date_range_withnew_startdate >> if_employeestatus_equals_no
        if_startdate_present_and_notequal_current >> rail.Label('No') >> is_enddate_present_andnot_eqaul_current
        is_enddate_present_andnot_eqaul_current >> rail.Label('Yes')  >> update_employment_date_range_with_new_enddate >> if_employeestatus_equals_no
        is_enddate_present_andnot_eqaul_current >> rail.Label('No') >> if_employeestatus_equals_no
        if_employeestatus_equals_no >> rail.Label('Yes')  >> if_login_enabled_equal_true
        if_login_enabled_equal_true >> rail.Label('Yes')  >> disable_login >> if_employeestatus_equals_yes
        if_login_enabled_equal_true >> rail.Label('No') >> if_employeestatus_equals_yes
        if_employeestatus_equals_no >> rail.Label('No') >> if_employeestatus_equals_yes
        if_employeestatus_equals_yes >> rail.Label('Yes')  >> if_login_enabled_not_true
        if_login_enabled_not_true >> rail.Label('Yes')  >> renable_user >> if_firstname_not_equal_current
        if_login_enabled_not_true >> rail.Label('No') >> if_firstname_not_equal_current
        if_employeestatus_equals_yes >> rail.Label('No') >> if_firstname_not_equal_current
        if_firstname_not_equal_current >> rail.Label('Yes')  >> update_first_name >> if_lastname_not_equal_current
        if_firstname_not_equal_current >> rail.Label('No') >> if_lastname_not_equal_current
        if_lastname_not_equal_current >> rail.Label('Yes')  >> update_last_name >> if_emailaddress_not_equal_current
        if_lastname_not_equal_current >> rail.Label('No') >> if_emailaddress_not_equal_current
        if_emailaddress_not_equal_current >> rail.Label('Yes')  >> update_emailaddress >> if_supervisoremployeeid_present
        if_emailaddress_not_equal_current >> rail.Label('No') >> if_supervisoremployeeid_present
        if_supervisoremployeeid_present >> rail.Label('Yes')  >> if_supervisorid_equals_employeeid
        if_supervisorid_equals_employeeid >> rail.Label('Yes')  >> log_supervisor_not_assigned >> get_effective_user_group_membership
        if_supervisorid_equals_employeeid >> rail.Label('No') >> search_supervisor_user >> if_supervisor_has_no_multicurrencyvalue
        if_supervisor_has_no_multicurrencyvalue >> rail.Label('Yes')  >> log_supervisor_not_present >> if_supervisor_has_multicurrencyvalue
        if_supervisor_has_no_multicurrencyvalue >> rail.Label('No') >> if_supervisor_has_multicurrencyvalue
        if_supervisor_has_multicurrencyvalue >> rail.Label('Yes')  >> get_supervisor_assignment_details >> log_supervisoruri >> if_supervisor_not_equal_current
        if_supervisor_not_equal_current >> rail.Label('Yes')  >> get_assigned_permission_sets_for_user >> if_supervisor_permission_present
        if_supervisor_permission_present >> rail.Label('Yes')  >> update_supervisor_assignment_schedule_over_date_range >> if_supervisor_permission_not_present
        if_supervisor_permission_present >> rail.Label('No') >> if_supervisor_permission_not_present
        if_supervisor_permission_not_present >> rail.Label('Yes')  >> log_insufficient_permissions >> if_supervisor_assignment_details_not_present
        if_supervisor_permission_not_present >> rail.Label('No') >> if_supervisor_assignment_details_not_present
        if_supervisor_not_equal_current >> rail.Label('No') >> if_supervisor_assignment_details_not_present
        if_supervisor_assignment_details_not_present >> rail.Label('Yes')  >> if_supervisoruri_not_present
        if_supervisoruri_not_present >> rail.Label('Yes')  >> logsupervisor_notpresent >> get_effective_user_group_membership
        if_supervisoruri_not_present >> rail.Label('No') >> get_assigned_permissionsets_foruser >> is_supervisor_permission_present
        is_supervisor_permission_present >> rail.Label('Yes')  >> update_supervisor_assignment >> is_supervisor_permission_notpresent
        is_supervisor_permission_present >> rail.Label('No') >> is_supervisor_permission_notpresent
        is_supervisor_permission_notpresent >> rail.Label('Yes')  >> log_not_sufficient_permissions >> get_effective_user_group_membership
        is_supervisor_permission_notpresent >> rail.Label('No') >> get_effective_user_group_membership
        if_supervisor_assignment_details_not_present >> rail.Label('No') >> get_effective_user_group_membership
        if_supervisor_has_multicurrencyvalue >> rail.Label('No') >> get_effective_user_group_membership
        if_supervisoremployeeid_present >> rail.Label('No') >> get_effective_user_group_membership >> if_employeetype_present
        if_employeetype_present >> rail.Label('Yes')  >> if_employeetype_uri_not_present
        if_employeetype_uri_not_present >> rail.Label('Yes')  >> log_employeetype_not_available >> if_costcenter_present
        if_employeetype_uri_not_present >> rail.Label('No') >> if_employeetypeuri_not_equal_current
        if_employeetypeuri_not_equal_current >> rail.Label('Yes')  >> update_employeetype >> if_costcenter_present
        if_employeetypeuri_not_equal_current >> rail.Label('No') >> if_costcenter_present
        if_employeetype_present >> rail.Label('No') >> if_costcenter_present
        if_costcenter_present >> rail.Label('Yes')  >> if_costcenter_uri_not_present
        if_costcenter_uri_not_present >> rail.Label('Yes')  >> log_costcenter_not_available >> if_department_present
        if_costcenter_uri_not_present >> rail.Label('No') >> if_costcenter_uri_not_equal_current
        if_costcenter_uri_not_equal_current >> rail.Label('Yes')  >> update_costcenter >> if_department_present
        if_costcenter_uri_not_equal_current >> rail.Label('No') >> if_department_present
        if_costcenter_present >> rail.Label('No') >> if_department_present
        if_department_present >> rail.Label('Yes')  >> if_department_uri_not_present
        if_department_uri_not_present >> rail.Label('Yes')  >> log_department_not_available >> if_location_present
        if_department_uri_not_present >> rail.Label('No') >> if_departmenturi_not_equal_current
        if_departmenturi_not_equal_current >> rail.Label('Yes')  >> update_department >> if_location_present
        if_departmenturi_not_equal_current >> rail.Label('No') >> if_location_present
        if_department_present >> rail.Label('No') >> if_location_present
        if_location_present >> rail.Label('Yes')  >> if_locationuri_not_present
        if_locationuri_not_present >> rail.Label('Yes')  >> log_location_not_present >> if_division_present
        if_locationuri_not_present >> rail.Label('No') >> if_locationuri_not_equal_current
        if_locationuri_not_equal_current >> rail.Label('Yes')  >> update_location >> if_division_present
        if_locationuri_not_equal_current >> rail.Label('No') >> if_division_present
        if_location_present >> rail.Label('No') >> if_division_present
        if_division_present >> rail.Label('Yes')  >> if_divisionuri_not_present
        if_divisionuri_not_present >> rail.Label('Yes')  >> log_division_not_available >> if_legalentity_present
        if_divisionuri_not_present >> rail.Label('No') >> if_dvisionuri_not_equal_current
        if_dvisionuri_not_equal_current >> rail.Label('Yes')  >> update_division >> if_legalentity_present
        if_dvisionuri_not_equal_current >> rail.Label('No') >> if_legalentity_present
        if_division_present >> rail.Label('No') >> if_legalentity_present
        if_legalentity_present >> rail.Label('Yes')  >> if_legalentity_uri_not_present
        if_legalentity_uri_not_present >> rail.Label('Yes')  >> log_legalentity_not_available >> add_log_to_lookuptable
        if_legalentity_uri_not_present >> rail.Label('No') >> if_legalentity_uri_not_equal_current
        if_legalentity_uri_not_equal_current >> rail.Label('Yes')  >> update_legalentity >> add_log_to_lookuptable
        if_legalentity_uri_not_equal_current >> rail.Label('No') >> add_log_to_lookuptable
        if_legalentity_present >> rail.Label('No') >> add_log_to_lookuptable >> catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
