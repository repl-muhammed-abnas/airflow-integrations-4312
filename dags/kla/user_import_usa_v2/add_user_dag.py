
from datetime import datetime, timedelta
import json
import pytz

from airflow.models import Variable
import rail

from kla.user_import_usa_v2.mapper.time_zone_mapper import time_zone_mapper
from kla.user_import_usa_v2.mapper.general_mapper import general_mapper

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'kla_user_import_usa_add_user_v2_{config.instance}',
        description=f'KLATencor User Sync Add V2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        def get_conf():
            return rail.get_current_context()['dag_run'].conf

        def get_replicon_date(date_str):
            if not date_str:
                return None
            # date format in "2007-12-31 00:00:00.0"
            date = datetime.strptime(date_str.split(" ")[0], '%Y-%m-%d')
            return {
                'year': date.year,
                'month': date.month,
                'day': date.day
            }

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_message_today'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_message_today',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_message_today = rail.PythonOperator(
            task_id='log_message_today',
            python_callable=lambda: {
                'year': datetime.now(tz=pytz.UTC).year,
                'month': datetime.now(tz=pytz.UTC).month,
                'day': datetime.now(tz=pytz.UTC).day,
            }
        )

        log_message_startdate = rail.PythonOperator(
            task_id='log_message_startdate',
            python_callable=lambda: get_replicon_date(get_conf()['startdate'])
        )

        has_empty_values = rail.IfOperator(
            task_id='has_empty_values',
            test="{{ dag_run.conf.employeeid | is_falsy or dag_run.conf.department | is_falsy or dag_run.conf.loginname | is_falsy }}",
            yes_task="add_empty_values_log",
            no_task="get_departmentdata",
        )

        add_empty_values_log = rail.WriteLogOperator(
            task_id='add_empty_values_log',
            log="{{ dag_run.conf.log }}",
            message="na",
            severity="Exception",
            properties=lambda: {
                "loginname": get_conf()['loginname'],
                "action": f"Add User|{get_conf()['employeeid']}",
                "status": "Exception",
                "message": ', '.join(list(filter(bool, [
                    'Login name not assigned' if get_conf()[
                        'loginname'] else None,
                    'Employee ID not assigned' if get_conf()[
                        'employeeid'] else None,
                    'Department not assigned' if get_conf()[
                        'department'] else None
                ])))
            }
        )

        get_departmentdata = rail.RepliconServiceOperator(
            task_id='get_departmentdata',
            endpoint="/services/DepartmentListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:department-list-column:name",
                    "urn:replicon:department-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: list(map(lambda x: {
                "name": x['cells'][0].get('textValue'),
                "uri": x['cells'][0].get('uri'),
            }, data['rows']))

        )

        get_all_employee_type_details = rail.RepliconServiceOperator(
            task_id='get_all_employee_type_details',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        getallpermissionsets = rail.RepliconServiceOperator(
            task_id='getallpermissionsets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        has_no_location_emptype = rail.IfOperator(
            task_id='has_no_location_emptype',
            test="{{ dag_run.conf.location | is_falsy and  dag_run.conf.employeetype | is_falsy }}",
            yes_task="log_message_employeetype_reg_sal",
            no_task="log_message_employeetype_final",
        )

        log_message_employeetype_reg_sal = rail.PythonOperator(
            task_id='log_message_employeetype_reg_sal',
            python_callable=lambda: 'Regular Salary'
        )

        log_message_employeetype_final = rail.PythonOperator(
            task_id='log_message_employeetype_final',
            python_callable=lambda: get_conf()['employeetype'] or rail.result(
                'log_message_employeetype_reg_sal')
        )

        log_message_get_requiredemployeetype_uri = rail.PythonOperator(
            task_id='log_message_get_requiredemployeetype_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_employee_type_details'), 'displayText', rail.result('log_message_employeetype_final'), 'uri')
        )

        get_all_cost_center = rail.RepliconServiceOperator(
            task_id='get_all_cost_center',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
        )

        get_all_policy_setsfor_timesheetsandtimeoffs = rail.RepliconServiceOperator(
            task_id='get_all_policy_setsfor_timesheetsandtimeoffs',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_custom_fielduri = rail.RepliconServiceOperator(
            task_id='get_custom_fielduri',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            }
        )

        has_no_emp_type_uri = rail.IfOperator(
            task_id='has_no_emp_type_uri',
            test="{{ result('log_message_get_requiredemployeetype_uri') | is_falsy }}",
            yes_task="add_invalid_emp_type_log",
            no_task="get_put_user_param",
        )

        add_invalid_emp_type_log = rail.WriteLogOperator(
            task_id='add_invalid_emp_type_log',
            log="{{ dag_run.conf.log }}",
            message="Employee Type not defined for the user",
            severity="Exception",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Add User",
                "status": "Exception",
                "message": "Employee Type not defined for the user"
            }
        )

        get_put_user_param = rail.PythonOperator(
            task_id='get_put_user_param',
            python_callable=lambda: {
                "permissionseturi": rail.find_first_by_attr_and_get_attr(rail.result('getallpermissionsets'), 'name', 'Employee Basic User', 'uri'),
                "timezone": rail.find_first_by_attr_and_get_attr(
                    time_zone_mapper, 'State', get_conf()['location'], 'URI', 'urn:replicon:time-zone:america-los-angeles'),
                "returntoworkdateuri": rail.find_first_by_attr_and_get_attr(rail.result('get_custom_fielduri'), 'displayText', 'Return to Work Date', 'uri'),
                "wfn_id_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_custom_fielduri'), 'displayText', 'WFN ID', 'uri'),
                "lasthiredateuri": rail.find_first_by_attr_and_get_attr(rail.result('get_custom_fielduri'), 'displayText', 'Last Hire Date2', 'uri'),
                "firstdayofleaveuri": rail.find_first_by_attr_and_get_attr(rail.result('get_custom_fielduri'), 'displayText', 'First Day of Leave', 'uri'),
                "lastrecordupdate": rail.find_first_by_attr_and_get_attr(rail.result('get_custom_fielduri'), 'displayText', 'Last Record Update', 'uri'),
                "companycodeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_custom_fielduri'), 'displayText', 'Company Code', 'uri'),
                "companycode": "1000" if get_conf()['company'] == "KLA" else "1010" if get_conf()['company'] == "VLSI" else None,
                "managersupervisoruri": rail.find_first_by_attr_and_get_attr(rail.result('getallpermissionsets'), 'displayText', "Manager's Supervisor", 'uri'),
                "managerbasicuseruri": rail.find_first_by_attr_and_get_attr(rail.result('getallpermissionsets'), 'displayText', "Manager Basic User", 'uri'),
                "scheduleuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_office_schedules'), 'displayText', "8 hours/day; Mon-Fri", 'uri'),
                "departmenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_departmentdata'), 'name', get_conf()['department'], 'uri'),
                "pdrcountryuri": rail.find_first_by_attr_and_get_attr(rail.result('get_custom_fielduri'), 'displayText', 'PDR Country', 'uri')
                if get_conf()['pdrcountry'] else None,
                "costcenteruri":  rail.find_first_by_attr_and_get_attr(rail.result('get_all_cost_center'), 'displayText', get_conf()['costcenter'], 'uri'),
            }
        )

        put_user2 = rail.RepliconServiceOperator(
            task_id='put_user2',
            endpoint="/services/ImportService1.svc/PutUser3",
            data=lambda: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": get_conf()['loginname'],
                        "parameterCorrelationId": null
                    },
                    "firstname": get_conf()['firstname'],
                    "lastname": get_conf()['lastname'],
                    "emailAddress": get_conf()['emailaddress'],
                    "employeeId": get_conf()['employeeid'],
                    "department": {
                        "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_departmentdata'), 'name', get_conf()['department'], 'uri'),
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
                    "employmentDateRange": {
                        "startDate": rail.result('log_message_startdate'),
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:replicon"
                        ],
                        "isLoginEnabled": "true",
                        "loginName": get_conf()['loginname'],
                        "password": "Replicon@123!"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": [{
                        "uri":  rail.find_first_by_attr_and_get_attr(rail.result('getallpermissionsets'), 'name', 'Employee Basic User', 'uri'),
                        "name": null
                    }],
                    "policySets": [],
                    "employeeType": {
                        "uri": null,
                        "name": rail.result('log_message_employeetype_final'),
                    },
                    "timesheetPeriodTypeUri": "urn:replicon:timesheet-period-type:system",
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "customFieldValues": [],
                    "assignedActivities": [],
                    "timeZone": {
                        "uri":  rail.find_first_by_attr_and_get_attr(
                            time_zone_mapper, 'State', get_conf()['location'], 'URI', 'urn:replicon:time-zone:america-los-angeles'),
                        "IANAName": null
                    },
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }}
        )

        set_sso_for_user = rail.RepliconServiceOperator(
            task_id='set_sso_for_user',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('put_user2').uri }}",
                "loginName": "{{ dag_run.conf.loginname }}"
            }
        )

        unassign_all_timeoffs_for_user = rail.RepliconServiceOperator(
            task_id='unassign_all_timeoffs_for_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('put_user2').uri }}",
                "timeOffTypeUris": []
            }
        )

        has_hourly_emp_type = rail.IfOperator(
            task_id='has_hourly_emp_type',
            test="{{ result('log_message_employeetype_final') == 'Regular Hourly' or result('log_message_employeetype_final') == 'Temporary/Interns - Hourly' }}",
            yes_task="has_ca_co_location",
            no_task="search_entries_for_payrule",
        )

        has_ca_co_location = rail.IfOperator(
            task_id='has_ca_co_location',
            test="{{ dag_run.conf.location == 'CA' or dag_run.conf.location == 'CO' }}",
            yes_task="log_message_locationlookupas_ca",
            no_task="log_message_locationlookupas_non_ca",
        )

        log_message_locationlookupas_ca = rail.PythonOperator(
            task_id='log_message_locationlookupas_ca',
            python_callable=lambda:  get_conf()['location']
        )

        log_message_locationlookupas_non_ca = rail.PythonOperator(
            task_id='log_message_locationlookupas_non_ca',
            python_callable=lambda:  'nonca'
        )

        search_entries_for_payrule = rail.PythonOperator(
            task_id='search_entries_for_payrule',
            python_callable=lambda:  next(iter(filter(lambda x: x['lookup'] == "pay rule" and x["Employee type"] == rail.result('log_message_employeetype_final')
                                                      and x['Additional'] == (rail.result('log_message_locationlookupas_ca') or
                                                                              rail.result('log_message_locationlookupas_non_ca') or ''), general_mapper)), {}).get('Value')
        )

        has_payrule_mapper_value = rail.IfOperator(
            task_id='has_payrule_mapper_value',
            test="{{ result('search_entries_for_payrule') | is_truthy }}",
            yes_task="assign_initial_payrule",
            no_task="has_pdrcountry_usa",
        )

        assign_initial_payrule = rail.RepliconServiceOperator(
            task_id='assign_initial_payrule',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('put_user2').uri }}",
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
                    "userDetailsToApply": null,
                    "payRulesToApply": {
                        "initialPayRule": {
                            "uri": null,
                            "name": "{{ result('search_entries_for_payrule') }}",
                        },
                        "scheduleEntries": []
                    },
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        has_pdrcountry_usa = rail.IfOperator(
            task_id='has_pdrcountry_usa',
            test="{{  dag_run.conf.pdrcountry == 'USA' }}",
            yes_task="has_salary_emptype",
            no_task="log_message_finalvaluefor_timesheet_templatename",
        )

        has_salary_emptype = rail.IfOperator(
            task_id='has_salary_emptype',
            test="{{  result('log_message_employeetype_final').endswith('Salary') }}",
            yes_task="log_message_timesheettemplate_to_assign_sal",
            no_task="has_hourly_ca_type",
        )

        log_message_timesheettemplate_to_assign_sal = rail.PythonOperator(
            task_id='log_message_timesheettemplate_to_assign_sal',
            python_callable=lambda:  'Salary'
        )

        has_hourly_ca_type = rail.IfOperator(
            task_id='has_hourly_ca_type',
            test="{{  result('log_message_employeetype_final').endswith('Hourly') and  dag_run.conf.location  == 'CA' }}",
            yes_task="log_message_timesheettemplatetoassign_ca_hourly",
            no_task="has_hourly_non_ca_type",
        )

        log_message_timesheettemplatetoassign_ca_hourly = rail.PythonOperator(
            task_id='log_message_timesheettemplatetoassign_ca_hourly',
            python_callable=lambda:  'Hourly California'
        )

        has_hourly_non_ca_type = rail.IfOperator(
            task_id='has_hourly_non_ca_type',
            test="{{  result('log_message_employeetype_final').endswith('Hourly') and  dag_run.conf.location  != 'CA' }}",
            yes_task="log_message_timesheettemplatetoassign_nonca_hr",
            no_task="log_message_finalvaluefor_timesheet_templatename",
        )

        log_message_timesheettemplatetoassign_nonca_hr = rail.PythonOperator(
            task_id='log_message_timesheettemplatetoassign_nonca_hr',
            python_callable=lambda: 'Hourly NonCA'
        )

        log_message_finalvaluefor_timesheet_templatename = rail.PythonOperator(
            task_id='log_message_finalvaluefor_timesheet_templatename',
            python_callable=lambda: rail.result('log_message_timesheettemplate_to_assign_sal') or rail.result(
                'log_message_timesheettemplatetoassign_ca_hourly') or rail.result('log_message_timesheettemplatetoassign_nonca_hr')
        )

        log_message_gettherequiredtimesheettemplate_uri = rail.PythonOperator(
            task_id='log_message_gettherequiredtimesheettemplate_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                get_all_policy_setsfor_timesheetsandtimeoffs.task_id), 'displayText', rail.result(log_message_finalvaluefor_timesheet_templatename.task_id), 'uri')
        )

        has_usa_salary_emptype = rail.IfOperator(
            task_id='has_usa_salary_emptype',
            test="{{ result('log_message_employeetype_final').endswith('Salary') and  dag_run.conf.pdrcountry == 'USA' }}",
            yes_task="log_message_timeofftemplatetoassign_timeoffsal",
            no_task="has_usa_hourly_emptype",
        )

        log_message_timeofftemplatetoassign_timeoffsal = rail.PythonOperator(
            task_id='log_message_timeofftemplatetoassign_timeoffsal',
            python_callable=lambda:  'Time Off-Salary'
        )

        has_usa_hourly_emptype = rail.IfOperator(
            task_id='has_usa_hourly_emptype',
            test="{{ result('log_message_employeetype_final').endswith('Hourly') and  dag_run.conf.pdrcountry == 'USA' }}",
            yes_task="log_message_timeofftemplatetoassign_timeoffhrly",
            no_task="log_message_checkifthevalueispresentfortimeofftemplate",
        )

        log_message_timeofftemplatetoassign_timeoffhrly = rail.PythonOperator(
            task_id='log_message_timeofftemplatetoassign_timeoffhrly',
            python_callable=lambda: 'Time Off-Hourly'
        )

        log_message_checkifthevalueispresentfortimeofftemplate = rail.PythonOperator(
            task_id='log_message_checkifthevalueispresentfortimeofftemplate',
            python_callable=lambda: rail.result('log_message_timeofftemplatetoassign_timeoffsal') or rail.result(
                'log_message_timeofftemplatetoassign_timeoffhrly')
        )

        log_message_gettherequiredtimeofftemplate_uri = rail.PythonOperator(
            task_id='log_message_gettherequiredtimeofftemplate_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_setsfor_timesheetsandtimeoffs'),
                                                                         'displayText', rail.result('log_message_checkifthevalueispresentfortimeofftemplate'), 'uri')
        )

        has_ca_hrly_emptype = rail.IfOperator(
            task_id='has_ca_hrly_emptype',
            test="{{ result('log_message_employeetype_final').endswith('Hourly') and  dag_run.conf.location == 'CA' }}",
            yes_task="log_message_punchpolicytoassign_alldevice",
            no_task="has_nonca_hrly_emptype",
        )

        log_message_punchpolicytoassign_alldevice = rail.PythonOperator(
            task_id='log_message_punchpolicytoassign_alldevice',
            python_callable=lambda:  'All Devices – CA'
        )

        has_nonca_hrly_emptype = rail.IfOperator(
            task_id='has_nonca_hrly_emptype',
            test="{{ result('log_message_employeetype_final').endswith('Hourly') and  dag_run.conf.location != 'CA' }}",
            yes_task="log_message_punchpolicy_nonca",
            no_task="log_message_finalvaluefor_punchentrypolicyname",
        )

        log_message_punchpolicy_nonca = rail.PythonOperator(
            task_id='log_message_punchpolicy_nonca',
            python_callable=lambda: 'All Devices – Non CA'
        )

        log_message_finalvaluefor_punchentrypolicyname = rail.PythonOperator(
            task_id='log_message_finalvaluefor_punchentrypolicyname',
            python_callable=lambda: rail.result(
                'log_message_punchpolicytoassign_alldevice') or rail.result('log_message_punchpolicy_nonca')
        )

        log_message_gettherequired_punch_entry_policy_uri = rail.PythonOperator(
            task_id='log_message_gettherequired_punch_entry_policy_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_setsfor_timesheetsandtimeoffs'),
                                                                         'displayText', rail.result('log_message_finalvaluefor_punchentrypolicyname'), 'uri')
        )

        log_message_required_policysetstobeassigned_final = rail.PythonOperator(
            task_id='log_message_required_policysetstobeassigned_final',
            python_callable=lambda: list(filter(bool, [
                rail.result('log_message_gettherequiredtimesheettemplate_uri'),
                rail.result('log_message_gettherequiredtimeofftemplate_uri'),
                rail.result(
                    'log_message_gettherequired_punch_entry_policy_uri')
            ]))

        )

        has_policysetstobeassigned = rail.IfOperator(
            task_id='has_policysetstobeassigned',
            test="{{ result('log_message_required_policysetstobeassigned_final') | is_truthy }}",
            yes_task="update_templates_foruser",
            no_task="update_blank_templatesforuser",
        )

        update_templates_foruser = rail.RepliconServiceOperator(
            task_id='update_templates_foruser',
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('put_user2')['uri'],
                "policySetUris": rail.result('log_message_required_policysetstobeassigned_final')
            }
        )

        update_blank_templatesforuser = rail.RepliconServiceOperator(
            task_id='update_blank_templatesforuser',
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data={
                "userUri": "{{ result('put_user2').uri }}",
                "policySetUris": []
            }
        )

        has_usa_sal_emptype = rail.IfOperator(
            task_id='has_usa_sal_emptype',
            test="{{ result('log_message_employeetype_final').endswith('Salary') and  dag_run.conf.pdrcountry == 'USA' }}",
            yes_task="log_message_timesheetapporvalpath_system",
            no_task="has_usa_hrly_emptype",
        )

        log_message_timesheetapporvalpath_system = rail.PythonOperator(
            task_id='log_message_timesheetapporvalpath_system',
            python_callable=lambda: 'System Approved'
        )

        has_usa_hrly_emptype = rail.IfOperator(
            task_id='has_usa_hrly_emptype',
            test="{{ result('log_message_employeetype_final').endswith('Hourly') and  dag_run.conf.pdrcountry == 'USA' }}",
            yes_task="log_message_timesheetapprovalpath_supervisor",
            no_task="log_message_finalvaluefortimesheetapprovalpath",
        )

        log_message_timesheetapprovalpath_supervisor = rail.PythonOperator(
            task_id='log_message_timesheetapprovalpath_supervisor',
            python_callable=lambda: 'Supervisor Approved'
        )

        log_message_finalvaluefortimesheetapprovalpath = rail.PythonOperator(
            task_id='log_message_finalvaluefortimesheetapprovalpath',
            python_callable=lambda: rail.result('log_message_timesheetapporvalpath_system') or rail.result(
                'log_message_timesheetapprovalpath_supervisor')
        )

        get_all_timesheet_approval_path = rail.RepliconServiceOperator(
            task_id='get_all_timesheet_approval_path',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
        )

        log_message_gettherequiredtimesheetapprovalpath_uri = rail.PythonOperator(
            task_id='log_message_gettherequiredtimesheetapprovalpath_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_all_timesheet_approval_path'),
                                                                         'displayText', rail.result('log_message_finalvaluefortimesheetapprovalpath'), 'uri')
        )

        has_timesheet_path_uri = rail.IfOperator(
            task_id='has_timesheet_path_uri',
            test="{{ result('log_message_gettherequiredtimesheetapprovalpath_uri') | is_truthy }}",
            yes_task="update_timesheet_approval_path_for_user",
            no_task="has_usa_salary_emptype2",
        )

        update_timesheet_approval_path_for_user = rail.RepliconServiceOperator(
            task_id='update_timesheet_approval_path_for_user',
            endpoint="/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{ result('put_user2').uri }}",
                "approvalPathUri": "{{ result('log_message_gettherequiredtimesheetapprovalpath_uri') }}"
            }
        )

        has_usa_salary_emptype2 = rail.IfOperator(
            task_id='has_usa_salary_emptype2',
            test="{{ dag_run.conf.pdrcountry == 'USA' and result('log_message_employeetype_final').endswith('Salary') }}",
            yes_task="log_message_timeoffapprovalpath2_timoffsal",
            no_task="has_usa_hrly_emptype2",
        )

        log_message_timeoffapprovalpath2_timoffsal = rail.PythonOperator(
            task_id='log_message_timeoffapprovalpath2_timoffsal',
            python_callable=lambda:  'Time Off - Salary'
        )

        has_usa_hrly_emptype2 = rail.IfOperator(
            task_id='has_usa_hrly_emptype2',
            test="{{ dag_run.conf.pdrcountry == 'USA' and result('log_message_employeetype_final').endswith('Hourly') }}",
            yes_task="log_message_timeoffapprovalpath2_timeoffhrly",
            no_task="log_message_finalvaluefor_time_offapprovalpath2",
        )

        log_message_timeoffapprovalpath2_timeoffhrly = rail.PythonOperator(
            task_id='log_message_timeoffapprovalpath2_timeoffhrly',
            python_callable=lambda:  'Time Off - Hourly'
        )

        log_message_finalvaluefor_time_offapprovalpath2 = rail.PythonOperator(
            task_id='log_message_finalvaluefor_time_offapprovalpath2',
            python_callable=lambda: rail.result('log_message_timeoffapprovalpath2_timoffsal') or rail.result(
                'log_message_timeoffapprovalpath2_timeoffhrly')
        )

        get_all_time_off_approval_path2 = rail.RepliconServiceOperator(
            task_id='get_all_time_off_approval_path2',
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths",
        )

        log_message_gettherequiredtimeoffapprovalpath_uri2 = rail.PythonOperator(
            task_id='log_message_gettherequiredtimeoffapprovalpath_uri2',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_approval_path2'), 'displayText',
                                                                         rail.result('log_message_finalvaluefor_time_offapprovalpath2'), 'uri')
        )

        has_approvalpath_uri2 = rail.IfOperator(
            task_id='has_approvalpath_uri2',
            test="{{ result('log_message_gettherequiredtimeoffapprovalpath_uri2') | is_truthy }}",
            yes_task="update_timeoff_approval_path_for_user2",
            no_task="log_message_timesheet_period_uri_system",
        )

        update_timeoff_approval_path_for_user2 = rail.RepliconServiceOperator(
            task_id='update_timeoff_approval_path_for_user2',
            endpoint="/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{ result('put_user2').uri }}",
                "approvalPathUri": "{{ result('log_message_gettherequiredtimeoffapprovalpath_uri2') }}"
            }
        )

        log_message_timesheet_period_uri_system = rail.PythonOperator(
            task_id='log_message_timesheet_period_uri_system',
            python_callable=lambda:  'urn:replicon:timesheet-period-type:system'
        )

        update_timesheet_period_type_for_user_assign_system_timesheet_period = rail.RepliconServiceOperator(
            task_id='update_timesheet_period_type_for_user_assign_system_timesheet_period',
            endpoint="/services/TimesheetPeriodService1.svc/UpdateTimesheetPeriodTypeForUser",
            data={
                "userUri": "{{ result('put_user2').uri }}",
                "timesheetPeriodTypeUri": "{{ result('log_message_timesheet_period_uri_system') }}"
            }
        )

        has_udf_returnto_work_date_uri = rail.IfOperator(
            task_id='has_udf_returnto_work_date_uri',
            test="{{ result('get_put_user_param').returntoworkdateuri | is_truthy }}",
            yes_task="update_returnto_work_date_udf",
            no_task="has_wfn_id_uri",
        )

        update_returnto_work_date_udf = rail.RepliconServiceOperator(
            task_id='update_returnto_work_date_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2').uri }}",
                "customFieldUri": "{{ result('get_put_user_param').returntoworkdateuri }}",
                "value": "{{ dag_run.conf.returntoworkdate | sn }}"
            }
        )

        has_wfn_id_uri = rail.IfOperator(
            task_id='has_wfn_id_uri',
            test="{{ result('get_put_user_param').wfn_id_uri | is_truthy }}",
            yes_task="update_wfn_id_udf",
            no_task="has_firstdayofleave_udfuri",
        )

        update_wfn_id_udf = rail.RepliconServiceOperator(
            task_id='update_wfn_id_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2').uri }}",
                "customFieldUri": "{{ result('get_put_user_param').wfn_id_uri }}",
                "value": "{{ dag_run.conf.wfn_id | sn }}"
            }
        )

        has_firstdayofleave_udfuri = rail.IfOperator(
            task_id='has_firstdayofleave_udfuri',
            test="{{ result('get_put_user_param').firstdayofleaveuri | is_truthy }}",
            yes_task="update_first_dayof_leave_udf",
            no_task="has_lasthiredateuri_udf",
        )

        update_first_dayof_leave_udf = rail.RepliconServiceOperator(
            task_id='update_first_dayof_leave_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2').uri }}",
                "customFieldUri": "{{ result('get_put_user_param').firstdayofleaveuri }}",
                "value": "{{ dag_run.conf.firstdayofleave | sn }}"
            }
        )

        has_lasthiredateuri_udf = rail.IfOperator(
            task_id='has_lasthiredateuri_udf',
            test="{{ result('get_put_user_param').lasthiredateuri | is_truthy }}",
            yes_task="update_last_hire_date2_udf",
            no_task="has_lastrecordupdate_udfuri",
        )

        update_last_hire_date2_udf = rail.RepliconServiceOperator(
            task_id='update_last_hire_date2_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2').uri }}",
                "customFieldUri": "{{ result('get_put_user_param').lasthiredateuri }}",
                "value": "{{ dag_run.conf.lasthiredate2 | sn }}"
            }
        )

        has_lastrecordupdate_udfuri = rail.IfOperator(
            task_id='has_lastrecordupdate_udfuri',
            test="{{ result('get_put_user_param').lastrecordupdate | is_truthy }}",
            yes_task="update_last_record_update_udf",
            no_task="has_companycode_udfuri",
        )

        update_last_record_update_udf = rail.RepliconServiceOperator(
            task_id='update_last_record_update_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('put_user2').uri }}",
                "customFieldUri": "{{ result('get_put_user_param').lastrecordupdate }}",
                "value": "{{ dag_run.conf.lastrecordupdate | sn }}"
            }
        )

        has_companycode_udfuri = rail.IfOperator(
            task_id='has_companycode_udfuri',
            test="{{ result('get_put_user_param').companycode | is_truthy }}",
            yes_task="get_enabled_custom_field_drop_down_optionsfor_company_code",
            no_task="log_message_permissionfor_manager_supervisor",
        )

        get_enabled_custom_field_drop_down_optionsfor_company_code = rail.RepliconServiceOperator(
            task_id='get_enabled_custom_field_drop_down_optionsfor_company_code',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_put_user_param').companycodeuri }}"
            }
        )

        has_drop_down_country_code_uri = rail.IfOperator(
            task_id='has_drop_down_country_code_uri',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_custom_field_drop_down_optionsfor_company_code'), 'displayText',
                                                                   rail.result('get_put_user_param')['companycode'], 'uri')),
            yes_task="update_company_code_udf",
            no_task="has_pdr_country_uri",
        )

        update_company_code_udf = rail.RepliconServiceOperator(
            task_id='update_company_code_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ result('put_user2').uri }}",
                "customFieldUri": "{{ result('get_put_user_param').companycodeuri }}",
                "customFieldDropDownOptionUri": "{{ result('get_enabled_custom_field_drop_down_optionsfor_company_code') | find_first_by_attr_and_get_attr('displayText',result('get_put_user_param').companycode,'uri') }}",
            }
        )

        has_pdr_country_uri = rail.IfOperator(
            task_id='has_pdr_country_uri',
            test="{{ result('get_put_user_param').pdrcountryuri | is_truthy }}",
            yes_task="get_enabled_custom_field_drop_down_optionsfor_company_code2",
            no_task="log_message_permissionfor_manager_supervisor",
        )

        get_enabled_custom_field_drop_down_optionsfor_company_code2 = rail.RepliconServiceOperator(
            task_id='get_enabled_custom_field_drop_down_optionsfor_company_code2',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_put_user_param').pdrcountryuri }}"
            }
        )

        has_valid_pdr_country_uri = rail.IfOperator(
            task_id='has_valid_pdr_country_uri',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_custom_field_drop_down_optionsfor_company_code2'),
                                                                   'displayText', 'USA' if get_conf()['pdrcountry'] and 'USA' in get_conf()['pdrcountry'] else
                                                                   'Non-USA' if get_conf()['pdrcountry']
                                                                   else None, 'uri')),
            yes_task="update_pdr_country_udf",
            no_task="log_message_permissionfor_manager_supervisor",
        )

        update_pdr_country_udf = rail.RepliconServiceOperator(
            task_id='update_pdr_country_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda: {
                "objectUri": rail.result('put_user2')['uri'],
                "customFieldUri": rail.result('get_put_user_param')['pdrcountryuri'],
                "customFieldDropDownOptionUri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_custom_field_drop_down_optionsfor_company_code2'),
                                                                                     'displayText', 'USA' if get_conf()['pdrcountry'] and 'USA' in get_conf()['pdrcountry'] else
                                                                                     'Non-USA' if get_conf()['pdrcountry']
                                                                                     else None, 'uri')
            }
        )

        log_message_permissionfor_manager_supervisor = rail.PythonOperator(
            task_id='log_message_permissionfor_manager_supervisor',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(getallpermissionsets.task_id),
                                                                         'displayText', "Manager's Supervisor", 'uri')
        )

        log_message_permissionfor_manager_basic_user = rail.PythonOperator(
            task_id='log_message_permissionfor_manager_basic_user',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(getallpermissionsets.task_id),
                                                                         'displayText', "Manager Basic User", 'uri')
        )

        has_supervisorid = rail.IfOperator(
            task_id='has_supervisorid',
            test="{{ dag_run.conf.supervisorid | is_truthy }}",
            yes_task="searchuser_supervisor",
            no_task="has_us_directreport_yes",
        )

        searchuser_supervisor = rail.RepliconServiceOperator(
            task_id='searchuser_supervisor',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
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
                            "text": "{{ dag_run.conf.supervisorid }}",
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
            data_handler=lambda data: next(iter(filter(lambda x: x['employeeid'] == get_conf()['supervisorid'],
                                                       map(lambda x: {
                                                           "employeeid": x['cells'][2].get('textValue'),
                                                           "uri": x['cells'][0].get('uri'),
                                                           "status": x['cells'][1].get('textValue'),
                                                       }, data['rows']))), None)
        )

        has_supervisor_uri_changed = rail.IfOperator(
            task_id='has_supervisor_uri_changed',
            test="{{ result('searchuser_supervisor') | is_truthy and result('searchuser_supervisor').uri != result('put_user2').uri }}",
            yes_task="can_update_supervisor",
            no_task="has_no_supervisor_found",
        )

        can_update_supervisor = rail.IfOperator(
            task_id='can_update_supervisor',
            test="{{ result('searchuser_supervisor').uri | is_truthy and result('searchuser_supervisor').status == 'True' }}",
            yes_task="get_assigned_permission_sets_for_supervisor",
            no_task="has_us_directreport_yes",
        )

        get_assigned_permission_sets_for_supervisor = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_supervisor',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{  result('searchuser_supervisor').uri }}"
            }
        )

        log_message_checkif_supervisorpermissionisassigned = rail.PythonOperator(
            task_id='log_message_checkif_supervisorpermissionisassigned',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(get_assigned_permission_sets_for_supervisor.task_id),
                                                                         'policyUri', "urn:replicon:policy:supervision")
        )

        has_no_suprevisor_permission = rail.IfOperator(
            task_id='has_no_suprevisor_permission',
            test="{{ result('log_message_checkif_supervisorpermissionisassigned') | is_falsy }}",
            yes_task="get_all_non_user_permission_policy",
            no_task="update_supervisorassignmentwithaneffectivedate",
        )

        get_all_non_user_permission_policy = rail.PythonOperator(
            task_id='get_all_non_user_permission_policy',
            python_callable=lambda: list(map(lambda x: x['permissionSet']['uri'],
                                             filter(lambda x: x['policyUri'] != 'urn:replicon:policy:user',
                                                    rail.result('get_assigned_permission_sets_for_supervisor'))))
        )

        log_message_new_permission_setforsupervisor = rail.PythonOperator(
            task_id='log_message_new_permission_setforsupervisor',
            python_callable=lambda: [rail.result('log_message_permissionfor_manager_supervisor'), rail.result(
                'log_message_permissionfor_manager_basic_user')]
        )

        log_message_permission_setsfor_user = rail.PythonOperator(
            task_id='log_message_permission_setsfor_user',
            python_callable=lambda:  rail.result('get_all_non_user_permission_policy') +
            rail.result('log_message_new_permission_setforsupervisor')
        )

        put_permission_set_assignments_for_supervisorofthe_user = rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_for_supervisorofthe_user',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('searchuser_supervisor')['uri'],
                "permissionSetUris": rail.result('log_message_permission_setsfor_user')

            }
        )

        update_supervisorassignmentwithaneffectivedate = rail.RepliconServiceOperator(
            task_id='update_supervisorassignmentwithaneffectivedate',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('put_user2').uri }}",
                "supervisorUri": "{{ result('searchuser_supervisor').uri }}",
                "dateRange": null
            }
        )

        has_no_supervisor_found = rail.IfOperator(
            task_id='has_no_supervisor_found',
            test="{{ result('searchuser_supervisor') | is_falsy }}",
            yes_task="queue_supervisor_assignment",
            no_task="log_message_tobeusedinfinallogmessage_disabled",
        )

        queue_supervisor_assignment = rail.PythonOperator(
            task_id='queue_supervisor_assignment',
            python_callable=lambda: {
                "useruri": rail.result('put_user2')['uri'],
                "supervisorid": rail.get_dag_run_conf()['supervisorid'],
                "loginname": rail.get_dag_run_conf()['loginname']
            }
        )

        log_message_tobeusedinfinallogmessage_disabled = rail.PythonOperator(
            task_id='log_message_tobeusedinfinallogmessage_disabled',
            python_callable=lambda: "Supervisor not assigned since the supervisor profile is disabled in Replicon" if rail.result(
                'searchuser_supervisor') and rail.result('searchuser_supervisor').get('uri') and rail.result('searchuser_supervisor').status != 'True' else ''
        )

        has_us_directreport_yes = rail.IfOperator(
            task_id='has_us_directreport_yes',
            test="{{ 'Y' in dag_run.conf.hasusdirectreport }}",
            yes_task="put_permission_set_assignments_forsupervisor_of_the_user",
            no_task="get_locations_data",
        )

        put_permission_set_assignments_forsupervisor_of_the_user = rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_forsupervisor_of_the_user',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data={
                "userUri": "{{ result('put_user2').uri }}",
                "permissionSetUris": [
                    "{{ result('get_put_user_param').managersupervisoruri }}", "{{ result('get_put_user_param').managerbasicuseruri }}"
                ]
            }
        )

        get_locations_data = rail.RepliconServiceOperator(
            task_id='get_locations_data',
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: next(iter(filter(lambda x: x['code'] == (get_conf()['location'] or "Non-USA"),
                                                       map(lambda x: {
                                                           "uri": x['cells'][0].get('uri'),
                                                           "name": x['cells'][0].get('textValue'),
                                                           "code": x['cells'][1].get('textValue'),
                                                       }, data['rows']))), None)
        )

        can_update_location = rail.IfOperator(
            task_id='can_update_location',
            test="{{ result('get_locations_data') | is_truthy }}",
            yes_task="put_location_schedule_for_user2",
            no_task="has_cost_center_uri",
        )

        put_location_schedule_for_user2 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user2',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('put_user2').uri }}",
                "scheduleEntries": [
                    {
                        "location": {
                           "uri": "{{ result('get_locations_data').uri }}",
                           "parentUri": null,
                           "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        has_cost_center_uri = rail.IfOperator(
            task_id='has_cost_center_uri',
            test="{{ dag_run.conf.costcenter | is_truthy and result('get_put_user_param').costcenteruri | is_truthy }}",
            yes_task="assign_cost_center_user",
            no_task="has_employeetype_final",
        )

        assign_cost_center_user = rail.RepliconServiceOperator(
            task_id='assign_cost_center_user',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data={
                "userUri": "{{ result('put_user2').uri }}",
                "scheduleEntries": [
                    {
                        "costCenter": {
                           "uri": "{{ result('get_put_user_param').costcenteruri }}",
                           "parentUri": null,
                           "name": null
                        },
                        "effectiveDate": null
                    }
                ]}
        )

        has_employeetype_final = rail.IfOperator(
            task_id='has_employeetype_final',
            test="{{ result('log_message_employeetype_final') | is_truthy }}",
            yes_task="get_enabled_divisions_emptype",
            no_task="put_schedule_policy_schedule_for_user2",
        )

        get_enabled_divisions_emptype = rail.RepliconServiceOperator(
            task_id='get_enabled_divisions_emptype',
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
        )

        log_message_pluck_emptype_div_uri = rail.PythonOperator(
            task_id='log_message_pluck_emptype_div_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(get_enabled_divisions_emptype.task_id),
                                                                         'displayText', rail.result('log_message_employeetype_final'), 'uri')
        )

        can_assign_division_uri = rail.IfOperator(
            task_id='can_assign_division_uri',
            test="{{ result('log_message_pluck_emptype_div_uri') | is_truthy }}",
            yes_task="assign_division_user",
            no_task="log_message_emptype_div_notfound",
        )

        assign_division_user = rail.RepliconServiceOperator(
            task_id='assign_division_user',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data={
                "userUri": "{{ result('put_user2').uri }}",
                "scheduleEntries": [
                    {
                        "division": {
                           "uri": "{{ result('log_message_pluck_emptype_div_uri') }}",
                           "parentUri": null,
                           "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        log_message_emptype_div_notfound = rail.PythonOperator(
            task_id='log_message_emptype_div_notfound',
            python_callable=lambda: f"{rail.result('log_message_employeetype_final')} - group not assigned, since EETypeEmail is not defined in Replicon"
        )

        log_message_group_notfound = rail.PythonOperator(
            task_id='log_message_group_notfound',
            python_callable=lambda: f"{rail.result('log_message_employeetype_final')} - group not available."
        )

        put_schedule_policy_schedule_for_user2 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_schedule_for_user2',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ result('put_user2').uri }}",
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                           "officeScheduleUri": "{{ result('get_put_user_param').scheduleuri }}",
                           "name": null,
                           "officeSchedule": null,
                           "scheduleTypeUri": null
                        },
                        "effectiveDate": null
                    }
                ]}
        )

        search_entries_for_holiday_calendar = rail.PythonOperator(
            task_id='search_entries_for_holiday_calendar',
            python_callable=lambda: next(iter(filter(lambda x: x['lookup'] == "holiday calendar" and x["Employee type"] == rail.result(
                'log_message_employeetype_final') and x['Additional'] == "", general_mapper)), {}).get('Value')
        )

        get_holiday_calendar_uri = rail.RepliconServiceOperator(
            task_id='get_holiday_calendar_uri',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda data: rail.find_first_by_attr_and_get_attr(
                data, 'name', rail.result('search_entries_for_holiday_calendar'), 'uri')
        )

        can_update_holiday_calendar = rail.IfOperator(
            task_id='can_update_holiday_calendar',
            test="{{ result('search_entries_for_holiday_calendar') | is_truthy and result('get_holiday_calendar_uri') | is_truthy }}",
            yes_task="update_holiday_calendar",
            no_task="search_entries_foractivities_mapper",
        )

        update_holiday_calendar = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ result('put_user2').uri }}",
                "holidayCalendarUri": "{{ result('get_holiday_calendar_uri') }}"
            }
        )

        search_entries_foractivities_mapper = rail.PythonOperator(
            task_id='search_entries_foractivities_mapper',
            python_callable=lambda: next(iter(filter(lambda x: x['lookup'] == "activity" and x["Employee type"] == rail.result(
                'log_message_employeetype_final') and x['Additional'] == "", general_mapper)), {}).get('Value')
        )

        has_activity_mapper_value = rail.IfOperator(
            task_id='has_activity_mapper_value',
            test="{{ result('search_entries_foractivities_mapper') | is_truthy }}",
            yes_task="get_all_activities_user",
            no_task="search_entries_for_time_off_types",
        )

        get_all_activities_user = rail.RepliconServiceOperator(
            task_id='get_all_activities_user',
            endpoint="/services/ActivityService1.svc/GetAllActivities",
        )

        put_activity_assignment = rail.RepliconServiceOperator(
            task_id='put_activity_assignment',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('put_user2')['uri'],
                "activityUris": list(
                    map(lambda x: rail.find_first_by_attr_and_get_attr(rail.result('get_all_activities_user'), 'name', x, 'uri'),
                        rail.result('search_entries_foractivities_mapper').split('|')))
            }
        )

        search_entries_for_time_off_types = rail.PythonOperator(
            task_id='search_entries_for_time_off_types',
            python_callable=lambda: next(iter(filter(lambda x: x['lookup'] == "time off type"
                                                     and x["Employee type"] == rail.result('log_message_employeetype_final'), general_mapper)), {}).get('Value')
        )

        has_mapper_entries_for_time_offtypes = rail.IfOperator(
            task_id='has_mapper_entries_for_time_offtypes',
            test="{{ result('search_entries_for_time_off_types') | is_truthy }}",
            yes_task="get_enabled_time_off_types",
            no_task="add_success_log",
        )

        get_enabled_time_off_types = rail.RepliconServiceOperator(
            task_id='get_enabled_time_off_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        put_time_off_type_assignments_for_user = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('put_user2')['uri'],
                "timeOffTypeUris": list(
                    map(lambda x: rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_time_off_types'), 'displayText', x, 'uri'),
                        rail.result('search_entries_for_time_off_types').split('|')))
            }
        )

        get_default_time_off_type_policy_schedule_for_user = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            items=lambda: list(
                    map(lambda x: rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_time_off_types'), 'displayText', x, 'uri'),
                        rail.result('search_entries_for_time_off_types').split('|'))),
            data={
                "timeOffAccount": {
                    "userUri": "{{ result('put_user2').uri }}",
                    "timeOffTypeUri": "{{ item }}"
                }
            }
        )

        assign_default_timeoff_policy = rail.RepliconServiceCallForEachItemOperator(
            task_id='assign_default_timeoff_policy',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            items=lambda: list(
                    map(lambda x: rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_time_off_types'), 'displayText', x, 'uri'),
                        rail.result('search_entries_for_time_off_types').split('|'))),
            data=lambda item: {
                "timeOffAccount": {
                    "userUri": rail.result('put_user2')['uri'],
                    "timeOffTypeUri": item
                },

                "policySetScheduleEntries": json.loads(json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user')
                                                                  [list(
                                                                      map(lambda x: rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_time_off_types'), 'displayText', x, 'uri'),
                                                                          rail.result('search_entries_for_time_off_types').split('|'))).index(item)])
                                                       .replace('"script"', '"scriptTarget"')
                                                       .replace('"description": null', '"description": "effective"'))
            }
        )

        add_success_log = rail.WriteLogOperator(
            task_id='add_success_log',
            log="{{ dag_run.conf.log }}",
            message="{{ result('log_message_tobeusedinfinallogmessage_disabled') | sn }} {{ result('log_message_emptype_div_notfound') | sn }} {{ result('log_message_group_notfound') | sn }}",
            severity="Success",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Add User|{{ dag_run.conf.employeeid }}",
                "status": "Success",
                "message": "{{ (result('log_message_tobeusedinfinallogmessage_disabled') | sn + result('log_message_emptype_div_notfound') | sn + result('log_message_group_notfound') | sn ).strip() }}",
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log }}",
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Add User|{{dag_run.conf.employeeid }}",
                'status': 'Error',
                'message': '{{ get_error_message() }}',

            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_message_today

        log_message_today >> log_message_startdate >> has_empty_values

        has_empty_values >> rail.Label('Yes') >> add_empty_values_log >> finish
        has_empty_values >> rail.Label(
            'No') >> get_departmentdata >> get_all_employee_type_details >> get_all_office_schedules >> getallpermissionsets >> has_no_location_emptype

        has_no_location_emptype >> rail.Label(
            'Yes') >> log_message_employeetype_reg_sal >> log_message_employeetype_final
        has_no_location_emptype >> rail.Label(
            'No') >> log_message_employeetype_final

        log_message_employeetype_final >> log_message_get_requiredemployeetype_uri >> get_all_cost_center >> get_all_policy_setsfor_timesheetsandtimeoffs >> get_custom_fielduri >> has_no_emp_type_uri

        has_no_emp_type_uri >> rail.Label(
            'Yes') >> add_invalid_emp_type_log >> finish
        has_no_emp_type_uri >> rail.Label(
            'No') >> get_put_user_param >> put_user2 >> set_sso_for_user >> unassign_all_timeoffs_for_user >> has_hourly_emp_type

        has_hourly_emp_type >> rail.Label(
            'Yes') >> has_ca_co_location
        has_hourly_emp_type >> rail.Label('No') >> search_entries_for_payrule

        has_ca_co_location >> rail.Label(
            'No') >> log_message_locationlookupas_non_ca >> search_entries_for_payrule
        has_ca_co_location >> rail.Label(
            'Yes') >> log_message_locationlookupas_ca >> search_entries_for_payrule
        search_entries_for_payrule >> has_payrule_mapper_value

        has_payrule_mapper_value >> rail.Label(
            'Yes') >> assign_initial_payrule >> has_pdrcountry_usa
        has_payrule_mapper_value >> rail.Label(
            'No') >> has_pdrcountry_usa

        has_pdrcountry_usa >> rail.Label(
            'Yes') >> has_salary_emptype
        has_pdrcountry_usa >> rail.Label(
            'No') >> log_message_finalvaluefor_timesheet_templatename

        has_salary_emptype >> rail.Label(
            'Yes') >> log_message_timesheettemplate_to_assign_sal >> has_hourly_ca_type
        has_salary_emptype >> rail.Label('No') >> has_hourly_ca_type

        has_hourly_ca_type >> rail.Label(
            'Yes') >> log_message_timesheettemplatetoassign_ca_hourly >> has_hourly_non_ca_type
        has_hourly_ca_type >> rail.Label('No') >> has_hourly_non_ca_type

        has_hourly_non_ca_type >> rail.Label(
            'Yes') >> log_message_timesheettemplatetoassign_nonca_hr >> log_message_finalvaluefor_timesheet_templatename
        has_hourly_non_ca_type >> rail.Label(
            'No') >> log_message_finalvaluefor_timesheet_templatename

        log_message_finalvaluefor_timesheet_templatename >> log_message_gettherequiredtimesheettemplate_uri >> has_usa_salary_emptype

        has_usa_salary_emptype >> rail.Label(
            'Yes') >> log_message_timeofftemplatetoassign_timeoffsal >> has_usa_hourly_emptype
        has_usa_salary_emptype >> rail.Label('No') >> has_usa_hourly_emptype

        has_usa_hourly_emptype >> rail.Label(
            'Yes') >> log_message_timeofftemplatetoassign_timeoffhrly >> log_message_checkifthevalueispresentfortimeofftemplate
        has_usa_hourly_emptype >> rail.Label(
            'No') >> log_message_checkifthevalueispresentfortimeofftemplate

        log_message_checkifthevalueispresentfortimeofftemplate >> log_message_gettherequiredtimeofftemplate_uri >> has_ca_hrly_emptype

        has_ca_hrly_emptype >> rail.Label(
            'Yes') >> log_message_punchpolicytoassign_alldevice >> has_nonca_hrly_emptype
        has_ca_hrly_emptype >> rail.Label('No') >> has_nonca_hrly_emptype

        has_nonca_hrly_emptype >> rail.Label(
            'Yes') >> log_message_punchpolicy_nonca >> log_message_finalvaluefor_punchentrypolicyname
        has_nonca_hrly_emptype >> rail.Label(
            'No') >> log_message_finalvaluefor_punchentrypolicyname
        log_message_finalvaluefor_punchentrypolicyname >> log_message_gettherequired_punch_entry_policy_uri >> log_message_required_policysetstobeassigned_final >> has_policysetstobeassigned
        has_policysetstobeassigned >> rail.Label(
            'Yes') >> update_templates_foruser >> has_usa_sal_emptype
        has_policysetstobeassigned >> rail.Label(
            'No') >> update_blank_templatesforuser >> has_usa_sal_emptype

        has_usa_sal_emptype >> rail.Label(
            'Yes') >> log_message_timesheetapporvalpath_system >> has_usa_hrly_emptype
        has_usa_sal_emptype >> rail.Label('No') >> has_usa_hrly_emptype

        has_usa_hrly_emptype >> rail.Label(
            'Yes') >> log_message_timesheetapprovalpath_supervisor >> log_message_finalvaluefortimesheetapprovalpath
        has_usa_hrly_emptype >> rail.Label(
            'No') >> log_message_finalvaluefortimesheetapprovalpath

        log_message_finalvaluefortimesheetapprovalpath >> get_all_timesheet_approval_path >> log_message_gettherequiredtimesheetapprovalpath_uri >> has_timesheet_path_uri

        has_timesheet_path_uri >> rail.Label(
            'Yes') >> update_timesheet_approval_path_for_user >> has_usa_salary_emptype2
        has_timesheet_path_uri >> rail.Label('No') >> has_usa_salary_emptype2

        has_usa_salary_emptype2 >> rail.Label(
            'Yes') >> log_message_timeoffapprovalpath2_timoffsal >> has_usa_hrly_emptype2
        has_usa_salary_emptype2 >> rail.Label('No') >> has_usa_hrly_emptype2

        has_usa_hrly_emptype2 >> rail.Label(
            'Yes') >> log_message_timeoffapprovalpath2_timeoffhrly >> log_message_finalvaluefor_time_offapprovalpath2
        has_usa_hrly_emptype2 >> rail.Label(
            'No') >> log_message_finalvaluefor_time_offapprovalpath2

        log_message_finalvaluefor_time_offapprovalpath2 >> get_all_time_off_approval_path2 >> log_message_gettherequiredtimeoffapprovalpath_uri2 >> has_approvalpath_uri2

        has_approvalpath_uri2 >> rail.Label(
            'Yes') >> update_timeoff_approval_path_for_user2 >> log_message_timesheet_period_uri_system
        has_approvalpath_uri2 >> rail.Label(
            'No') >> log_message_timesheet_period_uri_system

        log_message_timesheet_period_uri_system >> update_timesheet_period_type_for_user_assign_system_timesheet_period >> has_udf_returnto_work_date_uri

        has_udf_returnto_work_date_uri >> rail.Label(
            'Yes') >> update_returnto_work_date_udf >> has_wfn_id_uri
        has_udf_returnto_work_date_uri >> rail.Label(
            'No') >> has_wfn_id_uri

        has_wfn_id_uri >> rail.Label(
            'Yes') >> update_wfn_id_udf >> has_firstdayofleave_udfuri
        has_wfn_id_uri >> rail.Label(
            'No') >> has_firstdayofleave_udfuri

        has_firstdayofleave_udfuri >> rail.Label(
            'Yes') >> update_first_dayof_leave_udf >> has_lasthiredateuri_udf
        has_firstdayofleave_udfuri >> rail.Label(
            'No') >> has_lasthiredateuri_udf

        has_lasthiredateuri_udf >> rail.Label(
            'Yes') >> update_last_hire_date2_udf >> has_lastrecordupdate_udfuri
        has_lasthiredateuri_udf >> rail.Label(
            'No') >> has_lastrecordupdate_udfuri

        has_lastrecordupdate_udfuri >> rail.Label(
            'Yes') >> update_last_record_update_udf >> has_companycode_udfuri
        has_lastrecordupdate_udfuri >> rail.Label(
            'No') >> has_companycode_udfuri

        has_companycode_udfuri >> rail.Label(
            'Yes') >> get_enabled_custom_field_drop_down_optionsfor_company_code >> has_drop_down_country_code_uri
        has_companycode_udfuri >> rail.Label(
            'No') >> log_message_permissionfor_manager_supervisor

        has_drop_down_country_code_uri >> rail.Label(
            'Yes') >> update_company_code_udf >> has_pdr_country_uri
        has_drop_down_country_code_uri >> rail.Label(
            'No') >> has_pdr_country_uri

        has_pdr_country_uri >> rail.Label(
            'Yes') >> get_enabled_custom_field_drop_down_optionsfor_company_code2 >> has_valid_pdr_country_uri
        has_pdr_country_uri >> rail.Label(
            'No') >> log_message_permissionfor_manager_supervisor

        has_valid_pdr_country_uri >> rail.Label(
            'Yes') >> update_pdr_country_udf >> log_message_permissionfor_manager_supervisor
        has_valid_pdr_country_uri >> rail.Label(
            'No') >> log_message_permissionfor_manager_supervisor

        log_message_permissionfor_manager_supervisor >> log_message_permissionfor_manager_basic_user >> has_supervisorid

        has_supervisorid >> rail.Label(
            'Yes') >> searchuser_supervisor >> has_supervisor_uri_changed
        has_supervisorid >> rail.Label('No') >> has_us_directreport_yes

        has_supervisor_uri_changed >> rail.Label(
            'Yes') >> can_update_supervisor
        has_supervisor_uri_changed >> rail.Label(
            'No') >> has_no_supervisor_found

        can_update_supervisor >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_supervisor >> log_message_checkif_supervisorpermissionisassigned >> has_no_suprevisor_permission
        can_update_supervisor >> rail.Label(
            'No') >> has_us_directreport_yes

        has_no_suprevisor_permission >> rail.Label(
            'Yes') >> get_all_non_user_permission_policy >> log_message_new_permission_setforsupervisor >> log_message_permission_setsfor_user >> put_permission_set_assignments_for_supervisorofthe_user >> update_supervisorassignmentwithaneffectivedate >> has_us_directreport_yes
        has_no_suprevisor_permission >> rail.Label(
            'No') >> update_supervisorassignmentwithaneffectivedate >> has_us_directreport_yes

        has_no_supervisor_found >> rail.Label(
            'Yes') >> queue_supervisor_assignment >> has_us_directreport_yes
        has_no_supervisor_found >> rail.Label(
            'No') >> log_message_tobeusedinfinallogmessage_disabled >> has_us_directreport_yes

        has_us_directreport_yes >> rail.Label(
            'Yes') >> put_permission_set_assignments_forsupervisor_of_the_user >> get_locations_data
        has_us_directreport_yes >> rail.Label(
            'No') >> get_locations_data
        get_locations_data >> can_update_location

        can_update_location >> rail.Label(
            'Yes') >> put_location_schedule_for_user2 >> has_cost_center_uri
        can_update_location >> rail.Label('No') >> has_cost_center_uri

        has_cost_center_uri >> rail.Label(
            'Yes') >> assign_cost_center_user >> has_employeetype_final
        has_cost_center_uri >> rail.Label('No') >> has_employeetype_final

        has_employeetype_final >> rail.Label(
            'Yes') >> get_enabled_divisions_emptype >> log_message_pluck_emptype_div_uri >> can_assign_division_uri
        has_employeetype_final >> rail.Label(
            'No') >> put_schedule_policy_schedule_for_user2

        can_assign_division_uri >> rail.Label(
            'Yes') >> assign_division_user >> put_schedule_policy_schedule_for_user2
        can_assign_division_uri >> rail.Label(
            'No') >> log_message_emptype_div_notfound >> log_message_group_notfound >> put_schedule_policy_schedule_for_user2

        put_schedule_policy_schedule_for_user2 >> search_entries_for_holiday_calendar >> get_holiday_calendar_uri >> can_update_holiday_calendar

        can_update_holiday_calendar >> rail.Label(
            'yes') >> update_holiday_calendar >> search_entries_foractivities_mapper
        can_update_holiday_calendar >> rail.Label(
            'NO') >> search_entries_foractivities_mapper

        search_entries_foractivities_mapper >> has_activity_mapper_value

        has_activity_mapper_value >> rail.Label(
            'No') >> search_entries_for_time_off_types
        has_activity_mapper_value >> rail.Label(
            'yes') >> get_all_activities_user >> put_activity_assignment >> search_entries_for_time_off_types

        search_entries_for_time_off_types >> has_mapper_entries_for_time_offtypes

        has_mapper_entries_for_time_offtypes >> rail.Label(
            'Yes') >> get_enabled_time_off_types >> put_time_off_type_assignments_for_user >> get_default_time_off_type_policy_schedule_for_user >> assign_default_timeoff_policy >> add_success_log

        has_mapper_entries_for_time_offtypes >> rail.Label(
            'No') >> add_success_log

        add_success_log >> finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
