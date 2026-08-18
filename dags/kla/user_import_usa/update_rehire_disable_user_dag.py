
from datetime import datetime, timedelta
import pytz
from airflow.models import Variable
import rail
from kla.user_import_usa.mapper.time_zone_mapper import time_zone_mapper
from kla.user_import_usa.mapper.general_mapper import general_mapper
null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'kla_user_import_usa_update_rehire_disable_user_{config.instance}',
        description=f'KLATencor Update / Rehire / Disable User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        def get_conf():
            return rail.get_current_context()['dag_run'].conf

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='has_no_valid_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='has_no_valid_data',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        has_no_valid_data = rail.IfOperator(
            task_id='has_no_valid_data',
            test="{{ dag_run.conf.employeeid | is_falsy or dag_run.conf.department | is_falsy or dag_run.conf.loginname | is_falsy }}",
            yes_task="add_invalid_data_log",
            no_task="log_message_today",
        )

        add_invalid_data_log = rail.WriteLogOperator(
            task_id='add_invalid_data_log',
            log="{{ dag_run.conf.log }}",
            message="User not Updated, login name/email or Employee ID or department not present",
            severity="Exception",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Update User|{{ dag_run.conf.employeeid }}",
                "status": "Exception",
                "message": "User not Updated, login name/email or Employee ID or department not present"
            }
        )

        log_message_today = rail.PythonOperator(
            task_id='log_message_today',
            python_callable=lambda: {
                'year': datetime.now(tz=pytz.UTC).year,
                'month': datetime.now(tz=pytz.UTC).month,
                'day': datetime.now(tz=pytz.UTC).day,
            }
        )

        log_message_userfiltervaluebasedonuseruri = rail.PythonOperator(
            task_id='log_message_userfiltervaluebasedonuseruri',
            python_callable=lambda: rail.get_current_context(
            )['dag_run'].conf['useruri'].split(":")[-1]
        )

        log_message_report_name = rail.PythonOperator(
            task_id='log_message_report_name',
            python_callable=lambda:  config.user_import_report_name
        )

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name=config.user_import_report_name
        )

        log_message_userfilter_uri = rail.PythonOperator(
            task_id='log_message_userfilter_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_user_report_details')[
                'filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri')
        )

        generate_user_report = rail.RepliconServiceOperator(
            task_id='generate_user_report',
            endpoint="/services/reportService1.svc/GenerateReport",
            data={
                "reportUri": "{{ result('get_user_report_details').uri }}",
                "filterValues": [
                    {
                        "reportFilterUri": "{{ result('log_message_userfilter_uri') }}",
                        "value": "{{ result('log_message_userfiltervaluebasedonuseruri') }}"
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        load_csv_user_data = rail.LoadCSVFileOperator(
            task_id='load_csv_user_data',
            document="{{result('generate_user_report').payload}}"
        )

        # "header_line": "Employee ID,User First Name,User Last Name,User Email,User Status,Employee Type,User Department Name,User Start Date,
        # User End Date,Last Hire Date2,First Day of Leave,Return to Work Date,Last Record Update,Login Name,Company Code,Cost Center (Current),
        # Location (Current),Punch Entry Policy Name,Timesheet Template,Timesheet Approval Path,Pay Rule Name,Holiday Calendar,supervisoruri,
        # time off approval path,pdrcountry,timezone"
        parse_csv_user_data = rail.PythonOperator(
            task_id='parse_csv_user_data',
            python_callable=lambda: rail.load_all_records(
                rail.result('load_csv_user_data'))[0]
        )

        log_message_derive_employeetype = rail.PythonOperator(
            task_id='log_message_derive_employeetype',
            python_callable=lambda: rail.get_current_context(
            )['dag_run'].conf['employeetype'] or "Regular Salary"
        )

        can_update_employee_type = rail.IfOperator(
            task_id='can_update_employee_type',
            test="{{ result('parse_csv_user_data')['Employee Type'] | is_truthy and result('parse_csv_user_data')['Employee Type'] != result('log_message_derive_employeetype') }}",
            yes_task="update_employee_type",
            no_task="can_update_first_name",
        )

        update_employee_type = rail.RepliconServiceOperator(
            task_id='update_employee_type',
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
                    "employeeTypeToApply": {
                        "uri": null,
                        "name": "{{ result('log_message_derive_employeetype') }}"
                    },
                    "userDetailsToApply": null,
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        update_emptype_email_group_division = rail.RepliconServiceOperator(
            task_id='update_emptype_email_group_division',
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
                    "divisionScheduleToApply": {
                        "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementDivisionSchedule": [],
                        "updateDivisionScheduleOverDateRange": {
                            "replacementDivisionScheduleEntries": [
                                {
                                    "division": {
                                        "uri": null,
                                        "parentUri": null,
                                        "name": "{{ result('log_message_derive_employeetype') }}"
                                    },
                                    "effectiveDate":  {
                                        "year": "{{ result('log_message_today').year }}",
                                        "month": "{{ result('log_message_today').month }}",
                                        "day": "{{ result('log_message_today').day }}"
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "costCenterScheduleToApply": null,
                    "departmentGroupScheduleToApply": null,
                    "employeeTypeGroupScheduleToApply": null,
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
                    "userDetailsToApply": null,
                    "payRulesToApply": null,
                    "payRulesScheduleModifications": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        can_update_first_name = rail.IfOperator(
            task_id='can_update_first_name',
            test="{{ result('parse_csv_user_data')['User First Name'] != dag_run.conf.firstname }}",
            yes_task="update_first_name",
            no_task="can_update_last_name",
        )

        update_first_name = rail.RepliconServiceOperator(
            task_id='update_first_name',
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
                        "firstName": "{{ dag_run.conf.firstname }}",
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
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        can_update_last_name = rail.IfOperator(
            task_id='can_update_last_name',
            test="{{ result('parse_csv_user_data')['User Last Name'] != dag_run.conf.lastname }}",
            yes_task="update_last_name",
            no_task="can_update_loginname",
        )

        update_last_name = rail.RepliconServiceOperator(
            task_id='update_last_name',
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

        can_update_loginname = rail.IfOperator(
            task_id='can_update_loginname',
            test="{{ result('parse_csv_user_data')['Login Name'] != dag_run.conf.loginname }}",
            yes_task="update_loginname",
            no_task="can_update_department",
        )

        update_loginname = rail.RepliconServiceOperator(
            task_id='update_loginname',
            endpoint="/services/securityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ dag_run.conf.loginname }}"
            }
        )

        can_update_department = rail.IfOperator(
            task_id='can_update_department',
            test="{{ result('parse_csv_user_data')['User Department Name'] != dag_run.conf.department }}",
            yes_task="get_enabled_departments",
            no_task="can_update_email",
        )

        get_enabled_departments = rail.RepliconServiceOperator(
            task_id='get_enabled_departments',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
        )

        log_message_departmenturi = rail.PythonOperator(
            task_id='log_message_departmenturi',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_enabled_departments'), 'displayText', get_conf()['department'], 'uri')
        )

        has_department_uri = rail.IfOperator(
            task_id='has_department_uri',
            test="{{ result('log_message_departmenturi') | is_truthy }}",
            yes_task="update_department_user",
            no_task="can_update_email",
        )

        update_department_user = rail.RepliconServiceOperator(
            task_id='update_department_user',
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
                        "uri": "{{ result('log_message_departmenturi') }}",
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

        can_update_email = rail.IfOperator(
            task_id='can_update_email',
            test="{{ dag_run.conf.emailaddress | is_truthy and '@' in dag_run.conf.emailaddress and result('parse_csv_user_data')['User Email'] != dag_run.conf.emailaddress }}",
            yes_task="update_emailaddress",
            no_task="log_message_start_date",
        )

        update_emailaddress = rail.RepliconServiceOperator(
            task_id='update_emailaddress',
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
                        "emailAddress": {"emailAddress": "{{ dag_run.conf.emailaddress }}"},
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

        def get_replicon_date_from_report(date_str):
            if not date_str:
                return None
            # date format in Dec 31, 2007
            date = datetime.strptime(date_str, '%b %d, %Y')
            return {
                'year': date.year,
                'month': date.month,
                'day': date.day
            }

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

        log_message_start_date = rail.PythonOperator(
            task_id='log_message_start_date',
            python_callable=lambda: get_replicon_date(
                get_conf()['startdate'])
        )

        log_message_end_date = rail.PythonOperator(
            task_id='log_message_end_date',
            python_callable=lambda: get_replicon_date(
                get_conf()['enddate'])
        )

        can_update_enddate = rail.IfOperator(
            task_id='can_update_enddate',
            test=lambda: get_conf()['enddate'] and rail.result('log_message_end_date') and
                    (not rail.result('parse_csv_user_data')['User End Date'] or
                        (rail.result('parse_csv_user_data')['User End Date'] and
                         datetime(**get_replicon_date_from_report(rail.result('parse_csv_user_data')['User End Date'])) !=
                         datetime(**rail.result('log_message_end_date'))
                         )
                     ),
            yes_task="update_user_end_date",
            no_task="can_remove_user_end_date",
        )

        update_user_end_date = rail.RepliconServiceOperator(
            task_id='update_user_end_date',
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
                                "year": "{{ result('log_message_end_date').year }}",
                                "month":  "{{ result('log_message_end_date').month }}",
                                "day":  "{{ result('log_message_end_date').day }}",
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

        can_remove_user_end_date = rail.IfOperator(
            task_id='can_remove_user_end_date',
            test="{{ dag_run.conf.enddate | is_falsy and result('log_message_start_date') | is_truthy and result('log_message_end_date') | is_falsy and result('parse_csv_user_data')['User End Date'] | is_truthy }}",
            yes_task="remove_user_end_date",
            no_task="can_update_user_start_date",
        )

        remove_user_end_date = rail.RepliconServiceOperator(
            task_id='remove_user_end_date',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('log_message_start_date').year }}",
                        "month": "{{ result('log_message_start_date').month }}",
                        "day": "{{ result('log_message_start_date').day }}",
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        can_update_user_start_date = rail.IfOperator(
            task_id='can_update_user_start_date',
            test=lambda: get_conf()['startdate'] and rail.result('log_message_start_date') and
                    (not rail.result('parse_csv_user_data')['User Start Date'] or
                        (rail.result('parse_csv_user_data')['User Start Date'] and
                         datetime(**get_replicon_date_from_report(rail.result('parse_csv_user_data')['User Start Date'])) !=
                         datetime(**rail.result('log_message_start_date'))
                         )
                     ),
            yes_task="update_user_start_date",
            no_task="get_all_policy_setsfor_timesheetsandtimeoffs",
        )

        update_user_start_date = rail.RepliconServiceOperator(
            task_id='update_user_start_date',
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
                        "employmentStartDate": {
                            "date": {
                                "year": "{{ result('log_message_start_date').year }}",
                                "month": "{{ result('log_message_start_date').month }}",
                                "day": "{{ result('log_message_start_date').day }}",
                            },
                        },
                        "employmentEndDate": null,
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        get_all_policy_setsfor_timesheetsandtimeoffs = rail.RepliconServiceOperator(
            task_id='get_all_policy_setsfor_timesheetsandtimeoffs',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        has_salary_pdr_country_usa = rail.IfOperator(
            task_id='has_salary_pdr_country_usa',
            test="{{ dag_run.conf.pdrcountry == 'USA' }}",
            yes_task="has_salary_emptype",
            no_task="log_message_finalvalueispresentfortimesheettemplate",
        )

        has_salary_emptype = rail.IfOperator(
            task_id='has_salary_emptype',
            test="{{ result('log_message_derive_employeetype').endswith('Salary') }}",
            yes_task="log_message_employeetype_salary",
            no_task="has_hourly_emptype",
        )

        log_message_employeetype_salary = rail.PythonOperator(
            task_id='log_message_employeetype_salary',
            python_callable=lambda:  'Salary'
        )

        has_hourly_emptype = rail.IfOperator(
            task_id='has_hourly_emptype',
            test="{{ result('log_message_derive_employeetype').endswith('Hourly') and dag_run.conf.location =='CA' }}",
            yes_task="log_message_employeetype_hourly",
            no_task="has_hourly_nonca_emptype",
        )

        log_message_employeetype_hourly = rail.PythonOperator(
            task_id='log_message_employeetype_hourly',
            python_callable=lambda: 'Hourly California'
        )

        has_hourly_nonca_emptype = rail.IfOperator(
            task_id='has_hourly_nonca_emptype',
            test="{{ result('log_message_derive_employeetype').endswith('Hourly') and dag_run.conf.location !='CA' }}",
            yes_task="log_message_employeetype_hourly_non_ca",
            no_task="log_message_finalvalueispresentfortimesheettemplate",
        )

        log_message_employeetype_hourly_non_ca = rail.PythonOperator(
            task_id='log_message_employeetype_hourly_non_ca',
            python_callable=lambda: 'Hourly NonCA'
        )

        log_message_finalvalueispresentfortimesheettemplate = rail.PythonOperator(
            task_id='log_message_finalvalueispresentfortimesheettemplate',
            python_callable=lambda: rail.result('log_message_employeetype_salary') or rail.result(
                'log_message_employeetype_hourly') or rail.result('log_message_employeetype_hourly_non_ca')
        )

        has_timesheet_template = rail.IfOperator(
            task_id='has_timesheet_template',
            test="{{ result('log_message_finalvalueispresentfortimesheettemplate') | is_truthy and result('log_message_finalvalueispresentfortimesheettemplate') != result('parse_csv_user_data')['Timesheet Template'] }}",
            yes_task="log_message_gettherequiredtimesheettemplate_uri",
            no_task="has_blank_timesheet_template",
        )

        log_message_gettherequiredtimesheettemplate_uri = rail.PythonOperator(
            task_id='log_message_gettherequiredtimesheettemplate_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(get_all_policy_setsfor_timesheetsandtimeoffs.task_id),
                                                                         'displayText', rail.result('log_message_finalvalueispresentfortimesheettemplate'), 'uri')
        )

        update_timesheet_template = rail.RepliconServiceOperator(
            task_id='update_timesheet_template',
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
                    "policySetsToApply": {
                        "policySetUrisToAssign": [
                            "{{ result('log_message_gettherequiredtimesheettemplate_uri') }}"
                        ],
                        "policyUrisToRemovePolicySet": []
                    },
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

        has_blank_timesheet_template = rail.IfOperator(
            task_id='has_blank_timesheet_template',
            test="{{ result('log_message_finalvalueispresentfortimesheettemplate') | is_falsy }}",
            yes_task="log_message_gettherequiredtimesheettemplate_uri_to_remove",
            no_task="has_hourlyca_emptype",
        )

        log_message_gettherequiredtimesheettemplate_uri_to_remove = rail.PythonOperator(
            task_id='log_message_gettherequiredtimesheettemplate_uri_to_remove',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(get_all_policy_setsfor_timesheetsandtimeoffs.task_id), 'displayText',
                                                                         rail.result('parse_csv_user_data')['Timesheet Template'], 'uri')
        )

        has_timesheet_template_uri_remove = rail.IfOperator(
            task_id='has_timesheet_template_uri_remove',
            test="{{ result('log_message_gettherequiredtimesheettemplate_uri_to_remove') | is_truthy }}",
            yes_task="remove_timesheet_template",
            no_task="has_hourlyca_emptype",
        )

        remove_timesheet_template = rail.RepliconServiceOperator(
            task_id='remove_timesheet_template',
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('log_message_gettherequiredtimesheettemplate_uri_to_remove') }}"
            }
        )

        has_hourlyca_emptype = rail.IfOperator(
            task_id='has_hourlyca_emptype',
            test="{{ result('log_message_derive_employeetype').endswith('Hourly') and dag_run.conf.location == 'CA'}}",
            yes_task="log_message_punchentrypolicy_ca",
            no_task="has_hourlynonca_emptype",
        )

        log_message_punchentrypolicy_ca = rail.PythonOperator(
            task_id='log_message_punchentrypolicy_ca',
            python_callable=lambda:  'All Devices – CA'
        )

        has_hourlynonca_emptype = rail.IfOperator(
            task_id='has_hourlynonca_emptype',
            test="{{ result('log_message_derive_employeetype').endswith('Hourly') and dag_run.conf.location != 'CA'}}",
            yes_task="log_message_punchentrypolicy_nonca",
            no_task="log_message_checkifthevalueispresentfor_punch_entry_policy",
        )

        log_message_punchentrypolicy_nonca = rail.PythonOperator(
            task_id='log_message_punchentrypolicy_nonca',
            python_callable=lambda:  'All Devices – Non CA'
        )

        log_message_checkifthevalueispresentfor_punch_entry_policy = rail.PythonOperator(
            task_id='log_message_checkifthevalueispresentfor_punch_entry_policy',
            python_callable=lambda: rail.result('log_message_punchentrypolicy_ca') or rail.result(
                'log_message_punchentrypolicy_nonca')
        )

        can_update_punch_entry_policy = rail.IfOperator(
            task_id='can_update_punch_entry_policy',
            test="{{ result('log_message_checkifthevalueispresentfor_punch_entry_policy') | is_truthy and result('log_message_checkifthevalueispresentfor_punch_entry_policy') != result('parse_csv_user_data')['Punch Entry Policy Name']}}",
            yes_task="log_message_gettherequired_punch_entry_policy_uri",
            no_task="has_blank_punch_entry_policy",
        )

        log_message_gettherequired_punch_entry_policy_uri = rail.PythonOperator(
            task_id='log_message_gettherequired_punch_entry_policy_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(get_all_policy_setsfor_timesheetsandtimeoffs.task_id), 'displayText',
                                                                         rail.result(log_message_checkifthevalueispresentfor_punch_entry_policy.task_id), 'uri')
        )

        update_punch_entry_policy = rail.RepliconServiceOperator(
            task_id='update_punch_entry_policy',
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
                    "policySetsToApply": {
                        "policySetUrisToAssign": [
                            "{{ result('log_message_gettherequired_punch_entry_policy_uri') }}"
                        ],
                        "policyUrisToRemovePolicySet": []
                    },
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

        has_blank_punch_entry_policy = rail.IfOperator(
            task_id='has_blank_punch_entry_policy',
            test="{{ result('log_message_checkifthevalueispresentfor_punch_entry_policy') | is_falsy }}",
            yes_task="log_message_gettherequired_punch_entry_policyuri_remove",
            no_task="has_punch_policy_remove",
        )

        log_message_gettherequired_punch_entry_policyuri_remove = rail.PythonOperator(
            task_id='log_message_gettherequired_punch_entry_policyuri_remove',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(get_all_policy_setsfor_timesheetsandtimeoffs.task_id), 'displayText',
                                                                         rail.result(parse_csv_user_data.task_id)['Punch Entry Policy Name'], 'uri')
        )
        has_punch_policy_remove = rail.IfOperator(
            task_id='has_punch_policy_remove',
            test="{{ result('log_message_gettherequired_punch_entry_policyuri_remove') | is_truthy }}",
            yes_task="remove_punch_entry_policy",
            no_task="has_usa_pdr_country_salary",
        )

        remove_punch_entry_policy = rail.RepliconServiceOperator(
            task_id='remove_punch_entry_policy',
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('log_message_gettherequired_punch_entry_policyuri_remove') }}"
            }
        )

        has_usa_pdr_country_salary = rail.IfOperator(
            task_id='has_usa_pdr_country_salary',
            test="{{ dag_run.conf.pdrcountry == 'USA' and result('log_message_derive_employeetype').endswith('Salary') }}",
            yes_task="log_message_approvalpath_system",
            no_task="has_usa_pdr_country_hourly",
        )

        log_message_approvalpath_system = rail.PythonOperator(
            task_id='log_message_approvalpath_system',
            python_callable=lambda:  'System Approved'
        )

        has_usa_pdr_country_hourly = rail.IfOperator(
            task_id='has_usa_pdr_country_hourly',
            test="{{ dag_run.conf.pdrcountry == 'USA' and result('log_message_derive_employeetype').endswith('Hourly') }}",
            yes_task="log_message_approvalpath_supervisor",
            no_task="log_message_checkifthevalueispresentfortimesheetapprovalpath",
        )

        log_message_approvalpath_supervisor = rail.PythonOperator(
            task_id='log_message_approvalpath_supervisor',
            python_callable=lambda:  'Supervisor Approved'
        )

        log_message_checkifthevalueispresentfortimesheetapprovalpath = rail.PythonOperator(
            task_id='log_message_checkifthevalueispresentfortimesheetapprovalpath',
            python_callable=lambda: rail.result('log_message_approvalpath_system') or rail.result(
                'log_message_approvalpath_supervisor')
        )

        get_all_timesheet_approval_path = rail.RepliconServiceOperator(
            task_id='get_all_timesheet_approval_path',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
        )

        has_approval_path_changed = rail.IfOperator(
            task_id='has_approval_path_changed',
            test="{{ result('log_message_checkifthevalueispresentfortimesheetapprovalpath') | is_truthy and result('log_message_checkifthevalueispresentfortimesheetapprovalpath') != result('parse_csv_user_data')['Timesheet Approval Path'] }}",
            yes_task="log_message_gettherequiredtimesheetapprovalpath_uri",
            no_task="has_salary_emptype_usa",
        )

        log_message_gettherequiredtimesheetapprovalpath_uri = rail.PythonOperator(
            task_id='log_message_gettherequiredtimesheetapprovalpath_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(get_all_timesheet_approval_path.task_id), 'displayText',
                                                                         rail.result(log_message_checkifthevalueispresentfortimesheetapprovalpath.task_id), 'uri')
        )

        can_update_approval_path = rail.IfOperator(
            task_id='can_update_approval_path',
            test="{{ result('log_message_gettherequiredtimesheetapprovalpath_uri') | is_truthy }}",
            yes_task="update_timesheet_approval_path_for_user",
            no_task="has_salary_emptype_usa",
        )

        update_timesheet_approval_path_for_user = rail.RepliconServiceOperator(
            task_id='update_timesheet_approval_path_for_user',
            endpoint="/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "approvalPathUri": "{{ result('log_message_gettherequiredtimesheetapprovalpath_uri') }}"
            }
        )

        has_salary_emptype_usa = rail.IfOperator(
            task_id='has_salary_emptype_usa',
            test="{{ dag_run.conf.pdrcountry == 'USA' and result('log_message_derive_employeetype') | ends_with('Salary') }}",
            yes_task="log_message_timeofftemplate_salary",
            no_task="has_hourly_emptype_usa",
        )

        log_message_timeofftemplate_salary = rail.PythonOperator(
            task_id='log_message_timeofftemplate_salary',
            python_callable=lambda:  'Time Off-Salary'
        )

        has_hourly_emptype_usa = rail.IfOperator(
            task_id='has_hourly_emptype_usa',
            test="{{ dag_run.conf.pdrcountry == 'USA' and result('log_message_derive_employeetype')  | ends_with('Hourly') }}",
            yes_task="log_message_timeofftemplate_hourly",
            no_task="log_message_checkifthevalueispresentfortimeofftemplate",
        )

        log_message_timeofftemplate_hourly = rail.PythonOperator(
            task_id='log_message_timeofftemplate_hourly',
            python_callable=lambda:  'Time Off-Hourly'
        )

        log_message_checkifthevalueispresentfortimeofftemplate = rail.PythonOperator(
            task_id='log_message_checkifthevalueispresentfortimeofftemplate',
            python_callable=lambda: rail.result('log_message_timeofftemplate_salary') or rail.result(
                'log_message_timeofftemplate_hourly')
        )

        has_timeoff_template_changed = rail.IfOperator(
            task_id='has_timeoff_template_changed',
            test="{{ result('log_message_checkifthevalueispresentfortimeofftemplate') | is_truthy and result('log_message_checkifthevalueispresentfortimeofftemplate') != result('parse_csv_user_data')['Time Off Template'] }}",
            yes_task="log_message_gettherequiredtimeofftemplate_uri",
            no_task="log_message_checkiftheuriexistsforgivenlocationfrom_timezonemapper",
        )

        log_message_gettherequiredtimeofftemplate_uri = rail.PythonOperator(
            task_id='log_message_gettherequiredtimeofftemplate_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(get_all_policy_setsfor_timesheetsandtimeoffs.task_id), 'displayText',
                                                                         rail.result(log_message_checkifthevalueispresentfortimeofftemplate.task_id), 'uri')
        )

        can_update_timeoff_template = rail.IfOperator(
            task_id='can_update_timeoff_template',
            test="{{ result('log_message_gettherequiredtimeofftemplate_uri') | is_truthy }}",
            yes_task="update_timeoff_template_for_user",
            no_task="log_message_checkiftheuriexistsforgivenlocationfrom_timezonemapper",
        )

        update_timeoff_template_for_user = rail.RepliconServiceOperator(
            task_id='update_timeoff_template_for_user',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('log_message_gettherequiredtimeofftemplate_uri') }}"
            }
        )

        log_message_checkiftheuriexistsforgivenlocationfrom_timezonemapper = rail.PythonOperator(
            task_id='log_message_checkiftheuriexistsforgivenlocationfrom_timezonemapper',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                time_zone_mapper, 'State', get_conf()['location'], 'URI')
        )

        log_message_checkifthenameexistsforgivenlocationfrom_timezonemapper = rail.PythonOperator(
            task_id='log_message_checkifthenameexistsforgivenlocationfrom_timezonemapper',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                time_zone_mapper, 'State', get_conf()['location'], 'Timezone')
        )

        can_update_timezone = rail.IfOperator(
            task_id='can_update_timezone',
            test="{{ result('log_message_checkifthenameexistsforgivenlocationfrom_timezonemapper') | is_truthy and result('parse_csv_user_data').get('timezone') != result('log_message_checkifthenameexistsforgivenlocationfrom_timezonemapper') }}",
            yes_task="update_time_zone",
            no_task="get_custom_fieldsforuser",
        )

        update_time_zone = rail.RepliconServiceOperator(
            task_id='update_time_zone',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeZoneUri": "{{ result('log_message_checkiftheuriexistsforgivenlocationfrom_timezonemapper') }}"
            }
        )

        get_custom_fieldsforuser = rail.RepliconServiceOperator(
            task_id='get_custom_fieldsforuser',
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroup",
            data={
                "objectTypeUri": "urn:replicon:object-type:user"
            }
        )

        get_custom_fielduri = rail.RepliconServiceOperator(
            task_id='get_custom_fielduri',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{result('get_custom_fieldsforuser').uri }}"
            }
        )

        has_return_to_workdate = rail.IfOperator(
            task_id='has_return_to_workdate',
            test="{{ dag_run.conf.returntoworkdate | is_truthy and dag_run.conf.returntoworkdate != result('parse_csv_user_data')['Return to Work Date']}}",
            yes_task="log_message_gettherequiredcustomfield_uri_returnto_work_date",
            no_task="has_firstdayofleave",
        )

        log_message_gettherequiredcustomfield_uri_returnto_work_date = rail.PythonOperator(
            task_id='log_message_gettherequiredcustomfield_uri_returnto_work_date',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                get_custom_fielduri.task_id), 'displayText', 'Return to Work Date', 'uri')
        )

        can_update_update_returnto_work_date_udf = rail.IfOperator(
            task_id='can_update_update_returnto_work_date_udf',
            test="{{ result('log_message_gettherequiredcustomfield_uri_returnto_work_date') | is_truthy }}",
            yes_task="update_returnto_work_date_udf",
            no_task="has_firstdayofleave",
        )

        update_returnto_work_date_udf = rail.RepliconServiceOperator(
            task_id='update_returnto_work_date_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_message_gettherequiredcustomfield_uri_returnto_work_date') }}",
                "value": "{{ dag_run.conf.returntoworkdate }}"
            }
        )

        has_firstdayofleave = rail.IfOperator(
            task_id='has_firstdayofleave',
            test="{{ dag_run.conf.firstdayofleave | is_truthy and dag_run.conf.firstdayofleave != result('parse_csv_user_data')['First Day of Leave'] }}",
            yes_task="log_message_gettherequiredcustomfield_uri_first_dayof_leave",
            no_task="has_change_in_lasthiredate2",
        )

        log_message_gettherequiredcustomfield_uri_first_dayof_leave = rail.PythonOperator(
            task_id='log_message_gettherequiredcustomfield_uri_first_dayof_leave',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                get_custom_fielduri.task_id), 'displayText', 'First Day of Leave', 'uri')
        )

        can_update_first_dayleave_udf = rail.IfOperator(
            task_id='can_update_first_dayleave_udf',
            test="{{ result('log_message_gettherequiredcustomfield_uri_first_dayof_leave') | is_truthy }}",
            yes_task="update_first_dayof_leave_udf",
            no_task="has_change_in_lasthiredate2",
        )

        update_first_dayof_leave_udf = rail.RepliconServiceOperator(
            task_id='update_first_dayof_leave_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_message_gettherequiredcustomfield_uri_first_dayof_leave') }}",
                "value": "{{ dag_run.conf.firstdayofleave }}"
            }
        )

        has_change_in_lasthiredate2 = rail.IfOperator(
            task_id='has_change_in_lasthiredate2',
            test="{{ dag_run.conf.lasthiredate2 | is_truthy and dag_run.conf.lasthiredate2 != result('parse_csv_user_data')['Last Hire Date2'] }}",
            yes_task="log_message_gettherequiredcustomfield_uri_last_hire_date2",
            no_task="has_last_record_update",
        )

        log_message_gettherequiredcustomfield_uri_last_hire_date2 = rail.PythonOperator(
            task_id='log_message_gettherequiredcustomfield_uri_last_hire_date2',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                get_custom_fielduri.task_id), 'displayText', 'Last Hire Date2', 'uri')

        )

        can_update_last_hire_date2_udf = rail.IfOperator(
            task_id='can_update_last_hire_date2_udf',
            test="{{ result('log_message_gettherequiredcustomfield_uri_last_hire_date2') | is_truthy }}",
            yes_task="update_last_hire_date2_udf",
            no_task="has_last_record_update",
        )

        update_last_hire_date2_udf = rail.RepliconServiceOperator(
            task_id='update_last_hire_date2_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_message_gettherequiredcustomfield_uri_last_hire_date2') }}",
                "value": "{{ dag_run.conf.lasthiredate2 }}"
            }
        )

        has_last_record_update = rail.IfOperator(
            task_id='has_last_record_update',
            test="{{ dag_run.conf.lastrecordupdate | is_truthy and dag_run.conf.lastrecordupdate != result('parse_csv_user_data')['Last Record Update'] }}",
            yes_task="log_message_gettherequiredcustomfield_uri_jobcode",
            no_task="has_company",
        )

        log_message_gettherequiredcustomfield_uri_jobcode = rail.PythonOperator(
            task_id='log_message_gettherequiredcustomfield_uri_jobcode',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                get_custom_fielduri.task_id), 'displayText', 'Last Record Update', 'uri')

        )

        can_update_last_record_update = rail.IfOperator(
            task_id='can_update_last_record_update',
            test="{{ result('log_message_gettherequiredcustomfield_uri_jobcode') | is_truthy }}",
            yes_task="update_last_record_update",
            no_task="has_company",
        )

        update_last_record_update = rail.RepliconServiceOperator(
            task_id='update_last_record_update',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_message_gettherequiredcustomfield_uri_jobcode') }}",
                "value": "{{ dag_run.conf.lastrecordupdate }}"
            }
        )

        has_company = rail.IfOperator(
            task_id='has_company',
            test="{{ dag_run.conf.company | is_truthy }}",
            yes_task="has_company_kla",
            no_task="has_company_changed",
        )

        has_company_kla = rail.IfOperator(
            task_id='has_company_kla',
            test="{{ dag_run.conf.company  == 'KLA' }}",
            yes_task="log_message_companycode_kla",
            no_task="has_company_vlsi",
        )

        log_message_companycode_kla = rail.PythonOperator(
            task_id='log_message_companycode_kla',
            python_callable=lambda:  '1000'
        )

        has_company_vlsi = rail.IfOperator(
            task_id='has_company_vlsi',
            test="{{ dag_run.conf.company  == 'VLSI' }}",
            yes_task="log_message_companycode_vlsi",
            no_task="log_message_finalvaluefor_company_code",
        )

        log_message_companycode_vlsi = rail.PythonOperator(
            task_id='log_message_companycode_vlsi',
            python_callable=lambda:  '1010'
        )

        log_message_finalvaluefor_company_code = rail.PythonOperator(
            task_id='log_message_finalvaluefor_company_code',
            python_callable=lambda: rail.result(
                'log_message_companycode_kla') or rail.result('log_message_companycode_vlsi')
        )

        has_company_changed = rail.IfOperator(
            task_id='has_company_changed',
            test="{{ result('log_message_finalvaluefor_company_code') | is_truthy and result('log_message_finalvaluefor_company_code') != result('parse_csv_user_data')['Company Code'] }}",
            yes_task="log_message_gettherequiredcustomfield_uri_company_code",
            no_task="log_message_final_valuefor_pdr_country",
        )

        log_message_gettherequiredcustomfield_uri_company_code = rail.PythonOperator(
            task_id='log_message_gettherequiredcustomfield_uri_company_code',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result(get_custom_fielduri.task_id), 'displayText', 'Company Code', 'uri')
        )

        has_company_code_udf = rail.IfOperator(
            task_id='has_company_code_udf',
            test="{{ result('log_message_gettherequiredcustomfield_uri_company_code') | is_truthy }}",
            yes_task="get_enabled_custom_field_drop_down_optionsfor_company_code",
            no_task="log_message_final_valuefor_pdr_country",
        )

        get_enabled_custom_field_drop_down_optionsfor_company_code = rail.RepliconServiceOperator(
            task_id='get_enabled_custom_field_drop_down_optionsfor_company_code',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_message_gettherequiredcustomfield_uri_company_code') }}"
            }
        )

        log_message_gettherequireddropdownoption_pdrcountry_uri = rail.PythonOperator(
            task_id='log_message_gettherequireddropdownoption_pdrcountry_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result(
                    get_enabled_custom_field_drop_down_optionsfor_company_code.task_id),
                'displayText',
                rail.result('log_message_finalvaluefor_company_code'), 'uri')
        )

        has_compnaycode_dropdown_uri = rail.IfOperator(
            task_id='has_compnaycode_dropdown_uri',
            test="{{ result('log_message_gettherequireddropdownoption_pdrcountry_uri') | is_truthy }}",
            yes_task="update_company_udf",
            no_task="log_message_final_valuefor_pdr_country",
        )

        update_company_udf = rail.RepliconServiceOperator(
            task_id='update_company_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_message_gettherequiredcustomfield_uri_company_code') }}",
                "customFieldDropDownOptionUri": "{{ result('log_message_gettherequireddropdownoption_pdrcountry_uri') }}"
            }
        )

        log_message_final_valuefor_pdr_country = rail.PythonOperator(
            task_id='log_message_final_valuefor_pdr_country',
            python_callable=lambda: "USA" if get_conf()['pdrcountry'] and 'USA' in get_conf(
            )['pdrcountry'] else "Non-USA" if get_conf()['pdrcountry'] else null
        )

        has_country_changed = rail.IfOperator(
            task_id='has_country_changed',
            test="{{ result('log_message_final_valuefor_pdr_country') | is_truthy and result('log_message_final_valuefor_pdr_country') != result('parse_csv_user_data')['PDR Country'] }}",
            yes_task="log_message_gettherequiredcustomfield_uri_pdr_country",
            no_task="get_all_permission_sets",
        )

        log_message_gettherequiredcustomfield_uri_pdr_country = rail.PythonOperator(
            task_id='log_message_gettherequiredcustomfield_uri_pdr_country',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result(get_custom_fielduri.task_id), 'displayText', 'PDR Country', 'uri')
        )

        has_pdrcountry_udf_uri = rail.IfOperator(
            task_id='has_pdrcountry_udf_uri',
            test="{{ result('log_message_gettherequiredcustomfield_uri_pdr_country') | is_truthy }}",
            yes_task="get_enabled_custom_field_drop_down_options_for_company_code",
            no_task="get_all_permission_sets",
        )

        get_enabled_custom_field_drop_down_options_for_company_code = rail.RepliconServiceOperator(
            task_id='get_enabled_custom_field_drop_down_options_for_company_code',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_message_gettherequiredcustomfield_uri_pdr_country') }}"
            }
        )

        log_message_gettherequireddropdownoption_pdrcountry_uri2 = rail.PythonOperator(
            task_id='log_message_gettherequireddropdownoption_pdrcountry_uri2',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(get_enabled_custom_field_drop_down_options_for_company_code.task_id),
                                                                         'displayText', rail.result('log_message_final_valuefor_pdr_country'), 'uri')
        )

        has_dropdownoption_pdr_country_uri = rail.IfOperator(
            task_id='has_dropdownoption_pdr_country_uri',
            test="{{ result('log_message_gettherequireddropdownoption_pdrcountry_uri2') | is_truthy }}",
            yes_task="update_pdr_country_udf",
            no_task="get_all_permission_sets",
        )

        update_pdr_country_udf = rail.RepliconServiceOperator(
            task_id='update_pdr_country_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('log_message_gettherequiredcustomfield_uri_pdr_country') }}",
                "customFieldDropDownOptionUri": "{{ result('log_message_gettherequireddropdownoption_pdrcountry_uri2') }}"
            }
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        log_message_permissionfor_manager_supervisor = rail.PythonOperator(
            task_id='log_message_permissionfor_manager_supervisor',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(get_all_permission_sets.task_id),
                                                                         'displayText', "Manager's Supervisor", 'uri')
        )

        log_message_permissionfor_manager_basic_user = rail.PythonOperator(
            task_id='log_message_permissionfor_manager_basic_user',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(get_all_permission_sets.task_id),
                                                                         'displayText', "Manager Basic User", 'uri')
        )

        has_supervisorid = rail.IfOperator(
            task_id='has_supervisorid',
            test="{{ dag_run.conf.supervisorid | is_truthy }}",
            yes_task="searchuser_supervisor",
            no_task="has_usdirectreport",
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
            test="{{ result('searchuser_supervisor') | is_truthy and result('searchuser_supervisor').uri != result('parse_csv_user_data')['supervisoruri'] }}",
            yes_task="can_update_supervisor",
            no_task="has_no_supervisor_found",
        )

        can_update_supervisor = rail.IfOperator(
            task_id='can_update_supervisor',
            test="{{ result('searchuser_supervisor').uri | is_truthy and result('searchuser_supervisor').status == 'True' and result('searchuser_supervisor').uri != dag_run.conf.useruri }}",
            yes_task="get_assigned_permission_sets_for_supervisor",
            no_task="has_usdirectreport",
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
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('searchuser_supervisor').uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{result('log_message_today').year}}",
                        "month": "{{result('log_message_today').month}}",
                        "day": "{{result('log_message_today').day}}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
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
                "useruri": rail.get_dag_run_conf()['useruri'],
                "supervisorid": rail.get_dag_run_conf()['supervisorid'],
                "loginname": rail.get_dag_run_conf()['loginname']
            }
        )

        log_message_tobeusedinfinallogmessage_disabled = rail.PythonOperator(
            task_id='log_message_tobeusedinfinallogmessage_disabled',
            python_callable=lambda: "Supervisor not assigned since the supervisor profile is disabled in Replicon" if rail.result(
                'searchuser_supervisor') and rail.result('searchuser_supervisor').get('uri') and rail.result('searchuser_supervisor')['status'] != 'True' else ''
        )

        has_usdirectreport = rail.IfOperator(
            task_id='has_usdirectreport',
            test="{{'Y' in dag_run.conf.hasusdirectreport}}",
            yes_task="get_assigned_permission_sets_for_supervisor_directreport",
            no_task="log_message_final_locationvaluenew",
        )

        get_assigned_permission_sets_for_supervisor_directreport = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_supervisor_directreport',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_message_checkif_supervisorpermissionisassigned2 = rail.PythonOperator(
            task_id='log_message_checkif_supervisorpermissionisassigned2',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_assigned_permission_sets_for_supervisor_directreport'),
                                                                         'policyUri', 'urn:replicon:policy:supervision', 'user.uri')
        )

        has_no_supervisorpermissionisassigned = rail.IfOperator(
            task_id='has_no_supervisorpermissionisassigned',
            test="{{ result('log_message_checkif_supervisorpermissionisassigned2') | is_falsy }}",
            yes_task="get_all_non_user_permission_policy2",
            no_task="log_message_final_locationvaluenew",
        )

        get_all_non_user_permission_policy2 = rail.PythonOperator(
            task_id='get_all_non_user_permission_policy2',
            python_callable=lambda: list(map(lambda x: x['permissionSet']['uri'],
                                             filter(lambda x: x['policyUri'] != 'urn:replicon:policy:user',
                                                    rail.result('get_assigned_permission_sets_for_supervisor_directreport'))))
        )

        log_message_new_permission_setforsupervisor_us = rail.PythonOperator(
            task_id='log_message_new_permission_setforsupervisor_us',
            python_callable=lambda: [rail.result('log_message_permissionfor_manager_supervisor'), rail.result(
                'log_message_permissionfor_manager_basic_user')]
        )

        log_message_permission_setsfor_user_us = rail.PythonOperator(
            task_id='log_message_permission_setsfor_user_us',
            python_callable=lambda: rail.result(
                'get_all_non_user_permission_policy2') + rail.result('log_message_new_permission_setforsupervisor_us')
        )

        put_permission_set_assignments_forsupervisorofthe_user_us = rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_forsupervisorofthe_user_us',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda: {
                "userUri": get_conf()['useruri'],
                "permissionSetUris": rail.result('log_message_permission_setsfor_user_us')
            }
        )

        log_message_final_locationvaluenew = rail.PythonOperator(
            task_id='log_message_final_locationvaluenew',
            python_callable=lambda: get_conf()['location'] if get_conf()[
                'location'] else "Non-USA"
        )

        getlocation_data_final = rail.RepliconServiceOperator(
            task_id='getlocation_data_final',
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
            data_handler=lambda data: next(iter(filter(lambda x: x['code'] == get_conf()['location'],
                                                       map(lambda x: {
                                                           "uri": x['cells'][0].get('uri'),
                                                           "name": x['cells'][0].get('textValue'),
                                                           "code": x['cells'][1].get('textValue'),
                                                       }, data['rows']))), None)
        )

        log_message_messagetobeusedinfinallog_locnotfound = rail.PythonOperator(
            task_id='log_message_messagetobeusedinfinallog_locnotfound',
            python_callable=lambda: f"Location not assigned. Location with the code {get_conf()['location']} doesn't exist in Replicon" if not rail.result(
                'getlocation_data_final') else ''
        )

        can_update_location = rail.IfOperator(
            task_id='can_update_location',
            test="{{ result('getlocation_data_final') | is_truthy and result('parse_csv_user_data')['Location (Current)'] != result('getlocation_data_final').name }}",
            yes_task="update_location_schedule_for_user",
            no_task="has_cost_center",
        )

        update_location_schedule_for_user = rail.RepliconServiceOperator(
            task_id='update_location_schedule_for_user',
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
                                        "uri": "{{ result('getlocation_data_final').uri }}",
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year": "{{result('log_message_today').year}}",
                                        "month": "{{result('log_message_today').month}}",
                                        "day": "{{result('log_message_today').day}}"
                                    }
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

        has_cost_center = rail.IfOperator(
            task_id='has_cost_center',
            test="{{ dag_run.conf.costcenter | is_truthy and dag_run.conf.costcenter != result('parse_csv_user_data')['Cost Center (Current)']}}",
            yes_task="get_cost_center",
            no_task="search_entries_for_holiday_calendar",
        )

        get_cost_center = rail.RepliconServiceOperator(
            task_id='get_cost_center',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data_handler=lambda data: rail.find_first_by_attr_and_get_attr(
                data, 'displayText', get_conf()['costcenter'], 'uri')

        )

        can_update_cost_center = rail.IfOperator(
            task_id='can_update_cost_center',
            test="{{ result('get_cost_center') | is_truthy }}",
            yes_task="update_cost_center",
            no_task="search_entries_for_holiday_calendar",
        )

        update_cost_center = rail.RepliconServiceOperator(
            task_id='update_cost_center',
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
                    "costCenterScheduleToApply": {
                        "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementCostCenterSchedule": [],
                        "updateCostCenterScheduleOverDateRange": {
                            "replacementCostCenterScheduleEntries": [
                                {
                                    "costCenter": {
                                        "uri": "{{ result('get_cost_center') }}",
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year": "{{result('log_message_today').year}}",
                                        "month": "{{result('log_message_today').month}}",
                                        "day": "{{result('log_message_today').day}}"
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    },
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

        search_entries_for_holiday_calendar = rail.PythonOperator(
            task_id='search_entries_for_holiday_calendar',
            python_callable=lambda: next(iter(filter(lambda x: x['lookup'] == "holiday calendar" and x["Employee type"] == rail.result(
                'log_message_derive_employeetype'), general_mapper)), {}).get('Value')
        )

        get_holiday_calendar_uri = rail.RepliconServiceOperator(
            task_id='get_holiday_calendar_uri',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda data: rail.find_first_by_attr_and_get_attr(
                data, 'name', rail.result('search_entries_for_holiday_calendar'), 'uri')
        )

        can_update_holiday_calendar = rail.IfOperator(
            task_id='can_update_holiday_calendar',
            test="{{ result('search_entries_for_holiday_calendar') | is_truthy and result('get_holiday_calendar_uri') | is_truthy and  result('search_entries_for_holiday_calendar') != result('parse_csv_user_data')['Holiday Calendar']}}",
            yes_task="update_holiday_calendar",
            no_task="has_removed_holiday_calendar",
        )

        update_holiday_calendar = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": "{{ result('get_holiday_calendar_uri') }}"
            }
        )

        has_removed_holiday_calendar = rail.IfOperator(
            task_id='has_removed_holiday_calendar',
            test="{{ result('search_entries_for_holiday_calendar') | is_falsy and result('parse_csv_user_data')['Holiday Calendar'] | is_truthy }}",
            yes_task="remove_holiday_calendar",
            no_task="has_hourly_group_emptype_loc",
        )

        remove_holiday_calendar = rail.RepliconServiceOperator(
            task_id='remove_holiday_calendar',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": null
            }
        )

        has_hourly_group_emptype_loc = rail.IfOperator(
            task_id='has_hourly_group_emptype_loc',
            test="{{ result('log_message_derive_employeetype') == 'Regular Hourly' or result('log_message_derive_employeetype') == 'Temporary/Interns - Hourly'}}",
            yes_task="log_message_final_valueforlocationtobeusedforlookup",
            no_task="search_entries_for_payrule",
        )

        log_message_final_valueforlocationtobeusedforlookup = rail.PythonOperator(
            task_id='log_message_final_valueforlocationtobeusedforlookup',
            python_callable=lambda: get_conf()['location'] if get_conf()[
                'location'] in ['CA', 'CO'] else 'nonca'
        )

        search_entries_for_payrule = rail.PythonOperator(
            task_id='search_entries_for_payrule',
            python_callable=lambda: next(iter(filter(lambda x: x['lookup'] == "pay rule" and x["Employee type"] == rail.result('log_message_derive_employeetype')
                                                     and x['Additional'] == rail.result('log_message_final_valueforlocationtobeusedforlookup'), general_mapper)), {}).get('Value')
        )

        has_search_entries_for_payrule = rail.IfOperator(
            task_id='has_search_entries_for_payrule',
            test="{{ result('search_entries_for_payrule') | is_truthy }}",
            yes_task="get_asssigned_payrulelist_map",
            no_task="search_entries_for_activity",
        )

        get_asssigned_payrulelist_map = rail.RepliconServiceOperator(
            task_id='get_asssigned_payrulelist_map',
            endpoint="/services/PayRuleScriptService2.svc/GetPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda data: list(map(lambda x: {
                "payRuleScript": {
                    "uri": x['payRuleScript']['uri'],
                    "name": x['payRuleScript']['displayText']
                },
                "effectiveDate": x['effectiveDate']
            }, data))
        )

        map_current_payrule_script = rail.RepliconServiceOperator(
            task_id='map_current_payrule_script',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda data: {
                "payRuleScript": {
                    "uri": rail.find_first_by_attr_and_get_attr(data, 'displayText', rail.result('search_entries_for_payrule'), 'uri'),
                    "name": rail.find_first_by_attr_and_get_attr(data, 'displayText', rail.result('search_entries_for_payrule'), 'displayText'),
                },
                "effectiveDate": {
                    "year": rail. result('log_message_today')['year'],
                    "month": rail.result('log_message_today')['month'],
                    "day": rail.result('log_message_today')['day']
                }
            }
        )

        can_update_payrule_script_name = rail.IfOperator(
            task_id='can_update_payrule_script_name',
            test="{{ result('search_entries_for_payrule') | is_truthy and result('search_entries_for_payrule') != result('parse_csv_user_data')['Pay Rule Name (Current)'] }}",
            yes_task="put_pay_rule_script_assignment_schedule_for_user",
            no_task="search_entries_for_activity",
        )

        def get_date(replicon_date_obj):
            if not replicon_date_obj:
                return None
            return datetime(**replicon_date_obj)
        put_pay_rule_script_assignment_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_pay_rule_script_assignment_schedule_for_user',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda: {
                "userUri": get_conf()['useruri'],
                "scheduleEntries": list(
                    filter(lambda x: get_date(x['effectiveDate']) != get_date(rail.result('log_message_today')),
                           rail.result('get_asssigned_payrulelist_map'))) + [rail.result('map_current_payrule_script')]
            }
        )

        search_entries_for_activity = rail.PythonOperator(
            task_id='search_entries_for_activity',
            python_callable=lambda: next(iter(filter(lambda x: x['lookup'] == "activity" and x["Employee type"] == rail.result(
                'log_message_derive_employeetype'), general_mapper)), {}).get('Value')
        )

        get_all_activities = rail.RepliconServiceOperator(
            task_id='get_all_activities',
            endpoint="/services/ActivityService1.svc/GetAllActivities",

        )

        can_assign_activity = rail.IfOperator(
            task_id='can_assign_activity',
            test="{{ result('search_entries_for_activity') | is_truthy }}",
            yes_task="assign_activity",
            no_task="can_enable_login",
        )

        assign_activity = rail.RepliconServiceOperator(
            task_id='assign_activity',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda: {
                "userUri": get_conf()['useruri'],
                "activityUris": list(
                    map(lambda x: rail.find_first_by_attr_and_get_attr(rail.result('get_all_activities'), 'name', x, 'uri'),
                        rail.result('search_entries_for_activity').split('|')))
            }
        )

        can_enable_login = rail.IfOperator(
            task_id='can_enable_login',
            test="{{ result('parse_csv_user_data')['User Status'] == 'Disabled' and  dag_run.conf.empl_status == 'Active'}}",
            yes_task="re_enable_userprofile",
            no_task="has_empl_status_active",
        )

        re_enable_userprofile = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        can_update_user_timeoff = rail.IfOperator(
            task_id='can_update_user_timeoff',
            test="{{ result('log_message_derive_employeetype') != result('parse_csv_user_data')['Employee Type']}}",
            yes_task="process_update_user_timeoff",
            no_task="add_rehire_success_log",
        )

        process_update_user_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id='process_update_user_timeoff',
            retries=0,
            items=[1],
            trigger_dag_id=f'kla_user_import_usa_update_user_timeoff_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf={
                "useruri": "{{ dag_run.conf.useruri }}",
                "employeetype": "{{ result('log_message_derive_employeetype') }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "department": "{{ dag_run.conf.department }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "lasthiredate2": "{{ dag_run.conf.lasthiredate2 }}",
                "returntoworkdate": "{{ dag_run.conf.returntoworkdate }}",
                "enddate": "{{ dag_run.conf.enddate }}",
                "emailaddress": "{{ dag_run.conf.emailaddress }}",
                "supervisorid": "{{ dag_run.conf.supervisorid }}",
                "lastrecordupdate": "{{ dag_run.conf.lastrecordupdate }}",
                "userstatus": "{{ dag_run.conf.empl_status }}"
            }
        )

        wait_for_process_update_user_timeoff = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_user_timeoff',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("process_update_user_timeoff") }}'
        )

        add_rehire_success_log = rail.WriteLogOperator(
            task_id='add_rehire_success_log',
            log="{{ dag_run.conf.log }}",
            message="{{ result('log_message_tobeusedinfinallogmessage_disabled') | sn }} {{ result('log_message_messagetobeusedinfinallog_locnotfound') | sn }}",
            severity="Success",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Rehire User|{{ dag_run.conf.employeeid }}",
                "status": "Success",
                "message": "{{ result('log_message_tobeusedinfinallogmessage_disabled') | sn }} {{ result('log_message_messagetobeusedinfinallog_locnotfound') | sn }}"
            }
        )

        has_empl_status_active = rail.IfOperator(
            task_id='has_empl_status_active',
            test="{{ 'Active' in dag_run.conf.empl_status and 'Enabled' in result('parse_csv_user_data')['User Status'] }}",
            yes_task="has_employee_type",
            no_task="has_empl_status_terminated",
        )

        has_employee_type = rail.IfOperator(
            task_id='has_employee_type',
            test="{{ result('log_message_derive_employeetype') != result('parse_csv_user_data')['Employee Type']}}",
            yes_task="process_update_user_timeoff2",
            no_task="has_pdr_country_value",
        )

        process_update_user_timeoff2 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_update_user_timeoff2',
            retries=0,
            items=[1],
            trigger_dag_id=f'kla_user_import_usa_update_user_timeoff_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf={
                "useruri": "{{ dag_run.conf.useruri }}",
                "employeetype": "{{ result('log_message_derive_employeetype') }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "department": "{{ dag_run.conf.department }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "lasthiredate2": "{{ dag_run.conf.lasthiredate2 }}",
                "returntoworkdate": "{{ dag_run.conf.returntoworkdate }}",
                "enddate": "{{ dag_run.conf.enddate }}",
                "emailaddress": "{{ dag_run.conf.emailaddress }}",
                "supervisorid": "{{ dag_run.conf.supervisorid }}",
                "lastrecordupdate": "{{ dag_run.conf.lastrecordupdate }}",
                "userstatus": "{{ dag_run.conf.empl_status }}"
            }
        )

        wait_for_process_update_user_timeoff2 = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_user_timeoff2',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("process_update_user_timeoff2") }}'
        )

        has_pdr_country_value = rail.IfOperator(
            task_id='has_pdr_country_value',
            test="{{ result('log_message_final_valuefor_pdr_country') | is_truthy }}",
            yes_task="process_update_user_isolation_timeoff",
            no_task="has_empl_status_terminated",
        )

        process_update_user_isolation_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id='process_update_user_isolation_timeoff',
            retries=0,
            items=[1],
            trigger_dag_id=f'kla_user_import_usa_update_user_enable_isolation_time_off_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf={
                "useruri": "{{ dag_run.conf.useruri }}",
                "employeetype": "{{ result('log_message_derive_employeetype') }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "loginname": "{{ dag_run.conf.loginname }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "department": "{{ dag_run.conf.department }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "lasthiredate2": "{{ dag_run.conf.lasthiredate2 }}",
                "returntoworkdate": "{{ dag_run.conf.returntoworkdate }}",
                "enddate": "{{ dag_run.conf.enddate }}",
                "emailaddress": "{{ dag_run.conf.emailaddress }}",
                "supervisorid": "{{ dag_run.conf.supervisorid }}",
                "lastrecordupdate": "{{ dag_run.conf.lastrecordupdate }}",
                "userstatus": "{{ dag_run.conf.empl_status }}",
                "rehire": "false"
            }
        )

        wait_for_process_update_user_isolation_timeoff = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_user_isolation_timeoff',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("process_update_user_isolation_timeoff") }}'
        )

        add_success_log = rail.WriteLogOperator(
            task_id='add_success_log',
            log="{{ dag_run.conf.log }}",
            message="{{ result('log_message_tobeusedinfinallogmessage_disabled') | sn }} {{ result('log_message_messagetobeusedinfinallog_locnotfound') | sn }}",
            severity="Success",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Update User|{{dag_run.conf.employeeid }}",
                "status": "Success",
                "message": "{{ result('log_message_tobeusedinfinallogmessage_disabled') | sn }} {{ result('log_message_messagetobeusedinfinallog_locnotfound') | sn }}"
            }
        )

        has_empl_status_terminated = rail.IfOperator(
            task_id='has_empl_status_terminated',
            test="{{ dag_run.conf.empl_status == 'Terminated' }}",
            yes_task="has_user_status_is_enabled",
            no_task="has_empl_status_leave",
        )

        has_user_status_is_enabled = rail.IfOperator(
            task_id='has_user_status_is_enabled',
            test="{{ result('parse_csv_user_data')['User Status'] == 'Enabled' }}",
            yes_task="disable_userprofile_2",
            no_task="add_user_already_disabled_log",
        )

        disable_userprofile_2 = rail.RepliconServiceOperator(
            task_id='disable_userprofile_2',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        add_disabled_user_log1 = rail.WriteLogOperator(
            task_id='add_disabled_user_log1',
            log="{{ dag_run.conf.log }}",
            message="{{ result('log_message_tobeusedinfinallogmessage_disabled') | sn }} {{ result('log_message_messagetobeusedinfinallog_locnotfound') | sn }}",
            severity="Success",
            properties={

                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Disable User|{{dag_run.conf.employeeid }}",
                "status": "Success",
                "message": "{{ result('log_message_tobeusedinfinallogmessage_disabled') | sn }} {{ result('log_message_messagetobeusedinfinallog_locnotfound') | sn }}"
            }
        )

        add_user_already_disabled_log = rail.WriteLogOperator(
            task_id='add_user_already_disabled_log',
            log="{{ dag_run.conf.log }}",
            message="User is already Disabled",
            severity="Exception",
            properties={

                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Disable User|{{dag_run.conf.employeeid }}",
                "status": "Exception",
                "message": "User is already Disabled"
            }
        )

        has_empl_status_leave = rail.IfOperator(
            task_id='has_empl_status_leave',
            test="{{ dag_run.conf.empl_status == 'Leave' }}",
            yes_task="has_emp_type_hourly2",
            no_task="finish",
        )

        has_emp_type_hourly2 = rail.IfOperator(
            task_id='has_emp_type_hourly2',
            test="{{ 'Hourly' in result('log_message_derive_employeetype') }}",
            yes_task="has_emp_status_enabled2",
            no_task="has_user_status_enabled_4",
        )

        has_emp_status_enabled2 = rail.IfOperator(
            task_id='has_emp_status_enabled2',
            test="{{ result('parse_csv_user_data')['User Status'] == 'Enabled' }}",
            yes_task="disable_userprofile_leave",
            no_task="add_user_already_disabled_log4",
        )

        disable_userprofile_leave = rail.RepliconServiceOperator(
            task_id='disable_userprofile_leave',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        add_disable_user_leave_log = rail.WriteLogOperator(
            task_id='add_disable_user_leave_log',
            log="{{ dag_run.conf.log }}",
            message="na",
            severity="Success",
            properties={

                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Disable User|{{dag_run.conf.employeeid }}",
                "status": "Success",
                "message": "{{ result('log_message_tobeusedinfinallogmessage_disabled') | sn }} {{ result('log_message_messagetobeusedinfinallog_locnotfound') | sn }}"
            }
        )

        add_user_already_disabled_log4 = rail.WriteLogOperator(
            task_id='add_user_already_disabled_log4',
            log="{{ dag_run.conf.log }}",
            message="User is already Disabled",
            severity="Exception",
            properties={

                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Disable User|{{dag_run.conf.employeeid }}",
                "status": "Exception",
                "message": "User is already Disabled"
            }
        )

        has_user_status_enabled_4 = rail.IfOperator(
            task_id='has_user_status_enabled_4',
            test="{{ result('parse_csv_user_data')['User Status'] == 'Enabled' }}",
            yes_task="get_assigned_permission_sets_for_user4",
            no_task="add_user_already_disabled_log2",
        )

        get_assigned_permission_sets_for_user2_3 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user4',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_message_checkif_supervisorpermissionisassigned_3 = rail.PythonOperator(
            task_id='log_message_checkif_supervisorpermissionisassigned_3',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_assigned_permission_sets_for_user2_3'),
                                                                         'policyUri', 'urn:replicon:policy:user', 'permissionSet.displayText')
        )

        has_no_manager_basic_user_perm = rail.IfOperator(
            task_id='has_no_manager_basic_user_perm',
            test="{{ result('log_message_checkif_supervisorpermissionisassigned_3') != 'Manager Basic User' }}",
            yes_task="disable_userprofile_5",
            no_task="add_supervisor_perm_error",
        )

        disable_userprofile_5 = rail.RepliconServiceOperator(
            task_id='disable_userprofile_5',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        add_success_log_supervisorcheck = rail.WriteLogOperator(
            task_id='add_success_log_supervisorcheck',
            log="{{ dag_run.conf.log }}",
            message="get message from prop ",
            severity="get severity from prop ",
            properties={

                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Disable User|{{dag_run.conf.employeeid }}",
                "status": "Success",
                "message": "{{ result('log_message_tobeusedinfinallogmessage_disabled') | sn }} {{ result('log_message_messagetobeusedinfinallog_locnotfound') | sn }}"
            }
        )

        add_supervisor_perm_error = rail.WriteLogOperator(
            task_id='add_supervisor_perm_error',
            log="{{ dag_run.conf.log }}",
            message="User is Salaried and has supervisor permission assigned.",
            severity="Exception",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Disable User|{{dag_run.conf.employeeid }}",
                "status": "Exception",
                "message": "User is Salaried and has supervisor permission assigned."
            }
        )

        add_user_already_disabled_log2 = rail.WriteLogOperator(
            task_id='add_user_already_disabled_log2',
            log="{{ dag_run.conf.log }}",
            message="User is already Disabled",
            severity="Exception",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Disable User|{{dag_run.conf.employeeid }}",
                "status": "Exception",
                "message": "User is already Disabled"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log }}",
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Update User|{{dag_run.conf.employeeid }}",
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
        can_run_batch_task >> rail.Label(
            'No') >> has_no_valid_data

        has_no_valid_data
        has_no_valid_data >> rail.Label(
            'Yes') >> add_invalid_data_log >> finish
        has_no_valid_data >> rail.Label(
            'No') >> log_message_today >> log_message_userfiltervaluebasedonuseruri >> log_message_report_name >> get_user_report_details >> log_message_userfilter_uri >> generate_user_report >> load_csv_user_data >> parse_csv_user_data >> log_message_derive_employeetype >> can_update_employee_type
        can_update_employee_type >> rail.Label(
            'Yes') >> update_employee_type >> update_emptype_email_group_division >> can_update_first_name
        can_update_employee_type >> rail.Label('No') >> can_update_first_name
        can_update_first_name >> rail.Label(
            'Yes') >> update_first_name >> can_update_last_name
        can_update_first_name >> rail.Label('No') >> can_update_last_name
        can_update_last_name >> rail.Label(
            'Yes') >> update_last_name >> can_update_loginname
        can_update_last_name >> rail.Label('No') >> can_update_loginname
        can_update_loginname >> rail.Label(
            'Yes') >> update_loginname >> can_update_department
        can_update_loginname >> rail.Label('No') >> can_update_department
        can_update_department >> rail.Label(
            'Yes') >> get_enabled_departments >> log_message_departmenturi >> has_department_uri
        can_update_department >> rail.Label('No') >> can_update_email
        has_department_uri >> rail.Label(
            'Yes') >> update_department_user >> can_update_email
        has_department_uri >> rail.Label(
            'No') >> can_update_email
        can_update_email >> rail.Label(
            'No') >> log_message_start_date
        can_update_email >> rail.Label(
            'Yes') >> update_emailaddress >> log_message_start_date
        log_message_start_date >> log_message_end_date >> can_update_enddate
        can_update_enddate >> rail.Label(
            'Yes') >> update_user_end_date >> can_remove_user_end_date
        can_update_enddate >> rail.Label('No') >> can_remove_user_end_date
        can_remove_user_end_date >> rail.Label(
            'Yes') >> remove_user_end_date >> can_update_user_start_date
        can_remove_user_end_date >> rail.Label(
            'No') >> can_update_user_start_date
        can_update_user_start_date >> rail.Label(
            'Yes') >> update_user_start_date >> get_all_policy_setsfor_timesheetsandtimeoffs
        can_update_user_start_date >> rail.Label(
            'No') >> get_all_policy_setsfor_timesheetsandtimeoffs
        get_all_policy_setsfor_timesheetsandtimeoffs >> has_salary_pdr_country_usa
        has_salary_pdr_country_usa >> rail.Label(
            'Yes') >> has_salary_emptype
        has_salary_pdr_country_usa >> rail.Label(
            'No') >> log_message_finalvalueispresentfortimesheettemplate
        has_salary_emptype >> rail.Label(
            'Yes') >> log_message_employeetype_salary >> has_hourly_emptype
        has_salary_emptype >> rail.Label('No') >> has_hourly_emptype
        has_hourly_emptype >> rail.Label(
            'Yes') >> log_message_employeetype_hourly >> has_hourly_nonca_emptype
        has_hourly_emptype >> rail.Label('No') >> has_hourly_nonca_emptype
        has_hourly_nonca_emptype >> rail.Label(
            'Yes') >> log_message_employeetype_hourly_non_ca >> log_message_finalvalueispresentfortimesheettemplate
        has_hourly_nonca_emptype >> rail.Label(
            'No') >> log_message_finalvalueispresentfortimesheettemplate
        log_message_finalvalueispresentfortimesheettemplate >> has_timesheet_template
        has_timesheet_template >> rail.Label(
            'Yes') >> log_message_gettherequiredtimesheettemplate_uri >> update_timesheet_template >> has_blank_timesheet_template
        has_timesheet_template >> rail.Label(
            'No') >> has_blank_timesheet_template
        has_blank_timesheet_template >> rail.Label(
            'Yes') >> log_message_gettherequiredtimesheettemplate_uri_to_remove >> has_timesheet_template_uri_remove
        has_blank_timesheet_template >> rail.Label(
            'No') >> has_hourlyca_emptype
        has_timesheet_template_uri_remove >> rail.Label(
            'Yes') >> remove_timesheet_template >> has_hourlyca_emptype
        has_timesheet_template_uri_remove >> rail.Label(
            'No') >> has_hourlyca_emptype
        has_hourlyca_emptype >> rail.Label(
            'Yes') >> log_message_punchentrypolicy_ca >> has_hourlynonca_emptype
        has_hourlyca_emptype >> rail.Label('No') >> has_hourlynonca_emptype
        has_hourlynonca_emptype >> rail.Label(
            'Yes') >> log_message_punchentrypolicy_nonca >> log_message_checkifthevalueispresentfor_punch_entry_policy >> can_update_punch_entry_policy
        has_hourlynonca_emptype >> rail.Label(
            'No') >> log_message_checkifthevalueispresentfor_punch_entry_policy >> can_update_punch_entry_policy
        can_update_punch_entry_policy >> rail.Label(
            'Yes') >> log_message_gettherequired_punch_entry_policy_uri >> update_punch_entry_policy >> has_blank_punch_entry_policy
        can_update_punch_entry_policy >> rail.Label(
            'No') >> has_blank_punch_entry_policy
        has_blank_punch_entry_policy >> rail.Label(
            'Yes') >> log_message_gettherequired_punch_entry_policyuri_remove >> has_punch_policy_remove
        has_blank_punch_entry_policy >> rail.Label(
            'No') >> has_punch_policy_remove
        has_punch_policy_remove >> rail.Label(
            'Yes') >> remove_punch_entry_policy >> has_usa_pdr_country_salary
        has_punch_policy_remove >> rail.Label(
            'No') >> has_usa_pdr_country_salary
        has_usa_pdr_country_salary >> rail.Label(
            'Yes') >> log_message_approvalpath_system >> has_usa_pdr_country_hourly
        has_usa_pdr_country_salary >> rail.Label(
            'No') >> has_usa_pdr_country_hourly
        has_usa_pdr_country_hourly >> rail.Label(
            'Yes') >> log_message_approvalpath_supervisor >> log_message_checkifthevalueispresentfortimesheetapprovalpath
        has_usa_pdr_country_hourly >> rail.Label(
            'No') >> log_message_checkifthevalueispresentfortimesheetapprovalpath
        log_message_checkifthevalueispresentfortimesheetapprovalpath >> get_all_timesheet_approval_path >> has_approval_path_changed

        has_approval_path_changed >> rail.Label(
            'Yes') >> log_message_gettherequiredtimesheetapprovalpath_uri >> can_update_approval_path
        has_approval_path_changed >> rail.Label(
            'No') >> has_salary_emptype_usa
        can_update_approval_path >> rail.Label(
            'no') >> has_salary_emptype_usa
        can_update_approval_path >> rail.Label(
            'Yes') >> update_timesheet_approval_path_for_user >> has_salary_emptype_usa

        has_salary_emptype_usa >> rail.Label(
            'Yes') >> log_message_timeofftemplate_salary >> has_hourly_emptype_usa
        has_salary_emptype_usa >> rail.Label('No') >> has_hourly_emptype_usa
        has_hourly_emptype_usa >> rail.Label(
            'Yes') >> log_message_timeofftemplate_hourly >> log_message_checkifthevalueispresentfortimeofftemplate
        has_hourly_emptype_usa >> rail.Label(
            'No') >> log_message_checkifthevalueispresentfortimeofftemplate
        log_message_checkifthevalueispresentfortimeofftemplate >> has_timeoff_template_changed
        has_timeoff_template_changed >> rail.Label(
            'Yes') >> log_message_gettherequiredtimeofftemplate_uri >> can_update_timeoff_template
        has_timeoff_template_changed >> rail.Label(
            'No') >> log_message_checkiftheuriexistsforgivenlocationfrom_timezonemapper

        can_update_timeoff_template >> rail.Label(
            'No') >> log_message_checkiftheuriexistsforgivenlocationfrom_timezonemapper
        can_update_timeoff_template >> rail.Label(
            'Yes') >> update_timeoff_template_for_user >> log_message_checkiftheuriexistsforgivenlocationfrom_timezonemapper
        log_message_checkiftheuriexistsforgivenlocationfrom_timezonemapper >> log_message_checkifthenameexistsforgivenlocationfrom_timezonemapper >> can_update_timezone

        can_update_timezone >> rail.Label(
            'Yes') >> update_time_zone >> get_custom_fieldsforuser
        can_update_timezone >> rail.Label(
            'No') >> get_custom_fieldsforuser
        get_custom_fieldsforuser >> get_custom_fielduri >> has_return_to_workdate

        has_return_to_workdate >> rail.Label(
            'Yes') >> log_message_gettherequiredcustomfield_uri_returnto_work_date >> can_update_update_returnto_work_date_udf
        has_return_to_workdate >> rail.Label('No') >> has_firstdayofleave

        can_update_update_returnto_work_date_udf >> rail.Label(
            'No') >> has_firstdayofleave
        can_update_update_returnto_work_date_udf >> rail.Label(
            'Yes') >> update_returnto_work_date_udf >> has_firstdayofleave

        has_firstdayofleave >> rail.Label(
            'Yes') >> log_message_gettherequiredcustomfield_uri_first_dayof_leave >> can_update_first_dayleave_udf
        has_firstdayofleave >> rail.Label('No') >> has_change_in_lasthiredate2

        can_update_first_dayleave_udf >> rail.Label(
            'No') >> has_change_in_lasthiredate2
        can_update_first_dayleave_udf >> rail.Label(
            'Yes') >> update_first_dayof_leave_udf >> has_change_in_lasthiredate2

        has_change_in_lasthiredate2 >> rail.Label(
            'Yes') >> log_message_gettherequiredcustomfield_uri_last_hire_date2 >> can_update_last_hire_date2_udf
        has_change_in_lasthiredate2 >> rail.Label(
            'No') >> has_last_record_update

        can_update_last_hire_date2_udf >> rail.Label(
            'Yes') >> update_last_hire_date2_udf >> has_last_record_update
        can_update_last_hire_date2_udf >> rail.Label(
            'No') >> has_last_record_update

        has_last_record_update >> rail.Label(
            'Yes') >> log_message_gettherequiredcustomfield_uri_jobcode >> can_update_last_record_update
        has_last_record_update >> rail.Label(
            'No') >> has_company
        can_update_last_record_update >> rail.Label(
            'Yes') >> update_last_record_update >> has_company
        can_update_last_record_update >> rail.Label(
            'No') >> has_company

        has_company >> rail.Label(
            'Yes') >> has_company_kla
        has_company >> rail.Label(
            'No') >> has_company_changed

        has_company_kla >> rail.Label(
            'Yes') >> log_message_companycode_kla >> has_company_vlsi
        has_company_kla >> rail.Label(
            'No') >> has_company_vlsi

        has_company_vlsi >> rail.Label(
            'Yes') >> log_message_companycode_vlsi >> log_message_finalvaluefor_company_code
        has_company_vlsi >> rail.Label(
            'No') >> log_message_finalvaluefor_company_code

        log_message_finalvaluefor_company_code >> has_company_changed
        has_company_changed >> rail.Label(
            'Yes') >> log_message_gettherequiredcustomfield_uri_company_code >> has_company_code_udf
        has_company_changed >> rail.Label(
            'No') >> log_message_final_valuefor_pdr_country

        has_company_code_udf >> rail.Label(
            'Yes') >> get_enabled_custom_field_drop_down_optionsfor_company_code >> log_message_gettherequireddropdownoption_pdrcountry_uri >> has_compnaycode_dropdown_uri
        has_company_code_udf >> rail.Label(
            'no') >> log_message_final_valuefor_pdr_country

        has_compnaycode_dropdown_uri >> rail.Label(
            'Yes') >> update_company_udf >> log_message_final_valuefor_pdr_country
        has_compnaycode_dropdown_uri >> rail.Label(
            'No') >> log_message_final_valuefor_pdr_country
        log_message_final_valuefor_pdr_country >> has_country_changed

        has_country_changed >> rail.Label(
            'Yes') >> log_message_gettherequiredcustomfield_uri_pdr_country >> has_pdrcountry_udf_uri
        has_country_changed >> rail.Label('No') >> get_all_permission_sets

        has_pdrcountry_udf_uri >> rail.Label(
            'Yes') >> get_enabled_custom_field_drop_down_options_for_company_code >> log_message_gettherequireddropdownoption_pdrcountry_uri2 >> has_dropdownoption_pdr_country_uri
        has_pdrcountry_udf_uri >> rail.Label('no') >> get_all_permission_sets
        has_dropdownoption_pdr_country_uri >> rail.Label(
            'yes') >> update_pdr_country_udf >> get_all_permission_sets
        has_dropdownoption_pdr_country_uri >> rail.Label(
            'No') >> get_all_permission_sets

        get_all_permission_sets >> log_message_permissionfor_manager_supervisor >> log_message_permissionfor_manager_basic_user >> has_supervisorid

        has_supervisorid >> rail.Label(
            'Yes') >> searchuser_supervisor >> has_supervisor_uri_changed
        has_supervisorid >> rail.Label(
            'No') >> has_usdirectreport

        has_supervisor_uri_changed >> rail.Label(
            'Yes') >> can_update_supervisor
        has_supervisor_uri_changed >> rail.Label(
            'No') >> has_no_supervisor_found

        can_update_supervisor >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_supervisor >> log_message_checkif_supervisorpermissionisassigned >> has_no_suprevisor_permission
        can_update_supervisor >> rail.Label(
            'No') >> has_usdirectreport

        has_no_suprevisor_permission >> rail.Label(
            'Yes') >> get_all_non_user_permission_policy >> log_message_new_permission_setforsupervisor >> log_message_permission_setsfor_user >> put_permission_set_assignments_for_supervisorofthe_user >> update_supervisorassignmentwithaneffectivedate >> has_usdirectreport
        has_no_suprevisor_permission >> rail.Label(
            'No') >> update_supervisorassignmentwithaneffectivedate >> has_usdirectreport

        has_no_supervisor_found >> rail.Label(
            'Yes') >> queue_supervisor_assignment >> has_usdirectreport
        has_no_supervisor_found >> rail.Label(
            'No') >> log_message_tobeusedinfinallogmessage_disabled >> has_usdirectreport

        has_usdirectreport >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_supervisor_directreport >> log_message_checkif_supervisorpermissionisassigned2 >> has_no_supervisorpermissionisassigned
        has_usdirectreport >> rail.Label(
            'No') >> log_message_final_locationvaluenew

        has_no_supervisorpermissionisassigned >> rail.Label(
            'Yes') >> get_all_non_user_permission_policy2 >> log_message_new_permission_setforsupervisor_us >> log_message_permission_setsfor_user_us >> put_permission_set_assignments_forsupervisorofthe_user_us >> log_message_final_locationvaluenew
        has_no_supervisorpermissionisassigned >> rail.Label(
            'No') >> log_message_final_locationvaluenew

        log_message_final_locationvaluenew >> getlocation_data_final >> log_message_messagetobeusedinfinallog_locnotfound >> can_update_location
        can_update_location >> rail.Label(
            'Yes') >> update_location_schedule_for_user >> has_cost_center
        can_update_location >> rail.Label('No') >> has_cost_center

        has_cost_center >> rail.Label(
            'Yes') >> get_cost_center >> can_update_cost_center
        has_cost_center >> rail.Label(
            'no') >> search_entries_for_holiday_calendar

        can_update_cost_center >> rail.Label(
            'Yes') >> update_cost_center >> search_entries_for_holiday_calendar
        can_update_cost_center >> rail.Label(
            'no') >> search_entries_for_holiday_calendar

        search_entries_for_holiday_calendar >> get_holiday_calendar_uri >> can_update_holiday_calendar

        can_update_holiday_calendar >> rail.Label(
            'Yes') >> update_holiday_calendar >> has_removed_holiday_calendar
        can_update_holiday_calendar >> rail.Label(
            'no') >> has_removed_holiday_calendar

        has_removed_holiday_calendar >> rail.Label(
            'Yes') >> remove_holiday_calendar >> has_hourly_group_emptype_loc
        has_removed_holiday_calendar >> rail.Label(
            'no') >> has_hourly_group_emptype_loc

        has_hourly_group_emptype_loc >> rail.Label(
            'No') >> search_entries_for_payrule
        has_hourly_group_emptype_loc >> rail.Label(
            'Yes') >> log_message_final_valueforlocationtobeusedforlookup >> search_entries_for_payrule
        search_entries_for_payrule >> has_search_entries_for_payrule
        has_search_entries_for_payrule >> rail.Label(
            'Yes') >> get_asssigned_payrulelist_map >> map_current_payrule_script >> can_update_payrule_script_name
        has_search_entries_for_payrule >> rail.Label(
            'No') >> search_entries_for_activity

        can_update_payrule_script_name >> rail.Label(
            'yes') >> put_pay_rule_script_assignment_schedule_for_user >> search_entries_for_activity
        can_update_payrule_script_name >> rail.Label(
            'no') >> search_entries_for_activity

        search_entries_for_activity >> get_all_activities >> can_assign_activity

        can_assign_activity >> rail.Label(
            'yes') >> assign_activity >> can_enable_login
        can_assign_activity >> rail.Label('no') >> can_enable_login

        can_enable_login >> rail.Label(
            'Yes') >> re_enable_userprofile >> can_update_user_timeoff
        can_enable_login >> rail.Label('No') >> has_empl_status_active

        can_update_user_timeoff >> rail.Label(
            'Yes') >> process_update_user_timeoff >> wait_for_process_update_user_timeoff >> add_rehire_success_log >> finish
        can_update_user_timeoff >> rail.Label(
            'No') >> add_rehire_success_log >> finish

        has_empl_status_active >> rail.Label(
            'Yes') >> has_employee_type
        has_empl_status_active >> rail.Label(
            'No') >> has_empl_status_terminated

        has_employee_type >> rail.Label(
            'Yes') >> process_update_user_timeoff2 >> wait_for_process_update_user_timeoff2 >> has_pdr_country_value
        has_employee_type >> rail.Label('No') >> has_pdr_country_value

        has_pdr_country_value >> rail.Label(
            'Yes') >> process_update_user_isolation_timeoff >> wait_for_process_update_user_isolation_timeoff >> add_success_log >> has_empl_status_terminated
        has_pdr_country_value >> rail.Label('No') >> has_empl_status_terminated

        has_empl_status_terminated >> rail.Label(
            'Yes') >> has_user_status_is_enabled
        has_empl_status_terminated >> rail.Label('No') >> has_empl_status_leave

        has_user_status_is_enabled >> rail.Label(
            'Yes') >> disable_userprofile_2 >> add_disabled_user_log1 >> finish
        has_user_status_is_enabled >> rail.Label(
            'No') >> add_user_already_disabled_log >> finish

        has_empl_status_leave >> rail.Label(
            'Yes') >> has_emp_type_hourly2
        has_empl_status_leave >> rail.Label('No') >> finish

        has_emp_type_hourly2 >> rail.Label(
            'Yes') >> has_emp_status_enabled2
        has_emp_type_hourly2 >> rail.Label('No') >> has_user_status_enabled_4

        has_emp_status_enabled2 >> rail.Label(
            'Yes') >> disable_userprofile_leave >> add_disable_user_leave_log >> finish
        has_emp_status_enabled2 >> rail.Label(
            'No') >> add_user_already_disabled_log4 >> finish
        has_user_status_enabled_4 >> rail.Label(
            'no') >> add_user_already_disabled_log2 >> finish
        has_user_status_enabled_4 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user2_3 >> log_message_checkif_supervisorpermissionisassigned_3 >> has_no_manager_basic_user_perm
        has_no_manager_basic_user_perm >> rail.Label(
            'Yes') >> disable_userprofile_5 >> add_success_log_supervisorcheck >> finish
        has_no_manager_basic_user_perm >> rail.Label(
            'No') >> add_supervisor_perm_error >> finish
        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
