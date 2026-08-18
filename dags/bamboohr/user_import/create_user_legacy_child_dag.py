from datetime import datetime, timedelta
import rail
from airflow.models import Variable
null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_bamboohr_{config.region.replace('-', '_')}_user_create_legacy_child_dag_{config.instance}",
        description=f'BambooHR {config.region} User Create Legacy Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user_in_replicon'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_user_in_replicon',
            end_task='catch_user_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def get_datetime_obj(date_str, fmt='%Y-%m-%d'):
            if date_str == '0000-00-00':
                return {
                    'year': datetime.now().year,
                    'month': datetime.now().month,
                    'day': datetime.now().day
                }
            datetime_obj = datetime.strptime(date_str, fmt)
            return {
                'year': datetime_obj.year,
                'month': datetime_obj.month,
                'day': datetime_obj.day
            }

        def get_create_user_payload(dag_run):
            return {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['workemail'],
                        "employeeId": null,
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['firstname'],
                    "lastname": dag_run.conf['lastname'],
                    "emailAddress": dag_run.conf['workemail'],
                    "employeeId": dag_run.conf['employeenumber'],
                    "department": {
                        "uri": null,
                        "name": dag_run.conf['department'],
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": {
                        "startDate": get_datetime_obj(dag_run.conf['startdate']),
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:replicon"
                        ],
                        "isLoginEnabled": "true",
                        "loginName": dag_run.conf['workemail'],
                        "SSOName": null,
                        "password": "Replicon@12"
                    },
                    "holidayCalendar": null,
                    "holidayCalendarAssignmentSchedule": null,
                    "timeOffPolicy": null,
                    "permissionSets": [
                        {
                            "uri": null,
                            "name": "Basic User"
                        }
                    ],
                    "policySets": [
                        {
                            "uri": null,
                            "name": "Standard Timesheet"
                        }
                    ],
                    "employeeType": {
                        "uri": null,
                        "name": "Full-time Salaried"
                    },
                    "timesheetPeriodTypeUri": "urn:replicon:timesheet-period-type:system",
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": null,
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": null,
                    "workAuthorizationApprovalPath": null,
                    "timeOffBalancePayoutApprovalPath": null,
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
                    "timesheetPeriodSchedule": [],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": [],
                    "displayNameParameter": null,
                    "decimalSeparatorUri": null,
                    "numberGroupSeparatorUri": null,
                    "extensionFieldValues": []
                }
            }
        create_user_in_replicon = rail.RepliconServiceOperator(
            task_id='create_user_in_replicon',
            endpoint="/services/ImportService1.svc/PutUser3",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=get_create_user_payload
        )

        get_enabled_departments = rail.RepliconServiceOperator(
            task_id='get_enabled_departments',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=null
        )

        foreach_department = rail.ForEachOperator(
            task_id='foreach_department',
            items="{{ result('get_enabled_departments') | to_json }}",
            start_task='if_department_equals_to_bamboohr_jobinfo',
            end_task='foreach_department_end'
        )

        if_department_equals_to_bamboohr_jobinfo = rail.IfOperator(
            task_id='if_department_equals_to_bamboohr_jobinfo',
            test=lambda dag_run: rail.result('foreach_department')[
                'displayText'] == dag_run.conf['department'],
            yes_task="update_department_for_user",
            no_task="foreach_department_end",
        )

        update_department_for_user = rail.RepliconServiceOperator(
            task_id='update_department_for_user',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                "userUri": "{{ result('create_user_in_replicon').uri }}",
                "departmentUri": "{{ result('foreach_department').uri }}"
            }
        )

        foreach_department_end = rail.EmptyOperator(
            task_id='foreach_department_end',
        )

        def get_downstreamtasks_error(user_name, error_message):
            return {
                'error': f'Error with {user_name} - {error_message}'
            }
        catch_user_error = rail.PythonOperator(
            task_id='catch_user_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.workemail }}',
                     '{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> rail.Label(
                'on Error') >> catch_user_error

        can_run_batch_task >> rail.Label(
            'No') >> create_user_in_replicon >> get_enabled_departments \
            >> foreach_department \
            >> if_department_equals_to_bamboohr_jobinfo
        if_department_equals_to_bamboohr_jobinfo >> rail.Label(
            'Yes') >> update_department_for_user >> foreach_department_end
        if_department_equals_to_bamboohr_jobinfo >> rail.Label(
            'No') >> foreach_department_end
        foreach_department >> foreach_department_end >> rail.Label(
            'On Error') >> catch_user_error

    return dag


rail.for_each_instance(create_child_dag)
