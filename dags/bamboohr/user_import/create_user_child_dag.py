from datetime import datetime, timedelta
import rail
from airflow.models import Variable
null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_bamboohr_{config.region.replace('-', '_')}_user_create_child_dag_{config.instance}",
        description=f'BambooHR {config.region} User Create Child DAG {config.instance}',
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
            no_task='get_company_department'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_company_department',
            end_task='catch_user_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_company_department = rail.RepliconServiceOperator(
            task_id='get_company_department',
            endpoint="/services/DepartmentService1.svc/GetCompanyDepartment",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        get_timesheet_period_uri = rail.RepliconServiceOperator(
            task_id='get_timesheet_period_uri',
            endpoint="/services/TimesheetPeriodService2.svc/GetTimesheetPeriodForNewUsers",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
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
                    "departmentGroupSchedule": [
                        {
                            "departmentGroup": {
                                "uri": null,
                                "parent": null,
                                "name": rail.result('get_company_department')['name'],
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "employeeTypeGroupSchedule": [
                        {
                            "employeeTypeGroup": {
                                "uri": null,
                                "parent": null,
                                "name": "Salaried",
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "timesheetPeriodSchedule": [
                        {
                            "timesheetPeriod": {
                                "uri": rail.result('get_timesheet_period_uri').get('uri'),
                                "name": null
                            },
                            "effectiveDate": null
                        }
                    ]
                }
            }

        create_user_in_replicon = rail.RepliconServiceOperator(
            task_id='create_user_in_replicon',
            endpoint="/services/ImportService1.svc/PutUser3",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=get_create_user_payload
        )

        get_enabled_employee_type_groups = rail.RepliconServiceOperator(
            task_id='get_enabled_employee_type_groups',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        declare_employee_type_uri_list = rail.SetVariableOperator(
            task_id='declare_employee_type_uri_list',
            append=False,
            name='employeetypeuri',
            value=[]
        )

        foreach_employeetype_group = rail.ForEachOperator(
            task_id='foreach_employeetype_group',
            items="{{ result('get_enabled_employee_type_groups') | to_json}}",
            start_task='if_employeetype_equals_fulltime',
            end_task='foreach_employeetype_group_end'
        )

        if_employeetype_equals_fulltime = rail.IfOperator(
            task_id='if_employeetype_equals_fulltime',
            test=lambda: rail.result('foreach_employeetype_group')[
                'displayText'].lower() == 'full-time',
            yes_task="get_employeetype_group_details",
            no_task="foreach_employeetype_group_end",
        )

        get_employeetype_group_details = rail.RepliconServiceOperator(
            task_id='get_employeetype_group_details',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEmployeeTypeGroupDetails",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                "employeeTypeGroupUri": "{{ result('foreach_employeetype_group').uri }}"
            }
        )

        if_displaytext_downcase_equals_to_salaried = rail.IfOperator(
            task_id='if_displaytext_downcase_equals_to_salaried',
            test=lambda: rail.result('get_employeetype_group_details')[
                'parent']['displayText'].lower() == 'salaried',
            yes_task="accumulate_employee_group_uris",
            no_task="foreach_employeetype_group_end",
        )

        accumulate_employee_group_uris = rail.SetVariableOperator(
            task_id='accumulate_employee_group_uris',
            name='employeetypeuri',
            append=True,
            value={
                "uri": "{{ result('get_employeetype_group_details').uri }}"
            }
        )

        foreach_employeetype_group_end = rail.EmptyOperator(
            task_id='foreach_employeetype_group_end',
        )

        if_first_employee_uri_present = rail.IfOperator(
            task_id='if_first_employee_uri_present',
            test=lambda: rail.result('accumulate_employee_group_uris') and rail.result(
                'accumulate_employee_group_uris')['value'][0]['uri'],
            yes_task="add_employee_type_group_schedule_for_user",
            no_task="get_enabled_department_groups",
        )

        def add_employee_type_group_payload():
            def get_schedule_entries():
                return list(map(lambda item: {
                    "employeeTypeGroup": {
                            "uri": item.get('uri'),
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                            },
                    "effectiveDate": null
                }, rail.result('accumulate_employee_group_uris')['value'])) if rail.result('accumulate_employee_group_uris')['value'] else []
            return {
                "userUri": rail.result('create_user_in_replicon')['uri'],
                "scheduleEntries": get_schedule_entries()
            }
        add_employee_type_group_schedule_for_user = rail.RepliconServiceOperator(
            task_id='add_employee_type_group_schedule_for_user',
            endpoint="/services/EmployeeTypeGroupService1.svc/PutEmployeeTypeGroupScheduleForUser",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=add_employee_type_group_payload
        )

        get_enabled_department_groups = rail.RepliconServiceOperator(
            task_id='get_enabled_department_groups',
            endpoint="/services/DepartmentGroupService1.svc/GetEnabledDepartmentGroups",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        foreach_department_group = rail.ForEachOperator(
            task_id='foreach_department_group',
            items="{{ result('get_enabled_department_groups') | to_json }}",
            start_task='if_department_equals_to_bamboohr_jobinfo',
            end_task='foreach_department_group_end'
        )

        if_department_equals_to_bamboohr_jobinfo = rail.IfOperator(
            task_id='if_department_equals_to_bamboohr_jobinfo',
            test=lambda dag_run: rail.result('foreach_department_group')[
                'displayText'] == dag_run.conf['department'],
            yes_task="add_department_group_schedule_for_user",
            no_task="foreach_department_group_end",
        )

        add_department_group_schedule_for_user = rail.RepliconServiceOperator(
            task_id='add_department_group_schedule_for_user',
            endpoint="/services/DepartmentGroupService1.svc/PutDepartmentGroupScheduleForUser",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "userUri": rail.result('create_user_in_replicon')['uri'],
                "scheduleEntries": [
                    {
                        "departmentGroup": {
                            "uri": rail.result('foreach_department_group')['uri'],
                            "parent": null,
                            "name": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        foreach_department_group_end = rail.EmptyOperator(
            task_id='foreach_department_group_end',
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
            'No') >> get_company_department >> get_timesheet_period_uri \
            >> create_user_in_replicon >> get_enabled_employee_type_groups >> declare_employee_type_uri_list \
            >> foreach_employeetype_group >> if_employeetype_equals_fulltime
        if_employeetype_equals_fulltime >> rail.Label(
            'Yes') >> get_employeetype_group_details >> if_displaytext_downcase_equals_to_salaried
        if_displaytext_downcase_equals_to_salaried >> rail.Label(
            'Yes') >> accumulate_employee_group_uris >> foreach_employeetype_group_end
        if_displaytext_downcase_equals_to_salaried >> rail.Label(
            'No') >> foreach_employeetype_group_end
        if_employeetype_equals_fulltime >> rail.Label(
            'No') >> foreach_employeetype_group_end
        foreach_employeetype_group >> foreach_employeetype_group_end >> if_first_employee_uri_present
        if_first_employee_uri_present >> rail.Label(
            'Yes') >> add_employee_type_group_schedule_for_user >> get_enabled_department_groups
        if_first_employee_uri_present >> rail.Label('No') >> get_enabled_department_groups >> foreach_department_group \
            >> if_department_equals_to_bamboohr_jobinfo
        if_department_equals_to_bamboohr_jobinfo >> rail.Label(
            'Yes') >> add_department_group_schedule_for_user >> foreach_department_group_end
        if_department_equals_to_bamboohr_jobinfo >> rail.Label(
            'No') >> foreach_department_group_end
        foreach_department_group >> foreach_department_group_end >> rail.Label(
            'On Error') >> catch_user_error

    return dag


rail.for_each_instance(create_child_dag)
