from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/mccarthy/user_import/config.py


# pylint: disable=too-many-statements
def create_adduser_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'mccarthy_user_import_add_user_child_{config.instance}',
        description=f'LIVE | Mccarthy | User Sync_Child_Add User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_new_user'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_new_user',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def createuser_payload(dag_run):
            null = None

            def get_replicon_date(date_str, fmt='%m/%d/%Y'):
                datetime_obj = datetime.strptime(date_str, fmt)
                return {
                    'year': datetime_obj.year,
                    'month': datetime_obj.month,
                    'day': datetime_obj.day
                }

            def get_customfields(dag_run_conf):
                custom_fields_to_add = []
                payroll_name = dag_run_conf['Payrollname']
                if payroll_name:
                    custom_fields_to_add.append({
                        "customField": {
                            "uri": dag_run_conf['Payrollnameuri']
                        },
                        "dropDownOption": {
                            "name": payroll_name
                        }
                    })
                employeecategory = dag_run_conf['Employeecategory']
                if employeecategory:
                    custom_fields_to_add.append({
                        "customField": {
                            "uri": dag_run_conf['Employeecategoryuri']
                        },
                        "dropDownOption": {
                            "name": employeecategory
                        }
                    })
                employeeworkstate = dag_run_conf['Employeeworkstate']
                if employeeworkstate:
                    custom_fields_to_add.append({
                        "customField": {
                            "uri": dag_run_conf['Employeeworkstateuri']
                        },
                        "dropDownOption": {
                            "name": employeeworkstate
                        }
                    })
                legalentity = dag_run_conf['Legalentity']
                if legalentity:
                    custom_fields_to_add.append({
                        "customField": {
                            "uri": dag_run_conf['Legalentityuri']
                        },
                        "dropDownOption": {
                            "name": legalentity
                        }
                    })
                jobtitle = dag_run_conf['Jobtitle']
                if jobtitle:
                    custom_fields_to_add.append({
                        "customField": {
                            "uri": dag_run_conf['Jobtitleuri']
                        },
                        "text": jobtitle
                    })
                organization = dag_run_conf['Organization']
                if organization:
                    custom_fields_to_add.append({
                        "customField": {
                            "uri": dag_run_conf['Organizationuri']
                        },
                        "text": organization
                    })
                return custom_fields_to_add

            custom_fields_to_assign = get_customfields(dag_run.conf)
            return {
                "user": {
                    "target": {
                        "loginName": dag_run.conf['Loginname']
                    },
                    "firstname": dag_run.conf['Firstname'],
                    "lastname": dag_run.conf['Lastname'],
                    "emailAddress": dag_run.conf['Email'] if dag_run.conf['Email'] else None,
                    "employeeId": dag_run.conf['Employeeid'],
                    "schedulePolicySchedule": [
                        {
                            "schedulePolicy": {
                                "name": "Default Schedule"
                            }
                        }
                    ],
                    "workWeekStartDayUri": "urn:replicon:day-of-week:monday",
                    "employmentDateRange": {
                        "startDate": get_replicon_date(dag_run.conf['Startdate']) if dag_run.conf['Startdate'] else {
                            "year": null,
                            "month": null,
                            "day": null
                        }
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "isLoginEnabled": True,
                        "loginName": dag_run.conf['Loginname'],
                        "SSOName": dag_run.conf['Loginname']
                    },
                    "permissionSets": [
                        {
                            "uri": dag_run.conf['Permissionsuri']
                        }
                    ],
                    "policySets": [
                        {
                            "name": "*Gen3 - Default Timeoff Template"
                        }
                    ],
                    "customFieldValues": custom_fields_to_assign,
                    "locationSchedule": [
                        {
                            "location": {
                                "uri": dag_run.conf['Locationuri'],
                            },
                        }
                    ] if dag_run.conf['Locationuri'] else None,
                    "timesheetPeriodSchedule": [
                        {
                            "timesheetPeriod": {
                                "name": "Weekly starting on Monday"
                            }
                        }
                    ]
                }
            }

        create_new_user = rail.RepliconServiceOperator(
            task_id='create_new_user',
            endpoint="/services/importservice1.svc/PutUser3",
            data=createuser_payload
        )

        is_departmenturi_present = rail.IfOperator(
            task_id='is_departmenturi_present',
            test="{{ dag_run.conf.Departmenturi | is_truthy }}",
            yes_task="add_departmentgroup",
            no_task="is_employeetypeuri_present"
        )

        add_departmentgroup = rail.RepliconServiceOperator(
            task_id='add_departmentgroup',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "user": {
                    "loginName": "{{ result('create_new_user').loginName }}"
                },
                "modifications": {
                    "departmentGroupScheduleToApply": {
                        "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                        "replacementDepartmentGroupSchedule": [
                            {
                                "departmentGroup": {
                                    "uri": "{{ dag_run.conf.Departmenturi }}"
                                }
                            }
                        ]
                    }
                }
            }
        )

        is_employeetypeuri_present = rail.IfOperator(
            task_id='is_employeetypeuri_present',
            test="{{ dag_run.conf.Employeetypeuri | is_truthy }}",
            yes_task="add_employeetype_group",
            no_task="assign_timesheet_template"
        )

        add_employeetype_group = rail.RepliconServiceOperator(
            task_id='add_employeetype_group',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "user": {
                    "loginName": "{{ result('create_new_user').loginName }}"
                },
                "modifications": {
                    "employeeTypeGroupScheduleToApply": {
                        "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:replace-entire-schedule",
                        "replacementEmployeeTypeGroupSchedule": [
                            {
                                "employeeTypeGroup": {
                                    "name": "{{ dag_run.conf.Employeetype }}"
                                }
                            }
                        ]
                    }
                }
            }
        )

        assign_timesheet_template = rail.RepliconServiceOperator(
            task_id='assign_timesheet_template',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_new_user')['uri'],
                "policySetUri": dag_run.conf['Timesheettemplateuri'] if dag_run.conf['Timesheettemplateuri'] else dag_run.conf['Defaulttimesheettemplate']
            }
        )

        is_timezoneuri_present = rail.IfOperator(
            task_id='is_timezoneuri_present',
            test="{{ dag_run.conf.Timezoneuri | is_truthy }}",
            yes_task="add_timezone_user",
            no_task="is_activities_present"
        )

        add_timezone_user = rail.RepliconServiceOperator(
            task_id='add_timezone_user',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "user": {
                    "loginName": "{{ result('create_new_user').loginName }}"
                },
                "modifications": {
                    "timezoneToApply": {
                        "userTimeZoneModificationOptionUri": "urn:replicon:user-time-zone-modication-option:use-specified-time-zone",
                        "timezone": {
                            "uri": "{{ dag_run.conf.Timezoneuri }}"
                        }
                    }
                }
            }
        )

        is_activities_present = rail.IfOperator(
            task_id='is_activities_present',
            test="{{ dag_run.conf.Activities | length > 0 }}",
            yes_task="update_activity_assignments_user",
            no_task="get_enabled_timeoff_type_uris"
        )

        update_activity_assignments_user = rail.RepliconServiceOperator(
            task_id='update_activity_assignments_user',
            endpoint="/services/ActivityService1.svc/UpdateActivityAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_new_user')['uri'],
                "activityUris": dag_run.conf['Activities']
            }
        )

        def get_enabled_timeofftype_uris(response):
            return [x['uri'] for x in response if x['uri']]
        get_enabled_timeoff_type_uris = rail.RepliconServiceOperator(
            task_id='get_enabled_timeoff_type_uris',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=get_enabled_timeofftype_uris
        )

        is_timeoffuris_present = rail.IfOperator(
            task_id='is_timeoffuris_present',
            test="{{ result('get_enabled_timeoff_type_uris') | length > 0 }}",
            yes_task="put_timeoff_type_assignments",
            no_task="is_supervisor_assign_pending"
        )

        put_timeoff_type_assignments = rail.RepliconServiceOperator(
            task_id='put_timeoff_type_assignments',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_new_user')['uri'],
                "timeOffTypeUris": rail.result('get_enabled_timeoff_type_uris')
            }
        )

        for_each_timeoff_uri = rail.ForEachOperator(
            task_id='for_each_timeoff_uri',
            items=lambda: rail.result('get_enabled_timeoff_type_uris'),
            start_task='get_default_timeoff_type_policy_schedule',
            end_task='for_each_timeoff_uri_end'
        )

        get_default_timeoff_type_policy_schedule = rail.RepliconServiceOperator(
            task_id='get_default_timeoff_type_policy_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ result('create_new_user').uri }}",
                    "timeOffTypeUri": "{{ result('for_each_timeoff_uri') }}"
                }
            },
            data_handler=lambda response: json.loads(json.dumps([x for x in response if x['policySet']], ensure_ascii=False).replace(
                'null', '"effective"').replace('"script"', '"scriptTarget"')) if response and response[0] and response[0]['policySet'] else ''
        )

        is_policy_present = rail.IfOperator(
            task_id='is_policy_present',
            test="{{ result('get_default_timeoff_type_policy_schedule') | is_truthy }}",
            yes_task="put_user_timeoff_account_policyschedule",
            no_task="for_each_timeoff_uri_end"
        )

        put_user_timeoff_account_policyschedule = rail.RepliconServiceOperator(
            task_id='put_user_timeoff_account_policyschedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.result('create_new_user')['uri'],
                    "timeOffTypeUri": rail.result('for_each_timeoff_uri')
                },
                "policySetScheduleEntries": rail.result('get_default_timeoff_type_policy_schedule')
            }
        )

        for_each_timeoff_uri_end = rail.EmptyOperator(
            task_id='for_each_timeoff_uri_end'
        )

        is_supervisor_assign_pending = rail.IfOperator(
            task_id='is_supervisor_assign_pending',
            test="{{ dag_run.conf.Supervisorid | sn | is_truthy and \
              dag_run.conf.Supervisorid != dag_run.conf.Employeeid }}",
            yes_task="write_supervisor_pending_log",
            no_task="get_adduser_exception_logs"
        )

        write_supervisor_pending_log = rail.WriteLogOperator(
            task_id='write_supervisor_pending_log',
            log='{{ dag_run.conf.supervisor_log }}',
            message="na",
            severity="Pending",
            properties=lambda dag_run: {
                "userloginname": dag_run.conf['Loginname'],
                "useruri": rail.result('create_new_user')['uri'],
                "supervisorloginname": dag_run.conf['Supervisorid'],
                "action": "Add",
                "status": "Pending",
                "emplid": dag_run.conf['Employeeid'],
                "effective_date": dag_run.conf['Supervisoreffectivedate'],
                "user_log": dag_run.conf['log']
            }
        )

        def get_adduser_exception():
            dag_run_conf = rail.get_current_context()['dag_run'].conf
            if dag_run_conf['Supervisorid'] and dag_run_conf['Supervisorid'] == dag_run_conf['Employeeid']:
                return 'supervisor could not be assigned as the supervisor ID received is same as user employee id'
            return ''
        get_adduser_exception_logs = rail.PythonOperator(
            task_id='get_adduser_exception_logs',
            python_callable=get_adduser_exception
        )

        write_adduser_log = rail.WriteLogOperator(
            task_id='write_adduser_log',
            log="{{ dag_run.conf.log }}",
            message='\
                    {%- if result("get_adduser_exception_logs") | is_truthy -%} \
                        User created partially - {{ result("get_adduser_exception_logs") }}\
                    {%- else -%} \
                        User created successfully\
                    {%- endif -%}',
            severity='\
                    {%- if result("get_adduser_exception_logs") | is_truthy -%} \
                        Exception\
                    {%- else -%} \
                        Success\
                    {%- endif -%}',
            properties={
                'loginname': '{{ dag_run.conf.Loginname }}',
                'email': '{{ dag_run.conf.Email }}',
                'action': 'Add',
                'status': '\
                    {%- if result("get_adduser_exception_logs") | is_truthy -%} \
                        Exception\
                    {%- else -%} \
                        Success\
                    {%- endif -%}',
                'details': '\
                    {%- if result("get_adduser_exception_logs") | is_truthy -%} \
                        User created partially - {{ result("get_adduser_exception_logs") }}\
                    {%- else -%} \
                        User created successfully\
                    {%- endif -%}'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log }}",
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity="Error",
            properties={
                'loginname': '{{ dag_run.conf.Loginname }}',
                'email': '{{ dag_run.conf.Email }}',
                'action': 'Add',
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> create_new_user >> is_departmenturi_present
        is_departmenturi_present >> rail.Label(
            'Yes') >> add_departmentgroup >> is_employeetypeuri_present
        is_departmenturi_present >> rail.Label(
            'No') >> is_employeetypeuri_present
        is_employeetypeuri_present >> rail.Label(
            'Yes') >> add_employeetype_group >> assign_timesheet_template
        is_employeetypeuri_present >> rail.Label(
            'No') >> assign_timesheet_template
        assign_timesheet_template >> is_timezoneuri_present
        is_timezoneuri_present >> rail.Label(
            'Yes') >> add_timezone_user >> is_activities_present
        is_timezoneuri_present >> rail.Label(
            'No') >> is_activities_present
        is_activities_present >> rail.Label(
            'Yes') >> update_activity_assignments_user >> get_enabled_timeoff_type_uris
        is_activities_present >> rail.Label(
            'No') >> get_enabled_timeoff_type_uris
        get_enabled_timeoff_type_uris >> is_timeoffuris_present
        is_timeoffuris_present >> rail.Label(
            'Yes') >> put_timeoff_type_assignments >> for_each_timeoff_uri
        for_each_timeoff_uri >> get_default_timeoff_type_policy_schedule >> is_policy_present
        is_policy_present >> rail.Label(
            'Yes') >> put_user_timeoff_account_policyschedule >> for_each_timeoff_uri_end
        is_policy_present >> rail.Label(
            'No') >> for_each_timeoff_uri_end
        for_each_timeoff_uri >> for_each_timeoff_uri_end
        for_each_timeoff_uri_end >> is_supervisor_assign_pending
        is_timeoffuris_present >> rail.Label(
            'No') >> is_supervisor_assign_pending
        is_supervisor_assign_pending >> rail.Label(
            'Yes') >> write_supervisor_pending_log >> get_adduser_exception_logs
        is_supervisor_assign_pending >> rail.Label(
            'No') >> get_adduser_exception_logs
        get_adduser_exception_logs >> write_adduser_log >> catch_and_log_errors
        catch_and_log_errors >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_adduser_dag)
