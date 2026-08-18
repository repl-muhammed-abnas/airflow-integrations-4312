
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_uk_user_import_add_user_child_{config.instance}',
        description=f'MichaelKorsTnA UK_ Add V1.0 {config.instance}',
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
            name='exception_logger',
            value=[]
        )

        log_checkifrequiredfieldsarenotthere_4 = rail.PythonOperator(
            task_id='log_checkifrequiredfieldsarenotthere_4',
            python_callable=lambda dag_run: rail.smartjoin_by_delim((("" if dag_run.conf['firstname'] else "Employee First  Name not present") + ";" +
                ("" if dag_run.conf['lastname'] else "Employee Last  Name not present") + ";" + (
                "" if dag_run.conf['employeeid'] else "Employee ID not present") + ";" +
                ("" if dag_run.conf['hiredate'] else "Hire date is not present ")).split(";"), ";")
        )

        if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5 = rail.IfOperator(
            task_id='if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5',
            test='''{{ result('log_checkifrequiredfieldsarenotthere_4') | is_truthy }}''',
            yes_task="add_log_for_required_field_missing",
            no_task="search_entries_in_mapper_for_country",
        )

        add_log_for_required_field_missing = rail.WriteLogOperator(
            task_id='add_log_for_required_field_missing',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.employeeid}}",
                "action": "{{ dag_run.conf.type }}",
                "status": "Skipped",
                "details": "{{ result('log_checkifrequiredfieldsarenotthere_4') }}",
                "jobid": "{{ dag_run.conf.callerjobid }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"
            }
        )

        search_entries_in_mapper_for_country = rail.PythonOperator(
            task_id='search_entries_in_mapper_for_country',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["country"] == dag_run.conf['country'], config.michael_kors_gmbh_user_sync_master_mapper_uk))
        )

        if_first_id_blank_9 = rail.IfOperator(
            task_id='if_first_id_blank_9',
            test=lambda: len(rail.result(
                'search_entries_in_mapper_for_country')) < 1,
            yes_task="add_log_country_not_available_in_mapper",
            no_task="if_request_departmenturi_blank_12",
        )

        add_log_country_not_available_in_mapper = rail.WriteLogOperator(
            task_id='add_log_country_not_available_in_mapper',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Skipped",
            properties={
                "loginname": "{{dag_run.conf.employeeid}}",
                "action": "{{ dag_run.conf.type }}",
                "status": "Skipped",
                "details": "Country is not available in Mapper",
                "jobid": "{{ dag_run.conf.callerjobid }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"
            }
        )

        if_request_departmenturi_blank_12 = rail.IfOperator(
            task_id='if_request_departmenturi_blank_12',
            test='''{{ dag_run.conf.departmenturi | is_falsy }}''',
            yes_task="add_log_department_group_not_present",
            no_task="get_required_values_for_fields",
        )

        add_log_department_group_not_present = rail.WriteLogOperator(
            task_id='add_log_department_group_not_present',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                "loginname": "{{dag_run.conf.employeeid}}",
                "action": "{{ dag_run.conf.type }}",
                "status": "Error",
                "details": "Department group is not present",
                "jobid": "{{ dag_run.conf.callerjobid }}",
                "childjobid": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"
            }
        )

        def get_required_values(dag_run):
            country_entries = rail.result('search_entries_in_mapper_for_country')
            required_check = ("MK London Office" if "MK London Office" in dag_run.conf['location'] else 'Default') if dag_run.conf['location'] else 'Default'
            punchentrypolicydraft = (list(filter(lambda x: x['type'] == 'Punch Entry Policy' and x['identifier__1'] == required_check, country_entries)))
            timesheettemplatedraft = (list(filter(lambda x: x['type'] == 'Timesheet Template' and x['identifier__1'] == required_check, country_entries)))
            payrulebasedonjobprofile = (list(filter(lambda x: x['type'] == 'Payrule' and x['identifier__1'] == dag_run.conf['jobprofile'], country_entries)))
            payrulebasedonlocation = (list(filter(lambda x: x['type'] == 'Payrule' and x['identifier__1'] == required_check, country_entries)))
            officescheduledraft = (list(filter(lambda x: x['type'] == 'Schedule' and x['identifier__1'] == required_check, country_entries)))
            requiredsubstituteuserdraft = list(filter(lambda x:x['type'] == 'Substitute Exception' and x['identifier__1'] == dag_run.conf[
                'managerid'],country_entries))
            timesheet_approval_path_draft = list(filter(lambda x: x['type'] == 'Timesheet Approval Path' and x['identifier__1'] == (
            "MK London Office" if dag_run.conf['location'] == "MK London Office" else ""), country_entries))
            userpermissionsetdraft = list(filter(lambda x: x['type'] == 'Permission' and x['identifier__1'] == 'User',country_entries))
            requiredsubstituteuser = requiredsubstituteuserdraft[0]['value'] if requiredsubstituteuserdraft else ''
            return {
                'timesheetperiod': rail.find_first_by_attr_and_get_attr(country_entries, 'type', 'Timesheet Period', 'value', ''),
                'timeofftemplate': rail.find_first_by_attr_and_get_attr(country_entries, 'type', 'Timeoff Template', 'value', ''),
                'punchentrypolicy': punchentrypolicydraft[0]['value'] if punchentrypolicydraft else '',
                'timesheettemplate': timesheettemplatedraft[0]['value'] if timesheettemplatedraft else '',
                'timeoffapprovalpath': rail.find_first_by_attr_and_get_attr(country_entries, 'type', 'Timeoff Approval Path', 'value', ''),
                'timesheetapprovalpath': timesheet_approval_path_draft[0]['value'] if timesheet_approval_path_draft else rail.find_first_by_attr_and_get_attr(country_entries, 'type', 'Timesheet Approval Path', 'value', ''),
                'holidaycalendar': rail.find_first_by_attr_and_get_attr(country_entries, 'type', 'Holiday Calendar', 'value', ''),
                'workweek': rail.find_first_by_attr_and_get_attr(country_entries, 'type', 'Work Week', 'default__uri', ''),
                'authenticationtype': rail.find_first_by_attr_and_get_attr(country_entries, 'type', 'Authentication Type', 'default__uri', ''),
                'userpermissionset': userpermissionsetdraft[0]['value'] if userpermissionsetdraft else '',
                'payrule': payrulebasedonjobprofile[0]['value'] if payrulebasedonjobprofile else (
                    payrulebasedonlocation[0]['value']if payrulebasedonlocation else ''),
                'officeschedule': officescheduledraft[0]['value'] if officescheduledraft else '',
                'timezone': {
                    "uri": rail.find_first_by_attr_and_get_attr(country_entries,'type','Timezone','default__uri',''),
                    "IANAName": null
                },
                'licenses': [item['default__uri'] for item in list(filter(lambda x: x['type'] == 'License', country_entries))],
                'language': rail.find_first_by_attr_and_get_attr(country_entries, 'type', 'Language', 'default__uri', ''),
                'supervisorpermission': [entry['value'] for entry in (list(filter(lambda x: x['type'] == 'Permission' and x[
                    'identifier__1'] == 'Supervisor', country_entries)))],
                'requiredmanager': requiredsubstituteuser if requiredsubstituteuser else dag_run.conf['managerid']
            }

        get_required_values_for_fields = rail.PythonOperator(
            task_id='get_required_values_for_fields',
            python_callable=get_required_values
        )

        declare_variable_27 = rail.SetVariableOperator(
            task_id='declare_variable_27',
            append=False,
            name='schedule',
            value=None
        )

        if_log_required_office_schedule_26_present_28 = rail.IfOperator(
            task_id='if_log_required_office_schedule_26_present_28',
            test='''{{ result('get_required_values_for_fields').officeschedule | is_truthy }}''',
            yes_task="if_log_required_office_schedule_26_equals_to_shiftschedule_29",
            no_task="invoke_custom_ruby_code_33",
        )

        if_log_required_office_schedule_26_equals_to_shiftschedule_29 = rail.IfOperator(
            task_id='if_log_required_office_schedule_26_equals_to_shiftschedule_29',
            test='''{{ result('get_required_values_for_fields').officeschedule == 'Shift Schedule' }}''',
            yes_task="update_variable_30",
            no_task="update_variable_32",
        )

        update_variable_30 = rail.SetVariableOperator(
            task_id='update_variable_30',
            append=False,
            name='{{ result("declare_variable_27").name }}',
            value=[
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": null,
                        "officeSchedule": null,
                        "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                    },
                    "effectiveDate": null
                }
            ]
        )

        update_variable_32 = rail.SetVariableOperator(
            task_id='update_variable_32',
            append=False,
            name='{{ result("declare_variable_27").name }}',
            value=[
                {
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": "{{ result('get_required_values_for_fields').officeschedule }}",
                        "officeSchedule": {
                            "officeScheduleUri": null,
                            "name": "{{ result('get_required_values_for_fields').officeschedule }}"
                        },
                        "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                    },
                    "effectiveDate": null
                }
            ]
        )

        def get_date_object(datestring):
            dateobj = datetime.strptime(datestring, "%Y-%m-%d")
            return {
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year
            }

        invoke_custom_ruby_code_33 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_33',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['hiredate'])
        )

        create_user_34 = rail.RepliconServiceOperator(
            task_id='create_user_34',
            endpoint="/services/importservice1.svc/PutUser3",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['employeeid'],
                        "parameterCorrelationId": null
                    },
                    "firstname": dag_run.conf['firstname'],
                    "lastname": dag_run.conf['lastname'],
                    "emailAddress": dag_run.conf['workemail'],
                    "employeeId": dag_run.conf['employeeid'],
                    "department": null,
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": rail.get_dag_run_var('schedule'),
                    "workWeekStartDayUri": rail.result('get_required_values_for_fields')['workweek'],
                    "employmentDateRange": {
                        "startDate": {
                            "year": rail.result('invoke_custom_ruby_code_33')['year'],
                            "month": rail.result('invoke_custom_ruby_code_33')['month'],
                            "day": rail.result('invoke_custom_ruby_code_33')['day']
                        },
                        "endDate": null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            rail.result('get_required_values_for_fields')[
                                'authenticationtype']
                        ],
                        "isLoginEnabled": "true",
                        "loginName": dag_run.conf['employeeid'],
                        "SSOName": dag_run.conf['employeeid'],
                        "password": "Replicon@12#"
                    },
                    "holidayCalendar": {
                        "uri": null,
                        "name": rail.result('get_required_values_for_fields')['holidaycalendar']
                    },
                    "timeOffPolicy": null,
                    "permissionSets": [
                        {
                            "uri": null,
                            "name": rail.result('get_required_values_for_fields')['userpermissionset']
                        }
                    ],
                    "policySets": [
                        {
                            "uri": null,
                            "name": rail.result('get_required_values_for_fields')['timeofftemplate']
                        },
                        {
                            "uri": null,
                            "name": rail.result('get_required_values_for_fields')['punchentrypolicy']
                        },
                        {
                            "uri": null,
                            "name": rail.result('get_required_values_for_fields')['timesheettemplate']
                        }
                    ],
                    "employeeType": null,
                    "timesheetPeriodTypeUri": null,
                    "costRateSchedule": null,
                    "payrollRateSchedule": null,
                    "defaultBillingRate": null,
                    "timesheetApprovalPath": {
                        "uri": null,
                        "name": rail.result('get_required_values_for_fields')['timesheetapprovalpath']
                    },
                    "expenseApprovalPath": null,
                    "timeOffApprovalPath": {
                        "uri": null,
                        "name": rail.result('get_required_values_for_fields')['timeoffapprovalpath']
                    },
                    "customFieldValues": [],
                    "assignedActivities": [],
                    "timeZone": rail.result('get_required_values_for_fields')['timezone'],
                    "overtimeRuleAssignmentSchedule": null,
                    "validationRuleAssignmentSchedule": null,
                    "locationSchedule": [
                        {
                            "location": {
                                "uri": dag_run.conf['locationuri'],
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "divisionSchedule": [
                        {
                            "division": {
                                "uri": null,
                                "parentUri": null,
                                "name": dag_run.conf['country']
                            },
                            "effectiveDate": null
                        }
                    ],
                    "costCenterSchedule": [
                        {
                            "costCenter": {
                                "uri": dag_run.conf['costcenteruri'],
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "serviceCenterSchedule": [
                        {
                            "serviceCenter": {
                                "uri": dag_run.conf['weeklyscheduleuri'],
                                "parentUri": null,
                                "name": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "departmentGroupSchedule": [
                        {
                            "departmentGroup": {
                                "uri": dag_run.conf['departmenturi'],
                                "parent": null,
                                "name": null,
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "employeeTypeGroupSchedule": [],
                    "timesheetPeriodSchedule": [
                        {
                            "timesheetPeriod": {
                                "uri": null,
                                "name": rail.result('get_required_values_for_fields')['timesheetperiod']
                            },
                            "effectiveDate": null
                        }
                    ],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": [
                        {
                            "payRuleScript": {
                                "uri": null,
                                "name": rail.result('get_required_values_for_fields')['payrule']
                            },
                            "effectiveDate": null
                        }
                    ]
                }
            }
        )

        remove_timeoff_assignments_35 = rail.RepliconServiceOperator(
            task_id='remove_timeoff_assignments_35',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user_34').uri }}",
                "timeOffTypeUris": []
            }
        )

        put_product_assignments_for_user_37 = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user_37',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user_34')['uri'],
                "productUris": rail.result('get_required_values_for_fields')['licenses']
            }
        )

        update_language_39 = rail.RepliconServiceOperator(
            task_id='update_language_39',
            endpoint="/services/InternationalizationService1.svc/UpdateLanguageForUser",
            data={
                "userUri": "{{ result('create_user_34').uri }}",
                "languageUri": "{{ result('get_required_values_for_fields').language }}"
            }
        )

        if_request_managerid_present_40 = rail.IfOperator(
            task_id='if_request_managerid_present_40',
            test='''{{ dag_run.conf.managerid | is_truthy }}''',
            yes_task="if_requiredmanager_equals_employeeid",
            no_task="if_request_locationaddress_present_62",
        )

        if_requiredmanager_equals_employeeid = rail.IfOperator(
            task_id = 'if_requiredmanager_equals_employeeid',
            test=lambda dag_run: dag_run.conf['employeeid'] == rail.result('get_required_values_for_fields')['requiredmanager'],
            yes_task='add_exception_user_and_supervisor_same',
            no_task='search_users_search_supervisor_41'
        )

        add_exception_user_and_supervisor_same = rail.SetVariableOperator(
            task_id = 'add_exception_user_and_supervisor_same',
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Supervisor not assigned since the user and manager IDs are same"
            }

        )

        def get_user_uri_and_status(response):
            users_found = response['rows']
            matching_user = list(filter(
                lambda user: user['cells'][1]['textValue'] == rail.result('get_required_values_for_fields')['requiredmanager'], users_found))
            return {
                'uri': matching_user[0]['cells'][1]['uri'] if matching_user else '',
                'status': matching_user[0]['cells'][2].get('boolValue') if matching_user else ''
            }

        search_users_search_supervisor_41 = rail.RepliconServiceOperator(
            task_id='search_users_search_supervisor_41',
            endpoint='/services/UserListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:login-name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{ result('get_required_values_for_fields').requiredmanager }}"
                        }
                    }
                }
            },
            data_handler=get_user_uri_and_status
        )

        if_log_supervisor_uri_42_blank_43 = rail.IfOperator(
            task_id='if_log_supervisor_uri_42_blank_43',
            test='''{{ result('search_users_search_supervisor_41').uri | is_falsy }}''',
            yes_task="add_supervisor_assignment_to_queue",
            no_task="if_downcase_not_equals_to_true_47",
        )

        add_supervisor_assignment_to_queue = rail.WriteLogOperator(
            task_id='add_supervisor_assignment_to_queue',
            log="{{ dag_run.conf.supervisorlookup }}",
            message="na",
            severity="queued",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['callerjobid'],
                "username": dag_run.conf['employeeid'],
                "useruri": rail.result('create_user_34')['uri'],
                "supervisorloginname": rail.result('get_required_values_for_fields')['requiredmanager'],
                "action": "Add",
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "status": "queued",
                "supervisoreffectivedate": datetime.now().strftime("%d/%m/%Y"),
                "supervisorusername": dag_run.conf['workersmanager'],
                "country": dag_run.conf['country']
            }
        )

        if_downcase_not_equals_to_true_47 = rail.IfOperator(
            task_id='if_downcase_not_equals_to_true_47',
            test=lambda: not(rail.result('search_users_search_supervisor_41')[
                'status']),
            yes_task="add_supervisorassignment_to_queue",
            no_task="_adhoc_http_action_search_supervisor_50",
        )

        add_supervisorassignment_to_queue = rail.WriteLogOperator(
            task_id='add_supervisorassignment_to_queue',
            log="{{ dag_run.conf.supervisorlookup }}",
            message="na",
            severity="queued",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "username": "{{ dag_run.conf.employeeid }}",
                "useruri": "{{ result('create_user_34').uri }}",
                "supervisorloginname": "{{result('get_required_values_for_fields').requiredmanager}}",
                "action": "Add",
                "childjobid": "{{ dag_run_ecid() }}",
                "status": "queued",
                "supervisoreffectivedate": '{{current_time("%d/%m/%Y")}}',
                "supervisorusername": "{{ dag_run.conf.workersmanager }}",
                "country": "{{ dag_run.conf.country }}"
            }
        )

        _adhoc_http_action_search_supervisor_50 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_search_supervisor_50',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users_search_supervisor_41').uri }}"
            },
            data_handler=lambda response: {
                'supervisorpermission': rail.find_first_by_attr_and_get_attr(response, 'policyUri',
                    'urn:replicon:policy:supervision', 'permissionSet.name', '') if response and response[0]['policyUri'] else '',
                'enduserpermission': rail.find_first_by_attr_and_get_attr(response, 'policyUri',
                    'urn:replicon:policy:user', 'permissionSet.name', '') if response and response[0]['policyUri'] else '',
                'schedulemanagementpermission': rail.find_first_by_attr_and_get_attr(response, 'policyUri',
                    'urn:replicon:policy:schedule-management', 'permissionSet.name', '') if response and response[0]['policyUri'] else '',
            }
        )

        if_log_supervisorpermissionassignedtouser_52_blank_55 = rail.IfOperator(
            task_id='if_log_supervisorpermissionassignedtouser_52_blank_55',
            test=lambda: not (rail.result('_adhoc_http_action_search_supervisor_50')['supervisorpermission']) or not (
                rail.result('_adhoc_http_action_search_supervisor_50')['enduserpermission']) or (
                rail.result('_adhoc_http_action_search_supervisor_50')['supervisorpermission'] not in rail.result(
                'get_required_values_for_fields')['supervisorpermission']) or (
                rail.result('_adhoc_http_action_search_supervisor_50')['enduserpermission'] not in rail.result(
                'get_required_values_for_fields')['supervisorpermission']) or (
                rail.result('_adhoc_http_action_search_supervisor_50')['schedulemanagementpermission'] not in rail.result(
                'get_required_values_for_fields')['supervisorpermission']) or not (
                rail.result('_adhoc_http_action_search_supervisor_50')['schedulemanagementpermission']),
            yes_task="_adhoc_http_action_search_supervisor_56",
            no_task="assign_initial_supervisor_61",
        )

        _adhoc_http_action_search_supervisor_56 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_search_supervisor_56',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        foreach_document_58 = rail.ForEachOperator(
            task_id='foreach_document_58',
            items=lambda: list(filter(lambda entry: entry['type'] == 'Permission' and entry['identifier__1'] == 'Supervisor', rail.result(
                'search_entries_in_mapper_for_country'))),
            start_task='assign_permission_set_to_user_60',
            end_task='foreach_document_58_end'
        )

        assign_permission_set_to_user_60 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_60',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda: {
                "userUri": rail.result('search_users_search_supervisor_41')['uri'],
                "permissionSetUri": rail.find_first_by_attr_and_get_attr(rail.result('_adhoc_http_action_search_supervisor_56'), 'name', rail.result(
                    'foreach_document_58')['value'], 'uri', '')
            }
        )

        foreach_document_58_end = rail.EmptyOperator(
            task_id='foreach_document_58_end',
        )

        assign_initial_supervisor_61 = rail.RepliconServiceOperator(
            task_id='assign_initial_supervisor_61',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ result('create_user_34').uri }}",
                "supervisorUri": "{{ result('search_users_search_supervisor_41').uri }}",
                "dateRange": null
            }
        )

        if_request_locationaddress_present_62 = rail.IfOperator(
            task_id='if_request_locationaddress_present_62',
            test='''{{ dag_run.conf.locationaddress | is_truthy }}''',
            yes_task="if_request_locationaddressuri_blank_63",
            no_task="if_request_collectiveagreement_present_67",
        )

        if_request_locationaddressuri_blank_63 = rail.IfOperator(
            task_id='if_request_locationaddressuri_blank_63',
            test='''{{ dag_run.conf.locationaddressuri | is_falsy }}''',
            yes_task="insert_to_list_64",
            no_task="updated_u_d_ffor_location_address_66",
        )

        insert_to_list_64 = rail.SetVariableOperator(
            task_id='insert_to_list_64',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Location Address udf is not available"
            }
        )

        updated_u_d_ffor_location_address_66 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_location_address_66',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_34').uri }}",
                "customFieldUri": "{{ dag_run.conf.locationaddressuri }}",
                "value": "{{ dag_run.conf.locationaddress }}"
            }
        )

        if_request_collectiveagreement_present_67 = rail.IfOperator(
            task_id='if_request_collectiveagreement_present_67',
            test='''{{ dag_run.conf.collectiveagreement | is_truthy }}''',
            yes_task="if_request_collectiveagreementuri_blank_68",
            no_task="if_request_contracttype_present_72",
        )

        if_request_collectiveagreementuri_blank_68 = rail.IfOperator(
            task_id='if_request_collectiveagreementuri_blank_68',
            test='''{{ dag_run.conf.collectiveagreementuri | is_falsy }}''',
            yes_task="insert_to_list_69",
            no_task="updated_u_d_ffor_collective_agreement_71",
        )

        insert_to_list_69 = rail.SetVariableOperator(
            task_id='insert_to_list_69',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Collective Agreement udf is not available"
            }
        )

        updated_u_d_ffor_collective_agreement_71 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_collective_agreement_71',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_34').uri }}",
                "customFieldUri": "{{ dag_run.conf.collectiveagreementuri }}",
                "value": "{{ dag_run.conf.collectiveagreement }}"
            }
        )

        if_request_contracttype_present_72 = rail.IfOperator(
            task_id='if_request_contracttype_present_72',
            test='''{{ dag_run.conf.contracttype | is_truthy }}''',
            yes_task="if_request_contracttypeuri_blank_73",
            no_task="if_request_defaultweeklyhours_present_77",
        )

        if_request_contracttypeuri_blank_73 = rail.IfOperator(
            task_id='if_request_contracttypeuri_blank_73',
            test='''{{ dag_run.conf.contracttypeuri | is_falsy }}''',
            yes_task="insert_to_list_74",
            no_task="updated_u_d_ffor_contract_type_76",
        )

        insert_to_list_74 = rail.SetVariableOperator(
            task_id='insert_to_list_74',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Contract Type udf is not available"
            }
        )

        updated_u_d_ffor_contract_type_76 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_contract_type_76',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_34').uri }}",
                "customFieldUri": "{{ dag_run.conf.contracttypeuri }}",
                "value": "{{ dag_run.conf.contracttype }}"
            }
        )

        if_request_defaultweeklyhours_present_77 = rail.IfOperator(
            task_id='if_request_defaultweeklyhours_present_77',
            test='''{{ dag_run.conf.defaultweeklyhours | is_truthy }}''',
            yes_task="if_request_defaultweeklyhoursuri_blank_78",
            no_task="if_request_compensationgrade_present_82",
        )

        if_request_defaultweeklyhoursuri_blank_78 = rail.IfOperator(
            task_id='if_request_defaultweeklyhoursuri_blank_78',
            test='''{{ dag_run.conf.defaultweeklyhoursuri | is_falsy }}''',
            yes_task="insert_to_list_79",
            no_task="updated_u_d_ffor_default_weekly_hours_81",
        )

        insert_to_list_79 = rail.SetVariableOperator(
            task_id='insert_to_list_79',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Default Weekly Hours udf is not available"
            }
        )

        updated_u_d_ffor_default_weekly_hours_81 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_default_weekly_hours_81',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_34').uri }}",
                "customFieldUri": "{{ dag_run.conf.defaultweeklyhoursuri }}",
                "value": "{{ dag_run.conf.defaultweeklyhours }}"
            }
        )

        if_request_compensationgrade_present_82 = rail.IfOperator(
            task_id='if_request_compensationgrade_present_82',
            test='''{{ dag_run.conf.compensationgrade | is_truthy }}''',
            yes_task="if_request_compensationgradeuri_blank_83",
            no_task="if_request_jobprofilecode_present_87",
        )

        if_request_compensationgradeuri_blank_83 = rail.IfOperator(
            task_id='if_request_compensationgradeuri_blank_83',
            test='''{{ dag_run.conf.compensationgradeuri | is_falsy }}''',
            yes_task="insert_to_list_84",
            no_task="updated_u_d_ffor_compensation_grade_86",
        )

        insert_to_list_84 = rail.SetVariableOperator(
            task_id='insert_to_list_84',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Compensation Grade udf is not available"
            }
        )

        updated_u_d_ffor_compensation_grade_86 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_compensation_grade_86',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_34').uri }}",
                "customFieldUri": "{{ dag_run.conf.compensationgradeuri }}",
                "value": "{{ dag_run.conf.compensationgrade }}"
            }
        )

        if_request_jobprofilecode_present_87 = rail.IfOperator(
            task_id='if_request_jobprofilecode_present_87',
            test='''{{ dag_run.conf.jobprofilecode | is_truthy }}''',
            yes_task="if_request_jobprofilecodeuri_blank_88",
            no_task="if_request_jobprofile_present_92",
        )

        if_request_jobprofilecodeuri_blank_88 = rail.IfOperator(
            task_id='if_request_jobprofilecodeuri_blank_88',
            test='''{{ dag_run.conf.jobprofilecodeuri | is_falsy }}''',
            yes_task="insert_to_list_89",
            no_task="updated_u_d_ffor_job_profile_code_91",
        )

        insert_to_list_89 = rail.SetVariableOperator(
            task_id='insert_to_list_89',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Job Profile Code udf is not available"
            }
        )

        updated_u_d_ffor_job_profile_code_91 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_job_profile_code_91',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_34').uri }}",
                "customFieldUri": "{{ dag_run.conf.jobprofilecodeuri }}",
                "value": "{{ dag_run.conf.jobprofilecode }}"
            }
        )

        if_request_jobprofile_present_92 = rail.IfOperator(
            task_id='if_request_jobprofile_present_92',
            test='''{{ dag_run.conf.jobprofile | is_truthy }}''',
            yes_task="if_request_jobprofileuri_blank_93",
            no_task="if_request_businesstitle_present_97",
        )

        if_request_jobprofileuri_blank_93 = rail.IfOperator(
            task_id='if_request_jobprofileuri_blank_93',
            test='''{{ dag_run.conf.jobprofileuri | is_falsy }}''',
            yes_task="insert_to_list_94",
            no_task="updated_u_d_ffor_job_profile_96",
        )

        insert_to_list_94 = rail.SetVariableOperator(
            task_id='insert_to_list_94',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Job Profile udf is not available"
            }
        )

        updated_u_d_ffor_job_profile_96 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_job_profile_96',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_34').uri }}",
                "customFieldUri": "{{ dag_run.conf.jobprofileuri }}",
                "value": "{{ dag_run.conf.jobprofile }}"
            }
        )

        if_request_businesstitle_present_97 = rail.IfOperator(
            task_id='if_request_businesstitle_present_97',
            test='''{{ dag_run.conf.businesstitle | is_truthy }}''',
            yes_task="if_request_businesstitleuri_blank_98",
            no_task="if_request_scheduledweeklyhours_present_102",
        )

        if_request_businesstitleuri_blank_98 = rail.IfOperator(
            task_id='if_request_businesstitleuri_blank_98',
            test='''{{ dag_run.conf.businesstitleuri | is_falsy }}''',
            yes_task="insert_to_list_99",
            no_task="updated_u_d_ffor_business_title_101",
        )

        insert_to_list_99 = rail.SetVariableOperator(
            task_id='insert_to_list_99',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Business Title udf is not available"
            }
        )

        updated_u_d_ffor_business_title_101 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_business_title_101',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user_34').uri }}",
                "customFieldUri": "{{ dag_run.conf.businesstitleuri }}",
                "value": "{{ dag_run.conf.businesstitle }}"
            }
        )

        if_request_scheduledweeklyhours_present_102 = rail.IfOperator(
            task_id='if_request_scheduledweeklyhours_present_102',
            test='''{{ dag_run.conf.scheduledweeklyhours | is_truthy }}''',
            yes_task="if_request_scheduledweeklyhoursuri_blank_103",
            no_task="if_request_originalhiredate_present_107",
        )

        if_request_scheduledweeklyhoursuri_blank_103 = rail.IfOperator(
            task_id='if_request_scheduledweeklyhoursuri_blank_103',
            test='''{{ dag_run.conf.scheduledweeklyhoursuri | is_falsy }}''',
            yes_task="insert_to_list_104",
            no_task="updated_u_d_ffor_scheduled_weekly_hours_106",
        )

        insert_to_list_104 = rail.SetVariableOperator(
            task_id='insert_to_list_104',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Scheduled Weekly Hours udf is not available"
            }
        )

        updated_u_d_ffor_scheduled_weekly_hours_106 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_scheduled_weekly_hours_106',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ result('create_user_34').uri }}",
                "customFieldUri": "{{ dag_run.conf.scheduledweeklyhoursuri }}",
                "value": "{{ dag_run.conf.scheduledweeklyhours }}"
            }
        )

        if_request_originalhiredate_present_107 = rail.IfOperator(
            task_id='if_request_originalhiredate_present_107',
            test='''{{ dag_run.conf.originalhiredate | is_truthy }}''',
            yes_task="if_request_originalhiredateuri_blank_108",
            no_task="if_request_lastdayofwork_present_113",
        )

        if_request_originalhiredateuri_blank_108 = rail.IfOperator(
            task_id='if_request_originalhiredateuri_blank_108',
            test='''{{ dag_run.conf.originalhiredateuri | is_falsy }}''',
            yes_task="insert_to_list_109",
            no_task="invoke_custom_ruby_code_111",
        )

        insert_to_list_109 = rail.SetVariableOperator(
            task_id='insert_to_list_109',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Original Hire Date udf is not available"
            }
        )

        invoke_custom_ruby_code_111 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_111',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['originalhiredate'])
        )

        updated_u_d_ffor_original_hire_date_112 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_original_hire_date_112',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ result('create_user_34').uri }}",
                "customFieldUri": "{{ dag_run.conf.originalhiredateuri }}",
                "value": {
                    "year": "{{ result('invoke_custom_ruby_code_111').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_111').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_111').day }}"
                }
            }
        )

        if_request_lastdayofwork_present_113 = rail.IfOperator(
            task_id='if_request_lastdayofwork_present_113',
            test='''{{ dag_run.conf.lastdayofwork | is_truthy }}''',
            yes_task="if_request_lastdayofworkuri_blank_114",
            no_task="if_request_contractenddate_present_119",
        )

        if_request_lastdayofworkuri_blank_114 = rail.IfOperator(
            task_id='if_request_lastdayofworkuri_blank_114',
            test='''{{ dag_run.conf.lastdayofworkuri | is_falsy }}''',
            yes_task="insert_to_list_115",
            no_task="invoke_custom_ruby_code_117",
        )

        insert_to_list_115 = rail.SetVariableOperator(
            task_id='insert_to_list_115',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Last Day of Work udf is not available"
            }
        )

        invoke_custom_ruby_code_117 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_117',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['lastdayofwork'])
        )

        updated_u_d_ffor_last_dayof_work_118 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_last_dayof_work_118',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ result('create_user_34').uri }}",
                "customFieldUri": "{{ dag_run.conf.lastdayofworkuri }}",
                "value": {
                    "year": "{{ result('invoke_custom_ruby_code_117').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_117').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_117').day }}"
                }
            }
        )

        if_request_contractenddate_present_119 = rail.IfOperator(
            task_id='if_request_contractenddate_present_119',
            test='''{{ dag_run.conf.contractenddate | is_truthy }}''',
            yes_task="if_request_contractenddateuri_blank_120",
            no_task="trigger_child_add_timeoff_type_for_new_user",
        )

        if_request_contractenddateuri_blank_120 = rail.IfOperator(
            task_id='if_request_contractenddateuri_blank_120',
            test='''{{ dag_run.conf.contractenddateuri | is_falsy }}''',
            yes_task="insert_to_list_121",
            no_task="invoke_custom_ruby_code_123",
        )

        insert_to_list_121 = rail.SetVariableOperator(
            task_id='insert_to_list_121',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "log": "Original Hire Date udf is not available"
            }
        )

        invoke_custom_ruby_code_123 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_123',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['contractenddate'])
        )

        updated_u_d_ffor_contract_end_date_124 = rail.RepliconServiceOperator(
            task_id='updated_u_d_ffor_contract_end_date_124',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ result('create_user_34').uri }}",
                "customFieldUri": "{{ dag_run.conf.contractenddateuri }}",
                "value": {
                    "year": "{{ result('invoke_custom_ruby_code_123').year }}",
                    "month": "{{ result('invoke_custom_ruby_code_123').month }}",
                    "day": "{{ result('invoke_custom_ruby_code_123').day }}"
                }
            }
        )

        trigger_child_add_timeoff_type_for_new_user = rail.TriggerDagRunOperator(
            task_id='trigger_child_add_timeoff_type_for_new_user',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_child_workflow_to_add_timeoff_type_for_new_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['callerjobid'],
                "userloginname": dag_run.conf['employeeid'],
                "useruri": rail.result('create_user_34')['uri'],
                "startdate": str(rail.result('invoke_custom_ruby_code_33')['day']) + "/" + str(rail.result('invoke_custom_ruby_code_33')['month']) + "/" +
                    str(rail.result('invoke_custom_ruby_code_33')['year']),
                "type": "Add",
                "scheduledweeklyhours": dag_run.conf['scheduledweeklyhours'],
                "fullpart": ('Full Time' if float(dag_run.conf['scheduledweeklyhours']) >= 40 else 'Part Time') if dag_run.conf[
                    'scheduledweeklyhours'] else 'Full Time',
                "yearlyentitlement": dag_run.conf['yearlyentitlementuri'],
                "callerjobid": dag_run.conf['callerjobid']
            }
        )

        wait_for_child_add_timeoff_type_for_new_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_add_timeoff_type_for_new_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_add_timeoff_type_for_new_user") }}'
        )

        get_error_if_failure_in_timeoff_assignment = rail.GatherResultsFromDagRunsOperator(
            task_id='get_error_if_failure_in_timeoff_assignment',
            dag_runs='{{result("trigger_child_add_timeoff_type_for_new_user") }}',
            dagrun_task_id='catch_and_log_error'
        )

        if_reply_output_present_126 = rail.IfOperator(
            task_id='if_reply_output_present_126',
            test=lambda: rail.result(
                'get_error_if_failure_in_timeoff_assignment'),
            yes_task="insert_to_list_127",
            no_task="declare_variable_detailsfor_add_user_128",
        )

        insert_to_list_127 = rail.SetVariableOperator(
            task_id='insert_to_list_127',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value=lambda: {
                "log": rail.smartjoin_by_delim(rail.result('get_error_if_failure_in_timeoff_assignment'),',')
            }
        )

        declare_variable_detailsfor_add_user_128 = rail.SetVariableOperator(
            task_id='declare_variable_detailsfor_add_user_128',
            append=False,
            name='adduserjobdetails',
            value=None
        )

        if_request_type_equals_to_add_129 = rail.IfOperator(
            task_id='if_request_type_equals_to_add_129',
            test='''{{ dag_run.conf.type == 'Add' }}''',
            yes_task="update_variable_detailsfor_add_user_130",
            no_task="if_request_type_equals_to_rehire_131",
        )

        update_variable_detailsfor_add_user_130 = rail.SetVariableOperator(
            task_id='update_variable_detailsfor_add_user_130',
            append=False,
            name='{{ result("declare_variable_detailsfor_add_user_128").name }}',
            value=lambda: ('User (New) partially created, ' + (rail.smartjoin_by_delim([log['log'] for log in rail.get_dag_run_var(
                'exception_logger')], ","))) if rail.get_dag_run_var('exception_logger') else 'User (New) successfully created'
        )

        if_request_type_equals_to_rehire_131 = rail.IfOperator(
            task_id='if_request_type_equals_to_rehire_131',
            test='''{{ dag_run.conf.type == 'Rehire' }}''',
            yes_task="update_variable_detailsfor_add_user_rehire_132",
            no_task="add_final_log_for_user",
        )

        update_variable_detailsfor_add_user_rehire_132 = rail.SetVariableOperator(
            task_id='update_variable_detailsfor_add_user_rehire_132',
            append=False,
            name='{{ result("declare_variable_detailsfor_add_user_128").name }}',
            value=lambda: ('User (Rehire) partially created, ' + (rail.smartjoin_by_delim([log['log'] for log in rail.get_dag_run_var(
                'exception_logger')], ","))) if rail.get_dag_run_var('exception_logger') else 'User (Rehire) successfully created'
        )

        add_final_log_for_user = rail.WriteLogOperator(
            task_id='add_final_log_for_user',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity=lambda: 'Exception' if rail.get_dag_run_var(
                'exception_logger') else 'Success',
            properties=lambda dag_run: {
                "loginname": dag_run.conf['employeeid'],
                "action": dag_run.conf['type'],
                "status": 'Exception' if rail.get_dag_run_var('exception_logger') else 'Success',
                "jobid": dag_run.conf['callerjobid'],
                "details": rail.get_dag_run_var('adduserjobdetails'),
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "username": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.userimportlogtable }}",
            message="na",
            severity="Error",
            properties={
                "loginname": "{{dag_run.conf.employeeid}}",
                "action": "{{ dag_run.conf.type }}",
                "status": "Error",
                "jobid": "{{ dag_run.conf.callerjobid }}",
                "details": "{{get_error_message()}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}"
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
        declare_list_3 >> log_checkifrequiredfieldsarenotthere_4 >> if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5
        if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5 >> rail.Label(
            'Yes') >> add_log_for_required_field_missing >> catch_and_log_error
        if_log_20_present_conditiontocheckifrequiredfieldsexistin_inputfile_5 >> rail.Label(
            'No') >> search_entries_in_mapper_for_country >> if_first_id_blank_9
        if_first_id_blank_9 >> rail.Label(
            'Yes') >> add_log_country_not_available_in_mapper >> catch_and_log_error
        if_first_id_blank_9 >> rail.Label(
            'No') >> if_request_departmenturi_blank_12
        if_request_departmenturi_blank_12 >> rail.Label(
            'Yes') >> add_log_department_group_not_present >> catch_and_log_error
        if_request_departmenturi_blank_12 >> rail.Label(
            'No') >> get_required_values_for_fields >> declare_variable_27 >> if_log_required_office_schedule_26_present_28
        if_log_required_office_schedule_26_present_28 >> rail.Label(
            'Yes') >> if_log_required_office_schedule_26_equals_to_shiftschedule_29
        if_log_required_office_schedule_26_equals_to_shiftschedule_29 >> rail.Label(
            'Yes') >> update_variable_30 >> invoke_custom_ruby_code_33
        if_log_required_office_schedule_26_equals_to_shiftschedule_29 >> rail.Label(
            'No') >> update_variable_32 >> invoke_custom_ruby_code_33
        if_log_required_office_schedule_26_present_28 >> rail.Label(
            'No') >> invoke_custom_ruby_code_33 >> create_user_34 >> remove_timeoff_assignments_35 >> put_product_assignments_for_user_37 >> update_language_39
        update_language_39 >> if_request_managerid_present_40
        if_request_managerid_present_40 >> rail.Label(
            'Yes') >> if_requiredmanager_equals_employeeid
        if_requiredmanager_equals_employeeid >> rail.Label('Yes') >> add_exception_user_and_supervisor_same >> if_request_locationaddress_present_62
        if_requiredmanager_equals_employeeid >> rail.Label('No') >> search_users_search_supervisor_41
        search_users_search_supervisor_41 >> if_log_supervisor_uri_42_blank_43
        if_log_supervisor_uri_42_blank_43 >> rail.Label(
            'Yes') >> add_supervisor_assignment_to_queue >> if_request_locationaddress_present_62
        if_log_supervisor_uri_42_blank_43 >> rail.Label(
            'No') >> if_downcase_not_equals_to_true_47
        if_downcase_not_equals_to_true_47 >> rail.Label(
            'Yes') >> add_supervisorassignment_to_queue >> if_request_locationaddress_present_62
        if_downcase_not_equals_to_true_47 >> rail.Label(
            'No') >> _adhoc_http_action_search_supervisor_50 >> if_log_supervisorpermissionassignedtouser_52_blank_55
        if_log_supervisorpermissionassignedtouser_52_blank_55 >> rail.Label(
            'Yes') >> _adhoc_http_action_search_supervisor_56 >> foreach_document_58 >> assign_permission_set_to_user_60 >> foreach_document_58_end
        foreach_document_58 >> foreach_document_58_end >> assign_initial_supervisor_61 >> if_request_locationaddress_present_62
        if_log_supervisorpermissionassignedtouser_52_blank_55 >> rail.Label(
            'No') >> assign_initial_supervisor_61 >> if_request_locationaddress_present_62
        if_request_managerid_present_40 >> rail.Label(
            'No') >> if_request_locationaddress_present_62
        if_request_locationaddress_present_62 >> rail.Label(
            'Yes') >> if_request_locationaddressuri_blank_63
        if_request_locationaddressuri_blank_63 >> rail.Label(
            'Yes') >> insert_to_list_64 >> if_request_collectiveagreement_present_67
        if_request_locationaddressuri_blank_63 >> rail.Label(
            'No') >> updated_u_d_ffor_location_address_66 >> if_request_collectiveagreement_present_67
        if_request_locationaddress_present_62 >> rail.Label(
            'No') >> if_request_collectiveagreement_present_67
        if_request_collectiveagreement_present_67 >> rail.Label(
            'Yes') >> if_request_collectiveagreementuri_blank_68
        if_request_collectiveagreementuri_blank_68 >> rail.Label(
            'Yes') >> insert_to_list_69 >> if_request_contracttype_present_72
        if_request_collectiveagreementuri_blank_68 >> rail.Label(
            'No') >> updated_u_d_ffor_collective_agreement_71 >> if_request_contracttype_present_72
        if_request_collectiveagreement_present_67 >> rail.Label(
            'No') >> if_request_contracttype_present_72
        if_request_contracttype_present_72 >> rail.Label(
            'Yes') >> if_request_contracttypeuri_blank_73
        if_request_contracttypeuri_blank_73 >> rail.Label(
            'Yes') >> insert_to_list_74 >> if_request_defaultweeklyhours_present_77
        if_request_contracttypeuri_blank_73 >> rail.Label(
            'No') >> updated_u_d_ffor_contract_type_76 >> if_request_defaultweeklyhours_present_77
        if_request_contracttype_present_72 >> rail.Label(
            'No') >> if_request_defaultweeklyhours_present_77
        if_request_defaultweeklyhours_present_77 >> rail.Label(
            'Yes') >> if_request_defaultweeklyhoursuri_blank_78
        if_request_defaultweeklyhoursuri_blank_78 >> rail.Label(
            'Yes') >> insert_to_list_79 >> if_request_compensationgrade_present_82
        if_request_defaultweeklyhoursuri_blank_78 >> rail.Label(
            'No') >> updated_u_d_ffor_default_weekly_hours_81 >> if_request_compensationgrade_present_82
        if_request_defaultweeklyhours_present_77 >> rail.Label(
            'No') >> if_request_compensationgrade_present_82
        if_request_compensationgrade_present_82 >> rail.Label(
            'Yes') >> if_request_compensationgradeuri_blank_83
        if_request_compensationgradeuri_blank_83 >> rail.Label(
            'Yes') >> insert_to_list_84 >> if_request_jobprofilecode_present_87
        if_request_compensationgradeuri_blank_83 >> rail.Label(
            'No') >> updated_u_d_ffor_compensation_grade_86 >> if_request_jobprofilecode_present_87
        if_request_compensationgrade_present_82 >> rail.Label(
            'No') >> if_request_jobprofilecode_present_87
        if_request_jobprofilecode_present_87 >> rail.Label(
            'Yes') >> if_request_jobprofilecodeuri_blank_88
        if_request_jobprofilecodeuri_blank_88 >> rail.Label(
            'Yes') >> insert_to_list_89 >> if_request_jobprofile_present_92
        if_request_jobprofilecodeuri_blank_88 >> rail.Label(
            'No') >> updated_u_d_ffor_job_profile_code_91 >> if_request_jobprofile_present_92
        if_request_jobprofilecode_present_87 >> rail.Label(
            'No') >> if_request_jobprofile_present_92
        if_request_jobprofile_present_92 >> rail.Label(
            'Yes') >> if_request_jobprofileuri_blank_93
        if_request_jobprofileuri_blank_93 >> rail.Label(
            'Yes') >> insert_to_list_94 >> if_request_businesstitle_present_97
        if_request_jobprofileuri_blank_93 >> rail.Label(
            'No') >> updated_u_d_ffor_job_profile_96 >> if_request_businesstitle_present_97
        if_request_jobprofile_present_92 >> rail.Label(
            'No') >> if_request_businesstitle_present_97
        if_request_businesstitle_present_97 >> rail.Label(
            'Yes') >> if_request_businesstitleuri_blank_98
        if_request_businesstitleuri_blank_98 >> rail.Label(
            'Yes') >> insert_to_list_99 >> if_request_scheduledweeklyhours_present_102
        if_request_businesstitleuri_blank_98 >> rail.Label(
            'No') >> updated_u_d_ffor_business_title_101 >> if_request_scheduledweeklyhours_present_102
        if_request_businesstitle_present_97 >> rail.Label(
            'No') >> if_request_scheduledweeklyhours_present_102
        if_request_scheduledweeklyhours_present_102 >> rail.Label(
            'Yes') >> if_request_scheduledweeklyhoursuri_blank_103
        if_request_scheduledweeklyhoursuri_blank_103 >> rail.Label(
            'Yes') >> insert_to_list_104 >> if_request_originalhiredate_present_107
        if_request_scheduledweeklyhoursuri_blank_103 >> rail.Label(
            'No') >> updated_u_d_ffor_scheduled_weekly_hours_106 >> if_request_originalhiredate_present_107
        if_request_scheduledweeklyhours_present_102 >> rail.Label(
            'No') >> if_request_originalhiredate_present_107
        if_request_originalhiredate_present_107 >> rail.Label(
            'Yes') >> if_request_originalhiredateuri_blank_108
        if_request_originalhiredateuri_blank_108 >> rail.Label(
            'Yes') >> insert_to_list_109 >> if_request_lastdayofwork_present_113
        if_request_originalhiredateuri_blank_108 >> rail.Label(
            'No') >> invoke_custom_ruby_code_111 >> updated_u_d_ffor_original_hire_date_112 >> if_request_lastdayofwork_present_113
        if_request_originalhiredate_present_107 >> rail.Label(
            'No') >> if_request_lastdayofwork_present_113
        if_request_lastdayofwork_present_113 >> rail.Label(
            'Yes') >> if_request_lastdayofworkuri_blank_114
        if_request_lastdayofworkuri_blank_114 >> rail.Label(
            'Yes') >> insert_to_list_115 >> if_request_contractenddate_present_119
        if_request_lastdayofworkuri_blank_114 >> rail.Label(
            'No') >> invoke_custom_ruby_code_117 >> updated_u_d_ffor_last_dayof_work_118 >> if_request_contractenddate_present_119
        if_request_lastdayofwork_present_113 >> rail.Label(
            'No') >> if_request_contractenddate_present_119
        if_request_contractenddate_present_119 >> rail.Label(
            'Yes') >> if_request_contractenddateuri_blank_120
        if_request_contractenddateuri_blank_120 >> rail.Label(
            'Yes') >> insert_to_list_121 >> trigger_child_add_timeoff_type_for_new_user
        if_request_contractenddateuri_blank_120 >> rail.Label(
            'No') >> invoke_custom_ruby_code_123 >> updated_u_d_ffor_contract_end_date_124
        updated_u_d_ffor_contract_end_date_124 >> trigger_child_add_timeoff_type_for_new_user
        if_request_contractenddate_present_119 >> rail.Label(
            'No') >> trigger_child_add_timeoff_type_for_new_user >> wait_for_child_add_timeoff_type_for_new_user
        wait_for_child_add_timeoff_type_for_new_user >> get_error_if_failure_in_timeoff_assignment >> if_reply_output_present_126
        if_reply_output_present_126 >> rail.Label(
            'Yes') >> insert_to_list_127 >> declare_variable_detailsfor_add_user_128
        if_reply_output_present_126 >> rail.Label(
            'No') >> declare_variable_detailsfor_add_user_128 >> if_request_type_equals_to_add_129
        if_request_type_equals_to_add_129 >> rail.Label(
            'Yes') >> update_variable_detailsfor_add_user_130 >> if_request_type_equals_to_rehire_131
        if_request_type_equals_to_add_129 >> rail.Label(
            'No') >> if_request_type_equals_to_rehire_131
        if_request_type_equals_to_rehire_131 >> rail.Label(
            'Yes') >> update_variable_detailsfor_add_user_rehire_132 >> add_final_log_for_user
        if_request_type_equals_to_rehire_131 >> rail.Label(
            'No') >> add_final_log_for_user >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
