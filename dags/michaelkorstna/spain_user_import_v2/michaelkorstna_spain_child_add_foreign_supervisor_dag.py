
from datetime import timedelta
from airflow.models import Variable
import rail

from michaelkorstna.spain_user_import_v2.mappers.michael_kors_gmbh_user_sync_master_mapper_spain import michael_kors_gmbh_user_sync_master_mapper_spain

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_spain_user_import_add_foreign_supervisor_child_{config.instance}_{config.version}',
        description=f'MichaelKorsTnA Spain_Child_Add Foreign Supervisor {config.instance}_{config.version}',
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
            no_task='search_users_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_users_3',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_supervisor_uri_and_status(response, dag_run):
            users_found = response['rows']
            supervisor = {}
            for user in users_found:
                if user['cells'][0]['textValue'] == dag_run.conf['supervisorloginname']:
                    supervisor = user
                    break
            return {
                'uri': supervisor['cells'][0]['uri'] if supervisor else '',
                'status': supervisor['cells'][1]['textValue'] if supervisor else ''
            }

        search_users_3 = rail.RepliconServiceOperator(
            task_id='search_users_3',
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
                            "text": "{{ dag_run.conf.supervisorloginname }}",
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

        if_log_getsupervisor_uri_4_blank_5 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_4_blank_5',
            test='''{{ result('search_users_3').uri | is_falsy }}''',
            yes_task="log_first_name_6",
            no_task="catch_error",
        )

        log_first_name_6 = rail.PythonOperator(
            task_id='log_first_name_6',
            python_callable=lambda dag_run: {
                'firstname': (dag_run.conf['supervisorname'].split(" "))[0],
                'lastname': ((dag_run.conf['supervisorname']).replace((dag_run.conf['supervisorname'].split(" "))[0], "")).strip()
            }
        )

        search_mappings_for_foreignsupervisors = rail.PythonOperator(
            task_id='search_mappings_for_foreignsupervisors',
            python_callable=lambda:  list(filter(
                lambda x: x["country"] == "Foreign Supervisors", michael_kors_gmbh_user_sync_master_mapper_spain))
        )

        def get_requiredfields_values():
            mapperentries = rail.result(
                'search_mappings_for_foreignsupervisors')
            return {
                'country': rail.find_first_by_attr_and_get_attr(mapperentries, 'type', 'Country', 'value', ''),
                'permissionset': [{
                    'uri': null,
                    'name': permission['value']
                } for permission in list(filter(lambda entry: entry['type'] == 'Permission', mapperentries))],
                'authenticationtype': rail.find_first_by_attr_and_get_attr(mapperentries, 'type', 'Authentication Type', 'default__uri', ''),
                'language': rail.find_first_by_attr_and_get_attr(mapperentries, 'type', 'Language', 'default__uri', ''),
                'department': rail.find_first_by_attr_and_get_attr(mapperentries, 'type', 'Department', 'value', ''),
                'parentdepartment': rail.find_first_by_attr_and_get_attr(mapperentries,'type', 'Parent Department', 'value', ''),
                'employeetype': rail.find_first_by_attr_and_get_attr(mapperentries, 'type', 'Employee Type', 'value', ''),
                'licenses': [license['default__uri'] for license in list(filter(lambda entry: entry['type'] == 'License', mapperentries))],
                'officedefaultschedule': rail.find_first_by_attr_and_get_attr(mapperentries, 'type', 'Schedule', 'value', ''),
            }

        get_required_fields = rail.PythonOperator(
            task_id='get_required_fields',
            python_callable=get_requiredfields_values
        )

        create_userforrequiredforeign_supervisor_22 = rail.RepliconServiceOperator(
            task_id='create_userforrequiredforeign_supervisor_22',
            endpoint="/services/importservice1.svc/PutUser3",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['supervisorloginname'],
                        "parameterCorrelationId": null
                    },
                    "firstname": rail.result('log_first_name_6')['firstname'],
                    "lastname": rail.result('log_first_name_6')['lastname'],
                    "emailAddress": null,
                    "employeeId": dag_run.conf['supervisorloginname'],
                    "department": null,
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": rail.result('get_required_fields')['officedefaultschedule'],
                                "officeSchedule": {
                                    "officeScheduleUri": null,
                                    "name": rail.result('get_required_fields')['officedefaultschedule']
                                },
                                "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                            },
                            "effectiveDate": null
                        }
                    ],
                    "workWeekStartDayUri": null,
                    "employmentDateRange": null,
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            rail.result('get_required_fields')[
                                'authenticationtype']
                        ],
                        "isLoginEnabled": "true",
                        "loginName": dag_run.conf['supervisorloginname'],
                        "SSOName": dag_run.conf['supervisorloginname'],
                        "password": "Replicon@12#"
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": rail.result('get_required_fields')['permissionset'],
                    "policySets": [],
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
                    "divisionSchedule": [
                        {
                            "division": {
                                "uri": null,
                                "parentUri": null,
                                "name": rail.result('get_required_fields')['country']
                            },
                            "effectiveDate": null
                        }
                    ],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "departmentGroupSchedule": [
                        {
                            "departmentGroup": {
                                "uri": null,
                                "parent": {
                                    "uri": null,
                                    "parent": null,
                                    "name": rail.result('get_required_fields')['parentdepartment'],
                                    "parameterCorrelationId": null
                                },
                                "name": rail.result('get_required_fields')['department'],
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
                                "name": rail.result('get_required_fields')['employeetype'],
                                "parameterCorrelationId": null
                            },
                            "effectiveDate": null
                        }
                    ],
                    "timesheetPeriodSchedule": [],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        remove_timeoff_assignments_23 = rail.RepliconServiceOperator(
            task_id='remove_timeoff_assignments_23',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_userforrequiredforeign_supervisor_22').uri }}",
                "timeOffTypeUris": []
            }
        )

        remove_start_date_24 = rail.RepliconServiceOperator(
            task_id='remove_start_date_24',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ result('create_userforrequiredforeign_supervisor_22').uri }}",
                "dateRange": null
            }
        )

        put_product_assignments_for_user_25 = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user_25',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_userforrequiredforeign_supervisor_22')['uri'],
                "productUris": rail.result('get_required_fields')['licenses']
            }
        )

        update_language_26 = rail.RepliconServiceOperator(
            task_id='update_language_26',
            endpoint="/services/InternationalizationService1.svc/UpdateLanguageForUser",
            data={
                "userUri": "{{ result('create_userforrequiredforeign_supervisor_22').uri }}",
                "languageUri": "{{ result('get_required_fields').language }}"
            }
        )

        update_holiday_calendar_for_user_27 = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user_27',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ result('create_userforrequiredforeign_supervisor_22').uri }}",
                "holidayCalendarUri": null
            }
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "{{get_error_message()}}")
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label('No') >> search_users_3
        search_users_3 >> if_log_getsupervisor_uri_4_blank_5
        if_log_getsupervisor_uri_4_blank_5 >> rail.Label(
            'Yes') >> log_first_name_6 >> search_mappings_for_foreignsupervisors >> get_required_fields >> create_userforrequiredforeign_supervisor_22
        create_userforrequiredforeign_supervisor_22 >> remove_timeoff_assignments_23 >> remove_start_date_24 >> put_product_assignments_for_user_25
        put_product_assignments_for_user_25 >> update_language_26 >> update_holiday_calendar_for_user_27 >> catch_error
        if_log_getsupervisor_uri_4_blank_5 >> rail.Label(
            'No') >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
