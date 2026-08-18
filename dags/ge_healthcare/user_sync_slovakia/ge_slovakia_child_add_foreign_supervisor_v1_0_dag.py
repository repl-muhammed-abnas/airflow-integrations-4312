
from datetime import timedelta
import itertools
from ge_healthcare.user_sync_slovakia.slovakia_master_mapper import slovakia_master_mapper
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'gehealthcare_slovakia_child_add_foreign_supervisor_v1_0_{config.instance}',
        description=f'GE_slovakia_Child_Add Foreign Supervisor V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
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
            no_task='search_users_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_users_3',
            end_task='add_foreign_super_logs_28',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def compose_user_details(response, loginname):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            users_info = list(filter(lambda x: x['loginname'] == loginname, map(lambda row: {
                'loginname': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))
            return users_info[0] if users_info else None

        search_users_3 = rail.RepliconServicePageOperator(
            task_id='search_users_3',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                "sort": [],
                "filterExpression": {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['supervisorloginname'],
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response, dag_run: compose_user_details(
                response, dag_run.conf['supervisorloginname'])
        )

        if_log_getsupervisor_uri_4_blank_5 = rail.IfOperator(
            task_id='if_log_getsupervisor_uri_4_blank_5',
            test='''{{ result('search_users_3') | is_falsy }}''',
            yes_task="log_first_name_6",
            no_task="add_foreign_super_logs_28",
        )

        log_first_name_6 = rail.PythonOperator(
            task_id='log_first_name_6',
            python_callable=lambda dag_run:  dag_run.conf['supervisorname'].split(" ")[
                0]
        )

        log_last_name_7 = rail.PythonOperator(
            task_id='log_last_name_7',
            python_callable=lambda dag_run:  dag_run.conf['supervisorname'].replace(
                rail.result('log_first_name_6'), "").strip()
        )

        log_email_8 = rail.PythonOperator(
            task_id='log_email_8',
            python_callable=lambda dag_run:  dag_run.conf['supervisorloginname'] +
            "@mail.ad.ge.com"
        )

        slovakia_master_mapper_search_entries_getallthevaluestocreatea_foreign_supervisors_9 = rail.PythonOperator(
            task_id='slovakia_master_mapper_search_entries_getallthevaluestocreatea_foreign_supervisors_9',
            python_callable=lambda: list(
                filter(lambda x: x['legal_entity'] == 'Foreign Supervisors', slovakia_master_mapper))
        )

        def get_entity_from_mapper(entity, typeval):
            emp_types = list(filter(
                lambda x: x['legal_entity'] == entity
                and x['type'] == typeval, slovakia_master_mapper))
            return emp_types[0]['value'] if emp_types else ''

        def get_entity_uri_from_mapper(entity, typeval):
            emp_types = list(filter(
                lambda x: x['legal_entity'] == entity
                and x['type'] == typeval, slovakia_master_mapper))
            return emp_types[0]['default_uri'] if emp_types else ''

        log_location_10 = rail.PythonOperator(
            task_id='log_location_10',
            python_callable=lambda: get_entity_from_mapper(
                'Foreign Supervisors', 'Location'),
        )

        log_authentication_type_11 = rail.PythonOperator(
            task_id='log_authentication_type_11',
            python_callable=lambda: get_entity_uri_from_mapper(
                'Foreign Supervisors', 'Authentication Type')
        )

        log_language_12 = rail.PythonOperator(
            task_id='log_language_12',
            python_callable=lambda: get_entity_uri_from_mapper(
                'Foreign Supervisors', 'Language')
        )

        log_employee_type_13 = rail.PythonOperator(
            task_id='log_employee_type_13',
            python_callable=lambda: get_entity_from_mapper(
                'Foreign Supervisors', 'Employee Type')
        )

        def get_mapper_licenses(entity, typeval):
            employee_licenses = list(filter(
                lambda x: x['legal_entity'] == entity
                and x['type'] == typeval, slovakia_master_mapper))
            licenses = [licenses['default_uri']
                        for licenses in employee_licenses]
            return rail.smartjoin_by_delim(licenses, ',')

        log_required_licenses_14 = rail.PythonOperator(
            task_id='log_required_licenses_14',
            python_callable=lambda: get_mapper_licenses(
                'Foreign Supervisors', 'License')
        )

        log_required_office_default_schedule_15 = rail.PythonOperator(
            task_id='log_required_office_default_schedule_15',
            python_callable=lambda: get_entity_from_mapper(
                'Foreign Supervisors', 'Default Schedule')
        )

        def get_mapper_permissions(entity, mapper_type, identifier_1):
            employee_permissions = list(filter(
                lambda x: x['legal_entity'] == entity
                and x['type'] == mapper_type
                and x['identifier_1_(_legal_entity_code/_type/_timeoff_type)'] == identifier_1, slovakia_master_mapper))
            permissions = [permission['value']
                           for permission in employee_permissions]
            return permissions

        def get_permission_set_to_assign():
            permission_set_to_assign = []
            permissions = get_mapper_permissions(
                'Foreign Supervisors', 'Permission', 'Supervsior')
            for permission in permissions:
                permission_set_to_assign.append({
                    "uri": None,
                    "name": permission
                })

            return permission_set_to_assign

        create_userforrequiredforeign_supervisor_21 = rail.RepliconServiceOperator(
            task_id='create_userforrequiredforeign_supervisor_21',
            endpoint="/services/importservice1.svc/PutUser3",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "uri": null,
                        "loginName": dag_run.conf['supervisorloginname'],
                        "parameterCorrelationId": null
                    },
                    "firstname": rail.result('log_first_name_6'),
                    "lastname": rail.result('log_last_name_7'),
                    "emailAddress": rail.result('log_email_8'),
                    "employeeId": dag_run.conf['supervisorloginname'],
                    "department": {
                        "uri": dag_run.conf['foreignsupervisordepartmenturi'],
                        "name": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "supervisorAssignmentSchedule": null,
                    "schedulePolicySchedule": [
                        {
                            "schedulePolicy": {
                                "officeScheduleUri": null,
                                "name": rail.result('log_required_office_default_schedule_15'),
                                "officeSchedule": {
                                    "officeScheduleUri": null,
                                    "name": rail.result('log_required_office_default_schedule_15')
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
                            rail.result('log_authentication_type_11')
                        ],
                        "isLoginEnabled": "true",
                        "loginName": dag_run.conf['supervisorloginname'],
                        "SSOName": dag_run.conf['supervisorloginname'],
                        "password": null
                    },
                    "holidayCalendar": null,
                    "timeOffPolicy": null,
                    "permissionSets": get_permission_set_to_assign(),
                    "policySets": [],
                    "employeeType": {
                        "uri": null,
                        "name": rail.result('log_employee_type_13')
                    },
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
                    "locationSchedule": [
                        {
                            "location": {
                                "uri": null,
                                "parentUri": null,
                                "name": rail.result('log_location_10')
                            },
                            "effectiveDate": null
                        }
                    ],
                    "divisionSchedule": [],
                    "costCenterSchedule": [],
                    "serviceCenterSchedule": [],
                    "departmentGroupSchedule": [],
                    "employeeTypeGroupSchedule": [],
                    "timesheetPeriodSchedule": [],
                    "policyDataAccessScopes": [],
                    "policyDataAccessScopes2": [],
                    "payRuleScriptSchedule": []
                }
            }
        )

        remove_timeoff_assignments_22 = rail.RepliconServiceOperator(
            task_id='remove_timeoff_assignments_22',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_userforrequiredforeign_supervisor_21').uri }}",
                "timeOffTypeUris": []
            }
        )

        remove_start_date_23 = rail.RepliconServiceOperator(
            task_id='remove_start_date_23',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ result('create_userforrequiredforeign_supervisor_21').uri }}",
                "dateRange": null
            }
        )

        put_product_assignments_for_user_24 = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user_24',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_userforrequiredforeign_supervisor_21')['uri'],
                "productUris": rail.result('log_required_licenses_14').split(',')
            }
        )

        update_language_25 = rail.RepliconServiceOperator(
            task_id='update_language_25',
            endpoint="/services/InternationalizationService1.svc/UpdateLanguageForUser",
            data={
                "userUri": "{{ result('create_userforrequiredforeign_supervisor_21').uri }}",
                "languageUri": "{{ result('log_language_12') }}"
            }
        )

        update_holiday_calendar_for_user_26 = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user_26',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": "{{ result('create_userforrequiredforeign_supervisor_21').uri }}",
                "holidayCalendarUri": null
            }
        )

        add_foreign_super_logs_28 = rail.WriteLogOperator(
            task_id='add_foreign_super_logs_28',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "action": "",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "child_job_id": "{{ dag_run_ecid() }}",
                "username": ""
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> add_foreign_super_logs_28
        can_run_batch_task >> rail.Label('No') >> search_users_3
        search_users_3 >> if_log_getsupervisor_uri_4_blank_5
        if_log_getsupervisor_uri_4_blank_5 >> rail.Label(
            'Yes') >> log_first_name_6 >> log_last_name_7 >> log_email_8 >> \
            slovakia_master_mapper_search_entries_getallthevaluestocreatea_foreign_supervisors_9 >> \
            log_location_10 >> log_authentication_type_11 >> log_language_12 >> log_employee_type_13 >> \
            log_required_licenses_14 >> log_required_office_default_schedule_15 >> \
            create_userforrequiredforeign_supervisor_21 >> \
            remove_timeoff_assignments_22 >> remove_start_date_23 >> put_product_assignments_for_user_24 >> \
            update_language_25 >> update_holiday_calendar_for_user_26 >> add_foreign_super_logs_28
        if_log_getsupervisor_uri_4_blank_5 >> rail.Label(
            'No') >> add_foreign_super_logs_28 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
