
from datetime import timedelta, datetime
import hashlib
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from velaw.user_import_v1.task.generate_report_batch import report_batch
from velaw.user_import_v1.user_import_mapper import velaw_user_import_mapper

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'VelawG3_User Import_V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            sftp_conn_id=config.sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=10)
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='if_name_downcase_not_ends_with_csv_3',
            no_task='delete_this_dagrun',
        )

        if_name_downcase_not_ends_with_csv_3 = rail.IfOperator(
            task_id='if_name_downcase_not_ends_with_csv_3',
            test='{{ result("new_file_sensor") | lower | file_ext | lower != "csv" }}',
            yes_task="send_mail_notificationforincorrectfileformat_4",
            no_task="log_formattedjobstarttime_2",
        )

        send_mail_notificationforincorrectfileformat_4 = rail.EmailOperator(
            task_id='send_mail_notificationforincorrectfileformat_4',
            to=config.tenant_email,
            bcc=config.internal_logs_email,  # config.alert_email on error fixme
            subject='''{{ get_company_key() }} | User import has been skipped - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The User Import is skipped, since the file - '{{ result('new_file_sensor') | file_name }}' is not in .csv file format. Please correct the file name and place a new file for processing.</p><p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        rename_archivetheinputfile_5 = rail.SFTPMoveFileOperator(
            task_id='rename_archivetheinputfile_5',
            existing_filename='''{{ result('new_file_sensor') }}''',
            new_filename=config.archive_filepath +
            '''/Skipped_{{ result('log_formattedjobstarttime_2') }}_{{ result('new_file_sensor') | file_name }}'''
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        log_formattedjobstarttime_2 = rail.PythonOperator(
            task_id='log_formattedjobstarttime_2',
            python_callable=lambda: datetime.now().strftime("%d%m%YT%H%M%S")
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='velaw_user_import_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='velaw_user_import_logs',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        velaw_user_import_logs = rail.CreateLogOperator(
            task_id='velaw_user_import_logs',
        )

        velaw_supervisor_check_logs = rail.CreateLogOperator(
            task_id='velaw_supervisor_check_logs',
        )

        download_9 = rail.SFTPDownloadFileOperator(
            task_id='download_9',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        rename_archivetheinputfile_17 = rail.SFTPMoveFileOperator(
            task_id='rename_archivetheinputfile_17',
            existing_filename='''{{ result('new_file_sensor') }}''',
            new_filename=config.archive_filepath +
            '''/{{ result('log_formattedjobstarttime_2') }}_{{ result('new_file_sensor') | file_name }}'''
        )

        load_csv_create_list_from_csv_raw_input_list_10 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_raw_input_list_10",
            document="{{ result('download_9') }}",
        )

        create_collection_create_list_from_csv_raw_input_list_10 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_raw_input_list_10',
            source="{{ result('load_csv_create_list_from_csv_raw_input_list_10') }}",
            name="rawinputfile",
            columns={
                'FIRST_NAME': 'firstname',
                'LAST_NAME': 'lastname',
                'EMAIL': 'email',
                'EMPLOYEE_ID': 'employeeid',
                'START_DATE': 'startdate',
                'END_DATE': 'enddate',
                'JOB_CODE': 'jobcode',
                'JOB_TITLE': 'jobtitle',
                'FLSA_STATUS': 'flsastatus',
                'ASSIGNMENT_CATEGORY': 'assignmentcategory',
                'COUNTRY_ISO_CODE': 'countryisocode',
                'PERSON_TYPE': 'persontype',
                'LEGAL_EMPLOYER': 'legalemployer',
                'LOGIN_NAME': 'loginname',
                'SUPERVISOR_LOGIN_NAME': 'supervisorloginname',
                'IS_LOGIN_ENABLED': 'isloginenabled',
                'DEPARTMENT_NAME': 'departmentname',
                'DEPARTMENT_CODE': 'departmentcode',
                'EMPLOYEE_TYPE': 'employeetype',
                'LOCATION': 'location',
                'JOB_FAMILIES': 'jobfamilies',
                'PAY_TYPE': 'paytype',
                'PAY_RATES_AMOUNT': 'payratesamount',
                'PAY_RATES_CURRENCY': 'payratescurrency',
                'DEFAULT_BILLING_RATE_AMOUNT': 'defaultbillingrateamount',
                'DEFAULT_BILLING_RATE_CURRENCY': 'defaultbillingratecurrency',
                'HOURLY_COST_AMOUNT': 'hourlycostamount',
                'HOURLY_COST_CURRENCY': 'hourlycostcurrency'
            }
        )

        query_list_all_gb_and_us_users_11 = rail.QueryCollectionOperator(
            task_id='query_list_all_gb_and_us_users_11',
            query="""SELECT * FROM rawinputfile WHERE countryisocode='US' OR countryisocode='GB'""",
        )

        query_list_all_non_gb_and_us_users_12 = rail.QueryCollectionOperator(
            task_id='query_list_all_non_gb_and_us_users_12',
            query="""SELECT * FROM rawinputfile WHERE countryisocode!='US' AND countryisocode!='GB'""",
            name="nonusgbusers"
        )

        parse_csv_parse_input_file_14 = rail.EmptyOperator(
            task_id='parse_csv_parse_input_file_14',
        )

        if_parse_csv_parse_input_file_14_lines_less_than_1_15 = rail.IfOperator(
            task_id='if_parse_csv_parse_input_file_14_lines_less_than_1_15',
            test='''{{ result('query_list_all_gb_and_us_users_11', 'length') < 1 }}''',
            yes_task="send_mail_notificationfornorecords_blank_data_16",
            no_task="get_tenant_and_useridentity_details_19",
        )

        send_mail_notificationfornorecords_blank_data_16 = rail.EmailOperator(
            task_id='send_mail_notificationfornorecords_blank_data_16',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | User import has been skipped - {{ current_time() }}''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The User Import is skipped, since the file - '{{ result('new_file_sensor') | file_name }}' doesn't contain any row(data). Please correct the feed file and place a new file for processing.</p><p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> '''
        )

        get_tenant_and_useridentity_details_19 = rail.RepliconServiceOperator(
            task_id='get_tenant_and_useridentity_details_19',
            endpoint='/services/UserAccessControlService1.svc/GetMyActualUserIdentity'
        )

        trigger_dag_run_velaw_user_import_velawg3_child_groups_update_v2_020 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_groups_update_v2_020',
            retries=0,
            trigger_dag_id=config.groups_update_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "filepath": rail.result('download_9')
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_groups_update_v2_020 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_groups_update_v2_020',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_groups_update_v2_020") }}'
        )

        def get_csv_rows(item):
            def get_encoded():

                return hashlib.md5(
                    (str(item['firstname']) + ","
                     + str(item['lastname']) + ","
                     + str(item['email']) + ","
                     + str(item['employeeid']) + ","
                     + str(item['startdate']) + ","
                     + str(item['enddate']) + ","
                     + str(item['jobcode']) + ","
                     + str(item['jobtitle']) + ","
                     + str(item['flsastatus']) + ","
                     + str(item['assignmentcategory']) + ","
                     + str(item['countryisocode']) + ","
                     + str(item['persontype']) + ","
                     + str(item['legalemployer']) + ","
                     + str(item['loginname']) + ","
                     + str(item['supervisorloginname']) + ","
                     + str(item['isloginenabled']) + ","
                     + str(item['departmentname']) + ","
                     + str(item['departmentcode']) + ","
                     + str(item['employeetype']) + ","
                     + str(item['location']) + ","
                     + str(item['jobfamilies']) + ","
                     + str(item['paytype']) + ","
                     + str(item['payratesamount']) + ","
                     + str(item['payratescurrency']) + ","
                     + str(item['defaultbillingrateamount']) + ","
                     + str(item['defaultbillingratecurrency']) + ","
                     + str(item['hourlycostamount']) + ","
                     + str(item['hourlycostcurrency'])
                     ).encode('utf-8')).hexdigest()

            row_data = [
                item['firstname'].strip() if item['firstname'] else null,
                item['lastname'].strip() if item['lastname'] else null,
                item['email'].strip() if item['email'] else null,
                item['employeeid'].strip() if item['employeeid'] else null,
                item['startdate'].strip() if item['startdate'] else null,
                item['enddate'].strip() if item['enddate'] else null,
                item['jobcode'].strip() if item['jobcode'] else null,
                item['jobtitle'].strip() if item['jobtitle'] else null,
                item['flsastatus'].strip() if item['flsastatus'] else null,
                item['assignmentcategory'].strip(
                ) if item['assignmentcategory'] else null,
                item['countryisocode'].strip(
                ) if item['countryisocode'] else null,
                item['persontype'].strip() if item['persontype'] else null,
                item['legalemployer'].strip() if item['legalemployer'] else null,
                item['loginname'].strip() if item['loginname'] else null,
                item['supervisorloginname'].strip(
                ) if item['supervisorloginname'] else null,
                item['isloginenabled'].strip(
                ) if item['isloginenabled'] else null,
                item['departmentname'].strip(
                ) if item['departmentname'] else null,
                item['departmentcode'].strip(
                ) if item['departmentcode'] else null,
                item['employeetype'].strip() if item['employeetype'] else null,
                item['location'].strip() if item['location'] else null,
                item['jobfamilies'].strip() if item['jobfamilies'] else null,
                item['paytype'].strip() if item['paytype'] else null,
                item['payratesamount'].strip(
                ) if item['payratesamount'] else null,
                item['payratescurrency'].strip(
                ) if item['payratescurrency'] else null,
                item['defaultbillingrateamount'].strip(
                ) if item['defaultbillingrateamount'] else null,
                item['defaultbillingratecurrency'].strip(
                ) if item['defaultbillingratecurrency'] else null,
                item['hourlycostamount'].strip(
                ) if item['hourlycostamount'] else null,
                item['hourlycostcurrency'].strip(
                ) if item['hourlycostcurrency'] else null,
                get_encoded()
            ]
            return row_data

        create_csv_lines_stripthedata_25 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_stripthedata_25',
            source="{{ result('query_list_all_gb_and_us_users_11') }}",
            header=['firstname',
                    'lastname',
                    'email',
                    'employeeid',
                    'startdate',
                    'enddate',
                    'jobcode',
                    'jobtitle',
                    'flsastatus',
                    'assignmentcategory',
                    'countryisocode',
                    'persontype',
                    'legalemployer',
                    'loginname',
                    'supervisorloginname',
                    'isloginenabled',
                    'departmentname',
                    'departmentcode',
                    'employeetype',
                    'location',
                    'jobfamilies',
                    'paytype',
                    'payratesamount',
                    'payratescurrency',
                    'defaultbillingrateamount',
                    'defaultbillingratecurrency',
                    'hourlycostamount',
                    'hourlycostcurrency',
                    'encoded'],
            row=get_csv_rows
        )

        load_csv_create_list_from_csv_raw_input_list_26 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_raw_input_list_26",
            document="{{ result('create_csv_lines_stripthedata_25') }}"
        )

        create_collection_create_list_from_csv_raw_input_list_26 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_raw_input_list_26',
            source="{{ result('load_csv_create_list_from_csv_raw_input_list_26') }}",
            name="inputfile"
        )

        query_list_getuserwithblank_loginname_27 = rail.QueryCollectionOperator(
            task_id='query_list_getuserwithblank_loginname_27',
            query="""SELECT * FROM inputfile WHERE NULLIF(loginname, '') IS NULL"""
        )

        if_query_list_getuserwithblank_loginname_27_rows_greater_than_0_28 = rail.IfOperator(
            task_id='if_query_list_getuserwithblank_loginname_27_rows_greater_than_0_28',
            test='''{{ result('query_list_getuserwithblank_loginname_27', 'length') > 0 }}''',
            yes_task="create_csv_lines_validation_files_29",
            no_task="query_list_getuserwith_loginname_32"
        )

        create_csv_lines_validation_files_29 = rail.WriteLogOperator(
            task_id='create_csv_lines_validation_files_29',
            log="{{ result('velaw_user_import_logs') }}",
            items="{{ result('query_list_getuserwithblank_loginname_27') }}",
            message="Login name is blank in feed file",
            severity="Info",
            properties={
                "username": "{{ item.firstname }} {{ item.lastname }}",
                "loginname": "{{ item.loginname }}",
                "employeeid": "{{ item.employeeid }}",
                "importaction": "validation",
                "status": "Skipped",
                "details": "Login name is blank in feed file"
            }
        )

        query_list_getuserwith_loginname_32 = rail.QueryCollectionOperator(
            task_id='query_list_getuserwith_loginname_32',
            query="""SELECT * FROM inputfile WHERE NULLIF(loginname, '') IS NOT NULL""",
            name="validatedinputlist"
        )

        generate_report_35 = rail.EmptyOperator(
            task_id='generate_report_35'
        )

        get_report_details, create_collection_from_report_data, fail_no_report_data = report_batch(
            config)

        get_all_payrule_scripts_38 = rail.RepliconServiceOperator(
            task_id='get_all_payrule_scripts_38',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts"
        )

        get_enabled_activities_39 = rail.RepliconServiceOperator(
            task_id='get_enabled_activities_39',
            endpoint="/services/ActivityService1.svc/GetEnabledActivities"
        )

        get_all_currencies_40 = rail.RepliconServiceOperator(
            task_id='get_all_currencies_40',
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies"
        )

        get_base_currency_41 = rail.RepliconServiceOperator(
            task_id='get_base_currency_41',
            endpoint="/services/CurrencyService2.svc/GetBaseCurrency"
        )

        get_all_office_schedules_42 = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules_42',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        get_all_holiday_calendars_43 = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars_43',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars"
        )

        def timesheet_period_list_input(response):
            rows = response.json()['d']['rows']
            return list(map(lambda row: {
                "name": row['cells'][0].get('textValue'),
                "enabled": row['cells'][1].get('textValue'),
                "uri": row['cells'][0].get('uri')
            }, rows)) if rows else []

        get_all_enabled_timesheet_period_service_44 = rail.RepliconServiceOperator(
            task_id='get_all_enabled_timesheet_period_service_44',
            endpoint="/services/TimesheetPeriodListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000000",
                "columnUris": [
                    "urn:replicon:timesheet-period-list-column:timesheet-period",
                    "urn:replicon:timesheet-period-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:timesheet-period-list-filter:enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": "true",
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            response_filter=timesheet_period_list_input
        )

        get_all_time_off_approval_paths_45 = rail.RepliconServiceOperator(
            task_id='get_all_time_off_approval_paths_45',
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths"
        )

        get_all_timesheet_approval_paths_46 = rail.RepliconServiceOperator(
            task_id='get_all_timesheet_approval_paths_46',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths"
        )

        get_all_enabled_location_groups_47 = rail.RepliconServiceOperator(
            task_id='get_all_enabled_location_groups_47',
            endpoint="/services/LocationService1.svc/GetAllLocations"
        )

        get_all_enabled_employee_type_groups_48 = rail.RepliconServiceOperator(
            task_id='get_all_enabled_employee_type_groups_48',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups"
        )

        get_all_enabled_division_groups_49 = rail.RepliconServiceOperator(
            task_id='get_all_enabled_division_groups_49',
            endpoint="/services/DivisionService1.svc/GetAllDivisions"
        )

        get_all_enabled_cost_centers_groups_50 = rail.RepliconServiceOperator(
            task_id='get_all_enabled_cost_centers_groups_50',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters"
        )

        def location_list_input(res):
            rows = res.json()['d']['rows']

            def get_full_path(cell):
                values = []
                for collection in cell['cellCollection']:
                    values.append(collection['textValue'])
                return "|".join(values)

            return list(map(lambda row: {
                'name': row['cells'][0].get('textValue'),
                'uri': row['cells'][0].get('uri'),
                'fullpath': get_full_path(row['cells'][1]),
            }, rows)) if rows else []

        get_all_enabled_department_groupswith_full_path_51 = rail.RepliconServiceOperator(
            task_id='get_all_enabled_department_groupswith_full_path_51',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=location_list_input
        )

        get_all_policy_sets_templates_52 = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets_templates_52',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets"
        )

        get_all_permission_sets_53 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_53',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        get_all_custom_fields_54 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_54',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                'jobcode': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Job Code', 'uri', ''),
                "jobtitle": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Job Title', 'uri', ''),
                'flsastatus': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'FLSA Status', 'uri', ''),
                'assignmentcategory': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Assignment Category', 'uri', ''),
                'countryisocode': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Country ISO Code', 'uri', ''),
                'persontype': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Person Type', 'uri', ''),
                'legalemployer': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Legal Employer', 'uri', '')
            }
        )

        trigger_dag_run_velaw_user_import_velawg3_drop_down_udf_custom_field_check_v2_056 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_drop_down_udf_custom_field_check_v2_056',
            retries=0,
            items=[0],
            trigger_dag_id=config.drop_down_udf_custom_field_check_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "filepath": rail.result('download_9'),
                "jobcodeudfuri": rail.result('get_all_custom_fields_54')['jobcode'],
                "jobtitleudfuri": rail.result('get_all_custom_fields_54')['jobtitle'],
                "flsastatusudfuri": rail.result('get_all_custom_fields_54')['flsastatus'],
                "assignmentcategoryudfuri": rail.result('get_all_custom_fields_54')['assignmentcategory'],
                "countryisocodeudfuri": rail.result('get_all_custom_fields_54')['countryisocode'],
                "persontypeudfuri": rail.result('get_all_custom_fields_54')['persontype'],
                "legalemployerudfuri": rail.result('get_all_custom_fields_54')['legalemployer']
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_drop_down_udf_custom_field_check_v2_056 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_drop_down_udf_custom_field_check_v2_056',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_drop_down_udf_custom_field_check_v2_056") }}'
        )

        get_all_custom_fieldsdropdownvaluesfor_job_code_57 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fieldsdropdownvaluesfor_job_code_57',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_all_custom_fields_54').jobcode }}"
            }
        )

        get_all_custom_fieldsdropdownvaluesfor_job_title_58 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fieldsdropdownvaluesfor_job_title_58',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_all_custom_fields_54').jobtitle }}"
            }
        )

        get_all_custom_fieldsdropdownvaluesfor_f_l_s_a_status_59 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fieldsdropdownvaluesfor_f_l_s_a_status_59',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_all_custom_fields_54').flsastatus }}"
            }
        )

        get_all_custom_fieldsdropdownvaluesfor_assignment_category_60 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fieldsdropdownvaluesfor_assignment_category_60',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_all_custom_fields_54').assignmentcategory }}"
            }
        )

        get_all_custom_fieldsdropdownvaluesfor_country_i_s_o_code_61 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fieldsdropdownvaluesfor_country_i_s_o_code_61',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_all_custom_fields_54').countryisocode }}"
            }
        )

        get_all_custom_fieldsdropdownvaluesfor_person_type_62 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fieldsdropdownvaluesfor_person_type_62',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_all_custom_fields_54').persontype }}"
            }
        )

        get_all_custom_fieldsdropdownvaluesfor_legal_employer_63 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fieldsdropdownvaluesfor_legal_employer_63',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_all_custom_fields_54').legalemployer }}"
            }
        )

        get_all_scripts_time_off_validation_script_64 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_off_validation_script_64',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts"
        )

        get_all_scripts_time_off_balance_event_script_65 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_off_balance_event_script_65',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts"
        )

        velaw_user_import_mapper_search_entries_68 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_68',
            python_callable=lambda: list(
                filter(lambda x: x['mapper'] == 'Yes', velaw_user_import_mapper))
        )

        query_list_all_usersfrom_replicon_72 = rail.QueryCollectionOperator(
            task_id='query_list_all_usersfrom_replicon_72',
            query="""SELECT * FROM userlistfromreplicon"""
        )

        query_list_enabled_usersfrom_replicon_73 = rail.QueryCollectionOperator(
            task_id='query_list_enabled_usersfrom_replicon_73',
            query="""SELECT * FROM userlistfromreplicon WHERE enabled='Enabled'""",
            name="enabledusers"
        )

        query_list_disabled_usersfrom_replicon_75 = rail.QueryCollectionOperator(
            task_id='query_list_disabled_usersfrom_replicon_75',
            query="""SELECT * FROM userlistfromreplicon WHERE enabled='Disabled'""",
            name="disabledusers"
        )

        query_list_validated_userstodisablewhoarealreadydisabled_77 = rail.QueryCollectionOperator(
            task_id='query_list_validated_userstodisablewhoarealreadydisabled_77',
            query="""SELECT * FROM validatedinputlist WHERE loginname IN (SELECT DISTINCT loginname FROM disabledusers) AND isloginenabled='Inactive'"""
        )

        if_query_list_validated_userstodisablewhoarealreadydisabled_77_rows_greater_than_0_78 = rail.IfOperator(
            task_id='if_query_list_validated_userstodisablewhoarealreadydisabled_77_rows_greater_than_0_78',
            test='''{{ result('query_list_validated_userstodisablewhoarealreadydisabled_77', 'length') > 0 }}''',
            yes_task="create_csv_lines_disabled_skip_files_79",
            no_task="query_list_validated_userstodisablewithout_end_date_82"
        )

        create_csv_lines_disabled_skip_files_79 = rail.WriteLogOperator(
            task_id='create_csv_lines_disabled_skip_files_79',
            log="{{ result('velaw_user_import_logs') }}",
            items="{{ result('query_list_validated_userstodisablewhoarealreadydisabled_77') }}",
            message="Required user already disabled in Replicon",
            severity="Info",
            properties={
                "username": "{{ item.firstname }} {{ item.lastname }}",
                "loginname": "{{ item.loginname }}",
                "employeeid": "{{ item.employeeid }}",
                "importaction": "disable",
                "status": "Skipped",
                "details": "Required user already disabled in Replicon"
            }
        )

        query_list_validated_userstodisablewithout_end_date_82 = rail.QueryCollectionOperator(
            task_id='query_list_validated_userstodisablewithout_end_date_82',
            query="""SELECT * FROM validatedinputlist WHERE loginname IN (SELECT DISTINCT loginname FROM enabledusers) AND isloginenabled='Inactive' AND NULLIF(enddate, '') IS NULL""",
        )

        if_query_list_validated_userstodisablewithout_end_date_82_rows_greater_than_0_83 = rail.IfOperator(
            task_id='if_query_list_validated_userstodisablewithout_end_date_82_rows_greater_than_0_83',
            test='''{{ result('query_list_validated_userstodisablewithout_end_date_82', 'length') > 0 }}''',
            yes_task="create_csv_lines_disabled_skip_files_84",
            no_task="query_list_validated_userstodisablewith_enddate_87"
        )

        create_csv_lines_disabled_skip_files_84 = rail.WriteLogOperator(
            task_id='create_csv_lines_disabled_skip_files_84',
            log="{{ result('velaw_user_import_logs') }}",
            items="{{ result('query_list_validated_userstodisablewithout_end_date_82') }}",
            message="End date is not available in feed file",
            severity="Info",
            properties={
                "username": "{{ item.firstname }} {{ item.lastname }}",
                "loginname": "{{ item.loginname }}",
                "employeeid": "{{ item.employeeid }}",
                "importaction": "disable",
                "status": "Skipped",
                "details": "End date is not available in feed file"
            }
        )

        query_list_validated_userstodisablewith_enddate_87 = rail.QueryCollectionOperator(
            task_id='query_list_validated_userstodisablewith_enddate_87',
            query="""SELECT * FROM validatedinputlist WHERE loginname IN (SELECT DISTINCT loginname FROM enabledusers) AND isloginenabled='Inactive' AND NULLIF(enddate, '') IS NOT NULL"""
        )

        trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_90 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_90',
            retries=0,
            items="{{ result('query_list_validated_userstodisablewith_enddate_87') }}",
            trigger_dag_id=config.workflow_to_disable_user_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "useruri": next((x['useruri'] for x in rail.load_all_records(rail.result('query_list_enabled_usersfrom_replicon_73')) if item['loginname'] == x['loginname']), null),
                "username": item['firstname'] + ' ' + item['lastname'],
                "parentjobid": get_dagrun_ecid(dag_run),
                "userloginname": item['loginname'],
                "startdate": item['startdate'],
                "emplid": item['employeeid'],
                "enddate": item['enddate'],
                "startingbalancesettouri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts_time_off_balance_event_script_65'), 'displayText', "Starting Balance Set To", 'uri'),
                "preventbalanceoverdrawuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts_time_off_validation_script_64'), 'displayText', "Prevent balance overdraw", 'uri'),
                "actualuserlogin": rail.result('get_tenant_and_useridentity_details_19')['loginName']
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_90 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_90',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_90") }}'
        )

        gather_user_disable_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_disable_logs',
            dag_runs="{{ result('trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_90') }}",
            dagrun_task_id='velaw_user_disable_logs',
            flatten=True
        )

        query_list_validated_userstodisablewith_different_i_s_o_countrycode_95 = rail.QueryCollectionOperator(
            task_id='query_list_validated_userstodisablewith_different_i_s_o_countrycode_95',
            query="""SELECT * FROM enabledusers WHERE loginname IN (SELECT DISTINCT loginname FROM nonusgbusers)"""
        )

        trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_98 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_98',
            retries=0,
            items="{{ result('query_list_validated_userstodisablewith_different_i_s_o_countrycode_95') }}",
            trigger_dag_id=config.workflow_to_disable_user_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "useruri": item['useruri'],
                "username": item['username'],
                "parentjobid": "{{ dag_run_ecid() }}",
                "userloginname": item['loginname'],
                "startdate": item['startdate'],
                "emplid": "NA",
                "enddate": datetime.now().strftime("%m/%d/%Y"),
                "preventbalanceoverdrawuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts_time_off_validation_script_64'), 'displayText', "Prevent balance overdraw", 'uri'),
                "startingbalancesettouri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts_time_off_balance_event_script_65'), 'displayText', "Starting Balance Set To", 'uri'),
                "actualuserlogin": rail.result('get_tenant_and_useridentity_details_19')['loginName']
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_98 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_98',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_98") }}'
        )

        gather_user_disable_different_iso_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_disable_different_iso_logs',
            dag_runs="{{ result('trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_98') }}",
            dagrun_task_id='velaw_user_disable_logs',
            flatten=True
        )

        query_list_validated_newuserstoprocess_with_enabledstatusas_falseor_first_nameis_blankand_lastnameisblank_108 = rail.QueryCollectionOperator(
            task_id='query_list_validated_newuserstoprocess_with_enabledstatusas_falseor_first_nameis_blankand_lastnameisblank_108',
            query="""SELECT * FROM validatedinputlist WHERE loginname NOT IN (SELECT DISTINCT loginname FROM enabledusers) AND (isloginenabled='Inactive' OR NULLIF(firstname,'') IS NULL OR NULLIF(lastname,'') IS NULL OR NULLIF(startdate,'') IS NULL)""",
        )

        if_query_list_validated_newuserstoprocess_with_enabledstatusas_falseor_first_nameis_blankand_lastnameisblank_108_rows_greater_than_0_109 = rail.IfOperator(
            task_id='if_query_list_validated_newuserstoprocess_with_enabledstatusas_falseor_first_nameis_blankand_lastnameisblank_108_rows_greater_than_0_109',
            test='''{{ result('query_list_validated_newuserstoprocess_with_enabledstatusas_falseor_first_nameis_blankand_lastnameisblank_108', 'length') > 0 }}''',
            yes_task="create_csv_lines_validation_filesfornewusers_110",
            no_task="query_list_validated_newuserstoprocess_with_enabledstatusas_true_113",
        )

        def get_detail_message(item):
            message = []
            if item['isloginenabled'] == "Inactive":
                message.append(
                    'IS_LOGIN_ENABLED column is set to \'Inactive\' for new user": "')
            if not item['firstname']:
                message.append('First name is blank for new user')
            if not item['lastname']:
                message.append('Last name is blank for new user')
            if not item['startdate']:
                message.append('Start date is blank for new user')
            return ",".join(message)

        create_csv_lines_validation_filesfornewusers_110 = rail.WriteLogOperator(
            task_id='create_csv_lines_validation_filesfornewusers_110',
            log="{{ result('velaw_user_import_logs') }}",
            items="{{ result('query_list_validated_newuserstoprocess_with_enabledstatusas_falseor_first_nameis_blankand_lastnameisblank_108')  }}",
            message="na",
            severity="Info",
            properties=lambda item: {
                "username": item['firstname'] + ' ' + item['lastname'],
                "loginname": item['loginname'],
                "employeeid": item['employeeid'],
                "importaction": "add",
                "status": "Skipped",
                "details": get_detail_message(item)
            }
        )

        query_list_validated_newuserstoprocess_with_enabledstatusas_true_113 = rail.QueryCollectionOperator(
            task_id='query_list_validated_newuserstoprocess_with_enabledstatusas_true_113',
            query="""SELECT * FROM validatedinputlist WHERE loginname NOT IN (SELECT DISTINCT loginname FROM enabledusers) AND isloginenabled='Active' AND NULLIF(firstname,'') IS NOT NULL AND NULLIF(lastname,'') IS NOT NULL AND NULLIF(startdate,'') IS NOT NULL"""
        )

        trigger_dag_run_velaw_user_import_velawg3_child_add_user_v2_0async_115 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_add_user_v2_0async_115',
            retries=0,
            items="{{ result('query_list_validated_newuserstoprocess_with_enabledstatusas_true_113') }}",
            trigger_dag_id=config.add_user_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "loginname": item['loginname'],
                "lastname": item['lastname'],
                "firstname": item['firstname'],
                "department": "Vinson & Elkins|" + item['legalemployer'] + "|" + item['departmentname'],
                "employeetype": item['employeetype'],
                "location": item['location'],
                "employeeid": item['employeeid'],
                "startdate": item['startdate'],
                "enddate": item['enddate'],
                "timesheettemplate": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Template" and x['employee_type'] == item['employeetype'], rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "timesheetapprovalpath": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Approval Path" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "timezone": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeZone" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "timeofftemplate": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeOff Template", rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "timeoffapprovalpath": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeOff Approval Path" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'] and x['person_type'] == item['persontype'], rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "departmenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_department_groupswith_full_path_51'), 'fullpath', str("Vinson & Elkins|" + item['legalemployer'] + "|" + item['departmentname']), 'uri'),
                "locationuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_location_groups_47'), 'displayText', item['location'], 'uri') if item['location'] else null,
                "supervisoruri": next((x['useruri'] for x in rail.load_all_records(rail.result('query_list_all_usersfrom_replicon_72')) if item['supervisorloginname'] == x['loginname']), null),
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets_53'), 'displayText', '*Gen3 - Supervisor', 'uri'),
                "timesheettemplateuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets_templates_52'), 'name',
                                                                             next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Template" and x['employee_type'] == item['employeetype'], rail.result('velaw_user_import_mapper_search_entries_68'))))), 'uri')
                if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Template" and x['employee_type'] == item['employeetype'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "timesheetperioduri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_timesheet_period_service_44'), 'name', next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Period", rail.result('velaw_user_import_mapper_search_entries_68')))), null), 'uri'),
                "timesheetapprovalpathuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_timesheet_approval_paths_46'), 'displayText', next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Approval Path" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68'))))), 'uri') if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Approval Path" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "timezoneuri": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeZone" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68'))))).rsplit("|", 1)[-1] if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeZone" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_employee_type_groups_48'), 'displayText', item['employeetype'], 'uri') if item['employeetype'] else null,
                "workweekuri": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Work Week", rail.result('velaw_user_import_mapper_search_entries_68'))))).rsplit(" |", 1)[-1].strip() if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Work Week", rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "officescheduleuri":  rail.find_first_by_attr_and_get_attr(rail.result('get_all_office_schedules_42'), 'displayText',
                                                                           next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Schedule" and x['country_code'] == item['countryisocode'] and x['flsa'] == item['flsastatus'], rail.result('velaw_user_import_mapper_search_entries_68'))))), 'uri')
                if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Schedule" and x['country_code'] == item['countryisocode'] and x['flsa'] == item['flsastatus'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "timeofftemplateuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets_templates_52'), 'name',
                                                                           next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeOff Template", rail.result('velaw_user_import_mapper_search_entries_68'))))), 'uri')
                if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeOff Template", rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "timeoffapprovalpathuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_approval_paths_45'), 'displayText',
                                                                               next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeOff Approval Path" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'] and x['person_type'] == item['persontype'], rail.result('velaw_user_import_mapper_search_entries_68'))))), 'uri')
                if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeOff Approval Path" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'] and x['person_type'] == item['persontype'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "payruleuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_payrule_scripts_38'), 'displayText', next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Payrule" and x['employee_type'] == item['employeetype'], rail.result('velaw_user_import_mapper_search_entries_68'))))), 'uri')
                if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Payrule" and x['employee_type'] == item['employeetype'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else
                rail.find_first_by_attr_and_get_attr(rail.result('get_all_payrule_scripts_38'), 'displayText',
                                                     next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Payrule" and x['employee_type'] == 'Others', rail.result('velaw_user_import_mapper_search_entries_68'))))), 'uri'),
                "email": item['email'],
                "jobcode": item['jobcode'],
                "jobtitle": item['jobtitle'],
                "flsastatus": item['flsastatus'],
                "assignmentcategory": item['assignmentcategory'],
                "countryisocode": item['countryisocode'],
                "persontype": item['persontype'],
                "legalemployer": item['legalemployer'],
                "supervisorloginname": item['supervisorloginname'],
                "isloginenabled": item['isloginenabled'],
                "departmentname": item['departmentname'],
                "departmentcode": item['departmentcode'],
                "jobfamilies": item['jobfamilies'],
                "paytype": item['paytype'],
                "payratesamount": item['payratesamount'] if item['payratesamount'] else 0,
                "defaultbillingrateamount": item['defaultbillingrateamount'] if item['defaultbillingrateamount'] else 0,
                "hourlycostamount": item['hourlycostamount'] if item['hourlycostamount'] else 0,
                "jobcodeudfuri": rail.result('get_all_custom_fields_54')['jobcode'],
                "jobtitleudfuri": rail.result('get_all_custom_fields_54')['jobtitle'],
                "flsastatusudfuri": rail.result('get_all_custom_fields_54')['flsastatus'],
                "assignmentcategoryudfuri": rail.result('get_all_custom_fields_54')['assignmentcategory'],
                "countryisocodeudfuri": rail.result('get_all_custom_fields_54')['countryisocode'],
                "persontypeudfuri": rail.result('get_all_custom_fields_54')['persontype'],
                "legalemployerudfvalue": rail.result('get_all_custom_fields_54')['legalemployer'],
                "supervisorstatus": next((x['enabled'] for x in rail.load_all_records(rail.result('query_list_all_usersfrom_replicon_72')) if item['supervisorloginname'] == x['loginname']), null),
                "payratescurrency": item['payratescurrency'],
                "defaultbillingratecurrency": item['defaultbillingratecurrency'],
                "hourlycostcurrency": item['hourlycostcurrency'],
                "jobcodeudfvalueuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fieldsdropdownvaluesfor_job_code_57'), 'displayText', item['jobcode'], 'uri') if item['jobcode'] else null,
                "jobtitleudfvalueuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fieldsdropdownvaluesfor_job_title_58'), 'displayText', item['jobtitle'], 'uri') if item['jobtitle'] else null,
                "flsastatusudfvalueuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fieldsdropdownvaluesfor_f_l_s_a_status_59'), 'displayText', item['flsastatus'], 'uri') if item['flsastatus'] else null,
                "assignmentcategoryudfvalueuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fieldsdropdownvaluesfor_assignment_category_60'), 'displayText', item['assignmentcategory'], 'uri') if item['assignmentcategory'] else null,
                "countryisocodeudfvalueuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fieldsdropdownvaluesfor_country_i_s_o_code_61'), 'displayText', item['countryisocode'], 'uri') if item['countryisocode'] else null,
                "persontypeudfvalueuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fieldsdropdownvaluesfor_person_type_62'), 'displayText', item['persontype'], 'uri') if item['persontype'] else null,
                "legalemployerudfvalueuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fieldsdropdownvaluesfor_legal_employer_63'), 'displayText', item['legalemployer'], 'uri') if item['legalemployer'] else null,
                "jobfamiliesuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_cost_centers_groups_50'), 'displayText', item['jobfamilies'], 'uri') if item['jobfamilies'] else null,
                "payratescurrencyuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_currencies_40'), 'symbol', item['payratescurrency'], 'uri') if item['payratescurrency'] else rail.result('get_base_currency_41')['uri'],
                "defaultbillingratecurrencyuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_currencies_40'), 'symbol', item['defaultbillingratecurrency'], 'uri') if item['defaultbillingratecurrency'] else rail.result('get_base_currency_41')['uri'],
                "hourlycostcurrencyuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_currencies_40'), 'symbol', item['hourlycostcurrency'], 'uri') if item['hourlycostcurrency'] else rail.result('get_base_currency_41')['uri'],
                "paytypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_division_groups_49'), 'displayText', item['paytype'], 'uri') if item['paytype'] else null,
                "timesheetperiod": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Period", rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "activitylist": [x['uri'] for x in rail.result('get_enabled_activities_39') if x['name']],
                "holicaycalendar": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Holiday Calendar" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "holicaycalendaruri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_holiday_calendars_43'), 'displayText',
                                                                           next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Holiday Calendar" and x['country_code'] ==
                                                                                                                                    item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68'))))), 'uri')
                if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Holiday Calendar" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "officeschedule": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Schedule" and x['country_code'] == item['countryisocode'] and x['flsa'] == item['flsastatus'], rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "payrule": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Payrule" and x['employee_type'] == "Others", rail.result('velaw_user_import_mapper_search_entries_68')))))
                if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeZone" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else
                next(iter(map(lambda x: x['value_|_default_uri'], filter(
                    lambda x: x['type'] == "Payrule" and x['employee_type'] == "employeetype", rail.result('velaw_user_import_mapper_search_entries_68')))))
                if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeZone" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "enduserpermissionseturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets_53'), 'displayText', "*Gen3 - Project Resource with reports", 'uri'),
                "supervisorendusepermissionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets_53'), 'displayText', "*Gen3 - Project Resource with reports & Substitute User", 'uri')
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_add_user_v2_0async_115 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_add_user_v2_0async_115',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_add_user_v2_0async_115") }}'
        )

        gather_supervisor_check_add_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_supervisor_check_add_logs',
            dag_runs="{{ result('trigger_dag_run_velaw_user_import_velawg3_child_add_user_v2_0async_115') }}",
            dagrun_task_id='velaw_supervisor_check_user_add_logs',
            flatten=True
        )

        gather_user_add_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_add_logs',
            dag_runs="{{ result('trigger_dag_run_velaw_user_import_velawg3_child_add_user_v2_0async_115') }}",
            dagrun_task_id='velaw_add_user_import_logs',
            flatten=True
        )

        query_list_updateuserstoprocess_123 = rail.QueryCollectionOperator(
            task_id='query_list_updateuserstoprocess_123',
            query="""SELECT * FROM validatedinputlist WHERE loginname IN (SELECT DISTINCT loginname FROM userlistfromreplicon) AND isloginenabled='Active'""",
            name="updateuserslist"
        )

        dir_getthereferencefiledetails_125 = rail.SFTPListFilesOperator(
            task_id='dir_getthereferencefiledetails_125',
            paths=[config.reference_filepath]
        )

        def has_any_file(result_task_id, input_file_path):
            if not result_task_id or not input_file_path:
                raise Exception(
                    "Task_id" if not result_task_id else "input path" + "is not provided")
            data = rail.result(result_task_id)
            return False if not data else len(data[input_file_path]) > 0

        if_create_list_updateuserstoprocess_124_row_count_greater_than_0_126 = rail.IfOperator(
            task_id='if_create_list_updateuserstoprocess_124_row_count_greater_than_0_126',
            test=lambda: has_any_file(
                "dir_getthereferencefiledetails_125", config.reference_filepath),
            yes_task="reference_file",
            no_task="velaw_supervisor_check_search_entries_144"
        )

        def get_reference_file(result_task_id, input_file_path):
            if not result_task_id or not input_file_path:
                raise Exception(
                    "Task_id" if not result_task_id else "input path" + "is not provided")
            data = rail.result(result_task_id)
            return data[input_file_path][0]['name'] if (data and 'Reference' in data[input_file_path][0]['name']) else null

        reference_file = rail.PythonOperator(
            task_id="reference_file",
            python_callable=get_reference_file,
            op_args=["dir_getthereferencefiledetails_125",
                     config.reference_filepath]
        )

        if_parameters_usereferencefile_contains_yes_127 = rail.IfOperator(
            task_id='if_parameters_usereferencefile_contains_yes_127',
            test=lambda: rail.result('reference_file'),
            yes_task="download_downloadthereferencefile_128",
            no_task="velaw_supervisor_check_search_entries_144"
        )

        download_downloadthereferencefile_128 = rail.SFTPDownloadFileOperator(
            task_id='download_downloadthereferencefile_128',
            remote_filepath=config.reference_filepath +
            '/' + "{{ result('reference_file') }}"
        )

        load_csv_create_list_from_csv_129 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_129",
            document="{{ result('download_downloadthereferencefile_128') }}",
        )

        create_collection_create_list_from_csv_129 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_129',
            source="{{ result('load_csv_create_list_from_csv_129') }}",
            name="userreferencedata"
        )

        query_list_getallrecordsbasedonthereferenceidtofindunchangedrecords_130 = rail.QueryCollectionOperator(
            task_id='query_list_getallrecordsbasedonthereferenceidtofindunchangedrecords_130',
            query="""SELECT * FROM updateuserslist WHERE encoded IN (SELECT DISTINCT encoded FROM userreferencedata )""",
        )

        if_query_list_getallrecordsbasedonthereferenceidtofindunchangedrecords_130_rows_greater_than_0_131 = rail.IfOperator(
            task_id='if_query_list_getallrecordsbasedonthereferenceidtofindunchangedrecords_130_rows_greater_than_0_131',
            test='''{{ result('query_list_getallrecordsbasedonthereferenceidtofindunchangedrecords_130', 'length') > 0 }}''',
            yes_task="create_csv_lines_validation_files_132",
            no_task="query_list_getallrecordsbasedonthereferenceidtofindchangedrecords_135"
        )

        create_csv_lines_validation_files_132 = rail.WriteLogOperator(
            task_id='create_csv_lines_validation_files_132',
            log="{{ result('velaw_user_import_logs') }}",
            items="{{ result('query_list_getallrecordsbasedonthereferenceidtofindunchangedrecords_130') }}",
            message="No change in the user record",
            severity="Info",
            properties=lambda item: {
                "username": item['firstname'] + ' ' + item['lastname'],
                "loginname": item['loginname'],
                "employeeid": item['employeeid'],
                "importaction": "update",
                "status": "Skipped",
                "details": "No change in the user record"
            }
        )

        query_list_getallrecordsbasedonthereferenceidtofindchangedrecords_135 = rail.QueryCollectionOperator(
            task_id='query_list_getallrecordsbasedonthereferenceidtofindchangedrecords_135',
            query="""SELECT * FROM updateuserslist WHERE encoded NOT IN (SELECT DISTINCT encoded FROM userreferencedata)"""
        )

        trigger_dag_run_velawg3_user_update_v2_0async_137 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velawg3_user_update_v2_0async_137',
            retries=0,
            items="{{ result('query_list_getallrecordsbasedonthereferenceidtofindchangedrecords_135') }}",
            trigger_dag_id=config.user_update_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "loginname": item['loginname'],
                "lastname": item['lastname'],
                "firstname": item['firstname'],
                "department": "Vinson & Elkins|" + item['legalemployer'] + "|" + item['departmentname'],
                "employeetype": item['employeetype'],
                "location": item['location'],
                "enabled": "True" if item['isloginenabled'] == "Active" else "False",
                "employeeid": item['employeeid'],
                "startdate": item['startdate'],
                "enddate": item['enddate'],
                "timesheettemplate": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Template" and x['employee_type'] == item['employeetype'], rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "timesheetapprovalpath": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Approval Path" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "timezone": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeZone" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "timeofftemplate": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeOff Template", rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "timeoffapprovalpath": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeOff Approval Path" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'] and x['person_type'] == item['persontype'], rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "departmenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_department_groupswith_full_path_51'), 'fullpath', str("Vinson & Elkins|" + item['legalemployer'] + "|" + item['departmentname']), 'uri'),
                "locationuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_location_groups_47'), 'displayText', item['location'], 'uri') if item['location'] else null,
                "supervisoruri": next((x['useruri'] for x in rail.load_all_records(rail.result('query_list_all_usersfrom_replicon_72')) if item['supervisorloginname'] == x['loginname']), null),
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets_53'), 'displayText', '*Gen3 - Supervisor', 'uri'),
                "timesheettemplateuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets_templates_52'), 'name',
                                                                             next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Template" and x['employee_type'] == item['employeetype'], rail.result('velaw_user_import_mapper_search_entries_68'))))), 'uri') if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Template" and x['employee_type'] == item['employeetype'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "timesheetperioduri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_timesheet_period_service_44'), 'name',
                                                                           next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Period", rail.result('velaw_user_import_mapper_search_entries_68')))), null), 'uri') if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Period", rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "timesheetapprovalpathuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_timesheet_approval_paths_46'), 'displayText', next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Approval Path" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68'))))), 'uri') if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Approval Path" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "timezoneuri": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeZone" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68'))))).rsplit("|", 1)[-1] if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeZone" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_employee_type_groups_48'), 'displayText', item['employeetype'], 'uri') if item['employeetype'] else null,
                "workweekuri": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Work Week", rail.result('velaw_user_import_mapper_search_entries_68'))))).rsplit(" |", 1)[-1].strip() if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Work Week", rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "officescheduleuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_office_schedules_42'), 'displayText',
                                                                          next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Schedule" and x['country_code'] == item['countryisocode'] and x['flsa'] == item['flsastatus'], rail.result('velaw_user_import_mapper_search_entries_68'))))), 'uri') if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Schedule" and x['country_code'] == item['countryisocode'] and x['flsa'] == item['flsastatus'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "timeofftemplateuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_policy_sets_templates_52'), 'name',
                                                                           next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeOff Template", rail.result('velaw_user_import_mapper_search_entries_68'))))), 'uri') if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeOff Template", rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "timeoffapprovalpathuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_approval_paths_45'), 'displayText',
                                                                               next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeOff Approval Path" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'] and x['person_type'] == item['persontype'], rail.result('velaw_user_import_mapper_search_entries_68'))))), 'uri') if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeOff Approval Path" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'] and x['person_type'] == item['persontype'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "payruleuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_payrule_scripts_38'), 'displayText', next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Payrule" and x['employee_type'] == item['employeetype'], rail.result('velaw_user_import_mapper_search_entries_68'))))), 'uri') if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Payrule" and x['employee_type'] == item['employeetype'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "email": item['email'],
                "jobcode": item['jobcode'],
                "jobtitle": item['jobtitle'],
                "flsastatus": item['flsastatus'],
                "assignmentcategory": item['assignmentcategory'],
                "countryisocode": item['countryisocode'],
                "persontype": item['persontype'],
                "legalemployer": item['legalemployer'],
                "supervisorloginname": item['supervisorloginname'],
                "isloginenabled": item['isloginenabled'],
                "departmentname": item['departmentname'],
                "departmentcode": item['departmentcode'],
                "jobfamilies": item['jobfamilies'],
                "paytype": item['paytype'],
                "payratesamount": item['payratesamount'] if item['payratesamount'] else 0,
                "defaultbillingrateamount": item['defaultbillingrateamount'] if item['defaultbillingrateamount'] else 0,
                "hourlycostamount": item['hourlycostamount'] if item['hourlycostamount'] else 0,
                "jobcodeudfuri": rail.result('get_all_custom_fields_54')['jobcode'],
                "jobtitleudfuri": rail.result('get_all_custom_fields_54')['jobtitle'],
                "flsastatusudfuri": rail.result('get_all_custom_fields_54')['flsastatus'],
                "assignmentcategoryudfuri": rail.result('get_all_custom_fields_54')['assignmentcategory'],
                "countryisocodeudfuri": rail.result('get_all_custom_fields_54')['countryisocode'],
                "persontypeudfuri": rail.result('get_all_custom_fields_54')['persontype'],
                "legalemployerudfvalue": rail.result('get_all_custom_fields_54')['legalemployer'],
                "supervisorstatus": next((x['enabled'] for x in rail.load_all_records(rail.result('query_list_all_usersfrom_replicon_72')) if item['supervisorloginname'] == x['loginname']), null),
                "payratescurrency": item['payratescurrency'],
                "defaultbillingratecurrency": item['defaultbillingratecurrency'],
                "hourlycostcurrency": item['hourlycostcurrency'],
                "jobcodeudfvalueuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fieldsdropdownvaluesfor_job_code_57'), 'displayText', item['jobcode'], 'uri') if item['jobcode'] else null,
                "jobtitleudfvalueuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fieldsdropdownvaluesfor_job_title_58'), 'displayText', item['jobtitle'], 'uri') if item['jobtitle'] else null,
                "flsastatusudfvalueuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fieldsdropdownvaluesfor_f_l_s_a_status_59'), 'displayText', item['flsastatus'], 'uri') if item['flsastatus'] else null,
                "assignmentcategoryudfvalueuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fieldsdropdownvaluesfor_assignment_category_60'), 'displayText', item['assignmentcategory'], 'uri') if item['assignmentcategory'] else null,
                "countryisocodeudfvalueuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fieldsdropdownvaluesfor_country_i_s_o_code_61'), 'displayText', item['countryisocode'], 'uri') if item['countryisocode'] else null,
                "persontypeudfvalueuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fieldsdropdownvaluesfor_person_type_62'), 'displayText', item['persontype'], 'uri') if item['persontype'] else null,
                "legalemployerudfvalueuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fieldsdropdownvaluesfor_legal_employer_63'), 'displayText', item['legalemployer'], 'uri') if item['legalemployer'] else null,
                "jobfamiliesuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_cost_centers_groups_50'), 'displayText', item['jobfamilies'], 'uri') if item['jobfamilies'] else null,
                "payratescurrencyuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_currencies_40'), 'symbol', item['payratescurrency'], 'uri') if item['payratescurrency'] else rail.result('get_base_currency_41')['uri'],
                "defaultbillingratecurrencyuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_currencies_40'), 'symbol', item['defaultbillingratecurrency'], 'uri') if item['defaultbillingratecurrency'] else rail.result('get_base_currency_41')['uri'],
                "hourlycostcurrencyuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_currencies_40'), 'symbol', item['hourlycostcurrency'], 'uri') if item['hourlycostcurrency'] else rail.result('get_base_currency_41')['uri'],
                "paytypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_enabled_division_groups_49'), 'displayText', item['paytype'], 'uri') if item['paytype'] else null,
                "timesheetperiod": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Timesheet Period", rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "activitylist": [x['uri'] for x in rail.result('get_enabled_activities_39') if x['name']],
                "holicaycalendar": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Holiday Calendar" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "holicaycalendaruri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_holiday_calendars_43'), 'displayText',
                                                                           next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Holiday Calendar" and x['country_code'] ==
                                                                                                                                    item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68'))))), 'uri')
                if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Holiday Calendar" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "officeschedule": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Schedule" and x['country_code'] == item['countryisocode'] and x['flsa'] == item['flsastatus'], rail.result('velaw_user_import_mapper_search_entries_68')))), null),
                "payrule": next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "Payrule" and x['employee_type'] == "Others", rail.result('velaw_user_import_mapper_search_entries_68')))))
                if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeZone" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else
                next(iter(map(lambda x: x['value_|_default_uri'], filter(
                    lambda x: x['type'] == "Payrule" and x['employee_type'] == "employeetype", rail.result('velaw_user_import_mapper_search_entries_68')))))
                if next(iter(map(lambda x: x['value_|_default_uri'], filter(lambda x: x['type'] == "TimeZone" and x['country_code'] == item['countryisocode'] and x['location'] == item['location'], rail.result('velaw_user_import_mapper_search_entries_68')))), null) else null,
                "enduserpermissionseturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets_53'), 'displayText', "*Gen3 - Project Resource with reports", 'uri'),
                "supervisorendusepermissionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets_53'), 'displayText', "*Gen3 - Project Resource with reports & Substitute User", 'uri'),
                "useruri": next((x['useruri'] for x in rail.load_all_records(rail.result('query_list_all_usersfrom_replicon_72')) if item['loginname'] == x['loginname']), null),
                "preventbalanceoverdrawuri":  rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts_time_off_validation_script_64'), 'displayText', "Prevent balance overdraw", 'uri'),
                "startingbalancesettouri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts_time_off_balance_event_script_65'), 'displayText', "Starting Balance Set To", 'uri')
            }
        )

        wait_for_completion_trigger_dag_run_velawg3_user_update_v2_0async_137 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velawg3_user_update_v2_0async_137',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velawg3_user_update_v2_0async_137") }}'
        )

        gather_supervisor_check_update_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_supervisor_check_update_logs',
            dag_runs="{{ result('trigger_dag_run_velawg3_user_update_v2_0async_137') }}",
            dagrun_task_id='velaw_supervisor_check_user_update_logs',
            flatten=True
        )

        gather_user_update_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_update_logs',
            dag_runs="{{ result('trigger_dag_run_velawg3_user_update_v2_0async_137') }}",
            dagrun_task_id='velaw_check_user_update_logs',
            flatten=True
        )

        def accumulate_supervisor_check_entries():
            entries = []
            if rail.result("gather_supervisor_check_add_logs"):
                entries.append(rail.result("gather_supervisor_check_add_logs"))
            if rail.result("gather_supervisor_check_update_logs"):
                entries.extend(rail.result(
                    "gather_supervisor_check_update_logs"))
            return entries

        velaw_supervisor_check_search_entries_144 = rail.PythonOperator(
            task_id='velaw_supervisor_check_search_entries_144',
            python_callable=accumulate_supervisor_check_entries
        )

        if_entry_col1_present_145 = rail.IfOperator(
            task_id='if_entry_col1_present_145',
            test='''{{ result('velaw_supervisor_check_search_entries_144') | is_truthy }}''',
            yes_task="search_for_supervisor",
            no_task="dir_getthereferencefiledetails_159"
        )

        def get_supervisor_entries():
            def load_records(log_artifact):
                try:
                    logs = rail.load_all_records(log_artifact)
                    return logs
                except:  # pylint: disable=bare-except
                    return []

            log_artifacts = []

            if rail.result("gather_supervisor_check_add_logs"):
                log_artifacts.append(rail.result(
                    "gather_supervisor_check_add_logs"))

            if rail.result("gather_supervisor_check_update_logs"):
                log_artifacts.extend(rail.result(
                    "gather_supervisor_check_update_logs"))

            log_records = []
            if log_artifacts:
                for log in log_artifacts:
                    each_log_records = load_records(log)

                    if each_log_records:
                        log_records.extend(each_log_records)

            return list(map(lambda x: {
                **{k: v for k, v in x['properties'].items() if k != 'email'},
                **{
                    'jobid': x['ecid']
                }}, log_records))

        search_for_supervisor = rail.PythonOperator(
            task_id='search_for_supervisor',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=get_supervisor_entries
        )

        trigger_dag_run_velaw_user_import_velawg3_child_supervisor_assignment_v2_0async_147 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_supervisor_assignment_v2_0async_147',
            retries=0,
            items="{{ result('search_for_supervisor') | to_json }}",
            trigger_dag_id=config.supervisor_assignment_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "loginname": item['loginname'],
                "username": item['username'],
                "supervisorloginname": item['supervisorloginname'],
                "parentjobid": item['jobid'],
                "childjobid": item['status'],
                "useruri": item['user_uri'],
                "action": item['importaction'],
                "employeeid": item['employeeid'],
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets_53'), 'displayText', "*Gen3 - Supervisor", 'uri'),
                "enduserpermissionformanager": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets_53'), 'displayText', "*Gen3 - Project Resource with reports", 'uri'),
                "status": item['status'],
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_supervisor_assignment_v2_0async_147 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_supervisor_assignment_v2_0async_147',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_supervisor_assignment_v2_0async_147") }}'
        )

        gather_supervisor_assignment_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_supervisor_assignment_logs',
            dag_runs="{{ result('trigger_dag_run_velaw_user_import_velawg3_child_supervisor_assignment_v2_0async_147') }}",
            dagrun_task_id='velaw_supervisor_assignment_logs',
            flatten=True
        )

        dir_getthereferencefiledetails_159 = rail.SFTPListFilesOperator(
            task_id='dir_getthereferencefiledetails_159',
            paths=[config.reference_filepath]
        )

        if_reference_file_list_present_159_1 = rail.IfOperator(
            task_id='if_reference_file_list_present_159_1',
            test=lambda: has_any_file(
                "dir_getthereferencefiledetails_159", config.reference_filepath),
            yes_task="rename_archivethereferenceinputfile_160",
            no_task="upload_uploadreferencefile_161"
        )

        rename_archivethereferenceinputfile_160 = rail.SFTPMoveFileOperator(
            task_id='rename_archivethereferenceinputfile_160',
            new_filename=config.archive_filepath +
            "/Old_{{ result('log_formattedjobstarttime_2') }}_{{ result('reference_file') }}",
            existing_filename=config.reference_filepath +
            "/{{ result('reference_file') }}"
        )

        upload_uploadreferencefile_161 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadreferencefile_161',
            content='''{{ result('create_csv_lines_stripthedata_25') }}''',
            remote_filepath=config.reference_filepath +
            '''/Reference_{{ result('log_formattedjobstarttime_2') }}_{{ result('new_file_sensor')| file_name }}'''
        )

        trigger_user_import_log_generation = rail.TriggerDagRunOperator(
            task_id='trigger_user_import_log_generation',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.log_generation_child_dag_id,
            conf=lambda dag_run: {
                'user_import_logs': rail.result('velaw_user_import_logs'),
                'user_add_logs': rail.result('gather_user_add_logs'),
                'user_update_logs': rail.result('gather_user_update_logs') if rail.result('gather_user_update_logs') else [],
                'user_disable_logs': rail.result('gather_user_disable_logs'),
                'user_disable_different_iso_logs': rail.result('gather_user_disable_different_iso_logs'),
                'supervisor_assignment_logs': rail.result('gather_supervisor_assignment_logs'),
                'parentjobid': get_dagrun_ecid(rail.get_current_context()['dag_run']),
                'time': rail.result('log_formattedjobstarttime_2'),
                'filename': (rail.result('new_file_sensor')).split('/')[-1]
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        new_file_sensor >> was_new_file_found
        was_new_file_found >> rail.Label(
            'Yes') >> if_name_downcase_not_ends_with_csv_3
        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun >> log_to_sumo

        if_name_downcase_not_ends_with_csv_3 >> rail.Label(
            'Yes') >> send_mail_notificationforincorrectfileformat_4 >> rename_archivetheinputfile_5 >> log_to_sumo
        if_name_downcase_not_ends_with_csv_3 >> rail.Label(
            'No') >> log_formattedjobstarttime_2 >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> velaw_user_import_logs >> velaw_supervisor_check_logs \
            >> download_9 >> rename_archivetheinputfile_17 >> load_csv_create_list_from_csv_raw_input_list_10 \
            >> create_collection_create_list_from_csv_raw_input_list_10 \
            >> query_list_all_gb_and_us_users_11 >> query_list_all_non_gb_and_us_users_12 \
            >> parse_csv_parse_input_file_14 >> if_parse_csv_parse_input_file_14_lines_less_than_1_15

        if_parse_csv_parse_input_file_14_lines_less_than_1_15 >> rail.Label(
            'Yes') >> send_mail_notificationfornorecords_blank_data_16 >> log_to_sumo
        if_parse_csv_parse_input_file_14_lines_less_than_1_15 >> rail.Label(
            'No') >> get_tenant_and_useridentity_details_19 >> trigger_dag_run_velaw_user_import_velawg3_child_groups_update_v2_020 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_groups_update_v2_020 >> create_csv_lines_stripthedata_25 \
            >> load_csv_create_list_from_csv_raw_input_list_26 >> create_collection_create_list_from_csv_raw_input_list_26 \
            >> query_list_getuserwithblank_loginname_27 >> if_query_list_getuserwithblank_loginname_27_rows_greater_than_0_28
        if_query_list_getuserwithblank_loginname_27_rows_greater_than_0_28 >> rail.Label(
            'Yes') >> create_csv_lines_validation_files_29 >> query_list_getuserwith_loginname_32
        if_query_list_getuserwithblank_loginname_27_rows_greater_than_0_28 >> rail.Label(
            'No') >> query_list_getuserwith_loginname_32 >> generate_report_35 >> get_report_details

        fail_no_report_data >> log_to_sumo
        create_collection_from_report_data >> get_all_payrule_scripts_38 >> get_enabled_activities_39 >> get_all_currencies_40 \
            >> get_base_currency_41 >> get_all_office_schedules_42 >> get_all_holiday_calendars_43 >> get_all_enabled_timesheet_period_service_44 \
            >> get_all_time_off_approval_paths_45 >> get_all_timesheet_approval_paths_46 >> get_all_enabled_location_groups_47 \
            >> get_all_enabled_employee_type_groups_48 >> get_all_enabled_division_groups_49 >> get_all_enabled_cost_centers_groups_50 \
            >> get_all_enabled_department_groupswith_full_path_51 >> get_all_policy_sets_templates_52 >> get_all_permission_sets_53 \
            >> get_all_custom_fields_54 >> trigger_dag_run_velaw_user_import_velawg3_drop_down_udf_custom_field_check_v2_056 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_drop_down_udf_custom_field_check_v2_056 >> get_all_custom_fieldsdropdownvaluesfor_job_code_57 \
            >> get_all_custom_fieldsdropdownvaluesfor_job_title_58 >> get_all_custom_fieldsdropdownvaluesfor_f_l_s_a_status_59 \
            >> get_all_custom_fieldsdropdownvaluesfor_assignment_category_60 >> get_all_custom_fieldsdropdownvaluesfor_country_i_s_o_code_61 \
            >> get_all_custom_fieldsdropdownvaluesfor_person_type_62 >> get_all_custom_fieldsdropdownvaluesfor_legal_employer_63 \
            >> get_all_scripts_time_off_validation_script_64 >> get_all_scripts_time_off_balance_event_script_65 \
            >> velaw_user_import_mapper_search_entries_68 >> query_list_all_usersfrom_replicon_72 \
            >> query_list_enabled_usersfrom_replicon_73 >> query_list_disabled_usersfrom_replicon_75 \
            >> query_list_validated_userstodisablewhoarealreadydisabled_77 >> if_query_list_validated_userstodisablewhoarealreadydisabled_77_rows_greater_than_0_78
        if_query_list_validated_userstodisablewhoarealreadydisabled_77_rows_greater_than_0_78 >> rail.Label(
            'Yes') >> create_csv_lines_disabled_skip_files_79 >> query_list_validated_userstodisablewithout_end_date_82
        if_query_list_validated_userstodisablewhoarealreadydisabled_77_rows_greater_than_0_78 >> rail.Label(
            'No') >> query_list_validated_userstodisablewithout_end_date_82 >> if_query_list_validated_userstodisablewithout_end_date_82_rows_greater_than_0_83
        if_query_list_validated_userstodisablewithout_end_date_82_rows_greater_than_0_83 >> rail.Label(
            'Yes') >> create_csv_lines_disabled_skip_files_84 >> query_list_validated_userstodisablewith_enddate_87
        if_query_list_validated_userstodisablewithout_end_date_82_rows_greater_than_0_83 >> rail.Label(
            'No') >> query_list_validated_userstodisablewith_enddate_87 >> trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_90 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_90 >> gather_user_disable_logs \
            >> query_list_validated_userstodisablewith_different_i_s_o_countrycode_95 \
            >> trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_98 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0async_98 >> gather_user_disable_different_iso_logs \
            >> query_list_validated_newuserstoprocess_with_enabledstatusas_falseor_first_nameis_blankand_lastnameisblank_108 \
            >> if_query_list_validated_newuserstoprocess_with_enabledstatusas_falseor_first_nameis_blankand_lastnameisblank_108_rows_greater_than_0_109
        if_query_list_validated_newuserstoprocess_with_enabledstatusas_falseor_first_nameis_blankand_lastnameisblank_108_rows_greater_than_0_109 >> rail.Label(
            'Yes') >> create_csv_lines_validation_filesfornewusers_110 >> query_list_validated_newuserstoprocess_with_enabledstatusas_true_113
        if_query_list_validated_newuserstoprocess_with_enabledstatusas_falseor_first_nameis_blankand_lastnameisblank_108_rows_greater_than_0_109 >> rail.Label(
            'No') >> query_list_validated_newuserstoprocess_with_enabledstatusas_true_113 >> trigger_dag_run_velaw_user_import_velawg3_child_add_user_v2_0async_115 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_add_user_v2_0async_115 >> gather_supervisor_check_add_logs >> gather_user_add_logs \
            >> query_list_updateuserstoprocess_123 >> dir_getthereferencefiledetails_125 >> if_create_list_updateuserstoprocess_124_row_count_greater_than_0_126
        if_create_list_updateuserstoprocess_124_row_count_greater_than_0_126 >> rail.Label(
            'Yes') >> reference_file >> if_parameters_usereferencefile_contains_yes_127
        if_parameters_usereferencefile_contains_yes_127 >> rail.Label(
            'Yes') >> download_downloadthereferencefile_128 >> load_csv_create_list_from_csv_129 >> create_collection_create_list_from_csv_129 >> query_list_getallrecordsbasedonthereferenceidtofindunchangedrecords_130 >> if_query_list_getallrecordsbasedonthereferenceidtofindunchangedrecords_130_rows_greater_than_0_131
        if_query_list_getallrecordsbasedonthereferenceidtofindunchangedrecords_130_rows_greater_than_0_131 >> rail.Label(
            'Yes') >> create_csv_lines_validation_files_132 >> query_list_getallrecordsbasedonthereferenceidtofindchangedrecords_135
        if_query_list_getallrecordsbasedonthereferenceidtofindunchangedrecords_130_rows_greater_than_0_131 >> rail.Label(
            'No') >> query_list_getallrecordsbasedonthereferenceidtofindchangedrecords_135 >> trigger_dag_run_velawg3_user_update_v2_0async_137 \
            >> wait_for_completion_trigger_dag_run_velawg3_user_update_v2_0async_137 >> gather_supervisor_check_update_logs >> gather_user_update_logs >> velaw_supervisor_check_search_entries_144
        if_parameters_usereferencefile_contains_yes_127 >> rail.Label(
            'No') >> velaw_supervisor_check_search_entries_144
        if_create_list_updateuserstoprocess_124_row_count_greater_than_0_126 >> rail.Label(
            'No') >> velaw_supervisor_check_search_entries_144 >> if_entry_col1_present_145
        if_entry_col1_present_145 >> rail.Label(
            'Yes') >> search_for_supervisor >> trigger_dag_run_velaw_user_import_velawg3_child_supervisor_assignment_v2_0async_147 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_supervisor_assignment_v2_0async_147 \
            >> gather_supervisor_assignment_logs >> dir_getthereferencefiledetails_159
        if_entry_col1_present_145 >> rail.Label(
            'No') >> dir_getthereferencefiledetails_159 >> if_reference_file_list_present_159_1
        if_reference_file_list_present_159_1 >> rail.Label(
            'Yes') >> rename_archivethereferenceinputfile_160 >> upload_uploadreferencefile_161
        if_reference_file_list_present_159_1 >> rail.Label(
            'No') >> upload_uploadreferencefile_161 >> trigger_user_import_log_generation >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
