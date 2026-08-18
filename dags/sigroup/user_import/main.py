from datetime import timedelta
from pendulum import datetime
from airflow.models import Variable
from sigroup.user_import.utils import custom_methods
import rail
null = None
# pylint:disable = too-many-statements
def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"sigroup_user_import_master_{config.instance}",
        description="sigroup user import",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 2, 2),
        schedule_interval=timedelta(minutes=5),
        max_active_runs=config.master_max_active_runs,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id="new_file_sensor",
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            trigger_rule="all_done",
            test='{{get_task_state("new_file_sensor") == "success"}}',
            yes_task="archive_file",
            no_task="delete_dagrun"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id="archive_file",
            new_filename=config.archive_filepath+'/archive_{{ dag_run_ecid() |replace(":","_")}}_{{ result("new_file_sensor") | file_name }}',
            existing_filename='{{result("new_file_sensor")}}'
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dagrun"
        )

        create_sigroup_user_import_log = rail.CreateLogOperator(
            task_id="create_sigroup_user_import_log"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id="download_file",
            remote_filepath='{{result("new_file_sensor")}}'
        )

        can_decrypt_file = rail.IfOperator(
            task_id="can_decrypt_file",
            test=bool(Variable.get(
                config.sigroup_user_import_decrypt_var, "true").lower() == "true"),
            yes_task="decrypt_file",
            no_task="dummy_data_load"
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id="decrypt_file",
            source='{{result("download_file")}}',
            pgp_conn_id=config.pgp_conn_id
        )

        dummy_data_load = rail.PythonOperator(
            task_id="dummy_data_load",
            python_callable=lambda: rail.result("decrypt_file") if Variable.get(
                config.sigroup_user_import_decrypt_var, "true").lower() == "true" else rail.result("download_file")
        )

        load_user_data_csv = rail.LoadCSVFileOperator(
            task_id="load_user_data_csv",
            document='{{result("dummy_data_load")}}',
            headers=["loginname", "firstname", "lastname",
                     "displayname", "employee_type", "employeeid", "legalemployer",
                     "legalemployercode", "paygroup", "paygroupcode",
                     "businessunit", "businessunitcode", "location", "locationcode",
                     "locationstate", "locationcity", "department",
                     "departmentcode", "financecostcenter",
                     "financecostcentercode", "startdate", "enddate",
                     "timesheetstartdate", "status", "action",
                     "actioneffectivedate", "workinglifestartdate", "emailaddress",
                     "initialsupervisorloginname", "timezone",
                     "hourlypayrate", "hourlypayratecurrency", "hourlypayeffectivedate",
                     "hourlycostamount", "hourlycostcurrency", "hourlycosteffectivedate",
                     "shift", "cloudpay_paycode", "manufacturing",
                     "coefficientlevel", "elderlyallowance", "apprentice",
                     "timecode", "cbaappendix", "istariffemployee",
                     "tariffclassification", "stepinformation",
                     "fte", "ptoservicedate", "workleader"],
        )

        create_user_data_collection = rail.CreateCollectionOperator(
            task_id="create_user_data_collection",
            source='{{result("load_user_data_csv")}}',
            name="import_user_records"
        )

        if_no_user_records = rail.IfOperator(
            task_id="if_no_user_records",
            test='{{result("create_user_data_collection","length")>0}}',
            yes_task="query_user_data_without_mandatory_fields",
            no_task="send_no_records_mail"
        )

        send_no_records_mail = rail.EmailOperator(
            task_id="send_no_records_mail",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{get_company_key()}}' + " | Replicon user import skipped - " +
            '{{current_time_in_specified_tz()}}',
            html_content="templates/no_records_mail.html"
        )

        query_user_data_without_mandatory_fields = rail.QueryCollectionOperator(
            task_id="query_user_data_without_mandatory_fields",
            query="""SELECT *  FROM import_user_records WHERE NULLIF("loginname","") IS  NULL OR
            NULLIF("firstname","") IS  NULL OR NULLIF("lastname","") IS  NULL OR
            NULLIF("employeeid","") IS  NULL OR NULLIF("location","") IS  NULL OR
            NULLIF("department","") IS  NULL OR NULLIF("employee_type","") IS  NULL OR
            NULLIF("status","") IS  NULL OR NULLIF("paygroup","") IS  NULL
            """
        )

        if_user_data_without_mandatory_fields = rail.IfOperator(
            task_id="if_user_data_without_mandatory_fields",
            test='{{result("query_user_data_without_mandatory_fields", "length") > 0}}',
            yes_task="write_log_user_without_mandatory_fields",
            no_task="query_user_data_with_mandatory_fields"
        )

        write_log_user_without_mandatory_fields = rail.WriteLogOperator(
            task_id="write_log_user_without_mandatory_fields",
            log='{{result("create_sigroup_user_import_log")}}',
            items='{{result("query_user_data_without_mandatory_fields")}}',
            message="User mandatory fields missing",
            properties=lambda item: {
                "EmployeeId": item.get("employeeid", ""),
                "Username": item.get("firstname", "") + item.get("lastname", ""),
                "Action": "pre-check",
                "Status": "Exception" if item["paygroup"] else "Error",
                "Details": "One or more mandatory fields value is missing" if item["paygroup"] else "Paygroup is missing for the user",
            }
        )

        query_user_data_with_mandatory_fields = rail.QueryCollectionOperator(
            task_id="query_user_data_with_mandatory_fields",
            query="""SELECT *  FROM import_user_records WHERE NULLIF("loginname","") IS NOT NULL AND
            NULLIF("firstname","") IS NOT NULL AND NULLIF("lastname","") IS NOT NULL AND
            NULLIF("employeeid","") IS NOT NULL AND NULLIF("location","") IS NOT NULL AND
            NULLIF("department","") IS NOT NULL AND NULLIF("employee_type","") IS NOT NULL AND
            NULLIF("status","") IS NOT NULL AND NULLIF("paygroup","") IS NOT NULL
            """,
            name="user_records"
        )

        if_valid_user_data = rail.IfOperator(
            task_id="if_valid_user_data",
            test='{{result("query_user_data_with_mandatory_fields", "length") > 0}}',
            yes_task="get_all_customfields",
            no_task="process_log_generation"
        )

        get_all_customfields = rail.RepliconServiceOperator(
            task_id="get_all_customfields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                    "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=custom_methods.get_custom_fields_data
        )

        get_all_permissionset = rail.RepliconServiceOperator(
            task_id="get_all_permissionset",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id="get_all_timezones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones"
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id="get_all_office_schedules",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        get_all_approval_paths_timesheet = rail.RepliconServiceOperator(
            task_id="get_all_approval_paths_timesheet",
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths"
        )

        get_all_approval_paths_timeoff = rail.RepliconServiceOperator(
            task_id="get_all_approval_paths_timeoff",
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths"
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets"
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id="get_all_holiday_calendars",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars"
        )

        get_all_payrule_scripts = rail.RepliconServiceOperator(
            task_id="get_all_payrule_scripts",
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts"
        )

        get_all_currencies = rail.RepliconServiceOperator(
            task_id="get_all_currencies",
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies"
        )

        get_timesheet_period_list = rail.RepliconServiceOperator(
            task_id="get_timesheet_period_list",
            endpoint="/services/TimesheetPeriodListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:timesheet-period-list-column:timesheet-period"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: list(map(lambda i: {
                "textValue": i["cells"][0].get("textValue", ""),
                "uri": i["cells"][0]["uri"]
            }, response["rows"])) if response else null
        )

        get_all_activities = rail.RepliconServiceOperator(
            task_id="get_all_activities",
            endpoint="/services/ActivityService1.svc/GetEnabledActivities"
        )

        get_all_timeoff_validation_scripts = rail.RepliconServiceOperator(
            task_id="get_all_timeoff_validation_scripts",
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
        )

        get_all_time_off_balance_scripts = rail.RepliconServiceOperator(
            task_id="get_all_time_off_balance_scripts",
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts"
        )

        
        query_supervisors_from_feedfile = rail.QueryCollectionOperator(
            task_id="query_supervisors_from_feedfile",
            query="""SELECT * FROM user_records WHERE loginname in
              (SELECT DISTINCT initialsupervisorloginname FROM user_records)"""
        )


        query_legal_employers_from_feed_file = rail.QueryCollectionOperator(
            task_id="query_legal_employers_from_feed_file",
            query="""SELECT DISTINCT legalemployer, legalemployercode FROM user_records
                WHERE NULLIF("legalemployer", "") IS NOT NULL AND NULLIF("legalemployercode","") IS NOT NULL"""
        )

        process_legal_employers = rail.TriggerDagRunOperator(
            task_id="process_legal_employers",
            trigger_dag_id=config.sigroup_legal_employers_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_locations_from_feed_file = rail.QueryCollectionOperator(
            task_id="query_locations_from_feed_file",
            query="""SELECT DISTINCT location, locationcode FROM user_records WHERE
            NULLIF("location","") IS NOT NULL AND NULLIF("locationcode", "") IS NOT NULL"""
        )

        process_locations = rail.TriggerDagRunOperator(
            task_id="process_locations",
            trigger_dag_id=config.sigroup_locations_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_paygroups_from_feed_file = rail.QueryCollectionOperator(
            task_id="query_paygroups_from_feed_file",
            query="""SELECT DISTINCT paygroup, paygroupcode FROM user_records WHERE
            NULLIF("paygroup","" ) IS NOT NULL AND NULLIF("paygroupcode", "") IS NOT NULL"""
        )

        process_paygroups = rail.TriggerDagRunOperator(
            task_id="process_paygroups",
            trigger_dag_id=config.sigroup_paygroups_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_departments_from_feed_file = rail.QueryCollectionOperator(
            task_id="query_departments_from_feed_file",
            query="""SELECT DISTINCT department, departmentcode FROM user_records WHERE
            NULLIF("department","") IS NOT NULL AND NULLIF("departmentcode", "") IS NOT NULL"""
        )

        process_departments = rail.TriggerDagRunOperator(
            task_id="process_departments",
            trigger_dag_id=config.sigroup_departments_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_costcenters_from_feed_file = rail.QueryCollectionOperator(
            task_id="query_costcenters_from_feed_file",
            query="""SELECT DISTINCT financecostcenter, financecostcentercode FROM user_records WHERE
            NULLIF("financecostcenter","") IS NOT NULL AND NULLIF("financecostcentercode", "") IS NOT NULL"""
        )

        process_costcenters = rail.TriggerDagRunOperator(
            task_id="process_costcenters",
            trigger_dag_id=config.sigroup_costcenters_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_business_units_from_feed_file = rail.QueryCollectionOperator(
            task_id="query_business_units_from_feed_file",
            query="""SELECT DISTINCT businessunit, businessunitcode FROM user_records WHERE
            NULLIF("businessunit","") IS NOT NULL AND NULLIF("businessunitcode", "") IS NOT NULL"""
        )

        process_business_units = rail.TriggerDagRunOperator(
            task_id="process_business_units",
            trigger_dag_id=config.sigroup_business_units_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        query_states_from_feed_file = rail.QueryCollectionOperator(
            task_id="query_states_from_feed_file",
            query="""SELECT DISTINCT locationstate FROM user_records WHERE NULLIF("locationstate","") IS NOT NULL"""
        )

        process_states = rail.TriggerDagRunOperator(
            task_id="process_states",
            trigger_dag_id=config.sigroup_states_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "states_uri": rail.result("get_all_customfields")["Location State"]
            }
        )

        query_cities_from_feed_file = rail.QueryCollectionOperator(
            task_id="query_cities_from_feed_file",
            query="""SELECT DISTINCT locationcity FROM user_records WHERE NULLIF("locationcity","") IS NOT NULL"""
        )

        process_cities = rail.TriggerDagRunOperator(
            task_id="process_cities",
            trigger_dag_id=config.sigroup_cities_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "cities_uri": rail.result("get_all_customfields")["Location City"]
            }
        )

        query_coefficient_levels_from_feed_file = rail.QueryCollectionOperator(
            task_id="query_coefficient_levels_from_feed_file",
            query="""SELECT DISTINCT coefficientlevel FROM user_records WHERE NULLIF("coefficientlevel","") IS NOT NULL"""
        )

        if_coeffcient_level_data = rail.IfOperator(
            task_id="if_coeffcient_level_data",
            test='{{result("query_coefficient_levels_from_feed_file", "length") > 0}}',
            yes_task="process_coefficient_levels",
        )

        process_coefficient_levels = rail.TriggerDagRunOperator(
            task_id="process_coefficient_levels",
            trigger_dag_id=config.sigroup_coefficient_levels_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "coefficient_levels_uri": rail.result("get_all_customfields")["Coefficient Level"]
            }
        )

        query_elderly_allowance_from_feed_file = rail.QueryCollectionOperator(
            task_id="query_elderly_allowance_from_feed_file",
            query="""SELECT DISTINCT elderlyallowance FROM user_records WHERE NULLIF("elderlyallowance","") IS NOT NULL"""
        )

        if_elderly_allowance_data = rail.IfOperator(
            task_id="if_elderly_allowance_data",
            test='{{result("query_elderly_allowance_from_feed_file", "length") > 0}}',
            yes_task="process_elderly_allowance",
        )

        process_elderly_allowance = rail.TriggerDagRunOperator(
            task_id="process_elderly_allowance",
            trigger_dag_id=config.sigroup_elderly_allowance_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "elderly_allowance_uri": rail.result("get_all_customfields")["Elderly Allowance"]
            }
        )

        query_timecode_from_feed_file = rail.QueryCollectionOperator(
            task_id="query_timecode_from_feed_file",
            query="""SELECT DISTINCT timecode FROM user_records WHERE NULLIF("timecode","") IS NOT NULL"""
        )

        if_timecode_data = rail.IfOperator(
            task_id="if_timecode_data",
            test='{{result("query_timecode_from_feed_file", "length") > 0}}',
            yes_task="process_timecode",
        )

        process_timecode = rail.TriggerDagRunOperator(
            task_id="process_timecode",
            trigger_dag_id=config.sigroup_timecode_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "timecode_uri": rail.result("get_all_customfields")["Time Code"]
            }
        )

        query_cba_appendix_from_feed_file = rail.QueryCollectionOperator(
            task_id="query_cba_appendix_from_feed_file",
            query="""SELECT DISTINCT cbaappendix FROM user_records WHERE NULLIF("cbaappendix","") IS NOT NULL"""
        )

        if_cba_appendix_data = rail.IfOperator(
            task_id="if_cba_appendix_data",
            test='{{result("query_cba_appendix_from_feed_file", "length") > 0}}',
            yes_task="process_cba_appendix",
        )

        process_cba_appendix = rail.TriggerDagRunOperator(
            task_id="process_cba_appendix",
            trigger_dag_id=config.sigroup_cba_appendix_levels_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "cba_appendix_uri": rail.result("get_all_customfields")["CBA Appendix"]
            }
        )

        query_tariff_classification_from_feed_file = rail.QueryCollectionOperator(
            task_id="query_tariff_classification_from_feed_file",
            query="""SELECT DISTINCT tariffclassification FROM user_records WHERE NULLIF("tariffclassification","") IS NOT NULL"""
        )

        if_tariff_classification_data = rail.IfOperator(
            task_id="if_tariff_classification_data",
            test='{{result("query_tariff_classification_from_feed_file", "length") > 0}}',
            yes_task="process_tariff_classification",
        )

        process_tariff_classification = rail.TriggerDagRunOperator(
            task_id="process_tariff_classification",
            trigger_dag_id=config.sigroup_tariff_classification_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "tariff_classification_uri": rail.result("get_all_customfields")["Tariff Classification"]
            }
        )

        query_step_info_from_feed_file = rail.QueryCollectionOperator(
            task_id="query_step_info_from_feed_file",
            query="""SELECT DISTINCT stepinformation FROM user_records WHERE NULLIF("stepinformation","") IS NOT NULL"""
        )

        if_step_info = rail.IfOperator(
            task_id="if_step_info",
            test='{{result("query_step_info_from_feed_file", "length") > 0}}',
            yes_task="process_step_information"
        )

        process_step_information = rail.TriggerDagRunOperator(
            task_id="process_step_information",
            trigger_dag_id=config.sigroup_step_information_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "stepinformation_uri": rail.result("get_all_customfields")["Step Information"]
            }
        )

        start_new_groups_list1 = rail.EmptyOperator(
            task_id="start_new_groups_list1")

        get_employee_type_paygroups = rail.RepliconServiceOperator(
            task_id="get_employee_type_paygroups",
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "10000",
                    "columnUris": [
                        "urn:replicon:employee-type-group-list-column:code",
                        "urn:replicon:employee-type-group-list-column:employee-type-group"
                    ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: list(map(lambda i: {
                "code": i["cells"][0].get("textValue", ""),
                "textValue": i["cells"][1]["textValue"],
                "uri": i["cells"][1]["uri"]
            }, response["rows"]))
        )

        get_department_group = rail.RepliconServiceOperator(
            task_id="get_department_group",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "10000",
                    "columnUris":  [
                        "urn:replicon:department-group-list-column:code",
                        "urn:replicon:department-group-list-column:department-group"
                    ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: list(map(lambda i: {
                "code": i["cells"][0].get("textValue", ""),
                "textValue": i["cells"][1]["textValue"],
                "uri": i["cells"][1]["uri"]
            }, response["rows"]))
        )

        get_finance_cost_centers = rail.RepliconServiceOperator(
            task_id="get_finance_cost_centers",
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "10000",
                    "columnUris":  [
                        "urn:replicon:cost-center-list-column:code",
                        "urn:replicon:cost-center-list-column:cost-center"
                    ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: list(map(lambda i: {
                "code": i["cells"][0].get("textValue", ""),
                "textValue": i["cells"][1]["textValue"],
                "uri": i["cells"][1]["uri"]
            }, response["rows"]))
        )

        get_location_schedule = rail.RepliconServiceOperator(
            task_id="get_location_schedule",
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "10000",
                    "columnUris": [
                        "urn:replicon:location-list-column:code",
                        "urn:replicon:location-list-column:location"
                    ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: list(map(lambda i: {
                "code": i["cells"][0].get("textValue", ""),
                "textValue": i["cells"][1]["textValue"],
                "uri": i["cells"][1]["uri"]
            }, response["rows"]))
        )

        get_legal_employers = rail.RepliconServiceOperator(
            task_id="get_legal_employers",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "10000",
                    "columnUris": [
                        "urn:replicon:service-center-list-column:code",
                        "urn:replicon:service-center-list-column:service-center"
                    ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: list(map(lambda i: {
                "code": i["cells"][0].get("textValue", ""),
                "textValue": i["cells"][1]["textValue"],
                "uri": i["cells"][1]["uri"]
            }, response["rows"]))
        )

        get_business_units = rail.RepliconServiceOperator(
            task_id="get_business_units",
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "10000",
                    "columnUris": [
                        "urn:replicon:division-list-column:code",
                        "urn:replicon:division-list-column:division"
                    ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: list(map(lambda i: {
                "code": i["cells"][0].get("textValue", ""),
                "textValue": i["cells"][1]["textValue"],
                "uri": i["cells"][1]["uri"]
            }, response["rows"]))
        )

        get_admin_modified_custom_field_dropdown = rail.RepliconServiceOperator(
            task_id="get_admin_modified_custom_field_dropdown",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_all_customfields")["Admin Modified"]
            }
        )
        get_employee_type_custom_field_dropdown = rail.RepliconServiceOperator(
            task_id="get_employee_type_custom_field_dropdown",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_all_customfields")["EMPLOYEE_TYPE"]
            }
        )
        get_state_custom_field_dropdown = rail.RepliconServiceOperator(
            task_id="get_state_custom_field_dropdown",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_all_customfields")["Location State"]
            }
        )

        get_city_custom_field_dropdown = rail.RepliconServiceOperator(
            task_id="get_city_custom_field_dropdown",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_all_customfields")["Location City"]
            }
        )
        get_manufacturing_custom_field_dropdown = rail.RepliconServiceOperator(
            task_id="get_manufacturing_custom_field_dropdown",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_all_customfields")["Manufacturing"]
            }
        )
        get_coefficient_level_custom_field_dropdown = rail.RepliconServiceOperator(
            task_id="get_coefficient_level_custom_field_dropdown",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_all_customfields")["Coefficient Level"]
            }
        )
        get_elderly_allowance_custom_field_dropdown = rail.RepliconServiceOperator(
            task_id="get_elderly_allowance_custom_field_dropdown",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_all_customfields")["Elderly Allowance"]
            }
        )
        get_apprentice_custom_field_dropdown = rail.RepliconServiceOperator(
            task_id="get_apprentice_custom_field_dropdown",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_all_customfields")["Apprentice"]
            }
        )
        get_timecode_custom_field_dropdown = rail.RepliconServiceOperator(
            task_id="get_timecode_custom_field_dropdown",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_all_customfields")["Time Code"]
            }
        )
        get_cba_appendix_custom_field_dropdown = rail.RepliconServiceOperator(
            task_id="get_cba_appendix_custom_field_dropdown",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_all_customfields")["CBA Appendix"]
            }
        )
        get_tariff_employee_custom_field_dropdown = rail.RepliconServiceOperator(
            task_id="get_tariff_employee_custom_field_dropdown",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_all_customfields")["Is Tariff Employee"]
            }
        )
        get_tariff_classification_custom_field_dropdown = rail.RepliconServiceOperator(
            task_id="get_tariff_classification_custom_field_dropdown",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_all_customfields")["Tariff Classification"]
            }
        )
        get_step_information_custom_field_dropdown = rail.RepliconServiceOperator(
            task_id="get_step_information_custom_field_dropdown",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_all_customfields")["Step Information"]
            }
        )
        get_work_leader_custom_field_dropdown = rail.RepliconServiceOperator(
            task_id="get_work_leader_custom_field_dropdown",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result("get_all_customfields")["Work Leader"]
            }
        )

        end_new_groups_list2 = rail.EmptyOperator(
            task_id="end_new_groups_list2")

        process_valid_users = rail.trigger_parallel_dagrun(
            task_id="process_valid_users",
            items='{{result("query_user_data_with_mandatory_fields")}}',
            parallel_count=config.child_max_active_runs,
            trigger_dag_id=config.sigroup_valid_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=custom_methods.get_all_user_config
        )

        get_pending_supervisor = rail.FilterLogEntriesOperator(
            task_id="get_pending_supervisor",
            severity="Pending",
            log='{{result("create_sigroup_user_import_log")}}',
            remove_filtered_entries=True
        )

        if_pending_supervisor = rail.IfOperator(
            task_id="if_pending_supervisor",
            test='{{result("get_pending_supervisor", "length") > 0}}',
            yes_task="start_supervisor_assignment",
            no_task="start_log_generation"
        )

        start_supervisor_assignment = rail.EmptyOperator(task_id="start_supervisor_assignment")

        process_supervisor_assignment = rail.trigger_parallel_dagrun(
            task_id="process_supervisor_assignment",
            trigger_dag_id=config.sigroup_process_supervisor_dagid,
            items='{{result("get_pending_supervisor")}}',
            parallel_count=config.child_max_active_runs,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item["properties"]
            }
        )

        start_log_generation = rail.EmptyOperator(task_id="start_log_generation")

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.sigroup_process_log_generation_dagid,
            wait_for_completion=True,
            conf={
                'userlogs': '{{result("create_sigroup_user_import_log")}}',
                'log_filename': '{{dag_run_ecid()}}' + '{{result("new_file_sensor")|file_name}}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{get_error_message()|is_truthy}}',
            yes_task="fail_dag"
        )

        fail_dag = rail.FailOperator(
            task_id="fail_dag",
            message='{{get_error_message()}}'
        )

        
        new_file_sensor >> download_file >>\
            can_decrypt_file >> rail.Label(
                "Yes") >> decrypt_file >> dummy_data_load
        can_decrypt_file >> rail.Label("No") >> dummy_data_load >>\
            create_sigroup_user_import_log >> load_user_data_csv >>\
            create_user_data_collection >>\
            if_no_user_records >> rail.Label(
                "No") >> send_no_records_mail >> log_to_sumo
        if_no_user_records >> rail.Label("Yes") >>\
            query_user_data_without_mandatory_fields >>\
            if_user_data_without_mandatory_fields >> rail.Label("Yes") >>\
            write_log_user_without_mandatory_fields >> query_user_data_with_mandatory_fields
        if_user_data_without_mandatory_fields >> rail.Label("No") >>\
            query_user_data_with_mandatory_fields >>\
        if_valid_user_data >> rail.Label("No")  >> process_log_generation
        if_valid_user_data >> rail.Label("Yes")  >>get_all_customfields >>\
        [
            get_all_permissionset,
            get_all_timezones,
            get_all_office_schedules,
            get_all_approval_paths_timesheet,
            get_all_approval_paths_timeoff,
            get_all_policy_sets,
            get_all_holiday_calendars,
            get_all_payrule_scripts,
            get_all_currencies,
            get_timesheet_period_list,
            get_all_activities,
            get_all_timeoff_validation_scripts,
            get_all_time_off_balance_scripts,
        ] >>\
        query_supervisors_from_feedfile >>\
        query_legal_employers_from_feed_file >> process_legal_employers >> start_new_groups_list1
        query_supervisors_from_feedfile >> query_locations_from_feed_file >> process_locations >> start_new_groups_list1
        query_supervisors_from_feedfile >> query_paygroups_from_feed_file >> process_paygroups >> start_new_groups_list1
        query_supervisors_from_feedfile >> query_departments_from_feed_file >> process_departments >> start_new_groups_list1
        query_supervisors_from_feedfile >> query_costcenters_from_feed_file >> process_costcenters >> start_new_groups_list1
        query_supervisors_from_feedfile >> query_business_units_from_feed_file >> process_business_units >> start_new_groups_list1
        query_supervisors_from_feedfile >> query_states_from_feed_file >> process_states >> start_new_groups_list1
        query_supervisors_from_feedfile >> query_cities_from_feed_file >> process_cities >> start_new_groups_list1
        query_supervisors_from_feedfile >> query_coefficient_levels_from_feed_file >> \
        if_coeffcient_level_data >> rail.Label("Yes") >> process_coefficient_levels >> start_new_groups_list1
        query_supervisors_from_feedfile >> query_elderly_allowance_from_feed_file >>\
        if_elderly_allowance_data >> rail.Label("Yes") >>\
        process_elderly_allowance >> start_new_groups_list1
        query_supervisors_from_feedfile >> query_timecode_from_feed_file >>\
        if_timecode_data >> rail.Label("Yes") >> process_timecode >> start_new_groups_list1
        query_supervisors_from_feedfile >> query_cba_appendix_from_feed_file >>\
        if_cba_appendix_data >> rail.Label("Yes") >> process_cba_appendix >> start_new_groups_list1
        query_supervisors_from_feedfile >> query_tariff_classification_from_feed_file >>\
        if_tariff_classification_data >> rail.Label("Yes") >>\
        process_tariff_classification >> start_new_groups_list1
        query_supervisors_from_feedfile >> query_step_info_from_feed_file >>\
        if_step_info >> rail.Label("Yes") >> process_step_information >> start_new_groups_list1 >>\
            [
            get_employee_type_paygroups,
            get_department_group,
            get_finance_cost_centers,
            get_location_schedule,
            get_legal_employers,
            get_business_units,
            get_admin_modified_custom_field_dropdown,
            get_employee_type_custom_field_dropdown,
            get_state_custom_field_dropdown,
            get_city_custom_field_dropdown,
            get_manufacturing_custom_field_dropdown,
            get_coefficient_level_custom_field_dropdown,
            get_elderly_allowance_custom_field_dropdown,
            get_apprentice_custom_field_dropdown,
            get_timecode_custom_field_dropdown,
            get_cba_appendix_custom_field_dropdown,
            get_tariff_employee_custom_field_dropdown,
            get_tariff_classification_custom_field_dropdown,
            get_step_information_custom_field_dropdown,
            get_work_leader_custom_field_dropdown,
        ] >> end_new_groups_list2 >>\
        process_valid_users >> get_pending_supervisor >> \
        if_pending_supervisor >> rail.Label("No") >> start_log_generation >> process_log_generation
        if_pending_supervisor >> rail.Label("Yes") >> start_supervisor_assignment >> process_supervisor_assignment >>\
            process_log_generation >>\
            log_to_sumo >> can_fail_dag >> fail_dag
        download_file >> was_new_file_found >> rail.Label(
            "Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_dagrun
        return dag


rail.for_each_instance(create_master_dag)
