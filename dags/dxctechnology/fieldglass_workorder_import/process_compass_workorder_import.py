import ast
from datetime import timedelta
from pendulum import now
from dxctechnology.fieldglass_workorder_import.utils import custom_methods
import rail

# pylint: disable=too-many-statements

null = None
def create_child_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.compass_workorder_import_dag_id,
        description="dxtechnology compass workorder import",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=config.master_max_active_run,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_config")

        query_records_with_null_for_mandatory_fields = rail.QueryCollectionOperator(
            task_id="query_records_with_null_for_mandatory_fields",
            query="""SELECT * FROM fieldglass_workorder_import_collection_compass WHERE NULLIF("WorkOrderID","") IS NULL OR
                        NULLIF("ContingentWorkerID", "") IS NULL OR NULLIF("WorkOrderStartDate","") IS NULL OR
                        NULLIF("WorkOrderEndDate","") IS NULL OR NULLIF("WorkOrderStatus","") IS NULL OR
                        NULLIF("WorkerFirstName","") IS NULL OR NULLIF("WorkerLastName","") IS NULL OR
                        NULLIF("CostCenterCode","") IS NULL OR NULLIF("BillRateCategory","") IS NULL OR
                        NULLIF("BillRate","") IS NULL OR NULLIF("WO_workertype","") IS NULL OR NULLIF("FinanceSystem","") IS NULL OR
                        NULLIF("RemainingSpend","") IS NULL OR NULLIF("cc_CompanyCode","") IS NULL"""
        )

        if_records_with_null = rail.IfOperator(
            task_id="if_records_with_null",
            test='{{result("query_records_with_null_for_mandatory_fields", "length") > 0}}',
            yes_task="write_contigent_workerid_not_present_log",
            no_task="query_records_without_null_for_mandatory_fields"
        )

        write_contigent_workerid_not_present_log = rail.WriteLogOperator(
            task_id="write_contigent_workerid_not_present_log",
            log='{{dag_run.conf.lookuptable}}',
            severity="Ignored",
            message="Contingentworkorderid is missing",
            items='{{result("query_records_with_null_for_mandatory_fields")}}',
            properties=lambda item: {
                "WorkOrderID": item["WorkOrderID"],
                "ContingentWorkerID": item["ContingentWorkerID"],
                "status": "Ignored",
                "details": "WorkOrderID/ContingentWorkerID/WorkOrderStartDate/WorkOrderEndDate/WorkOrderStatus/\
                            WorkerFirstName/WorkerLastName/CostCenterCode/BillRateCategory/\
                            BillRate/RateUnit/WO_WorkerType/FinanceSystem/cc_CompanyCode\
                            value is missing ",
                "Action": "Workorder_update"
            }
        )

        query_records_without_null_for_mandatory_fields = rail.QueryCollectionOperator(
            task_id="query_records_without_null_for_mandatory_fields",
            query="""SELECT * FROM fieldglass_workorder_import_collection_compass WHERE NULLIF("WorkOrderID","") IS NOT NULL AND
                        NULLIF("ContingentWorkerID", "") IS NOT NULL AND NULLIF("WorkOrderStartDate","") IS NOT NULL AND
                        NULLIF("WorkOrderEndDate","") IS NOT NULL AND NULLIF("WorkOrderStatus","") IS NOT NULL AND
                        NULLIF("WorkerFirstName","") IS NOT NULL AND NULLIF("WorkerLastName","") IS NOT NULL AND
                        NULLIF("CostCenterCode","") IS NOT NULL AND NULLIF("BillRateCategory","") IS NOT NULL AND
                        NULLIF("BillRate","") IS NOT NULL AND NULLIF("WO_workertype","") IS NOT NULL AND NULLIF("FinanceSystem","") IS NOT NULL AND
                        NULLIF("RemainingSpend","") IS NOT NULL AND NULLIF("cc_CompanyCode","") IS NOT NULL""",
            name="actualuserrecords"
        )

        if_records_without_null = rail.IfOperator(
            task_id="if_records_without_null",
            test='{{result("query_records_without_null_for_mandatory_fields", "length") > 0}}',
            yes_task="get_all_reports",
            no_task="compose_log_csv"
        )

        get_all_reports = rail.RepliconServiceOperator(
            task_id="get_all_reports",
            endpoint="/services/ReportService1.svc/GetAllReports",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response,
                "displayText",
                config.user_list_report,
                "uri"
            )
        )

        run_report_batch_entry, run_report_batch_exit = rail.run_report(
            group_id="run_user_list_report",
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('get_all_reports')}}"
                    }
                ]
            }
        )

        if_payload_has_data = rail.IfOperator(
            task_id="if_payload_has_data",
            test='{{result("run_user_list_report.get_report_result", "has_data")}}',
            yes_task="has_valid_columns",
            no_task="fail_no_report_data"
        )

        fail_no_report_data = rail.FailOperator(
            task_id="fail_no_report_data",
            message="No data in the base report",
        )

        has_valid_columns = rail.IfOperator(
            task_id="has_valid_columns",
            # pylint: disable=consider-using-f-string,line-too-long
            test="{{result('run_user_list_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s')}}" % config.expected_report_columns,
            no_task='fail_invalid_report_columns',
            yes_task='report_payload_to_csv',
        )

        fail_invalid_report_columns = rail.FailOperator(
            task_id="fail_invalid_report_columns",
            message="Base report column does not match"
        )

        report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id='report_payload_to_csv',
            document="{{ result('run_user_list_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_user_list_collection = rail.CreateCollectionOperator(
            task_id="create_user_list_collection",
            source='{{result("report_payload_to_csv")}}',
            name="reportdata",
            columns={
                "User Name": "username",
                "Login Name": "loginname",
                "Employee ID": "employeeid",
                "CWF C1 alternate ID": "cwfc1alternateid",
                "UserUri": "useruri",
                "User Status": "status",
                "Employee Type (Current)": "employeetype",
                "Timesheet Template": "timesheettemplate",
                "Timesheet Approval Path": "timesheetapprovalpath",
                "Work Week": "workweek",
                "validation": "validation",
                "C1 Purchase Order": "c1purchaseorder",
                "Work Order ID": "WorkOrderID",
                "Timesheet Period (Current)": "timesheetperiod",
                "Rate Unit": "rateunit",
                "Time Entry Approval Path": "timeentryapprovalpath"
            }
        )

        query_compass_all_report_data = rail.QueryCollectionOperator(
            task_id="query_compass_all_report_data",
            query="""SELECT * FROM reportdata"""
        )

        get_all_compass_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_compass_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets"
        )

        get_all_compass_custom_fields = rail.RepliconServiceOperator(
            task_id="get_all_compass_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                    "objectUri": "urn:replicon:object-type:user"
            }
        )

        get_all_object_extension_fields = rail.RepliconServiceOperator(
            task_id="get_all_object_extension_fields",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                    "bindingContextUri": "urn:replicon:object-type:project"
            }
        )

        get_all_enabled_divisions = rail.RepliconServiceOperator(
            task_id="get_all_enabled_divisions",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response,
                "displayText",
                "COMPASS",
                "uri"
            )
        )

        if_compass_enabled_division = rail.IfOperator(
            task_id="if_compass_enabled_division",
            test='{{result("get_all_enabled_divisions") | is_truthy}}',
            yes_task="get_child_hierarchy_of_compass",
            no_task="compose_merged_data_csv"
        )

        get_child_hierarchy_of_compass = rail.RepliconServiceOperator(
            task_id="get_child_hierarchy_of_compass",
            endpoint="/services/DivisionListService1.svc/GetChildHierarchyData",
            data={
                    "page": "1",
                    "pagesize": "1000",
                    "columnUris": [
                        "urn:replicon:division-list-column:name",
                        "urn:replicon:division-list-column:division"
                    ],
                "parentUri": '{{result("get_all_enabled_divisions")}}'
            },
            data_handler=lambda response: list(map(lambda i: {
                "textValue": i["cells"][0]["textValue"],
                "uri": i["cells"][1]["uri"]
            }, response["rows"])) if response else null
        )

        compass_child_hierarchy = rail.SetVariableOperator(
            task_id="compass_child_hierarchy",
            name="compass_child_heirarchy_data_list",
            value=[],
            append=True
        )

        for_each_child_get_hierarchy = rail.ForEachOperator(
            task_id="for_each_child_get_hierarchy",
            items='{{result("get_child_hierarchy_of_compass")|to_json}}',
            start_task="get_child_hierarchy_of_compass_child",
            end_task="end_compass_child_hierarchy"
        )

        get_child_hierarchy_of_compass_child = rail.RepliconServiceOperator(
            task_id="get_child_hierarchy_of_compass_child",
            endpoint="/services/DivisionListService1.svc/GetChildHierarchyData",
            data=lambda:{
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                        "urn:replicon:division-list-column:name",
                        "urn:replicon:division-list-column:division"
                ],
                "parentUri": rail.result("for_each_child_get_hierarchy")["uri"]
            },
            data_handler=lambda response: list(map(lambda i: {
                "textValue": i["cells"][0]["textValue"],
                "uri": i["cells"][1]["uri"]
            }, response["rows"])) if response else null
        )

        update_compass_child_hierarchy_list = rail.SetVariableOperator(
            task_id="update_compass_child_hierachy_list",
            name='{{result("compass_child_hierarchy").name}}',
            value=['{{result("get_child_hierarchy_of_compass_child")}}'],
            append=True
        )

        end_compass_child_hierarchy = rail.EmptyOperator(
            task_id="end_compass_child_hierarchy")

        query_pseudo_contractor_users = rail.QueryCollectionOperator(
            task_id="query_pseudo_contractor_users",
            query="""SELECT * FROM actualuserrecords WHERE RateUnit != 'Hr' """
        )

        if_pseudo_contractor_users = rail.IfOperator(
            task_id="if_pseudo_contractor_users",
            test='{{result("query_pseudo_contractor_users", "length") > 0}}',
            yes_task="get_all_employee_type_groups",
            no_task="compose_merged_data_csv"
        )

        get_all_employee_type_groups = rail.RepliconServiceOperator(
            task_id="get_all_employee_type_groups",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response,
                "displayText",
                "Leveraged Non-Hrly AC",
                "uri"
            )
        )

        process_pseudo_contract_users = rail.trigger_parallel_dagrun(
            task_id="process_pseudo_contract_users",
            items='{{result("query_pseudo_contractor_users")}}',
            parallel_count=config.max_active_child_runs,
            trigger_dag_id=config.compass_pseudo_constractor_update_dag_id,
            execution_timeout=timedelta(days=14),
            conf=custom_methods.get_pseudo_contract_conf
        )

        compose_merged_data_csv = rail.DataAdaptorOperator(
            task_id="compose_merged_data_csv",
            source='{{result("query_records_without_null_for_mandatory_fields")}}',
            columns=[
                "WorkOrderID",
                "RevisionNumber",
                "ContingentWorkerID",
                "WorkOrderStartDate",
                "WorkOrderEndDate",
                "WorkOrderStatus",
                "WorkerFirstName",
                "WorkerLastName",
                "CostCenterCode",
                "BillRateCategory",
                "BillRate",
                "RateUnit",
                "SiteCountry_usewithWorkerbasedreport",
                "TaskCode",
                "WO_CATW",
                "WO_WorkerType",
                "FinanceSystem",
                "RemainingSpend",
                "cc_CompanyCode",
                "useruri",
                "timesheettemplate",
                "timesheetapprovalpath",
                "workweek",
                "Timesheeettemplateuri",
                "WorkOrderID_Customfielduri",
                "CWF_C1_alternateID_customfielduri",
                "actual_costcenter_value",
                "userstatus",
                "employeetype",
                "usercount",
                "loginname",
                "timesheettemplatetoassign",
                "workweektoassign",
                "timesheetperiod",
                "WorkOrderID_assigned",
                "WorkOrderID_projectoef_uri",
                "remainingspend_projectoef_uri",
                "cwf_agency_wbs_customfielduri"
            ],
            data=custom_methods.get_compass_merged_data
        )

        create_merged_user_and_input_collection = rail.CreateCollectionOperator(
            task_id="create_merged_user_and_input_collection",
            source='{{result("compose_merged_data_csv")}}',
            name="merged_report_and_input_compass"
        )

        query_records_without_users_in_replicon = rail.QueryCollectionOperator(
            task_id="query_records_without_users_in_replicon",
            query="""SELECT * FROM merged_report_and_input_compass WHERE usercount != 1"""
        )

        if_records_without_users = rail.IfOperator(
            task_id="if_records_without_users",
            test='{{result("query_records_without_users_in_replicon", "length")>0}}',
            yes_task="write_records_without_users_log",
            no_task="query_records_with_users_in_replicon"
        )

        write_records_without_users_log = rail.WriteLogOperator(
            task_id="write_records_without_users_log",
            log='{{dag_run.conf.lookuptable}}',
            severity="Ignored",
            message="No Record/Multiple user records found in replicon",
            items='{{result("query_records_without_users_in_replicon")}}',
            properties=lambda item: {
                "WorkOrderID": item["WorkOrderID"],
                "ContingentWorkerID": item["ContingentWorkerID"],
                "status": "Ignored",
                "details": "Employee is not present in Replicon" if (item["usercount"] == 0 or item["usercount"] == "0") else "Multiple employee record found in Replicon",
                "Action": "User_attributes_update"
            }
        )

        query_records_with_users_in_replicon = rail.QueryCollectionOperator(
            task_id="query_records_with_users_in_replicon",
            query="""SELECT * FROM merged_report_and_input_compass WHERE usercount=1 """
        )

        query_valid_users_with_unique_loginname = rail.QueryCollectionOperator(
            task_id="query_valid_users_with_unique_loginname",
            query="""SELECT * FROM merged_report_and_input_compass WHERE useruri IN
              ( SELECT  DISTINCT useruri FROM merged_report_and_input_compass WHERE usercount = 1) GROUP BY useruri """
        )

        get_variable_list_data = rail.GetVariableOperator(
            task_id='get_variable_list_data',
            name='compass_child_heirarchy_data_list',
        )

        if_valid_users = rail.IfOperator(
            task_id="if_valid_users",
            test='{{result("query_valid_users_with_unique_loginname", "length")>0}}',
            yes_task="start_user_update",
            no_task="compose_log_csv"
        )

        start_user_update = rail.EmptyOperator(task_id="start_user_update")

        def get_user_conf(dag_run,item):
            cost_centers = []
            for i in rail.result("get_variable_list_data")["value"]:
                if i:
                    cost_centers.extend(ast.literal_eval(i[0]))
            return {
                **item,
                "Timesheeettemplateuri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_compass_policy_sets"),
                    "displayText",
                    item["timesheettemplatetoassign"],
                    "uri"
                ),
                "costcenteruri": rail.find_first_by_attr_and_get_attr(
                    cost_centers,
                    "textValue",
                    item["actual_costcenter_value"],
                    "uri"
                ),
                "lookuptable": dag_run.conf["lookuptable"]
            }

        update_user_details = rail.trigger_parallel_dagrun(
            task_id="update_user_details",
            items='{{result("query_valid_users_with_unique_loginname")}}',
            trigger_dag_id=config.update_compass_user_details_dag_id,
            conf=get_user_conf,
            parallel_count=config.max_active_child_runs,
            execution_timeout=timedelta(days=14)
        )

        update_purchase_order_blob_details = rail.trigger_parallel_dagrun(
            task_id="update_purchase_order_blob_details",
            items='{{result("query_valid_users_with_unique_loginname")}}',
            trigger_dag_id=config.update_compass_purchase_order_blob_dag_id,
            conf=lambda dag_run,item: {
                **item,
                "key": item["loginname"],
                "lookuptable": dag_run.conf["lookuptable"]
            },
            parallel_count=config.max_active_child_runs,
            execution_timeout=timedelta(days=14)
        )

        compose_log_csv = rail.WriteCSVFileOperator(
            task_id="compose_log_csv",
            source='{{dag_run.conf.lookuptable}}',
            header=["Action", "WorkOrderID", "ContingentWorkerID",
                    "WO_GHRPersonnelNumber", "Status", "Details", "ecid"],
            row=[

                '{{ item.properties | attr_or_default("Action", "") }}',
                '{{ item.properties | attr_or_default("WorkOrderID", "") }}',
                '{{ item.properties | attr_or_default("ContingentWorkerID", "") }}',
                '{{ item.properties | attr_or_default("WO_GHRPersonnelNumber", "") }}',
                '{{ item.properties | attr_or_default("status", "") }}',
                '{{ item.properties | attr_or_default("details", "") }}',
                '{{ item | attr_or_default("ecid", "") }}',
            ]
        )

        get_log_file_name = rail.PythonOperator(
            task_id="get_log_file_name",
            python_callable=lambda dag_run: dag_run.conf["file_name"].split(
                ".")[0] + "_logfile" + now().strftime("%m%d%Y%H%M%S") + ".csv"
        )

        upload_log_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_file_to_sftp",
            content='{{result("compose_log_csv")}}',
            remote_filepath=config.sftp_log_filepath +
            '{{result("get_log_file_name")}}'
        )

        if_valid_records = rail.IfOperator(
            task_id="if_valid_records",
            test='{{result("query_records_without_null_for_mandatory_fields", "length") > 0}}',
            yes_task="filter_error_logs",
            no_task="send_no_valid_records_mail"
        )

        filter_error_logs = rail.FilterLogEntriesOperator(
            task_id="filter_error_logs",
            severity="Error",
            log='{{dag_run.conf.lookuptable}}'
        )

        send_compass_import_mail = rail.EmailOperator(
            task_id="send_compass_import_mail",
            to=config.tenant_mail,
            bcc="{%- if result('filter_error_logs', 'length') > 0 -%}\
                "+config.alert_mail+"\
            {%- else -%}\
                "+config.internal_logs_email+"\
            {%- endif -%}",
            subject="{{get_company_key()}} "+"| compass Work order data sync to Replicon completed " +
            '{% if result("filter_error_logs", "length") > 0%}\
                with errors.\
                {%endif%}' +
            '{{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/import_mail.html",
            params={
                "path": config.sftp_log_filepath,
                "workorder": "Compass"
            }
        )

        send_no_valid_records_mail = rail.EmailOperator(
            task_id="send_no_valid_records_mail",
            to=config.tenant_mail,
            bcc=config.internal_logs_email,
            subject="{{get_company_key()}}" + " | compass Work order data sync to Replicon - No valid records in file - " +
            '{{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/no_valid_import_records.html",
            params={
                "path": config.sftp_log_filepath,
                "workorder": "Compass"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        query_records_with_null_for_mandatory_fields >>\
        if_records_with_null >> rail.Label("Yes") >>\
        write_contigent_workerid_not_present_log >> query_records_without_null_for_mandatory_fields
        if_records_with_null >> rail.Label("No") >>\
        query_records_without_null_for_mandatory_fields >>\
        if_records_without_null >> rail.Label("Yes") >>\
        get_all_reports >>\
        run_report_batch_entry >> run_report_batch_exit >>\
        if_payload_has_data >> rail.Label("Yes") >>\
        has_valid_columns >> rail.Label("Yes") >>\
        report_payload_to_csv >> create_user_list_collection >>\
        query_compass_all_report_data >>\
        [get_all_compass_policy_sets, get_all_compass_custom_fields, get_all_object_extension_fields, get_all_enabled_divisions] >>\
        if_compass_enabled_division >> rail.Label("Yes") >>\
        get_child_hierarchy_of_compass >>\
        compass_child_hierarchy >> for_each_child_get_hierarchy >> end_compass_child_hierarchy
        for_each_child_get_hierarchy >> get_child_hierarchy_of_compass_child >>\
        update_compass_child_hierarchy_list >> end_compass_child_hierarchy >>\
        query_pseudo_contractor_users >> if_pseudo_contractor_users >> rail.Label("Yes") >>\
        get_all_employee_type_groups >> process_pseudo_contract_users >> compose_merged_data_csv
        if_pseudo_contractor_users >> rail.Label("No") >>\
        compose_merged_data_csv
        if_compass_enabled_division >> rail.Label("No") >>\
        compose_merged_data_csv >> create_merged_user_and_input_collection >>\
        query_records_without_users_in_replicon >>\
        if_records_without_users >> rail.Label("Yes") >>\
        write_records_without_users_log >> query_records_with_users_in_replicon
        if_records_without_users >> rail.Label("No") >>\
        query_records_with_users_in_replicon >> query_valid_users_with_unique_loginname >>\
        get_variable_list_data >> \
        if_valid_users >> rail.Label("Yes") >> start_user_update >>\
        update_user_details >> update_purchase_order_blob_details >>\
        compose_log_csv >> get_log_file_name >> upload_log_file_to_sftp >>\
        if_valid_records >> rail.Label("Yes") >> filter_error_logs >>\
        send_compass_import_mail >> log_to_sumo
        if_valid_users >> rail.Label("No") >> compose_log_csv
        if_valid_records >> rail.Label(
        "No") >> send_no_valid_records_mail >> log_to_sumo
        has_valid_columns >> rail.Label("No") >>\
        fail_invalid_report_columns
        if_payload_has_data >> rail.Label("No") >>\
        fail_no_report_data
        if_records_without_null >> rail.Label("No") >>\
        compose_log_csv
        return dag


rail.for_each_instance(create_child_airflow_dag)
