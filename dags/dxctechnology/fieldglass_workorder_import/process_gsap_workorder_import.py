from datetime import timedelta
from pendulum import now
from dxctechnology.fieldglass_workorder_import.utils import custom_methods
import rail

# pylint: disable=too-many-statements


def create_child_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_fieldglass_workorder_gsap_import_child_{config.instance}",
        description="dxtechnology gsap workorder import",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=config.master_max_active_run,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_config")
        dxctechnology_gsap_workorder_log = rail.CreateLogOperator(
            task_id="dxctechnology_gsap_workorder_log"
        )

        query_records_with_null_for_mandatory_fields = rail.QueryCollectionOperator(
            task_id="query_records_with_null_for_mandatory_fields",
            query="""SELECT * FROM fieldglass_workorder_import_collection_gsap WHERE NULLIF("WorkOrderID","") IS NULL OR
                        NULLIF("ContingentWorkerID", "") IS NULL OR NULLIF("WorkOrderStartDate","") IS NULL OR
                        NULLIF("WorkOrderEndDate","") IS NULL OR NULLIF("WorkOrderStatus","") IS NULL OR
                        NULLIF("WorkerFirstName","") IS NULL OR NULLIF("WorkerLastName","") IS NULL OR
                        NULLIF("CostCenterCode","") IS NULL OR NULLIF("BillRateCategory","") IS NULL OR
                        NULLIF("BillRate","") IS NULL OR NULLIF("WO_WorkerType","") IS NULL OR NULLIF("FinanceSystem","") IS NULL OR
                        NULLIF("WO_GHRPersonnelNumber", "") IS NULL OR NULLIF("cc_CompanyCode","") IS NULL"""
        )

        if_records_with_null = rail.IfOperator(
            task_id="if_records_with_null",
            test='{{result("query_records_with_null_for_mandatory_fields", "length") > 0}}',
            yes_task="write_contigent_workerid_not_present_log",
            no_task="query_records_without_null_for_mandatory_fields"
        )

        write_contigent_workerid_not_present_log = rail.WriteLogOperator(
            task_id="write_contigent_workerid_not_present_log",
            log='{{result("dxctechnology_gsap_workorder_log")}}',
            severity="Ignored",
            message="Contingentworkorderid is missing",
            items='{{result("query_records_with_null_for_mandatory_fields")}}',
            properties=lambda item: {
                "workorderid": item["WorkOrderID"],
                "ContingentWorkerID": item["ContingentWorkerID"],
                "WO_GHRPersonnelNumber": item["WO_GHRPersonnelNumber"],
                "status": "Ignored",
                "details": custom_methods.check_gsap_workorder_attr(item),
                "Action": "Workorder_update"
            }
        )

        query_records_without_null_for_mandatory_fields = rail.QueryCollectionOperator(
            task_id="query_records_without_null_for_mandatory_fields",
            query="""SELECT * FROM fieldglass_workorder_import_collection_gsap WHERE NULLIF("WorkOrderID","") IS NOT NULL AND
                        NULLIF("ContingentWorkerID", "") IS NOT NULL AND NULLIF("WorkOrderStartDate","") IS NOT NULL AND
                        NULLIF("WorkOrderEndDate","") IS NOT NULL AND NULLIF("WorkOrderStatus","") IS NOT NULL AND
                        NULLIF("WorkerFirstName","") IS NOT NULL AND NULLIF("WorkerLastName","") IS NOT NULL AND
                        NULLIF("CostCenterCode","") IS NOT NULL AND NULLIF("BillRateCategory","") IS NOT NULL AND
                        NULLIF("BillRate","") IS NOT NULL AND NULLIF("WO_WorkerType","") IS NOT NULL AND NULLIF("FinanceSystem","") IS NOT NULL AND
                        NULLIF("WO_GHRPersonnelNumber", "") IS NOT NULL AND NULLIF("cc_CompanyCode","") IS NOT NULL""",
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            query="""SELECT * FROM fieldglass_workorder_import_collection_gsap WHERE
             FinanceSystem!="GSAP" OR  WO_WorkerType!="Etes" """
        )


        if_invalid_records = rail.IfOperator(
            task_id="if_invalid_records",
            test='{{result("query_invalid_records","length")>0}}',
            yes_task="write_invalid_records_log",
            no_task="query_records_with_billrate_category"
        )

        write_invalid_records_log = rail.WriteLogOperator(
            task_id="write_invalid_records_log",
            log='{{result("dxctechnology_gsap_workorder_log")}}',
            severity="Ignored",
            message="Contingentworkorderid is missing",
            items='{{result("query_invalid_records")}}',
            properties=lambda item: {
                "workorderid": item["WorkOrderID"],
                "ContingentWorkerID": item["ContingentWorkerID"],
                "WO_GHRPersonnelNumber": item["WO_GHRPersonnelNumber"],
                "status": "Ignored",
                "details": custom_methods.check_gsap_workorder_attr_invalid(item),
                "Action": "Workorder_update"
            }
        )

        query_records_with_billrate_category = rail.QueryCollectionOperator(
            task_id="query_records_with_billrate_category",
            query="""SELECT * FROM fieldglass_workorder_import_collection_gsap WHERE
             FinanceSystem="GSAP" AND WO_WorkerType="Etes" AND BillRateCategory IN ("ST", "DT", "OT")""",
            name="actualuserrecords"
        )

        if_records_with_billrate_category = rail.IfOperator(
            task_id="if_records_with_billrate_category",
            test='{{result("query_records_with_billrate_category", "length") > 0 and result("query_records_without_null_for_mandatory_fields", "length") > 0 }}',
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
                "Time Entry Approval Path": "timesheetapprovalpath"
            }
        )

        query_gsap_all_report_data = rail.QueryCollectionOperator(
            task_id="query_gsap_all_report_data",
            query="""SELECT * FROM reportdata"""
        )

        get_all_gsap_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_gsap_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets"
        )

        get_all_gsap_custom_fields = rail.RepliconServiceOperator(
            task_id="get_all_gsap_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                    "objectUri": "urn:replicon:object-type:user"
            }
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id="get_all_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        get_all_cost_centers = rail.RepliconServiceOperator(
            task_id="get_all_cost_centers",
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
        )

        get_all_enabled_divisions = rail.RepliconServiceOperator(
            task_id="get_all_enabled_divisions",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response,
                "displayText",
                "GSAP",
                "uri"
            )
        )

        if_gsap_enabled_division = rail.IfOperator(
            task_id="if_gsap_enabled_division",
            test='{{result("get_all_enabled_divisions") | is_truthy}}',
            yes_task="get_child_hierarchy_of_gsap",
            no_task="compose_merged_data_csv"
        )

        get_child_hierarchy_of_gsap = rail.RepliconServiceOperator(
            task_id="get_child_hierarchy_of_gsap",
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
            }, response["rows"]))
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
                "TaskCode",
                "WO_GHRPersonnelNumber",
                "WO_CATW",
                "WO_WorkerType",
                "FinanceSystem",
                "cc_CompanyCode",
                "useruri",
                "timesheettemplate",
                "Timesheeettemplateuri",
                "timesheetapprovalpath",
                "workweek",
                "WorkOrderID_Customfielduri",
                "CWF_C1_alternateID_customfielduri",
                "actual_company_code_value",
                "userstatus",
                "employeetype",
                "usercount",
                "loginname",
                "timesheettemplatetoassign",
                "workweektoassign",
                "timesheetperiod",
                "workorderid_assigned",
                "GHR_personnel_number",
            ],
            data=custom_methods.get_gsap_merged_data
        )

        create_merged_user_and_input_collection = rail.CreateCollectionOperator(
            task_id="create_merged_user_and_input_collection",
            source='{{result("compose_merged_data_csv")|load_all_records|to_json}}',
            name="merged_report_and_input_gsap"
        )

        query_records_without_users_in_replicon = rail.QueryCollectionOperator(
            task_id="query_records_without_users_in_replicon",
            query="""SELECT * FROM merged_report_and_input_gsap WHERE usercount != 1"""
        )

        if_records_without_users = rail.IfOperator(
            task_id="if_records_without_users",
            test='{{result("query_records_without_users_in_replicon", "length")>0}}',
            yes_task="write_records_without_users_log",
            no_task="query_records_with_users_in_replicon"
        )

        write_records_without_users_log = rail.WriteLogOperator(
            task_id="write_records_without_users_log",
            log='{{result("dxctechnology_gsap_workorder_log")}}',
            severity="Ignored",
            message="No Record/Multiple user records found in replicon",
            items='{{result("query_records_without_users_in_replicon")}}',
            properties=lambda item: {
                "workorderid": item["WorkOrderID"],
                "ContingentWorkerID": item["ContingentWorkerID"],
                "WO_GHRPersonnelNumber": item["WO_GHRPersonnelNumber"],
                "status": "Ignored",
                "details": "Employee is not present in Replicon" if (item["usercount"] == 0 or item["usercount"] == "0") else "Multiple employee record found in Replicon",
                "Action": "User_attributes_update"
            }
        )

        query_records_with_users_in_replicon = rail.QueryCollectionOperator(
            task_id="query_records_with_users_in_replicon",
            query="""SELECT * FROM merged_report_and_input_gsap WHERE usercount=1 """
        )

        query_valid_users_with_unique_loginname = rail.QueryCollectionOperator(
            task_id="query_valid_users_with_unique_loginname",
            query="""SELECT * FROM merged_report_and_input_gsap WHERE useruri IN
              ( SELECT  DISTINCT useruri FROM merged_report_and_input_gsap WHERE usercount = 1) GROUP BY useruri """
        )

        if_valid_users = rail.IfOperator(
            task_id="if_valid_users",
            test='{{result("query_valid_users_with_unique_loginname", "length")>0}}',
            yes_task="start_user_update",
            no_task="compose_log_csv"
        )

        start_user_update = rail.EmptyOperator(task_id="start_user_update")

        update_user_details = rail.trigger_parallel_dagrun(
            task_id="update_user_details",
            items='{{result("query_valid_users_with_unique_loginname")}}',
            trigger_dag_id=config.update_gsap_user_details_dag_id,
            conf=lambda item: {
                **item,
                "companycodeuri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_child_hierarchy_of_gsap"),
                    "textValue",
                    item["actual_company_code_value"],
                    "uri"
                ) if rail.result("get_child_hierarchy_of_gsap") else "",
                "contingentworkercontractorpermission": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_permission_sets"),
                    "displayText",
                    "Contingent Worker/Contractor",
                    "uri"
                ) if rail.result("get_child_hierarchy_of_gsap") else "",
                "costcenteruri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_cost_centers"),
                    "displayText",
                    item["CostCenterCode"],
                    "uri"
                ),
                "lookuptable": rail.result("dxctechnology_gsap_workorder_log")
            },
            parallel_count=config.max_active_child_runs,
            execution_timeout=timedelta(days=14)
        )

        update_purchase_order_blob_details = rail.trigger_parallel_dagrun(
            task_id="update_purchase_order_blob_details",
            items='{{result("query_valid_users_with_unique_loginname")}}',
            trigger_dag_id=config.update_gsap_purchase_order_blob_dag_id,
            conf=lambda item: {
                **item,
                "key": item["useruri"],
                "lookuptable": rail.result("dxctechnology_gsap_workorder_log")
            },
            parallel_count=config.max_active_child_runs,
            execution_timeout=timedelta(days=14)
        )

        compose_log_csv = rail.WriteCSVFileOperator(
            task_id="compose_log_csv",
            source='{{result("dxctechnology_gsap_workorder_log")}}',
            header=["Action", "WorkOrderID", "ContingentWorkerID",
                    "WO_GHRPersonnelNumber", "Status", "Details", "ecid"],
            row=[

                '{{ item.properties | attr_or_default("Action", "") }}',
                '{{ item.properties | attr_or_default("workorderid", "") }}',
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
            log='{{result("dxctechnology_gsap_workorder_log")}}'
        )

        send_gsap_import_mail = rail.EmailOperator(
            task_id="send_gsap_import_mail",
            to=config.tenant_mail,
            bcc="{%- if result('filter_error_logs', 'length') > 0 -%}\
                "+config.alert_mail+"\
            {%- else -%}\
                "+config.internal_logs_email+"\
            {%- endif -%}",
            subject="{{get_company_key()}} "+"| GSAP Work order data sync to Replicon completed " +
            '{% if result("filter_error_logs", " length") > 0%}\
                with errors.\
                {%endif%}' +
            '{{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/import_mail.html",
            params={
                "path": config.sftp_log_filepath,
                "workorder": "GSAP"
            }
        )

        send_no_valid_records_mail = rail.EmailOperator(
            task_id="send_no_valid_records_mail",
            to=config.tenant_mail,
            bcc=config.internal_logs_email,
            subject="{{get_company_key()}}" + " | GSAP Work order data sync to Replicon - No valid records in file - " +
            '{{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/no_valid_import_records.html",
            params={
                "path": config.sftp_log_filepath,
                "workorder": "GSAP"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        dxctechnology_gsap_workorder_log >> \
            query_records_with_null_for_mandatory_fields >>\
            if_records_with_null >> rail.Label("Yes") >>\
            write_contigent_workerid_not_present_log >> query_records_without_null_for_mandatory_fields
        if_records_with_null >> rail.Label("No") >>\
            query_records_without_null_for_mandatory_fields >>\
            query_invalid_records >>\
        if_invalid_records >> rail.Label("No") >> query_records_with_billrate_category
        if_invalid_records >> rail.Label("Yes") >>\
        write_invalid_records_log >> query_records_with_billrate_category >>\
            if_records_with_billrate_category >> rail.Label("Yes") >>\
            get_all_reports >>\
            run_report_batch_entry >> run_report_batch_exit >>\
            if_payload_has_data >> rail.Label("Yes") >>\
            has_valid_columns >> rail.Label("Yes") >>\
            report_payload_to_csv >> create_user_list_collection >>\
            query_gsap_all_report_data >>\
            [get_all_gsap_policy_sets, get_all_gsap_custom_fields, get_all_cost_centers, get_all_enabled_divisions, get_all_permission_sets] >>\
            if_gsap_enabled_division >> rail.Label("Yes") >>\
            get_child_hierarchy_of_gsap >>\
            compose_merged_data_csv
        if_gsap_enabled_division >> rail.Label("No") >>\
            compose_merged_data_csv >> create_merged_user_and_input_collection >>\
            query_records_without_users_in_replicon >>\
            if_records_without_users >> rail.Label("Yes") >>\
            write_records_without_users_log >> query_records_with_users_in_replicon
        if_records_without_users >> rail.Label("No") >>\
            query_records_with_users_in_replicon >> query_valid_users_with_unique_loginname >>\
        if_valid_users >> rail.Label("Yes") >> start_user_update >>\
            update_user_details >> update_purchase_order_blob_details >>\
            compose_log_csv >> get_log_file_name >> upload_log_file_to_sftp >>\
            if_valid_records >> rail.Label("Yes") >> filter_error_logs >>\
            send_gsap_import_mail >> log_to_sumo
        if_valid_users >> rail.Label("No") >> compose_log_csv
        if_valid_records >> rail.Label(
            "No") >> send_no_valid_records_mail >> log_to_sumo
        has_valid_columns >> rail.Label("No") >>\
            fail_invalid_report_columns
        if_payload_has_data >> rail.Label("No") >>\
            fail_no_report_data
        if_records_with_billrate_category >> rail.Label("No") >>\
            compose_log_csv
        return dag


rail.for_each_instance(create_child_airflow_dag)
