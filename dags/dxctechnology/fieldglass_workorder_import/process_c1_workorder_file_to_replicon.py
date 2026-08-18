from datetime import timedelta
from dxctechnology.fieldglass_workorder_import.utils import custom_methods
from airflow.models import Variable
import rail

# pylint: disable=too-many-statements
def create_airflow_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.c1_file_to_replicon_dag_id,
        description="dxctechnology fieldglass c1,compass workorder import",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_config")
        dxctechnology_c1_workorder_log = rail.CreateLogOperator(
            task_id="dxctechnology_c1_workorder_log"
        )

        download_import_file = rail.SFTPDownloadFileOperator(
            task_id="download_import_file",
            remote_filepath='{{dag_run.conf.file_path}}'
        )

        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            test='{{get_task_state("download_import_file") == "success"}}',
            yes_task="archive_import_file",
            no_task="delete_dagrun"
        )

        archive_import_file = rail.SFTPMoveFileOperator(
            task_id="archive_import_file",
            new_filename=config.sftp_archive_filepath +
            '{{dag_run_ecid()|replace(":","-")}}_'+'{{dag_run.conf.file_path|file_name}}',
            existing_filename='{{dag_run.conf.file_path}}',
            trigger_rule="all_done"
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dagrun"
        )

        can_decrypt_file = rail.IfOperator(
            task_id ="can_decrypt_file",
            test=lambda: Variable.get(config.can_decrypt_file_var_name, default_var='true').lower() == 'true',
            yes_task='decrypt_feed_file',
            no_task='dummy_load_data'
        )

        decrypt_feed_file = rail.PGPDecryptionOperator(
            task_id="decrypt_feed_file",
            source='{{result("download_import_file")}}',
            pgp_conn_id=config.pgp_conn_id
        )

        dummy_load_data = rail.PythonOperator(
            task_id= "dummy_load_data",
            python_callable= lambda: rail.result('decrypt_feed_file') if Variable.get(
                config.can_decrypt_file_var_name, default_var='true').lower()== 'true' else  rail.result('download_import_file'),
            show_return_value_in_logs= False
        )

        load_import_csv = rail.LoadCSVFileOperator(
            task_id="load_import_csv",
            document='{{result("dummy_load_data")}}',
            delimiter="|",
            headers=[
                "WorkOrderID",
                "RevisionNumber",
                "ContingentWorkerID",
                "WorkOrderStartDate",
                "WorkOrderEndDate",
                "WorkOrderStatus",
                "WorkerFirstName",
                "WorkerLastName",
                "CostCenterCode",
                "CostCenterName",
                "BillRateCategory",
                "RateUnit",
                "SiteCountry_usewithWorkerbasedreport",
                "TaskCode",
                "WO_GHRPersonnelNumber",
                "FinanceSystem",
                "integration_resend_flag",
            ]
        )

        create_import_data_collection = rail.CreateCollectionOperator(
            task_id="create_import_data_collection",
            source='{{result("load_import_csv")}}',
            name="fieldglass_workorder_import_collection_c1"
        )

        if_import_data = rail.IfOperator(
            task_id="if_import_data",
            test='{{result("create_import_data_collection", "length") > 0}}',
            yes_task="query_contigent_workerid_not_present",
            no_task="send_no_records_mail"
        )

        send_no_records_mail = rail.EmailOperator(
            task_id="send_no_records_mail",
            to=config.tenant_mail,
            bcc=config.internal_logs_email,
            subject="{{get_company_key()}} "+"| C1 Work order data sync to Replicon - No records in file   -  " +
            '{{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/norecords_file.html",
            params={
                "erp": 'C1'
            }
        )

        query_contigent_workerid_not_present = rail.QueryCollectionOperator(
            task_id="query_contigent_workerid_not_present",
            query="""SELECT * FROM fieldglass_workorder_import_collection_c1 WHERE WorkOrderID IN
                (SELECT DISTINCT WorkOrderID FROM fieldglass_workorder_import_collection_c1 WHERE
                NULLIF(ContingentWorkerID,"") IS NULL) GROUP BY WorkOrderID """
        )

        if_contigent_workerid_not_present = rail.IfOperator(
            task_id="if_contigent_workerid_not_present",
            test='{{result("query_contigent_workerid_not_present", "length") > 0}}',
            yes_task="write_contigent_workerid_not_present_log",
            no_task="query_contigent_workerid_present"
        )

        write_contigent_workerid_not_present_log = rail.WriteLogOperator(
            task_id="write_contigent_workerid_not_present_log",
            log='{{result("dxctechnology_c1_workorder_log")}}',
            severity="Ignored",
            message="Contingentworkorderid is missing",
            items='{{result("query_contigent_workerid_not_present")}}',
            properties=lambda item: {
                "workorderid": item["WorkOrderID"],
                "ContingentWorkerID": item["ContingentWorkerID"],
                "WO_GHRPersonnelNumber": item["WO_GHRPersonnelNumber"],
                "status": "Ignored",
                "details": "Contingentworkorderid is missing",
                "Action": "User_attributes_update"
            }
        )

        query_contigent_workerid_present = rail.QueryCollectionOperator(
            task_id="query_contigent_workerid_present",
            query="""SELECT * FROM fieldglass_workorder_import_collection_c1 WHERE WorkOrderID NOT IN
                    ( SELECT DISTINCT WorkOrderID FROM fieldglass_workorder_import_collection_c1 WHERE
                     NULLIF(ContingentWorkerID,"") IS NULL) GROUP BY WorkOrderID"""
        )

        if_contigent_workerid_present = rail.IfOperator(
            task_id="if_contigent_workerid_present",
            test='{{result("query_contigent_workerid_present", "length") > 0}}',
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
            test='{{result("run_user_list_report.get_report_result","has_data")}}',
            yes_task="has_valid_columns",
            no_task="fail_no_report_data"
        )

        fail_no_report_data = rail.FailOperator(
            task_id="fail_no_report_data",
            message="No data in the base report",
        )

        has_valid_columns = rail.IfOperator(
            task_id="has_valid_columns",
            # pylint: disable=line-too-long
            test=lambda:bool(rail.result('run_user_list_report.get_report_result')
                             ["reportGenerationResults"][0]["payload"].startswith(
                                 config.expected_report_columns)),
            no_task='fail_invalid_report_columns',
            yes_task='report_payload_to_csv',
        )

        fail_invalid_report_columns = rail.FailOperator(
            task_id="fail_invalid_report_columns",
            message="Base report column does not match"
        )

        report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id='report_payload_to_csv',
            document="{{ result('run_user_list_report.get_report_result').reportGenerationResults[0].payload}}",
            headers=[
                "username",
                "loginname",
                "employeeid",
                "cwfc1alternateid",
                "useruri",
                "status",
                "employeetype",
                "timesheettemplate",
                "timesheetapprovalpath",
                "workweek",
                "validation",
                "c1purchaseorder",
                "workorderid",
                "timesheetperiod",
                "rateunit",
                "timesheetapprovalpath"
            ]
        )

        create_user_list_collection = rail.CreateCollectionOperator(
            task_id="create_user_list_collection",
            source='{{result("report_payload_to_csv")}}',
            name="reportdata"
        )

        query_all_report_data = rail.QueryCollectionOperator(
            task_id="query_all_report_data",
            query="""SELECT * FROM reportdata"""
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets"
        )

        get_all_custom_fields = rail.RepliconServiceOperator(
            task_id="get_all_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                    "objectUri": "urn:replicon:object-type:user"
            }
        )

        get_all_enabled_divisions = rail.RepliconServiceOperator(
            task_id="get_all_enabled_divisions",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response,
                "displayText",
                "C1",
                "uri"
            )
        )

        if_c1_enabled_division = rail.IfOperator(
            task_id="if_c1_enabled_division",
            test='{{result("get_all_enabled_divisions") | is_truthy}}',
            yes_task="get_child_hierarchy",
            no_task="compose_merged_data_csv"
        )

        get_child_hierarchy = rail.RepliconServiceOperator(
            task_id="get_child_hierarchy",
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
            source='{{result("query_contigent_workerid_present")}}',
            columns=[
                "workorderid",
                "contingentworkerid",
                "workorderstartdate",
                "workorderenddate",
                "workorderstatus",
                "workerfirstname",
                "workerlastname",
                "costcentercode",
                "costcentername",
                "WO_GHR_Personnel_Number",
                "Finance_System",
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
                "C1PurchaseOrder_value",
                "WorkOrderID_value",
                "CWFC1alternateID_value",
                "timesheetperiod_value",
                "cwf_agency_wbs_customfielduri"
            ],
            data=custom_methods.get_c1_merged_data
        )

        create_merged_user_and_input_collection = rail.CreateCollectionOperator(
            task_id="create_merged_user_and_input_collection",
            source='{{result("compose_merged_data_csv")}}',
            name="merged_report_and_input"
        )

        query_records_without_users_in_replicon = rail.QueryCollectionOperator(
            task_id="query_records_without_users_in_replicon",
            query="""SELECT * FROM merged_report_and_input WHERE usercount != 1"""
        )

        if_records_without_users = rail.IfOperator(
            task_id="if_records_without_users",
            test='{{result("query_records_without_users_in_replicon", "length")>0}}',
            yes_task="write_records_without_users_log",
            no_task="query_records_with_users_in_replicon"
        )

        write_records_without_users_log = rail.WriteLogOperator(
            task_id="write_records_without_users_log",
            log='{{result("dxctechnology_c1_workorder_log")}}',
            severity="Ignored",
            message="No Record/Multiple user records found in replicon",
            items='{{result("query_records_without_users_in_replicon")}}',
            properties=lambda item: {
                "workorderid": item["workorderid"],
                "ContingentWorkerID": item["contingentworkerid"],
                "WO_GHRPersonnelNumber": item["WO_GHR_Personnel_Number"],
                "status": "Ignored",
                "details": "Employee is not present in Replicon" if (item["usercount"] == 0 or item["usercount"] == "0") \
                    else "Multiple employee record found in Replicon",
                "Action": "User_attributes_update"
            }
        )

        query_records_with_users_in_replicon = rail.QueryCollectionOperator(
            task_id="query_records_with_users_in_replicon",
            query="""SELECT * FROM merged_report_and_input WHERE usercount=1 """
        )

        if_records_with_users = rail.IfOperator(
            task_id="if_records_with_users",
            test='{{result("query_records_with_users_in_replicon", "length")>0}}',
            yes_task="start_user_update",
            no_task="compose_log_csv"
        )
        start_user_update = rail.EmptyOperator(task_id="start_user_update")

        update_user_details = rail.trigger_parallel_dagrun(
            task_id="update_user_details",
            items='{{result("query_records_with_users_in_replicon")}}',
            trigger_dag_id=config.update_c1_user_details_dag_id,
            conf=lambda item: {
                **item,
                "costcenteruri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_child_hierarchy"),
                    "textValue",
                    item["actual_costcenter_value"],
                    "uri"
                ),
                "lookuptable": rail.result("dxctechnology_c1_workorder_log")
            },
            parallel_count=config.max_active_child_runs,
            execution_timeout=timedelta(days=14)
        )

        compose_log_csv = rail.WriteCSVFileOperator(
            task_id="compose_log_csv",
            source='{{result("dxctechnology_c1_workorder_log")}}',
            header=["Action", "WorkOrderID", "ContingentWorkerID",
                    "WO_GHRPersonnelNumber", "Status", "Details","ecid"],
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
            python_callable=lambda:rail.render_template('{{dag_run.conf.file_path|file_name}}') + "_logfile" +
            rail.render_template('{{ current_time_in_specified_tz(fmt="%m%d%Y%H%M%S")}}') + ".csv"
        )

        upload_log_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_file_to_sftp",
            content='{{result("compose_log_csv")}}',
            remote_filepath=config.sftp_log_filepath +'{{result("get_log_file_name")}}'
        )

        if_valid_records = rail.IfOperator(
            task_id="if_valid_records",
            test='{{result("query_contigent_workerid_present", "length") > 0}}',
            yes_task="filter_error_logs",
            no_task="send_no_valid_records_mail"
        )

        filter_error_logs = rail.FilterLogEntriesOperator(
            task_id="filter_error_logs",
            severity="Error",
            log='{{result("dxctechnology_c1_workorder_log")}}'
        )

        send_c1_import_mail = rail.EmailOperator(
            task_id="send_c1_import_mail",
            to=config.tenant_mail,
            bcc="{%- if result('filter_error_logs', 'length') > 0 -%}\
                "+config.alert_mail+"\
            {%- else -%}\
                "+config.internal_logs_email+"\
            {%- endif -%}",
            subject="{{get_company_key()}} "+"| C1 Work order data sync to Replicon completed " +
            '{% if result("filter_error_logs","length") > 0%}\
                with errors.\
                {%endif%}' +
            '{{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/import_mail.html",
            params={
                "path": config.sftp_log_filepath,
                "workorder": "C1"
            }
        )

        send_no_valid_records_mail = rail.EmailOperator(
            task_id="send_no_valid_records_mail",
            to=config.tenant_mail,
            bcc=config.internal_logs_email,
            subject="{{get_company_key()}}" + " | C1 Work order data sync to Replicon - No valid records in file - " +
            '{{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/no_valid_import_records.html",
            params={
                "path": config.sftp_log_filepath,
                "workorder": "C1"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        dxctechnology_c1_workorder_log >> \
        download_import_file >>\
            was_new_file_found >> rail.Label("Yes") >> archive_import_file
        was_new_file_found >> rail.Label("No") >> delete_dagrun
        download_import_file >> \
        can_decrypt_file >> rail.Label("No") >> dummy_load_data >> load_import_csv
        can_decrypt_file >> rail.Label("Yes") >>\
        decrypt_feed_file >> dummy_load_data >> load_import_csv >> create_import_data_collection >>\
        if_import_data >> rail.Label(
            "Yes") >>query_contigent_workerid_not_present >>\
            if_contigent_workerid_not_present >> rail.Label("Yes") >>\
            write_contigent_workerid_not_present_log >> query_contigent_workerid_present
        if_contigent_workerid_not_present >> rail.Label("No") >>\
            query_contigent_workerid_present >>\
            if_contigent_workerid_present >> rail.Label("Yes") >>\
            get_all_reports >>\
            run_report_batch_entry >> run_report_batch_exit >>\
            if_payload_has_data >> rail.Label("Yes") >>\
            has_valid_columns >> rail.Label("Yes") >>\
            report_payload_to_csv >> create_user_list_collection >>\
            query_all_report_data >>\
            [get_all_policy_sets, get_all_custom_fields, get_all_enabled_divisions] >>\
            if_c1_enabled_division >> rail.Label("Yes") >>\
            get_child_hierarchy >> compose_merged_data_csv
        if_c1_enabled_division >> rail.Label("No") >>\
            compose_merged_data_csv >> create_merged_user_and_input_collection >>\
            query_records_without_users_in_replicon >>\
            if_records_without_users >> rail.Label("Yes") >>\
            write_records_without_users_log >> query_records_with_users_in_replicon
        if_records_without_users >> rail.Label("No") >>\
            query_records_with_users_in_replicon >>\
        if_records_with_users >> rail.Label("No") >> compose_log_csv
        if_records_with_users >> rail.Label("Yes") >> start_user_update>>update_user_details >>\
            compose_log_csv >> get_log_file_name >> upload_log_file_to_sftp >>\
            if_valid_records >> rail.Label("Yes") >> filter_error_logs >>\
            send_c1_import_mail >> log_to_sumo
        if_valid_records >> rail.Label(
            "No") >> send_no_valid_records_mail >> log_to_sumo
        has_valid_columns >> rail.Label("No") >>\
            fail_invalid_report_columns
        if_payload_has_data >> rail.Label("No") >>\
            fail_no_report_data
        if_contigent_workerid_present >> rail.Label("No") >>\
            compose_log_csv  >> log_to_sumo
        if_import_data >> rail.Label(
            "No") >> send_no_records_mail >> log_to_sumo
        return dag


rail.for_each_instance(create_airflow_master_dag)
