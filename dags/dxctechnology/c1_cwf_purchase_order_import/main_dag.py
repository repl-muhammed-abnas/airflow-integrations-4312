from datetime import timedelta, datetime
import os
import rail
from dxctechnology.c1_cwf_purchase_order_import.tasks.send_logs import get_send_logs
from dxctechnology.c1_cwf_purchase_order_import.utils import custom_method, request_payload

# config: https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_cwf_purchase_order_import/config.py


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_c1_cwf_purchase_order_import_master_{config.instance}",
        description=f"DXCTechnology C1 CWF Purchase order import Master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.max_active_runs_master
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10),
            # We do the timeout with a soft fail here to yield to make sure this dag cycles once in a while so that transient network
            # failures have less of a chance of causing the dag to fail, and people to get notified. If this dag ran indefinitely
            # then 3 network failures several days apart would cause alerts to
            # be sent out, which really is not necessary.
        )
        is_xml = rail.IfOperator(
            task_id='is_xml',
            test='{{ result("new_file_sensor") | file_ext | lower == "xml" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id="send_bad_file_format_email",
            to=config.tenant_email,
            subject="{{get_company_key()}}" +
            " | C1 CWF Purchaseorder balance sync to Replicon - Invalid file extension - " +
            "{{current_time('%Y-%m-%dT%H:%M:%S.%f%z')}}",
            html_content="templates/emails/email_bad_file_format.html"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        list_input_dict = rail.SFTPListFilesOperator(
            task_id='list_input_dict',
            paths=[config.input_filepath]
        )

        def get_processing_file_details():
            feed_files = rail.result("list_input_dict")[config.input_filepath]
            return rail.find_first_by_attr_and_get_attr(feed_files, "name", os.path.split(rail.result("new_file_sensor"))[1])

        processing_file_details = rail.PythonOperator(
            task_id="processing_file_details",
            python_callable=get_processing_file_details
        )
        can_ignore_file = rail.IfOperator(
            task_id="can_ignore_file",
            test=lambda: rail.result('processing_file_details')[
                'size'] <= config.input_file_size_threshold,
            yes_task="send_no_data_email",
            no_task="parse_xml"
        )

        parse_xml = rail.LoadXMLFileOperator(
            task_id="parse_xml",
            document="{{result('download_file')}}",
            xsd_document="./dags/dxctechnology/c1_cwf_purchase_order_import/xml_schema/input_file.xsd"
        )

        has_data = rail.IfOperator(
            task_id="has_data",
            test="{{result('parse_xml') | xpath('Records') | length > 0}}",
            yes_task="get_details_from_xml",
            no_task="send_no_data_email"
        )

        send_no_data_email = rail.EmailOperator(
            task_id="send_no_data_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key()}}" +
            "| C1 CWF Purchaseorder balance sync to Replicon - No records in file - " +
            "{{current_time('%Y-%m-%dT%H:%M:%S.%f%z')}}",
            html_content="templates/emails/email_blank_input_file.html"
        )
        get_details_from_xml = rail.XMLAdaptorOperator(
            task_id="get_details_from_xml",
            source="{{result('parse_xml')}}",
            target="artifact",
            adaptor=[
                'Records',
                {
                    'WorkOrderNumber': "WorkOrderNumber/text()",
                    'PersonnelNumber': "PersonnelNumber/text()",
                    'FirstName': "FirstName/text()",
                    'LastName': "LastName/text()",
                    'CompanyCode': "CompanyCode/text()",
                    'PurchaseOrder': "PurchaseOrder/text()",
                    'POItem': "POItem/text()",
                    'Item_StartDate': "Item_StartDate/text()",
                    'Item_EndDate': "Item_EndDate/text()",
                    'RegularTimeBalance': "RegularTimeBalance/text()",
                    'OvertimeBalance': "OvertimeBalance/text()",
                    'DoubleTimeBalance': "DoubleTimeBalance/text()",
                }
            ]
        )
        create_md5 = rail.DataAdaptorOperator(
            task_id="create_md5",
            source="{{result('get_details_from_xml')}}",
            columns=['workordernumber', 'personnelnumber', 'firstname', 'lastname', 'companycode', 'purchaseorder',
                     'poitem', 'item_startdate', 'item_enddate', 'regulartimebalance', 'overtimebalance', 'doubletimebalance', 'md5', 'id'],
            data=custom_method.get_create_md5_data
        )
        create_input_collection = rail.CreateCollectionOperator(
            task_id="create_input_collection",
            name="input_data",
            source="{{result('create_md5')}}"
        )

        get_invalid_records = rail.QueryCollectionOperator(
            task_id="get_invalid_records",
            query="SELECT * FROM 'input_data' \
                    WHERE NULLIF(workordernumber,'') IS NULL OR NULLIF(companycode, '') IS NULL OR NULLIF(personnelnumber, '') IS NULL",
        )
        has_any_invalid_records = rail.IfOperator(
            task_id="has_any_get_invalid_records",
            test="{{result('get_invalid_records','length') > 0}}",
            yes_task="log_records_ignored",
            no_task="send_logs_start"
        )
        log_records_ignored = rail.WriteLogOperator(
            task_id="log_records_ignored",
            items="{{result('get_invalid_records')}}",
            message="Workorder/Personnelnumber/Companycode is missing",
            severity="ignored",
            properties={
                "workordernumber": "{{item.workordernumber}}",
                "personnelnumber": "{{item.personnelnumber}}",
                "companycode": "{{item.companycode}}",
                "purchaseorder": "{{item.purchaseorder}}",
                "status": "ignored",
                "details": "Workorder/Personnelnumber/Companycode is missing",
                "action": "Purchaseorder_update"
            }
        )

        get_valid_records = rail.QueryCollectionOperator(
            task_id="get_valid_records",
            query="SELECT * FROM 'input_data' \
                    WHERE NULLIF(workordernumber,'') IS NOT NULL AND NULLIF(companycode, '') IS NOT NULL AND NULLIF(personnelnumber, '') IS NOT NULL"
        )
        has_any_valid_records = rail.IfOperator(
            task_id="has_any_valid_records",
            test="{{result('get_valid_records','length') > 0}}",
            yes_task="get_report_details",
            no_task="send_logs_start"
        )
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=config.integration_report_name
        )
        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_cwf_purchase_order_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_cwf_purchase_order_report.get_report_result','has_data')}}",
            yes_task='has_valid_columns',
            no_task='fail_no_report_data'
        )

        fail_no_report_data = rail.FailOperator(
            task_id="fail_no_report_data",
            message="No data in the base report",
        )

        expected_report_columns = "User Name,Login Name,Employee ID,CWF C1 alternate ID,UserUri,User Status,\
Employee Type (Current),Timesheet Template,Timesheet Approval Path,Work Week,validation,C1 Purchase Order,Work Order ID"

        has_valid_columns = rail.IfOperator(
            task_id="has_valid_columns",
            #pylint: disable=consider-using-f-string,line-too-long
            test="{{result('run_cwf_purchase_order_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s')}}" % expected_report_columns,
            no_task='fail_invalid_report_columns',
            yes_task='report_payload_to_csv',
        )
        fail_invalid_report_columns = rail.FailOperator(
            task_id="fail_invalid_report_columns",
            message="Base report column does not match"
        )
        report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id='report_payload_to_csv',
            document="{{ result('run_cwf_purchase_order_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_report_collection = rail.CreateCollectionOperator(
            task_id='create_report_collection',
            source="{{ result('report_payload_to_csv') }}",
            columns={
                "User Name": "user_name",
                "Login Name": "login_name",
                "Employee ID": "employee_id",
                "CWF C1 alternate ID": "cwf_c1_alternate_id",
                "UserUri": "user_uri",
                "User Status": "user_status",
                "Employee Type (Current)": "employee_type_current",
                "Timesheet Template": "timesheet_template",
                "Timesheet Approval Path": "timesheet_approval_path",
                "Work Week": "work_week",
                "validation": "validation",
                "C1 Purchase Order": 'c1_purchase_order',
            }
        )
        get_all_cwf_c1_alternate_id_count = rail.QueryCollectionOperator(
            task_id="get_all_cwf_c1_alternate_id_count",
            query="""SELECT cwf_c1_alternate_id, count(1) as cwf_c1_alternate_id_count  FROM create_report_collection where
                        NULLIF (cwf_c1_alternate_id, '') group by cwf_c1_alternate_id"""
        )
        merge_input_report_data = rail.DataAdaptorOperator(
            task_id="merge_input_report_data",
            source="{{result('get_valid_records')}}",
            columns=['workordernumber', 'personnelnumber', 'firstname', 'lastname', 'companycode', 'purchaseorder', 'purchaseorder', 'poitem',
                     'item_startdate', 'item_enddate', 'regulartimebalance', 'overtimebalance', 'doubletimebalance', 'employee_id',
                     'login_name', 'effective_date', 'user_uri', 'user_count', 'md5', 'id'],
            data=custom_method.get_merged_data
        )
        merge_collection = rail.CreateCollectionOperator(
            task_id="merge_collection",
            source="{{result('merge_input_report_data')}}"
        )
        get_invalid_merged_records = rail.QueryCollectionOperator(
            task_id="get_invalid_merged_records",
            query="""SELECT * FROM merge_collection WHERE user_count != 1"""
        )
        has_any_invalid_merged_records = rail.IfOperator(
            task_id="has_any_invalid_merged_records",
            test="{{result('get_invalid_merged_records', 'length') > 0}}",
            yes_task="log_invalid_merged_records",
            no_task="send_logs_start"
        )
        log_invalid_merged_records = rail.WriteLogOperator(
            task_id="log_invalid_merged_records",
            items="{{result('get_invalid_merged_records')}}",
            message="Employee not found or Multiple Employee found",
            severity="ignored",
            properties={
                "workordernumber": "{{item.workordernumber}}",
                "personnelnumber": "{{item.personnelnumber}}",
                "companycode": "{{item.companycode}}",
                "purchaseorder": "{{item.purchaseorder}}",
                "status": "ignored",
                #pylint: disable=line-too-long
                "details": "{{'User is not found in the instance. Please Contact the https://support.deltek.com once the user is added.' if item.user_count == '0' else\
                            'Multiple employee record found in Replicon'}}",
                "action": "Purchaseorder_update"
            }
        )
        get_valid_merged_records = rail.QueryCollectionOperator(
            task_id="get_valid_merged_records",
            query="""SELECT * FROM merge_collection WHERE user_count = 1"""
        )
        has_any_valid_merged_records = rail.IfOperator(
            task_id="has_any_valid_merged_records",
            test="{{result('get_valid_merged_records', 'length') > 0}}",
            yes_task="get_unique_records",
            no_task="send_logs_start"
        )
        get_unique_records = rail.QueryCollectionOperator(
            task_id="get_unique_records",
            query="""SELECT DISTINCT login_name FROM merge_collection WHERE user_count = 1"""
        )
        get_c1_purchase_order_custom_fields = rail.RepliconServiceOperator(
            task_id="get_c1_purchase_order_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'displayText', 'C1 Purchase Order', 'uri')
        )

        process_update_user_udf = rail.TriggerDagRunForEachItemOperator(
            task_id="process_update_user_udf",
            items="{{result('get_unique_records')}}",
            trigger_dag_id=f"dxctechnology_c1_cwf_purchase_order_user_udf_update_child_{config.instance}",
            conf=request_payload.get_process_update_user_udf_conf,
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )
        wait_for_update_user_udf = rail.WaitForDagRunsSensor(
            task_id="wait_for_update_user_udf",
            dag_runs='{{ result("process_update_user_udf") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )
        process_purchaseorder_add_update_blob = rail.TriggerDagRunForEachItemOperator(
            task_id="process_purchaseorder_add_update_blob",
            items="{{result('get_unique_records')}}",
            trigger_dag_id=f"dxctechnology_c1_cwf_purchase_order_purchaseorder_add_update_blob_child_{config.instance}",
            conf={
                "file_name": "{{result('new_file_sensor') | file_name}}",
                "login_name": "{{item.login_name}}"
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )
        wait_for_purchaseorder_add_update_blob = rail.WaitForDagRunsSensor(
            task_id="wait_for_purchaseorder_add_update_blob",
            dag_runs='{{ result("process_purchaseorder_add_update_blob") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )
        send_logs_start = rail.EmptyOperator(
            task_id="send_logs_start"
        )
        def get_log_files_details():
            return os.path.splitext(os.path.split(rail.result('new_file_sensor'))[1])[0] +"_logfile"+ datetime.now().strftime("%m%d%Y%H%M%S")+".csv"

        log_files_details = rail.PythonOperator(
            task_id="log_files_details",
            python_callable=get_log_files_details
        )

        send_logs, send_import_complete_email = get_send_logs(config)

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule='all_done',
            test="{{ result('new_file_sensor') | is_truthy}}",
            yes_task="log_to_sumo"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                "file_name": "{{ result('new_file_sensor') | file_name }}",
                "archived_file_name":  "{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}",
            }
        )

        new_file_sensor >> is_xml >> rail.Label("Yes") >> download_file >> list_input_dict >> processing_file_details \
            >> can_ignore_file >> rail.Label("Yes") >> send_no_data_email
        can_ignore_file >> rail.Label("No") >> parse_xml

        parse_xml >> has_data >> rail.Label("Yes") >> get_details_from_xml >> create_md5 >> \
            create_input_collection >> [get_valid_records, get_invalid_records]
        is_xml >> rail.Label("No") >> send_bad_file_format_email

        has_data >> rail.Label("No") >> send_no_data_email

        get_invalid_records >> has_any_invalid_records >> rail.Label(
            "Yes") >> log_records_ignored >> send_logs_start >> log_files_details
        get_valid_records >> has_any_valid_records >> rail.Label(
            "Yes") >> get_report_details >> run_report_group_entry

        run_report_group_exit >> report_has_data >> rail.Label("Yes") >> has_valid_columns >> rail.Label("Yes") >> report_payload_to_csv \
            >> create_report_collection >> get_all_cwf_c1_alternate_id_count >> merge_input_report_data >> merge_collection
        report_has_data >> rail.Label("No") >> fail_no_report_data
        has_valid_columns >> rail.Label("No") >> fail_invalid_report_columns

        merge_collection >> [
            get_invalid_merged_records, get_valid_merged_records]
        get_invalid_merged_records >> has_any_invalid_merged_records >> rail.Label(
            "Yes") >> log_invalid_merged_records >> send_logs_start >> log_files_details
        get_valid_merged_records >> has_any_valid_merged_records >> rail.Label("Yes") >> get_unique_records >>\
            get_c1_purchase_order_custom_fields >> process_update_user_udf >> wait_for_update_user_udf >> \
            process_purchaseorder_add_update_blob >> wait_for_purchaseorder_add_update_blob >> send_logs_start >> log_files_details \
                >> send_logs >> send_import_complete_email

        [has_any_invalid_merged_records, has_any_valid_merged_records,
            has_any_invalid_records, has_any_valid_records] >> rail.Label("No") >> send_logs_start

        send_import_complete_email >> can_log_to_sumo >> rail.Label(
            "Yes") >> log_to_sumo

        list_input_dict >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
