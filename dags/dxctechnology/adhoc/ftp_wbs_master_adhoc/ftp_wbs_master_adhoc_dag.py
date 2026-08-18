from datetime import timedelta
import rail
from dxctechnology.adhoc.ftp_wbs_master_adhoc import request_payload
from dxctechnology.adhoc.ftp_wbs_master_adhoc.send_logs import get_send_logs

def create_main_airflow_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id = f'dxctechnology_ftp_wbs_master_adhoc{dag_id_postfix}',
        description = 'DXC_FTP_WBS_Master_ADHOC',
        company_key = config.company_key,
        replicon_conn_id = config.replicon_conn_id,
        schedule_interval = timedelta(seconds=30),
        max_active_runs = 1,
        default_args = {
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id = 'new_file_sensor',
            path = config.input_filepath,
            soft_fail_timeout= timedelta(minutes=10),
        )

        is_xml = rail.IfOperator(
            task_id = 'is_xml',
            test = '{{ result("new_file_sensor") | file_ext | lower == "xml" }}',
            yes_task = 'download_file',
            no_task = 'send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id = 'send_bad_file_format_email',
            to = config.tenant_email,
            bcc = config.internal_logs_email,
            subject = '{{ get_company_key() }} | Replicon project sync for FTP WBS MASTER - Incorrect File Format - {{ current_time_in_specified_tz() }}',
            html_content = "email_bad_file_format.html",
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id = 'download_file',
            remote_filepath = "{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id = 'was_new_file_found',
            trigger_rule = 'all_done',
            test = '{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task = 'archive_file',
            no_task = 'delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
                task_id = 'archive_file',
                trigger_rule='all_done',
                existing_filename = '{{ result("new_file_sensor") }}',
                new_filename = config.archive_filepath + "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        parse_xml = rail.LoadXMLFileOperator(
            task_id = 'parse_xml',
            document = "{{ result('download_file') }}",
            xsd_document = './dags/dxctechnology/adhoc/ftp_wbs_master_adhoc/input_schema.xsd'
        )

        has_data = rail.IfOperator(
            task_id = 'has_data',
            test = '{{ result("parse_xml") | xpath("Records") | length > 0 }}',
            yes_task = 'get_project_oefs',
            no_task = 'send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id = 'send_blank_payload_email',
            to = config.tenant_email,
            bcc = config.internal_logs_email,
            subject = '{{ get_company_key() }} | Replicon project sync for FTP WBS MASTER - Blank Payload - {{ current_time_in_specified_tz() }}',
            html_content = "email_blank_payload.html",
        )

        get_project_oefs= rail.RepliconServiceOperator(
                task_id = "get_project_oefs",
                endpoint = "/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
                data={  "bindingContextUri": "urn:replicon:object-type:project" },
                data_handler=lambda oefs: {
                'PRCTR': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Profit Center', 'uri'),
                'SCOPE': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'FTP Object Class', 'uri'),
                'CUST_USR00': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Sold to Party', 'uri'),
                'FunctionalArea': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'FTP Functional Area', 'uri'),
                'Product': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'FTP Product', 'uri'),
                'Stage': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'FTP Engagement Stage', 'uri'),
                'SalesForceID': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Salesforce Opportunity ID', 'uri'),
                'IWONo': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'IWO Number', 'uri'),
            },
        )

        get_details_from_xml = rail.XMLAdaptorOperator(
                task_id = "get_details_from_xml",
                source = '{{ result("parse_xml") }}',
                target = 'artifact',
                adaptor = [
                    'Records',
                    {
                        'Project_Code': 'ProjectCode/text()',
                        'Project_Name': 'ProjectName/text()',
                        'Start_date': 'Project_StartDate/text()',
                        'End_date': 'Project_EndDate/text()',
                        'Status': 'Status/text()',
                        'Project':'Project/text()',
                        'ProjectManager_EmpID': 'ProjectManager_EmpID/text()',
                        'ProjectCoManager_EmpID': 'ProjectCoManager_EmpID/text()',
                        'ProjectGroup': 'ProjectGroup/text()',
                        'ProjectUDF': 'ProjectUDF/text()',
                        'Type': 'Type/text()',
                        'Billing_Indicator':'Billing_Indicator/text()',
                        'PRCTR':'PRCTR/text()',
                        'SCOPE':'SCOPE/text()',
                        'CUST_USR00':'CUST_USR00/text()',
                        'FunctionalArea':'FunctionalArea/text()',
                        'Product':'Product/text()',
                        'Stage':'Stage/text()',
                        'SalesForceID':'SalesForceID/text()',
                        'IWONo':'IWONo/text()',
                        'Client':'Client/text()',
                    },
                ]
            )

        process_child= rail.TriggerDagRunForEachItemOperator(
            task_id = 'process_child',
            items = "{{ result('get_details_from_xml') }}",
            trigger_dag_id = f'dxctechnology_ftp_wbs_master_child_adhoc{dag_id_postfix}',
            conf = request_payload.get_child_conf,
            execution_timeout = timedelta(hours=12),
            retries = 0,
        )

        wait_for_process_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_child',
            dag_runs='{{ result("process_child") }}',
            execution_timeout=timedelta(days=14),
        )

        send_logs_enter, _ = get_send_logs(config)

        is_xml >> rail.Label("No") >> send_bad_file_format_email
        has_data >> rail.Label("No") >> send_blank_payload_email
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        new_file_sensor >> is_xml >> rail.Label("Yes") >> download_file >> rail.Label("Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        get_details_from_xml >> process_child >> wait_for_process_child >> send_logs_enter
        download_file >>  parse_xml >> has_data >> rail.Label("Yes")  >> get_project_oefs  >> get_details_from_xml

    return dag

rail.for_each_instance(create_main_airflow_dag)
