from datetime import datetime, timedelta
import rail
from dxctechnology.cwf_time_export_v8.utils import python_callable_method
from dxctechnology.cwf_time_export_v8.utils import request_payload
from dxctechnology.cwf_time_export_v8.utils import response_filter


null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_cwf_time_export_gsap_child_{config.instance}_v8',
        description=f'DXCTechnology_CWF Time export - GSAP v8 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.gsap_sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_last_time_export_details = rail.RepliconServiceOperator(
            task_id='get_last_time_export_details',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataExportDetails',
            data={
                "target": {
                    "uri": '{{ dag_run.conf.last_twb_uri }}',
                }
            },
            response_filter=response_filter.get_last_time_export_details
        )

        has_previous_export_processed = rail.IfOperator(
            task_id='has_previous_export_processed',
            test='{{ result("get_last_time_export_details") }}',
            yes_task="create_time_data_download_batch_gsap",
            no_task='process_acknowledgement_not_received'
        )

        process_acknowledgement_not_received = rail.TriggerDagRunForEachItemOperator(
            task_id='process_acknowledgement_not_received',
            retries=0,
            items='{{ dag_run.conf.twb_list | to_json }}',
            trigger_dag_id=f'dxctechnology_acknowledgement_not_received_notification_{config.instance}_v8',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "name": item["name"],
                "uri": item["uri"],
                "createdatetime": item["createdatetime"],
                "oef_name": 'GSAP_Payload_Processed',
                "twbname": python_callable_method.get_dag_run_conf()['twbname'],
                "erp": 'GSAP',
                'sender': 'GSAP'
            }
        )

        wait_to_process_acknowledgement_not_received = rail.WaitForDagRunsSensor(
            task_id='wait_to_process_acknowledgement_not_received',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_acknowledgement_not_received") }}',
        )

        gather_all_unckn_export_details = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_all_unckn_export_details',
            dag_runs="{{ result('process_acknowledgement_not_received') }}",
            dagrun_task_id='time_export_details_output',
            flatten=True,
        )

        get_unackn_email_content = rail.RenderTemplateOperator(
            task_id='get_unackn_email_content',
            target='result',
            template_file='templates/c1_output_template.html',
            dataset=lambda: request_payload.output_payload(
                rail.result("gather_all_unckn_export_details")),
        )

        send_unackn_email = rail.EmailOperator(
            task_id='send_unackn_email',
            to=config.internal_logs_email,
            subject='{{ get_company_key() + " | Priority 2 : Payload acknowledgement not received for GSAP " }}',
            html_content='{{ result("get_unackn_email_content")}}',
        )

        create_time_data_download_batch_gsap = rail.RepliconServiceOperator(
            task_id='create_time_data_download_batch_gsap',
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch",
            data={
                "columnUris": [],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": ["{{ dag_run.conf.timeexporturi }}"],
                            "bool": null,
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
                },
                "fileFormatScriptUri": "{{dag_run.conf.fileformaturi}}"
            }
        )

        batch_management_gsap = rail.batch_execution(
            group_id='execute_batch_management_gsap',
            creation_task_id=create_time_data_download_batch_gsap.task_id,
        )

        get_gsap_batch_result = rail.RepliconServiceOperator(
            task_id='get_gsap_batch_result',
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults",
            data={
                "timeDataDownloadBatchUri": "{{ result('create_time_data_download_batch_gsap') }}"
            }
        )

        log_message_filename_gsap = rail.PythonOperator(
            task_id='log_message_filename_gsap',
            python_callable=lambda dag_run: dag_run.conf['twbname']
        )

        read_file_gsap = rail.HTTPDownloadFileOperator(
            task_id='read_file_gsap',
            url='{{ result("get_gsap_batch_result").downloadUrl }}',
        )

        load_csv_create_list_from_csv_gsap = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_gsap",
            document="{{result('read_file_gsap')}}",
        )

        create_collection_create_list_from_csv_gsap = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_gsap',
            source="{{ result('load_csv_create_list_from_csv_gsap') }}",
            name="finaltimedata",
            columns={
                'Company Code Code': 'companycodecode',
                'Employee ID': 'employeeid',
                'PERNER': 'perner',
                'Approval Status': 'approvalstatus',
                'Entry Date': 'entrydate',
                'WBS / SO Name': 'projectname',
                'Cost Center Name': 'costcentercode',
                'Labor Type Name': 'labortype',
                'Job Activity Type': 'jobactivitytype',
                'Task Name': 'taskname',
                'Time Type': 'timetype',
                'Attendance Type Code': 'attendancetypecode',
                'Billable Indicator': 'billableindicator',
                'Hours (Current)': 'hours',
                'Rate Type': 'ratetype',
                'Short Time Entry ID': 'timeentryid',
                'Time Off Booking ID': 'timeoffbookingid',
                'Comments': 'comments',
                'WBS Type': 'wbstype',
                'Task Task Type': 'tasktype',
                'New Remaining Work': 'newremainningwork',
                'Customer 1': 'customer1',
                'Customer 2': 'customer2',
                'Customer 3': 'customer3',
                'GSAP Billable Flag': 'gsapbillableflag',
                'Time Off Type Description': 'timeofftypedescription',
                'Master WBS (SO, WO)': 'masterwbs',
                'Project Type': 'projecttype',
                'IWO Indicator': 'iwoindicator',
                'Parent WBS': 'parentwbs',
                'Company Code Name': 'companycodename',
                'Task Name (Full Path)': 'taskfullpath',
                'Time Entry ID': 'timentryid2',
                'Employee Type Name': 'employeetypename',
                'Timesheet Period': 'timesheetperiod',
                'Location Name': 'locationname',
                'Login Name': 'loginname',
                'User': 'user',
                'IWO WBS Element': 'iwowbselement',
                'Work Order ID': 'workorderid',
                'Parent Service Order': 'parentserviceorder',
                'CWF C1 alternate ID': 'c1cwfalternateid',
                'Parent Project': 'parentproject',
                'Attribute 1 (Code)': 'attributecode1',
                'Attribute 2 (Code)': 'attributecode2',
                'GSAP Reference Number': 'gsapreferencenumber',
                'PSA Flag':'psaflag',
                'GSAP Task': 'gsaptask',
                'GSAP Task (Code)': 'gsaptaskcode',
                'Attendance Type Name': 'attendancetypename',
                'Time Type (AUS) (Code)': 'timetypeauscode'
            }
        )

        has_row_count_gsap = rail.IfOperator(
            task_id='has_row_count_gsap',
            test="{{ result('create_collection_create_list_from_csv_gsap','length') > 0 }}",
            yes_task="query_list_filtered_data_gsap",
            no_task="get_final_line_no_data",
        )

        get_final_line_no_data = rail.PythonOperator(
            task_id="get_final_line_no_data",
            python_callable=python_callable_method.get_gsap_final_line_data
        )

        get_final_line_no_data_collection = rail.CreateCollectionOperator(
            task_id="get_final_line_no_data_collection",
            source=lambda: rail.result('get_final_line_no_data'),
            name="getfinaldatanodatacollection"
        )

        generate_xml_time_no_data = rail.RenderTemplateOperator(
            task_id='generate_xml_time_no_data',
            dataset='{{result(\'' + get_final_line_no_data_collection.task_id + '\')}}',
            target='artifact',
            template_file='xml_schema/gsap_outbound.xml'
        )

        send_time_no_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id='send_time_no_data_to_sftp',
            remote_filepath=config.gsap_output_filepath +
            '/{{ dag_run.conf.twbname }}.xml',
            content='{{result(\'' + generate_xml_time_no_data.task_id + '\')}}',
        )

        is_allowed_send_export_no_data = rail.IfOperator(
            task_id='is_allowed_send_export_no_data',
            test=config.is_allowed_send_export_data,
            yes_task='upload_time_no_data',
            no_task='send_mail_no_data'
        )

        upload_time_no_data = rail.HTTPUploadFileOperator(
            task_id='upload_time_no_data',
            content='{{result(\'' + generate_xml_time_no_data.task_id + '\')}}',
            retries=0,
            http_conn_id=config.gsap_http_conn_id,
            content_type="application/xml",
            extra_options={
                'verify': False
            } if config.instance == "DXCSandbox" else None
        )

        send_mail_no_data = rail.EmailOperator(
            task_id='send_mail_no_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=f-string-without-interpolation
            subject='{{get_company_key()}} | Replicon CWF time extract for GSAP - No records to export {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}} ',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
                The Replicon time extract for CWFTime for GSAP is completed successfully at {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}}.
                There are no records to export.The payload identifier is {{ dag_run.conf.payload_identifier_replicon_uniqueid }}.</p>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> '''
        )

        query_list_filtered_data_gsap = rail.QueryCollectionOperator(
            task_id='query_list_filtered_data_gsap',
            query='''SELECT * FROM finaltimedata
                    WHERE
                        (employeetypename LIKE '%Contractor%' AND companycodename in ('3001', '3124', '1602', '3118') AND attendancetypecode NOT LIKE '%799%') OR
                        (employeetypename LIKE '%Contractor%' AND companycodename in ('3001', '3124', '1602', '3118') AND CAST(hours as INT) = 0) OR
                        (employeetypename LIKE '%Contractor%' AND projecttype='IC' AND projectname LIKE 'X-%' AND attendancetypecode NOT LIKE '%799%') OR
                        (employeetypename LIKE '%Contractor%' AND projecttype='IC' AND projectname LIKE 'X-%' AND CAST(hours as INT) = 0) OR
                        (employeetypename LIKE '%Contractor%' AND projecttype='GS' AND attendancetypecode NOT LIKE '%799%') OR
                        (employeetypename LIKE '%Contractor%' AND projecttype='GS' AND CAST(hours as INT) == 0)
                        ORDER BY CAST(hours as DECIMAL) ASC
                        ''',
        )

        has_rows_gsap_query_list_filtered_data = rail.IfOperator(
            task_id='has_rows_gsap_query_list_filtered_data',
            test="{{ result('query_list_filtered_data_gsap','length') > 0}}",
            yes_task="create_csv_lines_final_gsap",
            no_task='get_final_line_no_filter_data'
        )

        get_final_line_no_filter_data = rail.PythonOperator(
            task_id="get_final_line_no_filter_data",
            python_callable=python_callable_method.get_gsap_final_line_data,
        )

        get_final_line_no_filter_data_collection = rail.CreateCollectionOperator(
            task_id="get_final_line_no_filter_data_collection",
            source=lambda: rail.result('get_final_line_no_filter_data'),
            name="getfinaldatanodatacollection"
        )

        generate_xml_time_no_filter_data = rail.RenderTemplateOperator(
            task_id='generate_xml_time_no_filter_data',
            dataset='{{result(\'' + get_final_line_no_filter_data_collection.task_id + '\')}}',
            target='artifact',
            template_file='xml_schema/gsap_outbound.xml'
        )

        send_time_no_filter_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id='send_time_no_filter_data_to_sftp',
            remote_filepath=config.gsap_output_filepath +
            '/{{ dag_run.conf.twbname }}.xml',
            content='{{result(\'' + generate_xml_time_no_filter_data.task_id + '\')}}'
        )

        is_allowed_send_export_no_filter_data = rail.IfOperator(
            task_id='is_allowed_send_export_no_filter_data',
            test=config.is_allowed_send_export_data,
            yes_task='upload_time_no_filter_data',
            no_task='send_mail_no_filter_data'
        )

        upload_time_no_filter_data = rail.HTTPUploadFileOperator(
            task_id='upload_time_no_filter_data',
            content='{{result(\'' + generate_xml_time_no_filter_data.task_id + '\')}}',
            retries=0,
            http_conn_id=config.gsap_http_conn_id,
            content_type="application/xml",
            extra_options={
                'verify': False
            } if config.instance == "DXCSandbox" else None
        )

        send_mail_no_filter_data = rail.EmailOperator(
            task_id='send_mail_no_filter_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=f-string-without-interpolation
            subject='{{get_company_key()}} | Replicon CWF time extract for GSAP - No records to export {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}} ',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
                The Replicon time extract for CWFTime for GSAP is completed successfully at {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}}.
                There are no records to export.The payload identifier is {{ dag_run.conf.payload_identifier_replicon_uniqueid }}.</p>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> '''
        )

        def map_row(item):
            return {
                "Replicon_Unique_ID": item['timeentryid'] if item['timeentryid'] else item['timeoffbookingid'],
                "Workday_PERNR": item['c1cwfalternateid'] if item['companycodecode'] == 'C1' else
                item['employeeid'] if item['companycodecode'] == 'COMPASS' else item['perner'],
                "Date": datetime.strptime(item['entrydate'], config.entry_date_format).strftime("%Y%m%d"),
                "GSAP_WBS": item['parentwbs'].rjust(8, "0") if item['projecttype'] == "IC" or item['projecttype'] == "GS" and item[
                                    'parentwbs'] else item['projectname'].rjust(8, "0"),
                "Billing_Key": '00' if item['iwoindicator'] == 'C1' else '00' if item['projecttype'] == "CP" else
                        item['taskname'] if item['tasktype'] == 'GSAP Billing Key' else None,
                "Billing_Indicator": 'X' if item['iwoindicator'] == 'C1' or item['projecttype'] == "CP" or (item['gsapbillableflag'] and
                        item['gsapbillableflag'] == "Billable") else None,
                "Tasks": item['gsaptaskcode'] if item['gsaptask'] else item['taskname'] if item['tasktype'] != 'GSAP Billing Key' else None,
                "Hours": format(float(item['hours']), '.2f') if float(item['hours']) != 0 else '0',
                "Attendance_Absence_Type": (item['timetypeauscode'] if item['timetypeauscode'] else item['attendancetypecode'] if
                    item['attendancetypecode'] in ['2087', '2850'] else '2082') if item['companycodename'] in ['3001', '3124', '1602', '3118'] else '1010',
                "Status": 30,
                "Remarks": item['comments'],
                "Reference_Number": item['gsapreferencenumber'],
            }.values()

        create_csv_lines_final_gsap = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_final_gsap',
            source="{{ result('query_list_filtered_data_gsap') }}",
            header=[
                'Replicon_Unique_ID',
                'Workday_PERNR',
                'Date',
                'GSAP_WBS',
                'Billing_Key',
                'Billing_Indicator',
                'Tasks',
                'Hours',
                'Attendance_Absence_Type',
                'Status',
                'Remarks',
                'Reference_Number',
            ],
            row=map_row
        )

        load_csv_create_list_from_csv_gsapfinal = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_gsapfinal",
            document="{{result('create_csv_lines_final_gsap')}}",
        )

        create_collection_create_list_from_csv_gsapfinal = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_gsapfinal',
            source="{{ result('load_csv_create_list_from_csv_gsapfinal') }}",
            name="finaldata1"
        )

        query_list_gsapfinal = rail.QueryCollectionOperator(
            task_id='query_list_gsapfinal',
            query='''SELECT * FROM finaldata1''',
        )

        get_final_line = rail.PythonOperator(
            task_id="get_final_line",
            python_callable=python_callable_method.get_gsap_final_line_data
        )

        get_final_line_collection = rail.CreateCollectionOperator(
            task_id="get_final_line_collection",
            source=lambda: rail.result('get_final_line'),
            name="getfinaldatacollection",
            columns={
                'Replicon_Unique_ID': 'Replicon_Unique_ID',
                'Workday_PERNR': 'Workday_PERNR',
                'Date': 'Date',
                'GSAP_WBS': 'GSAP_WBS',
                'Billing_Key': 'Billing_Key',
                'Billing_Indicator': 'Billing_Indicator',
                'Tasks': 'Tasks',
                'Hours': 'Hours',
                'Attendance_Absence_Type': 'Attendance_Absence_Type',
                'Status': 'Status',
                'Remarks': 'Remarks',
                'Reference_Number': 'Reference_Number',
            }
        )

        query_filtered_data_for_reversal = rail.QueryCollectionOperator(
            task_id='query_filtered_data_for_reversal',
            query='''SELECT * FROM finaltimedata WHERE (timeentryid NOT IN (SELECT DISTINCT timeentryid FROM query_list_filtered_data_gsap) AND CAST(hours AS FLOAT) = 0)'''
        )

        create_reversal_csv_lines_final_gsap = rail.WriteCSVFileOperator(
            task_id='create_reversal_csv_lines_final_gsap',
            source="{{ result('query_filtered_data_for_reversal') }}",
            header=[
                'Replicon_Unique_ID',
                'Workday_PERNR',
                'Date',
                'GSAP_WBS',
                'Billing_Key',
                'Billing_Indicator',
                'Tasks',
                'Hours',
                'Attendance_Absence_Type',
                'Status',
                'Remarks',
                'Reference_Number',
            ],
            row=map_row
        )

        create_collection_create_list_from_reversal = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_reversal',
            source="{{ result('create_reversal_csv_lines_final_gsap') }}",
            name="reversaldata"
        )

        get_final_export_data = rail.QueryCollectionOperator(
            task_id='get_final_export_data',
            query='''SELECT * FROM (SELECT *,2 as filter FROM query_list_gsapfinal UNION ALL
                    SELECT *,2 as filter FROM reversaldata UNION ALL
                    SELECT *,1 as filter FROM getfinaldatacollection) ORDER BY filter'''
        )

        create_document_gsap_xml = rail.RenderTemplateOperator(
            task_id='create_document_gsap_xml',
            target='artifact',
            template_file='xml_schema/gsap_outbound.xml',
            dataset="{{ result('get_final_export_data') }}",
        )

        upload_xmlfile_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_xmlfile_to_sftp',
            content="{{ result('create_document_gsap_xml') }}",
            remote_filepath=config.gsap_output_filepath +
            '/{{ dag_run.conf.twbname }}.xml',
        )

        send_mail_timedatafileexportfailed_gsap = rail.EmailOperator(
            task_id='send_mail_timedatafileexportfailed_gsap',
            to=config.tenant_email,
            trigger_rule='one_failed',
            subject='{{get_company_key()}} | GSAP Time data export automation - SFTP upload failure - {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}} ',
            html_content='''<p>Hi Team,<br /> <br /> The GSAP time date export has been completed at {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}},
                            however the file upload to sftp has failed with error.</p>
                            <ul>
                            <li>Recipe ID: {{ dag_run.dag_id}}</li>
                            <li>Job ID: {{ ecid() }}</li>
                            <li>Instance: {{ get_company_key() }}</li>
                            <li>File Name: {{ result('log_message_filename_gsap')}}.xml</li>
                            <li>SFTP Path: {{ params.sftp_path}}</li>
                            <li>Error: ''' + config.error_template + ''' </li>
                            </ul>
                            <p>Please find the attached file to be uploaded to sftp.
                            Upload the file to the given sftp and debug the issue.<br /> <br /> Regards,<br /> Deltek Inc</p> ''',
            params={
                'sftp_path': config.gsap_output_filepath
            },
            files=[
                ("{{ result('log_message_filename_gsap')}}.xml",
                 '{{result("create_document_gsap_xml")}}')
            ]

        )

        fail_sftp_upload_error = rail.FailOperator(
            task_id='fail_sftp_upload_error',
            message=config.error_template
        )

        send_mail_gsap = rail.EmailOperator(
            task_id='send_mail_gsap',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon CWF time export for GSAP- Completed Successfully - {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}}',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
            The Replicon CWF time export for GSAP job is Completed successfully at {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}}. Please find the file details below:<br /><br />
            File path: {{ params.sftp_path}} <br />
            File name: {{result('log_message_filename_gsap')}}.xml <br/>
            Payload identifier: {{ dag_run.conf.payload_identifier_replicon_uniqueid }}
            <br /></p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params={
                'sftp_path': config.gsap_output_filepath
            }
        )

        is_allowed_send_time_data = rail.IfOperator(
            task_id='is_allowed_send_time_data',
            test=config.is_allowed_send_export_data,
            yes_task='gsap_time_data_upload',
            no_task='send_mail_gsap'
        )

        gsap_time_data_upload = rail.HTTPUploadFileOperator(
            task_id='gsap_time_data_upload',
            method='POST',
            http_conn_id=config.gsap_http_conn_id,
            content_type='application/xml',
            content="{{ result('create_document_gsap_xml') }}",
            extra_options={
                'verify': False
            } if config.instance == "DXCSandbox" else None
        )

        get_all_oefs_for_the_exports = rail.RepliconServiceOperator(
            task_id = 'get_all_oefs_for_the_exports',
            endpoint= '/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings',
            data= {
                    "bindingContextUri": "urn:replicon:object-type:time-data-export"
                },
            response_filter= response_filter.get_oef_uris
        )

        acknowledge_current_export= rail.RepliconServiceOperator(
            task_id = 'acknowledge_current_export',
            endpoint= '/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data= request_payload.get_acknowlegement_payload
        )

        get_last_time_export_details >> has_previous_export_processed >> rail.Label(
            "Yes") >> create_time_data_download_batch_gsap

        has_previous_export_processed >> rail.Label(
            "No") >> process_acknowledgement_not_received >> wait_to_process_acknowledgement_not_received >> gather_all_unckn_export_details >>\
            get_unackn_email_content >> send_unackn_email >> create_time_data_download_batch_gsap

        create_time_data_download_batch_gsap >> batch_management_gsap >> get_gsap_batch_result >> \
            log_message_filename_gsap >> read_file_gsap >> load_csv_create_list_from_csv_gsap >> \
            create_collection_create_list_from_csv_gsap >> has_row_count_gsap

        has_row_count_gsap >> rail.Label(
            'No') >> get_final_line_no_data >> get_final_line_no_data_collection >> generate_xml_time_no_data >> send_time_no_data_to_sftp >> \
            is_allowed_send_export_no_data >> rail.Label(
                "No") >> send_mail_no_data

        is_allowed_send_export_no_data >> rail.Label(
            "Yes") >> upload_time_no_data >> send_mail_no_data >> get_all_oefs_for_the_exports >> acknowledge_current_export

        has_row_count_gsap >> rail.Label(
            'Yes') >> query_list_filtered_data_gsap >> has_rows_gsap_query_list_filtered_data

        has_rows_gsap_query_list_filtered_data >> rail.Label(
            'Yes') >> create_csv_lines_final_gsap >> load_csv_create_list_from_csv_gsapfinal >> create_collection_create_list_from_csv_gsapfinal >> \
            query_list_gsapfinal >> get_final_line >> get_final_line_collection >> query_filtered_data_for_reversal >> \
                create_reversal_csv_lines_final_gsap >> create_collection_create_list_from_reversal >> get_final_export_data >> \
                    create_document_gsap_xml >> upload_xmlfile_to_sftp

        has_rows_gsap_query_list_filtered_data >> rail.Label(
            'No') >> get_final_line_no_filter_data >> get_final_line_no_filter_data_collection >> generate_xml_time_no_filter_data >> \
            send_time_no_filter_data_to_sftp >> is_allowed_send_export_no_filter_data >> rail.Label(
                "No") >> send_mail_no_filter_data

        is_allowed_send_export_no_filter_data >> rail.Label(
            "Yes") >> upload_time_no_filter_data >> send_mail_no_filter_data >> get_all_oefs_for_the_exports >> acknowledge_current_export

        upload_xmlfile_to_sftp >> rail.Label(
            'error') >> send_mail_timedatafileexportfailed_gsap >> fail_sftp_upload_error

        upload_xmlfile_to_sftp >> rail.Label(
            'success') >> is_allowed_send_time_data >> rail.Label("No") >> send_mail_gsap

        is_allowed_send_time_data >> rail.Label(
            "Yes") >> gsap_time_data_upload >> send_mail_gsap >> get_all_oefs_for_the_exports >> acknowledge_current_export

    return dag


rail.for_each_instance(create_dag)
