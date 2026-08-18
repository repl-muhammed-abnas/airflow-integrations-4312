from datetime import datetime, timedelta
import itertools
import json
import rail
from dxctechnology.cwf_time_export_v4.utils import python_callable_method
from dxctechnology.cwf_time_export_v4.utils import request_payload
from dxctechnology.cwf_time_export_v4.utils import response_filter


null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_cwf_time_export_c1_child_{config.instance}_v4',
        description=f'DXCTechnology_CWF Time export - C1 V4 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.c1_sftp_conn_id,
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
            yes_task="create_time_data_download_batch_c1",
            no_task='process_acknowledgement_not_received'
        )

        process_acknowledgement_not_received = rail.TriggerDagRunForEachItemOperator(
            task_id='process_acknowledgement_not_received',
            retries=0,
            items='{{ dag_run.conf.twb_list | to_json }}',
            trigger_dag_id=f'dxctechnology_acknowledgement_not_received_notification_{config.instance}_v4',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "name": item["name"],
                "uri": item["uri"],
                "createdatetime": item["createdatetime"],
                "oef_name": 'C1_Payload_Processed',
                "erp": 'C1',
                'sender': 'C1'
            }
        )

        wait_to_process_acknowledgement_not_received = rail.WaitForDagRunsSensor(
            task_id='wait_to_process_acknowledgement_not_received',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_acknowledgement_not_received") }}',
            retries=0,
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
            to=config.c1_acknowledgement_email,
            bcc= config.alert_email,
            subject='{{ get_company_key() + " | Priority 2 : Payload acknowledgement not received for C1 " }}',
            html_content='{{ result("get_unackn_email_content")}}',
        )

        fail_for_no_ackn = rail.FailOperator(
            task_id='fail_for_no_ackn',
            message='Acknowledgement not received for previous export',
        )

        create_time_data_download_batch_c1 = rail.RepliconServiceOperator(
            task_id='create_time_data_download_batch_c1',
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

        batch_management_c1 = rail.batch_execution(
            group_id='execute_batch_management_c1',
            creation_task_id=create_time_data_download_batch_c1.task_id,
        )

        get_c1_batch_result = rail.RepliconServiceOperator(
            task_id='get_c1_batch_result',
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults",
            data={
                "timeDataDownloadBatchUri": "{{ result('create_time_data_download_batch_c1') }}"
            }
        )

        log_message_filename_c1 = rail.PythonOperator(
            task_id='log_message_filename_c1',
            python_callable=lambda dag_run: dag_run.conf['twbname']
        )

        read_file_c1 = rail.HTTPDownloadFileOperator(
            task_id='read_file_c1',
            url='{{ result("get_c1_batch_result").downloadUrl }}',
        )

        load_csv_create_list_from_csv_c1 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_c1",
            document="{{result('read_file_c1')}}",
        )

        create_collection_create_list_from_csv_c1 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_c1',
            source="{{ result('load_csv_create_list_from_csv_c1') }}",
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
            }
        )

        has_row_count_c1 = rail.IfOperator(
            task_id='has_row_count_c1',
            test="{{ result('create_collection_create_list_from_csv_c1','length') > 0 }}",
            yes_task="query_list_filtered_data_c1",
            no_task="get_final_line_no_data",
        )

        get_final_line_no_data = rail.PythonOperator(
            task_id="get_final_line_no_data",
            python_callable=python_callable_method.get_c1_final_line_data
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
            template_file='xml_schema/c1_outbound.xml'
        )

        send_time_no_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id='send_time_no_data_to_sftp',
            remote_filepath=config.c1_output_filepath +
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
            http_conn_id=config.c1_http_conn_id,
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
            subject='{{get_company_key()}} | Replicon CWF time extract for C1 - No records to export {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}} ',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
                The Replicon time extract for CWFTime for C1 is completed successfully at {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}}.
                There are no records to export.The payload identifier is {{ dag_run.conf.payload_identifier_replicon_uniqueid }}.</p>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> '''
        )

        query_list_filtered_data_c1 = rail.QueryCollectionOperator(
            task_id='query_list_filtered_data_c1',
            query='''SELECT * FROM finaltimedata
                    WHERE
                        employeetypename LIKE '%Contractor%' AND companycodecode = 'C1' AND attendancetypecode NOT LIKE '%799%'
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND iwoindicator= 'X' AND attendancetypecode NOT LIKE '%799%'
                            AND (parentproject IS NULL OR parentproject = '')
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND iwoindicator= 'C1' AND attendancetypecode NOT LIKE '%799%'
                        )
                        ORDER BY CAST(hours as DECIMAL) ASC
                        ''',
        )

        query_list_uniqueusers_c1 = rail.QueryCollectionOperator(
            task_id='query_list_uniqueusers_c1',
            query='''SELECT DISTINCT LoginName FROM query_list_filtered_data_c1''',
        )

        get_key_value_pobalance = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_key_value_pobalance',
            endpoint="/services/GenericKeyValueStoreService1.svc/GetKeyValue",
            items='{{ result("query_list_uniqueusers_c1") }}',
            flatten=True,
            data={
                "keyNamespace": "DXC_PurchaseOrderRateTypesBalanceDetails",
                "key": "{{item.loginname}}"
            },
            all_result_data_handler=lambda data: list(
                map(lambda item:  {
                    'key': item['key'],
                    'jsonValue': json.loads(item['jsonValue'])
                }, filter(lambda item: item, data))),
        )

        has_rows_c1_query_list_filtered_data = rail.IfOperator(
            task_id='has_rows_c1_query_list_filtered_data',
            test="{{ result('query_list_filtered_data_c1','length') > 0}}",
            yes_task="create_csv_lines_final_c1",
            no_task='get_final_line_no_filter_data'
        )

        get_final_line_no_filter_data = rail.PythonOperator(
            task_id="get_final_line_no_filter_data",
            python_callable=python_callable_method.get_c1_final_line_data,
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
            template_file='xml_schema/c1_outbound.xml'
        )

        send_time_no_filter_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id='send_time_no_filter_data_to_sftp',
            remote_filepath=config.c1_output_filepath +
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
            http_conn_id=config.c1_http_conn_id,
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
            subject='{{get_company_key()}} | Replicon CWF time extract for C1 - No records to export {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}} ',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
                The Replicon time extract for CWFTime for C1 is completed successfully at {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}}.
                There are no records to export.The payload identifier is {{ dag_run.conf.payload_identifier_replicon_uniqueid }}.</p>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> '''
        )

        def map_row(item):
            po_order_list = list(filter(lambda x: x['loginName'] == item['loginname'] and
                                        datetime.strptime(x['itemStartDate'], config.output_date_format) <=
                                        datetime.strptime(item['entrydate'], config.entry_date_format) and
                                        datetime.strptime(x['itemEndDate'], config.output_date_format) >= datetime.strptime(
                item['entrydate'], config.entry_date_format),
                list(itertools.chain(*map(lambda x: x['jsonValue'], rail.result('get_key_value_pobalance'))))))

            c1_cwf_alternate_id = po_order_list[0].get('personnelNumber', '') if len(po_order_list) > 0 else ''

            return {
                "Employeeid": item['perner'] if (item['iwoindicator'] and (item['iwoindicator'] == "X" or item['iwoindicator'] == "C1")) else
                c1_cwf_alternate_id if ("Agency" in item['employeetypename']) else item['employeeid'],
                "Date":  datetime.strptime(item['entrydate'], config.entry_date_format).strftime("%Y%m%d"),
                "Tasktype": "30" if item['approvalstatus'] == "Approved" else "20",
                "Costcenter": "C101099951" if item['iwoindicator'] == "X" or item['iwoindicator'] == "C1" else item['costcentercode'],
                "Activitytype": "HZD",
                "Recwbselement": item['parentwbs'] if item['iwoindicator'] == "X" or item['iwoindicator'] == "C1" and item['parentwbs'] else
                            item['projectname'] if item['masterwbs'] == "WBS" else None,
                "Recorder": item['parentserviceorder'] if item['iwoindicator'] == "X" or item['iwoindicator'] == "C1" and item['parentserviceorder'] else
                            item['projectname'] if item['masterwbs'] == "SO" else None,
                "Labortype": item['labortype'].split("|")[0] if item['labortype'] and '|' in item['labortype'] else None,
                "Billableindicator": "X" if "|Billable" in item['labortype'] else None,
                "Task": None if item['tasktype'] == 'GSAP Billing Key' else item['taskname'] if item[
                            'taskname'] and item['taskname'] != item['projectname'] else None,
                "Hours": round(float(item['hours']), 2),
                "Attendencetype": '1010' if item['iwoindicator'] == "X" or item['iwoindicator'] == "C1" else
                            item['timetype'].split("-")[0] if item['timetype'] and '-' in item['timetype'] else
                            item['timeofftypedescription'] if item['timeoffbookingid'] else
                            item['attendancetypecode'],
                "Comments": item['comments'],
                "Entryid": item['timeentryid'] if item['timeentryid'] else
                datetime.strptime(item['entrydate'], config.entry_date_format).strftime(
                    "%Y%m%d") + item['timeoffbookingid'],
                "Activitynumber": "3000002" if item['ratetype'] == "Double Time" else
                "3000001" if item['ratetype'] == "Overtime" else
                "3000000" if item['ratetype'] == "Straight Time" else None,
                "Sendingorder": next(reversed(po_order_list), {}).get('purchaseOrder', '') if len(po_order_list) > 0 else '',
                "Sendpoitem":  po_order_list[0].get('poItem', '') if len(po_order_list) > 0 else '',
            }.values()

        create_csv_lines_final_c1 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_final_c1',
            source="{{ result('query_list_filtered_data_c1') }}",
            header=[
                'Employeeid',
                'Date',
                'Tasktype',
                'Costcenter',
                'Activitytype',
                'Recwbselement',
                'Recorder',
                'Labortype',
                'Billableindicator',
                'Task',
                'Hours',
                'Attendencetype',
                'Comments',
                'Entryid',
                'Activitynumber',
                'Sendingorder',
                'Sendpoitem',
                'Oppid'
            ],
            row=map_row
        )

        load_csv_create_list_from_csv_c1final = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_c1final",
            document="{{result('create_csv_lines_final_c1')}}",
        )

        create_collection_create_list_from_csv_c1final = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_c1final',
            source="{{ result('load_csv_create_list_from_csv_c1final') }}",
            name="finaldata1"
        )

        query_list_c1final = rail.QueryCollectionOperator(
            task_id='query_list_c1final',
            query='''SELECT * FROM finaldata1''',
        )

        get_final_line = rail.PythonOperator(
            task_id="get_final_line",
            python_callable=python_callable_method.get_c1_final_line_data
        )

        get_final_line_collection = rail.CreateCollectionOperator(
            task_id="get_final_line_collection",
            source=lambda: rail.result('get_final_line'),
            name="getfinaldatacollection",
            columns={
                'Employeeid': 'Employeeid',
                'Date': 'Date',
                'Tasktype': 'Tasktype',
                'Costcenter': 'Costcenter',
                'Activitytype': 'Activitytype',
                'Recwbselement': 'Recwbselement',
                'Recorder': 'Recorder',
                'Labortype': 'Labortype',
                'Billableindicator': 'Billableindicator',
                'Task': 'Task',
                'Hours': 'Hours',
                'Attendencetype': 'Attendencetype',
                'Comments': 'Comments',
                'Entryid': 'Entryid',
                'Activitynumber': 'Activitynumber',
                'Sendingorder': 'Sendingorder',
                'Sendpoitem': 'Sendpoitem',
                'Oppid': 'Oppid'
            }
        )

        get_final_export_data = rail.QueryCollectionOperator(
            task_id='get_final_export_data',
            query='''SELECT * FROM (SELECT *,2 as filter FROM query_list_c1final UNION ALL
                    SELECT *,1 as filter FROM getfinaldatacollection) ORDER BY filter'''
        )

        create_document_c1_xml = rail.RenderTemplateOperator(
            task_id='create_document_c1_xml',
            target='artifact',
            template_file='xml_schema/c1_outbound.xml',
            dataset="{{ result('get_final_export_data') }}",
        )

        upload_xmlfile_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_xmlfile_to_sftp',
            content="{{ result('create_document_c1_xml') }}",
            remote_filepath=config.c1_output_filepath +
            '/{{ dag_run.conf.twbname }}.xml',
        )

        send_mail_timedatafileexportfailed_c1 = rail.EmailOperator(
            task_id='send_mail_timedatafileexportfailed_c1',
            to=config.tenant_email,
            trigger_rule='one_failed',
            subject='{{get_company_key()}} | C1 Time data export automation - SFTP upload failure - {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}} ',
            html_content='''<p>Hi Team,<br /> <br /> The C1 time date export has been completed at {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}},
                            however the file upload to sftp has failed with error.</p>
                            <ul>
                            <li>Recipe ID: {{ dag_run.dag_id}}</li>
                            <li>Job ID: {{ ecid() }}</li>
                            <li>Instance: {{ get_company_key() }}</li>
                            <li>File Name: {{ result('log_message_filename_c1')}}.xml</li>
                            <li>SFTP Path: {{ params.sftp_path}}</li>
                            <li>Error: ''' + config.error_template + ''' </li>
                            </ul>
                            <p>Please find the attached file to be uploaded to sftp.
                            Upload the file to the given sftp and debug the issue.<br /> <br /> Regards,<br /> Deltek Inc</p> ''',
            params={
                'sftp_path': config.c1_output_filepath
            },
            files=[
                ("{{ result('log_message_filename_c1')}}.xml",
                 '{{result("create_document_c1_xml")}}')
            ]

        )

        fail_sftp_upload_error = rail.FailOperator(
            task_id='fail_sftp_upload_error',
            message=config.error_template
        )

        send_mail_c1 = rail.EmailOperator(
            task_id='send_mail_c1',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon CWF time export for C1- Completed Successfully - {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}}',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
            The Replicon CWF time export for C1 job is Completed successfully at {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}}. Please find the file details below:<br /><br />
            File path: {{ params.sftp_path}} <br />
            File name: {{result('log_message_filename_c1')}}.xml <br/>
            Payload identifier: {{ dag_run.conf.payload_identifier_replicon_uniqueid }}
            <br /></p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params={
                'sftp_path': config.c1_output_filepath
            }
        )

        is_allowed_send_time_data = rail.IfOperator(
            task_id='is_allowed_send_time_data',
            test=config.is_allowed_send_export_data,
            yes_task='c1_time_data_upload',
            no_task='send_mail_c1'
        )

        c1_time_data_upload = rail.HTTPUploadFileOperator(
            task_id='c1_time_data_upload',
            method='POST',
            http_conn_id=config.c1_http_conn_id,
            content_type='application/xml',
            content="{{ result('create_document_c1_xml') }}",
            extra_options={
                'verify': False
            } if config.instance == "DXCSandbox" else None
        )

        get_last_time_export_details >> has_previous_export_processed >> rail.Label(
            "Yes") >> create_time_data_download_batch_c1

        has_previous_export_processed >> rail.Label(
            "No") >> process_acknowledgement_not_received >> wait_to_process_acknowledgement_not_received >> gather_all_unckn_export_details >>\
            get_unackn_email_content >> send_unackn_email >> fail_for_no_ackn

        create_time_data_download_batch_c1 >> batch_management_c1 >> get_c1_batch_result >> \
            log_message_filename_c1 >> read_file_c1 >> load_csv_create_list_from_csv_c1 >> \
            create_collection_create_list_from_csv_c1 >> has_row_count_c1

        has_row_count_c1 >> rail.Label(
            'No') >> get_final_line_no_data >> get_final_line_no_data_collection >> generate_xml_time_no_data >> send_time_no_data_to_sftp >> \
            is_allowed_send_export_no_data >> rail.Label(
                "No") >> send_mail_no_data

        is_allowed_send_export_no_data >> rail.Label(
            "Yes") >> upload_time_no_data >> send_mail_no_data

        has_row_count_c1 >> rail.Label(
            'Yes') >> query_list_filtered_data_c1 >> query_list_uniqueusers_c1 >> get_key_value_pobalance >> has_rows_c1_query_list_filtered_data

        has_rows_c1_query_list_filtered_data >> rail.Label(
            'Yes') >> create_csv_lines_final_c1 >> load_csv_create_list_from_csv_c1final >> create_collection_create_list_from_csv_c1final >> \
            query_list_c1final >> get_final_line >> get_final_line_collection >> get_final_export_data >> create_document_c1_xml >> upload_xmlfile_to_sftp

        has_rows_c1_query_list_filtered_data >> rail.Label(
            'No') >> get_final_line_no_filter_data >> get_final_line_no_filter_data_collection >> generate_xml_time_no_filter_data >> \
            send_time_no_filter_data_to_sftp >> is_allowed_send_export_no_filter_data >> rail.Label(
                "No") >> send_mail_no_filter_data

        is_allowed_send_export_no_filter_data >> rail.Label(
            "Yes") >> upload_time_no_filter_data >> send_mail_no_filter_data

        upload_xmlfile_to_sftp >> rail.Label(
            'error') >> send_mail_timedatafileexportfailed_c1 >> fail_sftp_upload_error

        upload_xmlfile_to_sftp >> rail.Label(
            'success') >> is_allowed_send_time_data >> rail.Label("No") >> send_mail_c1

        is_allowed_send_time_data >> rail.Label(
            "Yes") >> c1_time_data_upload >> send_mail_c1

    return dag


rail.for_each_instance(create_dag)
