from datetime import datetime
import itertools
import json
import rail

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/cwf_time_export/config.py

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_cwf_time_export_c1_child_{config.instance}',
        description=f'DXCTechnology_CWF Time export - C1 V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.c1_sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

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
            python_callable=lambda: f'RepliconCWFTimetoC1_{datetime.utcnow().strftime("%m%d%YT%H%M")}'
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
            no_task="update_time_data_export_nodata",
        )

        update_time_data_export_nodata = rail.RepliconServiceOperator(
            task_id='update_time_data_export_nodata',
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{dag_run.conf.timeexporturi}}",
                    "name": null
                },
                "name": "{{dag_run.conf.twbname}}_Nodata"
            }
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
                map(lambda item:  {'key': item['key'], 'jsonValue': json.loads(item['jsonValue'])},
                    filter(lambda item: item, data))),
        )

        has_rows_c1_query_list_filtered_data = rail.IfOperator(
            task_id='has_rows_c1_query_list_filtered_data',
            test="{{ result('query_list_filtered_data_c1','length') > 0}}",
            yes_task="create_csv_lines_final_c1",
        )

        def map_row(item):
            # item = dict((k.lower().replace('_', ''), v.lower())
            #             for k, v in item.items())
            po_order_list = list(filter(lambda x: x['loginName'] == item['loginname'] and
                                        datetime.strptime(x['itemStartDate'], config.output_date_format) <=
                                        datetime.strptime(item['entrydate'], config.entry_date_format) and
                                        datetime.strptime(x['itemEndDate'], config.output_date_format) >= datetime.strptime(
                item['entrydate'], config.entry_date_format),
                list(itertools.chain(*map(lambda x: x['jsonValue'], rail.result('get_key_value_pobalance'))))))

            return {
                "Employeeid": item['perner'] if item['iwoindicator'] and item['iwoindicator'] == "X" else
                item['c1cwfalternateid'] if "Agency" in item['employeetypename'] else
                item['employeeid'],
                "Date":  datetime.strptime(item['entrydate'], config.entry_date_format).strftime("%Y%m%d"),
                "Tasktype": "30" if item['approvalstatus'] == "Approved" else "20",
                "Costcenter": "C101099951" if item['iwoindicator'] == "X" else item['costcentercode'],
                "Activitytype": "HZD",
                "Recwbselement": item['parentwbs'] if item['iwoindicator'] == "X" and item['parentwbs'] else
                            item['projectname'] if item['masterwbs'] == "WBS" else None,
                "Recorder": item['parentserviceorder'] if item['iwoindicator'] == "X" and item['parentserviceorder'] else
                            item['projectname'] if item['masterwbs'] == "SO" else None,
                "Labortype": item['labortype'].split("|")[0] if item['labortype'] and '|' in item['labortype'] else None,
                "Billableindicator": "X" if "|Billable" in item['labortype'] else None,
                "Task": item['taskname'] if item['taskname'] and item['taskname'] != item['projectname'] else None,
                "Hours": round(float(item['hours']), 2),
                "Attendencetype": '1010' if item['iwoindicator'] == "X" else
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
                "Sendingorder": next(reversed(po_order_list), {}).get('purchaseOrder', '') if len(po_order_list)>0 else '',
                "Sendpoitem":  po_order_list[0].get('poItem', '') if len(po_order_list)>0 else '',
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
                'Sendpoitem'
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

        create_document_c1_xml = rail.RenderTemplateOperator(
            task_id='create_document_c1_xml',
            target='artifact',
            template_file='c1_outbound.xml',
            dataset="{{ result('query_list_c1final') }}",
        )

        upload_xmlfile_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_xmlfile_to_sftp',
            content="{{ result('create_document_c1_xml') }}",
            remote_filepath=config.c1_output_filepath +
            '/{{ result("log_message_filename_c1") }}.xml',
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
            File name: {{result('log_message_filename_c1')}}.xml
            <br /></p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params={
                'sftp_path': config.c1_output_filepath
            }
        )

        c1_time_data_upload = rail.HTTPUploadFileOperator(
            task_id='c1_time_data_upload',
            method='POST',
            http_conn_id=config.c1_http_conn_id,
            content_type='application/xml',
            content="{{ result('create_document_c1_xml') }}",
            extra_options= {
                'verify': False
            } if config.instance == "DXCSandbox" else None
        )

        create_time_data_download_batch_c1 >> batch_management_c1 >> get_c1_batch_result >> \
            log_message_filename_c1 >> read_file_c1 >> load_csv_create_list_from_csv_c1 >> create_collection_create_list_from_csv_c1 >> has_row_count_c1
        has_row_count_c1 >> rail.Label(
            'No') >> update_time_data_export_nodata
        has_row_count_c1 >> rail.Label(
            'Yes') >> query_list_filtered_data_c1 >> query_list_uniqueusers_c1 >> get_key_value_pobalance >> has_rows_c1_query_list_filtered_data
        has_rows_c1_query_list_filtered_data >> rail.Label(
            'Yes') >> create_csv_lines_final_c1 >> load_csv_create_list_from_csv_c1final >> create_collection_create_list_from_csv_c1final >> \
            query_list_c1final >> create_document_c1_xml >> upload_xmlfile_to_sftp
        upload_xmlfile_to_sftp >> rail.Label(
            'error') >> send_mail_timedatafileexportfailed_c1 >> fail_sftp_upload_error
        upload_xmlfile_to_sftp >> rail.Label('success') >> send_mail_c1 >> c1_time_data_upload \

    return dag


rail.for_each_instance(create_dag)
