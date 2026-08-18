from datetime import datetime, timedelta
import rail
from dxctechnology.cwf_time_export_v7.utils import python_callable_method
from dxctechnology.cwf_time_export_v7.utils import request_payload
from dxctechnology.cwf_time_export_v7.utils import response_filter


null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_cwf_time_export_psa_f142d_child_{config.instance}_v7',
        description=f'DXCTechnology_PSA F142D Child fro C1 IWO, Compass IWO, GSAP REG and GSAP IWO v7 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.psa_sftp_conn_id,
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
            yes_task="create_time_data_download_batch_psa",
            no_task='process_acknowledgement_not_received'
        )

        process_acknowledgement_not_received = rail.TriggerDagRunForEachItemOperator(
            task_id='process_acknowledgement_not_received',
            retries=0,
            items='{{ dag_run.conf.twb_list | to_json }}',
            trigger_dag_id=f'dxctechnology_acknowledgement_not_received_notification_{config.instance}_v7',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "name": item["name"],
                "uri": item["uri"],
                "createdatetime": item["createdatetime"],
                "oef_name": 'PSA_Payload_Processed',
                "twbname": python_callable_method.get_dag_run_conf()['twbname']+'_F142D',
                "erp": 'PSA',
                'sender': 'PSA'
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
            subject='{{ get_company_key() + " | Priority 2 : Payload acknowledgement not received for PSA " }}',
            html_content='{{ result("get_unackn_email_content")}}',
        )

        create_time_data_download_batch_psa = rail.RepliconServiceOperator(
            task_id='create_time_data_download_batch_psa',
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

        batch_management_psa = rail.batch_execution(
            group_id='batch_management_psa',
            creation_task_id=create_time_data_download_batch_psa.task_id,
        )

        get_psa_batch_result = rail.RepliconServiceOperator(
            task_id='get_psa_batch_result',
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults",
            data={
                "timeDataDownloadBatchUri": "{{ result('create_time_data_download_batch_psa') }}"
            }
        )

        read_file_psa = rail.HTTPDownloadFileOperator(
            task_id='read_file_psa',
            url='{{ result("get_psa_batch_result").downloadUrl }}',
        )

        load_csv_create_list_from_csv_psa = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_psa",
            document="{{result('read_file_psa')}}",
        )

        create_collection_create_list_from_csv_psa = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_psa',
            source="{{ result('load_csv_create_list_from_csv_psa') }}",
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
                'Organizational Unit Name': 'organizationunitname',
                'Location Code': 'locationcode',
                'Time Type (AUS) (Code)': 'timetypeauscode'
            }
        )

        has_row_count_psa = rail.IfOperator(
            task_id='has_row_count_psa',
            test="{{ result('create_collection_create_list_from_csv_psa','length') > 0 }}",
            yes_task="get_psa_cost_centers",
            no_task="get_final_line_no_data",
        )

        get_final_line_no_data = rail.PythonOperator(
            task_id="get_final_line_no_data",
            python_callable=python_callable_method.get_psa_f142d_final_line_data
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
            template_file='xml_schema/psa_outbound_f142d.xml'
        )

        pgp_encyrpt_time_no_data = rail.PGPEncryptionOperator(
            task_id="pgp_encyrpt_time_no_data",
            source="{{ result('generate_xml_time_no_data') }}",
            pgp_conn_id=config.pgp_conn_id_psa
        )

        send_time_no_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id='send_time_no_data_to_sftp',
            remote_filepath=config.psa_output_filepath +
            '/{{ dag_run.conf.twbname }}_F142D.xml.pgp',
            content='{{result(\'' + pgp_encyrpt_time_no_data.task_id + '\')}}',
        )

        send_mail_no_data = rail.EmailOperator(
            task_id='send_mail_no_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon CWF time extract for PSA - No records to export {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}} ',
            html_content="templates/psa_no_data_mail.html",
        )

        get_psa_cost_centers = rail.RepliconServiceOperator(
            task_id="get_psa_cost_centers",
            endpoint="/services/CostCenterListService1.svc/GetData",
            data=request_payload.get_psa_cost_centers,
            data_handler=response_filter.get_psa_cost_centers
        )

        get_psa_orgs = rail.RepliconServiceOperator(
            task_id="get_psa_orgs",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data=request_payload.get_psa_orgs,
            data_handler=response_filter.get_psa_orgs
        )

        query_list_filtered_data_psa = rail.QueryCollectionOperator(
            task_id='query_list_filtered_data_psa',
            query='''SELECT * FROM finaltimedata
                    WHERE
                        (((
                            employeetypename LIKE '%Contractor%' AND companycodecode = 'C1' AND attendancetypecode NOT LIKE '%799%' AND ParentWBS IS NOT Null
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND companycodecode = 'C1' AND CAST(hours as INT) = 0 AND ParentWBS IS NOT Null
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND iwoindicator= 'X' AND attendancetypecode NOT LIKE '%799%'
                            AND (parentproject IS NULL OR parentproject = '')
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND iwoindicator= 'X' AND CAST(hours as INT) = 0
                            AND (parentproject IS NULL OR parentproject = '')
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND iwoindicator= 'C1' AND attendancetypecode NOT LIKE '%799%'
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND iwoindicator= 'C1' AND CAST(hours as INT) = 0
                        ))
                        OR
                        ((
                            employeetypename LIKE '%Contractor%' AND companycodecode='COMPASS' AND attendancetypecode NOT LIKE '%799%' AND ParentWBS IS NOT Null
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND companycodecode='COMPASS' AND CAST(hours as INT) = 0 AND ParentWBS IS NOT Null
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND projecttype='ES' AND projectname LIKE 'E-%' AND attendancetypecode NOT LIKE '%799%'
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND projecttype='ES' AND projectname LIKE 'E-%' AND CAST(hours as INT) = 0
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND projecttype='CP' AND attendancetypecode NOT LIKE '%799%'
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND projecttype='CP' AND CAST(hours as INT) = 0
                        ))
                        OR
                        ((
                            employeetypename LIKE '%Contractor%' AND companycodename in ('3001', '3124', '1602', '3118') AND attendancetypecode NOT LIKE '%799%'
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND companycodename in ('3001', '3124', '1602', '3118') AND CAST(hours as INT) = 0
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND projecttype='IC' AND projectname LIKE 'X-%' AND attendancetypecode NOT LIKE '%799%'
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND projecttype='IC' AND projectname LIKE 'X-%' AND CAST(hours as INT) = 0
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND projecttype='GS' AND attendancetypecode NOT LIKE '%799%'
                        )
                        OR
                        (
                            employeetypename LIKE '%Contractor%' AND projecttype='GS' AND CAST(hours as INT) = 0
                        )))
                        AND
                        ((
                            psaflag IN ('x','X')
                        )
                        OR
                        (
                            costcentercode IN ({{result("get_psa_cost_centers")}})
                        )
                        OR
                        (
                            organizationunitname IN ({{result("get_psa_orgs")}})
                        ))
                        ORDER BY CAST(hours as DECIMAL) ASC
                        ''',
        )

        has_rows_psa_query_list_filtered_data = rail.IfOperator(
            task_id='has_rows_psa_query_list_filtered_data',
            test="{{ result('query_list_filtered_data_psa','length') > 0}}",
            yes_task="create_csv_lines_final_psa",
            no_task='get_final_line_no_filter_data'
        )

        get_final_line_no_filter_data = rail.PythonOperator(
            task_id="get_final_line_no_filter_data",
            python_callable=python_callable_method.get_psa_f142d_final_line_data,
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
            template_file='xml_schema/psa_outbound_f142d.xml'
        )

        pgp_encyrpt_time_no_filter_data = rail.PGPEncryptionOperator(
            task_id="pgp_encyrpt_time_no_filter_data",
            source="{{ result('generate_xml_time_no_filter_data') }}",
            pgp_conn_id=config.pgp_conn_id_psa
        )

        send_time_no_filter_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id='send_time_no_filter_data_to_sftp',
            remote_filepath=config.psa_output_filepath +
            '/{{ dag_run.conf.twbname }}_F142D.xml.pgp',
            content='{{result(\'' + pgp_encyrpt_time_no_filter_data.task_id + '\')}}'
        )

        send_mail_no_filter_data = rail.EmailOperator(
            task_id='send_mail_no_filter_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon CWF time extract for PSA - No records to export {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}} ',
            html_content="templates/psa_no_data_mail.html"
        )

        def map_row(item):
            return {
                "Employee_ID": item['employeeid'],
                "DATE": datetime.strptime(item['entrydate'], config.entry_date_format).strftime("%Y%m%d"),
                "WBS": (item['parentwbs'].rjust(8, "0") if item['parentwbs'] else item['projectname']) if item['projectname'] else
                    '9061' if item['attendancetypecode'] == '2087' else None,
                "Task": item['gsaptaskcode'] if item['gsaptask'] else None,
                "Hours": format(float(item['hours']), '.2f') if float(item['hours']) != 0 else '0',
                "RepliconUniqueID": item['timeentryid'] if item['timeentryid'] else item['timeoffbookingid'],
                "Comments": item['comments'] if item['comments'] else None,
                "Billing_Key": None if item['timeoffbookingid'] else ('00' if item['projecttype'] == 'CP' or item['iwoindicator'] == 'C1' else
                                            (item['taskname'] if item['tasktype'] == 'GSAP Billing Key' else None)),
                "Project_Key": None if item['timeoffbookingid'] else ("00" if (item['gsapbillableflag'] == "Billable") else ("01" if
                    item['gsapbillableflag'] == "Non-Billable" else ('00' if item['projecttype'] == 'CP' else None if item['projecttype'] == 'ES' else (('00' if
                    item['labortype'].endswith("Billable") else '01') if item['iwoindicator'] in ['X', 'C1'] else None)))),
                "Attendance_Type": '2082' if item['projecttype'] in ['IC', 'GS', 'ES'] or item['iwoindicator'] == 'X' else item['timetypeauscode'] if
                    item['timetypeauscode'] else item['attendancetypecode'] if item['attendancetypecode'] in ['2087', '2850'] else '2082',
                "Child_Project_WBS": (item['projectname']).rjust(8, "0") if item['parentwbs'] else None,
                "Child_Project_ERP": item['companycodecode'] if item['companycodecode'] else None,
                "IWO_WBS_Flag": None if item['wbstype'] == 'DIWO' else 'X' if item['parentwbs'] else None,
                "Parent_Project_ERP": "C1" if item['iwoindicator'] == 'X' or item['iwoindicator'] == 'C1' else "COMPASS" if item['projecttype'] == 'ES' or item[
                    'projecttype'] == 'CP' else "GSAP" if item['projecttype'] == 'IC' or item['projecttype'] == 'GS' or item['wbstype'] == "DIWO" else None,
                "Labour_Type": item['labortype'] if item['labortype'] and item['projecttype'] == 'CP' else item['labortype'].split("|")[0] if item[
                    'labortype'] and item['iwoindicator'] == 'C1' else item['labortype'] if item['labortype'] and item['projecttype'] == 'ES' else item[
                        'labortype'].split("|")[0] if item['labortype'] and item['iwoindicator'] == 'X' else None,
                "Home_ERP": item["companycodecode"]
            }.values()

        create_csv_lines_final_psa = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_final_psa',
            source="{{ result('query_list_filtered_data_psa') }}",
            header=[
                'Employee_ID',
                'DATE',
                'WBS',
                'Task',
                'Hours',
                'RepliconUniqueID',
                'Comments',
                'Billing_Key',
                'Project_Key',
                'Attendance_Type',
                'Child_Project_WBS',
                'Child_Project_ERP',
                'IWO_WBS_Flag',
                'Parent_Project_ERP',
                'Labour_Type',
                'Home_ERP'
            ],
            row=map_row
        )

        load_csv_create_list_from_csv_psa_final = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_psa_final",
            document="{{result('create_csv_lines_final_psa')}}",
        )

        create_collection_create_list_from_csv_psa_final = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_psa_final',
            source="{{ result('load_csv_create_list_from_csv_psa_final') }}",
            name="finaldata1"
        )

        query_list_psa_final = rail.QueryCollectionOperator(
            task_id='query_list_psa_final',
            query='''SELECT * FROM finaldata1''',
        )

        get_final_line = rail.PythonOperator(
            task_id="get_final_line",
            python_callable=python_callable_method.get_psa_f142d_final_line_data
        )

        get_final_line_collection = rail.CreateCollectionOperator(
            task_id="get_final_line_collection",
            source=lambda: rail.result('get_final_line'),
            name="getfinaldatacollection",
            columns={
                'Employee_ID': 'Employee_ID',
                'DATE': 'DATE',
                'WBS': 'WBS',
                'Task': 'Task',
                'Hours': 'Hours',
                'RepliconUniqueID': 'RepliconUniqueID',
                'Comments': 'Comments',
                'Billing_Key': 'Billing_Key',
                'Project_Key': 'Project_Key',
                'Attendance_Type': 'Attendance_Type',
                'Child_Project_WBS': 'Child_Project_WBS',
                'Child_Project_ERP': 'Child_Project_ERP',
                'IWO_WBS_Flag': 'IWO_WBS_Flag',
                'Parent_Project_ERP': 'Parent_Project_ERP',
                'Labour_Type': 'Labour_Type',
                'Home_ERP': 'Home_ERP'
            }
        )

        query_filtered_data_for_reversal = rail.QueryCollectionOperator(
            task_id='query_filtered_data_for_reversal',
            query='''SELECT * FROM finaltimedata WHERE timeentryid NOT IN (SELECT DISTINCT timeentryid FROM query_list_filtered_data_psa) AND hours IN (0, 0.0)'''
        )

        create_reversal_csv_lines_final_psa = rail.WriteCSVFileOperator(
            task_id='create_reversal_csv_lines_final_psa',
            source="{{ result('query_filtered_data_for_reversal') }}",
            header=[
                'Employee_ID',
                'DATE',
                'WBS',
                'Task',
                'Hours',
                'RepliconUniqueID',
                'Comments',
                'Billing_Key',
                'Project_Key',
                'Attendance_Type',
                'Child_Project_WBS',
                'Child_Project_ERP',
                'IWO_WBS_Flag',
                'Parent_Project_ERP',
                'Labour_Type',
                'Home_ERP'
            ],
            row=map_row
        )

        create_collection_create_list_from_reversal = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_reversal',
            source="{{ result('create_reversal_csv_lines_final_psa') }}",
            name="reversaldata"
        )

        get_final_export_data = rail.QueryCollectionOperator(
            task_id='get_final_export_data',
            query='''SELECT * FROM (SELECT *,2 as filter FROM query_list_psa_final UNION ALL
                    SELECT *,2 as filter FROM reversaldata UNION ALL
                    SELECT *,1 as filter FROM getfinaldatacollection) ORDER BY filter'''
        )

        create_document_psa_xml = rail.RenderTemplateOperator(
            task_id='create_document_psa_xml',
            target='artifact',
            template_file='xml_schema/psa_outbound_f142d.xml',
            dataset="{{ result('get_final_export_data') }}",
        )

        pgp_encyrpt_time_data = rail.PGPEncryptionOperator(
            task_id="pgp_encyrpt_time_data",
            source="{{ result('create_document_psa_xml') }}",
            pgp_conn_id=config.pgp_conn_id_psa
        )

        upload_xmlfile_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_xmlfile_to_sftp',
            content="{{ result('pgp_encyrpt_time_data') }}",
            remote_filepath=config.psa_output_filepath +
            '/{{ dag_run.conf.twbname }}_F142D.xml.pgp',
        )

        send_mail_timedatafileexportfailed_psa = rail.EmailOperator(
            task_id='send_mail_timedatafileexportfailed_psa',
            to=config.tenant_email,
            trigger_rule='one_failed',
            subject='{{get_company_key()}} | PSA Time data export automation - SFTP upload failure - {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}} ',
            html_content="templates/psa_failure_mail.html",
            params={
                'sftp_path': config.psa_output_filepath
            },
            files=[
                ("{{dag_run.conf.twbname}}_F142D.xml",
                 '{{result("create_document_psa_xml")}}')
            ]

        )

        fail_sftp_upload_error = rail.FailOperator(
            task_id='fail_sftp_upload_error',
            message=config.error_template
        )

        send_mail_psa = rail.EmailOperator(
            task_id='send_mail_psa',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon CWF time export for PSA - Completed Successfully - {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}}',
            html_content="templates/psa_success_mail.html",
            params={
                'sftp_path': config.psa_output_filepath
            }
        )

        get_all_oefs_for_the_exports = rail.RepliconServiceOperator(
            task_id = 'get_all_oefs_for_the_exports',
            endpoint= '/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings',
            data= {
                    "bindingContextUri": "urn:replicon:object-type:time-data-export"
                },
            response_filter= response_filter.get_psa_oef_uris
        )

        acknowledge_current_export= rail.RepliconServiceOperator(
            task_id = 'acknowledge_current_export',
            endpoint= '/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data= request_payload.get_acknowlegement_payload
        )

        get_last_time_export_details >> has_previous_export_processed >> rail.Label(
            "Yes") >> create_time_data_download_batch_psa

        has_previous_export_processed >> rail.Label(
            "No") >> process_acknowledgement_not_received >> wait_to_process_acknowledgement_not_received >> gather_all_unckn_export_details >>\
            get_unackn_email_content >> send_unackn_email >> create_time_data_download_batch_psa

        create_time_data_download_batch_psa >> batch_management_psa >> get_psa_batch_result >> \
            read_file_psa >> load_csv_create_list_from_csv_psa >> \
            create_collection_create_list_from_csv_psa >> has_row_count_psa

        has_row_count_psa >> rail.Label(
            'No') >> get_final_line_no_data >> get_final_line_no_data_collection >> generate_xml_time_no_data >> pgp_encyrpt_time_no_data >> \
            send_time_no_data_to_sftp >> send_mail_no_data

        send_mail_no_data >> get_all_oefs_for_the_exports >> acknowledge_current_export

        has_row_count_psa >> rail.Label(
            'Yes') >> get_psa_cost_centers >> get_psa_orgs >> query_list_filtered_data_psa >> has_rows_psa_query_list_filtered_data

        has_rows_psa_query_list_filtered_data >> rail.Label(
            'Yes') >> create_csv_lines_final_psa >> load_csv_create_list_from_csv_psa_final >> create_collection_create_list_from_csv_psa_final >> \
            query_list_psa_final >> get_final_line >> get_final_line_collection >> query_filtered_data_for_reversal >> create_reversal_csv_lines_final_psa >> \
            create_collection_create_list_from_reversal >> get_final_export_data >> create_document_psa_xml >> \
                pgp_encyrpt_time_data >> upload_xmlfile_to_sftp

        has_rows_psa_query_list_filtered_data >> rail.Label(
            'No') >> get_final_line_no_filter_data >> get_final_line_no_filter_data_collection >> generate_xml_time_no_filter_data >> \
            pgp_encyrpt_time_no_filter_data >> send_time_no_filter_data_to_sftp >> send_mail_no_filter_data

        send_mail_no_filter_data >> get_all_oefs_for_the_exports >> acknowledge_current_export

        upload_xmlfile_to_sftp >> rail.Label(
            'error') >> send_mail_timedatafileexportfailed_psa >> fail_sftp_upload_error

        upload_xmlfile_to_sftp >> rail.Label(
            'success') >> send_mail_psa

        send_mail_psa >> get_all_oefs_for_the_exports >> acknowledge_current_export

    return dag


rail.for_each_instance(create_dag)
