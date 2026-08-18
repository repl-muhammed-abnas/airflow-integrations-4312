from datetime import timedelta
import pendulum
from dxctechnology.time_export.gsap_outbound.utils import request_payload, custom_methods
from airflow.models import Variable
import rail

null = None

def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.gsap_iwo_create_time_export_child_dagid,
        description=f"DXC - GSAP IWO Time Export Create GSAP IWO Time export child - {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_child_dag_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_download_batch'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_download_batch',
            end_task='batch_end',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=lambda dag_run: request_payload.get_create_download_batch(dag_run.conf["timeexporturi"], dag_run.conf["fileformaturi"])
        )

        execute_download_batch, wait_for_download_batch = rail.batch_execution(
            group_id='execute_download_batch',
            creation_task_id=create_download_batch.task_id
        )

        get_download_url = rail.RepliconServiceOperator(
            task_id='get_download_url',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults',
            data={
                "timeDataDownloadBatchUri": "{{ result('create_download_batch') }}"
            },
            data_handler=lambda response: response['downloadUrl']
        )

        download_export = rail.HTTPDownloadFileOperator(
            task_id='download_export',
            url="{{ result('get_download_url') }}",
        )

        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{ result('download_export') }}"
        )

        data_existence_var = rail.SetVariableOperator(
            task_id='data_existence_var',
            name='data_existence',
            value=[]
        )

        create_final_time_data_collection = rail.CreateCollectionOperator(
            task_id='create_final_time_data_collection',
            source='{{ result("load_export") }}',
            columns={
                "Company Code Code": "companycodecode",
                "Employee ID": "employeeid",
                "PERNER": "perner",
                "Approval Status": "approvalstatus",
                "Entry Date": "entrydate",
                "WBS / SO Name": "projectname",
                "Cost Center Name": "costcentercode",
                "Labor Type Name": "labortype",
                "Job Activity Type": "jobactivitytype",
                "Task Name": "taskname",
                "Time Type US": "timetype",
                "Attendance Type Code": "attendancetypecode",
                "Billable Indicator": "billableindicator",
                "Hours (Current)": "hours",
                "Rate Type": "ratetype",
                "Short Time Entry ID": "timeentryid",
                "Time Off Booking ID": "timeoffbookingid",
                "Comments": "comments",
                "WBS Type": "wbstype",
                "Task Task Type": "tasktype",
                "New Remaining Work": "newremainningwork",
                "Customer 1": "customer1",
                "Customer 2": "customer2",
                "Customer 3": "customer3",
                "GSAP Billable Flag": "gsapbillableflag",
                "Time Off Type Description": "timeofftypedescription",
                "Master WBS (SO, WO)": "masterwbs",
                "Project Type": "projecttype",
                "IWO Indicator": "iwoindicator",
                "Parent WBS": "parentwbs",
                "Company Code Name": "companycodename",
                "Task Name (Full Path)": "taskfullpath",
                "Time Entry ID": "timeentryid2",
                "Parent Service Order": "parentserviceorder",
                "International Assignee": "internationalassignee",
                "IA PERNER ID": "iapernerid",
                "IWO WBS Element": "iwowbs",
                "Beeper Pay": "beeperpay",
                "Parent Project": "parentproject",
                "Oncall/Standby": "oncallstandby",
                "Time Type US 2": "timetype2",
                "Name": "breaktypename",
                "Time Off Type Name": "timeofftypename",
                "Oncall / Standby": "oncallstandby2",
                "Attribute 1 (Code)": "attribute1code",
                "Attribute 2 (Code)": "attribute2code",
                "Actual Employee ID": "actualempid",
                "GSAP Reference Number": "gsapreferencenumber",
                "Personnel Area Code": "personnelareacode",
                "Time Type (AUS) (Code)": "timetypeauscode",
                "Stand by (AUS)": "standbyauscode",
                "Parent WBS Code": "parentwbscode",
                "PSA Flag": "psaflag",
                "GSAP Task": "gsaptask",
                "GSAP Task (Code)": "gsaptaskcode",
                "Employee Type Name": "employeetype",
                "Employee Type Code": "employeetypecode",
                "Organizational Unit Name": "organizationalunitname",
                "Time Type BFI": "timetypebfi",
                "Supplemental Pay": "supplementalpay"
            },
            name='finaltimedata'
        )

        query_entries_without_shortid = rail.QueryCollectionOperator(
            task_id='query_entries_without_shortid',
            query="SELECT * FROM finaltimedata WHERE NULLIF(timeentryid, '') IS NULL"
        )

        if_short_id_not_exists = rail.IfOperator(
            task_id='if_short_id_not_exists',
            test='{{ result("query_entries_without_shortid", "length") > 0 }}',
            yes_task='fail_missing_short_id',
            no_task='if_final_timedata_has_data'
        )

        fail_missing_short_id = rail.FailOperator(
            task_id='fail_missing_short_id',
            message="Entries missing Short IDs"
        )

        if_final_timedata_has_data = rail.IfOperator(
            task_id='if_final_timedata_has_data',
            test='{{ result("create_final_time_data_collection", "length") > 0 }}',
            yes_task='get_timeoff_types_to_export',
            no_task='empty_final_data'
        )

        empty_final_data = rail.EmptyOperator(
            task_id='empty_final_data'
        )

        get_timeoff_types_to_export = rail.PythonOperator(
            task_id='get_timeoff_types_to_export',
            python_callable=lambda: Variable.get(config.timeoff_types_to_export, deserialize_json=True)
        )

        get_query_to_filter_time_export_data = rail.PythonOperator(
            task_id='get_query_to_filter_time_export_data',
            python_callable=custom_methods.get_query_to_filter_iwo_time_export_data
        )

        query_filter_time_export_data = rail.QueryCollectionOperator(
            task_id='query_filter_time_export_data',
            query='{{ result("get_query_to_filter_time_export_data") }}',
            name='filtered_time_export_data'
        )

        if_filtered_timedata_has_data = rail.IfOperator(
            task_id='if_filtered_timedata_has_data',
            test='{{ result("query_filter_time_export_data", "length") > 0 }}',
            yes_task='write_final_data_for_processing_csv',
            no_task='empty_filtered_data'
        )

        empty_filtered_data = rail.EmptyOperator(
            task_id='empty_filtered_data'
        )

        log_no_data_var = rail.SetVariableOperator(
            task_id='log_no_data_var',
            name='data_existence',
            value={
                "type": "No Data",
                "count": "0"
            }
        )

        write_final_data_for_processing_csv = rail.WriteCSVFileOperator(
            task_id='write_final_data_for_processing_csv',
            source='{{ result("query_filter_time_export_data") }}',
            header=config.export_columns,
            row=custom_methods.get_write_final_iwo_data_for_processing_csv
        )

        final_data_for_processing = rail.CreateCollectionOperator(
            task_id='final_data_for_processing',
            source='{{ result("write_final_data_for_processing_csv") }}',
            name='final_data_for_processing'
        )

        log_data_exist_var = rail.SetVariableOperator(
            task_id='log_data_exist_var',
            name='data_existence',
            value={
                "type": "Data",
                "count": "{{ result('final_data_for_processing', 'length') }}"
            }
        )

        get_data_existence_var = rail.GetVariableOperator(
            task_id='get_data_existence_var',
            name='data_existence'
        )

        create_document_gsap_xml = rail.RenderTemplateOperator(
            task_id='create_document_gsap_xml',
            target='artifact',
            template_file='xml_schema/gsap_outbound.xml',
            dataset=custom_methods.add_starting_line_to_final_data
        )

        upload_gsap_iwo_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_gsap_iwo_file_to_sftp',
            content="{{ result('create_document_gsap_xml') }}",
            remote_filepath=config.output_filepath + '/{{ dag_run.conf.twbname }}.xml',
        )

        check_any_failures = rail.IfOperator(
            task_id='check_any_failures',
            trigger_rule='all_done',
            test='{{ get_error_message() | is_truthy }}',
            yes_task='is_sftp_upload_failed',
            no_task='process_upload_sftp_success'
        )

        is_sftp_upload_failed = rail.IfOperator(
            task_id='is_sftp_upload_failed',
            test='{{ get_task_state("upload_gsap_iwo_file_to_sftp").lower() == "failed" }}',
            yes_task='generate_download_link',
            no_task='fail_dag'
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_document_gsap_xml')}}",
            output_file_name='{{ dag_run.conf.twbname }}.xml',
            expires_in_seconds=config.s3_download_link_expiry
        )

        send_gsap_time_data_file_sftp_failed_email = rail.EmailOperator(
            task_id='send_gsap_time_data_file_sftp_failed_email',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon time extract for GSAP - SFTP upload failure - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/gsap_sftp_failure.html",
            params={
                'sftp_path': config.output_filepath
            }
        )

        process_upload_sftp_success = rail.EmptyOperator(
            task_id='process_upload_sftp_success'
        )

        is_final_data_size_greater_than_record_count = rail.IfOperator(
            task_id='is_final_data_size_greater_than_record_count',
            test=lambda: (rail.result("get_data_existence_var")["value"]["type"]) == "Data" and
                int(rail.result("get_data_existence_var")["value"]["count"]) > config.record_count_limit,
            yes_task='dag_run_log_to_sumo_threshold',
            no_task='gsap_time_data_upload_http'
        )

        dag_run_log_to_sumo_threshold = rail.DagRunLogToSumoOperator(
            task_id='dag_run_log_to_sumo_threshold',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info=lambda kwargs: {
                "payloadidentifier": "{{ dag_run.conf.payload_identifier_replicon_uniqueid }}",
                "exporttype": "Employee",
                "downstreamapp": "GSAP",
                "twbrowcount": "{{ result('create_final_time_data_collection', 'length') }}",
                "twbname": "{{ dag_run.conf.twbname }}",
                "exportrowcount": "{{ result('get_data_existence_var').value.count }}",
                "exportfilepath": config.output_filepath,
                "jobstarttime": "{{ dag_run.conf.process_start_time }}",
                "jobendtime": pendulum.now(config.utc_timezone).isoformat(),
                "payloadthresold": "Yes",
                "exportscheduletime": "{{ dag_run.conf.process_start_time }}",
                "exportfilename": "{{ dag_run.conf.twbname }}.xml"
            }
        )

        fail_record_threshold = rail.FailOperator(
            task_id='fail_record_threshold',
            message='Payload threshold exceeded'
        )

        gsap_time_data_upload_http = rail.HTTPUploadFileOperator(
            task_id='gsap_time_data_upload_http',
            method='POST',
            http_conn_id=config.gsap_http_conn_id,
            content_type='application/xml',
            content="{{ result('create_document_gsap_xml') }}"
        )

        update_oef_to_acknowledge_current_export= rail.RepliconServiceOperator(
            task_id='update_oef_to_acknowledge_current_export',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_update_oef_acknowlegement_payload
        )

        if_no_data = rail.IfOperator(
            task_id='if_no_data',
            test=lambda: (rail.result("get_data_existence_var")["value"]["type"]) == "No Data",
            yes_task='send_no_data_email',
            no_task='send_gsap_export_complete_email'
        )

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon time extract for GSAP - No records to export - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/gsap_no_data.html",
        )

        send_gsap_export_complete_email = rail.EmailOperator(
            task_id='send_gsap_export_complete_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon time extract for GSAP - Completed Successfully - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/gsap_complete.html",
            params={
                'sftp_path': config.output_filepath
            }
        )

        dag_run_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dag_run_log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "payloadidentifier": "{{ dag_run.conf.payload_identifier_replicon_uniqueid }}",
                "exporttype": "Employee",
                "downstreamapp": "GSAP",
                "twbrowcount": "{{ result('create_final_time_data_collection', 'length') }}",
                "twbname": "{{ dag_run.conf.twbname }}",
                "exportrowcount": "{{ result('get_data_existence_var').value.count }}",
                "exportfilepath": config.output_filepath,
                "jobstarttime": "{{ dag_run.conf.process_start_time }}",
                "jobendtime": pendulum.now(config.utc_timezone).isoformat(),
                "payloadthresold": "No",
                "exportscheduletime": "{{ dag_run.conf.process_start_time }}",
                "exportfilename": "{{ dag_run.conf.twbname }}.xml"
            }
        )

        fail_dag = rail.FailOperator(
            task_id='fail_dag',
            message=config.error_template
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> batch_end
        can_run_batch_task >> rail.Label('No') >> create_download_batch

        create_download_batch >> execute_download_batch >> wait_for_download_batch >> get_download_url \
            >> download_export >> load_export >> data_existence_var \
                >> create_final_time_data_collection >> query_entries_without_shortid >> if_short_id_not_exists

        if_short_id_not_exists >> rail.Label("Yes") >> fail_missing_short_id
        if_short_id_not_exists >> rail.Label("No") >> if_final_timedata_has_data

        if_final_timedata_has_data >> rail.Label("Yes") >> get_timeoff_types_to_export \
            >> get_query_to_filter_time_export_data >> query_filter_time_export_data

        query_filter_time_export_data >> if_filtered_timedata_has_data
        if_final_timedata_has_data >> rail.Label("No") >> empty_final_data >> log_no_data_var

        if_filtered_timedata_has_data >> rail.Label("Yes") >> write_final_data_for_processing_csv \
            >> final_data_for_processing >> log_data_exist_var >> get_data_existence_var
        if_filtered_timedata_has_data >> rail.Label("No") >> empty_filtered_data >> log_no_data_var

        log_no_data_var >> get_data_existence_var >> create_document_gsap_xml >> upload_gsap_iwo_file_to_sftp \
            >> check_any_failures

        check_any_failures >> rail.Label("Yes") >> is_sftp_upload_failed
        check_any_failures >> rail.Label("No") >> process_upload_sftp_success >> is_final_data_size_greater_than_record_count

        is_sftp_upload_failed >> rail.Label("Yes") >> generate_download_link >> send_gsap_time_data_file_sftp_failed_email >> is_final_data_size_greater_than_record_count

        is_sftp_upload_failed >> rail.Label("No") >> fail_dag >> batch_end

        is_final_data_size_greater_than_record_count >> rail.Label("Yes") >> dag_run_log_to_sumo_threshold \
            >> fail_record_threshold >> batch_end
        is_final_data_size_greater_than_record_count >> rail.Label("No") >> gsap_time_data_upload_http \
            >> update_oef_to_acknowledge_current_export >> if_no_data

        if_no_data >> rail.Label("Yes") >> send_no_data_email >> dag_run_log_to_sumo >> batch_end
        if_no_data >> rail.Label("No") >> send_gsap_export_complete_email >> dag_run_log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
