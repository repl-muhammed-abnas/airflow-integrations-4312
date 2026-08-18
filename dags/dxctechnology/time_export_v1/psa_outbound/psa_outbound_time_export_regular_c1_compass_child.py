from datetime import timedelta
import pendulum
from dxctechnology.time_export_v1.psa_outbound.utils import request_payload, response_filters, custom_methods
from airflow.models import Variable
import rail

null = None
export_columns = ["employeeid","date","wbs","attendancetypecode","labortype","billableindicator",
    "hours","task","attributecode1","attributecode2","shorttext","billingkey","gsaptask","gsapbillableflag",
    "repliconuniqueid","homeerp","parentprojecterp","homelocation","timeoff","projecttypeflag"]

def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.psa_outbound_time_export_reg_c1_compass_child_dagid,
        description=f"DXC - PSA Outboud Time Export Create PSA Regular C1/Compass Time export child - {config.instance}",
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
            no_task='can_export_psa_regular_timedata'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='can_export_psa_regular_timedata',
            end_task='batch_end',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        can_export_psa_regular_timedata = rail.IfOperator(
            task_id='can_export_psa_regular_timedata',
            test=lambda: Variable.get(config.time_data_posting_mapper, deserialize_json=True)["PSA"]["posting"].lower() == "yes",
            yes_task='get_last_time_export_psa_details',
            no_task='batch_end'
        )

        get_last_time_export_psa_details = rail.RepliconServiceOperator(
            task_id='get_last_time_export_psa_details',
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataExportDetails",
            data={
                "target": {
                    "uri": "{{ dag_run.conf.lasttwburi }}",
                    "name": null
                }
            }
        )

        is_unckn_export_extension_field_value_present_for_psa = rail.IfOperator(
            task_id="is_unckn_export_extension_field_value_present_for_psa",
            test=lambda dag_run: response_filters.get_specific_time_export_details(
                rail.result("get_last_time_export_psa_details")['extensionFieldValues'],
                    dag_run.conf["oefname"]),
            yes_task='process_acknowledgement_not_received',
            no_task='create_download_batch'
        )

        process_acknowledgement_not_received = rail.TriggerDagRunOperator(
            task_id='process_acknowledgement_not_received',
            retries=0,
            trigger_dag_id=config.psa_outbound_acknowledgement_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_conf_for_process_ack_payload
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
                "Time Type": "timetype",
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
                "Time Type US": "timetype2",
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
                "Organizational Unit Name": "orgunitname",
                "Location Name": "locationname",
                "Location Code": "locationcode",
                "Time Type BFI": "timetypebfi",
                "Supplemental Pay": "supplementalpay",
                "PROF Supplemental Pay": "profsupplementalpay"
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
            yes_task='get_psa_org_unit',
            no_task='empty_final_data'
        )

        empty_final_data = rail.EmptyOperator(
            task_id='empty_final_data'
        )

        get_psa_org_unit = rail.RepliconServiceOperator(
            task_id='get_psa_org_unit',
            endpoint="/services/DepartmentGroupService1.svc/GetAllDepartmentGroups",
            data_handler=response_filters.get_psa_org_unit_uri
        )

        get_psa_org_unit_child_hierarchy = rail.RepliconServiceOperator(
            task_id='get_psa_org_unit_child_hierarchy',
            endpoint="/services/DepartmentGroupListService1.svc/GetChildHierarchyData",
            data=request_payload.get_psa_org_child_hierarchy_payload,
            data_handler=response_filters.get_psa_child_hierarchy_list
        )

        get_query_to_filter_time_export_data = rail.PythonOperator(
            task_id='get_query_to_filter_time_export_data',
            python_callable=custom_methods.get_query_to_filter_c1_compass_time_export_data,
            op_args=[config.timeoff_types_to_exclude]
        )

        query_filter_c1_reg_iwo_time_export_data = rail.QueryCollectionOperator(
            task_id='query_filter_c1_reg_iwo_time_export_data',
            query='{{ result("get_query_to_filter_time_export_data").c1_export_query }}',
            name='filtered_c1_reg_iwo_time_export_data'
        )

        query_c1_reg_iwo_ineligible_and_reversals = rail.QueryCollectionOperator(
            task_id='query_c1_reg_iwo_ineligible_and_reversals',
            query="""SELECT * FROM finaltimedata ftd WHERE ftd.timeentryid
                NOT IN (SELECT DISTINCT filtd.timeentryid FROM filtered_c1_reg_iwo_time_export_data filtd)
                AND CAST(ftd.hours AS FLOAT) = 0 AND companycodecode = 'C1'""",
            name='c1_reg_iwo_ineligible_and_reversals'
        )

        merge_c1_reg_iwo_filtered_and_reversals = rail.QueryCollectionOperator(
            task_id='merge_c1_reg_iwo_filtered_and_reversals',
            query="SELECT * FROM filtered_c1_reg_iwo_time_export_data UNION ALL SELECT * FROM c1_reg_iwo_ineligible_and_reversals",
            name='c1_reg_iwo_filtered_and_reversals'
        )

        query_filter_compass_reg_iwo_time_export_data = rail.QueryCollectionOperator(
            task_id='query_filter_compass_reg_iwo_time_export_data',
            query='{{ result("get_query_to_filter_time_export_data").compass_export_query }}',
            name='filtered_compass_reg_iwo_time_export_data'
        )

        query_compass_reg_iwo_ineligible_and_reversals = rail.QueryCollectionOperator(
            task_id='query_compass_reg_iwo_ineligible_and_reversals',
            query="""SELECT * FROM finaltimedata ftd WHERE ftd.timeentryid
                NOT IN (SELECT DISTINCT filtd.timeentryid FROM filtered_compass_reg_iwo_time_export_data filtd)
                AND CAST(ftd.hours AS FLOAT) = 0 AND companycodecode = 'COMPASS'""",
            name='compass_reg_iwo_ineligible_and_reversals'
        )

        merge_compass_reg_iwo_filtered_and_reversals = rail.QueryCollectionOperator(
            task_id='merge_compass_reg_iwo_filtered_and_reversals',
            query="SELECT * FROM filtered_compass_reg_iwo_time_export_data UNION ALL SELECT * FROM compass_reg_iwo_ineligible_and_reversals",
            name='compass_reg_iwo_filtered_and_reversals'
        )

        if_filtered_timedata_has_data = rail.IfOperator(
            task_id='if_filtered_timedata_has_data',
            test=lambda: rail.result("merge_c1_reg_iwo_filtered_and_reversals", key="length") > 0 or
                rail.result("merge_compass_reg_iwo_filtered_and_reversals", key="length") > 0,
            yes_task='final_c1_reg_iwo_time_export_data',
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

        final_c1_reg_iwo_time_export_data = rail.CreateCollectionOperator(
            task_id='final_c1_reg_iwo_time_export_data',
            source=custom_methods.get_final_c1_reg_iwo_time_export_data,
            columns=export_columns,
            name='final_c1_reg_iwo_time_export_data'
        )

        final_compass_reg_iwo_time_export_data = rail.CreateCollectionOperator(
            task_id='final_compass_reg_iwo_time_export_data',
            source=custom_methods.get_final_compass_reg_iwo_time_export_data,
            columns=export_columns,
            name='final_compass_reg_iwo_time_export_data'
        )

        final_data_for_processing = rail.QueryCollectionOperator(
            task_id='final_data_for_processing',
            query="SELECT * FROM final_c1_reg_iwo_time_export_data UNION ALL SELECT * FROM final_compass_reg_iwo_time_export_data",
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

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        create_document_psa_xml = rail.RenderTemplateOperator(
            task_id='create_document_psa_xml',
            target='artifact',
            template_file='xml_schema/psa_outbound_c1_compass.xml',
            dataset=custom_methods.add_starting_line_to_c1_compass_final_data
        )

        encrypt_time_export_data = rail.PGPEncryptionOperator(
            task_id='encrypt_time_export_data',
            pgp_conn_id=config.pgp_conn_id,
            source='{{ result("create_document_psa_xml") }}'
        )

        upload_psa_regular_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_psa_regular_file_to_sftp',
            content="{{ result('encrypt_time_export_data') }}",
            remote_filepath=config.output_filepath + '/{{ dag_run.conf.twbname }}.xml.pgp',
        )

        update_oef_to_acknowledge_current_export= rail.RepliconServiceOperator(
            task_id='update_oef_to_acknowledge_current_export',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_update_oef_acknowlegement_payload
        )

        check_any_failures = rail.IfOperator(
            task_id='check_any_failures',
            trigger_rule='all_done',
            test='{{ get_error_message() | is_truthy }}',
            yes_task='is_sftp_upload_or_update_ack_failed',
            no_task='is_final_data_size_greater_than_record_count'
        )

        is_sftp_upload_or_update_ack_failed = rail.IfOperator(
            task_id='is_sftp_upload_or_update_ack_failed',
            test='{{ get_task_state("upload_psa_regular_file_to_sftp").lower() == "failed" or \
                get_task_state("update_oef_to_acknowledge_current_export").lower() == "failed"}}',
            yes_task='send_psa_time_data_file_sftp_failed_email',
            no_task='fail_dag'
        )

        send_psa_time_data_file_sftp_failed_email = rail.EmailOperator(
            task_id='send_psa_time_data_file_sftp_failed_email',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon time extract for PSA - SFTP upload failure - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/psa_sftp_failure.html",
            params={
                'sftp_path': config.output_filepath
            },
            files=[('{{ dag_run.conf.twbname }}.xml', '{{result("create_document_psa_xml")}}')]
        )

        is_final_data_size_greater_than_record_count = rail.IfOperator(
            task_id='is_final_data_size_greater_than_record_count',
            test=lambda: (rail.result("get_data_existence_var")["value"]["type"]) == "Data" and
                int(rail.result("get_data_existence_var")["value"]["count"]) > config.record_count_limit,
            yes_task='dag_run_log_to_sumo_threshold',
            no_task='if_no_data'
        )

        dag_run_log_to_sumo_threshold = rail.DagRunLogToSumoOperator(
            task_id='dag_run_log_to_sumo_threshold',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "payloadidentifier": "{{ dag_run.conf.payload_identifier_replicon_uniqueid }}",
                "exporttype": "Employee",
                "downstreamapp": "PSA",
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

        fail_record_threshold = rail.FailOperator(
            task_id='fail_record_threshold',
            message='Payload threshold exceeded'
        )

        if_no_data = rail.IfOperator(
            task_id='if_no_data',
            test=lambda: (rail.result("get_data_existence_var")["value"]["type"]) == "No Data",
            yes_task='send_no_data_email',
            no_task='send_psa_export_complete_email'
        )

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon time extract for PSA (C1/Compass) - No records to export - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/psa_no_data.html",
            params={
                'export_type': " (C1/Compass)"
            }
        )

        send_psa_export_complete_email = rail.EmailOperator(
            task_id='send_psa_export_complete_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Replicon time extract for PSA (C1/Compass) - Completed Successfully - {{ current_time_in_specified_tz() }}",
            html_content="templates/emails/psa_complete.html",
            params={
                'sftp_path': config.output_filepath,
                'export_type': " (C1/Compass)"
            }
        )

        dag_run_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dag_run_log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "payloadidentifier": "{{ dag_run.conf.payload_identifier_replicon_uniqueid }}",
                "exporttype": "Employee",
                "downstreamapp": "PSA",
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

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> batch_end
        can_run_batch_task >> rail.Label('No') >> can_export_psa_regular_timedata

        can_export_psa_regular_timedata >> rail.Label("Yes") >> get_last_time_export_psa_details \
            >> is_unckn_export_extension_field_value_present_for_psa
        can_export_psa_regular_timedata >> rail.Label("No") >> batch_end

        is_unckn_export_extension_field_value_present_for_psa >> rail.Label("Yes") >> process_acknowledgement_not_received \
            >> create_download_batch
        is_unckn_export_extension_field_value_present_for_psa >> rail.Label("No") >> create_download_batch
        create_download_batch >> execute_download_batch >> wait_for_download_batch >> get_download_url \
            >> download_export >> load_export >> data_existence_var \
                >> create_final_time_data_collection >> query_entries_without_shortid >> if_short_id_not_exists

        if_short_id_not_exists >> rail.Label("Yes") >> fail_missing_short_id
        if_short_id_not_exists >> rail.Label("No") >> if_final_timedata_has_data

        if_final_timedata_has_data >> rail.Label("Yes") >> get_psa_org_unit \
            >> get_psa_org_unit_child_hierarchy >> get_query_to_filter_time_export_data >> query_filter_c1_reg_iwo_time_export_data \
                >> query_c1_reg_iwo_ineligible_and_reversals >> merge_c1_reg_iwo_filtered_and_reversals \
                    >> query_filter_compass_reg_iwo_time_export_data >> query_compass_reg_iwo_ineligible_and_reversals \
                        >> merge_compass_reg_iwo_filtered_and_reversals >> if_filtered_timedata_has_data

        if_final_timedata_has_data >> rail.Label("No") >> empty_final_data >> log_no_data_var

        if_filtered_timedata_has_data >> rail.Label("Yes") >> final_c1_reg_iwo_time_export_data >> final_compass_reg_iwo_time_export_data \
            >> final_data_for_processing >> log_data_exist_var >> get_data_existence_var

        if_filtered_timedata_has_data >> rail.Label("No") >> empty_filtered_data >> log_no_data_var

        log_no_data_var >> get_data_existence_var >> create_document_psa_xml \
            >> encrypt_time_export_data >> upload_psa_regular_file_to_sftp

        upload_psa_regular_file_to_sftp >> update_oef_to_acknowledge_current_export >> check_any_failures

        check_any_failures >> rail.Label("Yes") >> is_sftp_upload_or_update_ack_failed
        check_any_failures >> rail.Label("No") >> is_final_data_size_greater_than_record_count

        is_sftp_upload_or_update_ack_failed >> rail.Label("Yes") >> send_psa_time_data_file_sftp_failed_email >> fail_dag >> batch_end
        is_sftp_upload_or_update_ack_failed >> rail.Label("No") >> fail_dag >> batch_end

        is_final_data_size_greater_than_record_count >> rail.Label("Yes") >> dag_run_log_to_sumo_threshold >> fail_record_threshold >> batch_end
        is_final_data_size_greater_than_record_count >> rail.Label("No") >> if_no_data

        if_no_data >> rail.Label("Yes") >> send_no_data_email >> dag_run_log_to_sumo >> batch_end
        if_no_data >> rail.Label("No") >> send_psa_export_complete_email >> dag_run_log_to_sumo >> batch_end

    return dag

rail.for_each_instance(create_child_dag)
