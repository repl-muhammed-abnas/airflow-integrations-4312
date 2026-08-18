from datetime import timedelta
from pendulum import datetime
import pendulum
import rail
from crl.payroll_export_uk.utils import request_payload
from crl.payroll_export_uk.utils import python_callable


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"CRL UK Payroll Export Monthly Master {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2026, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:
        
        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_conf"
        )

        run_dag_on_payrollcalendar = rail.IfOperator(
            task_id="run_dag_on_payrollcalendar",
            test= lambda dag_run: not bool(dag_run.conf),
            yes_task='can_process_run',
            no_task='process_start_time'
        )

        def can_process_run_test():
            current_date = pendulum.now(config.time_zone).strftime("%d-%m-%Y")
            current_hour = int(pendulum.now(config.time_zone).strftime("%H"))
            matched_payroll_period = rail.find_first_by_attr_and_get_attr(
                config.UK_PAYROLL_CALENDER_MAPPER_TO_USE, "payroll_processing_date", current_date)
            return bool(
                matched_payroll_period and
                matched_payroll_period.get("processing_time") == current_hour
            )

        can_process_run = rail.IfOperator(
            task_id = "can_process_run",
            test=can_process_run_test,
            yes_task="process_start_time",
            no_task="finish_export_no_scheduled_run"
        )

        finish_export_no_scheduled_run = rail.EmptyOperator(
            task_id='finish_export_no_scheduled_run'
        )

        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=python_callable.get_time_in_formats,
            op_args=[config.time_zone]
        )

        send_email_for_export_started = rail.EmailOperator(
            task_id='send_email_for_export_started',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon ADP Payroll Export Notification - Export started |'+ \
            ' {{ result("process_start_time").start_time }} | ' + \
            config.location_for_mails,
            html_content="/templates/email/export_started.html"
        )

        get_adp_payroll_script = rail.RepliconServiceOperator(
            task_id="get_adp_payroll_script",
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response,
                               'displayText', config.payroll_export_file_format, 'uri')
        )

        is_file_format_script_present = rail.IfOperator(
            task_id='is_file_format_script_present',
            test='{{ result("get_adp_payroll_script") | is_truthy }}',
            yes_task='get_all_enabled_locations'
        )

        get_all_enabled_locations = rail.RepliconServiceOperator(
            task_id="get_all_enabled_locations",
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', config.export_location, 'uri')
        )

        get_all_enabled_users = rail.RepliconServiceOperator(
            task_id="get_all_enabled_users",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_user_data,
            data_handler=python_callable.get_enabled_employee
        )

        create_object = rail.TriggerDagRunForEachItemOperator(
            task_id="create_object",
            items=lambda: rail.result('get_all_enabled_users'),
            batch_size=config.create_object_batch_size,
            trigger_dag_id=config.child_dag_id,
            conf=lambda item: {"uri": item},
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_create_object = rail.WaitForDagRunsSensor(
            task_id="wait_create_object",
            dag_runs="{{result('create_object')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        create_object_uris = rail.GatherResultsFromDagRunsOperator(
            task_id='create_object_uris',
            dag_runs="{{ result('create_object') }}",
            dagrun_task_id='create_object_set'
        )

        get_all_employee_type = rail.RepliconServiceOperator(
            task_id="get_all_employee_type",
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data_handler=python_callable.get_employee_types
        )

        get_location_child_hierarchy_data = rail.RepliconServiceOperator(
            task_id='get_location_child_hierarchy_data',
            endpoint='/services/LocationListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:full-path"
                ]
            },
            data_handler=python_callable.get_filtered_allowed_location_uris
        )

        get_file_name = rail.PythonOperator(
            task_id='get_file_name',
            python_callable=lambda: "P" + config.adp_gv_system + config.gv_system_number + "476" + "_" +
            pendulum.now(config.time_zone).strftime(
                "%Y%m%d%H%M%S") + "_" + "GBTIME_HRMD01_MUT8G2I"
        )

        finish_export_no_payroll_data = rail.EmptyOperator(
            task_id='finish_export_no_payroll_data'
        )

        send_email_for_no_payroll_data = rail.EmailOperator(
            task_id='send_email_for_no_payroll_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon ADP Payroll Export Notification - No payroll data |'+ \
            ' {{ result("process_start_time").start_time }} | for ' + \
            config.location_for_mails + ' - Completed no records processed',
            html_content="/templates/email/blank_export.html"
        )

        create_payrun_batch = rail.RepliconServiceOperator(
            task_id="create_payrun_batch",
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data=lambda dag_run: request_payload.get_create_payrun_batch_payload(config.time_zone, dag_run)
        )

        execute_payrun_batch, wait_forpayrun_batch = rail.batch_execution(
            'execute_payrun_batch', create_payrun_batch.task_id)

        get_payrun_batch_result = rail.RepliconServiceOperator(
            task_id="get_payrun_batch_result",
            endpoint="/services/PayRunService1.svc/GetCreatePayRunBatchResults",
            data={"payRunBatchUri": "{{ result('create_payrun_batch') }}"}
        )

        update_payrun_name = rail.RepliconServiceOperator(
            task_id="update_payrun_name",
            endpoint="/services/PayRunService1.svc/UpdatePayRunName",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}",
                },
                "name":  "{{ result('get_file_name')}}"
            }
        )

        create_payrun_download_batch = rail.RepliconServiceOperator(
            task_id="create_payrun_download_batch",
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=request_payload.get_create_payrun_download_batch_payload
        )

        execute_payrun_download_batch, wait_for_payrun_download_batch = rail.batch_execution(
            'execute_payrun_download_batch', create_payrun_download_batch.task_id)

        get_payrun_download_batch_result = rail.RepliconServiceOperator(
            task_id="get_payrun_download_batch_result",
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={
                "payrollDownloadBatchUri": "{{ result('create_payrun_download_batch') }}"}
        )

        mark_payrun_as_complete = rail.RepliconServiceOperator(
            task_id='mark_payrun_as_complete',
            endpoint="/services/PayRunService1.svc/CreateMarkPayRunAsCompleteBatch",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}"
                }
            }
        )

        execute_payrun_complete_batch, wait_for_payrun_complete_batch = rail.batch_execution(
            'execute_payrun_complete_batch', mark_payrun_as_complete.task_id)
        
        send_invalid_records_email = rail.EmailOperator(
            task_id='send_invalid_records_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon ADP Payroll Export Notification - Invalid records |'+ \
            ' {{ result("process_start_time").start_time }} | for ' + \
            config.location_for_mails + ' - Completed with errors',
            html_content="templates/email/email_invalid_records_in_export.html"
        )

        download_final_payload_file_from_url = rail.HTTPDownloadFileOperator(
            task_id="download_final_payload_file_from_url",
            url="{{ result('get_payrun_download_batch_result').downloadUrl }}"
        )

        load_final_payload_file = rail.LoadCSVFileOperator(
            task_id="load_final_payload_file",
            document="{{ result('download_final_payload_file_from_url') }}"
        )

        create_final_payroll_data_collection = rail.CreateCollectionOperator(
            task_id='create_final_payroll_data_collection',
            name='finalpayrolldata',
            source="{{ result('load_final_payload_file') }}"
        )

        has_payroll_data = rail.IfOperator(
            task_id='has_payroll_data',
            test="{{ result('create_final_payroll_data_collection','length') > 0 }}",
            yes_task='query_final_payroll_data_without_empid',
            no_task='update_payrun_name_to_nodata'
        )

        update_payrun_name_to_nodata = rail.RepliconServiceOperator(
            task_id="update_payrun_name_to_nodata",
            endpoint="/services/PayRunService1.svc/UpdatePayRunName",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}",
                },
                "name":  "{{ result('get_file_name')}}" + "_ND"
            }
        )

        query_final_payroll_data_without_empid = rail.QueryCollectionOperator(
            task_id='query_final_payroll_data_without_empid',
            query='''SELECT * From finalpayrolldata WHERE NULLIF(CLIID, '') IS NULL OR CLIID=="" '''
        )

        has_empty_empid_data = rail.IfOperator(
            task_id='has_empty_empid_data',
            test="{{ result('query_final_payroll_data_without_empid','length') > 0 }}",
            yes_task='mark_payrun_as_draft',
            no_task='query_list_in_final_payroll_collection'
        )

        invalid_records = rail.WriteCSVFileOperator(
            task_id='invalid_records',
            source="{{ result('query_final_payroll_data_without_empid')}}"
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{result('invalid_records')}}",
            output_file_name="Invalid_PayrollExport_records_{{dag_run_ecid()}}_.csv",
            expires_in_seconds=7*24*60*60
        )

        mark_payrun_as_draft = rail.RepliconServiceOperator(
            task_id="mark_payrun_as_draft",
            endpoint="/services/PayRunService1.svc/MarkPayRunAsDraft",
            data=request_payload.get_payload
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        cancel_payrun = rail.RepliconServiceOperator(
            task_id="cancel_payrun",
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data=request_payload.get_payload
        )

        query_list_in_final_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_list_in_final_payroll_collection',
            query=f"SELECT * FROM finalpayrolldata WHERE finalpayrolldata.SUBTY IN {config.UK_2010_PAYCODE_MAPPER_TO_USE}"
        )

        has_item_data = rail.IfOperator(
            task_id='has_item_data',
            test="{{ result('query_list_in_final_payroll_collection','length') > 0 }}",
            yes_task='trigger_payroll_export_child',
            no_task='finish_export_no_payroll_data'
        )

        query_list_in_final_timeoff_collection = rail.QueryCollectionOperator(
            task_id='query_list_in_final_timeoff_collection',
            query=f"SELECT * FROM finalpayrolldata WHERE finalpayrolldata.SUBTY IN {config.UK_2001_TIMEOFF_MAPPER_TO_USE}"
        )

        has_timeoff_item_data = rail.IfOperator(
            task_id='has_timeoff_item_data',
            test="{{ result('query_list_in_final_timeoff_collection','length') > 0 }}",
            yes_task='get_timeoff_file_name',
            no_task='finish_export_no_timeoff_data'
        )

        finish_export_no_timeoff_data = rail.EmptyOperator(
            task_id='finish_export_no_timeoff_data'
        )

        send_email_for_no_timeoff_data = rail.EmailOperator(
            task_id='send_email_for_no_timeoff_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon ADP Timeoff Export Notification - No timeoff data |'+ \
            ' {{ result("process_start_time").start_time }} | for ' + \
            config.location_for_mails + ' - Completed no records processed',
            html_content="/templates/email/blank_export.html"
        )

        get_timeoff_file_name = rail.PythonOperator(
            task_id='get_timeoff_file_name',
            python_callable=lambda: "P" + config.adp_gv_system + config.gv_system_number + "476" + "_" +
            pendulum.now(config.time_zone).strftime(
                "%Y%m%d%H%M%S") + "_" + "GBTIME_HRMD02_MUT8G2I"
        )

        trigger_timeoff_export_child = rail.TriggerDagRunOperator(
            task_id="trigger_timeoff_export_child",
            trigger_dag_id=config.timeoff_export_child_dag_id,
            conf={
                "collection_data": "{{ result('query_list_in_final_timeoff_collection') }}",
                "collection_length": "{{ result('query_list_in_final_timeoff_collection','length') }}",
                "timeoff_file_name": "{{ result('get_timeoff_file_name') }}",
                "process_start_time": "{{ result('process_start_time').start_time }}",
                "ymd_format": "{{ result('process_start_time').ymd_format }}",
                "hms_format": "{{ result('process_start_time').hms_format }}",
                "total_records": "{{ (result('query_list_in_final_timeoff_collection','length') | int) + 2 }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_timeoff_export_child = rail.WaitForDagRunsSensor(
            task_id="wait_timeoff_export_child",
            dag_runs="{{ result('trigger_timeoff_export_child') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        trigger_payroll_export_child = rail.TriggerDagRunOperator(
            task_id="trigger_payroll_export_child",
            trigger_dag_id=config.payroll_export_child_dag_id,
            conf={
                "collection_data": "{{ result('query_list_in_final_payroll_collection') }}",
                "collection_length": "{{ result('query_list_in_final_payroll_collection','length') }}",
                "payroll_file_name": "{{ result('get_file_name') }}",
                "process_start_time": "{{ result('process_start_time').start_time }}",
                "ymd_format": "{{ result('process_start_time').ymd_format }}",
                "hms_format": "{{ result('process_start_time').hms_format }}",
                "total_records": "{{ (result('query_list_in_final_payroll_collection','length') | int) + 2 }}"
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_payroll_export_child = rail.WaitForDagRunsSensor(
            task_id="wait_payroll_export_child",
            dag_runs="{{ result('trigger_payroll_export_child') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )
        
        export_completion = rail.EmptyOperator(
            task_id='export_completion'
        )

        run_dag_on_payrollcalendar >> rail.Label("No") >> process_start_time
        run_dag_on_payrollcalendar >> rail.Label("Yes") >> can_process_run >> rail.Label('Yes') >> process_start_time
        can_process_run >> rail.Label('No') >> finish_export_no_scheduled_run

        process_start_time >> send_email_for_export_started >> get_adp_payroll_script >> is_file_format_script_present
        is_file_format_script_present >> rail.Label("Yes") >> get_all_enabled_locations >> get_all_enabled_users >> create_object\
            >> wait_create_object >> create_object_uris\
            >> get_all_employee_type >> get_location_child_hierarchy_data >> get_file_name \
            >> create_payrun_batch >> execute_payrun_batch >> wait_forpayrun_batch >> get_payrun_batch_result
        get_payrun_batch_result >> update_payrun_name >> create_payrun_download_batch >> execute_payrun_download_batch \
            >> wait_for_payrun_download_batch >> get_payrun_download_batch_result >> mark_payrun_as_complete >> execute_payrun_complete_batch >> wait_for_payrun_complete_batch >> rail.Label(
                "on_success") >> download_final_payload_file_from_url
        mark_payrun_as_complete >> execute_payrun_complete_batch >> wait_for_payrun_complete_batch >> rail.Label(
            "on_error") >> on_error >> cancel_payrun
        download_final_payload_file_from_url >> load_final_payload_file >> create_final_payroll_data_collection

        create_final_payroll_data_collection >> has_payroll_data >> rail.Label("Yes") >> query_final_payroll_data_without_empid >> has_empty_empid_data >> rail.Label(
            'Yes') >> mark_payrun_as_draft >> cancel_payrun >> invalid_records >> generate_download_link >> send_invalid_records_email
        has_empty_empid_data >> rail.Label(
            'No') >> query_list_in_final_payroll_collection >> query_list_in_final_timeoff_collection
        
        query_list_in_final_payroll_collection >> has_item_data >> rail.Label(
            'Yes') >> trigger_payroll_export_child >> wait_payroll_export_child
        has_item_data >> rail.Label(
            'No') >> finish_export_no_payroll_data >> send_email_for_no_payroll_data

        wait_payroll_export_child >> export_completion
        finish_export_no_payroll_data >> export_completion

        query_list_in_final_timeoff_collection >> has_timeoff_item_data >> rail.Label(
            'Yes') >> get_timeoff_file_name >> trigger_timeoff_export_child >> wait_timeoff_export_child
        has_timeoff_item_data >> rail.Label(
            'No') >> finish_export_no_timeoff_data >> send_email_for_no_timeoff_data

        wait_timeoff_export_child >> export_completion
        finish_export_no_timeoff_data >> export_completion

        create_final_payroll_data_collection >> has_payroll_data >> rail.Label("No") >> update_payrun_name_to_nodata >> finish_export_no_payroll_data

    return dag


rail.for_each_instance(create_main_dag)
