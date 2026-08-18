import rail
from dxctechnology.portugal_payroll_export.utils import request_payload, response_filter, custom_method

def create_child_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description=f'DXC_Portugal_PayrollData_Export_Child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_data_For_all_past_time_exports = rail.RepliconServiceOperator(
            task_id='get_data_For_all_past_time_exports',
            endpoint='/services/PayRunListService1.svc/GetData',
            data=request_payload.get_all_past_time_export_data_payload,
            data_handler= response_filter.get_completed_exports_list
        )

        current_export_name = rail.PythonOperator(
            task_id="current_export_name",
            python_callable=custom_method.get_current_export_name
        )

        logging_job_start_time = rail.WriteLogOperator(
            task_id="logging_job_start_time",
            log="{{ result('create_log') }}",
            message="{{ dag_run.conf.timenow }} - Process started",
            properties={
                "log": """{{ dag_run.conf.timenow }} - Process started"\
                            Company Code : {{dag_run.conf.division}}"""
            }
        )

        create_payroll_download_batch = rail.RepliconServiceOperator(
            task_id="create_payroll_download_batch",
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=request_payload.get_create_payroll_download_batch_payload
        )

        execute_payroll_download_batch, wait_for_payroll_download_batch = rail.batch_execution(
            'execute_payroll_download_batch', create_payroll_download_batch.task_id)

        get_payroll_run_batch_result = rail.RepliconServiceOperator(
            task_id="get_payroll_run_batch_result",
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data={
                "payrollDownloadBatchUri": "{{ result('create_payroll_download_batch') }}"
            }
        )

        download_payload_file_from_url = rail.HTTPDownloadFileOperator(
            task_id="download_payload_file_from_url",
            url="{{ result('get_payroll_run_batch_result').downloadUrl }}"
        )

        load_payload_file = rail.LoadCSVFileOperator(
            task_id="load_payload_file",
            document="{{ result('download_payload_file_from_url') }}"
        )

        create_payroll_data_collection = rail.CreateCollectionOperator(
            task_id='create_payroll_data_collection',
            name='payroll_data',
            source="{{ result('load_payload_file') }}"
        )

        has_payroll_data = rail.IfOperator(
            task_id='has_payroll_data',
            test="{{ result('create_payroll_data_collection','length') > 0 }}",
            yes_task='create_payrun_batch',
            no_task='finish_export_no_payroll_data'
        )

        finish_export_no_payroll_data = rail.EmptyOperator(
            task_id='finish_export_no_payroll_data'
        )

        create_payrun_batch = rail.RepliconServiceOperator(
            task_id="create_payrun_batch",
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data=request_payload.get_create_payrun_batch_payload
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
                "name":  "{{ result('current_export_name').replicon_export_name }}"
            }
        )

        update_payrun_description = rail.RepliconServiceOperator(
            task_id="update_payrun_description",
            endpoint="/services/PayRunService1.svc/UpdatePayRunDescription",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}",
                },
                "description":  "{{ result('current_export_name').replicon_export_name }}"
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
            task_id="mark_payrun_as_complete",
            endpoint="/services/PayRunService1.svc/MarkPayRunAsComplete",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}"
                }
            }
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed'
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

        query_final_payroll_data_without_empid = rail.QueryCollectionOperator(
            task_id='query_final_payroll_data_without_empid',
            query='''SELECT * From finalpayrolldata WHERE NULLIF(Employee_ID, '') IS NULL OR Employee_ID=="" '''
        )

        has_empty_empid_data = rail.IfOperator(
            task_id='has_empty_empid_data',
            test="{{ result('query_final_payroll_data_without_empid','length') > 0 }}",
            yes_task='mark_payrun_as_draft',
            no_task='get_all_regular_pay_codes_from_mapper'
        )

        mark_payrun_as_draft = rail.RepliconServiceOperator(
            task_id="mark_payrun_as_draft",
            endpoint="/services/PayRunService1.svc/MarkPayRunAsDraft",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}"
                }
            }
        )

        cancel_payrun = rail.RepliconServiceOperator(
            task_id="cancel_payrun",
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data={
                "target": {
                    "uri": "{{ result('get_payrun_batch_result').payRunUri }}"
                }
            }
        )

        fail_export = rail.FailOperator(
            task_id="fail_export",
            message="Employee ID not present for some users. Users available to validate in payrun \
                '{{ result('current_export_name').replicon_export_name }}'"
        )

        get_all_regular_pay_codes_from_mapper= rail.PythonOperator(
            task_id= 'get_all_regular_pay_codes_from_mapper',
            python_callable=lambda: request_payload.get_all_required_pacodes(config.regular_paycodes_mapper)
        )

        query_list_in_final_regular_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_list_in_final_regular_payroll_collection',
            query="""SELECT * FROM finalpayrolldata WHERE Pay_Code_Code IN ({{result('get_all_regular_pay_codes_from_mapper')}})"""
        )

        has_regular_item_data = rail.IfOperator(
            task_id='has_regular_item_data',
            test="{{ result('query_list_in_final_regular_payroll_collection','length') > 0 }}",
            yes_task='create_valid_data_collection',
            no_task='get_all_timeoff_pay_codes_from_mapper'
        )

        create_valid_data_collection = rail.CreateCollectionOperator(
            task_id='create_valid_data_collection',
            name='validdata',
            source="{{ result('query_list_in_final_regular_payroll_collection') }}"
        )

        query_regular_unique_emp_id = rail.QueryCollectionOperator(
            task_id = 'query_regular_unique_emp_id',
            query= """SELECT DISTINCT Employee_ID FROM validdata"""
        )

        variable_data_per_user = rail.SetVariableOperator(
            task_id='variable_data_per_user',
            append=False,
            name='variabledata',
            value=[]
        )

        for_each_regular_emp_id = rail.ForEachOperator(
            task_id='for_each_regular_emp_id',
            items="{{ result('query_regular_unique_emp_id') }}",
            start_task = 'query_user_data',
            end_task = 'for_each_regular_emp_id_end'
        )

        query_user_data = rail.QueryCollectionOperator(
            task_id = 'query_user_data',
            query= """SELECT * FROM validdata WHERE Employee_ID == '{{ result("for_each_regular_emp_id").Employee_ID }}' """
        )

        add_items_to_variable_data= rail.SetVariableOperator(
            task_id='add_items_to_variable_data',
            append=True,
            name='{{ result("variable_data_per_user").name }}',
            value=response_filter.add_variable_to_list
        )

        for_each_regular_emp_id_end = rail.EmptyOperator(
            task_id = 'for_each_regular_emp_id_end'
        )

        get_variable_list_data = rail.GetVariableOperator(
            task_id = 'get_variable_list_data',
            name= '{{ result("variable_data_per_user").name }}'
        )

        create_document = rail.RenderTemplateOperator(
            task_id='create_document',
            target='artifact',
            template_file='xml_schema/regular_export_schema.xml',
            dataset="{{ result('get_variable_list_data').value | to_json }}",
        )

        pgp_encrypt_regular_file = rail.PGPEncryptionOperator(
            task_id="pgp_encrypt_regular_file",
            source="{{ result('create_document') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        upload_encrypted_payroll_item_file_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_payroll_item_file_sftp",
            content="{{ result('pgp_encrypt_regular_file') }}",
            remote_filepath=config.output_filepath + '{{ result("current_export_name").variable_filename }}.pgp'
        )

        upload_payroll_file_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_payroll_file_to_secondary_sftp",
            content="{{ result('create_document') }}",
            sftp_conn_id= config.secondary_sftp_conn_id,
            remote_filepath=config.secondary_filepath + '{{ result("current_export_name").variable_filename }}'
        )

        send_email_for_regular_export_copmpletion = rail.EmailOperator(
            task_id='send_email_for_regular_export_copmpletion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll export for Portugal completed - {{ dag_run.conf.timenow }}',
            params={
                'output_filepath': config.output_filepath,
                'No_of_records': '{{ result("create_valid_data_collection",length) }}'
            },
            html_content="templates/email/regular_export_success.html"
        )

        get_all_timeoff_pay_codes_from_mapper= rail.PythonOperator(
            task_id= 'get_all_timeoff_pay_codes_from_mapper',
            python_callable=lambda: request_payload.get_all_required_pacodes(config.timeoff_paycodes)
        )

        query_list_in_final_timeoff_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_list_in_final_timeoff_payroll_collection',
            query="""SELECT * FROM finalpayrolldata WHERE Pay_Code_Code IN ({{result('get_all_timeoff_pay_codes_from_mapper')}})"""
        )

        has_timeoff_item_data = rail.IfOperator(
            task_id='has_timeoff_item_data',
            test="{{ result('query_list_in_final_timeoff_payroll_collection','length') > 0 }}",
            yes_task='create_valid_timeoff_data_collection',
            no_task='finish'
        )

        create_valid_timeoff_data_collection = rail.CreateCollectionOperator(
            task_id='create_valid_timeoff_data_collection',
            name='validabsencedata',
            source="{{ result('query_list_in_final_timeoff_payroll_collection') }}"
        )

        query_timeoff_unique_emp_id = rail.QueryCollectionOperator(
            task_id = 'query_timeoff_unique_emp_id',
            query= """SELECT DISTINCT Employee_ID FROM validabsencedata"""
        )

        absence_data_per_user = rail.SetVariableOperator(
            task_id='absence_data_per_user',
            append=False,
            name='absencedata',
            value=[]
        )

        for_each_timeoff_emp_id = rail.ForEachOperator(
            task_id='for_each_timeoff_emp_id',
            items="{{ result('query_timeoff_unique_emp_id') }}",
            start_task = 'query_timeoff_user_data',
            end_task = 'for_each_timeoff_emp_id_end'
        )

        query_timeoff_user_data = rail.QueryCollectionOperator(
            task_id = 'query_timeoff_user_data',
            query= """SELECT * FROM validabsencedata WHERE Employee_ID == '{{ result("for_each_timeoff_emp_id").Employee_ID }}' """
        )

        add_items_to_absence_data= rail.SetVariableOperator(
            task_id='add_items_to_absence_data',
            append=True,
            name='{{ result("absence_data_per_user").name }}',
            value=response_filter.add_absence_data_to_list
        )

        for_each_timeoff_emp_id_end = rail.EmptyOperator(
            task_id = 'for_each_timeoff_emp_id_end'
        )

        get_absence_list_data = rail.GetVariableOperator(
            task_id = 'get_absence_list_data',
            name= '{{ result("absence_data_per_user").name }}'
        )

        create_timeoff_document = rail.RenderTemplateOperator(
            task_id='create_timeoff_document',
            target='artifact',
            template_file='xml_schema/absence_export_schema.xml',
            dataset="{{ result('get_absence_list_data').value | to_json }}",
        )

        pgp_encrypt_timeoff_file = rail.PGPEncryptionOperator(
            task_id="pgp_encrypt_timeoff_file",
            source="{{ result('create_timeoff_document') }}",
            pgp_conn_id=config.pgp_conn_id,
            retries = 0
        )

        upload_encrypted_payroll_timeoff_file_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_payroll_timeoff_file_sftp",
            content="{{ result('pgp_encrypt_timeoff_file') }}",
            remote_filepath=config.output_filepath + '{{ result("current_export_name").absence_filename }}.pgp'
        )

        upload_timeoff_file_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_timeoff_file_to_secondary_sftp",
            content="{{ result('create_timeoff_document') }}",
            remote_filepath=config.secondary_filepath + '{{ result("current_export_name").absence_filename }}'
        )

        compose_csv =rail.WriteCSVFileOperator(
            task_id='compose_csv',
            source="{{ result('create_valid_timeoff_data_collection') }}",
            header=['Employee_ID',
                    'Entry_Date',
                    'Pay_Code_Code',
                    'Pay_Code_Hours'
                ],
            row=lambda item:[
                item['Employee_ID'],
                item['Entry_Date'],
                item['Pay_Code_Code'],
                item['Pay_Code_Hours']
            ]
        )

        upload_timeoff_log_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_timeoff_log_sftp",
            content="{{ result('compose_csv') }}",
            remote_filepath=config.output_filepath + '{{ result("current_export_name").log_filename }}'
        )

        send_email_for_timeoff_export_copmpletion = rail.EmailOperator(
            task_id='send_email_for_timeoff_export_copmpletion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll export for Portugal absence completed - {{current_time_in_specified_tz()}}',
            params={
                'output_filepath': config.output_filepath,
                'No_of_records': "create_valid_timeoff_data_collection"
            },
            html_content="templates/email/absence_export_success.html"
        )

        finish = rail.EmptyOperator(
            task_id = 'finish'
        )

        create_log >> get_data_For_all_past_time_exports >> current_export_name >> logging_job_start_time >> create_payroll_download_batch >> \
            execute_payroll_download_batch >> wait_for_payroll_download_batch >> get_payroll_run_batch_result >> download_payload_file_from_url >> \
                load_payload_file >> create_payroll_data_collection >> has_payroll_data

        has_payroll_data >> rail.Label(
            'Yes') >> create_payrun_batch >> execute_payrun_batch >> wait_forpayrun_batch >> get_payrun_batch_result

        has_payroll_data >> rail.Label(
            'No') >> finish_export_no_payroll_data

        get_payrun_batch_result >> update_payrun_name >> update_payrun_description >> create_payrun_download_batch >> execute_payrun_download_batch \
            >> wait_for_payrun_download_batch >> get_payrun_download_batch_result >> mark_payrun_as_complete

        mark_payrun_as_complete >> rail.Label(
                "on_success") >> download_final_payload_file_from_url

        mark_payrun_as_complete >> rail.Label(
            "on_error") >> catch_error >> cancel_payrun >> fail_export

        download_final_payload_file_from_url >> load_final_payload_file >> create_final_payroll_data_collection >> \
            query_final_payroll_data_without_empid >> has_empty_empid_data

        has_empty_empid_data >> rail.Label(
            'Yes') >> mark_payrun_as_draft >> cancel_payrun >> fail_export

        has_empty_empid_data >> rail.Label(
            'No') >> get_all_regular_pay_codes_from_mapper

        get_all_regular_pay_codes_from_mapper >> query_list_in_final_regular_payroll_collection >> has_regular_item_data

        has_regular_item_data >> rail.Label(
            'Yes') >> create_valid_data_collection >> query_regular_unique_emp_id >> variable_data_per_user >> for_each_regular_emp_id

        has_regular_item_data >> rail.Label(
            'No') >> get_all_timeoff_pay_codes_from_mapper

        for_each_regular_emp_id >> query_user_data >> add_items_to_variable_data >> for_each_regular_emp_id_end

        for_each_regular_emp_id >> for_each_regular_emp_id_end >> get_variable_list_data >> create_document >> pgp_encrypt_regular_file

        pgp_encrypt_regular_file >> rail.Label(
            "on_success") >> upload_encrypted_payroll_item_file_sftp >> send_email_for_regular_export_copmpletion

        pgp_encrypt_regular_file >> rail.Label(
            "on_error") >> upload_payroll_file_to_secondary_sftp >> send_email_for_regular_export_copmpletion

        send_email_for_regular_export_copmpletion >> get_all_timeoff_pay_codes_from_mapper >> \
            query_list_in_final_timeoff_payroll_collection >> has_timeoff_item_data

        has_timeoff_item_data >> rail.Label(
            "Yes") >> create_valid_timeoff_data_collection >> query_timeoff_unique_emp_id >> \
                absence_data_per_user >> for_each_timeoff_emp_id

        has_timeoff_item_data >> rail.Label(
            "No") >> finish

        for_each_timeoff_emp_id >> query_timeoff_user_data >> add_items_to_absence_data >> for_each_timeoff_emp_id_end

        for_each_timeoff_emp_id >> for_each_timeoff_emp_id_end >> get_absence_list_data >> create_timeoff_document >> pgp_encrypt_timeoff_file

        pgp_encrypt_timeoff_file >> rail.Label(
            "on_success") >> upload_encrypted_payroll_timeoff_file_sftp >> compose_csv

        pgp_encrypt_timeoff_file >> rail.Label(
            "on_error") >> upload_timeoff_file_to_secondary_sftp >> compose_csv

        upload_encrypted_payroll_timeoff_file_sftp >> compose_csv >> upload_timeoff_log_sftp >> send_email_for_timeoff_export_copmpletion >> finish

    return dag

rail.for_each_instance(create_child_dag)
