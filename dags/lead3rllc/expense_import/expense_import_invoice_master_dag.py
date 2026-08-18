from datetime import timedelta
import rail
from lead3rllc.expense_import.utils import custom_methods, request_payload


null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.expense_invoice_master_dag_id,
        description=f'LEAD3R LLC Expense Import Invoice Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.sftp_input_filepath_invoice,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        is_file_csv = rail.IfOperator(
            task_id='is_file_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task="download_input_csv",
            no_task="send_incorrect_file_format_mail",
        )

        send_incorrect_file_format_mail = rail.EmailOperator(
            task_id='send_incorrect_file_format_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Expense Invoice Import - Incorrect file format received - {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }}''',
            html_content="templates/incorrect_file_format_invoice.html"
        )

        download_input_csv = rail.SFTPDownloadFileOperator(
            task_id='download_input_csv',
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

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='''{{ result("new_file_sensor") }}''',
            new_filename=config.sftp_archive_filepath_invoice +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('download_input_csv') }}"
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('parse_csv') }}",
            columns={
                'User': 'User',
                'Vendor Name': 'Vendor_Name',
                'Invoice Number': 'Invoice_Number',
                'Invoice Date':  'Invoice_Date',
                'Date Incurred': 'Date_Incurred',
                'Expense Type': 'Expense_Type',
                'Line Description': 'Line_Description',
                'Amount': 'Amount',
                'Is Billable': 'Is_Billable',
                'Project': 'Project'
            },
            name="inputfile",
        )

        if_csv_has_data = rail.IfOperator(
            task_id='if_csv_has_data',
            test="{{ result('create_collection_from_csv','length') > 0 }}",
            yes_task='create_log_expense_invoice_import',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Expense Invoice Import - Blank File - {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} ',
            html_content="templates/email_blank_payload_invoice.html"
        )

        create_log_expense_invoice_import = rail.CreateLogOperator(
            task_id='create_log_expense_invoice_import'
        )

        query_list_expense_invoice_missing_required_fields = rail.QueryCollectionOperator(
            task_id='query_list_expense_invoice_missing_required_fields',
            query="""SELECT * FROM  inputfile 
                WHERE (NULLIF(Vendor_Name,'') IS NULL 
                OR NULLIF(Date_Incurred,'') IS NULL
                OR NULLIF(Project,'') IS NULL
                OR NULLIF(Expense_Type,'') IS NULL
                OR NULLIF(Amount,'') IS NULL)""",
            name="invalid_records"
        )

        if_records_with_missing_required_fields_list_has_data = rail.IfOperator(
            task_id='if_records_with_missing_required_fields_list_has_data',
            test=lambda: rail.result(
                'query_list_expense_invoice_missing_required_fields', 'length') > 0,
            yes_task='log_invalid_records',
            no_task='query_list_valid_records'
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{result('create_log_expense_invoice_import')}}",
            items="{{result('query_list_expense_invoice_missing_required_fields')}}",
            message='One or more mandatory field is missing',
            severity='Exception',
            properties=lambda item: {
                "vendor_name": item['Vendor_Name'],
                "invoice_date": item['Invoice_Date'],
                "line_description": item['Line_Description'],
                "expense_type": item['Expense_Type'],
                "project": item['Project'],
                "action": "Validation",
                "status": "Exception",
                "details": custom_methods.get_missing_field_message_invoice(item)
            }
        )

        query_list_valid_records = rail.QueryCollectionOperator(
            task_id='query_list_valid_records',
            query="""SELECT * FROM inputfile 
                WHERE (NULLIF(Vendor_Name,'') IS NOT NULL 
                AND NULLIF(Date_Incurred,'') IS NOT NULL
                AND NULLIF(Project,'') IS NOT NULL
                AND NULLIF(Expense_Type,'') IS NOT NULL
                AND NULLIF(Amount,'') IS NOT NULL)""",
            name="valid_records"
        )

        if_valid_records_present = rail.IfOperator(
            task_id='if_valid_records_present',
            test=lambda: rail.result(
                'query_list_valid_records', 'length') > 0,
            yes_task='get_all_replicon_expense_codes',
            no_task='process_log_generation'
        )

        get_all_replicon_expense_codes = rail.RepliconServiceOperator(
            task_id='get_all_replicon_expense_codes',
            endpoint="/services/ExpenseService1.svc/GetAllExpenseCodes",
            data_handler=lambda res: list(map(lambda x: {
                'expense_code': x['displayText'],
                'expense_code_enabled': "True" if bool(x['isEnabled']) else "False",
                'expense_code_uri': x['uri']}, res)) if res else [{'expense_code': '', 'expense_code_enabled': '', 'expense_code_uri': ''}]
        )

        create_collection_replicon_expense_codes_uri = rail.CreateCollectionOperator(
            task_id='create_collection_replicon_expense_codes_uri',
            source=lambda: rail.result('get_all_replicon_expense_codes'),
            name='replicon_expense_codes_uri'
        )

        get_required_project_column_uri = rail.RepliconServiceOperator(
            task_id='get_required_project_column_uri',
            endpoint="/services/ProjectListService1.svc/GetAllColumns",
            data_handler=lambda res: rail.find_first_by_attr_and_get_attr(
                rail.find_first_by_attr_and_get_attr(res, 'displayText', 'Basic', 'columns'), 'displayText', 'Netsuite Project ID', 'uri')
        )

        get_replicon_projects_list = rail.RepliconServiceOperator(
            task_id='get_replicon_projects_list',
            endpoint="/services/ProjectListService1.svc/GetData",
            data=lambda: {
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    rail.result('get_required_project_column_uri')
                ]
            },
            data_handler=lambda data: list(map(lambda x: {
                'project_name': x['cells'][0]['textValue'],
                'project_uri': x['cells'][0]['uri'],
                'netsuite_project_id': x['cells'][1]['textValue'] if x['cells'][1]['dataType'] != "urn:replicon:list-type:null" else null}, data['rows'])
            ) if data['rows'] else [{'project_name': '', 'project_uri': '', 'netsuite_project_id': ''}]
        )

        create_replicon_project_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_project_collection',
            source=lambda: rail.result('get_replicon_projects_list'),
            name='replicon_projects'
        )

        query_valid_records_to_join_expense_code_uris = rail.QueryCollectionOperator(
            task_id='query_valid_records_to_join_expense_code_uris',
            query="""SELECT * FROM  valid_records 
                LEFT JOIN replicon_projects ON valid_records.Project  = replicon_projects.netsuite_project_id
                LEFT JOIN replicon_expense_codes_uri ON valid_records.Expense_Type  = replicon_expense_codes_uri.expense_code """,
            name='valid_records_to_process'
        )

        get_owner_uri_for_expense_invoice_import = rail.RepliconServiceOperator(
            task_id='get_owner_uri_for_expense_invoice_import',
            endpoint="/services/UserService1.svc/GetUser2",
            data={
                "user": {
                    "loginName": config.user_loginname_for_invoice
                }
            },
            data_handler=lambda res: res['uri']
        )

        get_required_expense_sheet_customfield_uri = rail.RepliconServiceOperator(
            task_id='get_required_expense_sheet_customfield_uri',
            endpoint="/services/ExpenseEntryCustomFieldListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": ["urn:replicon:expense-entry-custom-field-list-column:expense-entry-custom-field"]
            },
            data_handler=lambda res: request_payload.required_expense_sheet_customfield_uri(
                res['rows'])
        )

        get_default_currency_uri = rail.RepliconServiceOperator(
            task_id='get_default_currency_uri',
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
            data_handler=lambda res: rail.find_first_by_attr_and_get_attr(
                res, 'displayText', config.default_currency, 'uri', '')
        )

        query_distinct_vendor_name_and_invoice_date_from_valid_records = rail.QueryCollectionOperator(
            task_id='query_distinct_vendor_name_and_invoice_date_from_valid_records',
            query="""SELECT DISTINCT Vendor_Name, Invoice_Date FROM valid_records""",
            name="distinct_vendor_name_invoice_date"
        )

        trigger_dag_create_expense_sheet = rail.trigger_parallel_dagrun(
            task_id='trigger_dag_create_expense_sheet',
            items="{{result('query_distinct_vendor_name_and_invoice_date_from_valid_records')}}",
            trigger_dag_id=config.child_create_expense_sheet_for_invoice_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            parallel_count=config.create_expense_sheet_trigger_parallel_count,
            conf=lambda item: {
                'vendor_name': item['Vendor_Name'],
                'invoice_date': item['Invoice_Date'],
                'owner_uri': rail.result('get_owner_uri_for_expense_invoice_import'),
                'default_currency_uri': rail.result('get_default_currency_uri'),
                'reference_number_customfield_uri': rail.result('get_required_expense_sheet_customfield_uri'),
                'expense_invoice_import_logs': rail.result('create_log_expense_invoice_import')
            }
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_for_invoice_dag_id,
            conf=lambda: {
                'expense_import_log': rail.result('create_log_expense_invoice_import'),
                'input_filename': rail.render_template("{{result('new_file_sensor') | file_name }}")
            }
        )

        new_file_sensor >> is_file_csv

        is_file_csv >> rail.Label(
            'No') >> send_incorrect_file_format_mail
        is_file_csv >> rail.Label('Yes') >> download_input_csv

        download_input_csv >> was_new_file_found

        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        was_new_file_found >> rail.Label('Yes') >> archive_file

        download_input_csv >> parse_csv >> create_collection_from_csv

        create_collection_from_csv >> if_csv_has_data

        if_csv_has_data >> rail.Label('No') >> send_blank_payload_email
        if_csv_has_data >> rail.Label('Yes') >> create_log_expense_invoice_import >> query_list_expense_invoice_missing_required_fields \
            >> if_records_with_missing_required_fields_list_has_data

        if_records_with_missing_required_fields_list_has_data >> rail.Label(
            'No') >> query_list_valid_records
        if_records_with_missing_required_fields_list_has_data >> rail.Label(
            'Yes') >> log_invalid_records >> query_list_valid_records

        query_list_valid_records >> if_valid_records_present

        if_valid_records_present >> rail.Label('No') >> process_log_generation
        if_valid_records_present >> rail.Label(
            'Yes') >> get_all_replicon_expense_codes

        get_all_replicon_expense_codes >> create_collection_replicon_expense_codes_uri

        create_collection_replicon_expense_codes_uri >> get_required_project_column_uri >> get_replicon_projects_list >> create_replicon_project_collection \
            >> query_valid_records_to_join_expense_code_uris >> get_owner_uri_for_expense_invoice_import >> get_required_expense_sheet_customfield_uri \
            >> get_default_currency_uri >> query_distinct_vendor_name_and_invoice_date_from_valid_records

        query_distinct_vendor_name_and_invoice_date_from_valid_records >> trigger_dag_create_expense_sheet \
            >> process_log_generation

    return dag


rail.for_each_instance(create_dag)
