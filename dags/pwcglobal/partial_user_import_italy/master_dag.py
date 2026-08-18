from datetime import timedelta
import rail
from pwcglobal.partial_user_import_italy.utils import response_filter

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.partial_user_import_master_dag_id,
        description=f'PwC - Partial User Import Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=60),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_incorrect_file_format_email',
        )

        send_incorrect_file_format_email = rail.EmailOperator(
            task_id='send_incorrect_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | User Remote Status Attribute Update - incorrect file format recieved - {{ current_time() }}",
            html_content="templates/incorrect_file_format.html"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        catch_and_download_archive_file = rail.IfOperator(
            task_id='catch_and_download_archive_file',
            trigger_rule='one_failed',
            test='{{ "No such file" in get_error_message() }}',
            yes_task='download_archive_file',
            no_task='download_fail'
        )

        download_fail = rail.FailOperator(
            task_id='download_fail',
            message="{{ result('download_file') }}"
        )

        download_archive_file = rail.SFTPDownloadFileOperator(
            task_id='download_archive_file',
            remote_filepath=config.archive_filepath +
            "/{{ result('new_file_sensor') | file_name }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_input_csv = rail.LoadCSVFileOperator(
            task_id='parse_input_csv',
            trigger_rule='one_success',
            document="{{result('download_file') or result('download_archive_file')}}",
            delimiter=';',
            headers=['user_party_id', 'legal_entity_party_id',
                     'remote_work_contract_effective_date', 'remote_work_contract_status'],
        )

        create_user_collection = rail.CreateCollectionOperator(
            task_id='create_user_collection',
            source="{{ result('parse_input_csv') }}",
            name='feed_file_data'
        )

        create_user_import_logs = rail.CreateLogOperator(
            task_id='create_user_import_logs',
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("create_user_collection", "length") > 0 }}',
            yes_task='invalid_records',
            no_task='send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | User Remote Status Attribute Update is completed - no records to process - {{ current_time() }}",
            html_content="templates/blank_payload.html"
        )

        invalid_records = rail.QueryCollectionOperator(
            task_id='invalid_records',
            query='''SELECT * FROM feed_file_data WHERE NULLIF(user_party_id, '') IS NULL OR NULLIF(legal_entity_party_id,'') IS NULL'''
        )

        if_invalid_reocrds_exist = rail.IfOperator(
            task_id='if_invalid_reocrds_exist',
            test='{{ result("invalid_records", "length") > 0 }}',
            yes_task='log_invalid_records',
            no_task='valid_records'
        )

        valid_records = rail.QueryCollectionOperator(
            task_id='valid_records',
            query='''SELECT * FROM feed_file_data WHERE NULLIF(user_party_id, '') IS NOT NULL AND NULLIF(legal_entity_party_id,'') IS NOT NULL'''
        )

        if_valid_records_exist = rail.IfOperator(
            task_id='if_valid_records_exist',
            test='{{ result("valid_records", "length") > 0 }}',
            yes_task='get_all_division_uri_code',
            no_task='create_csv_lines_for_logs'
        )

        get_all_division_uri_code = rail.RepliconServiceOperator(
            task_id='get_all_division_uri_code',
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000000",
                "columnUris": [
                    "urn:replicon:division-list-column:division",
                    "urn:replicon:division-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            # pylint: disable=unnecessary-lambda
            data_handler=lambda response: response_filter.get_all_div_response_filter(
                response)
        )

        get_user_customfieldgroupuri = rail.RepliconServiceOperator(
            task_id='get_user_customfieldgroupuri',
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroup",
            data={
                "objectTypeUri": "urn:replicon:object-type:user"
            }
        )

        get_required_user_customfields = rail.RepliconServiceOperator(
            task_id='get_required_user_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': "{{ result('get_user_customfieldgroupuri').uri }}"
            },
            data_handler=lambda response: {
                'remote_work_contract_effective_date_field_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Remote Work Contract Effective Date', 'uri', ''),
                'remote_work_contract_status_field_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Remote Work Contract Status', 'uri', ''),
            }
        )

        get_status_custom_field_dropdown_uris = rail.RepliconServiceOperator(
            task_id='get_status_custom_field_dropdown_uris',
            endpoint='/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions',
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').remote_work_contract_status_field_uri }}"
            },
            data_handler=lambda response: {
                'Y': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Y', 'uri', ''),
                'N': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'N', 'uri', '')
            }
        )

        trigger_dag_process_users_add_update_custom_field_values = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_process_users_add_update_custom_field_values',
            items='{{result("valid_records")}}',
            trigger_dag_id=config.process_add_update_custom_field_values_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'jobid': rail.render_template("{{dag_run_ecid()}}"),
                'user_party_id':  item['user_party_id'],
                'legal_entity_party_id': item['legal_entity_party_id'],
                'legal_entity_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_division_uri_code'), 'div_code', item['legal_entity_party_id'], 'div_uri'),
                'remote_work_contract_effective_date': item['remote_work_contract_effective_date'],
                'remote_work_contract_effective_date_field_uri': rail.result('get_required_user_customfields')['remote_work_contract_effective_date_field_uri'],
                'remote_work_contract_status': item['remote_work_contract_status'],
                'remote_work_contract_status_field_uri': rail.result('get_required_user_customfields')['remote_work_contract_status_field_uri'],
                'remote_work_contract_status_field_dropdown_uris': rail.result('get_status_custom_field_dropdown_uris'),
                'userimportlogs': rail.result('create_user_import_logs')
            }
        )

        wait_for_trigger_dag_process_users_add_update_custom_field_values = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_dag_process_users_add_update_custom_field_values',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{result("trigger_dag_process_users_add_update_custom_field_values")}}'
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log='{{ result("create_user_import_logs")}}',
            items="{{result('invalid_records')}}",
            severity='Exception',
            message='na',
            properties={
                'jobid': '{{dag_run_ecid()}}',
                'user_party_id': '{{item.user_party_id if item.user_party_id else ""}}',
                'legal_entity_party_id': '{{item.legal_entity_party_id if item.legal_entity_party_id else ""}}',
                'status': 'Exception',
                'details': 'User party ID / Legal Entity Party ID not available in feed file record',
                'child_job_id': ''
            }
        )

        create_csv_lines_for_logs = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_for_logs',
            source="{{ result('create_user_import_logs') }}",
            header=[
                    'User Party ID',
                    'Legal Entity Party ID',
                    'Status',
                    'Details',
                    'JobID',
                    'Child Job ID'],
            row=lambda item: [
                item['properties']['user_party_id'] if item['properties']['user_party_id'] else '',
                item['properties']['legal_entity_party_id'] if item['properties']['legal_entity_party_id'] else '',
                item['properties']['status'],
                item['properties']['details'],
                item['properties']['jobid'],
                item['properties']['child_job_id'] if item['properties']['child_job_id'] else '',
            ],
        )

        log_log_file_name_to_be_used = rail.PythonOperator(
            task_id='log_log_file_name_to_be_used',
            python_callable=lambda:  rail.render_template(
                '''{{ result("new_file_sensor") | file_base }}_logs.csv''')
        )

        upload_logs = rail.SFTPAppendCSVFileOperator(
            task_id='upload_logs',
            content='''{{ result('create_csv_lines_for_logs') }}''',
            remote_filepath=config.log_filepath +
            '''/{{ result('log_log_file_name_to_be_used') }}''',
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines_for_logs')}}",
            output_file_name='{{ result("log_log_file_name_to_be_used") }}',
            expires_in_seconds=7*24*60*60,
        )

        check_for_error_logs = rail.FilterLogEntriesOperator(
            task_id='check_for_error_logs',
            log="{{result('create_user_import_logs')}}",
            properties={
                'status': 'Error'
            }
        )

        if_check_for_error_logs_exist = rail.IfOperator(
            task_id='if_check_for_error_logs_exist',
            test='''{{ result('check_for_error_logs','length') > 0 }}''',
            yes_task="send_mail_completed_with_errors",
            no_task="check_for_exception_logs",
        )

        send_mail_completed_with_errors = rail.EmailOperator(
            task_id='send_mail_completed_with_errors',
            to=config.tenant_email,
            cc=config.internal_logs_email,
            bcc=config.alert_email,
            subject='''{{ get_company_key()}} | User Remote Status Attribute Update - completed with errors - {{ current_time() }} ''',
            html_content='''templates/completed_with_errors_mail.html''',
        )

        check_for_exception_logs = rail.FilterLogEntriesOperator(
            task_id='check_for_exception_logs',
            log="{{result('create_user_import_logs')}}",
            properties={
                'status': 'Exception'
            }
        )

        if_check_for_exception_logs_exist = rail.IfOperator(
            task_id='if_check_for_exception_logs_exist',
            test='''{{ result('check_for_exception_logs','length') > 0 }}''',
            yes_task="send_mail_completed_with_exceptions",
            no_task="send_mail_completed_successfully",
        )

        send_mail_completed_with_exceptions = rail.EmailOperator(
            task_id='send_mail_completed_with_exceptions',
            to=config.tenant_email,
            cc=config.internal_logs_email,
            subject='''{{ get_company_key()}} | User Remote Status Attribute Update - completed with exceptions - {{ current_time() }} ''',
            html_content='''templates/completed_with_exceptions_mail.html''',
        )

        send_mail_completed_successfully = rail.EmailOperator(
            task_id='send_mail_completed_successfully',
            to=config.tenant_email,
            cc=config.internal_logs_email,
            subject='''{{ get_company_key()}} | User Remote Status Attribute Update - completed successfully - {{ current_time() }} ''',
            html_content='''templates/completed_successfully_mail.html''',
        )

        new_file_sensor >> is_csv >> rail.Label(
            'Yes') >> download_file
        is_csv >> rail.Label('No') >> send_incorrect_file_format_email

        download_file >> rail.Label('Always') >> was_new_file_found
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun

        download_file >> catch_and_download_archive_file
        catch_and_download_archive_file >> rail.Label(
            'No file found error') >> download_archive_file >> parse_input_csv
        catch_and_download_archive_file >> rail.Label(
            'Other Error') >> download_fail

        download_file >> parse_input_csv

        parse_input_csv >> create_user_collection >> create_user_import_logs >> has_data
        has_data >> rail.Label("No") >> send_blank_payload_email
        has_data >> rail.Label(
            "Yes") >> invalid_records >> if_invalid_reocrds_exist

        if_invalid_reocrds_exist >> rail.Label(
            "Yes") >> log_invalid_records >> valid_records
        if_invalid_reocrds_exist >> rail.Label(
            "No") >> valid_records >> if_valid_records_exist

        if_valid_records_exist >> rail.Label("Yes") >> get_all_division_uri_code >> get_user_customfieldgroupuri >> get_required_user_customfields \
            >> get_status_custom_field_dropdown_uris >> trigger_dag_process_users_add_update_custom_field_values >> wait_for_trigger_dag_process_users_add_update_custom_field_values \
            >> create_csv_lines_for_logs
        if_valid_records_exist >> rail.Label("No") >> create_csv_lines_for_logs

        create_csv_lines_for_logs >> log_log_file_name_to_be_used >> upload_logs >> generate_download_link \
            >> check_for_error_logs >> if_check_for_error_logs_exist

        if_check_for_error_logs_exist >> rail.Label(
            "Yes") >> send_mail_completed_with_errors
        if_check_for_error_logs_exist >> rail.Label(
            "No") >> check_for_exception_logs >> if_check_for_exception_logs_exist

        if_check_for_exception_logs_exist >> rail.Label(
            "Yes") >> send_mail_completed_with_exceptions
        if_check_for_exception_logs_exist >> rail.Label(
            "No") >> send_mail_completed_successfully

    return dag


rail.for_each_instance(create_dag)
