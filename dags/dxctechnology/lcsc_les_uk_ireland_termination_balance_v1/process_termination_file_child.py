from datetime import timedelta
import pendulum as pd
import rail
from dxctechnology.lcsc_les_uk_ireland_termination_balance_v1.utils import request_payload
from airflow.models import Variable

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_termination_balance_child_dag_id,
        description=f'LCSC_LES_UK_Ireland_termination_balance_Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="process_start_time"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='process_start_time',
            end_task='create_document',
        )

        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=lambda: pd.now(config.time_zone).strftime("%Y-%m-%dT%H:%M:%S")
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        logging_job_start_time = rail.WriteLogOperator(
            task_id="logging_job_start_time",
            log="{{ result('create_log') }}",
            message="{{ result('process_start_time') }} - Process started",
            properties={
                "log": "{{ result('process_start_time') }} - Process started"
            }
        )

        get_all_enabled_divisions = rail.RepliconServiceOperator(
            task_id="get_all_enabled_divisions",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
        )

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name='{{ dag_run.conf.users_report_name }}',
        )

        run_user_details_report = rail.run_report2(
            group_id='run_user_report',
            report_params=lambda dag_run: request_payload.get_run_user_report_payload(
                dag_run, config.locations_company_codes_mapper),
            target='artifact'
        )

        is_users_report_failed = rail.IfOperator(
            task_id='is_users_report_failed',
            test="{{ (result('run_user_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_users_report_generation',
            no_task='users_report_has_data'
        )

        fail_users_report_generation = rail.FailOperator(
            task_id='fail_users_report_generation',
            message="{{ (result('run_user_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error }}"
        )

        users_report_has_data = rail.IfOperator(
            task_id='users_report_has_data',
            test="{{ result('run_user_report.get_report_result','has_data') }}",
            yes_task='is_users_report_has_expected_columns',
            no_task='send_email_for_no_users_data'
        )

        is_users_report_has_expected_columns = rail.IfOperator(
            task_id='is_users_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ (result('run_user_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.user_report_expected_report_columns,
            yes_task='load_users_details_csv',
            no_task='fail_no_expected_columns',
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        send_email_for_no_users_data = rail.EmailOperator(
            task_id='send_email_for_no_users_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export of termination balances for {{ dag_run.conf.region }} {{ dag_run.conf.location }} is skipped - {{ current_time_in_specified_tz() }}',
            html_content="/templates/emails/email_no_users_file_format.html",
        )

        finish_export = rail.EmptyOperator(
            task_id='finish_export'
        )

        load_users_details_csv = rail.LoadCSVFileOperator(
            task_id='load_users_details_csv',
            document="{{ (result('run_user_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}"
        )

        formated_users_data_to_csv = rail.WriteCSVFileOperator(
            task_id="formated_users_data_to_csv",
            source="{{ result('load_users_details_csv') }}",
            header=["username", "location", "useruri", "userenddate", "exported"],
            row=request_payload.get_formated_user_row,
            thread_pool_size=config.write_csv_threadpool_size
        )

        users_report_data_collection = rail.CreateCollectionOperator(
            task_id="users_report_data_collection",
            name='getalluserdata',
            source='{{result("formated_users_data_to_csv")}}'
        )

        query_disabled_users_data = rail.QueryCollectionOperator(
            task_id="query_disabled_users_data",
            query="""SELECT * FROM getalluserdata WHERE DATE(userenddate) >= DATE(:start_date)
                AND DATE(userenddate) <= DATE(:end_date) AND exported != 'Yes'""",
            query_params={
                "start_date": "{{ dag_run.conf.logging_details.start_date }}",
                "end_date": "{{ dag_run.conf.logging_details.end_date }}"
            }
        )

        has_any_users_data = rail.IfOperator(
            task_id='has_any_users_data',
            test='{{ result("query_disabled_users_data", "length") > 0 }}',
            yes_task="final_users_data_to_csv",
            no_task="finish_export_no_user"
        )

        finish_export_no_user = rail.EmptyOperator(
            task_id="finish_export_no_user",
        )

        final_users_data_to_csv = rail.WriteCSVFileOperator(
            task_id="final_users_data_to_csv",
            source="{{ result('query_disabled_users_data') }}",
            header=["username", "location", "useruri", "id"],
            row=request_payload.get_final_users_data_row
        )

        users_final_data_collection = rail.CreateCollectionOperator(
            task_id="users_final_data_collection",
            source='{{result("final_users_data_to_csv")}}'
        )

        query_all_users_data = rail.QueryCollectionOperator(
            task_id="query_all_users_data",
            query="SELECT * FROM users_final_data_collection",
        )

        has_any_users_final_data = rail.IfOperator(
            task_id='has_any_users_final_data',
            test='{{ result("query_all_users_data", "length") > 0 }}',
            yes_task="get_termination_balance_report_details",
        )

        get_termination_balance_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_termination_balance_report_details',
            report_name='{{ dag_run.conf.termination_balance_report_name }}',
        )

        run_termination_balance_report = rail.run_report2(
            group_id='run_termination_balance_report',
            report_params=request_payload.get_run_termination_balance_report_payload,
            target='artifact'
        )

        is_termination_balance_report_failed = rail.IfOperator(
            task_id='is_termination_balance_report_failed',
            test="{{ (result('run_termination_balance_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_termination_balance_report_generation',
            no_task='termination_balance_report_has_data'
        )

        fail_termination_balance_report_generation = rail.FailOperator(
            task_id='fail_termination_balance_report_generation',
            message="{{ (result('run_termination_balance_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error }}"
        )

        termination_balance_report_has_data = rail.IfOperator(
            task_id='termination_balance_report_has_data',
            test="{{ result('run_termination_balance_report.get_report_result','has_data') }}",
            yes_task='is_termination_balance_report_has_expected_columns',
            no_task='send_email_for_no_termination_balance_data'
        )

        is_termination_balance_report_has_expected_columns = rail.IfOperator(
            task_id='is_termination_balance_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ (result('run_termination_balance_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.termination_balance_expected_report_columns,
            yes_task='load_termination_balance_csv',
            no_task='fail_invalid_termination_report_colums',
        )

        fail_invalid_termination_report_colums = rail.FailOperator(
            task_id='fail_invalid_termination_report_colums',
            message='''Base report column order doesn't match'''
        )

        send_email_for_no_termination_balance_data = rail.EmailOperator(
            task_id='send_email_for_no_termination_balance_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export of termination balances for {{ dag_run.conf.region }} {{ dag_run.conf.location }} is skipped - {{ current_time_in_specified_tz() }}',
            html_content="/templates/emails/email_no_termination_balance_file_format.html",
        )

        load_termination_balance_csv = rail.LoadCSVFileOperator(
            task_id='load_termination_balance_csv',
            document="{{ (result('run_termination_balance_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}"
        )

        termination_balance_report_data_collection = rail.CreateCollectionOperator(
            task_id="termination_balance_report_data_collection",
            name='terminationbalance',
            source='{{ result("load_termination_balance_csv") }}'
        )

        query_invalid_termination_balance_data = rail.QueryCollectionOperator(
            task_id="query_invalid_termination_balance_data",
            query="SELECT * FROM terminationbalance WHERE NULLIF(Employee_ID, '') IS NULL",
        )

        has_invalid_data = rail.IfOperator(
            task_id='has_invalid_data',
            test='{{ result("query_invalid_termination_balance_data", "length") > 0 }}',
            yes_task="logging_no_of_invalid_records",
            no_task="query_valid_termination_balance_data"
        )

        logging_no_of_invalid_records = rail.WriteLogOperator(
            task_id="logging_no_of_invalid_records",
            log="{{ result('create_log') }}",
            message=lambda: "The number of users skipped - {{ result('query_invalid_termination_balance_data','length') }}",
            properties={
                "log": "The number of users skipped -{{ result('query_invalid_termination_balance_data','length') }}"
            }
        )

        query_valid_termination_balance_data = rail.QueryCollectionOperator(
            task_id="query_valid_termination_balance_data",
            query="SELECT * FROM terminationbalance WHERE NULLIF(Employee_ID, '') IS NOT NULL",
        )

        has_valid_data = rail.IfOperator(
            task_id='has_valid_data',
            test='{{ result("query_valid_termination_balance_data", "length") > 0 }}',
            yes_task="final_termination_balance_data_to_csv",
            no_task="finish_export_no_valid_data"
        )

        finish_export_no_valid_data = rail.EmptyOperator(
            task_id="finish_export_no_valid_data",
        )

        final_termination_balance_data_to_csv = rail.WriteCSVFileOperator(
            task_id="final_termination_balance_data_to_csv",
            source="{{ result('query_valid_termination_balance_data') }}",
            header=["RECTY","CLIID","INTCA","ORDNO","IOPER","INFTY","paycodecode","BEGDA",
            "ENDDA","OBJPS","SPRPS","SEQNR","EXTRA","paycodecode2","STDAZ","BEGUZ","ENDUZ","BETRG","WAERS",
            "PayCodeHours","ZEINH"],
            row=request_payload.get_termination_balance_data_row
        )

        no_of_records_size_including_header_footer=rail.PythonOperator(
            task_id="no_of_records_size_including_header_footer",
            python_callable=lambda: int(rail.result('query_valid_termination_balance_data','length')) + 2
        )

        get_exported_custom_field = rail.RepliconServiceOperator(
            task_id="get_exported_custom_field",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Term Exported (AUS)', 'uri')
        )

        process_child_udf_update = rail.TriggerDagRunForEachItemOperator(
            task_id='process_child_udf_update',
            retries=0,
            items="{{ result('query_valid_termination_balance_data') }}",
            trigger_dag_id=config.process_udf_update_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'exported_udf_uri': rail.result("get_exported_custom_field"),
                'user_uri': item['UserUri']
            }
        )

        create_document = rail.RenderTemplateOperator(
            task_id='create_document',
            target='artifact',
            template_file='/schema/termination_balance_export_data.txt',
            dataset="{{ result('final_termination_balance_data_to_csv') }}",
        )

        is_encryption_required = rail.IfOperator(
            task_id='is_encryption_required',
            test='{{ dag_run.conf.encrypt_file | is_truthy}}',
            yes_task=["pgp_encrypt_item_file","can_upload_to_tertiary_sftp"],
            no_task="upload_export_data_to_sftp"
        )

        pgp_encrypt_item_file = rail.PGPEncryptionOperator(
            task_id="pgp_encrypt_item_file",
            source="{{ result('create_document') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        upload_encrypted_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_export_data_to_sftp",
            content='{{result("pgp_encrypt_item_file")}}',
            remote_filepath=config.output_filepath +
            '{{ dag_run.conf.file_name}}.SAP.pgp'
        )

        upload_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_sftp",
            content='{{result("create_document")}}',
            remote_filepath=config.output_filepath +
            '{{ dag_run.conf.file_name}}.SAP'
        )

        can_upload_to_tertiary_sftp = rail.IfOperator(
            task_id = 'can_upload_to_tertiary_sftp',
            test= config.can_upload_to_tertiary_sftp,
            yes_task='pgp_encrypt_for_tertiary_sftp',
            no_task='finish'
        )

        finish = rail.EmptyOperator(
            task_id = "finish"
        )

        # this encryption is for uploading this encrypted file to Replicon SFTP(Tertiary SFTP)
        pgp_encrypt_for_tertiary_sftp = rail.PGPEncryptionOperator(
            task_id="pgp_encrypt_for_tertiary_sftp",
            source="{{ result('create_document') }}",
            pgp_conn_id=config.tertiary_pgp_conn_id
        )

        upload_encrypted_file_tertiary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_file_tertiary_sftp",
            sftp_conn_id=config.tertiary_sftp_conn_id,
            content="{{ result('pgp_encrypt_for_tertiary_sftp') }}",
            remote_filepath=config.tertiary_encrypted_filepath + "{{ dag_run.conf.location }}/" + "{{ dag_run.conf.file_name }}.SAP.pgp"
        )

        fail_tertiary_sftp_upload_error = rail.FailOperator(
            task_id='fail_tertiary_sftp_upload_error',
            trigger_rule='one_failed',
            message=config.error_template
        )

        upload_export_data_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_secondary_sftp",
            sftp_conn_id=config.secondary_sftp_conn_id,
            content='{{result("create_document")}}',
            remote_filepath=config.secondary_output_filepath +
            '{{ dag_run.conf.file_name }}.SAP'
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed'
        )

        is_upload_data_to_sftp_failed = rail.IfOperator(
            task_id='is_upload_data_to_sftp_failed',
            test=request_payload.is_upload_data_to_sftp_failed,
            yes_task="send_email_for_sftp_failure",
            no_task="fail_export"
        )

        send_email_for_sftp_failure = rail.EmailOperator(
            task_id='send_email_for_sftp_failure',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export of termination balances for {{ dag_run.conf.region }} {{ dag_run.conf.location }} is completed - SFTP failure - {{ current_time_in_specified_tz() }}',
            params={
                'output_filepath': config.output_filepath,
            },
            html_content="/templates/emails/email_for_sftp_failure.html",
            files=[
                ('{{ dag_run.conf.file_name }}.SAP', '{{result("final_termination_balance_data_to_csv")}}')]
        )

        logging_no_of_valid_records = rail.WriteLogOperator(
            task_id="logging_no_of_valid_records",
            log="{{ result('create_log') }}",
            message="{{ current_time_in_specified_tz() }} - INFO admin No of records exported = {{result('query_valid_termination_balance_data','length')}}",
            properties={
                "log": "{{ current_time_in_specified_tz() }} - INFO admin No of records exported = {{result('query_valid_termination_balance_data','length')}}",
            }
        )

        logging_file_creation = rail.WriteLogOperator(
            task_id="logging_file_creation",
            log="{{ result('create_log') }}",
            message="{{ current_time_in_specified_tz() }} - INFO admin Export File_{{ dag_run.conf.file_name }}.SAP created",
            properties={
                "log": "{{ current_time_in_specified_tz() }} - INFO admin Export File_{{ dag_run.conf.file_name }}.SAP created"
            }
        )

        process_end_time = rail.PythonOperator(
            task_id="process_end_time",
            python_callable=lambda: pd.now(config.time_zone).strftime("%Y-%m-%dT%H:%M:%S")
        )

        logging_job_end_time = rail.WriteLogOperator(
            task_id="logging_job_end_time",
            log="{{ result('create_log') }}",
            message="{{result('process_end_time')}} - Process ended",
            properties={
                "log": "{{result('process_end_time')}} - Process ended"
            }
        )

        log_file_data_to_csv = rail.WriteCSVFileOperator(
            task_id="log_file_data_to_csv",
            source="{{ result('create_log') }}",
            header=None,
            row=[
                '{{ item.properties | attr_or_default("log", "") }}'
            ]
        )

        send_email_for_export_completion = rail.EmailOperator(
            task_id='send_email_for_export_completion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll export of termination balances for {{ dag_run.conf.region }} {{ dag_run.conf.location }} is completed successfully - {{ current_time_in_specified_tz() }}',
            params={
                'output_filepath': config.output_filepath,
                'log_filepath': config.log_filepath

            },
            html_content="/templates/emails/email_for_export_success.html"
        )

        upload_log_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_data_to_sftp",
            content='{{ result("log_file_data_to_csv") }}',
            remote_filepath=config.log_filepath +
            'log_{{ dag_run.conf.file_name }}.txt'
        )

        can_upload_logs_to_tertiary_sftp = rail.IfOperator(
            task_id = 'can_upload_logs_to_tertiary_sftp',
            test= config.can_upload_to_tertiary_sftp and '{{ dag_run.conf.encrypt_file | is_truthy}}',
            yes_task='upload_log_data_to_tertiary_sftp',
            no_task='finish_log'
        )

        finish_log = rail.EmptyOperator(
            task_id = "finish_log"
        )

        upload_log_data_to_tertiary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_data_to_tertiary_sftp",
            sftp_conn_id=config.tertiary_sftp_conn_id,
            content='{{result("log_file_data_to_csv")}}',
            remote_filepath=config.tertiary_log_filepath + "{{ dag_run.conf.location }}/logs/" + "log_{{ dag_run.conf.file_name }}.txt"
        )

        fail_tertiary_sftp_log_upload_error = rail.FailOperator(
            task_id='fail_tertiary_sftp_log_upload_error',
            trigger_rule='one_failed',
            message=config.error_template
        )

        is_upload_log_to_sftp_failed = rail.IfOperator(
            task_id='is_upload_log_to_sftp_failed',
            test=request_payload.is_upload_log_to_sftp_failed,
            yes_task="send_email_for_log_upload_failure",
            no_task="fail_export_before_log"
        )

        send_email_for_log_upload_failure = rail.EmailOperator(
            task_id='send_email_for_log_upload_failure',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll export of termination balances for {{ dag_run.conf.region }} {{ dag_run.conf.location }} is completed - SFTP failure - {{ current_time_in_specified_tz() }}',
            params={
                'log_filepath': config.log_filepath
            },
            html_content="/templates/emails/email_for_log_upload_failure.html",
            files=[
                ('log_{{ dag_run.conf.file_name }}.txt', '{{ result("log_file_data_to_csv") }}')]
        )

        fail_export = rail.FailOperator(
            task_id="fail_export",
            message="termination file export has failed"
        )

        fail_export_before_log = rail.FailOperator(
            task_id="fail_export_before_log",
            message="termination file export has failed"
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> create_document
        can_run_batch_task >> rail.Label("No") >> process_start_time
        process_start_time >> create_log >> logging_job_start_time >> get_all_enabled_divisions \
            >> get_user_report_details >> run_user_details_report >> is_users_report_failed

        is_users_report_failed >> rail.Label("Yes") >> fail_users_report_generation
        is_users_report_failed >> rail.Label("No") >> users_report_has_data

        users_report_has_data >> rail.Label("Yes") >> is_users_report_has_expected_columns
        users_report_has_data >> rail.Label("No") >> send_email_for_no_users_data

        is_users_report_has_expected_columns >> rail.Label("Yes") >> load_users_details_csv
        is_users_report_has_expected_columns >> rail.Label("No") >> fail_no_expected_columns

        load_users_details_csv >> formated_users_data_to_csv >> users_report_data_collection \
            >> query_disabled_users_data >> has_any_users_data >> rail.Label("Yes") >> final_users_data_to_csv
        has_any_users_data >> rail.Label("No") >> finish_export_no_user
        final_users_data_to_csv >> users_final_data_collection >> query_all_users_data >> has_any_users_final_data
        has_any_users_final_data >> rail.Label("Yes") >> get_termination_balance_report_details \
            >> run_termination_balance_report >> is_termination_balance_report_failed

        is_termination_balance_report_failed >> rail.Label("Yes") >> fail_termination_balance_report_generation
        is_termination_balance_report_failed >> rail.Label("No") >> termination_balance_report_has_data

        termination_balance_report_has_data >> rail.Label("Yes") >> is_termination_balance_report_has_expected_columns
        termination_balance_report_has_data >> rail.Label("No") >> send_email_for_no_termination_balance_data

        is_termination_balance_report_has_expected_columns >> rail.Label("Yes") >> load_termination_balance_csv
        is_termination_balance_report_has_expected_columns >> rail.Label("No") >> fail_invalid_termination_report_colums

        load_termination_balance_csv >> termination_balance_report_data_collection \
            >> query_invalid_termination_balance_data >> has_invalid_data
        has_invalid_data >> rail.Label("Yes") >> logging_no_of_invalid_records >> query_valid_termination_balance_data
        has_invalid_data >> rail.Label("No") >> query_valid_termination_balance_data
        query_valid_termination_balance_data >> has_valid_data >> rail.Label("Yes") >> final_termination_balance_data_to_csv \
            >> no_of_records_size_including_header_footer >> get_exported_custom_field >> process_child_udf_update \
                >> create_document >> is_encryption_required >> rail.Label("Yes") \
                    >> [pgp_encrypt_item_file, can_upload_to_tertiary_sftp]
        can_upload_to_tertiary_sftp >> rail.Label('Yes') >> pgp_encrypt_for_tertiary_sftp >> upload_encrypted_file_tertiary_sftp
        can_upload_to_tertiary_sftp >> rail.Label('No') >> finish
        upload_encrypted_file_tertiary_sftp >> rail.Label("on_error") >>  fail_tertiary_sftp_upload_error
        is_encryption_required >> rail.Label("No") >> upload_export_data_to_sftp
        has_valid_data >> rail.Label("No") >> finish_export_no_valid_data
        pgp_encrypt_item_file >> upload_encrypted_export_data_to_sftp
        upload_encrypted_export_data_to_sftp >> rail.Label(
            "on_success") >> upload_export_data_to_secondary_sftp >> logging_no_of_valid_records
        upload_export_data_to_sftp >> rail.Label(
            "on_success") >> upload_export_data_to_secondary_sftp >> logging_no_of_valid_records >> logging_file_creation
        upload_encrypted_export_data_to_sftp >> rail.Label("on_error") >> catch_error
        upload_export_data_to_sftp >> rail.Label("on_error") >> catch_error >> is_upload_data_to_sftp_failed \
            >> rail.Label("Yes") >> send_email_for_sftp_failure
        is_upload_data_to_sftp_failed >> rail.Label("No") >> fail_export
        logging_file_creation >> process_end_time >> logging_job_end_time >> log_file_data_to_csv \
            >> send_email_for_export_completion >> [upload_log_data_to_sftp, can_upload_logs_to_tertiary_sftp]
        can_upload_logs_to_tertiary_sftp >> rail.Label('Yes') >> upload_log_data_to_tertiary_sftp
        can_upload_logs_to_tertiary_sftp >> rail.Label('No') >> finish_log
        upload_log_data_to_tertiary_sftp >> rail.Label('on_error') >> fail_tertiary_sftp_log_upload_error
        upload_log_data_to_tertiary_sftp >> rail.Label('on_success') >> finish_log
        upload_log_data_to_sftp >> rail.Label("on_success") >> finish_export
        upload_log_data_to_sftp >> rail.Label("on_error") >> catch_error >> is_upload_log_to_sftp_failed \
            >> rail.Label("Yes") >> send_email_for_log_upload_failure
        is_upload_log_to_sftp_failed >> rail.Label(
            "No") >> fail_export_before_log
    return dag

rail.for_each_instance(create_child_dag)
