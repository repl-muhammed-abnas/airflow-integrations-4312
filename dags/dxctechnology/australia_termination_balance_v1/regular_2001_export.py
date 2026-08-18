# pylint: disable=too-many-statements
from datetime import datetime as dt
import rail
from dxctechnology.australia_payroll_extract_v1.utils import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_australia_terminated_export_absence_taken_child_{config.instance}_v1',
        description=f'DXC_Australia_Terminated_Export_Absence_taken_Child V1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_file_name = rail.PythonOperator(
            task_id="get_file_name",
            python_callable=lambda dag_run: config.file_name_prefix +
            "_" + dt.utcnow().strftime("%Y%m%d%H%M%S")
            + "_AUREPL_RE"+ dag_run.conf['file_diff'] + dag_run.conf['sequence_no'] + "_DUT8G2I"
        )

        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=lambda:  dt.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        )

        process_start_time_ymd_format = rail.PythonOperator(
            task_id="process_start_time_ymd_format",
            python_callable=lambda:  dt.utcnow().strftime("%Y%m%d")
        )

        process_start_time_hms_format = rail.PythonOperator(
            task_id="process_start_time_hms_format",
            python_callable=lambda:  dt.utcnow().strftime("%H%M%S")
        )

        get_all_pay_codes_from_mapper= rail.PythonOperator(
            task_id= 'get_all_pay_codes_from_mapper',
            python_callable=lambda: request_payload.get_all_required_pacodes(config.absence_taken_mapper)
        )

        # pylint: disable=line-too-long
        query_list_in_final_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_list_in_final_payroll_collection',
            name='absencetakendata',
            query="""SELECT * FROM final_payroll_item_data WHERE NULLIF(Pay_Code_Description, '') IS NOT NULL AND Pay_Code_Description = '2001' """
        )

        has_absence_payroll_data = rail.IfOperator(
            task_id='has_absence_payroll_data',
            test="{{ result('query_list_in_final_payroll_collection','length') > 0 }}",
            yes_task='compose_item_payroll_csv_file',
            no_task='finish_export_no_payroll_data'
        )

        finish_export_no_payroll_data = rail.EmptyOperator(
            task_id='finish_export_no_payroll_data'
        )

        send_email_for_no_payroll_data = rail.EmailOperator(
            task_id='send_email_for_no_payroll_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export for AUS Absence Taken is skipped for Australia location  on - {{ current_time_in_specified_tz() }}',
            html_content="templates/email/es_blank_export.html"
        )

        compose_item_payroll_csv_file = rail.WriteCSVFileOperator(
            task_id='compose_item_payroll_csv_file',
            source="{{ result('query_list_in_final_payroll_collection') }}",
            header=["RECTY", "CLIID", "INTCA", "ORDNO", "IOPER", "INFTY", "SUBTY", "BEGDA",
                    "ENDDA", "OBJPS", "SPRPS", "SEQNR", "EXTRA", "AWART", "BEGUZ", "ENDUZ", "STDAZ", "VTKEN",
                    "ABWTG", "ABRTG", "ABRST", "ANRTG", "LFZED", "KRGED", "KBBEG", "RMDDA", "KENN1", "KENN2", "KALTG", "URMAN", "BEGVA",
                    "BWGRL", "AUFKZ", "TRFGR", "TRFST", "PRAKN", "PRAKZ", "OTYPE", "PLANS",
                    "MLDDA", "MLDUZ", "RMDUZ", "VORGS", "UMSKD", "UMSCH", "REFNR", "UNFAL", "STKRV", "STUND",
                    "PSARB", "AINFT", "GENER", "HRSIF", "ALLDF", "WAERS", "AWTYP", "AWREF", "AWORG", "PAYTY", "PAYID", "BONDT", "OCRSN",
                    "SPPE1", "SPPE2", "SPPE3", "SPPIN", "ZKMKT", "FAPRS", "TDLANGU", "TDSUBLA", "TDTYPE", "DOCSY", "DOCNR", "PRU_REFNR"],
            row=request_payload.get_compose_item_active_user_payroll_aus_data_row,
        )

        logging_no_of_records_exported = rail.WriteLogOperator(
            task_id="logging_no_of_records_exported",
            log="{{ result('create_log') }}",
            message="{{ current_time() }} - INFO admin No of records exported = {{result('query_list_in_final_payroll_collection','length')}}",
            properties={
                "log": "{{ current_time() }} - INFO admin No of records exported = {{result('query_list_in_final_payroll_collection','length')}}",
            }
        )

        no_of_records_size_including_header_footer = rail.PythonOperator(
            task_id="no_of_records_size_including_header_footer",
            python_callable=lambda:  int(rail.result(
                'query_list_in_final_payroll_collection', 'length')) + 2
        )

        create_document = rail.RenderTemplateOperator(
            task_id='create_document',
            target='artifact',
            template_file='schema/absence_taken_export_data.txt',
            dataset="{{ result('compose_item_payroll_csv_file') }}",
        )

        pgp_encyrpt_item_file = rail.PGPEncryptionOperator(
            task_id="pgp_encyrpt_item_file",
            source="{{ result('create_document') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        upload_payroll_item_file_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_payroll_item_file_secondary_sftp",
            sftp_conn_id=config.secondary_sftp_conn_id,
            content="{{ result('create_document')}}",
            remote_filepath=config.secondary_output_filepath +
            "{{ result('get_file_name')}}.SAP"
        )

        upload_encrypted_file_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_file_to_secondary_sftp",
            sftp_conn_id=config.secondary_encrypted_sftp_conn_id,
            content="{{ result('pgp_encyrpt_item_file') }}",
            remote_filepath=config.secondary_encrypted_output_filepath +
            "{{ result('get_file_name')}}.SAP.pgp"
        )

        upload_encrypted_payroll_item_file_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_payroll_item_file_sftp",
            content="{{ result('pgp_encyrpt_item_file') }}",
            remote_filepath=config.output_filepath +
            "{{ result('get_file_name')}}.SAP.pgp"
        )

        fail_sftp_upload_error = rail.FailOperator(
            task_id='fail_sftp_upload_error',
            message=config.error_template
        )

        send_email_for_sftp_failure = rail.EmailOperator(
            task_id='send_email_for_sftp_failure',
            trigger_rule='one_failed',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export for Australia AUS-2001 - SFTP failure for {{dag_run.conf.location_name}}-{{dag_run.conf.division_name}}-{{current_time_in_specified_tz()}}',
            params={
                'output_filepath': config.output_filepath,
            },
            html_content="templates/email/sftp_failure.html",
            files=[
                ("{{ result('get_file_name')}}.SAP.pgp", '{{result("pgp_encyrpt_item_file")}}')]
        )

        logging_file_creation = rail.WriteLogOperator(
            task_id="logging_file_creation",
            log="{{ result('create_log') }}",
            message="{{ current_time() }} - INFO admin Export File_" +
            "{{ result('get_file_name')}}",
            properties={
                "log": " {{ current_time() }} - INFO admin Export File_" +
                "{{ result('get_file_name')}}" + ".txt"
            }
        )

        process_end_time = rail.PythonOperator(
            task_id="process_end_time",
            python_callable=lambda:  dt.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
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

        upload_log_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_data_to_sftp",
            content='{{result("log_file_data_to_csv")}}',
            remote_filepath=config.log_filepath + "log_" +
            "{{ result('get_file_name')}}" + ".txt"
        )

        send_email_for_export_completion = rail.EmailOperator(
            task_id='send_email_for_export_completion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export for Australia AUS-2001 completed for - {{dag_run.conf.location_name}} - {{dag_run.conf.division_name}} - {{current_time_in_specified_tz()}}',
            params={
                'output_filepath': config.output_filepath,
                'log_filepath': config.log_filepath,
            },
            html_content="templates/email/export_success.html"
        )

        fail_log_upload_error = rail.FailOperator(
            task_id='fail_log_upload_error',
            message=config.error_template
        )

        send_email_for_log_upload_failure = rail.EmailOperator(
            task_id='send_email_for_log_upload_failure',
            trigger_rule='one_failed',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export for Australia AUS-2001 - SFTP failure for {{dag_run.conf.location_name}}-{{dag_run.conf.division_name}}-{{current_time_in_specified_tz()}}',
            params={
                'log_filepath': config.log_filepath,
            },
            html_content="templates/email/log_upload_failure.html",
            files=[
                ("log_{{ result('get_file_name')}}", '{{result("log_file_data_to_csv")}}')]
        )

        # pylint: disable=line-too-long
        create_log >> get_file_name >> process_start_time >> process_start_time_ymd_format >> process_start_time_hms_format >> get_all_pay_codes_from_mapper >> query_list_in_final_payroll_collection
        query_list_in_final_payroll_collection >> has_absence_payroll_data
        has_absence_payroll_data >> rail.Label("No") >> finish_export_no_payroll_data >> send_email_for_no_payroll_data
        has_absence_payroll_data >> rail.Label("Yes") >> compose_item_payroll_csv_file >> logging_no_of_records_exported >> no_of_records_size_including_header_footer >> create_document >>\
            upload_payroll_item_file_secondary_sftp >> pgp_encyrpt_item_file >> upload_encrypted_file_to_secondary_sftp >> upload_encrypted_payroll_item_file_sftp

        upload_encrypted_payroll_item_file_sftp >> rail.Label(
            "on_success") >> logging_file_creation >> process_end_time >> logging_job_end_time >> log_file_data_to_csv
        log_file_data_to_csv >> upload_log_data_to_sftp >> rail.Label(
            "on_success") >> send_email_for_export_completion
        upload_encrypted_payroll_item_file_sftp >> rail.Label(
            "on_error") >> send_email_for_sftp_failure >> fail_sftp_upload_error
        upload_log_data_to_sftp >> rail.Label(
            "on_error") >> send_email_for_log_upload_failure >> fail_log_upload_error
    return dag

rail.for_each_instance(create_child_dag)
