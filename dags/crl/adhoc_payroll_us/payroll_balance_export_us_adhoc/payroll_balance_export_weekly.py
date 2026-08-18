# pylint: disable=too-many-statements
from datetime import datetime as dt, timedelta
from pendulum import datetime
import rail
from crl.adhoc_payroll_us.payroll_balance_export_us_adhoc.utils import request_payload

OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.weekly_dag_id,
        description=f'CRL payroll balance_weekly_usa adhoc {config.instance}',
        company_key=config.company_key,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_dag_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        webhook_conf=[
            rail.WebhookConf(
                bearer_token_var=config.crl_payroll_export_bearer_token_variable)
        ]
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        get_file_name = rail.PythonOperator(
            task_id="get_file_name",
            python_callable=lambda: config.file_name_prefix + "_" + dt.utcnow().strftime("%Y%m%d%H%M%S") +
            "_USTIME_HRMD" +
            request_payload.get_sequence(
                config.USA_PAYROLL_CALENDER_MAPPER_TO_USE_WEEKLY, config.time_zone)+"_DUT8G2I"
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

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        logging_job_start_time = rail.WriteLogOperator(
            task_id="logging_job_start_time",
            log="{{ result('create_log') }}",
            message="{{result('process_start_time')}} - Process started",
            properties={
                "log": "{{result('process_start_time')}} - Process started"
            }
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        load_report = rail.run_report(
            group_id='load_report',
            report_params=request_payload.get_run_report_payload
        )

        has_data = rail.IfOperator(
            task_id="has_data",
            test='{{ result("load_report.get_report_result", "has_data") }}',
            yes_task='report_has_expected_columns',
            no_task='send_email_for_no_users_data'
        )

        send_email_for_no_users_data = rail.EmailOperator(
            task_id='send_email_for_no_users_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon payroll export file is skipped for ' + \
            config.location + \
            ' location  on - {{ current_time_in_specified_tz() }}',
            params={
                'start_date': request_payload.get_start_date_begin_of_week(),
                'end_date': request_payload.get_end_date_begin_of_week(),
                'location': config.location
            },
            html_content="templates/email/empty_users_data.html",
        )

        finish_export = rail.EmptyOperator(
            task_id='finish_export'
        )

        # pylint: disable=line-too-long
        report_expected_report_columns = config.report_column
        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            # pylint: disable=consider-using-f-string
            test="{{ result('load_report.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % report_expected_report_columns,
            no_task='fail_invalid_report_colums',
            yes_task='report_payload_to_csv',
        )

        fail_invalid_report_colums = rail.FailOperator(
            task_id="fail_invalid_report_colums",
            message="Base report column does not match"
        )

        report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="report_payload_to_csv",
            document='{{result("load_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        formated_data_to_csv = rail.WriteCSVFileOperator(
            task_id="formated_data_to_csv",
            source="{{ result('report_payload_to_csv') }}",
            header=["employeeid", "timeofftypes",
                    "timeoffaccrued", "timeofftaken", "timeoffbalance", "employeetype", "jobcode", "location","paygroup"],
            row=request_payload.get_formated_row,
            thread_pool_size=config.thread_pool_size_write_csv
        )

        report_data_collection = rail.CreateCollectionOperator(
            task_id="report_data_collection",
            name='getallbalancedata',
            source='{{result("formated_data_to_csv")}}'
        )

        query_timeoff_balance_data = rail.QueryCollectionOperator(
            task_id="query_timeoff_balance_data",
            # pylint: disable=line-too-long
            query="SELECT * FROM getallbalancedata WHERE paygroup ='NYW' AND  ((timeofftypes='[USA] Emergency Leave' AND employeetype NOT IN "+str(config.employee_types)+")" +
            "OR (timeofftypes='[USA] Floating Holiday' AND employeetype NOT IN "+str(config.employee_types)+")" +
            "OR (timeofftypes='[USA] Vacation' AND employeetype NOT IN "+str(config.employee_types)+" AND jobcode NOT IN "+str(config.job_code)+")" +
            "OR (timeofftypes='[USA] Sick' AND ((employeetype IN "+str(config.sick_employee_types)+") OR (employeetype IN "+str(
                config.salaried_employee_types)+" AND location IN "+str(config.location_code)+"))))"
        )

        has_any_data = rail.IfOperator(
            task_id='has_any_data',
            test='{{ result("query_timeoff_balance_data", "length") > 0 }}',
            yes_task="final_data_to_csv",
            no_task="finish_export_no_data"
        )

        finish_export_no_data = rail.EmptyOperator(
            task_id="finish_export_no_data",
        )

        final_data_to_csv = rail.WriteCSVFileOperator(
            task_id="final_data_to_csv",
            source="{{ result('query_timeoff_balance_data') }}",
            row=request_payload.get_final_formated_row,
            thread_pool_size=config.thread_pool_size_write_csv
        )

        final_data_collection = rail.CreateCollectionOperator(
            task_id="final_data_collection",
            source='{{result("final_data_to_csv")}}'
        )

        query_accrued_data = rail.QueryCollectionOperator(
            task_id="query_accrued_data",
            query="SELECT employeeid,timeofftypes,timeoffaccrued FROM final_data_collection",
        )

        final_accrued_data_to_csv = rail.WriteCSVFileOperator(
            task_id="final_accrued_data_to_csv",
            source="{{ result('query_accrued_data') }}",
            row=request_payload.get_accrued_data_row,
            header=["employeeid", "timeofftypes",
                    "balance", "balancetype"],
            thread_pool_size=config.thread_pool_size_write_csv
        )

        export_accured_data_collection = rail.WriteLogOperator(
            task_id="export_accured_data_collection",
            message="sucess",
            items='{{result("final_accrued_data_to_csv")}}',
            properties=lambda item: {
                'employeeid': item['employeeid'],
                'timeofftypes': item['timeofftypes'],
                'balance': item['balance'],
                'balancetype': item['balancetype']
            }
        )

        query_timeofftaken_data = rail.QueryCollectionOperator(
            task_id="query_timeofftaken_data",
            query="SELECT employeeid,timeofftypes,timeofftaken FROM final_data_collection",
        )

        final_timeoff_data_to_csv = rail.WriteCSVFileOperator(
            task_id="final_timeoff_data_to_csv",
            source="{{ result('query_timeofftaken_data') }}",
            row=request_payload.get_timeofftaken_data_row,
            header=["employeeid", "timeofftypes",
                    "balance", "balancetype"],
            thread_pool_size=config.thread_pool_size_write_csv
        )

        export_timeofftaken_data_collection = rail.WriteLogOperator(
            task_id="export_timeofftaken_data_collection",
            message="sucess",
            items='{{result("final_timeoff_data_to_csv")}}',
            properties=lambda item: {
                'employeeid': item['employeeid'],
                'timeofftypes': item['timeofftypes'],
                'balance': item['balance'],
                'balancetype': item['balancetype']
            }
        )

        query_timeoffbalance_data = rail.QueryCollectionOperator(
            task_id="query_timeoffbalance_data",
            query="SELECT employeeid,timeofftypes,timeoffbalance FROM final_data_collection",
        )

        final_timeoffbalance_data_to_csv = rail.WriteCSVFileOperator(
            task_id="final_timeoffbalance_data_to_csv",
            source="{{ result('query_timeoffbalance_data') }}",
            row=request_payload.get_timeoffbalance_data_row,
            header=["employeeid", "timeofftypes",
                    "balance", "balancetype"],
            thread_pool_size=config.thread_pool_size_write_csv
        )

        export_timeoffbalance_data_collection = rail.WriteLogOperator(
            task_id="export_timeoffbalance_data_collection",
            message="sucess",
            items='{{result("final_timeoffbalance_data_to_csv")}}',
            properties=lambda item: {
                'employeeid': item['employeeid'],
                'timeofftypes': item['timeofftypes'],
                'balance': item['balance'],
                'balancetype': item['balancetype']
            }
        )

        get_all_data = rail.WriteCSVFileOperator(
            task_id='get_all_data',
            source="{{ get_master_log() }}",
            header=["employeeid", "timeofftypes",
                    "balance", "balancetype"],
            row=['{{ item.properties.employeeid }}',
                 '{{ item.properties.timeofftypes }}',
                 '{{ item.properties.balance }}',
                 '{{ item.properties.balancetype }}'],
            thread_pool_size=config.thread_pool_size_write_csv
        )

        final_export_data_collection = rail.CreateCollectionOperator(
            task_id="final_export_data_collection",
            name='final_data',
            source='{{result("get_all_data")}}'
        )

        query_valid_balance_data = rail.QueryCollectionOperator(
            task_id="query_valid_balance_data",
            query="SELECT * FROM final_data",
        )

        has_valid_data = rail.IfOperator(
            task_id='has_valid_data',
            test='{{ result("query_valid_balance_data", "length") > 0 }}',
            yes_task="final_balance_data_to_csv",
            no_task="finish_export_no_valid_data"
        )

        finish_export_no_valid_data = rail.EmptyOperator(
            task_id="finish_export_no_valid_data",
        )

        no_of_records_size_including_header_footer = rail.PythonOperator(
            task_id="no_of_records_size_including_header_footer",
            python_callable=lambda:  int(rail.result(
                'query_valid_balance_data', 'length')) + 2
        )

        final_balance_data_to_csv = rail.WriteCSVFileOperator(
            task_id="final_balance_data_to_csv",
            source="{{ result('query_valid_balance_data') }}",
            header=["RECTY", "CLIID", "INTCA", "ORDNO", "IOPER", "INFTY", "SUBTY", "BEGDA",
                    "ENDDA", "OBJPS", "SPRPS", "SEQNR", "EXTRA", "LGART", "STDAZ", "BEGUZ", "ENDUZ", "BETRG",
                    "WAERS", "ANZHL", "ZEINH", "VTKEN", "BWGRL", "AUFKZ", "ENDOF", "UFLD1", "UFLD2", "UFLD3", "KEYPR", "TRFGR",
                    "TRFST", "PRAKN", "PRAKZ", "OTYPE", "PLANS", "VERSL", "EXBEL", "WTART", "TDLANGU", "TDSUBLA", "TDTYPE"],
            row=request_payload.get_balance_us_data_row,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            thread_pool_size=config.thread_pool_size_write_csv
        )

        create_document = rail.RenderTemplateOperator(
            task_id='create_document',
            target='artifact',
            template_file='schema/sample_balance.txt',
            dataset="{{ result('final_balance_data_to_csv') }}",
        )

        pgp_encyrpt_item_file = rail.PGPEncryptionOperator(
            task_id="pgp_encyrpt_item_file",
            source="{{ result('create_document') }}",
            pgp_conn_id=config.pgp_conn_id,
            sign=True
        )

        upload_encrypted_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_export_data_to_sftp",
            content='{{result("pgp_encyrpt_item_file")}}',
            remote_filepath=config.output_filepath +
            '{{ result("get_file_name")}}.SAP.pgp'
        )

        upload_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_sftp",
            sftp_conn_id=config.secondary_encrypted_sftp_conn_id,
            content="{{ result('pgp_encyrpt_item_file') }}",
            remote_filepath=config.secondary_encrypted_output_filepath +
            "{{ result('get_file_name')}}.SAP.pgp"
        )

        upload_export_data_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_secondary_sftp",
            sftp_conn_id=config.secondary_sftp_conn_id,
            content='{{result("create_document")}}',
            remote_filepath=config.secondary_encrypted_output_filepath +
            '{{ result("get_file_name")}}.SAP'
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
            subject='{{ get_company_key() }} | Payroll balance data export automation - SFTP failure for ' + \
            config.location+' location  on - current_time_in_specified_tz() }}',
            params={
                'output_filepath': config.output_filepath,
            },
            html_content="templates/email/sftp_failure.html",
            files=[
                ('{{ dag_run.conf.file_name }}.SAP.pgp', '{{result("pgp_encyrpt_item_file")}}')]
        )

        logging_no_of_valid_records = rail.WriteLogOperator(
            task_id="logging_no_of_valid_records",
            log="{{ result('create_log') }}",
            message="{{ current_time_in_specified_tz() }} - INFO admin No of records exported = {{result('query_valid_balance_data','length')}}",
            properties={
                "log": "{{ current_time_in_specified_tz() }} - INFO admin No of records exported = {{result('query_valid_balance_data','length')}}",
            }
        )

        logging_file_creation = rail.WriteLogOperator(
            task_id="logging_file_creation",
            log="{{ result('create_log') }}",
            message="{{ current_time_in_specified_tz() }} - INFO admin Export File_" +
            '{{ result("get_file_name")}}' + " created",
            properties={
                "log": "{{ current_time_in_specified_tz() }} - INFO admin Export File_" +
                    '{{ result("get_file_name")}}' + ".txt"
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

        send_email_for_export_copmpletion = rail.EmailOperator(
            task_id='send_email_for_export_copmpletion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll balance export file completed  on - {{ current_time_in_specified_tz() }}',
            params={
                'output_filepath': config.output_filepath,
                'log_filepath': config.log_filepath,
                'location': config.location,
                'file_name': '{{ result("get_file_name")}}'

            },
            html_content="templates/email/export_success.html"
        )

        upload_log_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_data_to_sftp",
            content='{{result("log_file_data_to_csv")}}',
            remote_filepath=config.log_filepath +
            "log_"+'{{ result("get_file_name")}}' + ".txt"
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
            subject='{{ get_company_key() }} | Replicon payroll balance export file - SFTP failure for ' +
            config.location+' location {{ current_time_in_specified_tz() }}',
            params={
                'log_filepath': config.log_filepath,
                'location': config.location
            },
            html_content="templates/email/log_upload_failure.html",
            files=[
                ("log_"+'{{ dag_run.conf.file_name }}', '{{result("log_file_data_to_csv")}}')]
        )

        fail_export = rail.FailOperator(
            task_id="fail_export",
            message="termination file export has failed"
        )

        fail_export_before_log = rail.FailOperator(
            task_id="fail_export_before_log",
            message="termination file export has failed"
        )

        log_to_sumo_valid_export = rail.SendToSumoOperator(
            task_id="log_to_sumo_valid_export",
            data={
                'job_start_time': '{{ result("process_start_time") }}',
                'job_end_time': f'{OPEN_BRACKETS} current_time_in_specified_tz("{config.time_zone}", "%Y-%m-%dT%H:%M:%S") {CLOSE_BRACKETS}',
                'export_file_name': '{{ result("get_file_name")}}',
                'export_filepath': config.output_filepath,
                'numberofrecords': "{{ result('query_valid_balance_data', 'length')}}",
            },
            sumo_conn_id="sumologic-exportlogger"
        )

        # pylint: disable=line-too-long
        get_file_name >> process_start_time >> process_start_time_ymd_format >> process_start_time_hms_format >> create_log >> \
            logging_job_start_time >> get_report_details >> load_report >> has_data >> rail.Label("Yes"
                                                                                                  ) >> report_has_expected_columns
        has_data >> rail.Label("No") >> send_email_for_no_users_data
        report_has_expected_columns >> rail.Label(
            "Yes") >> report_payload_to_csv >> formated_data_to_csv >> report_data_collection
        report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_colums
        report_data_collection >> query_timeoff_balance_data >> has_any_data >> rail.Label(
            "Yes") >> final_data_to_csv
        has_any_data >> rail.Label("No") >> finish_export_no_data
        final_data_to_csv >> final_data_collection >> query_accrued_data >> final_accrued_data_to_csv >> export_accured_data_collection >> \
            query_timeofftaken_data >> final_timeoff_data_to_csv >> export_timeofftaken_data_collection >> query_timeoffbalance_data >> \
            final_timeoffbalance_data_to_csv >> export_timeoffbalance_data_collection >> get_all_data >> final_export_data_collection >> query_valid_balance_data
        query_valid_balance_data >> has_valid_data >> rail.Label("Yes") >> final_balance_data_to_csv >> no_of_records_size_including_header_footer >> create_document >>\
            pgp_encyrpt_item_file >> upload_export_data_to_sftp >> upload_encrypted_export_data_to_sftp
        has_valid_data >> rail.Label("No") >> finish_export_no_valid_data
        upload_encrypted_export_data_to_sftp >> rail.Label(
            "on_success") >> upload_export_data_to_secondary_sftp >> logging_no_of_valid_records >> logging_file_creation
        upload_encrypted_export_data_to_sftp >> rail.Label("on_error") >> catch_error >> is_upload_data_to_sftp_failed >> rail.Label("Yes"
                                                                                                                                     ) >> send_email_for_sftp_failure
        is_upload_data_to_sftp_failed >> rail.Label("No") >> fail_export
        logging_file_creation >> process_end_time >> logging_job_end_time >> log_file_data_to_csv >> send_email_for_export_copmpletion >> upload_log_data_to_sftp
        upload_log_data_to_sftp >> rail.Label("on_success") >> finish_export >> log_to_sumo_valid_export
        upload_log_data_to_sftp >> rail.Label("on_error") >> catch_error >> is_upload_log_to_sftp_failed >> rail.Label("Yes"
                                                                                                                       ) >> send_email_for_log_upload_failure
        is_upload_log_to_sftp_failed >> rail.Label(
            "No") >> fail_export_before_log

    return dag


rail.for_each_instance(create_main_dag)
