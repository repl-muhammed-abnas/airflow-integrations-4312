from datetime import timedelta
from pendulum import datetime
import pendulum
import rail
from crl.timeoff_balance_export.utils import request_payload, python_callable, response_filter

# pylint: disable=too-many-statements line-too-long
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.crl_timeoff_balance_export_master,
        description=f"CRL Payout Export Master {config.instance}",
        company_key=config.company_key,
        start_date=datetime(2023, 12, 1, tz=config.time_zone),
        schedule_interval=config.daily_schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:
        
        def can_process_run_test():
            current_date = pendulum.now(config.time_zone).strftime("%d-%m-%Y")
            return bool(rail.find_first_by_attr_and_get_attr(
                config.CANADA_PAYROLL_CALENDER_MAPPER_TO_USE, "payroll_processing_date", current_date))

        can_process_run = rail.IfOperator(
            task_id = "can_process_run",
            test=can_process_run_test,
            yes_task="process_start_time"
        )

        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=python_callable.get_time_in_formats,
            op_args=[config.time_zone]
        )

        get_file_name = rail.PythonOperator(
            task_id='get_file_name',
            python_callable=lambda: python_callable.get_file_name(config)
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.timeoff_report_name,
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id='run_report',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": rail.result('get_report_details')['uri']
                    }
                ]
            }
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_expected_columns"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_expected_columns = rail.IfOperator(
            task_id = "report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_report_columns,
            no_task='fail_invalid_report_colums',
            yes_task='report_has_data',
        )

        fail_invalid_report_colums = rail.FailOperator(
            task_id = "fail_invalid_report_colums",
            message="Base report column does not match"
        )

        report_has_data = rail.IfOperator(
            task_id = "report_has_data",
            test= "{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_users_report_data',
            no_task= 'finish_export_no_payout_data'
        )

        finish_export_no_payout_data = rail.EmptyOperator(
            task_id='finish_export_no_payout_data'
        )

        send_email_for_no_payout_data = rail.EmailOperator(
            task_id='send_email_for_no_payout_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | ADP Payout Export Notification',
            html_content="/templates/email/blank_export.html"
        )

        load_users_report_data = rail.LoadCSVFileOperator(
            task_id='load_users_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        timeoff_report_data_collection = rail.CreateCollectionOperator(
            task_id='timeoff_report_data_collection',
            source="{{ result('load_users_report_data') }}",
            columns={
                "Employee ID": "empid",
                "Login Name": "loginname",
                "useruri": "useruri",
                "Time Off Type": "timeoff_type",
                "Time Off Balance": "timeoff_balance",
                "Sick Payout Eligible": "sick_eligible",
                "Banked Overtime Payout Eligible": "banked_eligible",
                "User Start Date": "user_start_date",
                "User End Date": "user_end_date"
            },
            name="sickandbanedtimeoffdetails"
        )

        create_log = rail.CreateLogOperator(
            task_id="create_log"
        )

        query_report_data = rail.QueryCollectionOperator(
            task_id='query_report_data',
            query="""SELECT * FROM sickandbanedtimeoffdetails WHERE (sick_eligible='Yes' OR banked_eligible='Yes') AND (NULLIF(empid, '') IS NOT NULL OR empid!="") AND CAST(timeoff_balance AS DECIMAL(10,2)) > 0"""
        )

        get_timeoff_values = rail.PythonOperator(
            task_id="get_timeoff_values",
            python_callable=python_callable.get_timeoff_values,
        )

        if_final_export_has_data = rail.IfOperator(
            task_id="if_final_export_has_data",
            test=lambda: bool(len(rail.result('get_timeoff_values'))),
            yes_task="compose_item_payout_csv_file",
            no_task="finish_export_no_payout_data"
        )

        compose_item_payout_csv_file = rail.WriteCSVFileOperator(
            task_id='compose_item_payout_csv_file',
            source="{{ result('get_timeoff_values') | to_json }}",
            delimiter='|',
            header=["RECTY", "CLIID", "INTCA", "ORDNO", "IOPER", "INFTY", "SUBTY", "BEGDA",
                    "ENDDA", "OBJPS", "SPRPS", "SEQNR", "EXTRA", "LGART", "STDAZ", "BEGUZ", "ENDUZ", "BETRG", "WAERS",
                    "ANZHL", "ZEINH", "VTKEN", "BWGRL", "AUFKZ", "ENDOF", "UFLD1", "UFLD2", "UFLD3", "KEYPR", "TRFGR", "TRFST", "PRAKN", "PRAKZ",
                    "OTYPE", "PLANS", "VERSL", "EXBEL", "WTART", "TDLANGU", "TDSUBLA", "TDTYPE"],
            row=lambda item: request_payload.get_compose_item_payout_data_row(item,config),
        )

        no_of_records_size_including_header_footer = rail.PythonOperator(
            task_id="no_of_records_size_including_header_footer",
            python_callable=lambda:  int(len(rail.result(
                'get_timeoff_values'))) + 2
        )

        get_exported_custom_field = rail.RepliconServiceOperator(
            task_id="get_exported_custom_field",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=response_filter.get_custom_field_uris
        )

        get_sick_payout_udf_option_uris = rail.RepliconServiceOperator(
            task_id = 'get_sick_payout_udf_option_uris',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('get_exported_custom_field')["sick_payout_eligible"]
            },
            data_handler=response_filter.get_sick_custom_field_dropdown_uris
        )

        get_banked_payout_udf_option_uris = rail.RepliconServiceOperator(
            task_id = 'get_banked_payout_udf_option_uris',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('get_exported_custom_field')["banked_payout_eligible"]
            },
            data_handler=response_filter.get_banked_custom_field_dropdown_uris
        )

        process_child_udf_update = rail.TriggerDagRunForEachItemOperator(
            task_id='process_child_udf_update',
            retries=0,
            items="{{ result('get_timeoff_values') | to_json }}",
            trigger_dag_id=config.process_udf_update_child_dag,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'user_uri': item['useruri'],
                'sick_payout_eligible': rail.result('get_exported_custom_field')["sick_payout_eligible"],
                'banked_payout_eligible': rail.result('get_exported_custom_field')["banked_payout_eligible"],
                'set_sick_payout': rail.result('get_sick_payout_udf_option_uris')["no"],
                'set_banked_payout': rail.result('get_banked_payout_udf_option_uris')["no"],
                'update_spo_udf':item['update_spo_udf'],
                'update_bpo_udf':item['update_bpo_udf']
            }
        )

        create_document = rail.RenderTemplateOperator(
            task_id='create_document',
            target='artifact',
            template_file='schema/canada_payout_export_data.txt',
            dataset="{{ result('compose_item_payout_csv_file') }}",
        )

        pgp_encyrpt_item_file = rail.PGPEncryptionOperator(
            task_id="pgp_encyrpt_item_file",
            source="{{ result('create_document') }}",
            pgp_conn_id=config.pgp_conn_id,
            sign=True
        )

        upload_payout_item_file_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_payout_item_file_secondary_sftp",
            sftp_conn_id=config.secondary_sftp_conn_id,
            content="{{ result('create_document')}}",
            remote_filepath= config.secondary_output_filepath +
            "/{{ result('get_file_name')}}.SAP"
        )

        upload_encrypted_file_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_file_to_secondary_sftp",
            sftp_conn_id=config.secondary_encrypted_sftp_conn_id,
            content="{{ result('pgp_encyrpt_item_file') }}",
            remote_filepath=config.secondary_encrypted_output_filepath +
            "/{{ result('get_file_name')}}.SAP.pgp"
        )

        upload_encrypted_payout_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_encrypted_payout_file_to_sftp",
            content="{{ result('pgp_encyrpt_item_file') }}",
            remote_filepath=config.output_filepath +
            "/{{ result('get_file_name')}}.SAP.pgp"
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
            subject='{{ get_company_key() }} | ADP Payout Export Notification',
            params={
                'output_filepath': config.output_filepath,
            },
            html_content="templates/email/sftp_failure.html",
            files=[
                ("{{ result('get_file_name')}}.SAP.pgp", '{{result("pgp_encyrpt_item_file")}}')]
        )

        send_email_for_export_copmpletion = rail.EmailOperator(
            task_id='send_email_for_export_copmpletion',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{get_company_key()}} | Replicon payout export completed for '+config.export_location+' {{result("process_start_time").start_time}}',
            params={
                'output_filepath': config.output_filepath
            },
            html_content="/templates/email/export_success.html"
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_process_run >> process_start_time >> get_file_name >> get_report_details >> run_report_entry
        run_report_exit >> is_report_failed >> rail.Label("Yes") >> fail_report_generation >> log_to_sumo
        is_report_failed >> rail.Label("No") >> report_has_expected_columns >> rail.Label("Yes") >> report_has_data
        report_has_expected_columns >> rail.Label("No") >> fail_invalid_report_colums >> log_to_sumo
        report_has_data >> rail.Label("Yes") >> load_users_report_data >> timeoff_report_data_collection >> create_log >> query_report_data
        query_report_data >> get_timeoff_values >> if_final_export_has_data >> rail.Label("Yes") >> compose_item_payout_csv_file
        compose_item_payout_csv_file >> no_of_records_size_including_header_footer >> get_exported_custom_field >> \
        get_sick_payout_udf_option_uris >> get_banked_payout_udf_option_uris >> process_child_udf_update >> create_document >> \
        pgp_encyrpt_item_file >> upload_payout_item_file_secondary_sftp >> upload_encrypted_file_to_secondary_sftp >> \
        upload_encrypted_payout_file_to_sftp
        if_final_export_has_data >> rail.Label("No") >> finish_export_no_payout_data
        report_has_data >> rail.Label('No') >> finish_export_no_payout_data >> send_email_for_no_payout_data >> log_to_sumo
        upload_payout_item_file_secondary_sftp >> rail.Label("on_success") >> send_email_for_export_copmpletion
        upload_payout_item_file_secondary_sftp >> rail.Label("on_error") >> send_email_for_sftp_failure >> fail_sftp_upload_error >> log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
