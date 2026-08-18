from datetime import  datetime as dt
from pendulum import datetime
import pytz
import rail
from pwcglobal.distance_data_extract.daily_export import request_payload
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= f"{config.instance}_distance_data_extract_daily_report_for_Netherlands",
        description= f"Daily Distance Extract for Netherlands {config.instance}",
        company_key= config.company_key,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2022, 4, 1, tz=config.europe_timezone),
        replicon_conn_id = config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs = config.max_active_runs
    ) as dag :


        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=lambda:  str(dt.now(pytz.timezone("Europe/Paris")).strftime("%Y%m%d%H%M%S"))
        )
        get_file_name = rail.PythonOperator(
            task_id="get_file_name",
            python_callable=lambda: "Daily_Distance_Extract_"  +
            str(dt.now(pytz.timezone("Europe/Paris")).strftime("%Y%m%d%H%M%S")) + "_NLD.csv"
        )
        logging_job_start_time = rail.WriteLogOperator(
            task_id = "logging_job_start_time",
            message= "{{result('process_start_time')}} - Process started",
            properties= {
                "log" : "{{result('process_start_time')}} - Process started"
            }
        )

        logging_the_country = rail.WriteLogOperator(
            task_id = "logging_the_country",
            message= "{{result('process_start_time')}}- INFO admin Exporting data for Territory : Netherlands",
            properties= {
                "log" : "{{result('process_start_time')}}- INFO admin Exporting data for Territory : Netherlands"
            }
        )

        expected_report_columns=config.column_order

        get_specific_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_specific_report_details',
            report_name=config.report_name,
        )

        load_report = rail.run_report(
            group_id='load_report',
            report_params=request_payload.get_run_report_payload
        )

        has_data = rail.IfOperator(
            task_id  = "has_data",
            test = '{{"No Data" not in result("load_report.get_report_result").reportGenerationResults[0].payload}}',
            yes_task= 'report_has_expected_columns',
            no_task= 'finish_export'
        )

        finish_export = rail.EmptyOperator(
           task_id= 'finish_export'
        )

        report_has_expected_columns = rail.IfOperator(
            task_id = "report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            # pylint: disable=line-too-long
            test="{{ result('load_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_report_columns,
            no_task='fail_invalid_report_colums',
            yes_task='report_payload_to_csv',
        )

        report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id = "report_payload_to_csv",
            document= '{{result("load_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        report_data_collection = rail.CreateCollectionOperator(
            task_id = "report_data_collection",
            source= '{{result("report_payload_to_csv")}}'
        )

        fail_invalid_report_colums = rail.FailOperator(
            task_id = "fail_invalid_report_colums",
            message="Base report column does not match"
        )

        final_data = rail.QueryCollectionOperator(
            task_id = "final_data",
            query= config.query
        )

        final_data_to_csv = rail.WriteCSVFileOperator2(
            task_id = "final_data_to_csv",
            source= "{{ result('final_data') }}",
            # pylint: disable=line-too-long
            header=["TransactionDate","TimeEntryID","PartyID","ResourceGrade","LegalEntityPartyID","WorkDayId","TimesheetStartDate","TimesheetEndDate","Mileage","ChargeCode","WorkItemType"],
            row=['{{item.TransactionDate}}',
                '{{item.TimeEntryID}}',
                '{{item.PartyID}}',
                '{{item.ResourceGrade}}',
                '{{item.LegalEntityPartyID}}',
                '{{item.WorkDayId}}',
                '{{item.TimesheetStartDate}}',
                '{{item.TimesheetEndDate}}',
                '{{item.Mileage}}',
                '{{item.ChargeCode}}',
                '{{item.WorkItemType}}']
        )

        logging_record_count = rail.WriteLogOperator(
            task_id = "logging_record_count",
            message = "{{result('process_start_time')}} INFO admin No of records exported = {{result('final_data','length')}}",
            properties= {
                "log" : "{{result('process_start_time')}} INFO admin No of records exported = {{result('final_data', 'length')}}"
            }
        )

        logging_the_file_creation = rail.WriteLogOperator(
            task_id = "logging_the_file_creation",
            message= "{{result('process_start_time')}} INFO admin Export File_"+ '{{result("get_file_name")}}' +"  created",
            properties={
                "log" : "{{result('process_start_time')}} INFO admin Export File_"+ '{{result("get_file_name")}}' +"  created"
            }
        )

        upload_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id ="upload_export_data_to_sftp",
            content= '{{result("final_data_to_csv")}}',
            remote_filepath = config.output_file_path + '{{result("get_file_name")}}'
        )

        logging_the_file_upload = rail.WriteLogOperator(
            task_id = "logging_the_file_upload",
            message= "{{result('process_start_time')}} INFO admin Export File_{{result('get_file_name')}} uploaded",
            properties={
                "log": "{{result('process_start_time')}} INFO admin Export File_{{result('get_file_name')}} uploaded"
            }
        )

        final_logs_to_csv = rail.WriteCSVFileOperator2(
            task_id = "final_logs_to_csv",
            source= lambda: rail.result('logging_job_start_time'),
            header = ['Log file'],
            row = [
                    '{{item.properties.log}}'
            ]
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id ="upload_log_to_sftp",
            content= "{{result('final_logs_to_csv')}}",
            remote_filepath = config.log_file_path +"Log_" + "{{result('get_file_name')}}"
        )

        is_upload_file_to_different_path = rail.IfOperator(
            task_id ="is_upload_file_to_different_path",
            test=config.is_upload_file_to_different_path_required,
            yes_task="upload_file_to_different_path",
            no_task="send_export_complete_email"
        )

        upload_file_to_different_path = rail.SFTPUploadFileOperator(
            task_id ="upload_file_to_different_path",
            content= '{{result("final_data_to_csv")}}',
            remote_filepath = config.alternate_file_path + '{{result("get_file_name")}}'
        )

        send_export_complete_email = rail.EmailOperator(
            task_id = 'send_export_complete_email',
            to = config.tenant_email,
            bcc = config.internal_logs_email,
            subject = "{{ get_company_key() }} | Daily Distance Extract for Netherlands -{{result('process_start_time')}}",
            html_content = "export_complete_mail.html",
            params={
                'output_file_path': config.output_file_path,
                'log_file_path': config.log_file_path
                }
        )

        process_start_time >> logging_job_start_time >> get_file_name >>logging_the_country >> get_specific_report_details
        get_specific_report_details >> load_report >> has_data
        has_data >> rail.Label("No") >> finish_export
        has_data >> rail.Label("Yes") >> report_has_expected_columns
        report_has_expected_columns >> rail.Label("Yes") >> report_payload_to_csv >> report_data_collection >> final_data
        final_data >> [logging_record_count, final_data_to_csv] >> logging_the_file_creation
        logging_the_file_creation >> [upload_export_data_to_sftp, logging_the_file_upload] >> final_logs_to_csv
        final_logs_to_csv >> upload_log_to_sftp >> is_upload_file_to_different_path

        is_upload_file_to_different_path >> rail.Label(
            "Yes") >> upload_file_to_different_path >> send_export_complete_email

        is_upload_file_to_different_path >> rail.Label(
            "No") >> send_export_complete_email

        report_has_expected_columns >> rail.Label("No") >> fail_invalid_report_colums

    return dag

rail.for_each_instance(create_main_dag)
