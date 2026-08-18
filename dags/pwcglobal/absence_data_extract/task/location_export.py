import datetime as dt
import rail
import pytz
from pwcglobal.absence_data_extract import custom_method
from pwcglobal.absence_data_extract import request_payload


def location_export_task(config):
    with rail.TaskGroup(group_id='location_export_task', prefix_group_id=False) as location_export:

        is_allowed = rail.IfOperator(
            task_id="is_allowed",
            test=config.allowed == "Yes",
            yes_task="get_logging_details",
            no_task="fail_not_allowed"
        )
        fail_not_allowed = rail.FailOperator(
            task_id="fail_not_allowed",
            message=f"Extract is not allowed for location= {config.location}, yet a run triggered"
        )
        def logging_details():
            time_stamp = dt.datetime.now(pytz.timezone("Etc/UTC"))
            return{
                "start_time_pst" : str(time_stamp),
                "email_time" : str(time_stamp.strftime("%Y-%m-%dT%H:%M:%S.%f%z")),
                "file_name_time": str(time_stamp.strftime("%Y%m%d%H%M%S")),
                "file_name": "Absence Extract_" + str(time_stamp.strftime("%Y%m%d%H%M%S")) + "_" + config.location_code + ".csv",
                "log_file_path" : config.log_filepath,
                "output_file_path" : config.output_filepath
            }

        get_logging_details = rail.PythonOperator(
            task_id = "get_logging_details",
            python_callable= logging_details
        )

        logging_job_start_time = rail.WriteLogOperator(
            task_id="logging_job_start_time",
            message=f"{custom_method.get_europe_paris_time_now()} - Process started",
            properties={
                "log": f"{custom_method.get_europe_paris_time_now()} - Process started"
            }
        )

        get_enabled_locations = rail.RepliconServiceOperator(
            task_id="get_enabled_locations",
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'displayText', config.location, 'uri')
        )

        get_all_reports = rail.RepliconServiceOperator(
            task_id="get_all_reports",
            endpoint="/services/ReportService1.svc/GetAllReports",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'displayText', config.report_name, 'uri')
        )

        logging_the_country = rail.WriteLogOperator(
            task_id="logging_the_country",
            message=custom_method.get_europe_paris_time_now(
            ) + "- INFO admin Exporting data for Territory : " + config.location,
            properties={
                "log": custom_method.get_europe_paris_time_now() + "- INFO admin Exporting data for Territory : " + config.location
            }
        )

        get_specific_report_details = rail.RepliconServiceOperator(
            task_id="get_specific_report_details",
            endpoint="/services/ReportService1.svc/GetReportDetails2",
            data={
                'reportUri': "{{result('get_all_reports')}}"
            },
            response_filter=lambda response: response.json()['d']
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id="report_generation",
            report_params=lambda: request_payload.get_run_report_payload(
                config.time_zone),
            replicon_conn_id=config.replicon_conn_id
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("report_generation.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('report_generation.get_report_result').reportGenerationResults[0].error}}"
        )

        has_data = rail.IfOperator(
            task_id="has_data",
            test='{{"No Data" in  result("report_generation.get_report_result").reportGenerationResults[0].payload}}',
            no_task='report_has_expected_columns',
            yes_task='send_blank_mail',
        )

        blank_export = '''<p><strong><em>This is a automated mail, please don't reply</em></strong></p>
            <p>Hi ,</p>
            <p>The Absence data extract from Replicon to Workday is completed on {{ result('get_logging_details').email_time }}.
            There were no records in the report to be exported.</p>
            <p>For any queries, Please contact our support team at https://support.deltek.com</p>
            <p>Thanks, <br />Deltek Inc.</p>
        '''

        send_blank_mail = rail.EmailOperator(
            task_id="send_blank_mail",
            to=config.tenant_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Absence data extract from Replicon to Workday is skipped {{ result("get_logging_details").email_time }}',
            html_content=blank_export
        )

        expected_report_columns = "UserFirstName,UserLastName,TimeEntryId,LoginName,iwfr\\InternalPerson\\PartyId,iwfr\\PwCLegalEntity\\PartyId,EmployeeId,\
TransactionDate,ChargeCode,WorkItemType,HoursQuantity,Comments,WorkLocation,WorkCategory,ApprovalStatus,SubmittedOn,TimesheetStartDate,TimesheetEndDate"

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('report_generation.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_report_columns,
            no_task='fail_invalid_report_columns',
            yes_task='report_payload_to_csv',
        )

        report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="report_payload_to_csv",
            document='{{result("report_generation.get_report_result").reportGenerationResults[0].payload}}'
        )

        report_data_collection = rail.CreateCollectionOperator(
            task_id="report_data_collection",
            source='{{result("report_payload_to_csv")}}'
        )

        fail_invalid_report_columns = rail.FailOperator(
            task_id="fail_invalid_report_columns",
            message="Base report column does not match"
        )

        final_data = rail.QueryCollectionOperator(
            task_id="final_data",
            query="SELECT * FROM report_data_collection WHERE loginname IS NOT NULL OR loginname != ''"
        )

        final_data_to_csv = rail.WriteCSVFileOperator(
            task_id="final_data_to_csv",
            source="{{ result('final_data') }}",
            header=["UserFirstName", "UserLastName", "TimeEntryId", "LoginName", "iwfr\\InternalPerson\\PartyId", "iwfr\\PwCLegalEntity\\PartyId",
                    "EmployeeId","TransactionDate", "ChargeCode", "WorkItemType", "HoursQuantity", "Comments", "WorkLocation", "WorkCategory", "ApprovalStatus",
                    "SubmittedOn", "TimesheetStartDate", "TimesheetEndDate"],
            row=['{{item.UserFirstName}}',
                 '{{item.UserLastName}}',
                 '{{item.TimeEntryId}}',
                 '{{item.LoginName}}',
                 '{{item.iwfr_InternalPerson_PartyId}}',
                 '{{item.iwfr_PwCLegalEntity_PartyId}}',
                 '{{item.EmployeeId}}',
                 '{{item.TransactionDate}}',
                 '{{item.ChargeCode}}',
                 '{{item.WorkItemType}}',
                 '{{item.HoursQuantity}}',
                 '{{item.Comments}}',
                 '{{item.WorkLocation}}',
                 '{{item.WorkCategory}}',
                 '{{item.ApprovalStatus}}',
                 '{{item.SubmittedOn}}',
                 '{{item.TimesheetStartDate}}',
                 '{{item.TimesheetEndDate}}']
        )

        logging_record_count = rail.WriteLogOperator(
            task_id="logging_record_count",
            message=custom_method.get_europe_paris_time_now(
            ) + " INFO admin No of records exported =" + '{{result("final_data","length") - 1}}',
            properties={
                "log": custom_method.get_europe_paris_time_now() + " INFO admin No of records exported =" + '{{result("final_data","length") - 1}}'
            }
        )

        logging_the_file_creation = rail.WriteLogOperator(
            task_id="logging_the_file_creation",
            message=custom_method.get_europe_paris_time_now() + " INFO admin Export File_" + "{{result('get_logging_details').file_name}}" + "  created",
            properties={
                "log": custom_method.get_europe_paris_time_now() + " INFO admin Export File_" + "{{result('get_logging_details').file_name}}" + "  created"
            }
        )

        upload_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_export_data_to_sftp",
            content='{{result("final_data_to_csv")}}',
            remote_filepath=config.output_filepath + "{{result('get_logging_details').file_name}}"
        )

        logging_the_file_upload = rail.WriteLogOperator(
            task_id="logging_the_file_upload",
            message=custom_method.get_europe_paris_time_now() +"INFO admin Export File_" + "{{result('get_logging_details').file_name}}" + "uploaded",
            properties={
                "log": custom_method.get_europe_paris_time_now() +"INFO admin Export File_" + "{{result('get_logging_details').file_name}}" + "uploaded"
            }
        )

        final_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="final_logs_to_csv",
            source=lambda: rail.result('logging_job_start_time'),
            header=None,
            row=[
                '{{item.properties.log}}'
            ]
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_to_sftp",
            content="{{result('final_logs_to_csv')}}",
            remote_filepath=config.log_filepath + "Logs_" + "{{result('get_logging_details').file_name}}"
        )

        can_upload_to_secondary_sftp = rail.IfOperator(
            task_id="can_upload_to_secondary_sftp",
            test=config.instance != "production",
            yes_task="upload_log_to_secondary_sftp",
            no_task="send_export_complete_email"
        )

        upload_log_to_secondary_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_log_to_secondary_sftp",
            content="{{result('final_logs_to_csv')}}",
            remote_filepath=config.alternate_log_path + "Logs_" + "{{result('get_logging_details').file_name}}"
        )

        complete_email = """<p><strong><em>This is a automated mail, please don't reply</em></strong></p>
            <p>Hi ,</p>
            <p>The Absence data extract from Replicon to Workday is completed on  {{result('get_logging_details').email_time}}</p>
            <p> File name: {{result('get_logging_details').file_name}}</p>
            <p>File path: {{result('get_logging_details').output_file_path}}</p>
            <p>Log file name: Logs_{{result('get_logging_details').file_name}}</p>
            <p>Log file path: {{result('get_logging_details').log_file_path}}</p>
            <p>For any queries, Please contact our support team at https://support.deltek.com</p>
            <p>Thanks, <br />Deltek Inc.</p>
        """

        send_export_complete_email = rail.EmailOperator(
            task_id='send_export_complete_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject="{{ get_company_key() }} | Absence data extract from Replicon to Workday is completed on - {{result('get_logging_details').email_time}} for the location - "+config.location,
            html_content=complete_email,
        )
        is_allowed >> rail.Label("No") >> fail_not_allowed
        is_allowed >> rail.Label("Yes") >> get_logging_details >> logging_job_start_time >> [get_all_reports, get_enabled_locations, logging_the_country]\
            >> get_specific_report_details >> run_report_group_entry
        run_report_group_exit >> is_report_failed >> rail.Label(
            "No") >> has_data
        is_report_failed >> rail.Label("Yes") >> fail_report_generation
        has_data >> rail.Label("Yes") >> send_blank_mail

        has_data >> rail.Label("No") >> report_has_expected_columns
        report_has_expected_columns >> rail.Label(
            "Yes") >> report_payload_to_csv >> report_data_collection >> final_data
        final_data >> [logging_record_count,
                       final_data_to_csv] >> logging_the_file_creation
        logging_the_file_creation >> [
            upload_export_data_to_sftp, logging_the_file_upload] >> final_logs_to_csv
        final_logs_to_csv >> upload_log_to_sftp >> can_upload_to_secondary_sftp >> rail.Label("Yes") >> \
            upload_log_to_secondary_sftp >> send_export_complete_email
        can_upload_to_secondary_sftp >> rail.Label(
            "No") >> send_export_complete_email

        report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_columns

    return location_export
