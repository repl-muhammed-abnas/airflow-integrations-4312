from datetime import  datetime as dt
import rail
from itvdaytime.time_off_export.utils import request_payload
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= f"itvdaytime_time_off_report_export_master_{config.instance}",
        description= f"iTV DayTime time off report export master {config.instance}",
        company_key= config.company_key,
        schedule_interval=config.schedule_interval,
        replicon_conn_id = config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs = config.max_active_runs
    ) as dag :


        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=lambda:  str(dt.now().strftime("%Y-%m-%dT%H-%M-%S"))
        )
        get_file_name = rail.PythonOperator(
            task_id="get_file_name",
            python_callable=lambda: "Time Off Booking Template.csv"
        )

        # pylint: disable=line-too-long
        expected_report_columns = "Employee ID,User Name,Time Off Type,Absence Entry ID ,Booking Start Date,Booking Start Date/Time,Booking End Date,Booking End Date/Time,Time Off Hrs,Approval Status"

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
            query= "SELECT * FROM report_data_collection"
        )

        final_data_to_csv = rail.WriteCSVFileOperator(
            task_id = "final_data_to_csv",
            source= "{{ result('final_data') }}",
            # pylint: disable=line-too-long
            header=["Employee ID","User Name","Time Off Type","Absence Entry ID ","Booking Start Date","Booking Start Date/Time","Booking End Date","Booking End Date/Time","Time Off Hrs","Approval Status"],
            row=request_payload.get_compose_item_time_off_data_row
        )

        upload_export_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id ="upload_export_data_to_sftp",
            content= '{{result("final_data_to_csv")}}',
            remote_filepath = config.output_file_path + '{{result("get_file_name")}}'
        )

        send_export_complete_email = rail.EmailOperator(
            task_id = 'send_export_complete_email',
            to = config.tenant_email,
            bcc = config.internal_logs_email,
            subject = "{{ get_company_key() }} | Daily Time off export report -{{result('process_start_time')}}",
            html_content = "export_complete_mail.html",
            params={
                'output_file_path': config.output_file_path,
                'log_file_path': config.log_file_path
                }
        )

        process_start_time >> get_file_name >> get_specific_report_details
        get_specific_report_details >> load_report >> has_data
        has_data >> rail.Label("No") >> finish_export
        has_data >> rail.Label("Yes") >> report_has_expected_columns
        report_has_expected_columns >> rail.Label("Yes") >> report_payload_to_csv >> report_data_collection >> final_data
        final_data >>  final_data_to_csv >> upload_export_data_to_sftp >> send_export_complete_email
        report_has_expected_columns >> rail.Label("No") >> fail_invalid_report_colums

    return dag

rail.for_each_instance(create_main_dag)
