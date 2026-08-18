from datetime import datetime, timedelta
import rail
from winco.timesheet_recalc.utils import request_payload, response_payload
from winco.timesheet_recalc.utils.custom_methods import logging_details


def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'winco_spectrum_export_{config.instance}',
        description='winco Timesheet Recal Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1),
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            hmac_secret_var=config.hmac_secret),
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_data_available = rail.IfOperator(
            task_id='is_data_available',
            test=lambda dag_run: bool(
                dag_run.conf),
            yes_task="get_logging_details",
            no_task="delete_this_dagrun"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=logging_details,
            op_args=[config.time_zone]
        )

        log_file_name = rail.PythonOperator(
            task_id='log_file_name',
            python_callable=lambda: 'Replicon to Spectrum_' +
            (datetime.now()).strftime("%Y-%m-%dT%H-%M-%S")+'.csv'
        )

        get_department_details = rail.RepliconServiceOperator(
            task_id='get_department_details',
            endpoint='/services/DepartmentService1.svc/GetEnabledDepartments',
            response_filter=response_payload.get_department_filtered__data
        )

        department_is_present = rail.IfOperator(
            task_id="department_is_present",
            test="{{ result('get_department_details')  | is_truthy}}",
            yes_task='get_timesheet_details',
            no_task='fail_department_not_present'
        )

        get_timesheet_details = rail.RepliconServiceOperator(
            task_id='get_timesheet_details',
            endpoint='/services/TimesheetListService1.svc/GetData',
            data=request_payload.get_timesheet_data,
            response_filter=response_payload.get_filtered__data
        )

        process_time_records = rail.TriggerDagRunForEachItemOperator(
            task_id="process_time_records",
            items="{{ result('get_timesheet_details') | to_json}}",
            batch_size=50,
            trigger_dag_id=f"winco_timesheet_data_process_each_record_child_{config.instance}",
            conf=lambda item: {
                "timesheetdetails": item
            },
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        wait_process_project_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_project_records",
            dag_runs="{{result('process_time_records')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        fail_department_not_present = rail.FailOperator(
            task_id="fail_department_not_present",
            message="Department not found"
        )

        get_report_deatails2 = rail.RepliconServiceOperator(
            task_id='get_report_deatails2',
            endpoint='/services/ReportService1.svc/GetReportDetails2',
            data=request_payload.get_report_data
        )

        get_all_paycode_details = rail.RepliconServiceOperator(
            task_id='get_all_paycode_details',
            endpoint='/services/PayCodeService1.svc/GetAllPayCodes',
            response_filter=response_payload.get_all_paycode_filtered__data
        )

        get_users_details = rail.RepliconServiceOperator(
            task_id='get_users_details',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_users__data,
            response_filter=response_payload.get_all_users__data
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='get_reports_details',
            report_params=request_payload.get_report_filter
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("get_reports_details.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('get_reports_details.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('get_reports_details.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='send_no_data_mail',
        )

        send_no_data_mail = rail.EmailOperator(
            task_id='send_no_data_mail',
            to="{{dag_run.conf.emailid}}",
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} |   Replicon to Spectrum report - No Records to export -  {{ result("get_logging_details")["timerange_start_time"] }}',
            html_content="templates/no_data.html"
        )

        send_failed_mail = rail.EmailOperator(
            task_id='send_failed_mail',
            to="{{dag_run.conf.emailid}}",
            bcc=config.alert_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} |   Replicon to Spectrum report - Failed -  {{ result("get_logging_details")["timerange_start_time"] }}',
            html_content="templates/failed.html"
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('get_reports_details.get_report_result').reportGenerationResults[0].payload }}",
            headers=['Batch Code', 'Employee Code', 'Department', 'Pay Type', 'Hours', 'Job', 'Phase', 'Cost Type', 'Date',
                     'Message Line', 'Pay rate Level', 'Wage Code', 'Union Code', 'Worker\'s Comp',
                     'State Tax Code', 'County Tax code', 'Local Tax Code', 'Equipment Code',
                     'Equipment Hour', 'Quantity', 'Company Code', 'Pay Rate', 'Equipment Cost Category ', 'Crew Number',
                     'Cost Center', 'Work Order', 'Site Equipment', 'Site Component ', 'Contract', 'Equipment Work order ',
                     'Billing Code', 'Billing Rate'],
            delimiter=','

        )

        write_report_data_csv = rail.WriteCSVFileOperator(
            task_id='write_report_data_csv',
            source="{{ result('load_report_data') }}",
            header=None
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('write_report_data_csv')}}",
            output_file_name='{{result("log_file_name")}}',
            expires_in_seconds=7*24*60*60,
        )

        send_success_mail = rail.EmailOperator(
            task_id='send_success_mail',
            to="{{dag_run.conf.emailid}}",
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} |  Replicon to Spectrum report - {{ result("get_logging_details")["timerange_start_time"] }}',
            html_content="templates/success_mail.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        is_data_available >> rail.Label(
            "Yes") >> get_logging_details >> log_file_name >> get_department_details >> department_is_present >> rail.Label("Yes") >> get_timesheet_details\
            >> process_time_records >> wait_process_project_records\
            >> get_report_details >> get_report_deatails2\
            >> get_all_paycode_details >> get_users_details >> report_group_entry
        report_group_exit >> is_report_failed >> rail.Label(
            "Yes") >> fail_report_generation >> send_failed_mail >> log_to_sumo

        department_is_present >> rail.Label(
            "No") >> fail_department_not_present >> send_failed_mail >> log_to_sumo

        is_report_failed >> rail.Label(
            "No") >> report_has_data >> load_report_data >> write_report_data_csv >> generate_download_link >> send_success_mail\
                  >> log_to_sumo >> can_fail_dag >> fail_dagrun

        report_has_data >> rail.Label("No") >> send_no_data_mail >> log_to_sumo

        is_data_available >> rail.Label("No") >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
