from datetime import timedelta
from pendulum import datetime
import rail
from fujifilmdimatixg3.punch_audit_report.utils import python_callable, response_filter

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'fujifilmdimatixg3_punch_audit_report_master_{config.instance}',
        description=f'FUJIFILMDimatixG3_punch_audit_report_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 8, 1, tz=config.schedule_time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        get_department_details = rail.RepliconServiceOperator(
            task_id='get_department_details',
            endpoint='/services/DepartmentService1.svc/GetEnabledDepartments',
            data_handler=lambda response: response_filter.get_department_uri_values(
                response, config.department_details_var_name, config.default_department_list_var)
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name
        )

        generate_report_group_entry, generate_report_group_exit = rail.run_report(
            group_id='generate_report_group',
            report_params=python_callable.get_report_params
        )

        parse_csv_data = rail.LoadCSVFileOperator(
            task_id='parse_csv_data',
            document="{{ result('generate_report_group.get_report_result').reportGenerationResults[0].payload }}",
        )

        fujifilm_time_punches_lookup_table = rail.CreateLogOperator(
            task_id='fujifilm_time_punches_lookup_table'
        )

        process_each_report_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_report_records',
            items="{{ result('parse_csv_data')}}",
            trigger_dag_id=f"fujifilmdimatixg3_punch_audit_report_child_{config.instance}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, index: {
                "lookup_table"                  : rail.result('fujifilm_time_punches_lookup_table'),
                "user_name"                     : item['User Name'],
                "timesheet_template"            : item['Timesheet Template'],
                "punch_entry_policy_name"       : item['Punch Entry Policy Name'],
                "user_uri"                      : item['UserUri'],
                "index"                         : index,
                "start_date"                    : python_callable.get_year_month_date('start'),
                "end_date"                      : python_callable.get_year_month_date('end')
            }
        )

        wait_process_time_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_time_records",
            dag_runs="{{result('process_each_report_records')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        if_entry_present = rail.IfOperator(
            task_id='if_entry_present',
            test="{{ result('fujifilm_time_punches_lookup_table') | load_all_records() | length > 0 }}",
            yes_task='create_csv',
            no_task='send_no_data_mail'
        )

        create_csv = rail.WriteCSVFileOperator(
            task_id='create_csv',
            source=lambda: rail.result(
                'fujifilm_time_punches_lookup_table'),
            header=[
                'Employee Name',
                'Employee ID',
                'Department Name',
                'Punch Date',
                'Original Punch',
                'Action',
                'Punch Type',
                'Modified Punch',
                'Modified By'],
            row=lambda item: [
                item['properties']['user_name'],
                item['properties']['employee_id'],
                item['properties']['department_name'],
                item['properties']['punch_date'],
                item['properties']['original_punch'],
                item['properties']['action'],
                item['properties']['punch_type'],
                item['properties']['modified_punch'],
                item['properties']['modified_by'],
            ]
        )
        
        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content = "{{ result('create_csv')}}",
            remote_filepath=config.punch_audit_report_filepath +
            '/fujifilm_time_punches_{{current_time("%m%d%Y%H%M%S")}}_{{dag_run_ecid()}}.csv',
        )

        send_completion_email = rail.EmailOperator(
            task_id='send_completion_email',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Punch Audit Report - Scheduled {{ current_time_in_specified_tz("America/New_York", "%Y-%m-%d") }}T05:00:00.000000-04:00',
            html_content="templates/emails/punch_audit_report_mail.html",
            params={
                'punch_audit_start_date_range': f"{python_callable.get_year_month_date('start')['month']}/{python_callable.get_year_month_date('start')['day']}/{python_callable.get_year_month_date('start')['year']}",
                'punch_audit_end_date_range': f"{python_callable.get_year_month_date('end')['month']}/{python_callable.get_year_month_date('end')['day']}/{python_callable.get_year_month_date('end')['year']}",
                'filepath': config.punch_audit_report_filepath
            }
        )

        send_no_data_mail = rail.EmailOperator(
            task_id='send_no_data_mail',
            to=config.tenant_email,
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Punch Audit Report - Scheduled {{ current_time_in_specified_tz("America/New_York", "%Y-%m-%d") }}T05:00:00.000000-04:00',
            html_content="templates/emails/no_data_mail.html",
            params={
                'punch_audit_start_date_range': f"{python_callable.get_year_month_date('start')['month']}/{python_callable.get_year_month_date('start')['day']}/{python_callable.get_year_month_date('start')['year']}",
                'punch_audit_end_date_range': f"{python_callable.get_year_month_date('end')['month']}/{python_callable.get_year_month_date('end')['day']}/{python_callable.get_year_month_date('end')['year']}",
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_department_details >> get_report_details >> generate_report_group_entry
        generate_report_group_exit >> parse_csv_data \
        >> fujifilm_time_punches_lookup_table >> process_each_report_records >> wait_process_time_records
        wait_process_time_records >> if_entry_present >> rail.Label("Yes") >> create_csv >> upload_log_to_sftp >> send_completion_email >> finish
        if_entry_present >> rail.Label("No") >> send_no_data_mail >> finish
        return dag

rail.for_each_instance(create_dag)
