from datetime import timedelta
import rail
from fujifilmdimatixg3.on_demand_punch_audit_report.utils import python_callable, response_filter


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'fujifilmdimatixg3_on_demand_punch_audit_report_master_{config.instance}',
        description=f'FUJIFILMDimatixG3_on_demand_punch_audit_report_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var),
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        check_mandatory_fields = rail.IfOperator(
            task_id='check_mandatory_fields',
            test=python_callable.check_all_mandatory_fields,
            yes_task='get_start_end_date',
            no_task='finish'
        )

        get_start_end_date = rail.PythonOperator(
            task_id="get_start_end_date",
            python_callable=response_filter.get_start_end_date
        )

        date_range_exceeded_threshold = rail.IfOperator(
            task_id='date_range_exceeded_threshold',
            test=lambda dag_run: python_callable.get_start_end_date_difference(
                dag_run) > 8,
            yes_task='send_date_range_exceeded_threshold_mail',
            no_task='check_invalid_date_range'
        )

        send_date_range_exceeded_threshold_mail = rail.EmailOperator(
            task_id='send_date_range_exceeded_threshold_mail',
            to='{{ dag_run.conf.webhook.email_address}}',
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Punch Audit Report - Date range exceeded threshold {{ dag_run.conf.webhook.start_date }}',
            html_content="templates/emails/date_range_exceeded_threshold_mail.html"
        )

        check_invalid_date_range = rail.IfOperator(
            task_id='check_invalid_date_range',
            test=lambda dag_run: python_callable.get_start_end_date_difference(
                dag_run) < 0,
            yes_task='send_invalid_date_range_mail',
            no_task='get_department_details'
        )

        send_invalid_date_range_mail = rail.EmailOperator(
            task_id='send_invalid_date_range_mail',
            to='{{ dag_run.conf.webhook.email_address}}',
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Punch Audit Report - invalid date range {{ dag_run.conf.webhook.start_date }}',
            html_content="templates/emails/invalid_date_range_mail.html"
        )

        get_department_details = rail.RepliconServiceOperator(
            task_id='get_department_details',
            endpoint='/services/DepartmentService1.svc/GetEnabledDepartments',
            data_handler=lambda response, dag_run: response_filter.get_department_uri_values(
                response, dag_run, config.department_details_var_name)
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
            trigger_dag_id=f"fujifilmdimatixg3_on_demand_punch_audit_report_child_{config.instance}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "lookup_table": rail.result('fujifilm_time_punches_lookup_table'),
                "user_name": item['User Name'],
                "timesheet_template": item['Timesheet Template'],
                "punch_entry_policy_name": item['Punch Entry Policy Name'],
                "user_uri": item['UserUri'],
                "start_date": python_callable.get_year_month_date(dag_run.conf['webhook']['start_date']),
                "end_date": python_callable.get_year_month_date(dag_run.conf['webhook']['end_date'])
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

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv')}}",
            output_file_name='fujifilm_time_punches_{{current_time("%m%d%Y%H%M%S")}}' + "_{{dag_run_ecid()}}" +
            '.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_csv_download_email = rail.EmailOperator(
            task_id='send_csv_download_email',
            to='{{ dag_run.conf.webhook.email_address}}',
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Punch Audit Report - Scheduled \
                {{ current_time_in_specified_tz("America/New_York", "%Y-%m-%d") }}T05:00:00.000000-04:00',
            html_content="templates/emails/punch_audit_report_mail.html"
        )

        send_no_data_mail = rail.EmailOperator(
            task_id='send_no_data_mail',
            to='{{ dag_run.conf.webhook.email_address}}',
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Punch Audit Report - Scheduled \
                {{ current_time_in_specified_tz("America/New_York", "%Y-%m-%d") }}T05:00:00.000000-04:00',
            html_content="templates/emails/no_data_mail.html"
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        check_mandatory_fields >> rail.Label(
            "Yes") >> get_start_end_date >> date_range_exceeded_threshold
        check_mandatory_fields >> rail.Label("No") >> finish

        date_range_exceeded_threshold >> rail.Label(
            "Yes") >> send_date_range_exceeded_threshold_mail >> finish
        date_range_exceeded_threshold >> rail.Label(
            "No") >> check_invalid_date_range

        check_invalid_date_range >> rail.Label(
            "Yes") >> send_invalid_date_range_mail >> finish
        check_invalid_date_range >> rail.Label("No") >> get_department_details
        get_department_details >> get_report_details >> generate_report_group_entry
        generate_report_group_exit >> parse_csv_data \
            >> fujifilm_time_punches_lookup_table >> process_each_report_records >> wait_process_time_records
        wait_process_time_records >> if_entry_present >> rail.Label(
            "Yes") >> create_csv >> generate_download_link >> send_csv_download_email >> finish
        if_entry_present >> rail.Label("No") >> send_no_data_mail >> finish
        return dag


rail.for_each_instance(create_dag)
