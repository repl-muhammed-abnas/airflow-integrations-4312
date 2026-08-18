from datetime import timedelta
from pendulum import datetime
import rail
from wipro.auto_shift_assignment.new_users_assignment_v1.utils import request_payload


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"Wipro Auto Shift Assignment For New Users Master {config.instance}",
        company_key=config.company_key,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2022, 4, 1, tz=config.time_zone),
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master
    ) as dag:

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name=config.report_name,
        )

        genarate_user_report = rail.run_report2(
            group_id='load_user_report',
            report_params=lambda: request_payload.get_user_report_payload(
                config)
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("load_user_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('load_user_report.get_report_result').reportGenerationResults[0].error}}"
        )

        has_data = rail.IfOperator(
            task_id="has_data",
            test='{{ result("load_user_report.get_report_result", "has_data") }}',
            yes_task='users_report_payload_to_csv',
            no_task='send_no_user_mail'
        )

        users_report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="users_report_payload_to_csv",
            document='{{result("load_user_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        user_report_expected_report_columns = config.column_name
        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            # pylint: disable=consider-using-f-string
            test="{{ result('load_user_report.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % user_report_expected_report_columns,
            no_task='fail_invalid_user_report_colums',
            yes_task='users_report_data_collection',
        )

        fail_invalid_user_report_colums = rail.FailOperator(
            task_id="fail_invalid_user_report_colums",
            message="Base report column does not match"
        )

        users_report_data_collection = rail.CreateCollectionOperator(
            task_id="users_report_data_collection",
            name='getalluserdata',
            source='{{result("users_report_payload_to_csv")}}',
            columns={
                'User Name': 'user_name',
                'Employee ID': 'employee_id',
                'User Uri': 'user_uri',
                'User Status': 'user_status',
                'User Start Date': 'user_start_date',
                'Country': 'country',
                'Schedule': 'schedule',
                "Onsite Direct Recruit": "onsite_direct_recruit",
                "Onsite Start Date": "onsite_start_date",
                "Legal Entity Code": "legal_entity_code",
                "Acquired Company": "acquired_company",
                "FJEmpIdentifier": "fj_identifier"
            }
        )

        query_enabled_users_data = rail.QueryCollectionOperator(
            task_id="query_enabled_users_data",
            query=f"""SELECT * FROM getalluserdata WHERE user_status = 'Enabled' AND schedule='Shift Schedule' AND (country IN
            {str(config.country)} or 
            (country = 'Spain' AND legal_entity_code = 'W001' AND acquired_company!='INETUM') or
            (country = 'Romania' AND NULLIF(legal_entity_code, '') IS NOT NULL AND 
            legal_entity_code IN ('W139','W204','W163')
            AND NULLIF(fj_identifier, '') is not null and fj_identifier IN ('01', '02','16','03')))"""
        )

        has_any_users_data = rail.IfOperator(
            task_id='has_any_users_data',
            test='{{ result("query_enabled_users_data", "length") > 0 }}',
            yes_task="process_each_records",
            no_task="send_no_user_mail"
        )

        # batch_size is 1 for this integration as we are processing one user at a time.
        # Since it is a replica of the monthly_assignment integration,
        # the same design was maintained

        process_each_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_records',
            items=lambda: rail.result('query_enabled_users_data'),
            trigger_dag_id=config.child_dag_auto,
            batch_size=config.batch_size,
            conf=lambda item: {
                "item": item
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_process_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_records",
            dag_runs="{{result('process_each_records')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_shift_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_shift_logs',
            dag_runs="{{ result('process_each_records') }}",
            dagrun_task_id='create_shift_log',
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=request_payload.do_format_logs,
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=['UserName', 'EmployeeID', 'Country',
                    'Schedule', 'Status', 'Ecid'],
            row=['{{ item.username }}', '{{ item.employeeid}}', '{{item.country}}',
                 '{{item.schedule}}', '{{ item.status }}', '{{ item.jobid }}']
        )

        generate_presigned_url = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_presigned_url",
            output_file_name='{{get_company_key()}}' + "Shift_Assignment_Logs" +
            '{{current_time_in_specified_tz("America/New_York")}}.csv',
            expires_in_seconds=7*24*60*60,
            artifact_name='{{result("render_logs_csv")}}'
        )

        any_records_failed = rail.IfOperator(
            task_id='any_records_failed',
            test="{{ result('format_logs', 'error_record_count') > 0 }}",
            yes_task='send_completion_error_mail',
            no_task='send_completion_mail'
        )

        send_completion_mail = rail.EmailOperator(
            task_id='send_completion_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Shift Assignment for New Users is completed successfully at {{ current_time_in_specified_tz("America/New_York") }}',
            html_content="templates/email/import_complete.html"
        )

        send_completion_error_mail = rail.EmailOperator(
            task_id='send_completion_error_mail',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='{{ get_company_key() }} | Shift Assignment for New Users is completed with error at {{ current_time_in_specified_tz("America/New_York") }}',
            html_content="templates/email/import_with_error.html"
        )

        send_no_user_mail = rail.EmailOperator(
            task_id='send_no_user_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Shift Assignment for New Users - No data to be Processed at {{ current_time_in_specified_tz("America/New_York") }}',
            html_content="templates/email/no_data.html"
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

    get_user_report_details >> genarate_user_report >> is_report_failed >> rail.Label(
        "Yes") >> fail_report_generation
    is_report_failed >> rail.Label("No") >> has_data >> rail.Label("Yes") >> users_report_payload_to_csv >>\
        report_has_expected_columns >> rail.Label("Yes")\
        >> users_report_data_collection >> query_enabled_users_data >> has_any_users_data >> rail.Label("Yes") >>\
        process_each_records >> wait_process_records >> gather_shift_logs >> format_logs >> render_logs_csv >> generate_presigned_url\
        >> any_records_failed >> rail.Label("Yes") >> send_completion_error_mail >> log_to_sumo >> can_fail_dag >> fail_dagrun

    any_records_failed >> rail.Label(
        "No") >> send_completion_mail >> log_to_sumo

    report_has_expected_columns >> rail.Label(
        "No") >> fail_invalid_user_report_colums

    has_any_users_data >> rail.Label("No") >> send_no_user_mail

    has_data >> rail.Label("No") >> send_no_user_mail
    return dag


rail.for_each_instance(create_main_dag)
