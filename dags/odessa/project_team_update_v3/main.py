from datetime import timedelta
from pendulum import datetime as dt
import rail

from odessa.project_team_update_v3.utils import custom_method


def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"odessa_project_team_update_master_v3_{config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2024, 1, 1, tz=config.pacific_timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs,
        catchup=False,
        tags=["odessa", "project_team_update"],
        default_args={"sftp_conn_id": config.sftp_conn_id},
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id="new_file_sensor",
            path=config.sftp_input_path,
            soft_fail_timeout=timedelta(minutes=config.sftp_sensor_timeout_minutes),
        )

        is_csv = rail.IfOperator(
            task_id="is_csv",
            test="{{ result('new_file_sensor') | file_ext | lower == 'csv' }}",
            yes_task="download_input_file",
            no_task="send_invalid_format_email",
        )

        send_invalid_format_email = rail.EmailOperator(
            task_id="send_invalid_format_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Project team update - Invalid file format - {{ current_time_in_specified_tz() }}",
            html_content="templates/invalid_format_mail.html",
        )

        download_input_file = rail.SFTPDownloadFileOperator(
            task_id="download_input_file",
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_file_found = rail.IfOperator(
            task_id="was_file_found",
            trigger_rule="all_done",
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task="archive_input_file",
            no_task="delete_this_dagrun",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_this_dagrun",
        )

        archive_input_file = rail.SFTPMoveFileOperator(
            task_id="archive_input_file",
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.sftp_archive_path
            + '/archive_{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}',
        )

        load_csv_file = rail.LoadCSVFileOperator(
            task_id="load_csv_file",
            document="{{ result('download_input_file') }}",
            delimiter=",",
        )

        create_input_collection = rail.CreateCollectionOperator(
            task_id="create_input_collection",
            source="{{ result('load_csv_file') }}",
            name="input_rows",
            columns=config.input_csv_columns,
        )

        has_rows = rail.IfOperator(
            task_id="has_rows",
            test='{{ result("create_input_collection", "length") > 0 }}',
            yes_task="get_userdata_report_details",
            no_task="send_no_data_email",
        )

        send_no_data_email = rail.EmailOperator(
            task_id="send_no_data_email",
            trigger_rule="none_failed_min_one_success",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Project team update - No data in file - {{ current_time_in_specified_tz() }}",
            html_content="templates/no_data_mail.html",
        )

        get_userdata_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_userdata_report_details",
            report_name=config.userdata_report_name,
        )

        generate_userdata_report = rail.RepliconServiceOperator(
            task_id="generate_userdata_report",
            endpoint="/services/ReportService1.svc/GenerateReport",
            data={
                "reportUri": '{{ result("get_userdata_report_details").uri }}',
                "filterValues": [],
                "outputFormatUri": config.report_output_format_uri,
            },
        )

        check_report_error = rail.PythonOperator(
            task_id="check_report_error",
            python_callable=custom_method.raise_if_report_error,
        )

        report_to_csv = rail.LoadCSVFileOperator(
            task_id="report_to_csv",
            document='{{ result("generate_userdata_report").payload }}',
        )

        create_user_lookup_collection = rail.CreateCollectionOperator(
            task_id="create_user_lookup_collection",
            source="{{ result('report_to_csv') }}",
            name="user_lookup",
            columns=config.userdata_report_columns,
        )

        get_company_billing_rates = rail.RepliconServiceOperator(
            task_id="get_company_billing_rates",
            endpoint="/services/billing/BillingRateService1.svc/GetCompanyBillingRates",
            extra_options={"force_interactive_services": True},
        )

        join_rows_with_users = rail.QueryCollectionOperator(
            task_id="join_rows_with_users",
            name="valid_rows",
            query="""SELECT input_rows.*,
                            user_lookup.useruri AS useruri,
                            user_lookup.customerrole AS customerrole,
                            user_lookup.location AS location
                     FROM input_rows
                     LEFT JOIN user_lookup
                       ON LOWER(TRIM(NULLIF(input_rows.loginname, ''))) = LOWER(TRIM(NULLIF(user_lookup.loginname, '')))""",
        )

        build_row_payloads = rail.PythonOperator(
            task_id="build_row_payloads",
            python_callable=custom_method.build_row_payloads,
        )

        trigger_process_row = rail.TriggerDagRunForEachItemOperator(
            task_id="trigger_process_row",
            retries=0,
            items=lambda: rail.result("build_row_payloads"),
            trigger_dag_id=config.process_row_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "employeeid": "{{ item.employeeid }}",
                "loginname": "{{ item.loginname }}",
                "projectname": "{{ item.projectname }}",
                "action": "{{ item.action }}",
                "role": "{{ item.role }}",
                "useruri": "{{ item.useruri }}",
                "customerrole": "{{ item.customerrole }}",
                "location": "{{ item.location }}",
                "billingratename": "{{ item.billingratename }}",
                "billingrateuri": "{{ item.billingrateuri }}",
                "billingratefound": "{{ item.billingratefound }}",
                "filename": "{{ result('new_file_sensor') | file_name }}",
                "assign_billing_rate_child_dag_id": config.assign_billing_rate_child_dag_id,
            },
        )

        wait_for_rows = rail.WaitForDagRunsSensor(
            task_id="wait_for_rows",
            dag_runs="{{ result('trigger_process_row') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_row_results = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_row_results",
            dag_runs="{{ result('trigger_process_row') }}",
            dagrun_task_id="row_result",
        )

        write_result_csv = rail.WriteCSVFileOperator(
            task_id="write_result_csv",
            source="{{ result('gather_row_results') | to_json }}",
            header=["filename", "loginname", "projectname", "action", "status"],
            row=[
                "{{ dag_run_ecid() }}-{{ result('new_file_sensor') | file_name }}",
                "{{ item.loginname }}",
                "{{ item.projectname }}",
                "{{ item.action }}",
                "{{ item.status }}",
            ],
        )

        upload_result_log = rail.SFTPUploadFileOperator(
            task_id="upload_result_log",
            content="{{ result('write_result_csv') }}",
            remote_filepath=config.sftp_logs_path
            + "/projectteamupdatelog_{{ dag_run_ecid() | replace(':', '-') }}.csv",
        )

        has_errors = rail.IfOperator(
            task_id="has_errors",
            test=lambda: any(
                (r.get("status") or "").lower().startswith("error")
                for r in rail.result("gather_row_results")
            ),
            yes_task="send_error_email",
            no_task="send_completion_email",
        )

        send_error_email = rail.EmailOperator(
            task_id="send_error_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Project team update processed with error - {{ current_time_in_specified_tz() }}",
            html_content="templates/error_mail.html",
            files=[
                ("Project_team_update_log_{{ dag_run_ecid() | replace(':', '-') }}.csv",
                 "{{ result('write_result_csv') }}")
            ],
        )

        send_completion_email = rail.EmailOperator(
            task_id="send_completion_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Project team update has been processed - {{ current_time_in_specified_tz() }}",
            html_content="templates/completion_mail.html",
            files=[
                ("Project_team_update_log_{{ dag_run_ecid() | replace(':', '-') }}.csv",
                 "{{ result('write_result_csv') }}")
            ],
        )

        new_file_sensor >> is_csv

        is_csv >> rail.Label("Yes") >> download_input_file >> was_file_found
        is_csv >> rail.Label("No") >> send_invalid_format_email

        was_file_found >> rail.Label("Yes") >> archive_input_file
        was_file_found >> rail.Label("No") >> delete_this_dagrun

        download_input_file >> load_csv_file >> create_input_collection >> has_rows
        has_rows >> rail.Label("No") >> send_no_data_email
        has_rows >> rail.Label("Yes") >> get_userdata_report_details >> generate_userdata_report \
            >> check_report_error >> report_to_csv >> create_user_lookup_collection \
            >> get_company_billing_rates >> join_rows_with_users >> build_row_payloads

        build_row_payloads >> trigger_process_row >> wait_for_rows \
            >> gather_row_results >> write_result_csv >> upload_result_log >> has_errors

        has_errors >> rail.Label("Yes") >> send_error_email
        has_errors >> rail.Label("No") >> send_completion_email

    return dag


rail.for_each_instance(create_master_dag)
