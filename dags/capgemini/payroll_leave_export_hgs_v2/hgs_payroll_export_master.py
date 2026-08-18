
from datetime import timedelta
from pendulum import datetime
from capgemini.payroll_leave_export_hgs_v2.tasks.timeoff_report import run_timeoff_report
from capgemini.payroll_leave_export_hgs_v2.utils import request_payload, custom_methods
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'capgemini_payroll_leave_export_hgs_master_{config.instance}_v2',
        description=f'HGS Payroll Export - Capgemini {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 6, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='logging_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='logging_details',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config]
        )

        declare_bookings_set = rail.SetVariableOperator(
            task_id='declare_bookings_set',
            name='bookings_list',
            value=[]
        )

        get_approved_timeoffs_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_approved_timeoffs_report_details',
            report_name=config.approved_timeoffs_report
        )

        run_approved_timeoffs_report_entry, run_approved_timeoffs_report_exit = rail.run_report(
            group_id='run_approved_timeoffs_report',
            report_params=lambda: request_payload.get_approved_timeoffs_report_batch_payload(config.time_zone)
        )

        is_approved_timeoffs_report_failed = rail.IfOperator(
            task_id='is_approved_timeoffs_report_failed',
            test='{{result("run_approved_timeoffs_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task='fail_approved_timeoffs_report_generation',
            no_task='approved_timeoffs_report_has_data'
        )

        fail_approved_timeoffs_report_generation = rail.FailOperator(
            task_id='fail_approved_timeoffs_report_generation',
            message="{{result('run_approved_timeoffs_report.get_report_result').reportGenerationResults[0].error}}"
        )

        approved_timeoffs_report_has_data = rail.IfOperator(
            task_id='approved_timeoffs_report_has_data',
            test="{{result('run_approved_timeoffs_report.get_report_result','has_data')}}",
            yes_task='is_approved_timeoffs_report_has_expected_columns',
            no_task='get_deleted_timeoffs_report_details'
        )

        is_approved_timeoffs_report_has_expected_columns = rail.IfOperator(
            task_id='is_approved_timeoffs_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{result('run_approved_timeoffs_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_approved_timeoffs_report_columns,
            yes_task='load_approved_timeoffs_report_data',
            no_task='fail_approved_timeoffs_has_no_expected_columns',
        )

        fail_approved_timeoffs_has_no_expected_columns = rail.FailOperator(
            task_id='fail_approved_timeoffs_has_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_approved_timeoffs_report_data = rail.LoadCSVFileOperator(
            task_id='load_approved_timeoffs_report_data',
            document="{{ result('run_approved_timeoffs_report.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_approved_timeoffs_collection = rail.CreateCollectionOperator(
            task_id='create_approved_timeoffs_collection',
            source='{{ result("load_approved_timeoffs_report_data") }}',
            columns={
                "Leave Request ID": "leave_request_id",
                "Local Employee Number": "local_employee_number",
                "Employee ID": "employee_id",
                "Time Off Type": "timeoff_type",
                "Time Off Type Description": "timeoff_type_description",
                "Booking Start Date": "booking_start_date",
                "Booking End Date": "booking_end_date",
                "Approval Status": "approval_status",
                "Cost Center (Current) (Full Path)": "cost_center_full_path"
            },
            name="approved_timeoff_bookings_data"
        )

        write_approved_bookings_csv = rail.WriteCSVFileOperator(
            task_id='write_approved_bookings_csv',
            source='{{ result("create_approved_timeoffs_collection") }}',
            header=["ENTITY", "EMP_ID", "GGID", "LWP_TYPE", "LWP_START_DATE", "LWP_END_DATE", "LWP_CODE",
                    "MODIFIED DATED", "REMARKS", "COMPANYNAME"],
            row=lambda item: custom_methods.get_bookings_csv(item, "L", "Approved")
        )

        append_approved_bookings = rail.SetVariableOperator(
            task_id='append_approved_bookings',
            append=True,
            name='bookings_list',
            value=lambda: rail.result("write_approved_bookings_csv")
        )

        get_deleted_timeoffs_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_deleted_timeoffs_report_details',
            report_name=config.deleted_timeoffs_report
        )

        run_deleted_timeoffs_report_entry, run_deleted_timeoffs_report_exit = rail.run_report(
            group_id='run_deleted_timeoffs_report',
            report_params=lambda: request_payload.get_deleted_timeoffs_report_batch_payload(config.time_zone)
        )

        is_deleted_timeoffs_report_failed = rail.IfOperator(
            task_id='is_deleted_timeoffs_report_failed',
            test='{{result("run_deleted_timeoffs_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task='fail_deleted_timeoffs_report_generation',
            no_task='deleted_timeoffs_report_has_data'
        )

        fail_deleted_timeoffs_report_generation = rail.FailOperator(
            task_id='fail_deleted_timeoffs_report_generation',
            message="{{result('run_deleted_timeoffs_report.get_report_result').reportGenerationResults[0].error}}"
        )

        deleted_timeoffs_report_has_data = rail.IfOperator(
            task_id='deleted_timeoffs_report_has_data',
            test="{{result('run_deleted_timeoffs_report.get_report_result','has_data')}}",
            yes_task='is_deleted_timeoffs_report_has_expected_columns',
            no_task='load_deleted_timeoffs_report_data'
        )

        is_deleted_timeoffs_report_has_expected_columns = rail.IfOperator(
            task_id='is_deleted_timeoffs_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{result('run_deleted_timeoffs_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_deleted_timeoffs_report_columns,
            yes_task='load_deleted_timeoffs_report_data',
            no_task='fail_deleted_timeoffs_has_no_expected_columns',
        )

        fail_deleted_timeoffs_has_no_expected_columns = rail.FailOperator(
            task_id='fail_deleted_timeoffs_has_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_deleted_timeoffs_report_data = rail.LoadCSVFileOperator(
            task_id='load_deleted_timeoffs_report_data',
            document="{{ result('run_deleted_timeoffs_report.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_deleted_timeoffs_collection = rail.CreateCollectionOperator(
            task_id='create_deleted_timeoffs_collection',
            source='{{ result("load_deleted_timeoffs_report_data") }}',
            columns={
                "Leave Request ID": "leave_request_id",
                "Local Employee Number": "local_employee_number",
                "Employee ID": "employee_id",
                "Current Time Off Type": "timeoff_type",
                "Current Start Date": "booking_start_date",
                "Current End Date": "booking_end_date",
                "Action": "action",
                "Cost Center (Current) (Full Path)": "cost_center_full_path"
            },
            name="deleted_timeoff_bookings_data"
        )

        approvedlast30days_timeoffs_report_run_start = rail.EmptyOperator(
            task_id='approvedlast30days_timeoffs_report_run_start'
        )

        run_approvedlast30days_timeoffs_report, finish_approvedlast30days_report_run = run_timeoff_report(config, "approvedlast30days",
            config.approvedlast30days_timeoffs_report, config.expected_approvedlast30days_timeoffs_report_columns)

        is_deleted_timeoffs_collection_has_data = rail.IfOperator(
            task_id='is_deleted_timeoffs_collection_has_data',
            test="{{ result('create_deleted_timeoffs_collection','length') > 0 }}",
            yes_task='query_list_all_added_bookings_last7days',
            no_task='is_approved_timeoffs_exists'
        )

        query_list_all_added_bookings_last7days=rail.QueryCollectionOperator(
            task_id='query_list_all_added_bookings_last7days',
            query="""SELECT * FROM deleted_timeoff_bookings_data
                WHERE deleted_timeoff_bookings_data.timeoff_type
                IN {{ result('logging_details').required_timeoffs }}
                AND deleted_timeoff_bookings_data.action='Added'
                AND deleted_timeoff_bookings_data.leave_request_id
                NOT IN
                (SELECT DISTINCT approvedlast30days_timeoffs_bookings_data.leave_request_id
                FROM approvedlast30days_timeoffs_bookings_data)""",
            name='bookings_added_last7days'
        )

        query_list_all_deleted_bookings=rail.QueryCollectionOperator(
            task_id='query_list_all_deleted_bookings',
            query="""SELECT * FROM deleted_timeoff_bookings_data
                WHERE deleted_timeoff_bookings_data.timeoff_type
                IN {{ result('logging_details').required_timeoffs }}
                AND deleted_timeoff_bookings_data.action='Deleted'
                AND deleted_timeoff_bookings_data.leave_request_id
                NOT IN (SELECT DISTINCT bookings_added_last7days.leave_request_id FROM bookings_added_last7days)""",
            name='bookings_deleted'
        )

        is_deleted_bookings_present = rail.IfOperator(
            task_id='is_deleted_bookings_present',
            test='{{ result("query_list_all_deleted_bookings", "length") > 0 }}',
            yes_task='write_deleted_bookings_csv',
            no_task='is_approved_timeoffs_exists'
        )

        write_deleted_bookings_csv = rail.WriteCSVFileOperator(
            task_id='write_deleted_bookings_csv',
            source='{{ result("query_list_all_deleted_bookings") }}',
            header=["ENTITY", "EMP_ID", "GGID", "LWP_TYPE", "LWP_START_DATE", "LWP_END_DATE", "LWP_CODE",
                    "MODIFIED DATED", "REMARKS", "COMPANYNAME"],
            row=lambda item: custom_methods.get_bookings_csv(item, "R", "Cancelled")
        )

        append_deleted_bookings = rail.SetVariableOperator(
            task_id='append_deleted_bookings',
            append=True,
            name='bookings_list',
            value=lambda: rail.result("write_deleted_bookings_csv")
        )

        is_approved_timeoffs_exists = rail.IfOperator(
            task_id='is_approved_timeoffs_exists',
            test="{{result('run_approved_timeoffs_report.get_report_result','has_data')}}",
            yes_task='get_modified_timeoffs_report_details',
            no_task='get_bookings_data_artifacts'
        )

        get_modified_timeoffs_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_modified_timeoffs_report_details',
            report_name=config.modified_timeoffs_report
        )

        run_modified_timeoffs_report_entry, run_modified_timeoffs_report_exit = rail.run_report(
            group_id='run_modified_timeoffs_report',
            report_params=lambda: request_payload.get_modified_timeoffs_report_batch_payload(config.time_zone)
        )

        is_modified_timeoffs_report_failed = rail.IfOperator(
            task_id='is_modified_timeoffs_report_failed',
            test='{{result("run_modified_timeoffs_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task='fail_modified_timeoffs_report_generation',
            no_task='modified_timeoffs_report_has_data'
        )

        fail_modified_timeoffs_report_generation = rail.FailOperator(
            task_id='fail_modified_timeoffs_report_generation',
            message="{{result('run_modified_timeoffs_report.get_report_result').reportGenerationResults[0].error}}"
        )

        modified_timeoffs_report_has_data = rail.IfOperator(
            task_id='modified_timeoffs_report_has_data',
            test="{{result('run_modified_timeoffs_report.get_report_result','has_data')}}",
            yes_task='is_modified_timeoffs_report_has_expected_columns',
            no_task='get_bookings_data_artifacts'
        )

        is_modified_timeoffs_report_has_expected_columns = rail.IfOperator(
            task_id='is_modified_timeoffs_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{result('run_modified_timeoffs_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_modified_timeoffs_report_columns,
            yes_task='load_modified_timeoffs_report_data',
            no_task='fail_modified_timeoffs_has_no_expected_columns',
        )

        fail_modified_timeoffs_has_no_expected_columns = rail.FailOperator(
            task_id='fail_modified_timeoffs_has_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_modified_timeoffs_report_data = rail.LoadCSVFileOperator(
            task_id='load_modified_timeoffs_report_data',
            document="{{ result('run_modified_timeoffs_report.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_modified_timeoffs_collection = rail.CreateCollectionOperator(
            task_id='create_modified_timeoffs_collection',
            source='{{ result("load_modified_timeoffs_report_data") }}',
            columns={
                "Leave Request ID": "leave_request_id",
                "Local Employee Number": "local_employee_number",
                "Employee ID": "employee_id",
                "Current Time Off Type": "current_timeoff_type",
                "Current Start Date": "current_start_date",
                "Current End Date": "current_end_date",
                "Action": "action",
                "Cost Center (Current) (Full Path)": "cost_center_full_path",
                "Field": "field",
                "Original Value": "original_value",
                "New Value": "new_value",
                "Modified On": "modified_on",
                "modifiedon": "modified_on_utc"
            },
            name="modified_timeoff_bookings_data"
        )

        query_list_all_modified_bookings_to_be_considered=rail.QueryCollectionOperator(
            task_id='query_list_all_modified_bookings_to_be_considered',
            query="""SELECT * FROM modified_timeoff_bookings_data
                WHERE modified_timeoff_bookings_data.current_timeoff_type
                IN {{ result('logging_details').required_timeoffs }}
                AND NULLIF(modified_timeoff_bookings_data.original_value,'') IS NOT NULL
                AND modified_timeoff_bookings_data.leave_request_id
                IN (SELECT DISTINCT approved_timeoff_bookings_data.leave_request_id
                FROM approved_timeoff_bookings_data)
                ORDER BY strftime('%b %d, %Y %I:%M:%S %p', modified_timeoff_bookings_data.modified_on_utc)
                DESC""",
            name='modified_bookings_to_consider'
        )

        is_list_all_modified_bookings_to_be_considered_records_present = rail.IfOperator(
            task_id='is_list_all_modified_bookings_to_be_considered_records_present',
            test='{{ result("query_list_all_modified_bookings_to_be_considered", "length") > 0 }}',
            yes_task='added_timeoffs_report_run_start',
            no_task='get_bookings_data_artifacts'
        )

        added_timeoffs_report_run_start = rail.EmptyOperator(
            task_id='added_timeoffs_report_run_start'
        )

        run_added_timeoffs_report, finish_added_report_run = run_timeoff_report(config, "added",
            config.added_timeoffs_report, config.expected_added_timeoffs_report_columns)

        added_and_not_approved_bookings_last30days = rail.QueryCollectionOperator(
            task_id='added_and_not_approved_bookings_last30days',
            query="""SELECT * FROM added_timeoffs_bookings_data WHERE added_timeoffs_bookings_data.leave_request_id
                NOT IN (SELECT DISTINCT approvedlast30days_timeoffs_bookings_data.leave_request_id FROM approvedlast30days_timeoffs_bookings_data)""",
            name='added_and_not_approved_bookings_last30days'
        )

        query_list_distinct_bookings=rail.QueryCollectionOperator(
            task_id='query_list_distinct_bookings',
            query="""SELECT DISTINCT modified_bookings_to_consider.leave_request_id FROM modified_bookings_to_consider
                WHERE modified_bookings_to_consider.leave_request_id
                NOT IN (SELECT DISTINCT added_and_not_approved_bookings_last30days.leave_request_id
                FROM added_and_not_approved_bookings_last30days)""",
        )

        is_distinct_bookings_present = rail.IfOperator(
            task_id='is_distinct_bookings_present',
            test='{{ result("query_list_distinct_bookings", "length") > 0 }}',
            yes_task='validate_start_and_end_dates_collection',
            no_task='get_bookings_data_artifacts'
        )

        validate_start_and_end_dates_collection = rail.CreateCollectionOperator(
            task_id='validate_start_and_end_dates_collection',
            source=custom_methods.get_start_end_validation_data,
            name="validated_modified_bookings_data"
        )

        query_modified_bookings_for_start_date_and_end_date_changes=rail.QueryCollectionOperator(
            task_id='query_modified_bookings_for_start_date_and_end_date_changes',
            query="""SELECT * FROM validated_modified_bookings_data
                WHERE validated_modified_bookings_data.start_date_update='Yes'
                AND validated_modified_bookings_data.end_date_update='Yes'
                AND validated_modified_bookings_data.booking_start_date!=validated_modified_bookings_data.original_start_date
                AND validated_modified_bookings_data.booking_end_date!=validated_modified_bookings_data.original_end_date""",
            name='modified_bookings_for_start_date_and_end_date_changes'
        )

        is_start_date_and_end_date_changes_present = rail.IfOperator(
            task_id='is_start_date_and_end_date_changes_present',
            test='{{ result("query_modified_bookings_for_start_date_and_end_date_changes", "length") > 0 }}',
            yes_task='write_modified_bookings_csv_1',
            no_task='query_modified_bookings_for_start_date_and_no_change_to_end_date'
        )

        write_modified_bookings_csv_1 = rail.WriteCSVFileOperator(
            task_id='write_modified_bookings_csv_1',
            source='{{ result("query_modified_bookings_for_start_date_and_end_date_changes") }}',
            header=["ENTITY", "EMP_ID", "GGID", "LWP_TYPE", "LWP_START_DATE", "LWP_END_DATE", "LWP_CODE",
                    "MODIFIED DATED", "REMARKS", "COMPANYNAME"],
            row=lambda item: custom_methods.get_modified_bookings_csv(item, item["original_start_date"], item["original_end_date"])
        )

        append_modified_bookings_1 = rail.SetVariableOperator(
            task_id='append_modified_bookings_1',
            append=True,
            name='bookings_list',
            value=lambda: rail.result("write_modified_bookings_csv_1")
        )

        query_modified_bookings_for_start_date_and_no_change_to_end_date=rail.QueryCollectionOperator(
            task_id='query_modified_bookings_for_start_date_and_no_change_to_end_date',
            query="""SELECT * FROM validated_modified_bookings_data
                WHERE validated_modified_bookings_data.start_date_update='Yes'
                AND validated_modified_bookings_data.end_date_update='No'
                AND validated_modified_bookings_data.booking_start_date!=validated_modified_bookings_data.original_start_date""",
            name='modified_bookings_for_start_date_and_no_end_date_changes'
        )

        is_start_date_changes_present = rail.IfOperator(
            task_id='is_start_date_changes_present',
            test='{{ result("query_modified_bookings_for_start_date_and_no_change_to_end_date", "length") > 0 }}',
            yes_task='write_modified_bookings_csv_2',
            no_task='query_modified_bookings_for_no_change_in_start_date_and_change_to_end_date'
        )

        write_modified_bookings_csv_2 = rail.WriteCSVFileOperator(
            task_id='write_modified_bookings_csv_2',
            source='{{ result("query_modified_bookings_for_start_date_and_no_change_to_end_date") }}',
            header=["ENTITY", "EMP_ID", "GGID", "LWP_TYPE", "LWP_START_DATE", "LWP_END_DATE", "LWP_CODE",
                    "MODIFIED DATED", "REMARKS", "COMPANYNAME"],
            row=lambda item: custom_methods.get_modified_bookings_csv(item, item["original_start_date"], item["booking_end_date"])
        )

        append_modified_bookings_2 = rail.SetVariableOperator(
            task_id='append_modified_bookings_2',
            append=True,
            name='bookings_list',
            value=lambda: rail.result("write_modified_bookings_csv_2")
        )

        query_modified_bookings_for_no_change_in_start_date_and_change_to_end_date=rail.QueryCollectionOperator(
            task_id='query_modified_bookings_for_no_change_in_start_date_and_change_to_end_date',
            query="""SELECT * FROM validated_modified_bookings_data
                WHERE validated_modified_bookings_data.start_date_update='No'
                AND validated_modified_bookings_data.end_date_update='Yes'
                AND validated_modified_bookings_data.booking_end_date!=validated_modified_bookings_data.original_end_date""",
            name='modified_bookings_for_end_date_and_no_start_date_changes'
        )

        is_end_date_changes_present = rail.IfOperator(
            task_id='is_end_date_changes_present',
            test='{{ result("query_modified_bookings_for_no_change_in_start_date_and_change_to_end_date", "length") > 0 }}',
            yes_task='write_modified_bookings_csv_3',
            no_task='get_bookings_data_artifacts'
        )

        write_modified_bookings_csv_3 = rail.WriteCSVFileOperator(
            task_id='write_modified_bookings_csv_3',
            source='{{ result("query_modified_bookings_for_no_change_in_start_date_and_change_to_end_date") }}',
            header=["ENTITY", "EMP_ID", "GGID", "LWP_TYPE", "LWP_START_DATE", "LWP_END_DATE", "LWP_CODE",
                    "MODIFIED DATED", "REMARKS", "COMPANYNAME"],
            row=lambda item: custom_methods.get_modified_bookings_csv(item, item["booking_start_date"], item["original_end_date"])
        )

        append_modified_bookings_3 = rail.SetVariableOperator(
            task_id='append_modified_bookings_3',
            append=True,
            name='bookings_list',
            value=lambda: rail.result("write_modified_bookings_csv_3")
        )

        get_bookings_data_artifacts = rail.GetVariableOperator(
            task_id='get_bookings_data_artifacts',
            name='bookings_list'
        )

        is_data_artifacts_present = rail.IfOperator(
            task_id='is_data_artifacts_present',
            test='{{ result("get_bookings_data_artifacts").value | is_truthy }}',
            yes_task='compose_booking_data',
            no_task='write_payroll_blankdata_to_csv'
        )

        compose_booking_data = rail.CreateCollectionOperator(
            task_id='compose_booking_data',
            source=custom_methods.load_logs
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("compose_booking_data", "length") > 0 }}',
            yes_task='write_payroll_data_to_csv',
            no_task='write_payroll_blankdata_to_csv'
        )

        write_payroll_data_to_csv = rail.WriteCSVFileOperator(
            task_id='write_payroll_data_to_csv',
            source='{{ result("compose_booking_data") }}',
            header=["ENTITY", "EMP_ID", "GGID", "LWP_TYPE", "LWP_START_DATE", "LWP_END_DATE", "LWP_CODE",
                    "MODIFIED DATED", "REMARKS", "COMPANYNAME"],
            row=custom_methods.get_payroll_data_csv_rows,
            delimiter='|',
            execution_timeout=timedelta(
                minutes=config.execution_timeout_mins_write_csv)
        )

        write_payroll_blankdata_to_csv = rail.WriteCSVFileOperator(
            task_id='write_payroll_blankdata_to_csv',
            source=[],
            header=["ENTITY", "EMP_ID", "GGID", "LWP_TYPE", "LWP_START_DATE", "LWP_END_DATE", "LWP_CODE",
                    "MODIFIED DATED", "REMARKS", "COMPANYNAME"],
            row=[],
            delimiter='|',
            execution_timeout=timedelta(
                minutes=config.execution_timeout_mins_write_csv)
        )

        encrypt_payroll_blankexport_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_payroll_blankexport_data_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_payroll_blankdata_to_csv') }}"
        )

        upload_payroll_blankexport_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_payroll_blankexport_to_sftp',
            content='{{ result("encrypt_payroll_blankexport_data_csv") }}',
            remote_filepath=config.input_filepath +
            '/{{ result("logging_details").payroll_export_filename }}.csv.pgp'
        )

        send_empty_export_email = rail.EmailOperator(
            task_id="send_empty_export_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll export for HGS - No records to export - {{ result("logging_details").process_start_time }}',
            html_content="/templates/emails/email_empty_export.html",
            params={
                'location': config.location
            }
        )

        upload_payroll_export_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_payroll_export_to_s3',
            source='{{ result("write_payroll_data_to_csv") }}',
            key_name=config.s3_upload_filepath +
            '/{{ result("logging_details").payroll_export_filename }}.csv',
            bucket_name=lambda: Variable.get(config.bucket_name),
            aws_conn_id=config.aws_conn_id
        )

        encrypt_payroll_export_data_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_payroll_export_data_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_payroll_data_to_csv') }}"
        )

        upload_payroll_export_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_payroll_export_to_sftp',
            content='{{ result("encrypt_payroll_export_data_csv") }}',
            remote_filepath=config.input_filepath +
            '/{{ result("logging_details").payroll_export_filename }}.csv.pgp'
        )

        send_valid_export_complete_email = rail.EmailOperator(
            task_id="send_valid_export_complete_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon payroll export for HGS is completed - {{ result("logging_details").process_start_time }}',
            html_content="/templates/emails/email_valid_export_complete.html",
            params={
                'upload_file_path': config.input_filepath,
                'location': config.location
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.dagrun_log_sumo_conn_id,
            extra_info=lambda: {
                'locations': config.location,
                'timeoff_types_considered': rail.result("logging_details")["required_timeoffs_mapper"],
                'exportrowcount': len(rail.load_all_records(rail.result("compose_booking_data"))) if rail.result("compose_booking_data") else 0,
                'filename': rail.result("logging_details")["payroll_export_filename"] + '.csv.pgp'
                    if rail.result("compose_booking_data") and len(rail.load_all_records(rail.result("compose_booking_data"))) > 0 else ""
            }
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_payroll_export'
        )

        fail_payroll_export = rail.FailOperator(
            task_id='fail_payroll_export',
            message='{{ get_error_message() }}'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label('No') >> logging_details

        logging_details >> declare_bookings_set >> get_approved_timeoffs_report_details >> run_approved_timeoffs_report_entry
        run_approved_timeoffs_report_exit >> is_approved_timeoffs_report_failed

        is_approved_timeoffs_report_failed >> rail.Label("Yes") >> fail_approved_timeoffs_report_generation >> dagrun_log_to_sumo
        is_approved_timeoffs_report_failed >> rail.Label("No") >> approved_timeoffs_report_has_data

        approved_timeoffs_report_has_data >> rail.Label("Yes") >> is_approved_timeoffs_report_has_expected_columns
        approved_timeoffs_report_has_data >> rail.Label("No") >> get_deleted_timeoffs_report_details

        is_approved_timeoffs_report_has_expected_columns >> rail.Label("Yes") >> load_approved_timeoffs_report_data \
            >> create_approved_timeoffs_collection >> write_approved_bookings_csv >> append_approved_bookings \
                >> get_deleted_timeoffs_report_details
        is_approved_timeoffs_report_has_expected_columns >> rail.Label("No") >> fail_approved_timeoffs_has_no_expected_columns \
            >> dagrun_log_to_sumo

        get_deleted_timeoffs_report_details >> run_deleted_timeoffs_report_entry
        run_deleted_timeoffs_report_exit >> is_deleted_timeoffs_report_failed

        is_deleted_timeoffs_report_failed >> rail.Label("Yes") >> fail_deleted_timeoffs_report_generation >> dagrun_log_to_sumo
        is_deleted_timeoffs_report_failed >> rail.Label("No") >> deleted_timeoffs_report_has_data

        deleted_timeoffs_report_has_data >> rail.Label("Yes") >> is_deleted_timeoffs_report_has_expected_columns
        deleted_timeoffs_report_has_data >> rail.Label("No") >> load_deleted_timeoffs_report_data

        is_deleted_timeoffs_report_has_expected_columns >> rail.Label("Yes") >> load_deleted_timeoffs_report_data \
            >> create_deleted_timeoffs_collection
        is_deleted_timeoffs_report_has_expected_columns >> rail.Label("No") >> fail_deleted_timeoffs_has_no_expected_columns \
            >> dagrun_log_to_sumo

        create_deleted_timeoffs_collection >> approvedlast30days_timeoffs_report_run_start >> run_approvedlast30days_timeoffs_report
        finish_approvedlast30days_report_run >> is_deleted_timeoffs_collection_has_data
        is_deleted_timeoffs_collection_has_data >> rail.Label("Yes") >> query_list_all_added_bookings_last7days \
            >> query_list_all_deleted_bookings >> is_deleted_bookings_present
        is_deleted_timeoffs_collection_has_data >> rail.Label("No") >> is_approved_timeoffs_exists

        is_deleted_bookings_present >> rail.Label("Yes") >> write_deleted_bookings_csv >> append_deleted_bookings >> is_approved_timeoffs_exists
        is_deleted_bookings_present >> rail.Label("No") >> is_approved_timeoffs_exists

        is_approved_timeoffs_exists >> rail.Label("No") >> get_bookings_data_artifacts
        is_approved_timeoffs_exists >> rail.Label("Yes") >> get_modified_timeoffs_report_details >> run_modified_timeoffs_report_entry
        run_modified_timeoffs_report_exit >> is_modified_timeoffs_report_failed

        is_modified_timeoffs_report_failed >> rail.Label("Yes") >> fail_modified_timeoffs_report_generation >> dagrun_log_to_sumo
        is_modified_timeoffs_report_failed >> rail.Label("No") >> modified_timeoffs_report_has_data

        modified_timeoffs_report_has_data >> rail.Label("Yes") >> is_modified_timeoffs_report_has_expected_columns
        modified_timeoffs_report_has_data >> rail.Label("No") >> get_bookings_data_artifacts

        is_modified_timeoffs_report_has_expected_columns >> rail.Label("Yes") >> load_modified_timeoffs_report_data \
            >> create_modified_timeoffs_collection >> query_list_all_modified_bookings_to_be_considered \
                >> is_list_all_modified_bookings_to_be_considered_records_present

        is_list_all_modified_bookings_to_be_considered_records_present >> rail.Label("Yes") >> added_timeoffs_report_run_start >> run_added_timeoffs_report
        finish_added_report_run >> added_and_not_approved_bookings_last30days >> query_list_distinct_bookings >> is_distinct_bookings_present

        is_distinct_bookings_present >> rail.Label("Yes") >> validate_start_and_end_dates_collection >> query_modified_bookings_for_start_date_and_end_date_changes
        is_distinct_bookings_present >> rail.Label("No") >> get_bookings_data_artifacts

        query_modified_bookings_for_start_date_and_end_date_changes >> is_start_date_and_end_date_changes_present

        is_start_date_and_end_date_changes_present >> rail.Label("Yes") >> write_modified_bookings_csv_1 >> append_modified_bookings_1 \
            >> query_modified_bookings_for_start_date_and_no_change_to_end_date

        is_start_date_and_end_date_changes_present >> rail.Label("No") >> query_modified_bookings_for_start_date_and_no_change_to_end_date

        query_modified_bookings_for_start_date_and_no_change_to_end_date >> is_start_date_changes_present

        is_start_date_changes_present >> rail.Label("Yes") >> write_modified_bookings_csv_2 >> append_modified_bookings_2 \
            >> query_modified_bookings_for_no_change_in_start_date_and_change_to_end_date

        is_start_date_changes_present >> rail.Label("No") >> query_modified_bookings_for_no_change_in_start_date_and_change_to_end_date

        query_modified_bookings_for_no_change_in_start_date_and_change_to_end_date >> is_end_date_changes_present

        is_end_date_changes_present >> rail.Label("Yes") >> write_modified_bookings_csv_3 >> append_modified_bookings_3

        is_end_date_changes_present >> rail.Label("No") >> get_bookings_data_artifacts

        append_modified_bookings_3 >> get_bookings_data_artifacts

        is_list_all_modified_bookings_to_be_considered_records_present >> rail.Label("No") >> get_bookings_data_artifacts

        get_bookings_data_artifacts >> is_data_artifacts_present

        is_data_artifacts_present >> rail.Label("Yes") >> compose_booking_data >> has_data
        is_data_artifacts_present >> rail.Label("No") >> write_payroll_blankdata_to_csv >> \
        encrypt_payroll_blankexport_data_csv >> upload_payroll_blankexport_to_sftp >> send_empty_export_email

        has_data >> rail.Label("Yes") >> write_payroll_data_to_csv >> upload_payroll_export_to_s3 >> encrypt_payroll_export_data_csv \
            >> upload_payroll_export_to_sftp >> send_valid_export_complete_email >> dagrun_log_to_sumo >> should_fail_dag

        has_data >> rail.Label("No") >> write_payroll_blankdata_to_csv >> encrypt_payroll_blankexport_data_csv >> \
        upload_payroll_blankexport_to_sftp >> send_empty_export_email >> dagrun_log_to_sumo

        is_modified_timeoffs_report_has_expected_columns >> rail.Label("No") >> fail_modified_timeoffs_has_no_expected_columns \
            >> dagrun_log_to_sumo

        should_fail_dag >> rail.Label("Yes") >> fail_payroll_export

    return dag

rail.for_each_instance(create_dag)
