from datetime import timedelta
from pendulum import datetime
import rail
from macquariegroup.recovery_reconciliation.tasks.run_base_report import run_base_report
from macquariegroup.recovery_reconciliation.tasks.send_logs import get_send_logs
from macquariegroup.recovery_reconciliation.utils.request_payload import get_all_available_timesheet_periods_payload, get_common_trigger_config
from macquariegroup.recovery_reconciliation.utils import custom_methods

# pylint: disable=too-many-statements


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"macquarie_recovery_reconciliation_master_{config.instance}",
        description=f"Macquarie Recovery Reconciliation Master {config.instance}",
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        company_key=config.company_key,
        start_date=datetime(2023, 1, 1, tz=config.timezone),
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        },
        max_active_runs=1
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10),
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Reconciliation Sync - Incorrect File Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/bad_file_format.html"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('download_file') }}",
            encoding='utf-8-sig'
        )

        create_md5 = rail.DataAdaptorOperator(
            task_id="create_md5",
            source="{{ result('load_data')}}",
            columns=["Employee Type", "DEPARTMENT", "Costcentre",
                     "GROUP", "OFFICE", "Timesheet Period", "DIVISION", "md5"],
            data=custom_methods.get_input_md5_data
        )

        create_input_collection = rail.CreateCollectionOperator(
            task_id="create_input_collection",
            source="{{ result('create_md5') }}",
            name="input_data",
            columns={
                "Employee Type": "employee_type",
                "DEPARTMENT": "department",
                "Costcentre": "cost_centre",
                "GROUP": "groups",  # added feed as prefix as group is a SQL command
                "OFFICE": "office",
                "Timesheet Period": "timesheet_period",
                "DIVISION": "division",
                "md5": "md5"
            }
        )

        has_any_records = rail.IfOperator(
            task_id="has_any_records",
            test="{{ result('create_input_collection', 'length') > 0 }}",
            yes_task="query_invalid_records",
            no_task="send_blank_file_email"
        )

        send_blank_file_email = rail.EmailOperator(
            task_id="send_blank_file_email",
            to=config.tenant_email,
            bcc=config.internal_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Reconciliation Sync - Skipped - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            query="""SELECT * FROM input_data WHERE
                            (NULLIF(employee_type, '') IS NULL
                            OR NULLIF(department, '') IS NULL
                            OR NULLIF(cost_centre, '') IS NULL
                            OR NULLIF(timesheet_period, '') is NULL
                            OR NULLIF(groups, '') IS NULL
                            OR NULLIF(office, '') IS NULL) AND groups in ('Risk Management Group', 'Financial Management Group')   
                    """,
            name="invalid_records"
        )

        has_any_invalid_records = rail.IfOperator(
            task_id="has_any_invalid_records",
            test="{{ result('query_invalid_records', 'length') > 0 }}",
            yes_task="log_invalid_records",
            no_task="query_valid_records"
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id="log_invalid_records",
            items="{{result('query_invalid_records')}}",
            severity="Skipped",
            message="Employee Type/ Department/ Cost Center is not available in the feed file",
            properties=custom_methods.get_invalid_records_conf
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            query="""SELECT * FROM input_data WHERE
                            NULLIF(employee_type, '') IS NOT NULL
                            AND NULLIF(department, '') IS NOT NULL
                            AND NULLIF(cost_centre, '') IS NOT NULL
                            AND NULLIF(timesheet_period, '') IS NOT NULL
                            AND NULLIF(groups, '') IS NOT NULL
                            AND NULLIF(office, '') IS NOT NULL
                            AND groups in ('Risk Management Group', 'Financial Management Group')
                    """,
            name="valid_records"
        )

        has_any_valid_records = rail.IfOperator(
            task_id="has_any_valid_records",
            test="{{ result('query_valid_records', 'length') > 0}}",
            yes_task="get_unique_groups_from_feed",
            no_task="dummy_send_logs_start"
        )

        get_unique_groups_from_feed = rail.QueryCollectionOperator(
            task_id="get_unique_groups_from_feed",
            # Added TRIM to remove the leading and Trailing whitespaces
            # Added lower to remove the case sensitivity while searching the groups in replicon
            query="SELECT DISTINCT TRIM(lower(groups)) as unique_groups FROM valid_records"
        )

        get_report_details, load_report_data = run_base_report(
            config, "run_report")

        create_report_md5 = rail.DataAdaptorOperator(
            task_id="create_report_md5",
            source="{{result('load_report_data')}}",
            columns=['login_name', 'cost_center', 'department', 'employee_type', 'groups',
                     'recovery_enabled', 'recovery_override', 'user_uri', 'user_status', 'user_start_date', 'md5'],
            data=custom_methods.get_report_data_with_md5
        )

        create_report_data_collection = rail.CreateCollectionOperator(
            task_id="create_report_data_collection",
            source="{{result('create_report_md5')}}",
            name="base_report_data",
            columns={
                "login_name": "login_name",
                "cost_center": "cost_center",
                "department": "department",
                "employee_type": "employee_type",
                "groups": "groups",
                "recovery_enabled": "recovery_enabled",
                "recovery_override": "recovery_override",
                "user_uri": "user_uri",
                "user_status": "user_status",
                "user_start_date": "user_start_date",
                "md5": "md5"
            }
        )

        query_invalid_users_records = rail.QueryCollectionOperator(
            task_id="query_invalid_users_records",
            query="""SELECT * FROM base_report_data
                        WHERE NULLIF(cost_center, '') IS NULL
                        OR NULLIF(department, '') IS NULL
                        OR NULLIF(employee_type, '') IS NULL
                """,
            name="invalid_user_records"
        )

        has_any_invalid_user_records = rail.IfOperator(
            task_id="has_any_invalid_user_records",
            test="{{ result('query_invalid_users_records', 'length') > 0 }}",
            yes_task="log_invalid_user_records",
            no_task="query_valid_users_records"
        )

        log_invalid_user_records = rail.WriteLogOperator(
            task_id="log_invalid_user_records",
            severity="Validation",
            items="{{result('query_invalid_users_records')}}",
            message="Employee Type/ Department/ CostCenter is not present in User profile",
            properties={
                "login_name": '{{item.login_name}}',
                "employee_type": "{{item.employee_type}}",
                "department": "{{item.department}}",
                "cost_centre": "{{item.cost_center}}",
                "action": "Validation",
                "Status": "Skipped",
                # pylint: disable=line-too-long
                "details": '{{"Employee Type" if not item.employee_type else ("Department" if not item.department else "Cost Centre")}}' + " is not present in User profile"
            }
        )

        query_valid_users_records = rail.QueryCollectionOperator(
            task_id="query_valid_users_records",
            query="""SELECT * FROM base_report_data
                        WHERE NULLIF(cost_center, '') IS NOT NULL
                        AND NULLIF(department, '') IS NOT NULL
                        AND NULLIF(employee_type, '') IS NOT NULL
                """,
            name="valid_user_records"
        )

        has_any_valid_user_records = rail.IfOperator(
            task_id="has_any_valid_user_records",
            test="{{ result('query_valid_users_records', 'length') > 0 }}",
            yes_task="get_all_timesheet_periods_from_replicon",
            no_task="dummy_send_logs_start"
        )

        get_all_timesheet_periods_from_replicon = rail.RepliconServiceOperator(
            task_id="get_all_timesheet_periods_from_replicon",
            endpoint="/services/TimesheetPeriodListService1.svc/GetData",
            data=get_all_available_timesheet_periods_payload,
            data_handler=custom_methods.filter_get_all_timesheet_periods
        )

        query_records_to_set_recovery_yes = rail.QueryCollectionOperator(
            task_id="query_records_to_set_recovery_yes",
            query="""SELECT * FROM valid_user_records WHERE md5 IN (SELECT DISTINCT md5 FROM valid_records) AND LOWER(recovery_enabled) != 'yes'""",
            name="recovery_enable_records"
        )

        process_records_to_set_recovery_yes = rail.trigger_parallel_dagrun(
            task_id="process_records_to_set_recovery_yes",
            trigger_dag_id=f"macquarie_recovery_reconciliation_process_users_recovery_yes_child_{config.instance}",
            parallel_count=10,
            execution_timeout=timedelta(days=10),
            items="{{ result('query_records_to_set_recovery_yes') }}",
            conf=lambda item: get_common_trigger_config(item, "Yes")
        )

        query_records_to_set_recovery_no = rail.QueryCollectionOperator(
            task_id="query_records_to_set_recovery_no",
            query="""SELECT * FROM valid_user_records WHERE md5 NOT IN (SELECT DISTINCT md5 FROM valid_records) AND LOWER(recovery_enabled) != 'no' """,
            name="recovery_disable_records"
        )

        process_records_to_set_recovery_no = rail.trigger_parallel_dagrun(
            task_id="process_records_to_set_recovery_no",
            trigger_dag_id=f"macquarie_recovery_reconciliation_process_users_recovery_no_child_{config.instance}",
            parallel_count=10,
            execution_timeout=timedelta(days=10),
            items="{{ result('query_records_to_set_recovery_no')}}",
            conf=lambda item: get_common_trigger_config(item, "No")
        )

        query_records_to_skip_set_recovery_yes = rail.QueryCollectionOperator(
            task_id="query_records_to_skip_set_recovery_yes",
            query="""SELECT * FROM valid_user_records WHERE md5 IN (SELECT DISTINCT md5 FROM valid_records) AND LOWER(recovery_enabled) == 'yes' """,
            name="skipped_set_recovery_yes_records"
        )

        log_skipped_records_for_recovery_yes = rail.WriteLogOperator(
            task_id="log_skipped_records_for_recovery_yes",
            severity="Skipped",
            items="{{ result('query_records_to_skip_set_recovery_yes') }}",
            message="User's Recovery Enable flag is already set to 'Yes'",
            properties=lambda item: custom_methods.get_skipped_records_log(
                item, "Yes")
        )

        query_records_to_skip_set_recovery_no = rail.QueryCollectionOperator(
            task_id="query_records_to_skip_set_recovery_no",
            query="""SELECT * FROM valid_user_records WHERE md5 NOT IN (SELECT DISTINCT md5 FROM valid_records) AND LOWER(recovery_enabled) == 'no' """,
            name="skipped_set_recovery_no_records"
        )

        log_skipped_records_for_recovery_no = rail.WriteLogOperator(
            task_id="log_skipped_records_for_recovery_no",
            severity="Skipped",
            items="{{ result('query_records_to_skip_set_recovery_no') }}",
            message="User's Recovery Enable flag is already set to 'No'",
            properties=lambda item: custom_methods.get_skipped_records_log(
                item, "No")
        )

        dummy_send_logs_start = rail.EmptyOperator(
            task_id="dummy_send_logs_start"
        )

        send_logs_start, send_logs_end = get_send_logs(config)

        create_reference_file = rail.WriteCSVFileOperator(
            task_id="create_reference_file",
            source="{{ result('query_valid_records') }}",
            header=["employee_type", "department", "cost_center",
                    "group", "office", "timesheet_period", "division", "md5"],
            row=[
                "{{item.employee_type}}",
                "{{item.department}}",
                "{{item.cost_centre}}",
                "{{item.groups}}",
                "{{item.office}}",
                "{{item.timesheet_period}}",
                "{{item.division}}",
                "{{item.md5}}"
            ]
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id="archive_reference_file",
            existing_filename=config.recovery_reconciliation_reference_filepath +
            config.recovery_reconciliation_reference_filename,
            new_filename=config.recovery_reconciliation_reference_archive_filepath +
            "{{ dag_run_ecid() }}_" + config.recovery_reconciliation_reference_filename
        )

        upload_new_recon_reference_file = rail.SFTPUploadFileOperator(
            task_id="upload_new_recon_reference_file",
            content="{{ result('create_reference_file') }}",
            remote_filepath=config.recovery_reconciliation_reference_filepath +
            config.recovery_reconciliation_reference_filename
        )

        trigger_email_processing_dag = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_email_processing_dag",
            trigger_dag_id=f"macquarie_recovery_reconciliation_recovery_enabled_notification_child_{config.instance}",
            items = [0],
            conf= {
                "file_name": "{{result('new_file_sensor') | file_name }}"
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        trigger_user_import_dag = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_user_import_dag",
            trigger_dag_id=f"macquarie_recovery_reconciliation_move_newest_file_to_processing_child_{config.instance}",
            items = [0],
            conf= {
                "file_name": "{{result('new_file_sensor') | file_name }}"
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        new_file_sensor >> is_csv >> rail.Label(
            "Yes") >> download_file >> load_data
        is_csv >> rail.Label("No") >> send_bad_file_format_email
        download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        load_data >> create_md5 >> create_input_collection >> has_any_records >> rail.Label(
            "No") >> send_blank_file_email

        has_any_records >> rail.Label("Yes") >> query_invalid_records >> has_any_invalid_records >> rail.Label(
            "Yes") >> log_invalid_records >> query_valid_records
        has_any_invalid_records >> rail.Label("No") >> query_valid_records >> has_any_valid_records \
            >> rail.Label("Yes") >> get_unique_groups_from_feed >> get_report_details
        has_any_valid_records >> rail.Label("No") >> dummy_send_logs_start

        load_report_data >> create_report_md5 >> create_report_data_collection >> query_invalid_users_records >> has_any_invalid_user_records >> rail.Label(
            "Yes") >> log_invalid_user_records >> query_valid_users_records
        has_any_invalid_user_records >> rail.Label(
            "No") >> query_valid_users_records >> has_any_valid_user_records

        has_any_valid_user_records >> rail.Label("Yes") >> get_all_timesheet_periods_from_replicon >> \
            [query_records_to_set_recovery_yes, query_records_to_skip_set_recovery_yes,
             query_records_to_set_recovery_no, query_records_to_skip_set_recovery_no]
        has_any_valid_user_records >> rail.Label(
            "No") >> dummy_send_logs_start >> send_logs_start

        query_records_to_set_recovery_yes >> process_records_to_set_recovery_yes >> dummy_send_logs_start
        query_records_to_set_recovery_no >> process_records_to_set_recovery_no >> dummy_send_logs_start
        query_records_to_skip_set_recovery_yes >> log_skipped_records_for_recovery_yes >> dummy_send_logs_start
        query_records_to_skip_set_recovery_no >> log_skipped_records_for_recovery_no >> dummy_send_logs_start

        send_logs_end >> create_reference_file >> archive_reference_file \
            >> upload_new_recon_reference_file >> trigger_email_processing_dag >> trigger_user_import_dag

    return dag


rail.for_each_instance(create_main_dag)
