from datetime import timedelta
from pendulum import datetime
import rail
from wipro.shiftassignment_export.utils import custom_methods
null = None


def create_airflow_master(config):
    with rail.create_airflow_dag(
        dag_id=config.shift_assignment_export_master,
        description="Shift assignment export master",
        schedule_interval=config.schedule_interval,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 9, 10, tz=config.time_zone),
        company_key=config.company_key
    ) as dag:

        get_start_and_end_time = rail.PythonOperator(
            task_id="get_start_and_end_time",
            python_callable=lambda: custom_methods.get_last_run_date_time(
                config)
        )

        get_shift_details = rail.RepliconServicePageOperator(
            task_id="get_shift_details",
            endpoint="/services/ShiftAssignmentService1.svc/GetPageOfLatestShiftAssignmentRevisionDetails",
            data=lambda: {
                "page": 1,
                "pageSize": 500,
                "modificationDateTimeRangeUtc": rail.result('get_start_and_end_time'),
                "modificationActionUris": [
                    "urn:replicon:shift-assignment-revision-modification-action:add",
                    "urn:replicon:shift-assignment-revision-modification-action:modify"
                ],
                "shiftAssignmentSearch": {
                    "assignmentDateRange": null,
                    "users": null,
                    "shiftSearch": null,
                    "publishState": "urn:replicon:shift-assignment-publish-state:published"
                }
            },
            page_handler=custom_methods.get_page_size,
            all_result_data_handler=custom_methods.get_shift_results,
        )

        create_shift_details_collection = rail.CreateCollectionOperator(
            task_id="create_shift_details_collection",
            source='{{result("get_shift_details")}}',
            columns=["date", "end_time", "action", "shift", "shift_uri", "start_time",
                     "user_loginname", "user_uri", 'work_week_start_date', 'work_week_end_date'],
            name="user_shift_details"
        )

        if_shift_revision_data_blank = rail.IfOperator(
            task_id="if_shift_revision_data_blank",
            test='{{result("create_shift_details_collection", "length") < 1}}',
            yes_task='send_no_data_to_export_mail',
            no_task='query_user_shift_details_grouped_by_work_week_start'
        )

        send_no_data_to_export_mail = rail.EmailOperator(
            task_id="send_no_data_to_export_mail",
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Shift Export - no records to process \
                    {{ " - " + current_time("%m-%d-%Y-%H-%M-%S") }}',
            html_content="templates/no_records_to_export.html"
        )

        query_user_shift_details_grouped_by_work_week_start = rail.QueryCollectionOperator(
            task_id="query_user_shift_details_grouped_by_work_week_start",
            query="""SELECT * FROM user_shift_details GROUP BY user_uri, work_week_start_date """,
            name="user_shift_details_grouped"
        )

        query_get_user_report_filter_dates = rail.QueryCollectionOperator(
            task_id="query_get_user_report_filter_dates",
            query="""SELECT
                    MIN(strftime('%Y-%m-%d', substr(work_week_start_date, 7, 4) || '-' || substr(work_week_start_date, 4, 2) || '-' || substr(work_week_start_date, 1, 2))) AS min_start_date,
                    MAX(strftime('%Y-%m-%d', substr(work_week_end_date, 7, 4) || '-' || substr(work_week_end_date, 4, 2) || '-' || substr(work_week_end_date, 1, 2))) AS max_end_date
                FROM
                    user_shift_details_grouped""",
        )

        log_user_report_filter_dates = rail.PythonOperator(
            task_id='log_user_report_filter_dates',
            python_callable=lambda: rail.load_all_records(
                rail.result('query_get_user_report_filter_dates'))[0]
        )

        get_all_shift_details = rail.RepliconServiceOperator(
            task_id="get_all_shift_details",
            endpoint="/services/ShiftListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "1000000",
                    "columnUris": [
                        "urn:replicon:shift-list-column:shift",
                        "urn:replicon:shift-list-column:description",
                        "urn:replicon:shift-list-column:name"
                    ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=custom_methods.get_details
        )

        create_shift_collection = rail.CreateCollectionOperator(
            task_id="create_shift_collection",
            source='{{result("get_all_shift_details")|to_json}}',
            name="shift_details_data"
        )

        get_active_user_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_active_user_report_details",
            report_name=config.active_user_report_name
        )

        get_required_filters = rail.PythonOperator(
            task_id='get_required_filters',
            python_callable=lambda: custom_methods.get_filter_uris(rail.result('get_active_user_report_details')[
                'filterConfiguration']['enabledFilters'])
        )

        get_required_country_service_center_uris_as_per_mapper = rail.RepliconServiceOperator(
            task_id="get_required_country_service_center_uris_as_per_mapper",
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters",
            data_handler=lambda res: list(map(lambda x: rail.find_first_by_attr_and_get_attr(
                res, 'displayText', x, 'uri'), config.countries_to_process))
        )

        run_user_report_start, run_user_report_end = rail.run_report(
            group_id="active_users_report_run",
            report_params=custom_methods.get_report_parameters
        )

        if_report_has_error = rail.IfOperator(
            task_id="if_report_has_error",
            test="{{result('active_users_report_run.get_report_result').reportGenerationResults[0].error | is_truthy}}",
            yes_task="fail_report_run_error",
            no_task="if_report_has_payload"
        )

        fail_report_run_error = rail.FailOperator(
            task_id="fail_report_run_error",
            message="Fail report run error"
        )

        if_report_has_payload = rail.IfOperator(
            task_id="if_report_has_payload",
            test="{{ result('active_users_report_run.get_report_result', 'has_data') }}",
            yes_task="if_report_has_valid_columns",
            no_task="fail_no_data"
        )

        fail_no_data = rail.FailOperator(
            task_id="fail_no_data",
            message="Fail report no data"
        )

        if_report_has_valid_columns = rail.IfOperator(
            task_id="if_report_has_valid_columns",
            test=lambda: rail.result('active_users_report_run.get_report_result')["reportGenerationResults"][0]["payload"].startswith(
                config.expected_report_columns),
            yes_task="load_user_data",
            no_task="fail_invlaid_report_columns"
        )

        fail_invlaid_report_columns = rail.FailOperator(
            task_id="fail_invlaid_report_columns",
            message="Fail invalid report columns"
        )

        load_user_data = rail.LoadCSVFileOperator(
            task_id="load_user_data",
            document="{{result('active_users_report_run.get_report_result').reportGenerationResults[0].payload}}",
            headers=["employee_id", "entry_date", "user_loginname",
                     "country", "user_uri", "user_status", "shift_uri", "shift_name", "shift_start_time", "shift_end_time"]
        )

        create_user_collection = rail.CreateCollectionOperator(
            task_id="create_user_collection",
            source='{{result("load_user_data")}}',
            name="enabled_user_data"
        )

        create_shift_assignment_export_log = rail.CreateLogOperator(
            task_id="create_shift_assignment_export_log"
        )

        merge_user_and_shift_data = rail.QueryCollectionOperator(
            task_id="merge_user_and_shift_data",
            query="""SELECT
                    eud.*, usdg.work_week_start_date, usdg.work_week_end_date
                FROM
                    user_shift_details_grouped AS usdg
                JOIN
                    enabled_user_data AS eud
                ON
                    usdg.user_uri = eud.user_uri
                WHERE
                    strftime('%Y-%m-%d', substr(usdg.work_week_start_date, 7, 4) || '-' || substr(usdg.work_week_start_date, 4, 2) || '-' || substr(usdg.work_week_start_date, 1, 2)) <= strftime('%Y-%m-%d', replace(eud.entry_date, '/', '-'))
                AND
                    strftime('%Y-%m-%d', substr(usdg.work_week_end_date, 7, 4) || '-' || substr(usdg.work_week_end_date, 4, 2) || '-' || substr(usdg.work_week_end_date, 1, 2)) >= strftime('%Y-%m-%d', replace(eud.entry_date, '/', '-'))""",
            name="enabled_users_with_shift_details"
        )

        query_invalid_data = rail.QueryCollectionOperator(
            task_id="query_invalid_data",
            query="""SELECT * FROM enabled_users_with_shift_details
                WHERE NULLIF(employee_id, "") IS NULL OR NULLIF(country, "") IS NULL OR NULLIF(shift_uri, "") IS NULL"""
        )

        if_invalid_data = rail.IfOperator(
            task_id="if_invalid_data",
            test='{{result("query_invalid_data", "length") > 0}}',
            yes_task='write_invalid_data_log',
            no_task='query_valid_data'
        )

        write_invalid_data_log = rail.WriteLogOperator(
            task_id="write_invalid_data_log",
            log='{{result("create_shift_assignment_export_log")}}',
            items='{{result("query_invalid_data")}}',
            severity="Skipped",
            message="Data not exported as there are missing values",
            properties=lambda item: {
                "empid": item.get("employee_id", ""),
                "country": item.get("country", ""),
                "begin_date": item.get("entry_date", ""),
                "shift_type": item.get("shift_type", ""),
                "shift_location": item.get("shift_location", ""),
                "shift_dws": item.get("shift_dws", ""),
                "status": "Skipped",
                "details": custom_methods.get_log_details(item)
            }
        )

        query_valid_data = rail.QueryCollectionOperator(
            task_id="query_valid_data",
            query="""SELECT * FROM enabled_users_with_shift_details
                WHERE
                    NULLIF(employee_id, "") IS NOT NULL
                    AND NULLIF(country, "") IS NOT NULL
                    AND NULLIF(shift_uri, "") IS NOT NULL""",
            name='valid_users_with_shift_details'
        )

        merge_user_shift_data_with_shift_details = rail.QueryCollectionOperator(
            task_id="merge_user_shift_data_with_shift_details",
            query="""SELECT vusd.*, sd.shift_type, sd.shift_location, sd.shift_dws FROM
                valid_users_with_shift_details vusd, shift_details_data sd WHERE vusd.shift_uri=sd.shift_uri""",
            name="user_shift_data_with_shift_details"
        )

        query_missing_shift_data_from_workweek = rail.QueryCollectionOperator(
            task_id="query_missing_shift_data_from_workweek",
            query="""SELECT
                d.employee_id, d.entry_date,
                d.user_loginname, d.country,  d.user_uri, d.user_status,
                "" AS shift_uri,
                "" AS shift_name,
                "00:00:00" AS shift_start_time,
                "00:00:00" AS shift_end_time,
                d.work_week_start_date, d.work_week_end_date,
                "02" AS shift_type,
                d.shift_location AS shift_location,
                "FRE1" AS shift_dws
            FROM
                -- Generate all the 7 days (Monday to Sunday) for each employee based on work_week_start_date
                (SELECT DISTINCT user_uri,
                    employee_id, user_loginname, country, user_status, work_week_start_date, work_week_end_date,shift_location,
                    STRFTIME("%Y/%m/%d",
                        date(strftime("%Y-%m-%d",
                            substr(work_week_start_date, 7, 4) || '-' ||
                            substr(work_week_start_date, 4, 2) || '-' ||
                            substr(work_week_start_date, 1, 2)),
                                '+' || days.day || ' day')) AS entry_date
                FROM user_shift_data_with_shift_details
                JOIN (SELECT 0 AS day UNION ALL
                    SELECT 1 UNION ALL
                    SELECT 2 UNION ALL
                    SELECT 3 UNION ALL
                    SELECT 4 UNION ALL
                    SELECT 5 UNION ALL
                    SELECT 6) days
                ) d
            LEFT JOIN user_shift_data_with_shift_details existing_shifts
                ON d.user_uri = existing_shifts.user_uri
                AND existing_shifts.entry_date = d.entry_date
            WHERE
                existing_shifts.entry_date IS NULL  -- Only the entries where shift_date is NOT present in user_shift_data_with_shift_details.entry_date
            ORDER BY
                d.user_uri,
                d.entry_date""",
            name='missing_work_week_shift_data'
        )

        final_data_to_process = rail.QueryCollectionOperator(
            task_id="final_data_to_process",
            query="""SELECT * FROM user_shift_data_with_shift_details
                UNION
                    SELECT * FROM missing_work_week_shift_data
                ORDER BY
                    user_uri,
                    entry_date""",
            name='final_data_to_process'
        )

        if_final_data_to_process = rail.IfOperator(
            task_id="if_final_data_to_process",
            test='{{result("final_data_to_process", "length")>0}}',
            yes_task="write_valid_data_log",
            no_task="write_logs_csv"
        )

        write_valid_data_log = rail.WriteLogOperator(
            task_id="write_valid_data_log",
            log='{{result("create_shift_assignment_export_log")}}',
            items='{{result("final_data_to_process")}}',
            severity="",
            message="Data exported",
            properties=lambda item: {
                "empid": item.get("employee_id", ""),
                "country": item.get("country", ""),
                "begin_date": item.get("entry_date", ""),
                "shift_type": item.get("shift_type", ""),
                "shift_location": item.get("shift_location", ""),
                "shift_dws": item.get("shift_dws", ""),
                "status": "Success",
                "details": "Shift exported successfully."
            }
        )

        process_shift_data_start = rail.EmptyOperator(
            task_id="process_shift_data_start"
        )

        process_shift_data = rail.TriggerDagRunForEachItemOperator(
            task_id="process_shift_data",
            items='{{result("final_data_to_process")}}',
            trigger_dag_id=config.shift_assignment_export_child,
            batch_size=config.batch_size,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "items": item
            }
        )

        wait_for_shift_data = rail.WaitForDagRunsSensor(
            task_id="wait_for_shift_data",
            dag_runs='{{result("process_shift_data")}}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        write_logs_csv = rail.WriteCSVFileOperator(
            task_id="write_logs_csv",
            source='{{result("create_shift_assignment_export_log")}}',
            header=["Employee Id", "Country", "Begin Date", "Shift type",
                    "Shift Location", "Shift DWS", "Status", "Details", "JobId"],
            row=[
                '{{item.properties.empid}}',
                '{{item.properties.country}}',
                '{{item.properties.begin_date}}',
                '{{item.properties.shift_type}}',
                '{{item.properties.shift_location}}',
                '{{item.properties.shift_dws}}',
                '{{item.properties.status}}',
                '{{item.properties.details}}',
                '{{item.ecid}}'
            ]
        )

        generate_pre_signed_download_url = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_pre_signed_download_url",
            artifact_name='{{result("write_logs_csv")}}',
            output_file_name='Shift_Export_Logs_' +
            '{{current_time("%m-%d-%Y-%H-%M-%S")}}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_export_complete_email = rail.EmailOperator(
            task_id="send_export_complete_email",
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Shift Export - Completed Successfully \
                    {{ " - " + current_time("%m-%d-%Y-%H-%M-%S") }}',
            html_content="templates/export_complete_mail.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done",
            extra_info={
                "No of valid records": '{{result("query_valid_data", "length")}}',
                "No of invalid records": '{{result("query_invalid_data", "length")}}'
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id='can_fail_dag',
            test='{{get_error_message()|is_truthy}}',
            yes_task="fail_dagrun"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{get_error_message()}}'
        )

        get_start_and_end_time >> get_shift_details >> create_shift_details_collection >> if_shift_revision_data_blank

        if_shift_revision_data_blank >> rail.Label(
            "Yes") >> send_no_data_to_export_mail >> log_to_sumo
        if_shift_revision_data_blank >> rail.Label(
            "No") >> query_user_shift_details_grouped_by_work_week_start

        query_user_shift_details_grouped_by_work_week_start >>\
            query_get_user_report_filter_dates >> log_user_report_filter_dates >>\
            get_all_shift_details >> create_shift_collection >> get_active_user_report_details >> get_required_filters >>\
            get_required_country_service_center_uris_as_per_mapper >> run_user_report_start >> run_user_report_end >>\
            if_report_has_error >> rail.Label("Yes") >> fail_report_run_error
        if_report_has_error >> rail.Label("No") >>\
            if_report_has_payload >> rail.Label("No") >> fail_no_data
        if_report_has_payload >> rail.Label("Yes") >> \
            if_report_has_valid_columns >> rail.Label(
                "No") >> fail_invlaid_report_columns
        if_report_has_valid_columns >> rail.Label("Yes") >>\
            load_user_data >> create_user_collection >> create_shift_assignment_export_log >> merge_user_and_shift_data

        merge_user_and_shift_data >> query_invalid_data >> if_invalid_data

        if_invalid_data >> rail.Label(
            "No") >> query_valid_data
        if_invalid_data >> rail.Label(
            "Yes") >> write_invalid_data_log >> query_valid_data

        query_valid_data >> merge_user_shift_data_with_shift_details

        merge_user_shift_data_with_shift_details >>\
            query_missing_shift_data_from_workweek >> final_data_to_process >> if_final_data_to_process

        if_final_data_to_process >> rail.Label("No") >> write_logs_csv
        if_final_data_to_process >> rail.Label("Yes") >> write_valid_data_log >> process_shift_data_start >>\
            process_shift_data >> wait_for_shift_data >> write_logs_csv

        write_logs_csv >>\
            generate_pre_signed_download_url >> send_export_complete_email >> log_to_sumo

        log_to_sumo >> can_fail_dag >> fail_dagrun

        return dag


rail.for_each_instance(create_airflow_master)
