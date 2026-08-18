from datetime import timedelta
from pendulum import now, datetime as pdt
import rail
from guidehouse.time_export.time_export_master.utils import custom_methods
from guidehouse.time_export.time_export_master.utils.date_range import (
    get_hourly_date_window,
    get_daily_date_window,
    format_date_for_export,
)
from guidehouse.time_export.time_export_master.tasks.download_time_export import download_export
from guidehouse.time_export.time_export_master.tasks.approval_data_report import approval_data_report_task
null=None
def create_wrapper_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description="Guidehouse Time Export - Wrapper DAG for coordinated PS, India, and CP exports",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pdt(year=2026, month=5, day=1, tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_max_active_run,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        check_can_trigger_next_run = rail.PythonOperator(
            task_id="check_can_trigger_next_run",
            python_callable=lambda: custom_methods.check_previous_wrapper_dag_runs(config),
        )

        if_previous_wrapper_unsuccessful = rail.IfOperator(
            task_id="if_previous_wrapper_unsuccessful",
            test=lambda: not (
                rail.result("check_can_trigger_next_run")["can_process_further"]
            ),
            yes_task="fail_wrapper_blocked",
            no_task="get_date_window",
        )

        fail_wrapper_blocked = rail.FailOperator(
            task_id="fail_wrapper_blocked",
            message="Previous wrapper run failed. Blocking until resolved manually.",
        )

        def get_date_window_callable(dag_run):
            run_date = now(tz=config.timezone)
            if config.run_type == "hourly":
                start_date, end_date = get_hourly_date_window(
                    run_date, tz_name=config.timezone
                )
            elif config.run_type == "daily":
                hourly_start, _ = get_hourly_date_window(
                    run_date, tz_name=config.timezone
                )
                start_date, end_date = get_daily_date_window(
                    run_date, hourly_start, tz_name=config.timezone
                )
            _datestr = run_date.strftime("%Y%m%d_%H%M%S")
            return {
                "run_date": run_date.strftime("%Y-%m-%d"),
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "start_date_formatted": format_date_for_export(start_date),
                "end_date_formatted": format_date_for_export(end_date),
                "report_start_date": dag_run.conf.get("report_start_date"),
                "report_end_date": dag_run.conf.get("report_end_date"),
                "timestamp": _datestr,
                "export_type": config.run_type,
            }

        get_date_window = rail.PythonOperator(
            task_id="get_date_window",
            python_callable=get_date_window_callable,
        )

        get_all_work_locations = rail.RepliconServiceOperator(
            task_id="get_all_work_locations",
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
                },
            data_handler = lambda response: list(map(lambda row: {
                "level1_name": row["cells"][0]["textValue"],
                "uri": row["cells"][0]["uri"],
                "level1_code": row["cells"][1].get("textValue")
            }, response["rows"]))
        )

        get_level2_work_locations = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_level2_work_locations",
            items=config.level2_countries,
            endpoint="/services/LocationListService1.svc/GetChildHierarchyData",
            data=lambda item: {
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:location-list-column:name",
                    "urn:replicon:location-list-column:code"
                ],
                "parentUri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_work_locations"),
                    "level1_name", item, "uri"),
            },
            data_handler=lambda data, item: {
                f"{item}/{row['cells'][0]['textValue']}": row["cells"][1].get("textValue")
                for row in data["rows"]
            },
            all_result_data_handler=lambda results: {
                key: value for country_map in results for key, value in country_map.items()
            },
        )

        trigger_ps_child = rail.TriggerDagRunOperator(
            task_id="trigger_ps_child",
            trigger_dag_id=config.ps_child_dag_id,
            conf=lambda: {
                "start_date": rail.result("get_date_window")["start_date"],
                "end_date": rail.result("get_date_window")["end_date"],
                "timestamp": rail.result("get_date_window")["timestamp"],
                "export_type": config.run_type,
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        trigger_india_child = rail.TriggerDagRunOperator(
            task_id="trigger_india_child",
            trigger_dag_id=config.india_child_dag_id,
            conf=lambda: {
                "start_date": rail.result("get_date_window")["start_date"],
                "end_date": rail.result("get_date_window")["end_date"],
                "timestamp": rail.result("get_date_window")["timestamp"],
                "export_type": config.run_type,
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        trigger_cp_child = rail.TriggerDagRunOperator(
            task_id="trigger_cp_child",
            trigger_dag_id=config.dl_cp_export_dag_id,
            conf=lambda dag_run: {
                "start_date": rail.result("get_date_window")["start_date"],
                "end_date": rail.result("get_date_window")["end_date"],
                "timestamp": rail.result("get_date_window")["timestamp"],
                "report_start_date": dag_run.conf.get("report_start_date"),
                "report_end_date": dag_run.conf.get("report_end_date"),
                "level1_locations": { data["level1_name"] : data["level1_code"]
                 for data in rail.result("get_all_work_locations") },
                "level2_locations": rail.result("get_level2_work_locations")
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        def get_dag_ids_to_wait_callable():
            return [
                rail.result("trigger_ps_child"),
                rail.result("trigger_india_child"),
            ]

        get_dag_ids_to_wait = rail.PythonOperator(
            task_id="get_dag_ids_to_wait",
            python_callable=get_dag_ids_to_wait_callable,
        )

        wait_for_exports = rail.WaitForDagRunsSensor(
            task_id="wait_for_exports",
            dag_runs="{{ result('get_dag_ids_to_wait') | to_json }}",
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_export_results = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_export_results",
            dag_runs="{{ result('get_dag_ids_to_wait') | to_json }}",
            dagrun_task_id="final_response_from_dag",
            flatten=True,
        )

        validate_export_results = rail.PythonOperator(
            task_id="validate_export_results",
            python_callable=lambda: custom_methods.validate_export_results(
                rail.result("gather_export_results")
            ),
        )

        if_exports_valid = rail.IfOperator(
            task_id="if_exports_valid",
            test=lambda: rail.result("validate_export_results")["are_valid"],
            yes_task="if_data_in_ps_or_india",
            no_task="fail_exports_invalid",
        )

        if_data_in_ps_or_india = rail.IfOperator(
            task_id="if_data_in_ps_or_india",
            test=lambda: rail.result("validate_export_results")["has_data"],
            yes_task="start_process",
            no_task="mark_success"
        )

        start_process = rail.EmptyOperator(task_id="start_process")
        
        download_ps_export = download_export("ps_time_export", 
        config.dl_time_export_format, 
        "peoplesoft",
          "ps_raw_time_export")

        download_india_export = download_export("india_time_export", 
        config.dl_time_export_format,"india",
            "india_raw_time_export")
        
        run_report = approval_data_report_task("ps_india_approval_data", config.ps_india_report_name,
                                               "ps_india_report_data")

        paycodes_str = ", ".join(f"'{code}'" for code in config.paycodes_to_exclude)
        query_union_ps_india = rail.QueryCollectionOperator(
            task_id="query_union_ps_india",
            query=f"""SELECT * FROM ps_raw_time_export WHERE nullif(employee_id,'') IS NOT NULL AND
            (NULLIF(pay_code, '') IS NULL OR pay_code NOT IN ({paycodes_str}))
            UNION
            SELECT * FROM india_raw_time_export WHERE nullif(employee_id,'') IS NOT NULL AND
            (NULLIF(pay_code, '') IS NULL OR pay_code NOT IN ({paycodes_str}))"""
        )
        
        apply_timeoff_mapping = rail.DataAdaptorOperator(
            task_id="apply_timeoff_mapping",
            source='{{ result("query_union_ps_india") }}',
            data=lambda item: custom_methods.get_peoplesoft_export_rows(
                item, config.TIMEOFF_PROJECT_TASK_MAPPER, config.level2_countries
            ),
        )

        create_final_data_collection = rail.CreateCollectionOperator(
            task_id="create_final_data_collection",
            source='{{result("apply_timeoff_mapping")}}',
            name="export_data",
            columns={'employee_id': 'employee_id', 'user': 'user', 'entry_date': 'entry_date', 
                     'project_code': 'project_code', 'task_name': 'task_name', 'task_name_full_path': 'task_name_full_path',
                       'task_code': 'task_code', 'pay_code': 'pay_code', 'hours': 'hours', 
                       'company_code_code': 'company_code_code', 'short_time_entry_id': 'short_time_entry_id', 'work_location_code': 'work_location_code', 'timeoff_type': 'timeoff_type', 
                     'timesheet_period': 'timesheet_period', 'fmla': 'fmla', 
                     'financial_system_name': 'financial_system_name', 
                     'timeoff_booking_id': 'timeoff_booking_id', 'login_name': 'login_name',
                     "timeoff_hours":"timeoff_hours","plc":"plc","plc_name":"plc_name",
                     "comments":"comments"}
        )

        query_approval_ps_india_approval_data = rail.QueryCollectionOperator(
            task_id="query_approval_ps_india_approval",
            query="""SELECT rd.unique_id, ps.employee_id,
             ps.user, ps.entry_date, ps.project_code, ps.task_code,
             ps.pay_code, ps.hours, ps.company_code_code,
             rd.submitted_on AS timesheet_submitted_on,
             rd.approval_datetime,
             rd.approval_status AS timesheet_approval_status,
             ps.short_time_entry_id, ps.work_location_code,
             ps.timeoff_type, ps.timeoff_hours, ps.plc_name, ps.plc, ps.timesheet_period, ps.fmla,
             ps.financial_system_name, ps.comments
            FROM export_data ps LEFT JOIN ps_india_report_data rd ON
            ps.employee_id = rd.employee_id AND ps.timesheet_period = rd.timesheet_period""",
            name="final_ps_india_data"
        )

        write_export_psindia_csv = rail.WriteCSVFileOperator(
            task_id="write_export_psindia_csv",
            source='{{result("query_approval_ps_india_approval")}}',
            header=['Unique ID', 'Employee ID', 'Username', 'Entry Date', 'Project Code',
                    'Task Code', 'Pay code', 'Hours',
                    'Company Code', 'Timesheet Submitted On', 'Approval Date/Time',
                    'Timesheet Approval Status', 'Short Entry ID', 'Work Location',
                    'Time Off Type', 'Time Off Hours', 'PLC Name', 'PLC', 'Timesheet Period',
                    'Financial System', 'Comments'],
            row=[
                "{{item.unique_id}}",
                "{{item.employee_id}}",
                "{{item.user}}",
                "{{item.entry_date}}",
                "{{item.project_code}}",
                "{{item.task_code}}",
                "{{item.pay_code}}",
                "{{item.hours}}",
                "{{item.company_code_code}}",
                "{{item.timesheet_submitted_on}}",
                "{{item.approval_datetime}}",
                "{{item.timesheet_approval_status}}",
                "{{item.short_time_entry_id}}",
                "{{item.work_location_code}}",
                "{{item.timeoff_type}}",
                "{{item.timeoff_hours}}",
                "{{item.plc_name}}",
                "{{item.plc}}",
                "{{item.timesheet_period}}",
                "{{item.financial_system_name}}",
                "{{item.comments}}",
            ],
            delimiter="|",
        )

        encrypt_time_export_data_csv = rail.PGPEncryptionOperator(
            task_id="encrypt_time_export_data_csv",
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('write_export_psindia_csv') }}"
        )

        upload_dl_psindia_export_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_dl_psindia_export_to_sftp",
            content='{{ result("encrypt_time_export_data_csv") }}',
            remote_filepath=config.dl_outbound_path
            + f"/DL_PPS_Time{config.env_suffix}_"
            + "{{ result('get_date_window').timestamp }}.csv.pgp",
        )

        send_success_email = rail.EmailOperator(
            task_id="send_success_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | PeopleSoft India Replicon Data Lake {{ result('get_date_window').export_type | capitalize }} time data extract is completed - {{ result('get_date_window').timestamp }}",
            html_content="/templates/email_valid_export_complete_ps_india.html",
            params={
                "upload_file_path": config.dl_outbound_path,
                "ps_file_prefix": f"DL_PPS_Time{config.env_suffix}_",
                "cp_file_prefix": f"DL_CP_Time{config.env_suffix}_",
            },
        )
        
        fail_exports_invalid = rail.FailOperator(
            task_id="fail_exports_invalid",
            message="{{ result('validate_export_results').validation_message }}",
        )

        mark_success = rail.PythonOperator(
            task_id="mark_success",
            python_callable=lambda: "All exports completed successfully"
        )

        (
            check_can_trigger_next_run
            >> if_previous_wrapper_unsuccessful
            >> rail.Label("Yes")
            >> fail_wrapper_blocked
        )

        (
            check_can_trigger_next_run
            >> if_previous_wrapper_unsuccessful
            >> rail.Label("No")
            >> get_date_window
            >> get_all_work_locations
            >> get_level2_work_locations
            >> [trigger_ps_child, trigger_india_child, trigger_cp_child]
            >> get_dag_ids_to_wait
            >> wait_for_exports
            >> gather_export_results
            >> validate_export_results
            >> if_exports_valid
        )

        if_exports_valid >> rail.Label("Yes") >> \
        if_data_in_ps_or_india >> rail.Label("No") >> mark_success
        if_data_in_ps_or_india >> rail.Label("Yes")>>\
        start_process >>\
        download_ps_export >> download_india_export >>\
        run_report >> query_union_ps_india >>\
        apply_timeoff_mapping >> create_final_data_collection >>\
        query_approval_ps_india_approval_data >>\
        write_export_psindia_csv >> encrypt_time_export_data_csv >> upload_dl_psindia_export_to_sftp >>\
        send_success_email >> mark_success
        if_exports_valid >> rail.Label("No") >> fail_exports_invalid

    return dag


rail.for_each_instance(create_wrapper_dag)
