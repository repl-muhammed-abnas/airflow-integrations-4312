
from datetime import timedelta, datetime as dt
import json
from pendulum import datetime
from ascendmaterials.addbreakpunch import request_payload, custom_methods
import rail
null=None
def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"ascendmaterials_add_break_punch_in_replicon_master_{config.instance}",
        description="ascendmaterial add break punch in replicon",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023,8,10, tz=config.central_time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master
    ) as dag:

        ascendmaterials_log_lookup_table = rail.CreateLogOperator(
            task_id="ascendmaterials_log_lookup_table"
        )
        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_user_report_details",
            report_name=config.enabled_user_report_name
        )

        run_user_report_entry, run_user_report_exit = rail.run_report(
            group_id="run_user_report",
            report_params={
                    "reportParameters": [
                        {
                            "reportUri": '{{ result("get_user_report_details").uri }}',
                            "filterValues": [],
                            "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                        }
                    ]
                },
        )

        if_report_run_failed = rail.IfOperator(
            task_id="if_report_run_failed",
            test='{{result("run_user_report.get_report_result").reportGenerationResults[0].error|is_truthy}}',
            yes_task="fail_dagrun",
            no_task="load_user_report_csv"
        )

        load_user_report_csv = rail.LoadCSVFileOperator(
            task_id="load_user_report_csv",
            document='{{result("run_user_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        create_enabled_users_collection = rail.CreateCollectionOperator(
            task_id="create_enabled_user_collection",
            source="{{result('load_user_report_csv')}}",
            columns={
                "User Name": "username" ,
                "useruri":"useruri",
                "Location (Current)":"currentlocation"},
            name="userdataforallowedlocation"
        )

        get_time_punch_entry_data_for_current_date = rail.RepliconServiceOperator(
            task_id="get_time_punch_entry_data_for_current_date",
            endpoint="/services/TimePunchListService1.svc/GetData",
            data=request_payload.time_punch_data_request,
            data_handler=lambda response:list(map(lambda item:{
                                "useruri" : item["cells"][0]["uri"],
                                "username": item["cells"][0]["textValue"],
                                "datetime" :str(dt.strptime(str(item["cells"][1]["dateValue"]["year"])+" "+
                                                                    str(item["cells"][1]["dateValue"]["month"]) + " "+
                                                                    str(item["cells"][1]["dateValue"]["day"]) +" "+
                                                                    str(item["cells"][1]["timeValue"]["hour"])+":"+
                                                                    str(item["cells"][1]["timeValue"]["minute"])+":"+
                                                                    str(item["cells"][1]["timeValue"]["second"]), "%Y %m %d %H:%M:%S")),
                                "punchaction": item["cells"][2]["textValue"],
                                "punchuri":item["cells"][3]["uri"],
                                "activityuri": item["cells"][4]["uri"] if "uri" in item["cells"][4] else null
                            }, response["rows"]))
        )

        if_time_punches_present_for_current_date = rail.IfOperator(
            task_id = "if_time_punches_present_for_current_date",
            test='{{result("get_time_punch_entry_data_for_current_date") | length > 0}}',
            yes_task="create_time_punch_entry_collection",
            no_task="log_to_sumo"
        )

        create_time_punch_entry_collection = rail.CreateCollectionOperator(
            task_id="create_time_punch_entry_collection",
            source='{{result("get_time_punch_entry_data_for_current_date")|to_json}}',
            name="rawtimepunchdata",
        )

        get_shift_details_for_current_date = rail.RepliconServiceOperator(
            task_id="get_shift_details_for_current_date",
            endpoint="/services/ShiftAssignmentListService1.svc/GetData",
            data=request_payload.shift_details_request,
            data_handler=lambda response:json.dumps(list(map(lambda item:{
                "user": item["cells"][0]["textValue"],
                "useruri": item["cells"][0]["uri"],
                "shift": item["cells"][1]["textValue"],
                "shifturi": item["cells"][1]["uri"]
            }, response["rows"])))
        )

        if_shift_details_present_for_current_date = rail.IfOperator(
            task_id="if_shift_details_present_for_current_date",
            test='{{result("get_shift_details_for_current_date") | length > 0}}',
            yes_task="create_shift_details_collection",
            no_task="log_to_sumo"
        )

        create_shift_details_collection = rail.CreateCollectionOperator(
            task_id="create_shift_details_collection",
            source='{{result("get_shift_details_for_current_date")}}',
            name="shiftdetails"
        )

        query_enabled_users_with_shift_schedule = rail.QueryCollectionOperator(
            task_id="query_enabled_users_with_shift_schedule",
            query="""SELECT * FROM shiftdetails  s WHERE s.useruri IN
                    (SELECT DISTINCT u.useruri from userdataforallowedlocation u)
                    """
        )

        query_all_current_date_shifts = rail.QueryCollectionOperator(
            task_id="query_all_current_date_shifts",
            query="""SELECT DISTINCT shifturi from query_enabled_users_with_shift_schedule""",
        )

        get_bulk_shift_schedule_details = rail.RepliconServiceOperator(
            task_id="get_bulk_shift_schedule_details",
            endpoint="/services/ShiftService1.svc/BulkGetShiftDetails",
            data=lambda: json.dumps({
                    "shiftUris": list(map(lambda item:item["shifturi"],
                                          rail.load_all_records(rail.result("query_all_current_date_shifts"))))
                }),
            data_handler=custom_methods.get_shift_details
        )

        query_enabled_user_time_punch_for_current_date = rail.QueryCollectionOperator(
            task_id="query_enabled_user_time_punch_for_current_date",
            query="""SELECT DISTINCT useruri from rawtimepunchdata
                    WHERE useruri IN (SELECT DISTINCT useruri FROM userdataforallowedlocation)"""
        )

        process_break_punch_for_users = rail.trigger_parallel_dagrun(
            task_id="process_break_punch_for_users",
            trigger_dag_id=f"ascendmaterials_add_break_punch_in_replicon_child_{config.instance}",
            items='{{result("query_enabled_user_time_punch_for_current_date")|load_all_records()|to_json}}',
            parallel_count=10,
            execution_timeout=timedelta(days=14),
            conf=lambda item:{
                **item,
                "lookuptable":rail.result("ascendmaterials_log_lookup_table"),
                "parentecid": rail.render_template('{{ecid()}}'),
                "shifturi":custom_methods.get_user_shifturi(item["useruri"]),
                "breakuri":custom_methods.get_breakuri(item["useruri"]),
                "starthour":rail.find_first_by_attr_and_get_attr(
                            rail.result("get_bulk_shift_schedule_details"), "uri",
                            custom_methods.get_user_shifturi(item["useruri"]),"starthour"),
                "startmin":rail.find_first_by_attr_and_get_attr(
                            rail.result("get_bulk_shift_schedule_details"), "uri",
                            custom_methods.get_user_shifturi(item["useruri"]),"startmin"),
                "duration":rail.find_first_by_attr_and_get_attr(
                            rail.result("get_bulk_shift_schedule_details"), "uri",
                            custom_methods.get_user_shifturi(item["useruri"]),"duration"),
            }
        )

        if_users_with_punch_entry_no_break = rail.IfOperator(
            task_id="if_users_with_punch_entry_no_break",
            test='{{result("query_enabled_user_time_punch_for_current_date", "length") > 0)}}' and
                    '{{result("ascendmaterials_log_lookup_table")|load_all_records()|length == 0}}' ,
            yes_task="fail_dagrun",
            no_task="write_logs_to_csv"
        )

        write_logs_to_csv = rail.WriteCSVFileOperator(
            task_id="write_logs_to_csv",
            source='{{result("ascendmaterials_log_lookup_table")}}',
            header=["User", "Date", "Status", "Details", "JobID"],
            row=lambda item: [
                item["properties"]["User|date"].split("|")[0],
                item["properties"]["User|date"].split("|")[1],
                item["properties"]["Status"],
                item["properties"]["Details"],
                item["properties"]["Jobid"]
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name='{{result("write_logs_to_csv")}}',
            output_file_name="Automatic_Break_logs_"+'{{current_time_in_specified_tz(fmt="%d%m%YT%H%M%S", tz="US/Central")}}'+".csv",
            expires_in_seconds=7*24*60*60
        )

        send_success_email = rail.EmailOperator(
            task_id="send_success_email",
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{get_company_key()}}' + "| Automation to add break hours - completed successfully " +
                    '{{current_time_in_specified_tz(fmt="%d/%m/%YT%H:%M:%S", tz="US/Central")}}',
            html_content="templates/success_mail.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{get_error_message()| is_truthy}}',
            yes_task="fail_dagrun"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{get_error_message()}}'
        )

        ascendmaterials_log_lookup_table >> get_user_report_details >> run_user_report_entry >> run_user_report_exit >>\
        if_report_run_failed >> rail.Label("Yes") >> fail_dagrun
        if_report_run_failed >> rail.Label("No") >> load_user_report_csv >> create_enabled_users_collection >>\
        get_shift_details_for_current_date >>\
        if_shift_details_present_for_current_date >> rail.Label("No") >> log_to_sumo
        if_shift_details_present_for_current_date >> rail.Label("Yes") >> create_shift_details_collection >>\
        query_enabled_users_with_shift_schedule >> query_all_current_date_shifts >>\
        get_bulk_shift_schedule_details >>\
        get_time_punch_entry_data_for_current_date >>\
        if_time_punches_present_for_current_date >> rail.Label("No") >> log_to_sumo
        if_time_punches_present_for_current_date >> rail.Label("Yes") >> create_time_punch_entry_collection >>\
        query_enabled_user_time_punch_for_current_date >> process_break_punch_for_users >>\
        if_users_with_punch_entry_no_break >> rail.Label("Yes") >> fail_dagrun
        if_users_with_punch_entry_no_break >> rail.Label("No") >>\
        write_logs_to_csv >> generate_download_link >> send_success_email >>\
        log_to_sumo >> can_fail_dag >> fail_dagrun
    return dag

rail.for_each_instance(create_main_airflow_dag)
