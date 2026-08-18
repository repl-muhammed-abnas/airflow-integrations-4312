from datetime import datetime
from ascendmaterials.addbreakpunch import custom_methods, request_payload
import rail

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"ascendmaterials_add_break_punch_in_replicon_child_{config.instance}",
        description="ascend materials add break punch in replicon child",
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        company_key=config.company_key
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        query_for_user_time_punch_data = rail.QueryCollectionOperator(
            task_id="query_for_user_time_punch_data",
            query="""SELECT * FROM rawtimepunchdata WHERE useruri='{{dag_run.conf["useruri"]}}' ORDER BY datetime ASC"""
        )

        get_first_punch_record = rail.PythonOperator(
            task_id="get_first_punch_record",
            python_callable=lambda:rail.load_all_records(rail.result("query_for_user_time_punch_data"))[0]
        )

        if_first_punchaction_is_In = rail.IfOperator(
            task_id="if_first_punch_is_In",
            test='{{result("get_first_punch_record").punchaction == "In"}}',
            yes_task="if_punches_are_even",
            no_task="write_skipped_user_log"
        )

        write_skipped_user_log = rail.WriteLogOperator(
            task_id="write_skipped_user_log",
            log='{{dag_run.conf.lookuptable}}',
            message="No in punch found for the user",
            properties=lambda dag_run: {
                        "Jobid":dag_run.conf["parentecid"],
                        "User|date":rail.result("get_first_punch_record")["username"] + "|" +
                                    datetime.strftime(datetime.strptime(
                                    rail.result("get_first_punch_record")["datetime"], "%Y-%m-%d %H:%M:%S"), "%d/%m/%Y"),
                        "Status":"Skipped",
                        "Details":"No in punch found for the user"
                    }
        )

        if_punches_are_even = rail.IfOperator(
            task_id="if_punches_are_even",
            test='{{result("query_for_user_time_punch_data")|load_all_records()|length == 2}}',
            yes_task="if_shifturi_present_for_user",
            no_task="write_incomplete_punchpair_log",
        )

        write_incomplete_punchpair_log = rail.WriteLogOperator(
            task_id="write_incomplete_punchpair_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Incomplete Punch Pair",
            properties=lambda dag_run: {
                        "Jobid":dag_run.conf["parentecid"],
                        "User|date":rail.result("get_first_punch_record")["username"] + "|" +
                                    datetime.strftime(datetime.strptime(
                                    rail.result("get_first_punch_record")["datetime"], "%Y-%m-%d %H:%M:%S"), "%d/%m/%Y"),
                        "Status":"Skipped",
                        "Details":"Incomplete Punch Pair"
                    }
        )

        if_shifturi_present_for_user = rail.IfOperator(
            task_id="if_shifturi_present_for_user",
            test='{{dag_run.conf.shifturi|is_truthy}}',
            yes_task="if_breakuri_present_for_user",
            no_task="write_no_shift_assigned_log"
        )

        write_no_shift_assigned_log = rail.WriteLogOperator(
            task_id="write_no_shift_assigned_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Shift not assigned to user",
            properties=lambda dag_run: {
                        "Jobid":dag_run.conf["parentecid"],
                        "User|date":rail.result("get_first_punch_record")["username"] + "|" +
                                    datetime.strftime(datetime.strptime(
                                    rail.result("get_first_punch_record")["datetime"], "%Y-%m-%d %H:%M:%S"), "%d/%m/%Y"),
                        "Status":"Skipped",
                        "Details":"Shift not assigned to user"
                    }
        )

        if_breakuri_present_for_user = rail.IfOperator(
            task_id="if_breakuri_present_for_user",
            test='{{dag_run.conf.breakuri|is_truthy}}',
            yes_task="add_break_punch_for_user",
            no_task="write_no_breaks_configured_log"
        )

        write_no_breaks_configured_log = rail.WriteLogOperator(
            task_id="write_no_breaks_configured_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Breaks not configured in the assigned shift",
            properties=lambda dag_run: {
                        "Jobid":dag_run.conf["parentecid"],
                        "User|date":rail.result("get_first_punch_record")["username"] + "|" +
                                    datetime.strftime(datetime.strptime(
                                    rail.result("get_first_punch_record")["datetime"], "%Y-%m-%d %H:%M:%S"), "%d/%m/%Y"),
                        "Status":"Skipped",
                        "Details":"Breaks not configured in the assigned shift"
                    }
        )

        add_break_punch_for_user = rail.RepliconServiceOperator(
            task_id="add_break_punch_for_user",
            endpoint="/services/TimePunchService1.svc/BulkPutTimePunch4",
            data=request_payload.break_time_punch_entry_request
        )

        write_success_log = rail.WriteLogOperator(
            task_id="write_success_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Success",
            properties=custom_methods.get_success_log
        )

        query_for_user_time_punch_data >> get_first_punch_record >>\
        if_first_punchaction_is_In >> rail.Label("Yes") >>\
        if_punches_are_even >> rail.Label("Yes") >>\
        if_shifturi_present_for_user >> rail.Label("Yes") >>\
        if_breakuri_present_for_user >> rail.Label("Yes") >>\
        add_break_punch_for_user >> write_success_log
        if_first_punchaction_is_In >> rail.Label("No") >> write_skipped_user_log
        if_punches_are_even >> rail.Label("No") >> write_incomplete_punchpair_log
        if_shifturi_present_for_user >> rail.Label("No") >> write_no_shift_assigned_log
        if_breakuri_present_for_user >> rail.Label("No") >> write_no_breaks_configured_log

    return dag

rail.for_each_instance(create_child_dag)
