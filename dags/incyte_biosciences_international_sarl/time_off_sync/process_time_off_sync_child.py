from incyte_biosciences_international_sarl.time_off_sync.utils import custom_methods, request_methods
import rail
null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"incyte_biosciences_international_sarl_time_off_sync_child_{config.instance}",
        description="incyte time off sync add and delete time off",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run")
        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                    "users": [
                        {
                            "uri": null,
                            "loginName": null,
                            "employeeId": '{{dag_run.conf.employee_id}}',
                            "parameterCorrelationId": null
                        }
                    ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: {"uri":response[0]["userDetails"]["uri"] ,
                                           "email": response[0]["userDetails"]["emailAddress"] if response[0]["userDetails"]["emailAddress"] else ""
                                           }if response and "userDetails" in response[0] else ""
        )

        if_user_present_in_replicon = rail.IfOperator(
            task_id="if_user_present_in_replicon",
            test='{{result("get_user_details")| is_truthy}}',
            yes_task="get_time_off_details_based_on_uniqueid",
            no_task="write_log_user_not_present"
        )

        def get_details_of_uniqueid(response,dag_run):
            return list(filter (lambda i: i["uniqueid"] == dag_run.conf["peoplesoft_unique_id"],
                list(map(lambda i: {
                    "uri": i["cells"][0]["uri"],
                    "uniqueid": i["cells"][2]["textValue"],
                    "startdate": i["cells"][3]["textValue"],
                }, response["rows"])))
            ) if response else null

        get_time_off_details_based_on_uniqueid = rail.RepliconServiceOperator(
            task_id="get_time_off_details_based_on_uniqueid",
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_methods.get_time_off_details_for_uniqueid,
            data_handler=get_details_of_uniqueid
        )

        if_unique_id_in_replicon = rail.IfOperator(
            task_id="if_unique_id_in_replicon",
            test=lambda dag_run: bool(rail.result(
                "get_time_off_details_based_on_uniqueid") and len(rail.result(
                "get_time_off_details_based_on_uniqueid")) > 0 and dag_run.conf["status"] == "A"),
            yes_task="write_log_uniqueid_present",
            no_task="if_unique_id_not_present_for_deletion",
        )

        if_unique_id_not_present_for_deletion = rail.IfOperator(
            task_id="if_unique_id_not_present_for_deletion",
            test=lambda dag_run: bool(not rail.result(
                "get_time_off_details_based_on_uniqueid") and dag_run.conf["status"] == "C"),
            yes_task="write_no_booking_to_delete_log",
            no_task="if_time_off_type_present_in_replicon",
        )

        write_no_booking_to_delete_log = rail.WriteLogOperator(
            task_id="write_no_booking_to_delete_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "time_off_type": dag_run.conf["time_off_type"],
                "start_date": dag_run.conf["start_date"],
                "end_date": dag_run.conf["end_date"],
                "unique_id": dag_run.conf["peoplesoft_unique_id"],
                "time_off_status": dag_run.conf["status"],
                "status": "Exception",
                "details": "No booking to delete"
            }
        )

        write_log_uniqueid_present = rail.WriteLogOperator(
            task_id="write_log_uniqueid_present",
            log='{{dag_run.conf.lookuptable}}',
            message="Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "time_off_type": dag_run.conf["time_off_type"],
                "start_date": dag_run.conf["start_date"],
                "end_date": dag_run.conf["end_date"],
                "unique_id": dag_run.conf["peoplesoft_unique_id"],
                "time_off_status": dag_run.conf["status"],
                "status": "Exception",
                "email": rail.result("get_user_details")["email"],
                "details": "People soft unique id already present in Replicon."
            }
        )

        write_log_user_not_present = rail.WriteLogOperator(
            task_id="write_log_user_not_present",
            log='{{dag_run.conf.lookuptable}}',
            message="Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "time_off_type": dag_run.conf["time_off_type"],
                "start_date": dag_run.conf["start_date"],
                "end_date": dag_run.conf["end_date"],
                "unique_id": dag_run.conf["peoplesoft_unique_id"],
                "time_off_status": dag_run.conf["status"],
                "status": "Exception",
                "email": "",
                "details": "User not present in Replicon."
            }
        )

        if_time_off_type_present_in_replicon = rail.IfOperator(
            task_id="if_time_off_type_present_in_replicon",
            test='{{dag_run.conf.time_off_type_uri | is_truthy}}',
            yes_task="if_start_date_less_than_end_date",
            no_task="write_log_time_off_not_present"
        )

        write_log_time_off_not_present = rail.WriteLogOperator(
            task_id="write_log_time_off_not_present",
            log='{{dag_run.conf.lookuptable}}',
            message="Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "time_off_type": dag_run.conf["time_off_type"],
                "start_date": dag_run.conf["start_date"],
                "end_date": dag_run.conf["end_date"],
                "unique_id": dag_run.conf["peoplesoft_unique_id"],
                "time_off_status": dag_run.conf["status"],
                "status": "Exception",
                "details": "Timeoff type not present in Replicon."
            }
        )

        if_start_date_less_than_end_date = rail.IfOperator(
            task_id="if_start_date_less_than_end_date",
            test=lambda dag_run: bool(
                custom_methods.check_valid_dates(dag_run)),
            yes_task="get_time_off_applicable_to_user",
            no_task="write_log_invalid_dates"
        )

        write_log_invalid_dates = rail.WriteLogOperator(
            task_id="write_log_invalid_dates",
            log='{{dag_run.conf.lookuptable}}',
            message="Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "time_off_type": dag_run.conf["time_off_type"],
                "start_date": dag_run.conf["start_date"],
                "end_date": dag_run.conf["end_date"],
                "unique_id": dag_run.conf["peoplesoft_unique_id"],
                "time_off_status": dag_run.conf["status"],
                "status": "Exception",
                "email": rail.result("get_user_details")["email"],
                "details": "Start date is greater than enddate."
            }
        )

        get_time_off_applicable_to_user = rail.RepliconServiceOperator(
            task_id="get_time_off_applicable_to_user",
            endpoint="/services/TimeOffService1.svc/GetEligibleTimeOffTypesForBookingTimeOff",
            data=lambda: {
                "userUri": rail.result("get_user_details")["uri"]
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response,
                "displayText",
                dag_run.conf["time_off_type"],
                "uri"
            )
        )

        if_time_off_applicable_to_user = rail.IfOperator(
            task_id="if_time_off_applicable_to_user",
            test='{{result("get_time_off_applicable_to_user") |is_truthy}}',
            yes_task="if_invalid_duration",
            no_task="write_log_time_off_not_applicable"
        )

        write_log_time_off_not_applicable = rail.WriteLogOperator(
            task_id="write_log_time_off_not_applicable",
            log='{{dag_run.conf.lookuptable}}',
            message="Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "time_off_type": dag_run.conf["time_off_type"],
                "start_date": dag_run.conf["start_date"],
                "end_date": dag_run.conf["end_date"],
                "unique_id": dag_run.conf["peoplesoft_unique_id"],
                "time_off_status": dag_run.conf["status"],
                "status": "Exception",
                "email": rail.result("get_user_details")["email"],
                "details": "Time off type not applicable to user."
            }
        )

        if_invalid_duration = rail.IfOperator(
            task_id="if_invalid_duration",
            test=lambda dag_run:bool(float(dag_run.conf["duration"]) > 1 and float(dag_run.conf["duration"]) % 1 > 0\
            and (dag_run.conf["duration_type"] == "D" or
                 dag_run.conf["duration_type"] == "H" and dag_run.conf["start_date"] != dag_run.conf["end_date"])),
            yes_task="write_invalid_duration_log",
            no_task="if_valid_action"
        )

        write_invalid_duration_log = rail.WriteLogOperator(
            task_id="write_invalid_duration_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "time_off_type": dag_run.conf["time_off_type"],
                "start_date": dag_run.conf["start_date"],
                "end_date": dag_run.conf["end_date"],
                "unique_id": dag_run.conf["peoplesoft_unique_id"],
                "time_off_status": dag_run.conf["status"],
                "status": "Exception",
                "email": rail.result("get_user_details")["email"],
                "details": "Invalid duration when duration type is D."
            }
        )

        if_valid_action = rail.IfOperator(
            task_id="if_valid_action",
            test='{{dag_run.conf.status == "C" or dag_run.conf.status == "A"}}',
            yes_task="create_time_off_date_list",
            no_task="write_invalid_action_log"
        )

        write_invalid_action_log = rail.WriteLogOperator(
            task_id="write_invalid_action_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "time_off_type": dag_run.conf["time_off_type"],
                "start_date": dag_run.conf["start_date"],
                "end_date": dag_run.conf["end_date"],
                "unique_id": dag_run.conf["peoplesoft_unique_id"],
                "time_off_status": dag_run.conf["status"],
                "status": "Exception",
                "email": rail.result("get_user_details")["email"],
                "details": "Invalid action " + dag_run.conf["status"]
            }
        )

        create_time_off_date_list = rail.PythonOperator(
            task_id="create_time_off_date_list",
            python_callable=custom_methods.get_valid_multiple_dates
        )

        if_valid_days = rail.IfOperator(
            task_id="if_valid_days",
            test=lambda: bool(
                len(rail.result("create_time_off_date_list")) > 0),
            yes_task="if_time_off_add",
            no_task="write_novalid_days_log"
        )

        write_novalid_days_log = rail.WriteLogOperator(
            task_id="write_novalid_days_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "time_off_type": dag_run.conf["time_off_type"],
                "start_date": dag_run.conf["start_date"],
                "end_date": dag_run.conf["end_date"],
                "unique_id": dag_run.conf["peoplesoft_unique_id"],
                "time_off_status": dag_run.conf["status"],
                "status": "Exception",
                "email": rail.result("get_user_details")["email"],
                "details": "No valid working days " + dag_run.conf["status"]
            }
        )

        if_time_off_add = rail.IfOperator(
            task_id="if_time_off_add",
            test='{{dag_run.conf.status == "A"}}',
            yes_task="start_add_time_off_booking",
            no_task="start_delete_time_off_booking"
        )

        start_add_time_off_booking = rail.EmptyOperator(
            task_id="start_add_time_off_booking")

        add_time_off_booking = rail.TriggerDagRunOperator(
            task_id="add_time_off_booking",
            trigger_dag_id=f"incyte_biosciences_international_sarl_time_off_sync_add_time_off_child_{config.instance}",
            conf=lambda dag_run: {
                **dag_run.conf,
                "useruri": rail.result("get_user_details")["uri"],
                "email": rail.result("get_user_details")["email"],
                "lookuptable": dag_run.conf["lookuptable"]
            },
            wait_for_completion=True
        )

        start_delete_time_off_booking = rail.EmptyOperator(
            task_id="start_delete_time_off_booking")

        delete_timeoff_booking = rail.TriggerDagRunOperator(
            task_id="delete_timeoff_booking",
            trigger_dag_id=f"incyte_biosciences_international_sarl_time_off_sync_delete_time_off_child_{config.instance}",
            conf=lambda dag_run:{
                **dag_run.conf,
                "useruri": rail.result("get_user_details")["uri"],
                "email": rail.result("get_user_details")["email"],
                "lookuptable": dag_run.conf["lookuptable"]
            },
            wait_for_completion=True
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{dag_run.conf.lookuptable}}',
            severity="Error",
            message="Time off sync failed",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "time_off_type": dag_run.conf["time_off_type"],
                "start_date": dag_run.conf["start_date"],
                "end_date": dag_run.conf["end_date"],
                "unique_id": dag_run.conf["peoplesoft_unique_id"],
                "time_off_status": dag_run.conf["status"],
                "status": "Failed",
                "email": "",
                "details": "Time off sync failed"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            trigger_rule="all_done",
            sumo_conn_id='sumologic-dagrunlogger'
        )

        get_user_details >> if_user_present_in_replicon >> rail.Label("Yes") >>\
        get_time_off_details_based_on_uniqueid >>\
        if_unique_id_in_replicon >> rail.Label("No") >>\
        if_unique_id_not_present_for_deletion >> rail.Label("Yes") >> write_no_booking_to_delete_log >> catch_and_log_errors
        if_unique_id_not_present_for_deletion >> rail.Label("No") >>\
        if_time_off_type_present_in_replicon >> rail.Label("Yes") >>\
        if_start_date_less_than_end_date >> rail.Label("Yes") >>\
        get_time_off_applicable_to_user >>\
        if_time_off_applicable_to_user >> rail.Label("Yes") >>\
        if_invalid_duration >> rail.Label("No") >>\
        if_valid_action >> rail.Label("Yes") >>\
        create_time_off_date_list >>\
        if_valid_days >> rail.Label("Yes") >>\
        if_time_off_add >> rail.Label("Yes") >>\
        start_add_time_off_booking >>\
        add_time_off_booking >> catch_and_log_errors
        if_time_off_add >> rail.Label("No") >>\
            start_delete_time_off_booking >>\
            delete_timeoff_booking >> catch_and_log_errors
        if_valid_action >> rail.Label("No") >>\
            write_invalid_action_log >> catch_and_log_errors
        if_invalid_duration >> rail.Label("Yes") >>\
            write_invalid_duration_log >> catch_and_log_errors
        if_time_off_applicable_to_user >> rail.Label("No") >>\
            write_log_time_off_not_applicable >> catch_and_log_errors
        if_start_date_less_than_end_date >> rail.Label("No") >>\
            write_log_invalid_dates >> catch_and_log_errors
        if_time_off_type_present_in_replicon >> rail.Label("No") >>\
            write_log_time_off_not_present >> catch_and_log_errors
        if_unique_id_in_replicon >> rail.Label("Yes") >>\
            write_log_uniqueid_present >> catch_and_log_errors
        if_user_present_in_replicon >> rail.Label("No") >>\
            write_log_user_not_present >> catch_and_log_errors
        if_valid_days >> rail.Label("No") >>\
            write_novalid_days_log >> catch_and_log_errors >> log_to_sumo
        return dag


rail.for_each_instance(create_child_dag)
