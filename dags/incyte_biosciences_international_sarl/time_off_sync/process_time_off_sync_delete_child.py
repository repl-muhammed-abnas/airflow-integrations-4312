from datetime import datetime
from incyte_biosciences_international_sarl.time_off_sync.utils import request_methods, custom_methods
import rail
null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"incyte_biosciences_international_sarl_time_off_sync_delete_time_off_child_{config.instance}",
        description="incyte timeoff add time off",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        def get_details_of_uniqueid(response,dag_run):
            start_date = datetime.strftime(datetime.strptime(dag_run.conf["start_date"], "%d/%m/%Y"), "%d/%m/%Y")
            return rail.find_first_by_attr_and_get_attr(list(filter (lambda i: i["uniqueid"] == dag_run.conf["peoplesoft_unique_id"],
                list(map(lambda i: {
                    "uri": i["cells"][0]["uri"],
                    "uniqueid": i["cells"][2]["textValue"],
                    "startdate": i["cells"][3]["textValue"],
                }, response["rows"])))
            ),
            "startdate",
            start_date,
            "uri"
            ) if response else null

        get_time_off_details_based_on_uniqueid = rail.RepliconServiceOperator(
            task_id="get_time_off_details_based_on_uniqueid",
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=request_methods.get_time_off_details_for_uniqueid,
            data_handler=get_details_of_uniqueid
        )

        if_booking_present = rail.IfOperator(
            task_id="if_booking_present",
            test=lambda: bool(rail.result(
                "get_time_off_details_based_on_uniqueid")),
            yes_task="get_time_sheet_periods_for_user",
            no_task="write_no_booking_to_delete_log"
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
        
        get_time_sheet_periods_for_user = rail.RepliconServiceOperator(
            task_id="get_time_sheet_periods_for_user",
            endpoint="/services/TimesheetPeriodService1.svc/GetTimesheetPeriodsForUser",
            data=lambda dag_run:{
                "userUri": dag_run.conf["useruri"],
                "dateRange": {
                    "startDate": rail.parse_date(dag_run.conf["start_date"],"%d/%m/%Y"),
                    "endDate": rail.parse_date(dag_run.conf["end_date"],"%d/%m/%Y"),
                }
            },
            data_handler=custom_methods.get_time_sheet_periods
        )

        if_time_sheet_periods = rail.IfOperator(
            task_id="if_time_sheet_periods",
            test='{{result("get_time_sheet_periods_for_user") | length > 0}}',
            yes_task="start_time_sheet_period",
            no_task="end_time_sheet_period_check"
        )

        start_time_sheet_period = rail.EmptyOperator(task_id="start_time_sheet_period")

        for_each_time_sheet_period = rail.ForEachOperator(
            task_id="for_each_time_sheet_period",
            items=lambda:rail.result("get_time_sheet_periods_for_user"),
            start_task="get_time_sheet_for_start_date",
            end_task="end_time_sheet_period_check"
        )

        get_time_sheet_for_start_date = rail.RepliconServiceOperator(
            task_id="get_time_sheet_for_start_date",
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run:{
                    "userUri": dag_run.conf["useruri"],
                    "date": rail.parse_date(rail.result("for_each_time_sheet_period").split("-")[0].strip(), "%d/%m/%Y"),
                    "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
                },
            data_handler=lambda response: response["timesheet"]["uri"] if response else null
        )

        if_timesheet_template_present = rail.IfOperator(
            task_id="if_timesheet_template_present",
            test=lambda:bool(rail.result("get_time_sheet_for_start_date")),
            yes_task="get_time_sheet_status",
            no_task="write_time_sheet_not_present_log"
        )

        write_time_sheet_not_present_log = rail.WriteLogOperator(
            task_id="write_time_sheet_not_present_log",
            log="{{dag_run.conf.lookuptable}}",
            message="Time sheet not present for timeoff booking",
            severity="Exception",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "time_off_type": dag_run.conf["time_off_type"],
                "start_date": dag_run.conf["start_date"],
                "end_date": dag_run.conf["end_date"],
                "unique_id": dag_run.conf["peoplesoft_unique_id"],
                "time_off_status": dag_run.conf["status"],
                "status": "Exception",
                "email": dag_run.conf["email"],
                "details": "Time sheet not present for timeoff booking"
            }
        )

        get_time_sheet_status = rail.RepliconServiceOperator(
            task_id="get_time_sheet_status",
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
                    "timesheetUri": '{{result("get_time_sheet_for_start_date")}}'
                },
            data_handler=lambda response:response["statusUri"].split(":")[-1] if response else null
        )

        if_timesheet_reopened = rail.IfOperator(
            task_id="if_timesheet_reopened",
            test=lambda: bool(rail.result("get_time_sheet_status") != "open" ),
            yes_task="if_email_present",
            no_task="end_time_sheet_period_check"
        )

        if_email_present = rail.IfOperator(
            task_id="if_email_present",
            test='{{dag_run.conf.email | is_truthy}}',
            yes_task="write_time_sheet_reopened_log",
            no_task="write_notification_not_sent_log"
        )

        write_time_sheet_reopened_log = rail.WriteLogOperator(
            task_id="write_time_sheet_reopened_log",
            log="{{dag_run.conf.lookuptable}}",
            message="Time sheet reopened for timeoff booking",
            properties=lambda dag_run: {
                "first_name": dag_run.conf["first_name"],
                "employee_id": dag_run.conf["employee_id"],
                "time_off_type": dag_run.conf["time_off_type"],
                "start_date": dag_run.conf["start_date"],
                "end_date": dag_run.conf["end_date"],
                "unique_id": dag_run.conf["peoplesoft_unique_id"],
                "time_off_status": dag_run.conf["status"],
                "status": "Exception",
                "email": dag_run.conf["email"],
                "time_sheet_period": rail.result("for_each_time_sheet_period"),
                "details": "Time sheet reopened for timeoff booking"
            }
        )

        write_notification_not_sent_log = rail.WriteLogOperator(
            task_id="write_notification_not_sent_log",
            log="{{dag_run.conf.lookuptable}}",
            message="Exception",
            properties=lambda dag_run:{
                 "employee_id": dag_run.conf["employee_id"],
                "time_off_type": dag_run.conf["time_off_type"],
                "start_date": dag_run.conf["start_date"],
                "end_date": dag_run.conf["end_date"],
                "unique_id": dag_run.conf["peoplesoft_unique_id"],
                "time_off_status": dag_run.conf["status"],
                "status": "Exception",
                "email": dag_run.conf["email"],
                "details": "User not notified as email is blank or time sheet period not present"
            }
        )

        end_time_sheet_period_check = rail.EmptyOperator(
           task_id="end_time_sheet_period_check"
        )

        delete_time_off = rail.RepliconServiceOperator(
            task_id="delete_time_off",
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data=lambda: {
                "timeOffUri": rail.result("get_time_off_details_based_on_uniqueid")
            }
        )

        write_deletion_success_log = rail.WriteLogOperator(
            task_id="write_deletion_success_log",
            log='{{dag_run.conf.lookuptable}}',
            message="Time off deleted successfully",
            trigger_rule="all_success",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "time_off_type": dag_run.conf["time_off_type"],
                "start_date": dag_run.conf["start_date"],
                "end_date": dag_run.conf["end_date"],
                "unique_id": dag_run.conf["peoplesoft_unique_id"],
                "time_off_status": dag_run.conf["status"],
                "status": "Success",
                "details": "Time off deleted successfully"
            }
        )

        
        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log='{{dag_run.conf.lookuptable}}',
            severity="Error",
            message="Time off deletion failed",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf["employee_id"],
                "time_off_type": dag_run.conf["time_off_type"],
                "start_date": dag_run.conf["start_date"],
                "end_date": dag_run.conf["end_date"],
                "unique_id": dag_run.conf["peoplesoft_unique_id"],
                "time_off_status": dag_run.conf["status"],
                "status": "Failed",
                "details": "Time off deletion failed"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            trigger_rule="all_done",
            sumo_conn_id='sumologic-dagrunlogger'
        )

        get_time_off_details_based_on_uniqueid >>\
        if_booking_present >> rail.Label("Yes") >>\
        get_time_sheet_periods_for_user >> if_time_sheet_periods >> rail.Label("Yes") >>\
        start_time_sheet_period >> for_each_time_sheet_period >> end_time_sheet_period_check
        for_each_time_sheet_period >> get_time_sheet_for_start_date >>\
        if_timesheet_template_present >> rail.Label("Yes") >> get_time_sheet_status >>\
        if_timesheet_reopened >> rail.Label("Yes") >> if_email_present >> rail.Label("Yes") >>\
        write_time_sheet_reopened_log >> end_time_sheet_period_check
        if_email_present >> rail.Label("No") >> write_notification_not_sent_log >> end_time_sheet_period_check
        if_timesheet_reopened >> rail.Label("No") >> end_time_sheet_period_check
        if_timesheet_template_present >> rail.Label("No") >> write_time_sheet_not_present_log >>\
        end_time_sheet_period_check >>\
        delete_time_off >> write_deletion_success_log  >> catch_and_log_error >> log_to_sumo
        if_booking_present >> rail.Label("No") >>\
            write_no_booking_to_delete_log >> catch_and_log_error
        if_time_sheet_periods >> rail.Label("No") >> end_time_sheet_period_check

    return dag


rail.for_each_instance(create_child_dag)
