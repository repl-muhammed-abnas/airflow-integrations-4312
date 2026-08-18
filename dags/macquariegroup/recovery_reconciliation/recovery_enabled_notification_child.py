from datetime import timedelta
import rail
from macquariegroup.recovery_reconciliation.utils import request_payload
from macquariegroup.recovery_reconciliation.utils.custom_methods import generate_effective_date_callable, get_23rd_of_last_month, get_current_month_end_day
from macquariegroup.recovery_reconciliation.utils.data_handlers import get_holiday_date_list
from macquariegroup.recovery_reconciliation.tasks.send_logs_aleart_notification import get_send_logs


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"macquarie_recovery_reconciliation_recovery_enabled_notification_child_{config.instance}",
        description=f"Macquarie Recovery Reconciliation Update recovery Yes {config.instance}",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=10,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_conf")

        create_log = rail.CreateLogOperator(
            task_id="create_log"
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=config.custom_notification_base_report
        )

        run_report_start,  run_report_end = rail.run_report(
            group_id="generate_base_report",
            report_params=request_payload.get_report_parameters_for_timesheet_period_report
        )

        is_report_generation_failed = rail.IfOperator(
            task_id="is_report_generation_failed",
            test="{{ result('generate_base_report.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task="fail_report_generation_failed",
            no_task="report_has_data"
        )

        fail_report_generation_failed = rail.FailOperator(
            task_id="fail_report_generation_failed",
            message="{{ result('generate_base_report.get_report_result').reportGenerationResults[0].error }}"
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test=lambda: rail.result('generate_base_report.get_report_result')[
                'reportGenerationResults'][0]['payload'].startswith("No Data"),
            yes_task="fail_report_does_not_have_data",
            no_task="report_has_expected_columns"
        )

        fail_report_does_not_have_data = rail.FailOperator(
            task_id="fail_report_does_not_have_data",
            message="User Base report for recovery reconciliation import does not have contains any records"
        )
        # pylint: disable=line-too-long
        expected_report_columns = "User Name,Employee ID,Login Name,Timesheet End Date,Group (Current),Employee Type (Current),user_uri"
        # pylint: disable=consider-using-f-string
        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            test="{{ result('generate_base_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_report_columns,
            yes_task="load_report_data",
            no_task="fail_invalid_report_columns"
        )

        fail_invalid_report_columns = rail.FailOperator(
            task_id="fail_invalid_report_columns",
            message="Base report column does not match"
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id="load_report_data",
            document='{{result("generate_base_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        create_report_collection = rail.CreateCollectionOperator(
            task_id="create_report_collection",
            source="{{result('load_report_data')}}",
            columns={
                "User Name": "user_name",
                "Employee ID": "employee_id",
                "Login Name": "login_name",
                "Timesheet End Date":"timesheet_end_date",
                "Group (Current)":"groups",
                "Employee Type (Current)":"employee_type",
                "user_uri":"user_uri",
                },
            name="custom_notification_report_collection"
        )

        get_holiday_calender_australia = rail.RepliconServiceOperator(
            task_id="get_holiday_calender_australia",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, "displayText", config.australia_holiday_calender)
        )

        get_holidays_for_current_month = rail.RepliconServiceOperator(
            task_id="get_holidays_for_current_month",
            endpoint="/services/HolidayCalendarService2.svc/GetHolidaysInDateRange",
            data={
                "holidayCalendarUri": "{{result('get_holiday_calender_australia').uri}}",
                "dateRange": {
                    "startDate": get_23rd_of_last_month(),
                    "endDate": get_current_month_end_day()
                }
            },
            data_handler=get_holiday_date_list
        )

        generate_effective_date = rail.PythonOperator(
            task_id="generate_effective_date",
            python_callable=generate_effective_date_callable
        )

        trigger_send_email_for_each_user = rail.trigger_parallel_dagrun(
            task_id="trigger_send_email_for_each_user",
            trigger_dag_id=f"macquarie_recovery_reconciliation_send_recovery_enabled_emails_child_{config.instance}",
            parallel_count=10,
            items="{{result('create_report_collection')}}",
            execution_timeout=timedelta(days=14),
            conf=lambda item : {
                **{k : v if v is not None else '' for k,v in item.items()},
                **{
                    "custom_due_date": rail.result("generate_effective_date"),
                    "log": rail.result("create_log"),
                    "rmg_exception_message": rail.result("generate_effective_date")['rmg_exception_message'],
                    "fmg_exception_message": rail.result("generate_effective_date")['fmg_exception_message']
                   }}
        )

        send_logs_start, _ = get_send_logs(config)

        create_log >> get_report_details >> run_report_start
        run_report_end >> is_report_generation_failed >> rail.Label(
            "Yes") >> fail_report_generation_failed
        is_report_generation_failed >> rail.Label(
            "No") >> report_has_data >> rail.Label("No") >> fail_report_does_not_have_data
        report_has_data >> rail.Label(
            "Yes") >> report_has_expected_columns >> rail.Label("Yes") >> load_report_data
        report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_columns

        load_report_data >> create_report_collection >> get_holiday_calender_australia >> get_holidays_for_current_month >> generate_effective_date
        generate_effective_date >> trigger_send_email_for_each_user >> send_logs_start
        return dag


rail.for_each_instance(create_child_dag)
