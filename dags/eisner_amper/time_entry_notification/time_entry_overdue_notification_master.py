from datetime import  datetime as dt, timedelta
from pendulum import datetime
import rail
from eisner_amper.time_entry_notification.utils import request_payload
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= f"eisneramper_time_entry_notification_master_{config.instance}",
        description= f"EisnerAmper time entry notification master {config.instance}",
        company_key= config.company_key,
        start_date=datetime(2022, 4, 1, tz=config.timezone),
        schedule_interval=config.schedule_interval,
        replicon_conn_id = config.replicon_conn_id,
        max_active_runs = config.max_active_runs
    ) as dag :


        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable= lambda: str(dt.now().strftime("%Y-%m-%dT%H-%M-%S"))
        )

        expected_report_columns = config.report_columns

        get_specific_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_specific_report_details',
            report_name=config.report_name,
        )

        load_report = rail.run_report(
            group_id='load_report',
            report_params=lambda: request_payload.get_run_report_payload(config.duration_days)
        )

        has_data = rail.IfOperator(
            task_id  = "has_data",
            test = '{{ result("load_report.get_report_result", "has_data") }}',
            yes_task= 'report_has_expected_columns',
            no_task= 'finish_export'
        )

        finish_export = rail.EmptyOperator(
           task_id= 'finish_export'
        )

        report_has_expected_columns = rail.IfOperator(
            task_id = "report_has_expected_columns",
            #pylint: disable=consider-using-f-string line-too-long
            test="{{ result('load_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_report_columns,
            no_task='fail_invalid_report_colums',
            yes_task='report_payload_to_csv',
        )

        report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id = "report_payload_to_csv",
            document= '{{result("load_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        report_data_collection = rail.CreateCollectionOperator(
            task_id = "report_data_collection",
            source= '{{result("report_payload_to_csv")}}'
        )

        fail_invalid_report_colums = rail.FailOperator(
            task_id = "fail_invalid_report_colums",
            message="Base report column does not match"
        )

        expected_timesheet_report_columns = config.timesheet_report_columns

        get_timesheet_specific_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timesheet_specific_report_details',
            report_name=config.timesheet_report_name,
        )

        load_timesheet_report = rail.run_report(
            group_id='load_timesheet_report',
            report_params=lambda: request_payload.get_run_timesheet_report_payload(config.duration_days)
        )

        has_timehseet_data = rail.IfOperator(
            task_id  = "has_timehseet_data",
            test = '{{result("load_timesheet_report.get_report_result", "has_data")}}',
            yes_task= 'timesheet_report_has_expected_columns',
            no_task= 'finish_timesheet_export'
        )

        finish_timesheet_export = rail.EmptyOperator(
           task_id= 'finish_timesheet_export'
        )

        timesheet_report_has_expected_columns = rail.IfOperator(
            task_id = "timesheet_report_has_expected_columns",
            #pylint: disable=consider-using-f-string line-too-long
            test="{{ result('load_timesheet_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_timesheet_report_columns,
            no_task='fail_invalid_timesheet_report_colums',
            yes_task='timesheet_report_payload_to_csv',
        )

        timesheet_report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id = "timesheet_report_payload_to_csv",
            document= '{{result("load_timesheet_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        timesheet_report_data_collection = rail.CreateCollectionOperator(
            task_id = "timesheet_report_data_collection",
            source= '{{result("timesheet_report_payload_to_csv")}}'
        )

        fail_invalid_timesheet_report_colums = rail.FailOperator(
            task_id = "fail_invalid_timesheet_report_colums",
            message="Base report column does not match"
        )


        query_valid_users_data = rail.QueryCollectionOperator(
            task_id = "query_valid_users_data",
            query= """SELECT report_data_collection.User_First_Name,report_data_collection.User_Last_Name,
            report_data_collection.User_Email,
            CAST(report_data_collection.Date AS DATE) AS date,
            report_data_collection.Date,
            CAST(report_data_collection.Scheduled_Work_Hours AS INTEGER) AS Scheduled_Work_Hours,
            CAST(report_data_collection.Time_Off_Hours AS INTEGER) AS Time_Off_Hours,
            CAST(report_data_collection.Total_Actual_Hours AS INTEGER) AS Total_Actual_Hours,report_data_collection.User_Status,
            report_data_collection.useruri,timesheet_report_data_collection.Timesheet_Period,CAST(timesheet_report_data_collection.Entry_Date AS DATE) AS Entry_Date,
            timesheet_report_data_collection.useruri 
            FROM report_data_collection,timesheet_report_data_collection 
            WHERE  date = Entry_Date AND report_data_collection.useruri = timesheet_report_data_collection.useruri 
            AND (CAST(Total_Actual_Hours as decimal) < CAST(Scheduled_Work_Hours as decimal))
            AND Scheduled_Work_Hours != 0 AND (Time_Off_Hours = 0 OR (Time_Off_Hours != 0 
            AND CAST(Time_Off_Hours as decimal) < CAST(Scheduled_Work_Hours as decimal)))"""
        )

        valid_users_data_collection = rail.CreateCollectionOperator(
            task_id = "valid_users_data_collection",
            source= '{{result("query_valid_users_data")}}'
        )

        query_unique_user_uri=rail.QueryCollectionOperator(
            task_id = "query_unique_user_uri",
            query= "SELECT DISTINCT useruri, User_Last_Name,User_First_Name FROM valid_users_data_collection"
        )

        process_notofication_for_each_user = rail.trigger_parallel_dagrun(
            task_id='process_notofication_for_each_user',
            items=lambda: rail.result('query_unique_user_uri'),
            trigger_dag_id=f'eisneramper_missing_time_entry_notification_user_vise_child_{config.instance}',
            parallel_count=config.max_parallel_run,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.process_process_notofication_for_user_conf,
        )

        #pylint: disable=consider-using-f-string line-too-long
        process_start_time >> get_specific_report_details
        get_specific_report_details >> load_report >> has_data
        has_data >> rail.Label("No") >> finish_export
        has_data >> rail.Label("Yes") >> report_has_expected_columns
        report_has_expected_columns >> rail.Label("Yes") >> report_payload_to_csv >> report_data_collection
        report_has_expected_columns >> rail.Label("No") >> fail_invalid_report_colums

        report_data_collection >> get_timesheet_specific_report_details
        get_timesheet_specific_report_details >> load_timesheet_report >> has_timehseet_data
        has_timehseet_data >> rail.Label("No") >> finish_timesheet_export
        has_timehseet_data >> rail.Label("Yes") >> timesheet_report_has_expected_columns
        timesheet_report_has_expected_columns >> rail.Label("Yes") >> timesheet_report_payload_to_csv >> timesheet_report_data_collection >> query_valid_users_data
        query_valid_users_data >> valid_users_data_collection >> query_unique_user_uri >> process_notofication_for_each_user
        timesheet_report_has_expected_columns >> rail.Label("No") >> fail_invalid_timesheet_report_colums

    return dag

rail.for_each_instance(create_main_dag)
