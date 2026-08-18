from datetime import  datetime as dt, timedelta
from pendulum import datetime
import rail
from eisner_amper.time_entry_overdue_notification.utils import request_payload
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= f"eisneramper_time_entry_overdue_notification_master_{config.instance}",
        description= f"EisnerAmper time entry overdue notification master {config.instance}",
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

        # pylint: disable=line-too-long
        expected_report_columns = "User Name,userUri,Entry Date,Timesheet Period,Time Entry Approval Status,Notification,User First Name"

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
            test = '{{"No Data" not in result("load_report.get_report_result").reportGenerationResults[0].payload}}',
            yes_task= 'report_has_expected_columns',
            no_task= 'finish_export'
        )

        finish_export = rail.EmptyOperator(
           task_id= 'finish_export'
        )

        report_has_expected_columns = rail.IfOperator(
            task_id = "report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            # pylint: disable=line-too-long
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

        query_valid_users_data = rail.QueryCollectionOperator(
            task_id = "query_valid_users_data",
            query= "SELECT * FROM report_data_collection WHERE Time_Entry_Approval_Status = 'Not Submitted' AND Notification ='Yes'"
        )

        valid_users_data_collection = rail.CreateCollectionOperator(
            task_id = "valid_users_data_collection",
            source= '{{result("query_valid_users_data")}}'
        )

        query_unique_user_uri=rail.QueryCollectionOperator(
            task_id = "query_unique_user_uri",
            query= "SELECT DISTINCT userUri, User_Name,User_First_Name FROM valid_users_data_collection"
        )

        process_notofication_for_each_user = rail.trigger_parallel_dagrun(
            task_id='process_notofication_for_each_user',
            items=lambda: rail.result('query_unique_user_uri'),
            trigger_dag_id=f'eisneramper_time_entry_overdue_notification_user_vise_child_{config.instance}',
            parallel_count=25,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.process_process_notofication_for_user_conf,
        )

        # pylint: disable=line-too-long
        process_start_time >> get_specific_report_details
        get_specific_report_details >> load_report >> has_data
        has_data >> rail.Label("No") >> finish_export
        has_data >> rail.Label("Yes") >> report_has_expected_columns
        report_has_expected_columns >> rail.Label("Yes") >> report_payload_to_csv >> report_data_collection >> query_valid_users_data
        query_valid_users_data >> valid_users_data_collection >> query_unique_user_uri >> process_notofication_for_each_user
        report_has_expected_columns >> rail.Label("No") >> fail_invalid_report_colums

    return dag

rail.for_each_instance(create_main_dag)
