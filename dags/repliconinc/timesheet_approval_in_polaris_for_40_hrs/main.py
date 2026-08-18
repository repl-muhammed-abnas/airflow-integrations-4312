import rail
from repliconinc.timesheet_approval_in_polaris_for_40_hrs.utils import request_payload
from pendulum import datetime as dt


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag,
        description='Auto timesheet approval for all timeoff in polaris for 40 hours',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2022, 1, 1, tz=config.ist_timezone),
        max_active_runs=config.max_active_runs_master,
        schedule_interval=config.schedule_interval,
    ) as dag:

        get_timesheet_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timesheet_report_details',
            report_name=config.timesheet_report_all_timeoff,
        )

        genarate_timesheet_report = rail.run_report2(
            group_id='load_timsheet_report',
            report_params=lambda: request_payload.get_user_report_payload()
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("load_timsheet_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('load_timsheet_report.get_report_result').reportGenerationResults[0].error}}"
        )

        has_data = rail.IfOperator(
            task_id="has_data",
            test='{{ result("load_timsheet_report.get_report_result", "has_data") }}',
            yes_task='timesheet_report_payload_to_csv'
        )

        timesheet_report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="timesheet_report_payload_to_csv",
            document='{{result("load_timsheet_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        create_timesheet_collection = rail.CreateCollectionOperator(
            task_id='create_timesheet_collection',
            name='timsheet_data',
            source="{{result('timesheet_report_payload_to_csv')}}"
        )

        query_timesheet_records = rail.QueryCollectionOperator(
            task_id='query_timesheet_records',
            query="""SELECT * FROM timsheet_data WHERE Scheduled_Hrs__In_Period_ = Total_TimeOff_Hrs__In_Period_""",
        )

        process_approve_timesheet_records = rail.TriggerDagRunForEachItemOperator(
            task_id='process_approve_timesheet_records',
            trigger_dag_id=config.process_timesheet_approval_child_dag,
            items="{{ result('query_timesheet_records') }}",
            conf=lambda item: {
                **dict(item.items())
            }
        )

        wait_process_approve_timesheet_records = rail.WaitForDagRunsSensor(
            task_id="wait_process_approve_timesheet_records",
             dag_runs="{{result('process_approve_timesheet_records')}}"
        )


        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        get_timesheet_report_details >> genarate_timesheet_report >> is_report_failed >> rail.Label("Yes") >> fail_report_generation

        is_report_failed >> rail.Label("No") >> has_data >> timesheet_report_payload_to_csv >> create_timesheet_collection >> query_timesheet_records\
            >> process_approve_timesheet_records >> wait_process_approve_timesheet_records  >> log_to_sumo >> can_fail_dag >> fail_dagrun

        return dag


rail.for_each_instance(create_main_airflow_dag)
