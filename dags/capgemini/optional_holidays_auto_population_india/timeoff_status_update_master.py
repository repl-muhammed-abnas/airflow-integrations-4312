from datetime import timedelta
from pendulum import datetime
from capgemini.optional_holidays_auto_population_india.utils import request_payload
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'capgemini_auto_population_of_optional_holidays_india_timeoff_status_change_master_{config.instance}',
        description=f'Capgemini Auto Population of Optional Holidays India Timeoff status change Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 9, 1),
        schedule_interval=config.timeoff_status_update_schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            'retries': 0
        },
    ) as dag:

        get_optional_holiday_status_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_optional_holiday_status_report_details',
            report_name=config.optional_holiday_status_report
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params=request_payload.optional_holiday_status_report_payload
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task='fail_report_generation',
            no_task='report_has_data'
        )

        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{result('run_report.get_report_result','has_data')}}",
            yes_task='is_report_has_expected_columns',
            no_task='dagrun_log_to_sumo'
        )

        is_report_has_expected_columns = rail.IfOperator(
            task_id='is_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_status_report_columns,
            yes_task='load_csv',
            no_task='fail_no_expected_columns',
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id='load_csv',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}"
        )

        optional_holiday_status_collection = rail.CreateCollectionOperator(
            task_id='optional_holiday_status_collection',
            source='{{ result("load_csv") }}',
            columns={
                "User Name": "username",
                "Time Off Type": "timeoff_type",
                "Time Off Days": "timeoff_days",
                "Approval Status": "approval_status",
                "Booking Start Date": "booking_start_date",
                "timeoffuri": "timeoff_uri",
                "Submitted By Employee ID": "submitted_by_employee_id",
                "Submitted By Employee Name": "submitted_by_employee_name"
            },
            name='optional_holiday_status_data'
        )

        query_waiting_for_approval_timeoffs = rail.QueryCollectionOperator(
            task_id='query_waiting_for_approval_timeoffs',
            query="""SELECT * FROM optional_holiday_status_data WHERE approval_status = 'Waiting for Approval'
                    AND submitted_by_employee_name = 'optional holiday, integration'"""
        )

        query_not_submitted_timeoffs = rail.QueryCollectionOperator(
            task_id='query_not_submitted_timeoffs',
            query="""SELECT * FROM optional_holiday_status_data WHERE approval_status = 'Not Submitted'
                    AND NULLIF(timeoff_uri, '') IS NOT NULL"""
        )

        check_waiting_timeoffs_exists = rail.IfOperator(
            task_id='check_waiting_timeoffs_exists',
            test='{{ result("query_waiting_for_approval_timeoffs", "length") > 0 }}',
            yes_task='process_timeoffs_approval_start',
            no_task='check_not_submitted_timeoffs_exists'
        )

        process_timeoffs_approval_start = rail.EmptyOperator(
            task_id='process_timeoffs_approval_start'
        )

        trigger_approval_child = rail.trigger_parallel_dagrun(
            task_id='trigger_approval_child',
            parallel_count=config.approve_parallel_count,
            items='{{ result("query_waiting_for_approval_timeoffs")}}',
            trigger_dag_id=f'capgemini_book_optional_holiday_approve_timeoff_child_{config.instance}',
            conf={
                "timeoff_uri": '{{ item.timeoff_uri }}'
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        check_not_submitted_timeoffs_exists = rail.IfOperator(
            task_id='check_not_submitted_timeoffs_exists',
            test='{{ result("query_not_submitted_timeoffs", "length") > 0 }}',
            yes_task='process_timeoffs_delete_start'
        )

        process_timeoffs_delete_start = rail.EmptyOperator(
            task_id='process_timeoffs_delete_start'
        )

        trigger_delete_not_submitted_timeoffs_child = rail.trigger_parallel_dagrun(
            task_id='trigger_delete_not_submitted_timeoffs_child',
            parallel_count=config.delete_parallel_count,
            items='{{ result("query_not_submitted_timeoffs")}}',
            trigger_dag_id=f'capgemini_book_optional_holiday_delete_timeoff_child_{config.instance}',
            conf={
                "timeoff_uri": '{{ item.timeoff_uri }}'
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        get_optional_holiday_status_report_details >> run_report_group_entry

        run_report_group_exit >> is_report_failed >> rail.Label("Yes") >> fail_report_generation >> dagrun_log_to_sumo
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns
        report_has_data >> rail.Label("No") >> dagrun_log_to_sumo

        is_report_has_expected_columns >> rail.Label("Yes") >> load_csv
        is_report_has_expected_columns >> rail.Label("No") >> fail_no_expected_columns >> dagrun_log_to_sumo

        load_csv >> optional_holiday_status_collection >> query_waiting_for_approval_timeoffs \
            >> query_not_submitted_timeoffs >> check_waiting_timeoffs_exists
        check_waiting_timeoffs_exists >> rail.Label("Yes") >> process_timeoffs_approval_start \
            >> trigger_approval_child >> check_not_submitted_timeoffs_exists
        check_waiting_timeoffs_exists >> rail.Label("No") >> check_not_submitted_timeoffs_exists
        check_not_submitted_timeoffs_exists >> rail.Label("Yes") >> process_timeoffs_delete_start \
            >> trigger_delete_not_submitted_timeoffs_child \
                >> dagrun_log_to_sumo


    return dag

rail.for_each_instance(create_dag)
