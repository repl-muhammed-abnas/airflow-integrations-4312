from datetime import timedelta
from frontdoorinc.timesheet_autopopulation.task.send_logs import get_send_logs
from frontdoorinc.timesheet_autopopulation.utils import request_payload
import rail
from pendulum import datetime as dt


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'frontdoorinc_timesheet_autopopulation_master_{config.instance}',
        description=f'FrontdoorInc Timesheet Auto Population{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2022, 12, 7, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
    ) as dag:

        get_population_script_uri = rail.RepliconServiceOperator(
            task_id="get_population_script_uri",
            endpoint="/services/TimesheetPopulationService1.svc/GetTimesheetPopulationScriptsAvailableForAssignmentToTimesheetPolicySets",
            data={},
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(response.json()['d'], "displayText",
                            config.time_population_script_name, "uri")
        )

        has_population_script = rail.IfOperator(
            task_id='has_population_script',
            test="{{ result('get_population_script_uri') | is_truthy }}",
            yes_task='get_report_details',
            no_task='fail_no_script'
        )

        fail_no_script = rail.FailOperator(
            task_id='fail_no_script',
            message='Timesheet auto population script is missing'
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id='run_report',
            report_params=request_payload.get_report_generate_batch_payload,
            replicon_conn_id=config.replicon_conn_id,
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='is_report_has_expected_columns',
            no_task='finish'
        )

        is_report_has_expected_columns = rail.IfOperator(
            task_id='is_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_report_columns,
            yes_task="load_timehseets_csv",
            no_task="fail_no_expected_columns",
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_timehseets_csv = rail.LoadCSVFileOperator(
            task_id='load_timehseets_csv',
            document='{{ result("run_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        timesheets_data = rail.CreateCollectionOperator(
            task_id='timesheets_data',
            source='{{ result("load_timehseets_csv") }}',
            name='timesheetsdata'
        )

        final_timesheets_not_admin_list = rail.QueryCollectionOperator(
            task_id='final_timesheets_not_admin_list',
            query="SELECT * FROM timesheetsdata WHERE Login_Name != 'admin'"
        )

        process_timesheets = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timesheets',
            items='{{ result("final_timesheets_not_admin_list") }}',
            trigger_dag_id=f'frontdoorinc_process_timesheets_autopopulation_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
            conf=lambda item: {
                "timesheet_uri": item["TimesheetPeriodUri"],
                "login_name": item["Login_Name"],
                "username": item["User_Name"],
                "timesheet_period": item["Timesheet_Period"],
                "script_uri": rail.result("get_population_script_uri")
            }
        )

        wait_for_process_timesheets = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_timesheets',
            dag_runs="{{ result('process_timesheets') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        send_logs_enter, _ = get_send_logs(config)

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_population_script_uri >> has_population_script >> rail.Label(
            'Yes') >> get_report_details >> run_report_entry
        run_report_exit >> is_report_failed
        is_report_failed >> rail.Label("No") >> report_has_data
        is_report_failed >> rail.Label("Yes") >> fail_report_generation

        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns
        report_has_data >> rail.Label("No") >> finish
        is_report_has_expected_columns >> rail.Label(
            'Yes') >> load_timehseets_csv >> timesheets_data >> final_timesheets_not_admin_list \
                >> process_timesheets >> wait_for_process_timesheets >> send_logs_enter
        has_population_script >> rail.Label('No') >> fail_no_script
        is_report_has_expected_columns >> rail.Label('No') >> fail_no_expected_columns

    return dag

rail.for_each_instance(create_main_dag)
