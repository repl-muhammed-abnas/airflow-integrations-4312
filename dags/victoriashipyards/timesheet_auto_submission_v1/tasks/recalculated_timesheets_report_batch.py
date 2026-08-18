import rail
from victoriashipyards.timesheet_auto_submission_v1.utils import request_payload


def report_batch(config):
    with rail.TaskGroup(group_id='recalculated_timesheets_report_batch', prefix_group_id=False):

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            wait_timeout=config.run_report_wait_timeout,
            retries=0,
            report_params=request_payload.get_report_generate_batch_payload,
            replicon_conn_id=config.replicon_conn_id,
        )

        payload_has_data = rail.IfOperator(
            task_id='payload_has_data',
            test="{{ result('run_report.get_report_result','has_data') }}",
            yes_task='no_error_exists',
            no_task='finish_no_data'
        )

        no_error_exists = rail.IfOperator(
            task_id='no_error_exists',
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].error | is_falsy }}",
            yes_task='load_csv_data',
            no_task='fail_error_report'
        )

        fail_error_report = rail.FailOperator(
            task_id="fail_error_report",
            message='{{ result("run_report.get_report_result").reportGenerationResults[0].error }}',
        )

        load_csv_data = rail.LoadCSVFileOperator(
            task_id='load_csv_data',
            document='{{ result("run_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        recalculated_timesheet_data = rail.CreateCollectionOperator(
            task_id='recalculated_timesheet_data',
            source="{{ result('load_csv_data') }}",
            columns={
                'Timesheet Period': 'timesheetperiod',
                'Login Name': 'username',
                'Validation Message': 'validationmessages',
                'Approval Status': 'approvalstatus',
                'Timesheet URI': 'timesheeturi',
                'Timesheet Start Date': 'timesheetstartdate',
                'Timesheet End Date': 'timesheetenddate',
            },
            name='recalculated_timesheets'
        )

        finish_no_data = rail.EmptyOperator(
            task_id='finish_no_data'
        )

        run_report_group_entry
        run_report_group_exit >> payload_has_data >> rail.Label(
            "Yes") >> no_error_exists
        payload_has_data >> rail.Label("No") >> finish_no_data

        no_error_exists >> rail.Label(
            "Yes") >> load_csv_data >> recalculated_timesheet_data
        no_error_exists >> rail.Label("No") >> fail_error_report

        return run_report_group_entry, recalculated_timesheet_data
