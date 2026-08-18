from capgemini.payroll_leave_export_hgs_v2.utils import request_payload
import rail


def run_timeoff_report(config, status, report_name, expected_report_columns):
    with rail.TaskGroup(group_id=f'{status}_timeoff_report_run', prefix_group_id=False):

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id=f'get_{status}_timeoffs_report_details',
            report_name=report_name
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id=f'run_{status}_timeoffs_report',
            report_params=lambda: request_payload.get_added_timeoffs_report_batch_payload(config.time_zone)
                if status == "added" else request_payload.get_approvedlast30days_timeoffs_report_batch_payload(config.time_zone)
        )

        is_report_failed = rail.IfOperator(
            task_id=f'is_{status}_timeoffs_report_failed',
            test='{{result("run_' + status + '_timeoffs_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task=f'fail_{status}_timeoffs_report_generation',
            no_task=f'report_{status}_timeoffs_has_data'
        )

        fail_report_generation = rail.FailOperator(
            task_id=f'fail_{status}_timeoffs_report_generation',
            message="{{result('run_" + status + "_timeoffs_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id=f'report_{status}_timeoffs_has_data',
            test="{{result('run_" + status + "_timeoffs_report.get_report_result','has_data')}}",
            yes_task=f'is_{status}_timeoffs_report_has_expected_columns',
            no_task=f'load_{status}_timeoffs_report_data'
        )

        is_report_has_expected_columns = rail.IfOperator(
            task_id=f'is_{status}_timeoffs_report_has_expected_columns',
            # pylint: disable=consider-using-f-string line-too-long
            test="{{result('run_" + status + "_timeoffs_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_report_columns,
            yes_task=f'load_{status}_timeoffs_report_data',
            no_task=f'fail_{status}_timeoffs_has_no_expected_columns',
        )

        fail_has_no_expected_columns = rail.FailOperator(
            task_id=f'fail_{status}_timeoffs_has_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id=f'load_{status}_timeoffs_report_data',
            document="{{ result('run_" + status + "_timeoffs_report.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_collection = rail.CreateCollectionOperator(
            task_id=f'create_{status}_timeoffs_collection',
            source='{{ result("' + load_report_data.task_id + '") }}',
            columns={
                "Leave Request ID": "leave_request_id",
                "Local Employee Number": "local_employee_number",
                "Employee ID": "employee_id",
                "Current Time Off Type": "timeoff_type",
                "Current Start Date": "booking_start_date",
                "Current End Date": "booking_end_date",
                "Action": "action",
                "Modified On": "modified_on"
            },
            name=f"{status}_timeoffs_bookings_data"
        )

        report_run_finish = rail.EmptyOperator(
            task_id=f'{status}_timeoffs_report_finish'
        )

        get_report_details >> run_report_entry
        run_report_exit >> is_report_failed

        is_report_failed >> rail.Label("Yes") >> fail_report_generation >> report_run_finish
        is_report_failed >> rail.Label("No") >> report_has_data

        report_has_data >> rail.Label("Yes") >> is_report_has_expected_columns
        report_has_data >> rail.Label("No") >> load_report_data

        is_report_has_expected_columns >> rail.Label("Yes") >> load_report_data >> create_collection >> report_run_finish
        is_report_has_expected_columns >> rail.Label("No") >> fail_has_no_expected_columns >> report_run_finish

        return(get_report_details, report_run_finish)
