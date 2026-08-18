import rail
from macquariegroup.recovery_reconciliation.utils import data_handlers, request_payload


def run_base_report(config, group_id):
    with rail.TaskGroup(group_id=group_id, prefix_group_id=False):
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=config.user_base_report
        )

        get_required_division = rail.RepliconServiceOperator(
            task_id="get_required_division",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
            data_handler=data_handlers.get_required_divisions
        )

        run_report_start,  run_report_end = rail.run_report(
            group_id="generate_base_report",
            report_params=request_payload.get_report_parameters
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
        expected_report_columns = "Login Name,Cost Center (Current),Department (Current),Employee Type (Current),Group (Current),Recovery Enabled (Current),Recovery Override,useruri,User Status"
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

        get_report_details >> get_required_division >> run_report_start
        run_report_end >> is_report_generation_failed >> rail.Label(
            "Yes") >> fail_report_generation_failed
        is_report_generation_failed >> rail.Label(
            "No") >> report_has_data >> rail.Label("No") >> fail_report_does_not_have_data
        report_has_data >> rail.Label(
            "Yes") >> report_has_expected_columns >> rail.Label("Yes") >> load_report_data
        report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_columns

        return get_report_details, load_report_data
