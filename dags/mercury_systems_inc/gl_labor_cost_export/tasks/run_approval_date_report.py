null=None
import rail
def run_approval_date_report_for_labor_cost(config):
    with rail.TaskGroup(
            group_id="run_report_for_approval_date",
            prefix_group_id=False
    ) as approval_date_task_group:
            # Get report details
        get_approval_date_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_approval_date_report_details',
            report_name=config.export_approval_report_name,
        )

        def report_params():
            entry_date_filter = rail.find_first_by_attr_and_get_attr(
                        rail.result('get_approval_date_report_details')['filterConfiguration']['enabledFilters'],
                        'displayText', "EntryDateFilter", 'uri')
            approval_date_filter = rail.find_first_by_attr_and_get_attr(
                        rail.result('get_approval_date_report_details')['filterConfiguration']['enabledFilters'],
                        'displayText', "ApprovalDateFilter", 'uri')
            return   {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_approval_date_report_details')["uri"],
                        "filterValues": [
                {
                    "reportFilterUri":entry_date_filter,
                    "value": null,
                },
                {
                    "reportFilterUri": entry_date_filter,
                    "value": null,
                },
                {
                    "reportFilterUri": entry_date_filter,
                    "value": rail.result("get_date_range_values")["beyond_nine_days"],
                },
                {
                    "reportFilterUri": approval_date_filter,
                    "value": null,
                },
                {
                    "reportFilterUri": approval_date_filter,
                    "value": rail.result("get_date_range_values")["approval_start_date"]
                },
                {
                    "reportFilterUri": approval_date_filter,
                    "value": rail.result("get_date_range_values")["approval_end_date"]}
            ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
            

        # Run report with filters
        report_approval_date_group_entry, report_approval_group_exit = rail.run_report(
            group_id='run_approval_date_report',
            report_params=report_params
        )

        # Check if report generation failed
        approval_date_report_failed = rail.IfOperator(
            task_id="approval_date_report_failed",
            test='{{result("run_approval_date_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="approval_date_fail_report_generation",
            no_task="approval_date_report_has_data"
        )

        # Handle report generation failure
        approval_date_fail_report_generation = rail.FailOperator(
            task_id="approval_date_fail_report_generation",
            message="{{result('run_approval_date_report.get_report_result').reportGenerationResults[0].error}}"
        )

        # Check if report has data
        approval_date_report_has_data = rail.IfOperator(
            task_id="approval_date_report_has_data",
            test="{{ result('run_approval_date_report.get_report_result', 'has_data') }}",
            yes_task='approval_date_report_has_expected_columns',
            no_task='report_end',
        )

        approval_date_report_has_expected_columns = rail.IfOperator(
            task_id="approval_date_report_has_expected_columns",
            test="{{ result('run_approval_date_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_report_columns,
            yes_task="load_approval_date_report_data",
            no_task="approval_date_fail_base_report_error"
        )

        approval_date_fail_base_report_error = rail.FailOperator(
            task_id="approval_date_fail_base_report_error",
            message="Base report error. Invalid column names."
        )

        # Load report data into memory
        load_approval_date_report_data = rail.LoadCSVFileOperator(
            task_id='load_approval_date_report_data',
            document="{{ result('run_approval_date_report.get_report_result').reportGenerationResults[0].payload }}",
            headers=["legal_name", "employee_id","uses_activity", "union_code",
                    "job_code", "pay_type", "week_ending","bu",
                    "department", "activity_pay_Code", "weekly_hours", "weekly_earnings",
                    "pay_code_multiplier", "hourly_rate"]
        )

        create_approval_date_collection_report_data = rail.CreateCollectionOperator(
            task_id="create_approval_date_collection_report_data",
            source='{{result("load_approval_date_report_data")}}',
            name="cost_approval_data"
        )

        report_end= rail.CreateCollectionOperator(
            task_id="report_end",
            source=[],
            name="cost_approval_data",
            columns=["legal_name", "employee_id","uses_activity", "union_code",
                    "job_code", "pay_type", "week_ending","bu",
                    "department", "activity_pay_Code", "weekly_hours", "weekly_earnings",
                    "pay_code_multiplier", "hourly_rate"]
        )

        get_approval_date_report_details >> report_approval_date_group_entry >>\
        report_approval_group_exit >> approval_date_report_failed >>\
        rail.Label("Yes") >> approval_date_fail_report_generation
        approval_date_report_failed >> rail.Label("No") >>\
        approval_date_report_has_data >> rail.Label("Yes") >>\
        approval_date_report_has_expected_columns >> rail.Label("Yes")>>\
        load_approval_date_report_data >> create_approval_date_collection_report_data
        approval_date_report_has_expected_columns >> rail.Label("No") >>\
        approval_date_fail_base_report_error
        approval_date_report_has_data >> rail.Label("No") >> report_end
        return approval_date_task_group
