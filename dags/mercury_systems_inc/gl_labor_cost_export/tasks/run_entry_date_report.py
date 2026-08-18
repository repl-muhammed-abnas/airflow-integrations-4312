null=None
import rail
def run_entry_date_labor_cost_report(config):
        
    with rail.TaskGroup(
            group_id="run_entry_date_labor_cost",
            prefix_group_id=False
    ) as entry_date_report:

        # Get report details
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.export_report_name
        )

        # Run report with filters
        report_group_entry, report_group_exit = rail.run_report(
            group_id='run_report',
            report_params=lambda:  {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details')["uri"],
                        "filterValues": [
                            {
                                "reportUri": rail.result('get_report_details')["uri"],
                                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_report_details')['filterConfiguration']['enabledFilters'],
                                    'displayText', "EntryDateFilter", 'uri'),
                                "value": null,
                            },
                            {
                                "reportFilterUri": rail.result('get_report_details')["uri"],
                                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_report_details')['filterConfiguration']['enabledFilters'],
                                    'displayText', "EntryDateFilter", 'uri'),
                            "value": rail.result("get_date_range_values")["start_date"],
                            },
                            {
                                "reportFilterUri": rail.result('get_report_details')["uri"],
                                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_report_details')['filterConfiguration']['enabledFilters'],
                                    'displayText', "EntryDateFilter", 'uri'),
                                "value": rail.result("get_date_range_values")["end_date"],
                            },
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        # Check if report generation failed
        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        # Handle report generation failure
        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        # Check if report has data
        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_report.get_report_result', 'has_data') }}",
            yes_task='report_has_expected_columns',
            no_task='end_report',
        )

        end_report= rail.CreateCollectionOperator(
            task_id="end_report",
            source=[],
            name="cost_data",
            columns=["legal_name", "employee_id","uses_activity", "union_code",
                    "job_code", "pay_type", "week_ending","bu",
                    "department", "activity_pay_Code", "weekly_hours", "weekly_earnings",
                    "pay_code_multiplier", "hourly_rate"]
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_report_columns,
            yes_task="load_report_data",
            no_task="fail_base_report_error"
        )

        fail_base_report_error = rail.FailOperator(
            task_id="fail_base_report_error",
            message="Base report error. Invalid column names."
        )

        # Load report data into memory
        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
            headers=["legal_name", "employee_id","uses_activity", "union_code",
                    "job_code", "pay_type", "week_ending","bu",
                    "department", "activity_pay_Code", "weekly_hours", "weekly_earnings",
                    "pay_code_multiplier", "hourly_rate"]
        )

        create_collection_report_data = rail.CreateCollectionOperator(
            task_id="create_collection_report_data",
            source='{{result("load_report_data")}}',
            name="cost_data"
        )

        get_report_details >> report_group_entry >> report_group_exit >>\
        is_report_failed >> rail.Label("Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data
        report_has_data >> rail.Label("Yes") >> report_has_expected_columns
        report_has_data >> rail.Label("No") >> end_report
        report_has_expected_columns >> rail.Label("Yes") >> load_report_data>>\
        create_collection_report_data
        report_has_expected_columns >> rail.Label("No") >> fail_base_report_error
        return entry_date_report

        
        