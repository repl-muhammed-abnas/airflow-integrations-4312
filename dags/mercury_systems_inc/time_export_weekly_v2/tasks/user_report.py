import rail

def user_data_export(config):
    """
    Creates task group for exporting time data from Replicon.

    Args:
        group_id: Unique identifier for the task group
        get_user_export_name: Name to be assigned to the export

    Returns:
        tuple: A tuple containing the first and last tasks of the task group
    """
    with rail.TaskGroup(group_id="user_report_data") as task_group:
        # Get report details
        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_report_details',
            report_name=config.user_report_name
        )

        # Run report with filters
        report_group_entry, report_group_exit = rail.run_report(
            group_id='run_report',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('user_report_data.get_user_report_details')["uri"],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        # Check if report generation failed
        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("user_report_data.run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="user_report_data.fail_user_report_generation",
            no_task="user_report_data.report_has_data"
        )

        # Handle report generation failure
        fail_user_report_generation = rail.FailOperator(
            task_id="fail_user_report_generation",
            message="{{result('user_report_data.run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        # Check if report has data
        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('user_report_data.run_report.get_report_result', 'has_data') }}",
            yes_task='user_report_data.report_has_expected_columns',
            no_task='user_report_data.stop_report',
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            test="{{ result('user_report_data.run_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.user_report_columns,
            yes_task="user_report_data.load_report_data",
            no_task="user_report_data.fail_user_base_report_error"
        )

        fail_user_base_report_error = rail.FailOperator(
            task_id="fail_user_base_report_error",
            message="Base report error. Invalid column names."
        )

        # Load report data into memory
        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('user_report_data.run_report.get_report_result').reportGenerationResults[0].payload }}",
            delimiter=',',
            headers=["employee_id", "user_first_name", "user_last_name"]
        )

        create_user_report_data = rail.CreateCollectionOperator(
            task_id="create_user_report_data",
            source='{{result("user_report_data.load_report_data")}}',
            name="user_details"
        )

        stop_report = rail.EmptyOperator(task_id="stop_report")

        get_user_report_details >> report_group_entry >> report_group_exit >>\
        is_report_failed >> rail.Label("Yes") >> fail_user_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data >>\
        rail.Label("Yes") >> report_has_expected_columns >> rail.Label("Yes") >> load_report_data >>\
        create_user_report_data
        report_has_data >> rail.Label("No") >> stop_report
        report_has_expected_columns >> rail.Label("No") >> fail_user_base_report_error

        return task_group
