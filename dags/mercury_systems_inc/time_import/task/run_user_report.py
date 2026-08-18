import rail

def run_report_task_group_for_user(config, report_name, report_collection_name,
                          csv_headers):
    with rail.TaskGroup(
            group_id="run_report_for_user_data",
            prefix_group_id=False
    ) as tg:

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_user_report_details",
            report_name=report_name
        )

        run_report = rail.run_report2(
            group_id="run_user_data_report",
            report_params=lambda:{
                        "reportParameters": [
                            {
                                "reportUri": rail.result('get_user_report_details')["uri"],
                                "filterValues":[],
                                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                            }
                        ]
                    }
        )

        if_user_report_data_has_error = rail.IfOperator(
            task_id="if_user_report_data_has_error",
            test="{{ result('run_user_data_report.get_report_result').reportGenerationResults[0].error | is_truthy or \
                result('run_user_data_report.get_report_result').reportGenerationResults[0].payload | starts_with('No Data') }}",
            yes_task="fail_report_data",
            no_task="user_report_has_expected_columns"
        )

        fail_report_data = rail.FailOperator(
            task_id="fail_report_data",
            message="Base report error"
        )

        user_report_has_expected_columns = rail.IfOperator(
            task_id="user_report_has_expected_columns",
            # pylint: disable=consider-using-f-string line-too-long
            test="{{ result('run_user_data_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_user_report_columns,
            no_task="fail_invalid_user_report_columns",
            yes_task="load_user_data"
        )

        fail_invalid_user_report_columns = rail.FailOperator(
            task_id="fail_invalid_user_report_columns",
            message="Base report column does not match"
        )

        # Load project task data
        load_user_data = rail.LoadCSVFileOperator(
            task_id="load_user_data",
            document="{{ result('run_user_data_report.get_report_result').reportGenerationResults[0].payload }}",
            headers=csv_headers
        )
        # Create collection for project task data
        create_user_and_project_data_collection = rail.CreateCollectionOperator(
            task_id="create_user_and_project_data_collection",
            source="{{ result('load_user_data') }}",
            name=report_collection_name,
        )

        get_user_report_details >>\
            run_report >>\
            if_user_report_data_has_error >> rail.Label("Yes") >> fail_report_data
        if_user_report_data_has_error >> rail.Label("No") >>\
            user_report_has_expected_columns >> rail.Label(
                "No") >> fail_invalid_user_report_columns
        user_report_has_expected_columns >> rail.Label("Yes") >>\
            load_user_data >> create_user_and_project_data_collection

    return tg
