import rail

def run_schedule_base_report(config, group_id):
    with rail.TaskGroup(group_id= group_id, prefix_group_id=False):
        get_base_schedule_report = rail.RepliconReportDetailsOperator(
            task_id = "get_base_schedule_report",
            report_name=config.base_schedule_report_name
        )

        run_schedule_base_report_task = rail.run_report2(
            group_id = "generate_base_report",
            report_params={"reportParameters":[
                {
                    "reportUri": "{{ result('get_base_schedule_report').uri }}",
                    "filterValues":[],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }
            ]},
        )

        is_report_generation_failed = rail.IfOperator(
            task_id="is_report_generation_failed",
            test="{{ result('generate_base_report.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task="fail_report_generation_failed",
            no_task="report_has_excepted_columns"
        )

        fail_report_generation_failed = rail.FailOperator(
            task_id="fail_report_generation_failed",
            message="{{ result('generate_base_report.get_report_result').reportGenerationResults[0].error }}"
        )

        # pylint: disable=line-too-long
        # pylint: disable=consider-using-f-string
        report_has_excepted_columns = rail.IfOperator(
            task_id = "report_has_excepted_columns",
            test="{{ result('generate_base_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_report_columns,
            yes_task="load_report_data",
            no_task="fail_report_column_headers_not_match"
        )

        fail_report_column_headers_not_match = rail.FailOperator(
            task_id = "fail_report_column_headers_not_match",
            message="Report Columns are mismatched"
        )

        load_report_data =rail.LoadCSVFileOperator(
            task_id = "load_report_data",
            document='{{result("generate_base_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        create_report_collection = rail.CreateCollectionOperator(
            task_id = "create_report_collection",
            source= "{{ result('load_report_data') }}",
            name= "schedule_base_report_data",
            columns={
                'Employee ID': 'EmployeeID',
                'Schedule Name (Current)': 'current_schedule_name',
                'useruri': 'useruri'
            }
        )

        get_base_schedule_report >> run_schedule_base_report_task >> is_report_generation_failed >> rail.Label(
            "Yes") >> report_has_excepted_columns >> rail.Label(
            "Yes") >> load_report_data >> create_report_collection
        is_report_generation_failed >> rail.Label("No") >> fail_report_generation_failed
        report_has_excepted_columns >> rail.Label("No") >> fail_report_column_headers_not_match

        return get_base_schedule_report, create_report_collection
