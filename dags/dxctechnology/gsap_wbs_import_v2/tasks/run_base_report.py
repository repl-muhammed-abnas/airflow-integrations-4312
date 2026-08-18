import rail

def run_base_report(config):
    with rail.TaskGroup(group_id="run_base_report", prefix_group_id=None):
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=config.base_report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id="report_generation",
            report_params=lambda :{
                "reportParameters": [
                    {
                        "reportUri": rail.result("get_report_details")['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("report_generation.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="has_report_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('report_generation.get_report_result').reportGenerationResults[0].error}}"
        )

        has_report_data = rail.IfOperator(
            task_id="has_report_data",
            test='{{"No Data" in result("report_generation.get_report_result").reportGenerationResults[0].payload}}',
            yes_task="fail_report_has_report_data",
            no_task='report_has_expected_columns',
        )

        fail_report_has_report_data = rail.FailOperator(
            task_id = "fail_report_has_report_data",
            message= "Base report does not have any data"
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('report_generation.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_report_columns,
            yes_task="report_payload_to_csv",
            no_task="fail_invalid_report_columns"
        )

        fail_invalid_report_columns = rail.FailOperator(
            task_id="fail_invalid_report_columns",
            message="Base report column does not match"
        )

        report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="report_payload_to_csv",
            document='{{result("report_generation.get_report_result").reportGenerationResults[0].payload}}'
        )

        base_report_collection = rail.CreateCollectionOperator(
            task_id="base_report_collection",
            name="report_data",
            source="{{result('report_payload_to_csv')}}",
            columns={
                'user_uri': 'user_uri',
                'perner': 'perner',
                'Employee Type (Current) (Full Path)': 'current_employee_type_full_path',
                'Company Code (Current)': 'current_company_code'
            }
        )

        get_required_users_details = rail.QueryCollectionOperator(
            task_id = "get_required_users_details",
            name="required_user_details_from_report",
            query="""SELECT * FROM report_data WHERE perner IN (SELECT DISTINCT Primary_Project_Manager_ID FROM gsaprecordscollection)"""
        )


        get_report_details >> run_report_group_entry
        run_report_group_exit >> is_report_failed >> rail.Label("Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> has_report_data >> rail.Label("Yes")\
            >> report_has_expected_columns >> rail.Label("No") >> fail_invalid_report_columns
        has_report_data >> rail.Label("No") >> fail_report_has_report_data
        report_has_expected_columns >> rail.Label("Yes") >> report_payload_to_csv >> base_report_collection >> get_required_users_details

        return get_report_details, get_required_users_details
