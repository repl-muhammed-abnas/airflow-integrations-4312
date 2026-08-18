import rail


def report_batch(config):
    with rail.TaskGroup(group_id='generate_report_batch', prefix_group_id=False):
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.client_report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_client_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
            target="artifact"
        )

    report_has_data = rail.IfOperator(
        task_id="report_has_data",
        test='''{{ not (result('run_client_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload |  matches('No Data') }}''',
        yes_task='report_has_expected_columns',
        no_task='fail_no_report_data'
    )

    expected_report_columns = "Client Name,Client URI,Tiger Client Name"

    report_has_expected_columns = rail.IfOperator(
        task_id="report_has_expected_columns",
        #pylint: disable=consider-using-f-string
        test="{{ (result('run_client_report.get_report_result'\
        ) | load_json_artifact).reportGenerationResults[0].payload | starts_with('%s') }}" % expected_report_columns,
        yes_task='load_report_data',
        no_task='fail_invalid_report_columns',
    )

    fail_invalid_report_columns = rail.FailOperator(
        task_id="fail_invalid_report_columns",
        message="Base report columns do not match",
    )

    fail_no_report_data = rail.FailOperator(
        task_id="fail_no_report_data",
        message="No data in the base report",
    )

    load_report_data = rail.LoadCSVFileOperator(
        task_id='load_report_data',
        document="{{ (result('run_client_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}",
    )

    get_report_details >> run_report_group_entry
    run_report_group_exit >> report_has_data >> rail.Label(
        "Yes") >> report_has_expected_columns >> rail.Label('Yes') >> load_report_data
    report_has_expected_columns >> rail.Label(
        'No') >> fail_invalid_report_columns
    report_has_data >> rail.Label("No") >> fail_no_report_data

    return get_report_details, load_report_data
