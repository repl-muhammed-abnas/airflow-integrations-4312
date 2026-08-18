import rail


def report_batch(config):
    with rail.TaskGroup(group_id='generate_report_batch', prefix_group_id=False):
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.enabled_users_report
        )

        run_enabled_users_report = rail.run_report2(
            group_id='run_enabled_users_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test='{{ result("run_enabled_users_report.get_report_result", "has_data") }}',
            yes_task='check_report_column_order',
            no_task='fail_no_report_data'
        )

        fail_no_report_data = rail.FailOperator(
            task_id='fail_no_report_data',
            message='Base report does not have user data',
        )

        expected_report_columns = 'User Name,Login Name,Employee ID,UserUri,User End Date,daydiff'

        check_report_column_order = rail.IfOperator(
            task_id='check_report_column_order',
            # pylint: disable=consider-using-f-string line-too-long
            test='{{ result("run_enabled_users_report.get_report_result").reportGenerationResults[0].payload | starts_with("%s") }}' % expected_report_columns,
            yes_task='load_report_data',
            no_task='fail_column_order_mismatch'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document='{{ result("run_enabled_users_report.get_report_result").reportGenerationResults[0].payload }}',
        )

        fail_column_order_mismatch = rail.FailOperator(
            task_id='fail_column_order_mismatch',
            message='Base report coolumn order does not match',
        )

        get_report_details >> run_enabled_users_report >> report_has_data >> rail.Label(
            "Yes") >> check_report_column_order
        report_has_data >> rail.Label("No") >> fail_no_report_data
        check_report_column_order >> rail.Label(
            "Yes") >> load_report_data
        check_report_column_order >> rail.Label(
            "No") >> fail_column_order_mismatch

    return get_report_details, load_report_data, fail_no_report_data, fail_column_order_mismatch
