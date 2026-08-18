import rail


def report_batch(config):
    with rail.TaskGroup(group_id='generate_report_batch', prefix_group_id=False):
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.iwo_details_update_report,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_iwo_details_update_report',
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
        )

    report_has_data = rail.IfOperator(
        task_id='report_has_data',
        test='{{ result("run_iwo_details_update_report.get_report_result", "has_data") }}',
        yes_task='check_report_column_order',
        no_task='fail_no_report_data'
    )

    fail_no_report_data = rail.FailOperator(
        task_id='fail_no_report_data',
        message='Base report does not have user data',
    )

    expected_report_columns = 'Employee ID,UserUri,User Status,User End Date'

    check_report_column_order = rail.IfOperator(
        task_id='check_report_column_order',
        # pylint: disable=consider-using-f-string line-too-long
        test='{{ result("run_iwo_details_update_report.get_report_result").reportGenerationResults[0].payload | starts_with("%s") }}' % expected_report_columns,
        yes_task='load_report_data',
        no_task='fail_column_order_mismatch'
    )

    load_report_data = rail.LoadCSVFileOperator(
        task_id='load_report_data',
        document='{{ result("run_iwo_details_update_report.get_report_result").reportGenerationResults[0].payload }}',
    )

    create_report_collection = rail.CreateCollectionOperator(
        task_id='create_report_collection',
        source='{{ result("load_report_data") }}',
        name='userdatafromreplicon',
        columns={
                'Employee ID': 'employeeid',
                'UserUri': 'uri',
                'User Status': 'userstatus',
                'User End Date': 'userenddate'
        }
    )

    user_data_with_employee_id = rail.QueryCollectionOperator(
        task_id='user_data_with_employee_id',
        query='SELECT * FROM userdatafromreplicon WHERE employeeid IS NOT NULL',
        name='userdatawithemployeeid'
    )

    fail_column_order_mismatch = rail.FailOperator(
        task_id='fail_column_order_mismatch',
        message='Base report coolumn order does not match',
    )

    get_report_details >> run_report_group_entry
    run_report_group_exit >> report_has_data >> rail.Label(
        "Yes") >> check_report_column_order
    report_has_data >> rail.Label("No") >> fail_no_report_data

    check_report_column_order >> rail.Label(
        "Yes") >> load_report_data >> create_report_collection >> user_data_with_employee_id
    check_report_column_order >> rail.Label(
        "No") >> fail_column_order_mismatch

    return get_report_details, user_data_with_employee_id
