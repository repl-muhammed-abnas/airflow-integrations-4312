import rail


def report_batch(config):
    with rail.TaskGroup(group_id='generate_report_batch', prefix_group_id=False):
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.userlist_report
        )

        run_timeoff_balance_report = rail.run_report2(
            group_id='run_timeoff_balance_report',
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
            test='{{ result("run_timeoff_balance_report.get_report_result", "has_data") }}',
            yes_task='load_report_data',
            no_task='fail_no_report_data'
        )

        fail_no_report_data = rail.FailOperator(
            task_id='fail_no_report_data',
            message='Base report does not have user data',
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document='{{ result("run_timeoff_balance_report.get_report_result").reportGenerationResults[0].payload }}',
        )

        create_collection_from_report_data = rail.CreateCollectionOperator(
            task_id='create_collection_from_report_data',
            source="{{ result('load_report_data') }}",
            name="userlistfromreplicon",
            columns={
                'User Name': 'username',
                'Login Name': 'loginname',
                'User Status': 'enabled',
                'useruri': 'useruri',
                'User Start Date': 'startdate',
                'Country ISO Code': 'countryisocode'
            }
        )

        get_report_details >> run_timeoff_balance_report >> report_has_data >> rail.Label(
            "Yes") >> load_report_data >> create_collection_from_report_data
        report_has_data >> rail.Label("No") >> fail_no_report_data

    return get_report_details, create_collection_from_report_data, fail_no_report_data
