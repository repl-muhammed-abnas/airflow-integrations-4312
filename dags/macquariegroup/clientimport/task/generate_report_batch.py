import rail


def report_batch(config):
    with rail.TaskGroup(group_id='generate_report_batch', prefix_group_id=False):
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.client_report_for_integration,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_shift_assignment_report',
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
        test='{{ result("run_shift_assignment_report.get_report_result", "has_data") }}',
        yes_task='load_report_data',
        no_task='fail_no_report_data'
    )

    fail_no_report_data = rail.EmptyOperator(
        task_id='fail_no_report_data',
    )

    load_report_data = rail.LoadCSVFileOperator(
        task_id='load_report_data',
        document='{{ result("run_shift_assignment_report.get_report_result").reportGenerationResults[0].payload }}',
    )

    create_report_collection = rail.CreateCollectionOperator(
        task_id='create_report_collection',
        source='{{ result("load_report_data") }}',
        name='allclients',
        columns={
                'Client Name': 'clientname',
                'Client Code': 'clientcode',
                'Location': 'location',
                'Client Status': 'clientstatus',
                'ClientUri':  'uri'
        }
    )

    get_report_details >> run_report_group_entry
    run_report_group_exit >> report_has_data >> rail.Label(
        "Yes") >> load_report_data >> create_report_collection
    report_has_data >> rail.Label("No") >> fail_no_report_data

    return get_report_details, create_report_collection, fail_no_report_data
