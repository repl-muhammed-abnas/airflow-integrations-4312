import rail


def load_attribute_parents(config):
    with rail.TaskGroup(group_id='load_attribute_parents', prefix_group_id=False):
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.gsap_report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_gsap_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_report_details').uri }}",
                        "filterValues": [
                            {
                                # pylint: disable=consider-using-f-string
                                "reportFilterUri": "{{ result('get_report_details').filterConfiguration.enabledFilters | " + \
                                    "find_first_by_attr_and_get_attr('displayText', '%s', 'uri') }}" % config.gsap_report_projectfilter_name,
                                "value": "{{ result('load_project').uri | split(':') | last }}"
                            },
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
        )

        expected_report_columns = 'Task Name,Task Type,Attribute2,Attribute1,TaskUri,Attribute1 name'
        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            # pylint: disable=consider-using-f-string
            test="{{ result('run_gsap_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % expected_report_columns,
            no_task='fail_bad_report_columns',
            yes_task='report_has_data',
        )

        fail_bad_report_columns = rail.FailOperator(
            task_id="fail_bad_report_columns",
            message="Base report column does not match",
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_gsap_report.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_gsap_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_report_collection = rail.CreateCollectionOperator(
            task_id='create_report_collection',
            source="{{ result('load_report_data') }}",
        )

        query_attribute2 = rail.QueryCollectionOperator(
            task_id='query_attribute2',
            query='SELECT Task_Name, TaskUri, Attribute1_name FROM create_report_collection WHERE Attribute2 = "Yes"',
        )

        query_attribute1 = rail.QueryCollectionOperator(
            task_id='query_attribute1',
            query='SELECT Task_Name, TaskUri, Attribute1_name FROM create_report_collection WHERE Attribute1 = "Yes" AND Task_Name NOT IN ( \
                SELECT Attribute1_name FROM create_report_collection WHERE Attribute2 = "Yes")',
        )

        merge_parent_tasks = rail.QueryCollectionOperator(
            task_id='merge_parent_tasks',
            query='SELECT Task_Name as TaskName, TaskUri FROM query_attribute2 UNION SELECT Task_Name as TaskName, TaskUri FROM query_attribute1',
        )

    get_report_details >> run_report_group_entry
    run_report_group_exit >> report_has_expected_columns >> rail.Label("Yes") >> report_has_data >> rail.Label("Yes") >> load_report_data >> \
        create_report_collection >> [
        query_attribute2,
        query_attribute1] >> merge_parent_tasks
    report_has_expected_columns >> rail.Label("No") >> fail_bad_report_columns
    return get_report_details, merge_parent_tasks
