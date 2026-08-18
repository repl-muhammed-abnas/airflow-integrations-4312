from hashlib import md5
import rail


def user_data_report(config):
    with rail.TaskGroup(group_id="user_data_report", prefix_group_id=False) as taskgroup:
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.user_details_report,
        )

        run_user_report_entry, run_user_report_exit = rail.run_report('run_user_report', {
            "reportParameters": [
                {
                    "reportUri": '{{result("get_report_details").uri}}',
                    "filterValues": [],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }
            ]}
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_user_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_expected_columns"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_user_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            # pylint: disable=consider-using-f-string
            test="{{ result('run_user_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_report_columns,
            no_task='fail_invalid_report_colums',
            yes_task='report_has_data',
        )

        fail_invalid_report_colums = rail.FailOperator(
            task_id="fail_invalid_report_colums",
            message="Base report column does not match"
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_user_report.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task="report_end"
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            headers=['user_email', 'win_id', 'login_name',
                     'useruri', 'user_status', 'assignment_status'],
            document="{{ result('run_user_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_user_report_data_collection = rail.CreateCollectionOperator(
            task_id="create_user_report_data_collection",
            source='{{result("load_report_data")}}',
            name="user_report_data"
        )

        query_user_report_data_collection = rail.QueryCollectionOperator(
            task_id="query_user_report_data_collection",
            query="""SELECT * FROM user_report_data"""
        )

        def get_data(item):
            if not item:
                return []
            return {
                **item,
                "md5": md5((item["win_id"] + item["login_name"]).encode()).hexdigest()
            }

        create_existing_user_md5 = rail.DataAdaptorOperator(
            task_id="create_existing_user_md5",
            source='{{result("query_user_report_data_collection")}}',
            columns=['user_email', 'win_id', 'login_name', 'useruri',
                     'user_status', 'assignment_status', "md5"],
            data=get_data
        )

        create_user_report_collection = rail.CreateCollectionOperator(
            task_id="create_user_report_collection",
            source='{{result("create_existing_user_md5")}}',
            name="existing_user_records"
        )

        report_end = rail.EmptyOperator(task_id="report_end")
        get_report_details >> run_user_report_entry >>\
            run_user_report_exit >> is_report_failed >> rail.Label(
                "Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >>\
            report_has_expected_columns >> rail.Label(
                "No") >> fail_invalid_report_colums
        report_has_expected_columns >> rail.Label("Yes") >>\
            report_has_data >> rail.Label("No") >> report_end
        report_has_data >> rail.Label("Yes") >>\
            load_report_data >> create_user_report_data_collection >>\
            query_user_report_data_collection >>\
            create_existing_user_md5 >>\
            create_user_report_collection >> report_end

        return taskgroup
