from transparentbpo.time_entry_import.utils import request_payload
import rail


def run_report_task_group(config, report_name, report_collection_name,
                          csv_headers):
    with rail.TaskGroup(
            group_id="run_report_for",
            prefix_group_id=False
    ) as tg:

        query_unique_projects_from_input = rail.QueryCollectionOperator(
            task_id="query_unique_projects_from_input",
            query="""SELECT DISTINCT project FROM valid_entries""",
            name="feed_unique_projects"
        )

        get_all_project_details = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_all_project_details",
            items="{{result('query_unique_projects_from_input')}}",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            # unique identifier is project name
            data=lambda item: {
                "projects": [
                    {
                        "name": item['project']
                    }
                ]
            },
            data_handler=lambda res: res[0].get('projectDetails')
        )

        is_any_project_found = rail.IfOperator(
            task_id="is_any_project_found",
            test=lambda: len(
                list(filter(None, rail.result("get_all_project_details")))) > 0,
            yes_task="get_report_details",
            no_task="fail_dag_run"
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=report_name
        )

        run_report = rail.run_report2(
            group_id="run_user_project_data_report",
            report_params=request_payload.get_project_report_params
        )

        if_report_data_has_error = rail.IfOperator(
            task_id="if_report_data_has_error",
            test="{{ result('run_user_project_data_report.get_report_result').reportGenerationResults[0].error | is_truthy or \
                result('run_user_project_data_report.get_report_result').reportGenerationResults[0].payload | starts_with('No Data') }}",
            yes_task="fail",
            no_task="report_has_expected_columns"
        )

        fail_report_data = rail.FailOperator(
            task_id="fail",
            message="Base report error"
        )

        project_report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            # pylint: disable=consider-using-f-string line-too-long
            test="{{ result('run_user_project_data_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_csv_columns,
            no_task="fail_invalid_columns",
            yes_task="load_project_task_data"
        )

        fail_invalid_project_report_columns = rail.FailOperator(
            task_id="fail_invalid_columns",
            message="Base report column does not match"
        )

        # Load project task data
        load_project_task_data = rail.LoadCSVFileOperator(
            task_id="load_project_task_data",
            document="{{ result('run_user_project_data_report.get_report_result').reportGenerationResults[0].payload }}",
            headers=csv_headers
        )
        # Create collection for project task data
        create_user_and_project_data_collection = rail.CreateCollectionOperator(
            task_id="create_collection",
            source="{{ result('load_project_task_data') }}",
            name=report_collection_name,
        )

        fail_dag_run = rail.FailOperator(
            task_id="fail_dag_run",
            message="No projects found in replicon"
        )

        query_unique_projects_from_input >> get_all_project_details >>\
            is_any_project_found >> rail.Label("No") >> fail_dag_run
        is_any_project_found >> rail.Label("Yes") >> get_report_details >>\
            run_report >>\
            if_report_data_has_error >> rail.Label("Yes") >> fail_report_data
        if_report_data_has_error >> rail.Label("No") >>\
            project_report_has_expected_columns >> rail.Label(
                "No") >> fail_invalid_project_report_columns
        project_report_has_expected_columns >> rail.Label("Yes") >>\
            load_project_task_data >> create_user_and_project_data_collection

    return tg
