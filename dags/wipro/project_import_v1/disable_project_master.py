from pendulum import datetime
import rail

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.disable_project_master_dag_id,
        description= "Wipro Project/task disable master dag",
        start_date= datetime(2023,9,1),
        schedule_interval= config.disable_project_schedule,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.master_max_active_run,
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.project_report_name,
        )

        load_project_data_from_report = rail.run_report(
            group_id='load_project_data_from_report',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('get_report_details').uri}}"
                    }
                ]
            }
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test=lambda: rail.result(
                "load_project_data_from_report.get_report_result", "has_data"),
            yes_task='report_has_expected_columns',
            no_task="fail_no_data_in_report"
        )

        fail_no_data_in_report = rail.FailOperator(
            task_id="fail_no_data_in_report",
            message="No Data in the Base report"
        )

        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            #pylint: disable=consider-using-f-string line-too-long
            test="{{ result('load_project_data_from_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_project_report_columns,
            yes_task="load_report_payload_to_csv",
            no_task="fail_invalid_report_columns"
        )

        fail_invalid_report_columns = rail.FailOperator(
            task_id="fail_invalid_report_columns",
            message="Base report column does not match"
        )

        load_report_payload_to_csv = rail.LoadCSVFileOperator(
            task_id="load_report_payload_to_csv",
            document='{{ result("load_project_data_from_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        project_report_collection = rail.CreateCollectionOperator(
            task_id='project_report_collection',
            name='projectreport',
            source='{{ result("load_report_payload_to_csv") }}',
            columns={
                'Project Status': 'projectstatus',
                'Task Status': 'taskstatus',
                'projectdaydiff': 'projectdaydiff',
                'taskdaydiff': 'taskdaydiff',
                'ProjectUri': 'projecturi',
                'TaskUri': 'taskuri',
            }
        )

        query_project_collection_data = rail.QueryCollectionOperator(
            task_id="query_project_collection_data",
            query="""SELECT * FROM projectreport WHERE projectdaydiff < '0.00' AND projectstatus = 'In Progress' AND taskstatus = '' """,
        )

        has_project_valid_data = rail.IfOperator(
            task_id='has_project_valid_data',
            test='{{ result("query_project_collection_data", "length") > 0 }}',
            yes_task="close_projects",
            no_task='query_task_collection_data'
        )

        close_projects = rail.RepliconServiceCallForEachItemOperator(
            task_id = 'close_projects',
            endpoint= '/services/ProjectService1.svc/UpdateStatus',
            items='{{ result("query_project_collection_data") }}',
            data=lambda item: {
                "projectUri": item['projecturi'],
                "projectStatusUri": "urn:replicon:project-status-type:completed"
            }
        )

        query_task_collection_data = rail.QueryCollectionOperator(
            task_id="query_task_collection_data",
            query="""SELECT * FROM projectreport WHERE taskdaydiff < '0.00' AND taskstatus = 'Open' """,
        )

        has_task_valid_data = rail.IfOperator(
            task_id='has_task_valid_data',
            test='{{ result("query_task_collection_data", "length") > 0 }}',
            yes_task="close_tasks",
            no_task='finish'
        )

        close_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id = 'close_tasks',
            endpoint= '/services/TaskService1.svc/Close',
            items='{{ result("query_task_collection_data") }}',
            data=lambda item: {
                "taskUri": item['taskuri']
            }
        )

        finish = rail.EmptyOperator(
            task_id = 'finish'
        )

        get_report_details >> load_project_data_from_report >> report_has_data

        report_has_data >> rail.Label(
            "No") >> fail_no_data_in_report

        report_has_data >> rail.Label(
            "Yes") >> report_has_expected_columns

        report_has_expected_columns >> rail.Label(
            "No") >> fail_invalid_report_columns

        report_has_expected_columns >> rail.Label(
            "Yes") >> load_report_payload_to_csv >> project_report_collection >> query_project_collection_data >>\
            has_project_valid_data

        has_project_valid_data >> rail.Label(
            "Yes") >> close_projects >> query_task_collection_data

        has_project_valid_data >> rail.Label(
            "No") >> query_task_collection_data >> has_task_valid_data

        has_task_valid_data >> rail.Label(
            "Yes") >> close_tasks >> finish

        has_task_valid_data >> rail.Label(
            "No") >> finish

    return dag

rail.for_each_instance(create_main_dag)
