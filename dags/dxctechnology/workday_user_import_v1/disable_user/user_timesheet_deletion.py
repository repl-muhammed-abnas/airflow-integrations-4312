from datetime import datetime as dt, timedelta
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.disable_user.utils import custom_methods

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.user_timesheet_deletion_dag_id,
        description=f'User Timesheet Deletion Report integration {config.instance}',
        start_date=datetime(2024, 1, 1),
        schedule_interval=config.user_timesheet_deletion_schedule,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_master,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_disable_user, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_closure_date_filter_definition"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id="batch_task",
            start_task="get_closure_date_filter_definition",
            end_task="finish_dag",
            execution_timeout=timedelta(days=14)
        )

        get_closure_date_filter_definition = rail.RepliconServiceOperator(
            task_id='get_closure_date_filter_definition',
            endpoint='/services/UserListService1.svc/GetAllFilterDefinitions',
            data_handler=lambda response: ((rail.find_first_by_attr_and_get_attr(
                response, 'name', 'ClosureDate', 'uri')).split(':')[-1]).replace("-", "")
        )

        # Step 1: Get report details for User Timesheet Deletion Report
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.user_timesheet_deletion_report_name
        )

        # Step 2: Run the report and fetch data
        load_timesheet_deletion_report = rail.run_report2(
            group_id='load_timesheet_deletion_report',
            report_params=lambda :{
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details')['uri'],
                        "filterValues": custom_methods.get_report_filter(),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        # Step 3: Check if report has data - if not, stop gracefully
        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('load_timesheet_deletion_report.get_report_result','has_data') }}",
            yes_task='report_has_expected_columns',
            no_task="log_no_data_and_finish"
        )

        # Log no data scenario and finish gracefully
        log_no_data_and_finish = rail.WriteLogOperator(
            task_id="log_no_data_and_finish",
            message="User Timesheet Deletion Report contains no data. DAG completed successfully"
        )

        # Validate report has expected columns
        report_has_expected_columns = rail.IfOperator(
            task_id="report_has_expected_columns",
            #pylint: disable=consider-using-f-string
            test="{{ result('load_timesheet_deletion_report.get_report_result').reportGenerationResults[0].payload | starts_with('%s') }}" % config.expected_report_columns,
            yes_task="load_report_data_to_csv",
            no_task="fail_invalid_report_columns"
        )

        fail_invalid_report_columns = rail.FailOperator(
            task_id="fail_invalid_report_columns",
            message="User Timesheet Deletion Report does not have expected column structure"
        )

        # Load CSV data from report payload
        load_report_data_to_csv = rail.LoadCSVFileOperator(
            task_id="load_report_data_to_csv",
            document="{{ result('load_timesheet_deletion_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        # Step 4: Create collection of report data with specified fields
        create_timesheet_deletion_collection = rail.CreateCollectionOperator(
            task_id='create_timesheet_deletion_collection',
            source="{{ result('load_report_data_to_csv') }}",
            name='timesheet_deletion_data',
            columns={
                'User Name': 'user_name',
                'User Email': 'user_email', 
                'ClosureDate': 'closure_date',
                'Timesheet Start Date': 'timesheet_start_date',
                'Timesheet End Date': 'timesheet_end_date',
                'Timesheet Period': 'timesheet_period',
                'TimesheetPeriodUri': 'timesheet_period_uri',
                'remove_timesheets': 'remove_timesheets',
                'User End Date': 'user_end_date'
            }
        )

        # Step 5: Filter collection where remove_timesheets == "Yes"
        filter_timesheets_for_deletion = rail.QueryCollectionOperator(
            task_id='filter_timesheets_for_deletion',
            name='deletion_candidates',
            query="""SELECT * FROM timesheet_deletion_data WHERE closure_date == '{{ current_time('%m-%d-%Y') }}' AND UPPER(TRIM(remove_timesheets)) = 'YES' """
        )

        # Check if there are timesheets to delete
        check_deletion_candidates = rail.IfOperator(
            task_id='check_deletion_candidates',
            test="{{ result('filter_timesheets_for_deletion','length') > 0 }}",
            yes_task="prepare_timesheet_uris",
            no_task="log_no_deletions_and_finish"
        )

        log_no_deletions_and_finish = rail.WriteLogOperator(
            task_id="log_no_deletions_and_finish", 
            message="No timesheets marked for deletion found. DAG completed successfully.",
        )

        # Prepare timesheet URIs for deletion batch
        prepare_timesheet_uris = rail.PythonOperator(
            task_id='prepare_timesheet_uris',
            python_callable=lambda: [
                record['timesheet_period_uri'] 
                for record in rail.load_all_records(rail.result('filter_timesheets_for_deletion'))
                if record['timesheet_period_uri'] and record['timesheet_period_uri'].strip()
            ]
        )

        # Step 6: Create timesheet deletion batch using CreateTimesheetDeleteBatch API
        create_timesheet_delete_batch = rail.RepliconServiceOperator(
            task_id='create_timesheet_delete_batch',
            endpoint="/services/TimesheetService1.svc/CreateTimesheetDeleteBatch",
            data=lambda: {
                "timesheetUris": rail.result('prepare_timesheet_uris'),
                "deleteOptionUri": "urn:replicon:timesheet-delete-option:delete-overlapping-time-and-payable-time-entries"
            }
        )

        # Step 7: Execute the batch (similar to delete_future_entries_child.py pattern)
        execute_timesheet_delete_batch, wait_for_timesheet_delete_batch = rail.batch_execution(
            group_id='execute_timesheet_delete_batch',
            creation_task_id=create_timesheet_delete_batch.task_id,
        )

        # Get batch results and check for errors
        get_batch_results = rail.RepliconServiceOperator(
            task_id='get_batch_results',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDeleteBatchResults", 
            data={
                "timesheetDeleteBatchUri": "{{ result('create_timesheet_delete_batch') }}"
            }
        )

        if_any_errors_present = rail.IfOperator(
            task_id='if_any_errors_present',
            test="{{ result('get_batch_results').errors | is_truthy }}",
            yes_task='fail_batch_errors',
            no_task='finish_dag'
        )

        fail_batch_errors = rail.FailOperator(
            task_id='fail_batch_errors',
            message="Errors encountered during timesheet deletion batch execution: {{ result('get_batch_results').errors }}"
        )

        finish = rail.EmptyOperator(
            task_id='finish_dag'
        )

        # Batch task setup
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> finish
        can_run_batch_task >> rail.Label("No") >> get_closure_date_filter_definition

        # Define task dependencies
        get_closure_date_filter_definition >> get_report_details >> load_timesheet_deletion_report >> report_has_data
        
        # No data path
        report_has_data >> rail.Label('No') >> log_no_data_and_finish >> finish
        
        # Data processing path
        report_has_data >> rail.Label('Yes') >> report_has_expected_columns
        report_has_expected_columns >> rail.Label('Yes') >> load_report_data_to_csv
        report_has_expected_columns >> rail.Label('No') >> fail_invalid_report_columns
        
        load_report_data_to_csv >> create_timesheet_deletion_collection >> filter_timesheets_for_deletion
        filter_timesheets_for_deletion >> check_deletion_candidates
        
        # deletions path
        check_deletion_candidates >> rail.Label('No') >> log_no_deletions_and_finish >> finish
        check_deletion_candidates >> rail.Label('Yes') >> prepare_timesheet_uris >> create_timesheet_delete_batch
        
        # Batch execution path
        create_timesheet_delete_batch >> execute_timesheet_delete_batch >> wait_for_timesheet_delete_batch
        wait_for_timesheet_delete_batch >> get_batch_results >> if_any_errors_present
        
        if_any_errors_present >> rail.Label('No') >> finish
        if_any_errors_present >> rail.Label('Yes') >> fail_batch_errors

    return dag

rail.for_each_instance(create_child_dag)