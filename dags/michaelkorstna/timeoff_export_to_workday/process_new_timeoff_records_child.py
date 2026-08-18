"""
Process new time-off records from Replicon and prepare them for Workday export.

This DAG extracts new time-off records that haven't been exported yet,
validates the data, and triggers processing for valid records.
"""

from datetime import timedelta
from airflow.models import Variable
import rail
from michaelkorstna.timeoff_export_to_workday.utils import request_payload
from michaelkorstna.timeoff_export_to_workday.utils import custom_methods
from michaelkorstna.timeoff_export_to_workday.tasks.update_export_status import cancel_time_export
from rail.lib.ecid import get_dagrun_ecid

def create_dag(config):
    """Create DAG for processing new time-off records."""

    with rail.create_airflow_dag(
        dag_id=config.extract_new_bookings_child_dag_id,
        description=f'Michaelkors extract new timeoff records {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_new_records_runs
    ) as dag:

        # View configuration
        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config
        )

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.get_logging_details,
            op_args=[config.time_zone, 'New', config.export_file_prefix_new]
        )

        create_export = rail.RepliconServiceOperator(
            task_id='create_export',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportBatch',
            data=request_payload.create_time_data_export_batch_payload_new
        )

        execute_export, wait_for_export = rail.batch_execution(
            group_id='execute_time_export',
            creation_task_id=create_export.task_id,
            retries=0
        )

        get_export_uri = rail.RepliconServiceOperator(
            task_id='get_export_uri',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data={
                "timeDataExportBatchUri": "{{ result('" + create_export.task_id + "') }}"
            },
            data_handler=custom_methods.retrieve_export_uri
        )

        update_export_name = rail.RepliconServiceOperator(
            task_id="update_export_name",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data=lambda: request_payload.update_time_data_export_name_payload(get_export_uri.task_id, rail.result('logging_details')['time_export_filename'])
        )

        create_export_status_complete_batch = rail.RepliconServiceOperator(
            task_id='create_export_status_complete_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataExportStatusBatch',
            data=lambda: request_payload.create_export_status_complete_batch_payload(get_export_uri.task_id)
        )

        execute_export_status_complete_batch, wait_for_export_status_complete_batch = rail.batch_execution(
            group_id='execute_time_export_status_complete_batch',
            creation_task_id=create_export_status_complete_batch.task_id
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_download_batch'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_download_batch',
            end_task='finish',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=lambda dag_run: request_payload.create_time_data_download_batch_payload(dag_run, get_export_uri.task_id)
        )

        execute_download_batch, wait_for_download_batch = rail.batch_execution(
            group_id='execute_download_batch',
            creation_task_id=create_download_batch.task_id
        )

        get_download_url = rail.RepliconServiceOperator(
            task_id='get_download_url',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults',
            data={
                "timeDataDownloadBatchUri": "{{ result('" + create_download_batch.task_id + "') }}"
            },
            data_handler=lambda response: response['downloadUrl']
        )

        download_export = rail.HTTPDownloadFileOperator(
            task_id='download_export',
            url="{{ result('" + get_download_url.task_id + "') }}",
        )

        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{ result('" + download_export.task_id + "') }}",
            delimiter=','
        )

        create_timeexport_collection = rail.CreateCollectionOperator(
            task_id='create_timeexport_collection',
            name='timeoffdata',
            source='{{ result("load_export") }}',
            columns={
                "Employee ID": "employeeid",
                "Login Name": "loginname",
                "Time Off Booking ID": "timeoffbookingid",
                "Time Off Type Name": "timeofftypename",
                "Hours": "hours",
                "Approval Status": "approvalstatus",
                "Entry Date": "entrydate",
                "Time Off Type Description": "timeofftypedescription",
                "Comments": "comments"
            }
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("create_timeexport_collection", "length") > 0 }}',
            yes_task='create_log',
            no_task='update_export_name_to_no_data'
        )

        # Update export name when no data
        update_export_name_to_no_data = rail.RepliconServiceOperator(
            task_id="update_export_name_to_no_data",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data=lambda: request_payload.update_time_data_export_name_payload(get_export_uri.task_id, rail.result('logging_details')['time_export_filename_nodata'])
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        # Query for records with missing employee IDs
        query_invalid_employees = rail.QueryCollectionOperator(
            task_id='query_invalid_employees',
            query="""SELECT * FROM timeoffdata WHERE NULLIF(employeeid, "") IS NULL""",
        )

        # Check if we have invalid records
        check_invalid_records = rail.IfOperator(
            task_id='check_invalid_records',
            test="{{ result('query_invalid_employees', 'length') > 0 }}",
            yes_task="log_invalid_records",
            no_task="query_valid_records",
        )

        # Log invalid records
        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log='{{ result("create_log") }}',
            items='{{ result("query_invalid_employees") }}',
            message="Employeeid is missing in Replicon",
            properties=lambda item, dag_run:{
                'employeeid': item['employeeid'],
                'loginname': item['loginname'],
                'timeoffbookingid': item['timeoffbookingid'],
                'timeofftypename|timeoffdescription': f"{item['timeofftypename']}|{item['timeofftypedescription']}",
                'hours': item['hours'],
                'entrydate': item['entrydate'],
                'status': 'Exception',
                'details': 'Employeeid is missing in Replicon',
                'job_id': dag_run.conf["parent_ecid"],
                'transactiontype|childjob': f"New|{get_dagrun_ecid(dag_run)}"
            }
        )

        # Query for valid records to process
        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            query="""SELECT * FROM timeoffdata WHERE NULLIF(employeeid, "") IS NOT NULL AND timeofftypedescription != "" """,
        )

        if_valid_records_exist = rail.IfOperator(
            task_id='if_valid_records_exist',
            test="{{ result('query_valid_records', 'length') > 0 }}",
            yes_task="process_timeoff_records",
            no_task="finish",
        )

        process_timeoff_records = rail.EmptyOperator(
            task_id='process_timeoff_records'
        )

        # Trigger processing for each record
        trigger_process_timeoff_records = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_process_timeoff_records',
            items='{{ result("query_valid_records") }}',
            trigger_dag_id=config.process_timeoff_records_to_workday_dag_id,
            conf=lambda item, dag_run: {
                **item,
                "recordtype": "New"
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Wait for all record processing to complete
        wait_for_trigger_process_timeoff_records = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_process_timeoff_records',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_process_timeoff_records") }}'
        )

        # Gather logs from all process_records child DAG runs
        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs='{{ result("trigger_process_timeoff_records") }}',
            dagrun_task_id='create_log',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True
        )

        # Final task
        finish = rail.EmptyOperator(
            task_id='finish'
        )

        mark_timedata_export_error = rail.EmptyOperator(
            task_id='mark_timedata_export_error',
            trigger_rule='one_failed'
        )

        get_export_uri_failed = rail.RepliconServiceOperator(
            task_id='get_export_uri_failed',
            endpoint='/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults',
            data={
                "timeDataExportBatchUri": "{{ result('create_export') }}"
            },
            data_handler=custom_methods.retrieve_export_uri
        )

        mark_export_status_cancel_start, mark_export_status_cancel_end = cancel_time_export()

        fail_time_export = rail.FailOperator(
            task_id='fail_time_export',
            message='{{ get_error_message() }}'
        )

        dagrun_log_to_sumo = rail.EmptyOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done'
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_time_export',
            no_task='time_export_finish'
        )

        time_export_finish = rail.EmptyOperator(
            task_id='time_export_finish'
        )

        # Define task dependencies
        logging_details >> create_export >> execute_export >> wait_for_export >> get_export_uri >> update_export_name

        update_export_name >> create_export_status_complete_batch \
            >> execute_export_status_complete_batch >> wait_for_export_status_complete_batch
        wait_for_export_status_complete_batch >> rail.Label("On Error") >> mark_timedata_export_error
        wait_for_export_status_complete_batch >> rail.Label("On Success") >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> create_download_batch
        create_download_batch >> execute_download_batch >> wait_for_download_batch >> get_download_url
        get_download_url >> download_export >> load_export >> create_timeexport_collection >> has_data

        has_data >> rail.Label('No') >> update_export_name_to_no_data >> finish
        has_data >> rail.Label('Yes') >> create_log >> query_invalid_employees >> check_invalid_records

        check_invalid_records >> rail.Label('Yes') >> log_invalid_records >> query_valid_records
        check_invalid_records >> rail.Label('No') >> query_valid_records

        query_valid_records >> if_valid_records_exist
        if_valid_records_exist >> rail.Label("Yes") >> process_timeoff_records \
            >> trigger_process_timeoff_records >> wait_for_trigger_process_timeoff_records >> gather_child_logs >> finish
        if_valid_records_exist >> rail.Label("No") >> finish

        mark_timedata_export_error >> get_export_uri_failed >> mark_export_status_cancel_start
        mark_export_status_cancel_end >> finish
        finish >> dagrun_log_to_sumo >> should_fail_dag
        should_fail_dag >> rail.Label("Yes") >> fail_time_export
        should_fail_dag >> rail.Label("No") >> time_export_finish

    return dag

rail.for_each_instance(create_dag)
