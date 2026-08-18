from datetime import timedelta
import itertools
from pendulum import datetime
import rail
from unisys.resource_assignment_export.utils import custom_methods


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.scheduled_master_dag_id,
        description=f'Unisys Resource Assignment Export Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2025, 1, 1, tz=config.timezone),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        # Step 1: Retrieve webhook log entries since last export
        get_webhook_log = rail.CreateLogOperator(
            task_id="get_webhook_log",
            tenant_wide_name=config.webhook_log_name,
            existing_log_mode="truncate"
        )

        # Step 2: Check if there's any data to export
        has_any_data = rail.HasDataOperator(
            task_id="has_any_data",
            source="{{ result('get_webhook_log', 'truncated_data') }}",
            yes_task='write_csv_webhook_log'
        )

        write_csv_webhook_log = rail.WriteCSVFileOperator(
            task_id="write_csv_webhook_log",
            source="{{ result('get_webhook_log', 'truncated_data') }}",
            header=[
                'resource_uri',
                'event_type',
                'project_uri',
                'user_uri',
                'modified_date'],
            row=['{{ item.properties.resource_uri }}', '{{ item.properties.event_type }}', '{{ item.properties.project_uri }}', '{{ item.properties.user_uri }}',
                 '{{ item.properties.modified_date }}'],
        )

        # Step 3: Create collection of webhook events
        create_events_collection = rail.CreateCollectionOperator(
            task_id='create_events_collection',
            source="{{ result('write_csv_webhook_log') }}",
            name='resource_allocation_events'
        )

        # Step 4: Query for unique resource URIs with latest modified_date
        # Get only the LATEST event for each resource allocation (not all event types)
        query_unique_allocations = rail.QueryCollectionOperator(
            task_id='query_unique_allocations',
            query="""SELECT a.resource_uri, a.event_type, a.project_uri, a.user_uri, a.modified_date as latest_modified_date
                     FROM resource_allocation_events a
                     INNER JOIN (
                         SELECT resource_uri, MAX(modified_date) as max_date
                         FROM resource_allocation_events
                         GROUP BY resource_uri
                     ) b ON a.resource_uri = b.resource_uri AND a.modified_date = b.max_date""",
            name='unique_resource_allocations'
        )

        # Step 5: Run reports to get all users and projects
        # User Report
        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_user_report_details",
            report_name=config.user_base_report_name
        )

        generate_user_report = rail.run_report2(
            group_id="generate_user_report",
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_user_report_details')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id
        )

        load_user_report_data = rail.LoadCSVFileOperator(
            task_id='load_user_report_data',
            document="{{ result('generate_user_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_user_report_collection = rail.CreateCollectionOperator(
            task_id="create_user_report_collection",
            source="{{ result('load_user_report_data') }}",
            name="user_report_collection",
            columns= {
                'Employee ID': 'Employee_ID',
                'UserUri': 'UserUri',
            }
        )

        # Project Report
        get_project_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_project_report_details",
            report_name=config.project_base_report_name
        )

        generate_project_report = rail.run_report2(
            group_id="generate_project_report",
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_project_report_details')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id
        )

        load_project_report_data = rail.LoadCSVFileOperator(
            task_id='load_project_report_data',
            document="{{ result('generate_project_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_project_report_collection = rail.CreateCollectionOperator(
            task_id="create_project_report_collection",
            source="{{ result('load_project_report_data') }}",
            name="project_report_collection",
            columns= {
                'Project Code': 'Project_Code',
                'ProjectUri': 'ProjectUri',
            }
        )

        query_valid_allocation_records = rail.QueryCollectionOperator(
            task_id="query_valid_allocation_records",
            query="""SELECT 
                        v.*,
                        p.Project_Code as project_code,
                        u.Employee_ID as employee_id 
                            FROM unique_resource_allocations v 
                            INNER JOIN project_report_collection p 
                                ON v.project_uri = p.ProjectUri 
                            INNER JOIN user_report_collection u 
                                ON v.user_uri = u.UserUri""",
            name="valid_user_records"
        )

        # Step 6: Trigger child DAG for each unique allocation
        process_each_allocation_record = rail.trigger_parallel_dagrun(
            task_id='process_each_allocation_record',
            trigger_dag_id=config.allocation_details_child_dag_id,
            items="{{ result('query_valid_allocation_records') }}",
            parallel_count= config.parallel_child_dag_count,
            conf=lambda item: {
                'allocation_id': item['resource_uri'].split(":")[-1],
                'resource_uri': item['resource_uri'],
                'event_type': item['event_type'],
                'project_uri': item['project_uri'],
                'user_uri': item['user_uri'],
                'project_code': item['project_code'],
                'employee_id': item['employee_id'],
            },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_process_projects_dag_ids =rail.PythonOperator(
            task_id= 'get_process_projects_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_each_allocation_record_{x+1}'), range(config.parallel_child_dag_count))))),
            show_return_value_in_logs= False
        )

        # Step 7: Gather results from child DAG runs
        gather_allocation_results = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_allocation_results',
            dag_runs="{{ result('get_process_projects_dag_ids') }}",
            dagrun_task_id='prepare_allocation_row',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            flatten=True
        )

        # Step 8: Generate CSV file
        generate_csv = rail.WriteCSVFileOperator(
            task_id="generate_csv",
            source="{{ result('gather_allocation_results') | to_json }}",
            header=[
                'Allocation Id',
                'Action',
                'Project Code',
                'Employee ID',
                'AllocationStartDate',
                'AllocationEndDate',
                'AllocatedHours'
            ],
            row=[
                '{{ item["Allocation Id"] }}',
                '{{ item["Action"] }}',
                '{{ item["Project Code"] }}',
                '{{ item["Employee ID"] }}',
                '{{ item["AllocationStartDate"] }}',
                '{{ item["AllocationEndDate"] }}',
                '{{ item["AllocatedHours"] }}'
            ]
        )

        # Step 9: Get file name based on instance and date
        get_file_name = rail.PythonOperator(
            task_id="get_file_name",
            python_callable=lambda: custom_methods.get_file_name_for_instance(config)
        )

        # Step 10: Encrypt CSV with PGP
        encrypt_file = rail.PGPEncryptionOperator(
            task_id="encrypt_file",
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('generate_csv') }}"
        )

        # Step 11: Upload encrypted file to SFTP
        upload_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_sftp',
            content= '{{ result("encrypt_file") }}' if config.enable_encryption else '{{ result("generate_csv") }}',
            remote_filepath=f"{config.sftp_remote_path}/{{{{ result('get_file_name') }}}}.pgp"
        )

        # Step 12: Send success email notification
        send_success_email = rail.EmailOperator(
            task_id='send_success_email',
            to=config.tenant_email,
            subject='{{ get_company_key() + " | Replicon Resource Assignment Export is completed successfully - " + current_time_in_specified_tz() }}',
            html_content="templates/email/export_complete.html",
            params={
                'log_filepath': config.sftp_remote_path
            }
        )

        # Task flow
        get_webhook_log >> has_any_data
        
        # Main processing path
        has_any_data >> rail.Label("Yes") >> write_csv_webhook_log >> create_events_collection >> query_unique_allocations
        
        # Run user report tasks
        query_unique_allocations >> get_user_report_details >> generate_user_report >> load_user_report_data >> create_user_report_collection
        
        # Run project report tasks  
        query_unique_allocations >> get_project_report_details >> generate_project_report >> load_project_report_data >> create_project_report_collection
        
        # Wait for both report collections before triggering child DAGs
        [create_user_report_collection, create_project_report_collection] >> query_valid_allocation_records >> process_each_allocation_record
        
        process_each_allocation_record >> get_process_projects_dag_ids >> gather_allocation_results
        gather_allocation_results >> generate_csv >> get_file_name >> encrypt_file >> upload_to_sftp >> send_success_email

    return dag


rail.for_each_instance(create_dag)