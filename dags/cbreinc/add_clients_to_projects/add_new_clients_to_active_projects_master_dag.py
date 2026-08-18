
from datetime import timedelta
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'cbreinc_add_clients_to_projects_add_new_clients_to_active_projects_master_{config.instance}',
        description=f'CBREInc - Add new clients to active projects - Master V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
        default_args={
        },
    ) as dag:

        get_new_clients_log = rail.CreateLogOperator(
            task_id="get_new_clients_log",
            tenant_wide_name="cbreinc_add_clients_to_projects_new_clients",
            existing_log_mode="truncate",
        )

        has_any_data = rail.IfOperator(
            task_id="has_any_data",
            test=lambda: bool(rail.load_all_records(
                rail.result('get_new_clients_log', 'truncated_data'))),
            yes_task='write_csv_backup_data',
            no_task='finish'
        )

        write_csv_backup_data = rail.WriteCSVFileOperator(
            task_id='write_csv_backup_data',
            source="{{ result('get_new_clients_log', 'truncated_data') }}",
            header=[
                    'uri',
                    'name',
                    'jobdatetime',
            ],
            row=['{{ item.properties.uri }}', '{{ item.properties.name }}',
                 '{{ item.properties.jobdatetime }}'],
        )

        invoke_custom_ruby_code_9 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_9',
            python_callable=lambda: list(map(lambda x: {
                "client": {
                    "uri": x['properties']['uri'],
                },
                "costAllocationPercentage": "0"
            }, rail.load_all_records(rail.result('get_new_clients_log', 'truncated_data'))))
        )

        log_getallclientsuri_10 = rail.RepliconServiceOperator(
            task_id='log_getallclientsuri_10',
            endpoint="/services/ClientService1.svc/GetActiveClients",
            data_handler=lambda data: list(map(lambda x: {
                "client": {
                    "uri": x['uri'],
                },
                "costAllocationPercentage": "0"
            }, data)
            )
        )

        get_report_details_11 = rail.RepliconReportDetailsOperator(
            task_id='get_report_details_11',
            report_name='Project details  for projectsync',
        )

        generate_reports_batch_12 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_12',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data={"reportParameters": [
                    {
                        "reportUri": "{{ result('get_report_details_11').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_generate_reports_batch_12 = rail.batch_execution(
            group_id='execute_execute_generate_reports_batch_12',
            creation_task_id='generate_reports_batch_12',
        )

        get_report_batch_results_13 = rail.RepliconServiceOperator(
            task_id='get_report_batch_results_13',
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data={
                'reportGenerationBatchUri': "{{ result('generate_reports_batch_12') }}"},
        )

        if_first_payload_not_contains_nodata_16 = rail.IfOperator(
            task_id='if_first_payload_not_contains_nodata_16',
            test='''{{ result('get_report_batch_results_13').reportGenerationResults | is_truthy and not result('get_report_batch_results_13').reportGenerationResults[0].payload |  matches('No Data') }}''',
            yes_task="if_first_payload_not_starts_with_projectnameprojecturi_17",
            no_task="finish",
        )

        if_first_payload_not_starts_with_projectnameprojecturi_17 = rail.IfOperator(
            task_id='if_first_payload_not_starts_with_projectnameprojecturi_17',
            test='''{{ (result('get_report_batch_results_13').reportGenerationResults[0].payload | starts_with('Project Name,ProjectUri')) | is_falsy }}''',
            yes_task="stop_18",
            no_task="parse_csv_readingreportdata_19",
        )

        stop_18 = rail.FailOperator(
            task_id='stop_18',
            message='''Base report column does not match'''
        )

        parse_csv_readingreportdata_19 = rail.LoadCSVFileOperator(
            task_id='parse_csv_readingreportdata_19',
            document="{{ result('get_report_batch_results_13').reportGenerationResults[0].payload }}",
        )

        def get_split_lsit_project_uri():
            data = [rec['ProjectUri'] for rec in rail.load_all_records(
                rail.result('parse_csv_readingreportdata_19'))]
            final_list = [{"project_uri_chunk":data[i:i+5]} for i in range(0, len(data), 5)]
            return final_list

        load_project_uri_from_csv = rail.PythonOperator(
            task_id='load_project_uri_from_csv',
            python_callable=get_split_lsit_project_uri
        )
        
        trigger_child_batch_client_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_batch_client_assignment',
            items="{{ result('load_project_uri_from_csv') | to_json }}",
            trigger_dag_id=f'cbreinc_add_clients_to_projects_project_sync_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "projecturis": item['project_uri_chunk'],
                "clients": rail.result('log_getallclientsuri_10')
            }
        )

        wait_for_trigger_child_batch_client_assignment = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_child_batch_client_assignment',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_child_batch_client_assignment") }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        get_new_clients_log >> has_any_data
        has_any_data >> rail.Label('yes') >> write_csv_backup_data
        has_any_data >> rail.Label('No') >> finish
        write_csv_backup_data >> invoke_custom_ruby_code_9 >> log_getallclientsuri_10 >> get_report_details_11 >> generate_reports_batch_12 >> execute_generate_reports_batch_12[
            0] >> execute_generate_reports_batch_12[1] >> get_report_batch_results_13 >> if_first_payload_not_contains_nodata_16
        if_first_payload_not_contains_nodata_16 >> rail.Label(
            'Yes') >> if_first_payload_not_starts_with_projectnameprojecturi_17
        if_first_payload_not_starts_with_projectnameprojecturi_17 >> rail.Label(
            'Yes') >> stop_18
        if_first_payload_not_starts_with_projectnameprojecturi_17 >> rail.Label(
            'No') >> parse_csv_readingreportdata_19 >> load_project_uri_from_csv >> trigger_child_batch_client_assignment >> wait_for_trigger_child_batch_client_assignment >> finish
        if_first_payload_not_contains_nodata_16 >> rail.Label(
            'No') >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
