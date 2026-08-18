# pylint: disable=too-many-statements, line-too-long
from datetime import timedelta
import rail
from daimlertrucks.cbfc_export_eng.utils import python_callable, request_payload
null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'{config.company_key}_dtna_cbfc_export_eng_{config.instance}',
        description=f'Live|DTNA_CbFC_Export_ENG_Prod {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.master_dag_schedule_interval,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_the_task = rail.IfOperator(
            task_id='can_run_the_task',
            test=python_callable.check_for_trigger_day,
            yes_task='get_required_data_for_run',
            no_task=''
        )

        get_required_data_for_run = rail.PythonOperator(
            task_id='get_required_data_for_run',
            python_callable=python_callable.get_required_date_data
        )

        get_all_costcenters = rail.RepliconServiceOperator(
            task_id='get_all_costcenters',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data=request_payload.get_all_costcenter_payload
        )

        create_list_of_costcenters = rail.PythonOperator(
            task_id='create_list_of_costcenters',
            python_callable=python_callable.get_cost_center_list
        )

        create_collection_of_costcenterdata = rail.CreateCollectionOperator(
            task_id='create_collection_of_costcenterdata',
            source=lambda: rail.result('create_list_of_costcenters'),
            name="costcenterdata",
        )
        get_costcenter_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_costcenter_report_details',
            report_name=config.report1_name
        )

        query_costcenter_with_dtna_eng = rail.QueryCollectionOperator(
            task_id='query_costcenter_with_dtna_eng',
            query="""SELECT * FROM  costcenterdata WHERE ( costcenterdata.fullpath LIKE '%DTNA ENG' OR  costcenterdata.fullpath LIKE '%DTNA ENG%') AND  costcenterdata.status='True'""",
        )

        costcenters_list_to_process = rail.PythonOperator(
            task_id='costcenters_list_to_process',
            python_callable=python_callable.get_cost_center_list_to_process
        )

        create_report_generation_batch = rail.RepliconServiceOperator(
            task_id='create_report_generation_batch',
            endpoint="/services/reportservice1.svc/CreateReportGenerationBatch",
            data=request_payload.get_report_generate_batch_payload
        )

        trigger_dag_run_generate_report_batch_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_generate_report_batch_child',
            retries=0,
            items=lambda: [rail.result('create_report_generation_batch')],
            trigger_dag_id=f'{config.company_key}_dtna_cbfc_export_eng_generatereportbatch_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "reporturi": rail.result("get_costcenter_report_details")["uri"],
                "batchUri": item
            }
        )

        wait_for_completion_trigger_dag_run_generate_report_batch_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_generate_report_batch_child',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_generate_report_batch_child") }}'
        )

        get_report_batch_results = rail.RepliconServiceOperator(
            task_id='get_report_batch_results',
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data={'reportGenerationBatchUri':  "{{ result('create_report_generation_batch') }}"
                  },
        )

        parse_csv_for_batch_results = rail.LoadCSVFileOperator(
            task_id="parse_csv_for_batch_results",
            document="{{ result('get_report_batch_results').reportGenerationResults[0].payload }}",
        )

        get_formated_the_batch_result = rail.PythonOperator(
            task_id='get_formated_the_batch_result',
            python_callable=python_callable.get_result_formated
        )

        join_both_the_arrays = rail.PythonOperator(
            task_id='join_both_the_arrays',
            python_callable=lambda:  rail.result('get_formated_the_batch_result').get(
                'plus') + rail.result('get_formated_the_batch_result').get('minus')
        )

        compose_csv_file = rail.WriteCSVFileOperator(
            task_id='compose_csv_file',
            source="{{ result('join_both_the_arrays') | to_json }}",
            header=None,
            row=lambda item: {
                "column_0": item['sendingsystemid'],
                "column_1": item['documenttypecode'],
                "column_2": item['postingdate'],
                "column_3": item['documentheadertext'],
                "column_4": item['documentdate'],
                "column_5": item['debitcostcenterid'],
                "column_6": item['ccreferencefield_1'],
                "column_7": item['debititemtext'],
                "column_8": item['ioreferencefield_1'],
                "column_9": item['ioreferencefield_2'],
                "column_10": item['ioreferencefield_3'],
                "column_11": item['quantitysign'],
                "column_12": item['quantity'],
                "column_13": item['baseunitofmeasure'],
                "column_14": item['controllingareaid'],
                "column_15": item['activitytypecode']
            }.values(),
            # footer=[],
        )

        list_sftp_files = rail.SFTPListFilesOperator(
            task_id='list_sftp_files',
            paths=[config.sftp_filepath]
        )
        check_if_file_present = rail.IfOperator(
            task_id='check_if_file_present',
            test="{{ result('list_sftp_files') | length > 0 }}",
            yes_task="foreach_file_in_directory",
            no_task="upload_file_to_outbound_directory",
        )
        foreach_file_in_directory = rail.ForEachOperator(
            task_id='foreach_file_in_directory',
            items=lambda: rail.result('list_sftp_files').get(
                config.sftp_filepath),
            start_task='check_if_filename_present',
            end_task='foreach_file_in_directory_end'
        )

        check_if_filename_present = rail.IfOperator(
            task_id='check_if_filename_present',
            test="{{ result('foreach_file_in_directory').name | is_truthy }}",
            yes_task="get_file_path_to_archive",
            no_task="foreach_file_in_directory_end",
        )

        get_file_path_to_archive = rail.PythonOperator(
            task_id='get_file_path_to_archive',
            python_callable=lambda: python_callable.get_existing_filename(
                config.sftp_filepath, config.sftp_Archive_filepath)
        )

        rename_movethefilefrom_outbound_to_archive = rail.SFTPMoveFileOperator(
            task_id='rename_movethefilefrom_outbound_to_archive',
            existing_filename="{{ result('get_file_path_to_archive').get('existing_filename') }}",
            new_filename="{{ result('get_file_path_to_archive').get('new_filename') }}"
        )

        foreach_file_in_directory_end = rail.EmptyOperator(
            task_id='foreach_file_in_directory_end',
        )

        upload_file_to_outbound_directory = rail.SFTPUploadFileOperator(
            task_id='upload_file_to_outbound_directory',
            content="{{ result('compose_csv_file') }}",
            remote_filepath=config.sftp_filepath +
            '/DTNA-512_TimeDirectActivityAllocation_Eng_' +
            "{{ result('get_required_data_for_run').get('date_today_format1') }}.csv",
        )

        send_mail_for_success = rail.EmailOperator(
            task_id='send_mail_for_success',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''DaimlerTrucks- CbFC Export for ENG Department completed''',
            html_content="templates/email.html",
            params=None,
        )

        can_run_the_task >> rail.Label(
            'Yes') >> get_required_data_for_run
        get_required_data_for_run >> get_all_costcenters >> create_list_of_costcenters >> create_collection_of_costcenterdata >> get_costcenter_report_details >> query_costcenter_with_dtna_eng
        query_costcenter_with_dtna_eng >> costcenters_list_to_process >> create_report_generation_batch
        create_report_generation_batch >> trigger_dag_run_generate_report_batch_child >> wait_for_completion_trigger_dag_run_generate_report_batch_child >> get_report_batch_results >> parse_csv_for_batch_results >> get_formated_the_batch_result
        get_formated_the_batch_result >> join_both_the_arrays >> compose_csv_file >> list_sftp_files >> check_if_file_present
        check_if_file_present >> rail.Label(
            'Yes') >> foreach_file_in_directory >> check_if_filename_present
        check_if_file_present >> rail.Label(
            'No') >> upload_file_to_outbound_directory >> send_mail_for_success

        check_if_filename_present >> rail.Label(
            'Yes') >> get_file_path_to_archive >> rename_movethefilefrom_outbound_to_archive >> foreach_file_in_directory_end
        check_if_filename_present >> rail.Label(
            'No') >> foreach_file_in_directory_end
        foreach_file_in_directory >> foreach_file_in_directory_end >> upload_file_to_outbound_directory >> send_mail_for_success

    return dag


rail.for_each_instance(create_dag)
