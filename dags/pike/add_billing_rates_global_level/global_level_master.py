from datetime import timedelta
from pike.add_billing_rates_global_level.utils import custom_methods
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pike_adding_billing_rates_global_level_master_{config.instance}',
        description=f'Pike Adding Billing Rates Global level Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval = timedelta(seconds=config.schedule_interval),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id = 'download_file',
            remote_filepath = "{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id = 'was_new_file_found',
            trigger_rule = 'all_done',
            test = '{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task = 'archive_processed_file',
            no_task = 'delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        archive_processed_file = rail.SFTPMoveFileOperator(
            task_id='archive_processed_file',
            new_filename=config.archive_filepath + '/{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv',
            existing_filename=config.input_filepath+'/{{ result("new_file_sensor") | file_name }}',
        )

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('download_file') }}",
            encoding='utf-8-sig'
        )

        create_billing_rates_input_collection = rail.CreateCollectionOperator(
            task_id='create_billing_rates_input_collection',
            source="{{ result('load_data') }}",
            columns={
                "Billing Rate Name": "Billingratename",
                "Description": "description",
                "Bill Rate ": "billrate",
                "Action": "action"
            },
            name="billingratesinputdata",
        )

        has_billing_rates_data = rail.IfOperator(
            task_id='has_billing_rates_data',
            test="{{ result('create_billing_rates_input_collection','length') > 0 }}",
            yes_task='get_all_billing_rates',
            no_task='finish'
        )

        get_all_billing_rates = rail.RepliconServiceOperator(
            task_id="get_all_billing_rates",
            endpoint="/services/BillingRateService1.svc/GetAllBillingRates"
        )

        process_billing_rate = rail.TriggerDagRunForEachItemOperator(
            task_id='process_billing_rate',
            items='{{ result("create_billing_rates_input_collection") }}',
            trigger_dag_id=f'pike_adding_billing_rates_global_level_child_{config.instance}',
            conf=custom_methods.get_process_billing_rate_payload
        )

        wait_for_process_billing_rate = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_billing_rate",
            dag_runs="{{result('process_billing_rate')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        filter_master_logs = rail.FilterLogEntriesOperator(
            task_id='filter_master_logs',
            log='{{ get_master_log()}}',
            filter_callable=custom_methods.do_filter_log
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source='{{ result("filter_master_logs") }}',
            header=["Billingrate", "action", "results"],
            row=['{{ item.properties.billing_rate_name }}', '{{ item.properties.action }}', '{{ item.properties.results }}']
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='Processedjobs_{{ dag_run_ecid() | replace(":", "-") }}.csv',
            expires_in_seconds=config.download_link_validity,
        )

        download_email_file = rail.SFTPDownloadFileOperator(
            task_id='download_email_file',
            remote_filepath=config.email_id_path + '/{{ result("new_file_sensor") | file_base }}.txt'
        )

        get_email_from_file = rail.PythonOperator(
            task_id = "get_email_from_file",
            python_callable= custom_methods.get_txt_file_data_callable
        )

        send_complete_email = rail.EmailOperator(
            task_id='send_complete_email',
            to='{{ result("get_email_from_file") }},' +config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Request to add billing rate - Completed",
            html_content='/templates/emails/complete_email.html'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        new_file_sensor >> download_file >> was_new_file_found
        was_new_file_found >> rail.Label("Yes") >> archive_processed_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        download_file >> load_data >> create_billing_rates_input_collection >> has_billing_rates_data

        has_billing_rates_data >> rail.Label("Yes") >> get_all_billing_rates >> process_billing_rate \
            >> wait_for_process_billing_rate >> filter_master_logs >> render_logs_csv >> generate_download_link \
                >> download_email_file >> get_email_from_file >> send_complete_email
        has_billing_rates_data >> rail.Label("No") >> finish

    return dag

rail.for_each_instance(create_dag)
