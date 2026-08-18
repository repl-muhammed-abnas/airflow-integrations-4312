
from datetime import timedelta, datetime
import rail
from bccsstechnologyservices.update_paid_invoice.send_logs import get_send_logs

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long, bad-indentation
    with rail.create_airflow_dag(
        dag_id=f'bccss_phsa_update_paid_invoice_master_{config.instance}',
        description=f'BCCSSTechnologyServices PHSA Update Paid Invoice - Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(minutes=15),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        start=rail.EmptyOperator(
            task_id='start',
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Paid Invoice Update - Incorrect File Format - {{ current_time_in_specified_tz() }}',
            html_content='templates/email/bad_file_format.html',
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_input_webhooks',
            no_task='delete_this_dagrun',
        )

        archive_input_webhooks = rail.SFTPMoveFileOperator(
            task_id='archive_input_webhooks',
            sftp_conn_id=config.sftp_conn_id,
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_file_path + "/" +
            (datetime.now()).strftime("%m%d%YT%H%S%M") + "_" + ("{{ result('new_file_sensor')  | split('/') | last }}"),
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_inputfile_csv = rail.LoadCSVFileOperator(
            task_id='load_inputfile_csv',
            headers=['Invoice #','Payment Date'],
            document="{{ result('download_file') }}"
        )

        create_invoice_input_collection = rail.CreateCollectionOperator(
            task_id='create_invoice_input_collection',
            source="{{ result('load_inputfile_csv') }}",
            name="invoice_input_list",
            columns={
                'Invoice #': 'Invoice_Number',
                'Payment Date': 'Payment Date'
            }
        )

        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{ result('create_invoice_input_collection', 'length') > 0 }}",
            yes_task='get_all_invoices',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Paid Invoice Update - No records in the file {{ current_time_in_specified_tz() }}',
            html_content="templates/email/blank_file.html"
        )

        get_all_invoices = rail.QueryCollectionOperator(
            task_id='get_all_invoices',
            query="""SELECT * FROM invoice_input_list""",
        )

        get_query_data = rail.PythonOperator(
            task_id='get_query_data',
            python_callable=lambda: rail.load_all_records(
                rail.result("get_all_invoices")),
        )

        process_update_paid_invoice = rail.TriggerDagRunForEachItemOperator(
            task_id='process_update_paid_invoice',
            retries=0,
            items=lambda:[x for x in rail.result('get_query_data') if x["Invoice_Number"] and x["Payment_Date"]],
            trigger_dag_id=f'bccss_phsa_update_paid_invoice_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'invoice_number': item["Invoice_Number"],
                'payment_date': item["Payment_Date"]
            }
        )

        wait_for_process_update_paid_invoice = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_paid_invoice',
            dag_runs='{{ result("process_update_paid_invoice") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        download_file_from_address = rail.SFTPDownloadFileOperator(
            task_id='download_file_from_address',
            remote_filepath=config.from_address_file_path + "{{ result('new_file_sensor') | file_name | replace('.csv', '.txt') }}"
        )

        load_fromaddress_csv = rail.LoadCSVFileOperator(
            task_id='load_fromaddress_csv',
            headers = None,
            document="{{ result('download_file_from_address') }}"
        )

        def get_from_data_func(from_address_data):
            if from_address_data:
                return from_address_data
            return ""

        get_from_address = rail.PythonOperator(
            task_id='get_from_address',
            python_callable=lambda:rail.read_artifact(rail.result("load_fromaddress_csv"))
        )

        check_from_data = rail.PythonOperator(
            task_id='check_from_data',
            python_callable=lambda: get_from_data_func(rail.result("get_from_address"))
        )

        send_logs_enter, _ = get_send_logs(config, "{{ result('check_from_data') }}")

        start >> new_file_sensor >> is_csv >> rail.Label("No") >> send_bad_file_format_email >> finish

        is_csv >> rail.Label("Yes") >> download_file >> load_inputfile_csv >> create_invoice_input_collection >> has_any_records
        has_any_records >> rail.Label("No") >> send_blank_payload_email >> finish
        download_file >> was_new_file_found >> rail.Label("Yes") >> archive_input_webhooks
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        has_any_records >> rail.Label("Yes") >> get_all_invoices >> get_query_data >> process_update_paid_invoice >> wait_for_process_update_paid_invoice\
            >>download_file_from_address >> load_fromaddress_csv >> get_from_address\
        >> check_from_data >> send_logs_enter >> finish
    return dag


rail.for_each_instance(create_dag)
