
from datetime import timedelta
from bccsstechnologyservices.create_invoices_for_each_project_code.utils import python_callable
import rail
null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long, unnecessary-lambda
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'{config.company_key}_create_invoices_for_each_project_code_master{dag_id_postfix}',
        description=f'When there is a new sheet row added in Google Sheets, make request via HTTP and other actions{dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.schedule_interval),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_file_path,
            soft_fail_timeout=timedelta(minutes=10)
        )

        download_input_file = rail.SFTPDownloadFileOperator(
            task_id='download_input_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ result('new_file_sensor') }}",
        )
        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_input_file',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        archive_input_file = rail.SFTPMoveFileOperator(
            task_id='archive_input_file',
            # trigger_rule='none_skipped',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.input_file_archive_path +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}",
        )

        check_if_csv = rail.IfOperator(
            task_id='check_if_csv',
            test='''{{ result('new_file_sensor') | ends_with('.csv')}}''',
            yes_task="parse_csv",
            no_task="finish",
        )
        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            delimiter=',',
            document="{{ result('download_input_file') }}",
        )

        compose_csv_for_collection = rail.WriteCSVFileOperator(
            task_id='compose_csv_for_collection',
            source="{{ result('parse_csv') }}",
            header=["client_name",
                    "client_uri",
                    "project_code",
                    "project_uri",
                    "start_date",
                    "end_date"],
            row=lambda item: [
                item["Client Name"],
                item["Client URI"],
                item["Project Code"],
                item["Project URI"],
                item["Start Date"],
                item["End Date"]
            ]
        )
        create_collection_from_referencedata = rail.CreateCollectionOperator(
            task_id='create_collection_from_referencedata',
            source="{{ result('compose_csv_for_collection') }}",
            name="clientdata",
            # todo update this map from actual csv header for key name
            columns=["client_name",
                     "client_uri",
                     "project_code",
                     "project_uri",
                     "start_date",
                     "end_date"
                     ]
        )

        query_records_list = rail.QueryCollectionOperator(
            task_id='query_records_list',
            query="""SELECT * FROM  clientdata WHERE clientdata.client_uri like 'urn:replicon-tenant%'""",
        )

        process_entry_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_entry_child',
            items=lambda: rail.result('query_records_list'),
            trigger_dag_id=f'{config.company_key}_create_invoice_for_project_child{dag_id_postfix}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
            conf=lambda item: {"client_name": item["client_name"],
                               "client_uri": item["client_uri"],
                               "project_code": item["project_code"],
                               "project_uri": item["project_uri"],
                               "start_date": item["start_date"],
                               "end_date": item["end_date"]}
        )
        wait_for_process_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_child',
            dag_runs='{{ result("process_entry_child") }}',
            execution_timeout=timedelta(days=14),
        )
        download_email_file = rail.SFTPDownloadFileOperator(
            task_id='download_email_file',
            remote_filepath=config.email_file_path +
            "/{{ result('new_file_sensor') | file_name | replace('.csv', '.txt')}}",
        )
        archive_email_file = rail.SFTPMoveFileOperator(
            task_id='archive_email_file',
            existing_filename=config.email_file_path +
            "/{{ result('new_file_sensor') | file_name | replace('.csv', '.txt') }}",
            new_filename=config.email_file_archive_path +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name | replace('.csv', '.txt') }}",
        )
        get_email_ids = rail.PythonOperator(
            task_id='get_email_ids',
            python_callable=python_callable.get_email_file_data
        )

        send_mail_for_success = rail.EmailOperator(
            task_id='send_mail_for_success',
            to="{{ result('get_email_ids') }}",
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Request to create invoices by project code - Completed''',
            html_content="templates/email.html",
            params=None,
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        new_file_sensor >> download_input_file >> check_if_csv
        download_input_file >> rail.Label("Always") >> was_new_file_found
        was_new_file_found >> rail.Label("Yes") >> archive_input_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        check_if_csv >> rail.Label(
            "Yes") >> parse_csv >> compose_csv_for_collection >> create_collection_from_referencedata \
            >> query_records_list >> process_entry_child >> wait_for_process_child >> download_email_file >> get_email_ids \
            >> send_mail_for_success >> finish
        check_if_csv >> rail.Label("No") >> finish
        download_email_file >> rail.Label("Always") >> archive_email_file

    return dag


rail.for_each_instance(create_dag)
