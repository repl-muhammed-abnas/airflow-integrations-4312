from datetime import timedelta, datetime
from os import path
import hashlib
import rail


null = None


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/oxfordfinancial/client_import_advisor/config.py


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'oxfordfinancial_client_import_advisor_master_dag_{config.instance}',
        description=f'New Client import file in SFTP_Advisor {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        is_advisor_file = rail.IfOperator(
            task_id='is_advisor_file',
            test="{{ result('new_file_sensor') | file_base | lower | matches('advisor') }}",
            yes_task='download_file',
            no_task='should_fail_dag'
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        def get_dagrun_start_time(start_time):
            return datetime.fromisoformat(start_time).strftime('%m%d%YT%H%M%S')
        get_time_for_file = rail.PythonOperator(
            task_id='get_time_for_file',
            python_callable=get_dagrun_start_time,
            op_args=['{{ dag_run.start_date }}']
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test="{{ get_task_state('new_file_sensor') == 'success' and result('is_advisor_file') == 'download_file' }}",
            yes_task='archive_file',
            no_task='delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('get_time_for_file') }}_{{ result('new_file_sensor') | file_base }}.csv"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('download_file') }}",
            headers=['SF_18_Digit_ID', 'Advisor_Full_Name', 'Household_Firm_Name', 'Contact_Status', 'Email', 'Household_Firm_18_Digit_ID',
                     'Household_Firm_15_Digit_ID', 'SF_15_Digit_ID', 'IsDeleted', 'First_Name', 'Middle_Name', 'Last_Name']
        )

        def get_row(item):
            def get_md5(item):
                column_vals = [str(v) if v else '' for k, v in item.items() if k in ('SF_18_Digit_ID',
                                                                                     'Advisor_Full_Name',
                                                                        'Household_Firm_Name', 'Contact_Status',
                                                                        'Email', 'Household_Firm_18_Digit_ID',
                                                                        'Household_Firm_15_Digit_ID',
                                                                        'SF_15_Digit_ID', 'IsDeleted',
                                                                        'First_Name', 'Middle_Name', 'Last_Name')]
                input_reference = hashlib.md5(
                    ','.join(column_vals).encode('utf-8'))
                return input_reference.hexdigest()
            return [
                item['SF_18_Digit_ID'],
                item['Advisor_Full_Name'],
                item['Household_Firm_Name'],
                item['Contact_Status'],
                item['Email'],
                item['Household_Firm_18_Digit_ID'],
                item['Household_Firm_15_Digit_ID'],
                item['SF_15_Digit_ID'],
                item['IsDeleted'],
                item['First_Name'],
                item['Middle_Name'],
                item['Last_Name'],
                get_md5(item)
            ]
        compose_csv = rail.WriteCSVFileOperator(
            task_id='compose_csv',
            source="{{ result('parse_csv') }}",
            header=['SF_18_Digit_ID',
                    'Advisor_Full_Name',
                    'Household_Firm_Name',
                    'Contact_Status',
                    'Email',
                    'Household_Firm_18_Digit_ID',
                    'Household_Firm_15_Digit_ID',
                    'SF_15_Digit_ID',
                    'IsDeleted',
                    'First_Name',
                    'Middle_Name',
                    'Last_Name',
                    'Md5'
                    ],
            row=get_row
        )

        create_inputfile_collection = rail.CreateCollectionOperator(
            task_id='create_inputfile_collection',
            source="{{ result('compose_csv') }}",
            name="inputfile"
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            remote_filepath=config.reference_file
        )

        load_reference_file_csv = rail.LoadCSVFileOperator(
            task_id="load_reference_file_csv",
            document="{{ result('download_reference_file') }}"
        )

        create_referencefile_collection = rail.CreateCollectionOperator(
            task_id='create_referencefile_collection',
            source="{{ result('load_reference_file_csv') }}",
            name="referencefile"
        )

        query_changed_records = rail.QueryCollectionOperator(
            task_id='query_changed_records',
            query="""SELECT * FROM inputfile
                    WHERE Md5 NOT IN (SELECT DISTINCT Md5 FROM referencefile)"""
        )

        query_unchanged_records = rail.QueryCollectionOperator(
            task_id='query_unchanged_records',
            query="""SELECT * FROM inputfile
                    WHERE Md5 IN (SELECT DISTINCT Md5 FROM referencefile)"""
        )

        is_unchanged_records = rail.IfOperator(
            task_id='is_unchanged_records',
            test="{{ result('query_unchanged_records', 'length') > 0 }}",
            yes_task='create_unchangedrecords_log',
            no_task='is_changed_records'
        )

        create_unchangedrecords_log = rail.CreateLogOperator(
            task_id='create_unchangedrecords_log'
        )

        write_unchanged_records = rail.WriteLogOperator(
            task_id='write_unchanged_records',
            log="{{ result('create_unchangedrecords_log') }}",
            severity='Skipped',
            message='Skipped Users',
            items="{{ result('query_unchanged_records') }}",
            properties={
                'sf18digitid': '{{ item.SF_18_Digit_ID }}',
                'status': 'Skipped',
                'reason': 'No change in client record '
            }
        )

        is_changed_records = rail.IfOperator(
            task_id='is_changed_records',
            test="{{ result('query_changed_records', 'length') > 0 }}",
            yes_task='get_required_client_custom_fields',
            no_task='process_log_generation'
        )

        get_required_client_custom_fields = rail.RepliconServiceOperator(
            task_id='get_required_client_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={"objectUri": "urn:replicon:object-type:client"},
            data_handler=lambda response: {
                'household_name_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Household Name', 'uri', ''),
                'household_firmid_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Household_Firm_ID', 'uri', ''),
                'salesforce_id_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Salesforce_ID', 'uri', ''),
                'contact_status_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Contact Status', 'uri', '')
            }
        )

        get_required_project_custom_fields = rail.RepliconServiceOperator(
            task_id='get_required_project_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={"objectUri": "urn:replicon:object-type:project"},
            data_handler=lambda response: {
                'service_name_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Service Name', 'uri', '')
            }
        )

        trigger_client_import_create_update_advisor_client = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_client_import_create_update_advisor_client',
            retries=0,
            items="{{ result('query_changed_records') }}",
            trigger_dag_id=f'oxfordfinancial_client_import_advisor_child_create_update_client_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf={
                "SF_18_Digit_ID": "{{ item.SF_18_Digit_ID }}",
                "Advisor_Full_Name": "{{ item.Advisor_Full_Name }}",
                "Household_Firm_Name": "{{ item.Household_Firm_Name }}",
                "Contact_Status": "{{ item.Contact_Status }}",
                "Email": "{{ item.Email }}",
                "Household_Firm_18_Digit_ID": "{{ item.Household_Firm_18_Digit_ID }}",
                "Household_Firm_15_Digit_ID": "{{ item.Household_Firm_15_Digit_ID }}",
                "SF_15_Digit_ID": "{{ item.SF_15_Digit_ID }}",
                "IsDeleted": "{{ item.IsDeleted }}",
                "Household_Name_Custom_Field": "{{ result('get_required_client_custom_fields').household_name_uri }}",
                "Household_Firm_Id_Custom_Field": "{{ result('get_required_client_custom_fields').household_firmid_uri }}",
                "Salesforce_Id_Custom_Field": "{{ result('get_required_client_custom_fields').salesforce_id_uri }}",
                "Contact_Status_Custom_Field": "{{ result('get_required_client_custom_fields').contact_status_uri }}",
                "Service_Name_Custom_Field": "{{ result('get_required_project_custom_fields').service_name_uri }}"
            }
        )

        wait_for_client_import_create_update_advisor_client = rail.WaitForDagRunsSensor(
            task_id='wait_for_client_import_create_update_advisor_client',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_client_import_create_update_advisor_client") }}'
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            dag_runs="{{ result('trigger_client_import_create_update_advisor_client') }}",
            dagrun_task_id='create_log',
            flatten=True
        )

        def get_logs():
            logs = []
            unchanged_records_log = rail.result('create_unchangedrecords_log')
            if unchanged_records_log:
                logs.append(unchanged_records_log)
            gathered_logs = rail.result('gather_logs')
            if gathered_logs:
                logs.extend(gathered_logs)
            return logs
        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            trigger_dag_id=f'oxfordfinancial_client_import_advisor_child_log_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda: {
                "filename": path.split(rail.result('new_file_sensor'))[1],
                "logs": get_logs()
            }
        )

        rename_reference_file = rail.SFTPMoveFileOperator(
            task_id='rename_reference_file',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_reference_household.csv",
            existing_filename=config.reference_file
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            content="{{ result('compose_csv') }}",
            remote_filepath=config.reference_file
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            trigger_rule='all_done',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_dag',
            no_task='process_logtosumo'
        )

        fail_dag = rail.FailOperator(
            task_id='fail_dag',
            message="{{ get_error_message() }}"
        )

        process_logtosumo = rail.EmptyOperator(
            task_id='process_logtosumo'
        )

        check_if_new_file_found = rail.IfOperator(
            task_id='check_if_new_file_found',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='dagrun_log_to_sumo'
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            extra_info={
                'Filename': "{{ result('new_file_sensor') | file_base }}"
            }
        )

        new_file_sensor >> is_advisor_file

        is_advisor_file >> rail.Label(
            'Yes') >> download_file >> get_time_for_file

        get_time_for_file >> rail.Label(
            'Always') >> was_new_file_found
        was_new_file_found >> rail.Label(
            'Yes') >> archive_file
        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun
        get_time_for_file >> parse_csv >> compose_csv >> create_inputfile_collection >> \
            download_reference_file >> load_reference_file_csv >> create_referencefile_collection >> query_changed_records >> \
            query_unchanged_records >> is_unchanged_records

        is_unchanged_records >> rail.Label(
            'Yes') >> create_unchangedrecords_log >> write_unchanged_records >> is_changed_records

        is_unchanged_records >> rail.Label(
            'No') >> is_changed_records

        is_changed_records >> rail.Label(
            'Yes') >> get_required_client_custom_fields >> get_required_project_custom_fields >> \
            trigger_client_import_create_update_advisor_client >> \
            wait_for_client_import_create_update_advisor_client >> gather_logs >> \
            process_log_generation

        is_changed_records >> rail.Label(
            'No') >> process_log_generation

        process_log_generation >> rename_reference_file >> upload_new_reference_file >> should_fail_dag

        is_advisor_file >> rail.Label(
            'No') >> should_fail_dag

        should_fail_dag >> rail.Label(
            'Yes') >> fail_dag

        should_fail_dag >> rail.Label(
            'No') >> process_logtosumo >> check_if_new_file_found

        check_if_new_file_found >> rail.Label(
            'Yes') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
