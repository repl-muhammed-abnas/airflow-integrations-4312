from datetime import timedelta
from os import path
from airflow.models import Variable
import rail
from rail.filters import split

from moodys.user_sync.split_input_data_based_on_country.tasks.move_to_processing import move_to_processing_task_group

# pylint: disable=too-many-statements


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description='Moodys User Sync',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
        )

        is_pgp = rail.IfOperator(
            task_id='is_pgp',
            test='{{ result("new_file_sensor") | file_ext | lower == "pgp" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User Sync - Incorrect Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/bad_file_format.html"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        can_decrypt_file = rail.IfOperator(
            task_id="can_decrypt_file",
            test=Variable.get(config.can_decrypt_file_var_name,
                              default_var='true').lower() == 'true',
            yes_task='decrypt_file',
            no_task='dummy_load_data'
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        dummy_load_data = rail.PythonOperator(
            task_id="dummy_load_data",
            python_callable=lambda: rail.result('decrypt_file') if Variable.get(
                config.can_decrypt_file_var_name, default_var='true').lower() == 'true' else rail.result('download_file'),
            show_return_value_in_logs=False
        )

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('dummy_load_data') }}",
            delimiter="|"
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                'Country ID': 'countryid',
                'Login Name': 'loginname',
                'Employee ID': 'employeeid',
                'Date of Birth': 'dateofbirth',
                'Rehire': 'rehire',
                'Start Date': 'startdate',
                'LastName': 'lastname',
                'FirstName': 'firstname',
                'Last day worked': 'lastdayworked',
                'EndDate': 'enddate',
                'Email ID': 'emailid',
                'Time Zone': 'timezone',
                'Language': 'language',
                'PN Flag': 'pnflag',
                'ADP File#': 'adpfile',
                'FTE%': 'ftepercent',
                'Employee Category': 'employeecategory',
                'Actual Working hours': 'actualworkinghrs',
                'Statutory limit': 'statutorylimit',
                'Effective Date': 'effectivedate',
                'Employee Type Name': 'employeetypename',
                'Division Name': 'divisionname',
                'Location Name': 'locationname',
                'Location Code': 'locationcode',
                'Company Name': 'companyname',
                'Company Code': 'companycode',
                'Supervisor ID/Emp ID': 'supervisorid',
                'Supervisor First name': 'supervisorfirstname',
                'Supervisor Last name': 'supervisorlastname',
                'Supervisor Email ID': 'supervisoremailid',
                'Job Title': 'jobtitle',
            }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task=['query_non_permitted_country_records',
                      'query_permitted_country_records'],
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User Sync - no records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        query_non_permitted_country_records = rail.QueryCollectionOperator(
            task_id="query_non_permitted_country_records",
            query=f"""Select * from inputdatacollection where countryid NOT IN ({str(config.ALLOWED_COUNTRIES)[1:-1]})""",
        )

        has_non_permitted_country_records = rail.IfOperator(
            task_id='has_non_permitted_country_records',
            test="{{ result('query_non_permitted_country_records','length') > 0 }}",
            yes_task="log_non_permitted_country_records",
        )

        log_non_permitted_country_records = rail.WriteLogOperator(
            task_id="log_non_permitted_country_records",
            items="{{result('query_non_permitted_country_records')}}",
            severity="Skipped",
            message="Record Skipped belong to non-permitted Country",
            properties=lambda item: {
                "countryid": item['countryid'],
                "loginname": item['loginname'],
                "lastname": item['lastname'],
                "firstname": item['firstname'],
                "action": "Validation",
                "status": "Skipped"
            }
        )

        create_skip_logs_csv = rail.WriteCSVFileOperator(
            task_id='create_skip_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                'Country ID',
                'Login Name',
                'LastName',
                'FirstName',
                'Action',
                'Status',
                'Details',
                'JobId'
            ],
            row=[
                '{{ item.properties.countryid }}',
                '{{ item.properties.loginname }}',
                '{{ item.properties.lastname }}',
                '{{ item.properties.firstname }}',
                '{{ item.properties.action }}',
                '{{ item.properties.status }}',
                '{{ item.message }}',
                '{{ item.ecid }}'

            ]
        )

        upload_skip_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_skip_log_to_sftp',
            content='{{ result("create_skip_logs_csv") }}',
            remote_filepath=config.log_filepath +
            '/log_skipped_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv'
        )

        query_permitted_country_records = rail.QueryCollectionOperator(
            task_id="query_permitted_country_records",
            query=f"""Select * from inputdatacollection where countryid IN ({str(config.ALLOWED_COUNTRIES)[1:-1]})""",
        )

        has_permitted_country_records = rail.IfOperator(
            task_id='has_permitted_country_records',
            test="{{ result('query_permitted_country_records','length') > 0 }}",
            yes_task="get_file_name",
            no_task='send_no_permitted_countries_records_mail'
        )

        send_no_permitted_countries_records_mail = rail.EmailOperator(
            task_id='send_no_permitted_countries_records_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User Sync - no allowed countries records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/no_permitted_record_email.html"
        )

        get_file_name = rail.PythonOperator(
            task_id='get_file_name',
            python_callable=lambda: split(string=path.split(
                rail.result("new_file_sensor"))[1], separator=".")[0]
        )

        dummy_move_to_procesing_each_country = rail.EmptyOperator(
            task_id="dummy_move_to_procesing_each_country"
        )

        move_to_processing_lithuania = move_to_processing_task_group("lithuania", "LT",
                                                                     "{{result('get_file_name')}}", config.lithuania_processing_filepath)

        move_to_processing_costa_rica = move_to_processing_task_group("costa_rica", "CR",
                                                                      "{{result('get_file_name')}}", config.costa_rica_processing_filepath)

        move_to_processing_united_states = move_to_processing_task_group("united_states", "US",
                                                                         "{{result('get_file_name')}}", config.united_states_processing_filepath)

        move_to_processing_canada = move_to_processing_task_group("canada", "CA",
                                                                  "{{result('get_file_name')}}", config.canada_processing_filepath)

        move_to_processing_france = move_to_processing_task_group("france", "FR",
                                                                  "{{result('get_file_name')}}", config.france_processing_filepath)

        move_to_processing_japan = move_to_processing_task_group("japan", "JP",
                                                                 "{{result('get_file_name')}}", config.japan_processing_filepath)

        move_to_processing_germany = move_to_processing_task_group("germany", "DE",
                                                                   "{{result('get_file_name')}}", config.germany_processing_filepath)

        new_file_sensor >> is_pgp >> rail.Label(
            'Yes') >> download_file >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun

        download_file >> can_decrypt_file >> rail.Label(
            'Yes') >> decrypt_file >> dummy_load_data
        can_decrypt_file >> rail.Label('No') >> dummy_load_data >> load_data

        is_pgp >> rail.Label('No') >> send_bad_file_format_email
        load_data >> create_input_data_collection >> has_input_data
        has_input_data >> rail.Label('No') >> send_blank_payload_email

        has_input_data >> rail.Label('Yes') >> [
            query_non_permitted_country_records, query_permitted_country_records]

        query_non_permitted_country_records >> has_non_permitted_country_records
        has_non_permitted_country_records >> rail.Label(
            'Yes') >> log_non_permitted_country_records >> create_skip_logs_csv >> upload_skip_log_to_sftp

        query_permitted_country_records >> has_permitted_country_records >> rail.Label(
            'No') >> send_no_permitted_countries_records_mail

        has_permitted_country_records >> rail.Label(
            'Yes') >> get_file_name >> dummy_move_to_procesing_each_country

        dummy_move_to_procesing_each_country >> [move_to_processing_lithuania, move_to_processing_costa_rica, move_to_processing_united_states,
                                                 move_to_processing_canada, move_to_processing_france, move_to_processing_japan, move_to_processing_germany]

    return dag


rail.for_each_instance(create_main_dag)
