
from datetime import timedelta
from airflow.models import Variable
import rail
from macquariegroup.clientimport.task.generate_report_batch import report_batch
from macquariegroup.clientimport.utils import request_payload, custom_methods

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'macquarie_process_clientimport_child_{config.instance}',
        description=f'Macquarie - Process_ClientImport_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log_client_import'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_log_client_import',
            end_task='catch_and_log_errors',
        )

        create_log_client_import = rail.CreateLogOperator(
            task_id='create_log_client_import'
        )

        generate_report = rail.EmptyOperator(task_id='generate_report')

        get_report_details, create_report_collection, fail_no_report_data = report_batch(
            config)

        send_mail_no_report_data = rail.EmailOperator(
            task_id='send_mail_no_report_data',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }}| Client Import - Import not processed - {{ dag_run.conf.time }}''',
            html_content='templates/email/no_report.html',
        )

        download_client_csv_file = rail.SFTPDownloadFileOperator(
            task_id='download_client_csv_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.input_filepath + '/' + config.client_csv_file
        )

        load_client_csv_file = rail.LoadCSVFileOperator(
            task_id='load_client_csv_file',
            document='{{ result("download_client_csv_file") }}'
        )

        download_reference_client_csv_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_client_csv_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_filepath +
            '/' + config.reference_client_csv_file
        )

        load_reference_client_csv_file = rail.LoadCSVFileOperator(
            task_id='load_reference_client_csv_file',
            document='{{ result("download_reference_client_csv_file") }}'
        )

        download_bu_csv_file = rail.SFTPDownloadFileOperator(
            task_id='download_bu_csv_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.input_filepath + '/' + config.bu_csv_file
        )

        load_bu_csv_file = rail.LoadCSVFileOperator(
            task_id='load_bu_csv_file',
            document='{{ result("download_bu_csv_file") }}',
            headers=['businessunit', 'businessunitname',
                     'businessgroup', 'businessdivision']
        )

        def get_row_data(item):
            row_data = []
            for v in item.values():
                row_data.append(v.strip())
            return row_data

        create_bu_csv_file = rail.WriteCSVFileOperator(
            task_id='create_bu_csv_file',
            source='{{ result("load_bu_csv_file") }}',
            header=['businessunit', 'businessunitname',
                    'businessgroup', 'division'],
            row=get_row_data
        )

        download_locations_csv_file = rail.SFTPDownloadFileOperator(
            task_id='download_locations_csv_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.input_filepath + '/' + config.locations_csv_file
        )

        load_locations_csv_file = rail.LoadCSVFileOperator(
            task_id='load_locations_csv_file',
            document='{{ result("download_locations_csv_file") }}',
            headers=['location', 'locationdescription']
        )

        create_locations_csv_file = rail.WriteCSVFileOperator(
            task_id='create_locations_csv_file',
            source='{{ result("load_locations_csv_file") }}',
            header=['location', 'locationdescription'],
            row=get_row_data
        )

        create_collection_from_client_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_client_csv',
            source='{{ result("load_client_csv_file") }}',
            name='rawinputfile',
            columns={
                'ClientName': 'clientname',
                'ClientCode': 'clientcode',
                'Location': 'location',
                'ClientDisabled': 'disabled'
            }
        )

        query_client_raw_collection = rail.QueryCollectionOperator(
            task_id='query_client_raw_collection',
            query="""SELECT * FROM rawinputfile""",
        )

        has_client_name = rail.IfOperator(
            task_id='has_client_name',
            test='{{ result("query_client_raw_collection", "length") > 0 }}',
            yes_task='create_csv_encode_from_client_raw_collection',
            no_task='send_mail_client_csv_empty',
        )

        send_mail_client_csv_empty = rail.EmailOperator(
            task_id='send_mail_client_csv_empty',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject="{{ get_company_key() }} | Client Import - No records in 'Client' file - {{ dag_run.conf.time }}",
            html_content='templates/email/no_records_in_client_csv.html'
        )

        create_csv_encode_from_client_raw_collection = rail.WriteCSVFileOperator(
            task_id='create_csv_encode_from_client_raw_collection',
            source='{{ result("query_client_raw_collection") }}',
            header=['Clientname',
                    'Clientcode',
                    'Location',
                    'Encoded'],
            row=custom_methods.get_csv_rows
        )

        load_csv_encode_from_client_raw = rail.LoadCSVFileOperator(
            task_id='load_csv_encode_from_client_raw',
            document='{{ result("create_csv_encode_from_client_raw_collection") }}',
        )

        create_inputfile_collection = rail.CreateCollectionOperator(
            task_id='create_inputfile_collection',
            source='{{ result("load_csv_encode_from_client_raw") }}',
            name='inputfile',
            columns={
                'Clientname': 'clientname',
                'Clientcode': 'clientcode',
                'Location': 'location',
                'Encoded': 'encoded'
            }
        )

        load_csv_from_client_referencefile = rail.LoadCSVFileOperator(
            task_id='load_csv_from_client_referencefile',
            document='{{ result("load_reference_client_csv_file") }}',
        )

        create_referencefile_collection = rail.CreateCollectionOperator(
            task_id='create_referencefile_collection',
            source='{{ result("load_csv_from_client_referencefile") }}',
            name='rawreference',
            columns={
                'ClientName': 'clientname',
                'ClientCode': 'clientcode',
                'Location': 'location',
                'ClientDisabled': 'disabled'
            }
        )

        query_client_reference_collection = rail.QueryCollectionOperator(
            task_id='query_client_reference_collection',
            query="""SELECT * FROM  rawreference""",
        )

        create_csv_encode_from_reference_raw_collection = rail.WriteCSVFileOperator(
            task_id='create_csv_encode_from_reference_raw_collection',
            source='{{ result("query_client_reference_collection") }}',
            header=['Clientname',
                    'Clientcode',
                    'Location',
                    'Encoded'],
            row=custom_methods.get_csv_rows
        )

        load_csv_encode_from_referencefile = rail.LoadCSVFileOperator(
            task_id='load_csv_encode_from_referencefile',
            document='{{ result("create_csv_encode_from_reference_raw_collection") }}',
        )

        create_reference_collection = rail.CreateCollectionOperator(
            task_id='create_reference_collection',
            source='{{ result("load_csv_encode_from_referencefile") }}',
            name='reference',
            columns={
                'Clientname': 'clientname',
                'Clientcode': 'clientcode',
                'Location': 'location',
                'Encoded': 'encoded'
            }
        )

        query_for_client_deltas = rail.QueryCollectionOperator(
            task_id='query_for_client_deltas',
            query="""SELECT * FROM inputfile WHERE encoded NOT IN (SELECT DISTINCT encoded FROM reference)""",
            name="deltavalues",
        )

        has_delta_values = rail.IfOperator(
            task_id='has_delta_values',
            test='{{ result("query_for_client_deltas", "length") > 0 }}',
            yes_task='query_existing_clients_tobe_updated',
            no_task='query_unchanged_clients',
        )

        query_existing_clients_tobe_updated = rail.QueryCollectionOperator(
            task_id='query_existing_clients_tobe_updated',
            # pylint: disable=line-too-long
            query="""SELECT allclients.uri, deltavalues.clientname, deltavalues.clientcode, deltavalues.location, deltavalues.encoded FROM allclients INNER JOIN deltavalues ON allclients.clientname=deltavalues.clientname""",
        )

        query_unchanged_clients = rail.QueryCollectionOperator(
            task_id='query_unchanged_clients',
            query="""SELECT * FROM inputfile WHERE encoded IN (SELECT DISTINCT encoded FROM reference)""",
        )

        query_new_clients = rail.QueryCollectionOperator(
            task_id='query_new_clients',
            query="""SELECT * FROM rawinputfile WHERE NOT EXISTS (SELECT * FROM allclients WHERE allclients.clientname=rawinputfile.clientname)""",
        )

        query_clients_tobe_disabled = rail.QueryCollectionOperator(
            task_id='query_clients_tobe_disabled',
            query="""SELECT * FROM allclients WHERE clientname NOT IN (SELECT clientname FROM inputfile) AND clientstatus='Enabled'""",
        )

        def check_delta_threshold():
            new_clients_count = rail.result(
                'query_new_clients', 'length') if rail.result('query_new_clients') else 0
            update_clients_count = rail.result('query_existing_clients_tobe_updated', 'length') if rail.result(
                'query_existing_clients_tobe_updated') else 0
            disable_clients_count = rail.result('query_clients_tobe_disabled', 'length') if rail.result(
                'query_clients_tobe_disabled') else 0
            return (new_clients_count + update_clients_count + disable_clients_count) > config.delta_threshold

        is_exceeding_delta_threshold = rail.IfOperator(
            task_id='is_exceeding_delta_threshold',
            test=check_delta_threshold,
            yes_task='send_mail_threshold_exceeded',
            no_task='create_collection_from_bu_csv',
        )

        send_mail_threshold_exceeded = rail.EmailOperator(
            task_id='send_mail_threshold_exceeded',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }}| Client Import Skipped - {{ dag_run.conf.time }}''',
            html_content='templates/email/threshold_exceeded.html',
        )

        create_collection_from_bu_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_bu_csv',
            source='{{ result("create_bu_csv_file") }}',
            name='rawbufile'
        )

        query_bu_raw_collection = rail.QueryCollectionOperator(
            task_id='query_bu_raw_collection',
            query="""SELECT * FROM  rawbufile""",
        )

        has_business_unit = rail.IfOperator(
            task_id='has_business_unit',
            test='{{ result("query_bu_raw_collection", "length") > 0 }}',
            yes_task='create_collection_from_locations_csv',
            no_task='send_mail_bu_csv_empty',
        )

        send_mail_bu_csv_empty = rail.EmailOperator(
            task_id='send_mail_bu_csv_empty',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject="{{ get_company_key() }} | Client Import - No records in 'BU' file - {{ dag_run.conf.time }}",
            html_content='templates/email/no_records_in_bu_csv.html'
        )

        create_collection_from_locations_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_locations_csv',
            source='{{ result("create_locations_csv_file") }}',
            name='rawlocationsfile'
        )

        query_locations_raw_collection = rail.QueryCollectionOperator(
            task_id='query_locations_raw_collection',
            query="""SELECT * FROM  rawlocationsfile""",
        )

        has_locations = rail.IfOperator(
            task_id='has_locations',
            test='{{ result("query_locations_raw_collection", "length") > 0 }}',
            yes_task="get_customfield_groups",
            no_task="send_mail_locations_csv_empty"
        )

        send_mail_locations_csv_empty = rail.EmailOperator(
            task_id='send_mail_locations_csv_empty',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject="{{ get_company_key() }} | Client Import - No records in 'Locations' file - {{ dag_run.conf.time }}",
            html_content='templates/email/no_records_in_locations_csv.html'
        )

        get_customfield_groups = rail.RepliconServiceOperator(
            task_id='get_customfield_groups',
            endpoint='/services/CustomFieldService1.svc/GetCustomFieldGroups',
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], 'displayText', 'Client', 'uri')
        )

        def get_required_client_custom_fields(response):
            response = response.json()['d']
            if not response:
                return []

            return {
                "groupuri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Group', 'uri'),
                "divisionuri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Division', 'uri'),
                "locationnameuri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Location', 'uri'),
                "businessunitnameuri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Business Unit Name', 'uri')
            }

        get_all_client_custom_fields = rail.RepliconServiceOperator(
            task_id="get_all_client_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{result('get_customfield_groups')}}"
            },
            response_filter=get_required_client_custom_fields
        )

        trigger_dag_run_client_add_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_client_add_child',
            retries=0,
            batch_size=config.batch_size,
            items='{{ result("query_new_clients") }}',
            trigger_dag_id=f'macquarie_client_add_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, index, dag_run: request_payload.get_conf_client(
                item, index, dag_run, action='add')
        )

        wait_for_completion_trigger_dag_run_client_add_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_client_add_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_client_add_child") }}'
        )

        trigger_dag_run_client_disable_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_client_disable_child',
            retries=0,
            batch_size=config.batch_size,
            items='{{ result("query_clients_tobe_disabled") }}',
            trigger_dag_id=f'macquarie_client_disable_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, index, dag_run: request_payload.get_conf_client(
                item, index, dag_run, action='disable')
        )

        wait_for_completion_trigger_dag_run_client_disable_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_client_disable_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_client_disable_child") }}'
        )

        has_updates_to_client = rail.IfOperator(
            task_id='has_updates_to_client',
            test=lambda: rail.result("query_existing_clients_tobe_updated") and rail.result(
                "query_existing_clients_tobe_updated", "length") > 0,
            yes_task='trigger_dag_run_client_update_child',
            no_task='check_new_records'
        )

        trigger_dag_run_client_update_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_client_update_child',
            retries=0,
            batch_size=config.batch_size,
            items='{{ result("query_existing_clients_tobe_updated") }}',
            trigger_dag_id=f'macquarie_client_update_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, index, dag_run: request_payload.get_conf_client(
                item, index, dag_run, action='update')
        )

        wait_for_completion_trigger_dag_run_client_update_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_client_update_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_client_update_child") }}'
        )

        check_new_records = rail.IfOperator(
            task_id='check_new_records',
            test=lambda: bool((rail.result('query_existing_clients_tobe_updated') and rail.result('query_existing_clients_tobe_updated', 'length') > 0)
                              or rail.result('query_new_clients', 'length') > 0
                              or rail.result('query_clients_tobe_disabled', 'length') > 0),
            yes_task='gather_client_add_logs',
            no_task='send_mail_no_new_records'
        )

        gather_client_add_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_client_add_logs',
            dag_runs='{{ result("trigger_dag_run_client_add_child") }}',
            dagrun_task_id='create_log_per_client_add',
            flatten=True
        )

        gather_client_disable_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_client_disable_logs',
            dag_runs='{{ result("trigger_dag_run_client_disable_child") }}',
            dagrun_task_id='create_log_per_client_disable',
            flatten=True
        )

        has_updates_client_logs = rail.IfOperator(
            task_id='has_updates_client_logs',
            test=lambda: rail.result("query_existing_clients_tobe_updated") and rail.result(
                "query_existing_clients_tobe_updated", "length") > 0,
            yes_task='gather_client_update_logs',
            no_task='trigger_client_import_log_generation'
        )

        gather_client_update_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_client_update_logs',
            dag_runs='{{ result("trigger_dag_run_client_update_child") }}',
            dagrun_task_id='create_log_per_client_update',
            flatten=True
        )

        trigger_client_import_log_generation = rail.TriggerDagRunOperator(
            task_id='trigger_client_import_log_generation',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'macquarie_client_import_loggeneration_{config.instance}',
            conf=lambda dag_run: {
                'client_import_logs': rail.result('create_log_client_import'),
                'client_add_logs': rail.result('gather_client_add_logs'),
                'client_update_logs': rail.result('gather_client_update_logs') if rail.result('gather_client_update_logs') else [],
                'client_disable_logs': rail.result('gather_client_disable_logs'),
                'parentjobid': dag_run.conf['parentjobid'],
                'time': dag_run.conf['time']
            }
        )

        send_mail_no_new_records = rail.EmailOperator(
            task_id='send_mail_no_new_records',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Client Import - No new records to process  - {{ dag_run.conf.time }}''',
            html_content='templates/email/no_new_records_in_client_csv.html',
        )

        rename_and_move_reference_client_to_archive = rail.SFTPMoveFileOperator(
            task_id='rename_and_move_reference_client_to_archive',
            new_filename=config.archive_filepath +
            '/{{ dag_run.conf.time }}_{{ dag_run_ecid() | replace(":", "-") }}_Old_Reference_Client.csv',
            existing_filename=config.reference_filepath +
            '/' + config.reference_client_csv_file,
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id='upload_new_reference_file',
            content='{{ result("download_client_csv_file") }}',
            remote_filepath=config.reference_filepath +
            '/' + config.reference_client_csv_file,
        )

        rename_and_move_client_to_archive = rail.SFTPMoveFileOperator(
            task_id='rename_and_move_client_to_archive',
            new_filename=config.archive_filepath +
            '/{{ dag_run.conf.time }}_{{ dag_run_ecid() | replace(":", "-") }}_Raw_Input_Client.csv',
            existing_filename=config.input_filepath + '/' + config.client_csv_file,
        )

        upload_bu_file_copy_to_archive = rail.SFTPUploadFileOperator(
            task_id='upload_bu_file_copy_to_archive',
            content='{{ result("download_bu_csv_file") }}',
            remote_filepath=config.archive_filepath +
            '/{{ dag_run.conf.time }}_{{ dag_run_ecid() | replace(":", "-") }}_BU.csv',
        )

        upload_locations_file_copy_to_archive = rail.SFTPUploadFileOperator(
            task_id='upload_locations_file_copy_to_archive',
            content='{{ result("download_locations_csv_file") }}',
            remote_filepath=config.archive_filepath +
            '/{{ dag_run.conf.time }}_{{ dag_run_ecid() | replace(":", "-") }}_Locations.csv',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_log_client_import") }}',
            trigger_rule='one_failed',
            severity='Error',
            message=config.error_template,
            properties={
                'details': {config.error_template}
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info=custom_methods.get_extra_info
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> create_log_client_import

        create_log_client_import >> generate_report >> get_report_details

        create_report_collection >> download_client_csv_file >> load_client_csv_file >> \
            download_reference_client_csv_file >> load_reference_client_csv_file >> \
            download_bu_csv_file >> load_bu_csv_file >> create_bu_csv_file >> download_locations_csv_file >> load_locations_csv_file \
            >> create_locations_csv_file >> create_collection_from_client_csv >> query_client_raw_collection >> has_client_name

        fail_no_report_data >> send_mail_no_report_data >> finish

        has_client_name >> rail.Label('Yes') >> create_csv_encode_from_client_raw_collection >> load_csv_encode_from_client_raw \
            >> create_inputfile_collection >> load_csv_from_client_referencefile >> \
            create_referencefile_collection >> query_client_reference_collection >> \
            create_csv_encode_from_reference_raw_collection >> load_csv_encode_from_referencefile >> \
            create_reference_collection >> query_for_client_deltas >> has_delta_values
        has_client_name >> rail.Label(
            'No') >> send_mail_client_csv_empty >> finish

        has_delta_values >> rail.Label(
            'Yes') >> query_existing_clients_tobe_updated >> query_unchanged_clients
        has_delta_values >> rail.Label(
            'No') >> query_unchanged_clients >> query_new_clients >> query_clients_tobe_disabled >> \
            is_exceeding_delta_threshold

        is_exceeding_delta_threshold >> rail.Label(
            'Yes') >> send_mail_threshold_exceeded >> rename_and_move_reference_client_to_archive
        is_exceeding_delta_threshold >> rail.Label(
            'No') >> create_collection_from_bu_csv >> query_bu_raw_collection >> has_business_unit

        has_business_unit >> rail.Label(
            'Yes') >> create_collection_from_locations_csv >> query_locations_raw_collection >> has_locations
        has_business_unit >> rail.Label(
            'No') >> send_mail_bu_csv_empty >> finish

        has_locations >> rail.Label('Yes') >> get_customfield_groups >> get_all_client_custom_fields \
            >> trigger_dag_run_client_add_child >> wait_for_completion_trigger_dag_run_client_add_child \
            >> trigger_dag_run_client_disable_child >> wait_for_completion_trigger_dag_run_client_disable_child \
            >> has_updates_to_client

        has_updates_to_client >> rail.Label('Yes') >> trigger_dag_run_client_update_child \
            >> wait_for_completion_trigger_dag_run_client_update_child >> check_new_records
        has_updates_to_client >> rail.Label('No') >> check_new_records

        check_new_records >> rail.Label('Yes') >> gather_client_add_logs
        check_new_records >> rail.Label(
            'No') >> send_mail_no_new_records >> rename_and_move_reference_client_to_archive

        gather_client_add_logs >> gather_client_disable_logs >> has_updates_client_logs

        has_updates_client_logs >> rail.Label(
            'Yes') >> gather_client_update_logs >> trigger_client_import_log_generation
        has_updates_client_logs >> rail.Label('No') >> trigger_client_import_log_generation >> rename_and_move_reference_client_to_archive \
            >> upload_new_reference_file >> rename_and_move_client_to_archive >> upload_bu_file_copy_to_archive \
            >> upload_locations_file_copy_to_archive >> finish

        has_locations >> rail.Label(
            'No') >> send_mail_locations_csv_empty >> finish

        finish >> catch_and_log_errors >> log_dagrun_to_sumo

    return dag


rail.for_each_instance(create_dag)
