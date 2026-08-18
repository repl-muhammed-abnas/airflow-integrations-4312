
from datetime import timedelta, datetime
from velaw.timesheet_oef_import.utils import request_payload, python_callable_method
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'velaw_timesheet_oef_import_master_{config.instance}',
        description=f'Velaw - Timesheet_OEF_Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            sftp_conn_id=config.sftp_conn_id,
            soft_fail_timeout=timedelta(minutes=10)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ result('new_file_sensor') }}"
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
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "{{ result('new_file_sensor') | file_base }}" + "_archived.csv"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        declare_unprocessedlogs_list = rail.SetVariableOperator(
            task_id='declare_unprocessedlogs_list',
            append=False,
            name='unprocessedlogs',
            value=[]
        )

        if_file_ends_with_csv = rail.IfOperator(
            task_id='if_file_ends_with_csv',
            test='''{{ result('new_file_sensor') | ends_with('.csv')}}''',
            yes_task='parse_csv',
            no_task='insert_to_unprocessed_logs'
        )

        insert_to_unprocessed_logs = rail.SetVariableOperator(
            task_id='insert_to_unprocessed_logs',
            append=True,
            name='{{ result("declare_unprocessedlogs_list").name }}',
            value={
                "processingstatus": "Skipped",
                "details": "Skipped processing the file {{ result('new_file_sensor') | file_name }} due to incorrect file format",
                "jobid": "{{ dag_run_ecid() }}",
                "authorizername": "",
                "terminationdate": ""
            }
        )

        compose_csv_file_not_csv = rail.WriteCSVFileOperator(
            task_id='compose_csv_file_not_csv',
            source=lambda: rail.get_dag_run_var(
                rail.result('insert_to_unprocessed_logs')['name']),
            header=['Authorizer Name',
                    'Termination Date',
                    'Processing Status',
                    'Details',
                    'JobID'],
            row=[
                "{{ item.authorizername }}",
                "{{ item.terminationdate }}",
                "{{ item.processingstatus }}",
                "{{ item.details }}",
                "{{ item.jobid }}"
            ]
        )

        generate_log_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_log_link',
            artifact_name="{{ result('compose_csv_file_not_csv')}}",
            output_file_name="{{ result('new_file_sensor') | file_base }}" + "_logs.csv",
            expires_in_seconds=7*24*60*60,
        )

        send_mail_file_not_csv = rail.EmailOperator(
            task_id='send_mail_file_not_csv',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }}" + " | " + \
            "Daily Authorize Update - Import not started",
            html_content='templates/file_not_csv_mail.html',
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            encoding='latin-1',
            document="{{result('download_file')}}",
            delimiter=',',
            headers=['AUTHORIZER_NAME', 'TERMINATION_DATE']
        )

        has_no_data = rail.IfOperator(
            task_id='has_no_data',
            test="{{result('parse_csv') | length < 1}}",
            yes_task="insert_to_unprocessedlogs",
            no_task='get_object_extension_tag_uri'
        )

        insert_to_unprocessedlogs = rail.SetVariableOperator(
            task_id='insert_to_unprocessedlogs',
            append=True,
            name='{{ result("declare_unprocessedlogs_list").name }}',
            value={
                "processingstatus": "Skipped",
                "details": "Blank input file received. Skipped processing file {{ result('new_file_sensor') | file_name }}",
                "jobid": "{{ dag_run_ecid() }}",
                "authorizername": "",
                "terminationdate": ""
            }
        )

        compose_csv_has_no_data = rail.WriteCSVFileOperator(
            task_id='compose_csv_has_no_data',
            source=lambda: rail.get_dag_run_var(
                rail.result('insert_to_unprocessed_logs')['name']),
            header=['Authorizer Name',
                    'Termination Date',
                    'Processing Status',
                    'Details',
                    'JobID'],
            row=[
                "{{ item.authorizername }}",
                "{{ item.terminationdate }}",
                "{{ item.processingstatus }}",
                "{{ item.details }}",
                "{{ item.jobid }}"
            ]
        )

        generate_logs_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_logs_link',
            artifact_name="{{ result('compose_csv_has_no_data')}}",
            output_file_name="{{ result('new_file_sensor') | file_base }}" + "_logs.csv",
            expires_in_seconds=7*24*60*60,
        )

        send_mail_file_has_no_data = rail.EmailOperator(
            task_id='send_mail_file_has_no_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }}" + " | " + \
            "Daily Authorize Update - Import not started",
            html_content='templates/file_blank_mail.html',
        )

        get_object_extension_tag_uri = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_uri',
            endpoint="/services/ObjectExtensionDefinitionListService1.svc/GetData",
            data=request_payload.get_payload_oef_uri
        )

        get_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_dropdown_options',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_object_extension_tag_uri').rows[0].cells[0].uri }}"
            }
        )

        get_tags_available_in_system = rail.PythonOperator(
            task_id='get_tags_available_in_system',
            python_callable=python_callable_method.get_tags_available
        )

        create_logs_list = rail.SetVariableOperator(
            task_id='create_logs_list',
            append=False,
            name='logs',
            value=[]
        )

        create_existingdata_list_collection = rail.CreateCollectionOperator(
            task_id='create_existingdata_list_collection',
            source="{{ result('get_tags_available_in_system') | to_json }}",
            name="existingdata",
        )

        compose_csv_from_inputfile = rail.WriteCSVFileOperator(
            task_id='compose_csv_from_inputfile',
            source="{{ result('parse_csv') }}",
            header=['Authorizer Name',
                    'Termination Date'],
            row=lambda item: [
                item['AUTHORIZER_NAME'],
                item['TERMINATION_DATE']
            ]
        )

        create_inputfile_collection = rail.CreateCollectionOperator(
            task_id='create_inputfile_collection',
            source="{{ result('compose_csv_from_inputfile') }}",
            name="inputfile",
            columns={
                'Authorizer Name': 'authorizername',
                'Termination Date': 'terminationdate'
            }
        )

        query_list_forlogging = rail.QueryCollectionOperator(
            task_id='query_list_forlogging',
            query="""SELECT DISTINCT inputfile.authorizername FROM inputfile WHERE
                    inputfile.authorizername IN (SELECT  existingdata.name FROM  existingdata) AND  NULLIF(terminationdate,'') IS NULL""",
        )

        query_oef_not_available_in_replicon = rail.QueryCollectionOperator(
            task_id='query_oef_not_available_in_replicon',
            query="""SELECT DISTINCT  inputfile.authorizername FROM  inputfile WHERE
                    inputfile.authorizername NOT IN (SELECT  existingdata.name FROM  existingdata)""",
        )

        get_newoeftoadd = rail.PythonOperator(
            task_id='get_newoeftoadd',
            python_callable=python_callable_method.get_newoeftoadd
        )

        if_newoeftoadd_greater_than_zero = rail.IfOperator(
            task_id='if_newoeftoadd_greater_than_zero',
            test="{{ result('get_newoeftoadd') | length > 0}}",
            yes_task="foreach_newoeftoadd",
            no_task="if_query_list_for_logging_has_data",
        )

        foreach_newoeftoadd = rail.ForEachOperator(
            task_id='foreach_newoeftoadd',
            items="{{ result('get_newoeftoadd') | to_json}}",
            start_task='trigger_child_to_add_oef',
            end_task='foreach_newoeftoadd_end'
        )

        trigger_child_to_add_oef = rail.TriggerDagRunOperator(
            task_id='trigger_child_to_add_oef',
            retries=0,
            trigger_dag_id=f'velaw_add_new_oef_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'tagDefinitionUri': "{{result('get_object_extension_tag_uri').rows[0].cells[0].uri}}",
                'tagName': "{{result('foreach_newoeftoadd').name}}",
                'existingTags': "{{ result('get_tags_available_in_system')}}"
            }
        )

        insert_logs_oef_added = rail.SetVariableOperator(
            task_id='insert_logs_oef_added',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value={
                "authorizername": "{{ result('foreach_newoeftoadd').name }}",
                "terminationdate": null,
                "processingstatus": "Success",
                "details": "{{ result('foreach_newoeftoadd').name }}" + " added in Replicon",
                "jobid": "{{ dag_run_ecid() }}"
            }
        )

        foreach_newoeftoadd_end = rail.EmptyOperator(
            task_id='foreach_newoeftoadd_end',
        )

        wait_for_oef_tobe_added = rail.WaitForDagRunsSensor(
            task_id='wait_for_oef_tobe_added',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_to_add_oef") }}'
        )

        if_query_list_for_logging_has_data = rail.IfOperator(
            task_id='if_query_list_for_logging_has_data',
            test='''{{ result("query_list_forlogging") | length > 0 }}''',
            yes_task="foreach_existing_oefs",
            no_task="get_object_extension_tag_definition_details_dropdownoptions"
        )

        foreach_existing_oefs = rail.ForEachOperator(
            task_id='foreach_existing_oefs',
            items="{{ result('query_list_forlogging')}}",
            start_task='insert_log_oef_present',
            end_task='foreach_existing_oefs_end'
        )

        insert_log_oef_present = rail.SetVariableOperator(
            task_id='insert_log_oef_present',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value={
                "authorizername": "{{ result('foreach_existing_oefs').authorizername }}",
                "terminationdate": null,
                "processingstatus": "Skipped",
                "details": "Authorizer name already exist",
                "jobid": "{{ dag_run_ecid() }}"
            }
        )

        foreach_existing_oefs_end = rail.EmptyOperator(
            task_id= 'foreach_existing_oefs_end'
        )

        get_object_extension_tag_definition_details_dropdownoptions = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details_dropdownoptions',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ result('get_object_extension_tag_uri').rows[0].cells[0].uri }}"
            }
        )

        def existing_oef_afteradd():
            tags = []
            for item in rail.result('get_object_extension_tag_definition_details_dropdownoptions')['tags']:
                tags.append({
                    'name': item['name'],
                    'uri': item['uri'],
                    'status': item['isEnabled']
                })
            return tags

        get_existing_oef_afteradd = rail.PythonOperator(
            task_id='get_existing_oef_afteradd',
            python_callable=existing_oef_afteradd
        )

        create_existingdataupdated_collection = rail.CreateCollectionOperator(
            task_id='create_existingdataupdated_collection',
            source="{{ result('get_existing_oef_afteradd') | to_json }}",
            name="existingdataupdated",
        )

        query_list_oef_valuesavailablein_repliconwith_termination_date = rail.QueryCollectionOperator(
            task_id='query_list_oef_valuesavailablein_repliconwith_termination_date',
            query="""SELECT * FROM   inputfile WHERE
                    inputfile.authorizername IN (SELECT DISTINCT  existingdataupdated.name FROM existingdataupdated WHERE existingdataupdated.status = '1') AND
                    NULLIF(terminationdate,'') IS NOT NULL""",
        )

        if_query_list_with_termination_date_has_data = rail.IfOperator(
            task_id='if_query_list_with_termination_date_has_data',
            test="{{ result('query_list_oef_valuesavailablein_repliconwith_termination_date', 'length') > 0}}",
            yes_task='foreach_in_oef_valuesavailablein_repliconwith_termination_date',
            no_task='get_logs_list_value'
        )

        foreach_in_oef_valuesavailablein_repliconwith_termination_date = rail.ForEachOperator(
            task_id='foreach_in_oef_valuesavailablein_repliconwith_termination_date',
            items="{{ result('query_list_oef_valuesavailablein_repliconwith_termination_date') }}",
            start_task='if_terminationdate_plus_ten_equals_today',
            end_task='foreach_in_oef_valuesavailablein_repliconwith_termination_date_disable_end'
        )

        def if_terminationdate_valid():
            if (datetime.strptime(
                rail.result('foreach_in_oef_valuesavailablein_repliconwith_termination_date')['terminationdate'], "%m/%d/%Y")
                    + timedelta(days=10)) == datetime.strptime(datetime.now().strftime('%m/%d/%Y'), "%m/%d/%Y"):
                return True
            return False

        if_terminationdate_plus_ten_equals_today = rail.IfOperator(
            task_id='if_terminationdate_plus_ten_equals_today',
            test=if_terminationdate_valid,
            yes_task="log_oef_tag_uri",
            no_task="insert_logs_date_doesnt_meet_logic",
        )

        def get_oef_tag_uri():
            uri = ''
            if rail.result('get_object_extension_tag_definition_details_dropdownoptions'):
                uri = rail.find_first_by_attr_and_get_attr(rail.result('get_object_extension_tag_definition_details_dropdownoptions')['tags'], 'name',
                                                           rail.result('foreach_in_oef_valuesavailablein_repliconwith_termination_date')['authorizername'],
                                                            'uri')
            return uri

        log_oef_tag_uri = rail.PythonOperator(
            task_id='log_oef_tag_uri',
            python_callable=  get_oef_tag_uri,
        )

        if_oef_tag_uri_present = rail.IfOperator(
            task_id='if_oef_tag_uri_present',
            test='''{{ result('log_oef_tag_uri') | is_truthy }}''',
            yes_task="disable_tag",
            no_task="foreach_in_oef_valuesavailablein_repliconwith_termination_date_disable_end",
        )

        disable_tag = rail.RepliconServiceOperator(
            task_id='disable_tag',
            endpoint="/services/ObjectExtensionTagService1.svc/Disable",
            data={
                "objectExtensionTagUri": "{{ result('log_oef_tag_uri') }}"
            }
        )

        insert_logs_oef_is_disabled = rail.SetVariableOperator(
            task_id='insert_logs_oef_is_disabled',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value=lambda: {
                "authorizername": rail.result('foreach_in_oef_valuesavailablein_repliconwith_termination_date')['authorizername'],
                "terminationdate": rail.result('foreach_in_oef_valuesavailablein_repliconwith_termination_date')['terminationdate']
                if rail.result('foreach_in_oef_valuesavailablein_repliconwith_termination_date')['terminationdate']
                else 0,
                "processingstatus": "Disabled",
                "details": rail.result('foreach_in_oef_valuesavailablein_repliconwith_termination_date')['authorizername'] + " name is disabled",
                "jobid": "{{ dag_run_ecid() }}"
            }
        )

        insert_logs_date_doesnt_meet_logic = rail.SetVariableOperator(
            task_id='insert_logs_date_doesnt_meet_logic',
            append=True,
            name='{{ result("create_logs_list").name }}',
            value=lambda: {
                "authorizername": rail.result('foreach_in_oef_valuesavailablein_repliconwith_termination_date')['authorizername'],
                "terminationdate": rail.result('foreach_in_oef_valuesavailablein_repliconwith_termination_date')['terminationdate']
                if rail.result('foreach_in_oef_valuesavailablein_repliconwith_termination_date')['terminationdate']
                else 0,
                "processingstatus": "Skipped",
                "details": "The termination date doesn't meet the date logic",
                "jobid": "{{ dag_run_ecid() }}"
            }
        )

        foreach_in_oef_valuesavailablein_repliconwith_termination_date_disable_end = rail.EmptyOperator(
            task_id='foreach_in_oef_valuesavailablein_repliconwith_termination_date_disable_end',
        )

        get_logs_list_value = rail.GetVariableOperator(
            task_id='get_logs_list_value',
            name='{{ result("create_logs_list").name}}'
        )

        if_loglist_has_records = rail.IfOperator(
            task_id='if_loglist_has_records',
            test='''{{ result('get_logs_list_value').value | length > 0 }}''',
            yes_task="compose_csv_for_success",
        )

        compose_csv_for_success = rail.WriteCSVFileOperator(
            task_id='compose_csv_for_success',
            source="{{ result('get_logs_list_value').value | to_json}}",
            header=['Authorizer Name',
                    'Termination Date',
                    'Processing Status',
                    'Details',
                    'JobID'],
            row=[
                "{{ item.authorizername }}",
                "{{ item.terminationdate }}",
                "{{ item.processingstatus }}",
                "{{ item.details }}",
                "{{ dag_run_ecid() }}",
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_csv_for_success')}}",
            output_file_name="{{ result('new_file_sensor') | file_base }}" + "_logs.csv",
            expires_in_seconds=7*24*60*60,
        )

        send_mail_for_success = rail.EmailOperator(
            task_id='send_mail_for_success',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }}" + " | " + \
            "Daily Authorize Update - Completed Successfully",
            html_content='templates/success_mail.html',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )
        new_file_sensor >> download_file >> rail.Label("Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        download_file >> declare_unprocessedlogs_list
        declare_unprocessedlogs_list >> if_file_ends_with_csv
        if_file_ends_with_csv >> rail.Label(
            'No') >> insert_to_unprocessed_logs >> compose_csv_file_not_csv >> generate_log_link >> send_mail_file_not_csv
        send_mail_file_not_csv >> finish
        if_file_ends_with_csv >> rail.Label(
            'Yes') >> parse_csv >> has_no_data
        has_no_data >> rail.Label(
            'Yes') >> insert_to_unprocessedlogs >> compose_csv_has_no_data >> generate_logs_link >> send_mail_file_has_no_data
        send_mail_file_has_no_data >> finish
        has_no_data >> rail.Label('No') >> get_object_extension_tag_uri >> get_dropdown_options >> get_tags_available_in_system
        get_tags_available_in_system >> create_logs_list >> create_existingdata_list_collection >> compose_csv_from_inputfile >> create_inputfile_collection
        create_inputfile_collection >> query_list_forlogging >> query_oef_not_available_in_replicon >> get_newoeftoadd >> if_newoeftoadd_greater_than_zero
        if_newoeftoadd_greater_than_zero >> rail.Label(
            'Yes') >> foreach_newoeftoadd >> trigger_child_to_add_oef >> insert_logs_oef_added >> foreach_newoeftoadd_end >> wait_for_oef_tobe_added
        if_newoeftoadd_greater_than_zero >> rail.Label('No') >> if_query_list_for_logging_has_data >> rail.Label(
            'Yes') >> foreach_existing_oefs >> insert_log_oef_present >> foreach_existing_oefs_end
        foreach_existing_oefs >> foreach_existing_oefs_end >> get_object_extension_tag_definition_details_dropdownoptions
        if_query_list_for_logging_has_data >> rail.Label(
            'No') >> get_object_extension_tag_definition_details_dropdownoptions
        foreach_newoeftoadd >> foreach_newoeftoadd_end >> wait_for_oef_tobe_added >> get_object_extension_tag_definition_details_dropdownoptions
        get_object_extension_tag_definition_details_dropdownoptions >> get_existing_oef_afteradd >> create_existingdataupdated_collection
        create_existingdataupdated_collection >> query_list_oef_valuesavailablein_repliconwith_termination_date >> if_query_list_with_termination_date_has_data
        if_query_list_with_termination_date_has_data >> rail.Label(
            "Yes") >> foreach_in_oef_valuesavailablein_repliconwith_termination_date >> if_terminationdate_plus_ten_equals_today
        if_terminationdate_plus_ten_equals_today >> rail.Label(
            'Yes') >> log_oef_tag_uri >> if_oef_tag_uri_present
        if_terminationdate_plus_ten_equals_today >> rail.Label(
            'No') >> insert_logs_date_doesnt_meet_logic >> foreach_in_oef_valuesavailablein_repliconwith_termination_date_disable_end
        if_oef_tag_uri_present >> rail.Label(
            'Yes') >> disable_tag >> insert_logs_oef_is_disabled >> foreach_in_oef_valuesavailablein_repliconwith_termination_date_disable_end
        if_oef_tag_uri_present >> rail.Label(
            'No')>> foreach_in_oef_valuesavailablein_repliconwith_termination_date_disable_end
        foreach_in_oef_valuesavailablein_repliconwith_termination_date >> foreach_in_oef_valuesavailablein_repliconwith_termination_date_disable_end
        foreach_in_oef_valuesavailablein_repliconwith_termination_date_disable_end >> get_logs_list_value >> if_loglist_has_records
        if_loglist_has_records >> rail.Label(
            'Yes') >> compose_csv_for_success >> generate_download_link >> send_mail_for_success
        send_mail_for_success >> finish
        if_query_list_with_termination_date_has_data >> rail.Label(
            "No") >> get_logs_list_value

    return dag


rail.for_each_instance(create_dag)
