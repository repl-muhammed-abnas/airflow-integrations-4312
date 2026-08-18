import hashlib
from datetime import timedelta, datetime
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nttdata_user_cost_rate_sync_master_{config.instance}',
        description=f'NttData - User cost rate sync- Master {config.instance}',
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
            new_filename=config.archive_filepath + "{{current_time('%m%d%YT%H%S%M')}}" +
            "_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        log_current_time=rail.PythonOperator(
            task_id='log_current_time',
            python_callable= lambda:  datetime.now().strftime("%m%d%YT%H%S%M")
        )

        if_file_ends_with_csv=rail.IfOperator(
            task_id='if_file_ends_with_csv',
            test='''{{ result('new_file_sensor') | ends_with('.csv')}}''',
            yes_task="parse_csv",
            no_task="send_mail_incorrect_file_format",
        )

        send_mail_incorrect_file_format=rail.EmailOperator(
            task_id='send_mail_incorrect_file_format',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''NTTData | Cost Rate import | Completed - Invalid file format ''',
            html_content= '''templates/incorrect_file_format_mail.html''',
        )

        parse_csv=rail.LoadCSVFileOperator(
            task_id='parse_csv',
            delimiter=',',
            headers=['EmployeeId','CurrencyCode','HourlyRate','EffectiveDate','AnnualHours'],
            has_no_header=True,
            document="{{result('download_file')}}"
        )

        compose_csv_with_md5=rail.WriteCSVFileOperator(
            task_id='compose_csv_with_md5',
            source="{{ result('parse_csv') }}",
            header=['EmployeeId',
                    'CurrencyCode',
                    'HourlyRate',
                    'EffectiveDate',
                    'AnnualHours',
                    'md5'],
            row=lambda item: [
                item['EmployeeId'],
                item['CurrencyCode'],
                item['HourlyRate'],
                item['EffectiveDate'],
                item['AnnualHours'],
                hashlib.md5(str(str(item['EmployeeId']) + ',' +
                    str(item['CurrencyCode']) + ',' +
                    str(item['HourlyRate']) + ',' +
                    str(item['AnnualHours'])).encode('utf-8')).hexdigest()
            ],
        )

        create_collection_costratelist_raw = rail.CreateCollectionOperator(
            task_id='create_collection_costratelist_raw',
            source = "{{ result('compose_csv_with_md5') }}",
            name = "costratelist_raw",
            columns = {
                'EmployeeId':'empid', 
                'CurrencyCode':'currencycode', 
                'HourlyRate':'hourlyrate', 
                'EffectiveDate':'effectivedate', 
                'AnnualHours':'annualhours', 
                'md5':'md5'
            }
        )

        if_costrate_raw_collection_has_no_data=rail.IfOperator(
            task_id='if_costrate_raw_collection_has_no_data',
            test="{{ result('create_collection_costratelist_raw','length') < 1 }}",
            yes_task="send_mail_no_data_in_file",
            no_task="download_reference_file",
        )

        send_mail_no_data_in_file=rail.EmailOperator(
            task_id='send_mail_no_data_in_file',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''NTTData | Cost Rate import | Completed - No content in the file ''',
            html_content= '''templates/no_data_mail.html''',
        )

        download_reference_file=rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_filepath + 'costcenter_reference.csv'
        )

        load_reference_file_csv=rail.LoadCSVFileOperator(
            task_id="load_reference_file_csv",
            document="{{result('download_reference_file')}}",
        )

        create_collection_costratereference = rail.CreateCollectionOperator(
            task_id='create_collection_costratereference',
            source = "{{ result('load_reference_file_csv') }}",
            name = "costratereferencee_list",
            columns = {
                'EmployeeId':'empid', 
                'CurrencyCode':'cuurrencycode', 
                'HourlyRate':'hourlyrate', 
                'EffectiveDate':'effectivedate', 
                'AnnualHours':'annualhours', 
                'md5':'md5'
            }
        )

        query_unchanged_records=rail.QueryCollectionOperator(
            task_id='query_unchanged_records',
            query="""SELECT * FROM  costratelist_raw WHERE  costratelist_raw.md5 IN (SELECT  costratereferencee_list.md5 FROM  costratereferencee_list)""",
        )

        create_nttdata_costrate_logs_lookup_table = rail.CreateLogOperator(
            task_id = 'create_nttdata_costrate_logs_lookup_table',
        )

        add_log_entries_for_unchanged_records=rail.WriteLogOperator(
            task_id='add_log_entries_for_unchanged_records',
            log="{{result('create_nttdata_costrate_logs_lookup_table')}}",
            message='No change received for the cost rate',
            severity="Skipped",
            items="{{result('query_unchanged_records')}}",
            properties=lambda item:{
                'jobid': rail.render_template("{{dag_run_ecid()}}"),
                'employeeid': item['empid'],
                'hourlycost': item['hourlyrate'],
                'effectivedate': 'Null',
                'status': 'Skipped',
                'details': 'No change received for the cost rate'
            }
        )

        query_delta_records=rail.QueryCollectionOperator(
            task_id='query_delta_records',
            query="""SELECT * FROM  costratelist_raw WHERE  costratelist_raw.md5 NOT IN (SELECT  costratereferencee_list.md5 FROM  costratereferencee_list)""",
        )

        create_child_dag_runs_list = rail.SetVariableOperator(
            task_id = 'create_child_dag_runs_list',
            name='cost_rate_update_childs',
            value=[]
        )

        if_delta_records_present=rail.IfOperator(
            task_id='if_delta_records_present',
            test='''{{ result('query_delta_records','length') > 0 }}''',
            yes_task="foreach_delta_record",
            no_task="log_size_of_input_file",
        )

        foreach_delta_record=rail.ForEachOperator(
            task_id='foreach_delta_record',
            items="{{ result('query_delta_records') }}",
            start_task = 'if_employeeid_present',
            end_task = 'foreach_delta_record_end'
        )

        if_employeeid_present=rail.IfOperator(
            task_id='if_employeeid_present',
            test='''{{ result('foreach_delta_record').empid | is_truthy }}''',
            yes_task="search_user_by_employeeid",
            no_task="log_employeeid_not_present",
        )

        search_user_by_employeeid=rail.RepliconServiceOperator(
            task_id='search_user_by_employeeid',
            endpoint="/services/UserListService1.svc/GetData",
            data={
              "page": "1",
              "pagesize": "100",
              "columnUris": [
                  "urn:replicon:user-list-column:login-name",
                  "urn:replicon:user-list-column:employee-id",
                  "urn:replicon:user-list-column:enabled"
              ],
              "sort": [],
              "filterExpression": {
                  "leftExpression": {
                      "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                  },
                  "operatorUri": "urn:replicon:filter-operator:text-search",
                  "rightExpression": {
                      "value": {
                          "text": "{{result('foreach_delta_record').empid}}"
                      }
                  }
              }
            }
        )

        def get_user_uri():
            users_data = rail.result('search_user_by_employeeid')['rows']
            uri = ''
            for user in users_data:
                if user['cells'][0]['textValue'] == rail.result('foreach_delta_record')['empid']:
                    uri = user['cells'][0]['uri']
                    break
            return uri

        log_user_uri=rail.PythonOperator(
            task_id='log_user_uri',
            python_callable= lambda: get_user_uri() if rail.result('search_user_by_employeeid') and rail.result('search_user_by_employeeid')['rows'] and
                                rail.result('search_user_by_employeeid')['rows'][0]['cells'][0]['uri'] else null
        )

        if_uri_present=rail.IfOperator(
            task_id='if_uri_present',
            test='''{{ result('log_user_uri') | is_truthy }}''',
            yes_task="get_user_enabled_value",
            no_task="log_user_not_found",
        )

        def get_enabled_value():
            users_data = rail.result('search_user_by_employeeid')['rows']
            for user in users_data:
                if user['cells'][0]['textValue'] == rail.result('foreach_delta_record')['empid']:
                    return user['cells'][2]['textValue']
            return False

        get_user_enabled_value=rail.PythonOperator(
            task_id='get_user_enabled_value',
            python_callable= lambda: get_enabled_value() if rail.result('search_user_by_employeeid') and rail.result('search_user_by_employeeid')['rows'] and
                                rail.result('search_user_by_employeeid')['rows'][0]['cells'][0]['textValue'] else null
        )

        if_user_enabled=rail.IfOperator(
            task_id='if_user_enabled',
            test='''{{ result('get_user_enabled_value') == 'True' }}''',
            yes_task="if_hourlyrate_present",
            no_task="log_user_is_disabled",
        )

        if_hourlyrate_present=rail.IfOperator(
            task_id='if_hourlyrate_present',
            test='''{{ result('foreach_delta_record').hourlyrate | is_truthy }}''',
            yes_task="if_effectivedate_present",
            no_task="log_hourlyrate_not_present",
        )

        if_effectivedate_present=rail.IfOperator(
            task_id='if_effectivedate_present',
            test='''{{ result('foreach_delta_record').effectivedate | is_truthy }}''',
            yes_task="trigger_child_cost_rate_update",
            no_task="log_effectivedate_not_present",
        )

        trigger_child_cost_rate_update=rail.TriggerDagRunOperator(
            task_id='trigger_child_cost_rate_update',
            retries=0,
            trigger_dag_id=f'nttdata_cost_rate_import_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "empid": rail.result('foreach_delta_record')['empid'],
                "useruri": rail.result('log_user_uri'),
                "hourlyrate": rail.result('foreach_delta_record')['hourlyrate'],
                "effectivedate": rail.result('foreach_delta_record')['effectivedate'],
                "currency": (rail.result('foreach_delta_record')['currencycode']).strip() if rail.result('foreach_delta_record')['currencycode'] else null,
                "annualhours": rail.result('foreach_delta_record')['annualhours'],
                "loglookuptable": rail.result('create_nttdata_costrate_logs_lookup_table'),
                "callerjobid": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        if_child_dag_triggered = rail.IfOperator(
            task_id = 'if_child_dag_triggered',
            test="{{result('trigger_child_cost_rate_update') | is_truthy}}",
            yes_task='insert_to_child_dag_runs_list',
            no_task='foreach_delta_record_end'
        )

        insert_to_child_dag_runs_list = rail.SetVariableOperator(
            task_id='insert_to_child_dag_runs_list',
            append=True,
            name='{{ result("create_child_dag_runs_list").name }}',
            value='{{result("trigger_child_cost_rate_update")}}'
        )

        log_effectivedate_not_present=rail.WriteLogOperator(
            task_id='log_effectivedate_not_present',
            log="{{ result('create_nttdata_costrate_logs_lookup_table') }}",
            message="Effective date must be present",
            severity="Exception",
            properties=lambda:{
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "employeeid": rail.result('foreach_delta_record')['empid'],
                "hourlycost": rail.result('foreach_delta_record')['hourlyrate'],
                "effectivedate": 'Null',
                "status": "Exception",
                "details": "Effective date must be present"
            }
        )

        log_hourlyrate_not_present=rail.WriteLogOperator(
            task_id='log_hourlyrate_not_present',
            log="{{ result('create_nttdata_costrate_logs_lookup_table') }}",
            message="Hourly rate must be present",
            severity="Exception",
            properties=lambda:{
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "employeeid": rail.result('foreach_delta_record')['empid'],
                "hourlycost": rail.result('foreach_delta_record')['hourlyrate'] if rail.result('foreach_delta_record')['hourlyrate'] else "Null",
                "effectivedate": rail.result('foreach_delta_record')['effectivedate'] if rail.result('foreach_delta_record')['effectivedate'] else "Null",
                "status": "Exception",
                "details": "Hourly rate must be present"
            }
        )

        log_user_is_disabled=rail.WriteLogOperator(
            task_id='log_user_is_disabled',
            log="{{result('create_nttdata_costrate_logs_lookup_table') }}",
            message="User profile disabled in Replicon",
            severity="Exception",
            properties=lambda:{
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "employeeid": rail.result('foreach_delta_record')['empid'],
                "hourlycost": rail.result('foreach_delta_record')['hourlyrate'] if rail.result('foreach_delta_record')['hourlyrate'] else "Null",
                "effectivedate": rail.result('foreach_delta_record')['effectivedate'] if rail.result('foreach_delta_record')['effectivedate'] else "Null",
                "status": "Exception",
                "details": "User profile disabled in Replicon"
            }
        )

        log_user_not_found=rail.WriteLogOperator(
            task_id='log_user_not_found',
            log="{{ result('create_nttdata_costrate_logs_lookup_table') }}",
            message="User profile not found in Replicon",
            severity="Exception",
            properties=lambda:{
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "employeeid": rail.result('foreach_delta_record')['empid'],
                "hourlycost": rail.result('foreach_delta_record')['hourlyrate'] if rail.result('foreach_delta_record')['hourlyrate'] else "Null",
                "effectivedate": rail.result('foreach_delta_record')['effectivedate'] if rail.result('foreach_delta_record')['effectivedate'] else "Null",
                "status": "Exception",
                "details": "User profile not found in Replicon"
            }
        )

        log_employeeid_not_present=rail.WriteLogOperator(
            task_id='log_employeeid_not_present',
            log="{{ result('create_nttdata_costrate_logs_lookup_table') }}",
            message="EmployeeID must be present",
            severity="Exception",
            properties=lambda:{
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "employeeid": rail.result('foreach_delta_record')['empid'] if rail.result('foreach_delta_record')['empid'] else "Null",
                "hourlycost": rail.result('foreach_delta_record')['hourlyrate'] if rail.result('foreach_delta_record')['hourlyrate'] else "Null",
                "effectivedate": rail.result('foreach_delta_record')['effectivedate'] if rail.result('foreach_delta_record')['effectivedate'] else "Null",
                "status": "Exception",
                "details": "EmployeeID must be present"
            }
        )

        foreach_delta_record_end=rail.EmptyOperator(
            task_id='foreach_delta_record_end',
        )

        if_cost_rate_update_child_triggered = rail.IfOperator(
            task_id = 'if_cost_rate_update_child_triggered',
            test = "{{result('insert_to_child_dag_runs_list') | is_truthy}}",
            yes_task='wait_for_child_cost_rate_update',
            no_task='log_size_of_input_file'
        )

        wait_for_child_cost_rate_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_cost_rate_update',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("insert_to_child_dag_runs_list").value | to_json }}'
        )

        log_size_of_input_file=rail.PythonOperator(
            task_id='log_size_of_input_file',
            python_callable= lambda: int(int(rail.render_template("{{result('query_delta_records','length')}}")) * 3) if
                                int(rail.render_template("{{result('query_delta_records','length')}}")) > 0 else 120
        )

        search_log_entries=rail.FilterLogEntriesOperator(
            task_id='search_log_entries',
            log= "{{ result('create_nttdata_costrate_logs_lookup_table') }}",
            properties={
                'jobid': "{{ dag_run_ecid() }}"
            }
        )

        get_error_entries=rail.PythonOperator(
            task_id='get_error_entries',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr(
                                rail.load_all_records(rail.result('search_log_entries')),'properties.status','Error','properties.status','')
        )

        compose_csv_for_logs=rail.WriteCSVFileOperator(
            task_id='compose_csv_for_logs',
            source="{{ result('search_log_entries') }}",
            header=['EmployeeId',
                    'Hourlyrate',
                    'Effectivedate',
                    'Status',
                    'Details',
                    'JobId'],
            row=lambda item: [
                item['properties']['employeeid'],
                item['properties']['hourlycost'],
                item['properties']['effectivedate'],
                item['properties']['status'],
                (item['properties']['details'].split('|'))[0] if ('|' in item['properties']['details']) else item['properties']['details'],
                (item['properties']['details'] + '|' + (item['properties']['details'].split('|'))[-1]) if (
                    '|' in item['properties']['details'] ) else item['properties']['details']
            ],
        )

        get_log_file_name=rail.PythonOperator(
            task_id='get_log_file_name',
            python_callable= lambda: "logs_" + rail.result('log_current_time') + "_" + rail.render_template("{{ result('new_file_sensor') | file_name }}")
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_csv_for_logs')}}",
            output_file_name="{{ result('get_log_file_name')}}",
            expires_in_seconds=7*24*60*60,
        )

        if_error_log_present=rail.IfOperator(
            task_id='if_error_log_present',
            test='''{{ result('get_error_entries') | is_truthy }}''',
            yes_task="send_mail_completed_with_errors",
            no_task="send_mail_completed_successfully",
        )

        send_mail_completed_with_errors=rail.EmailOperator(
            task_id='send_mail_completed_with_errors',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='''NTTData | Cost Rate import | Completed with errors - {{ result('log_current_time') }} ''',
            html_content= '''templates/completed_with_error_mail.html''',
        )

        send_mail_completed_successfully=rail.EmailOperator(
            task_id='send_mail_completed_successfully',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''NTTData | Cost Rate import | Completed successfully - {{ result('log_current_time') }} ''',
            html_content= '''templates/completed_successfully_mail.html''',
            params=None,
        )

        upload_new_referencefile=rail.SFTPUploadFileOperator(
            task_id='upload_new_referencefile',
            content='''{{ result('compose_csv_with_md5') }}''',
            remote_filepath= config.reference_filepath+ '''costcenter_reference.csv''',
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> download_file >> rail.Label("Always") >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        download_file >>log_current_time >> if_file_ends_with_csv
        if_file_ends_with_csv >> rail.Label('No') >> send_mail_incorrect_file_format >> finish
        if_file_ends_with_csv >> rail.Label('Yes')  >> parse_csv >> compose_csv_with_md5
        compose_csv_with_md5 >> create_collection_costratelist_raw >> if_costrate_raw_collection_has_no_data >> rail.Label('No') >> download_reference_file
        download_reference_file >> load_reference_file_csv >> create_collection_costratereference >> query_unchanged_records
        query_unchanged_records >> create_nttdata_costrate_logs_lookup_table >> add_log_entries_for_unchanged_records
        add_log_entries_for_unchanged_records >> query_delta_records >> create_child_dag_runs_list >> if_delta_records_present
        if_costrate_raw_collection_has_no_data >> rail.Label('Yes') >> send_mail_no_data_in_file >> finish
        if_delta_records_present >> rail.Label('Yes')  >> foreach_delta_record >> if_employeeid_present
        if_employeeid_present >> rail.Label('Yes')  >> search_user_by_employeeid >> log_user_uri >> if_uri_present
        if_uri_present >> rail.Label('Yes')  >> get_user_enabled_value >> if_user_enabled
        if_user_enabled >> rail.Label('Yes')  >> if_hourlyrate_present
        if_hourlyrate_present >> rail.Label('Yes')  >> if_effectivedate_present
        if_effectivedate_present >> rail.Label('Yes')  >> trigger_child_cost_rate_update >> if_child_dag_triggered
        if_child_dag_triggered >> rail.Label('Yes') >> insert_to_child_dag_runs_list >> foreach_delta_record_end
        if_child_dag_triggered >> rail.Label('No') >> foreach_delta_record_end
        if_effectivedate_present >> rail.Label('No') >> log_effectivedate_not_present >> foreach_delta_record_end
        if_hourlyrate_present >> rail.Label('No') >> log_hourlyrate_not_present >> foreach_delta_record_end
        if_user_enabled >> rail.Label('No') >> log_user_is_disabled >> foreach_delta_record_end
        if_uri_present >> rail.Label('No') >> log_user_not_found >> foreach_delta_record_end
        if_employeeid_present >> rail.Label('No') >> log_employeeid_not_present >> foreach_delta_record_end
        foreach_delta_record >> foreach_delta_record_end >> if_cost_rate_update_child_triggered
        if_cost_rate_update_child_triggered >> rail.Label('Yes') >> wait_for_child_cost_rate_update >> log_size_of_input_file
        if_cost_rate_update_child_triggered >> rail.Label('Yes') >> log_size_of_input_file
        if_delta_records_present >> rail.Label('No') >> log_size_of_input_file >> search_log_entries >> get_error_entries >> compose_csv_for_logs
        compose_csv_for_logs >> get_log_file_name >> generate_download_link >> if_error_log_present
        if_error_log_present >> rail.Label('Yes')  >> send_mail_completed_with_errors >> upload_new_referencefile
        if_error_log_present >> rail.Label('No') >> send_mail_completed_successfully >> upload_new_referencefile
        upload_new_referencefile >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
