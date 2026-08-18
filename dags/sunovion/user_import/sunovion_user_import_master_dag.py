
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'sunovion_user_import_master_{config.instance}',
        description=f'Sunovion User Import Master {config.instance}',
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
            new_filename=config.archive_filepath +
            "{{result('new_file_sensor') | file_name}}_{{current_time('%m_%d_%Y_T%H_%M_%S')}}",
            existing_filename="{{ result('new_file_sensor') }}",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_file_name_ends_with_csv_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_file_name_ends_with_csv_2',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_file_name_ends_with_csv_2 = rail.IfOperator(
            task_id='if_file_name_ends_with_csv_2',
            test='''{{ result('new_file_sensor') | ends_with('csv') }}''',
            yes_task="load_csv_downloaded_file",
            no_task="finish",
        )

        load_csv_downloaded_file = rail.LoadCSVFileOperator(
            task_id='load_csv_downloaded_file',
            document="{{result('download_file')}}",
            delimiter='|'
        )

        if_cafa1634_size_greater_than_0_7 = rail.IfOperator(
            task_id='if_cafa1634_size_greater_than_0_7',
            test=lambda: bool(len(rail.load_all_records(
                rail.result('load_csv_downloaded_file'))) > 0),
            yes_task="create_collection_create_list_from_csv_9",
            no_task="finish",
        )

        create_collection_create_list_from_csv_9 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_9',
            source="{{ result('load_csv_downloaded_file') }}",
            name="input_data",
            columns={
                'LOGIN NAME': 'loginname',
                'FIRST NAME': 'firstname',
                'LAST NAME': 'lastname',
                'EMPLOYEE TYPE': 'employeetype',
                'ENABLED': 'enabled',
                'EMPLOYEE ID': 'employeeid',
                'START DATE': 'startdate',
                'VACATION ACCRUAL DATE': 'vacationaccrualdate',
                'WORKDAY EMPLOYEE TYPE': 'workdayemployeetype',
                'WORKDAY EXECUTIVE': 'workdayexecutive',
                'SCHEDULED HOURS PER WEEK': 'scheduledhoursperweek',
                'END DATE': 'enddate',
                'EMAIL ADDRESS': 'emailaddress',
                'SUPERVISOR ID': 'supervisorid',
                'PERMISSION SETS': 'permissionsets',
                'INITIAL SCHEDULE NAME': 'initialschedulename',
                'GROUP1-PAYGROUP': 'paygroup',
                'PAY GROUP EFFECTIVE DATE': 'paygroupeffectivedate',
                'GROUP2-RESIDENT STATE': 'residentstate',
                'RESIDENT STATE EFFECTIVE DATE': 'residentstateeffectivedate',
                'GROUP3-COST CENTER': 'costcenter',
                'COST CENTER EFFECTIVE DATE': 'costcentereffectivedate'
            }
        )

        query_list_10 = rail.QueryCollectionOperator(
            task_id='query_list_10',
            query="""SELECT  input_data.loginname, input_data.firstname, input_data.lastname, input_data.employeetype, input_data.enabled,
                input_data.employeeid, input_data.startdate, input_data.vacationaccrualdate, input_data.workdayemployeetype,
                input_data.workdayexecutive, input_data.scheduledhoursperweek, input_data.enddate, input_data.emailaddress,
                input_data.supervisorid, input_data.permissionsets, input_data.initialschedulename, input_data.paygroup,
                input_data.paygroupeffectivedate, input_data.residentstate, input_data.costcenter FROM
                input_data WHERE  NULLIF(loginname,'') IS NOT NULL""",
        )

        if_query_list_10_rows_greater_than_0_11 = rail.IfOperator(
            task_id='if_query_list_10_rows_greater_than_0_11',
            test='''{{ result('query_list_10','length') > 0 }}''',
            yes_task="create_add_update_child_dag_list",
            no_task="if_query_list_10_rows_less_than_1_52",
        )

        create_add_update_child_dag_list = rail.SetVariableOperator(
            task_id='create_add_update_child_dag_list',
            name='childdags',
            append=False,
            value=[]
        )

        create_supervisor_user_mapping_lookuptable = rail.CreateLogOperator(
            task_id='create_supervisor_user_mapping_lookuptable'
        )

        create_user_import_logs_lookuptable = rail.CreateLogOperator(
            task_id='create_user_import_logs_lookuptable'
        )

        foreach_query_list_10_12 = rail.ForEachOperator(
            task_id='foreach_query_list_10_12',
            items="{{ result('query_list_10') }}",
            start_task='search_users_13',
            end_task='foreach_query_list_10_12_end'
        )

        def get_user_uri(response):
            matching_user = list(filter(
                lambda user: user['cells'][0]['textValue'] == rail.result('foreach_query_list_10_12')['loginname'], response['rows']))
            return {
                'uri': matching_user[0]['cells'][0]['uri'] if matching_user else ''
            }

        search_users_13 = rail.RepliconServiceOperator(
            task_id='search_users_13',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled",
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{result('foreach_query_list_10_12').loginname}}"
                        }
                    }
                }
            },
            data_handler=get_user_uri
        )

        if_log_gettherequiredusersuri_14_blank_15 = rail.IfOperator(
            task_id='if_log_gettherequiredusersuri_14_blank_15',
            test='''{{ result('search_users_13').uri | is_falsy }}''',
            yes_task="trigger_child_add_user",
            no_task="trigger_child_update_user",
        )

        def get_add_update_user_payload(action):
            user = rail.result('foreach_query_list_10_12')
            child_payload = {
                "loginname": (user['loginname']).strip() if user['loginname'] else '',
                "firstname": (user['firstname']).strip() if user['firstname'] else '',
                "lastname": (user['lastname']).strip() if user['lastname'] else '',
                "employeetype": (user['employeetype']).strip() if user['employeetype'] else '',
                "enabled": (user['enabled']).strip() if user['enabled'] else '',
                "employeeid": (user['employeeid']).strip() if user['employeeid'] else '',

                "startdate": (user['startdate']).replace('-', '/') if user['startdate'] else '',
                "vacationaccrualdate": (user['vacationaccrualdate']).replace('-', '/') if user['vacationaccrualdate'] else '',
                "workdayemployeetype": (user['workdayemployeetype']).strip() if user['workdayemployeetype'] else '',
                "workdayexecutive": (user['workdayexecutive']).strip() if user['workdayexecutive'] else '',
                "scheduledhoursperweek": (user['scheduledhoursperweek']).strip() if user['scheduledhoursperweek'] else '',
                "enddate": (user['enddate']).replace('-', '/') if user['enddate'] else '',
                "emailaddress": user['emailaddress'],
                "supervisorid": (user['supervisorid']).strip() if user['supervisorid'] else '',
                "permissionsets": (user['permissionsets']).strip() if user['permissionsets'] else '',
                "initialschedulename": (user['initialschedulename']).strip() if user['initialschedulename'] else '',
                "paygroup": (user['paygroup']).strip() if user['paygroup'] else '',
                "paygroupeffectivedate": (user['paygroupeffectivedate']).replace('-', '/') if user['paygroupeffectivedate'] else '',
                "residentstate": (user['residentstate']).strip() if user['residentstate'] else '',
                "costcenter": (user['costcenter']).strip() if user['costcenter'] else '',
                "callerjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "userimportlogtable": rail.result('create_user_import_logs_lookuptable'),
                "supervisorlookuptable": rail.result('create_supervisor_user_mapping_lookuptable')
            }
            if action == 'update':
                child_payload.update(
                    {'useruri': rail.result('search_users_13')['uri']})
            return child_payload

        trigger_child_add_user = rail.TriggerDagRunOperator(
            task_id='trigger_child_add_user',
            retries=0,
            trigger_dag_id=f'sunovion_user_import_add_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: get_add_update_user_payload('add')
        )

        trigger_child_update_user = rail.TriggerDagRunOperator(
            task_id='trigger_child_update_user',
            retries=0,
            trigger_dag_id=f'sunovion_user_import_update_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: get_add_update_user_payload('update')
        )

        insert_dag_id_to_wait = rail.SetVariableOperator(
            task_id='insert_dag_id_to_wait',
            name="childdags",
            append=True,
            value="{{result('trigger_child_add_user') or result('trigger_child_update_user')}}"
        )

        foreach_query_list_10_12_end = rail.EmptyOperator(
            task_id='foreach_query_list_10_12_end',
        )

        wait_for_add_update_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_update_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("insert_dag_id_to_wait").value | to_json }}'
        )

        sunovion_user_supervisor_mapping_table_search_entries_23 = rail.FilterLogEntriesOperator(
            task_id='sunovion_user_supervisor_mapping_table_search_entries_23',
            log="{{result('create_supervisor_user_mapping_lookuptable')}}",
            properties={
                'jobid': "{{dag_run_ecid()}}",
                'status': 'Error'
            }
        )

        if_entry_col1_present_24 = rail.IfOperator(
            task_id='if_entry_col1_present_24',
            test='''{{ result('sunovion_user_supervisor_mapping_table_search_entries_23','length') > 0 }}''',
            yes_task="trigger_child_add_supervisor",
            no_task="sunovion_user_logs_file_search_entries_29",
        )

        trigger_child_add_supervisor = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_add_supervisor',
            retries=0,
            items="{{ result('sunovion_user_supervisor_mapping_table_search_entries_23') }}",
            trigger_dag_id=f'sunovion_user_import_add_supervisor_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "loginname": (item['properties']['loginname']).strip() if item['properties']['loginname'] else '',
                "username": item['properties']['username'],
                "supervisorid": (item['properties']['supervisorid']).strip() if item['properties']['supervisorid'] else '',
                "callerjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "userimportlogtable": rail.result('create_user_import_logs_lookuptable')
            }
        )

        wait_for_child_add_supervisor = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_add_supervisor',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_add_supervisor") }}'
        )

        sunovion_user_logs_file_search_entries_29 = rail.FilterLogEntriesOperator(
            task_id='sunovion_user_logs_file_search_entries_29',
            log="{{result('create_user_import_logs_lookuptable')}}",
            properties={
                'parentjobid': "{{dag_run_ecid()}}"
            }
        )

        create_csv_lines_30 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_30',
            source="{{ result('sunovion_user_logs_file_search_entries_29') }}",
            header=['jobid',
                    'username ',
                    'status',
                    'failure/reason'],
            row=[
                "{{ item.properties.parentjobid}} | {{ item.properties.childjobid }}",
                "{{ item.properties.loginname }}",
                "{{ item.properties.status }}",
                "{{ item.properties.failurereason }}"
            ],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines_30')}}",
            output_file_name="Logs_SunovionUserImport_{{current_time('%m_%d_%Y_T%H_%M_%S')}}.csv",
            expires_in_seconds=7*24*60*60,
        )

        check_for_error_logs = rail.FilterLogEntriesOperator(
            task_id='check_for_error_logs',
            log="{{result('create_user_import_logs_lookuptable')}}",
            properties={
                'status': 'Error'
            }
        )

        upload_uploadslogstosftp_37 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadslogstosftp_37',
            content='''{{ result('create_csv_lines_30') }}''',
            remote_filepath=config.log_filepath +
            '''Logs_SunovionUserImport_{{ current_time("%m_%d_%Y_T%H_%M_%S") }}.csv''',
        )

        if_log_35_blank_43 = rail.IfOperator(
            task_id='if_log_35_blank_43',
            test='''{{ result('check_for_error_logs','length') < 1 }}''',
            yes_task="send_mail_with_cshare_completedsuccessfully_44",
            no_task="if_log_35_present_45",
        )

        send_mail_with_cshare_completedsuccessfully_44 = rail.EmailOperator(
            task_id='send_mail_with_cshare_completedsuccessfully_44',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            cc=config.tenant_support_email_cc,
            subject='''{{get_company_key()}} | User import completed successfully - {{ current_time('%m/%d/%Y') }} ''',
            html_content='''templates/completed_successfully_mail.html''',
        )

        if_log_35_present_45 = rail.IfOperator(
            task_id='if_log_35_present_45',
            test='''{{ result('check_for_error_logs','length') > 0 }}''',
            yes_task="send_mail_with_cshare_completedwitherrors_46",
            no_task="if_query_list_10_rows_less_than_1_52",
        )

        send_mail_with_cshare_completedwitherrors_46 = rail.EmailOperator(
            task_id='send_mail_with_cshare_completedwitherrors_46',
            to=config.tenant_email,
            bcc=config.alert_email,
            cc=config.tenant_support_email_cc,
            subject='''{{ get_company_key()}}| User import completed with errors - {{ current_time('%m/%d/%Y') }} ''',
            html_content='''templates/completed_with_errors_mail.html''',
        )

        if_query_list_10_rows_less_than_1_52 = rail.IfOperator(
            task_id='if_query_list_10_rows_less_than_1_52',
            test='''{{ result('query_list_10','length') < 1 }}''',
            yes_task="send_mail_blank_file_57",
            no_task="finish",
        )

        send_mail_blank_file_57 = rail.EmailOperator(
            task_id='send_mail_blank_file_57',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} |  Replicon user import has been completed - {{ current_time('%m/%d/%Y') }} ''',
            html_content='''templates/blank_file_mail.html''',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> download_file >> rail.Label(
            "Always") >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_file >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> if_file_name_ends_with_csv_2
        if_file_name_ends_with_csv_2 >> rail.Label(
            'Yes') >> load_csv_downloaded_file >> if_cafa1634_size_greater_than_0_7
        if_file_name_ends_with_csv_2 >> rail.Label('No') >> finish
        if_cafa1634_size_greater_than_0_7 >> rail.Label(
            'Yes') >> create_collection_create_list_from_csv_9 >> query_list_10
        query_list_10 >> if_query_list_10_rows_greater_than_0_11
        if_query_list_10_rows_greater_than_0_11 >> rail.Label(
            'Yes') >> create_add_update_child_dag_list >> create_supervisor_user_mapping_lookuptable >> create_user_import_logs_lookuptable
        create_user_import_logs_lookuptable >> foreach_query_list_10_12 >> search_users_13
        search_users_13 >> if_log_gettherequiredusersuri_14_blank_15
        if_log_gettherequiredusersuri_14_blank_15 >> rail.Label(
            'Yes') >> trigger_child_add_user >> insert_dag_id_to_wait >> foreach_query_list_10_12_end
        if_log_gettherequiredusersuri_14_blank_15 >> rail.Label(
            'No') >> trigger_child_update_user >> insert_dag_id_to_wait >> foreach_query_list_10_12_end
        foreach_query_list_10_12 >> foreach_query_list_10_12_end >> wait_for_add_update_child >> sunovion_user_supervisor_mapping_table_search_entries_23
        sunovion_user_supervisor_mapping_table_search_entries_23 >> if_entry_col1_present_24
        if_entry_col1_present_24 >> rail.Label(
            'Yes') >> trigger_child_add_supervisor >> wait_for_child_add_supervisor
        wait_for_child_add_supervisor >> sunovion_user_logs_file_search_entries_29
        if_entry_col1_present_24 >> rail.Label(
            'No') >> sunovion_user_logs_file_search_entries_29 >> create_csv_lines_30 >> generate_download_link
        generate_download_link >> check_for_error_logs >> upload_uploadslogstosftp_37 >> if_log_35_blank_43
        if_log_35_blank_43 >> rail.Label(
            'Yes') >> send_mail_with_cshare_completedsuccessfully_44 >> if_log_35_present_45
        if_log_35_blank_43 >> rail.Label('No') >> if_log_35_present_45
        if_log_35_present_45 >> rail.Label(
            'Yes') >> send_mail_with_cshare_completedwitherrors_46 >> if_query_list_10_rows_less_than_1_52
        if_log_35_present_45 >> rail.Label(
            'No') >> if_query_list_10_rows_less_than_1_52
        if_query_list_10_rows_greater_than_0_11 >> rail.Label(
            'No') >> if_query_list_10_rows_less_than_1_52
        if_query_list_10_rows_less_than_1_52 >> rail.Label(
            'Yes') >> send_mail_blank_file_57 >> finish
        if_query_list_10_rows_less_than_1_52 >> rail.Label('No') >> finish
        if_cafa1634_size_greater_than_0_7 >> rail.Label(
            'No') >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
