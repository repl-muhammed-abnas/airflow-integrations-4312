
from datetime import timedelta
import hashlib
import itertools
import chardet
from rail.lib.log import get_master_log_artifact_name
from rail.lib.artifact import existing_artifact
import pendulum
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.user_import_master,
        description=f'AmericanIntegrated_User Import Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        get_time_for_file = rail.PythonOperator(
            task_id='get_time_for_file',
            python_callable=lambda: pendulum.now(
                config.pacific_timezone).strftime('%Y%m%dT%H%M%S')
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='can_run_batch_task',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_tobeusedinsubjectline_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_tobeusedinsubjectline_2',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_tobeusedinsubjectline_2 = rail.PythonOperator(
            task_id='log_tobeusedinsubjectline_2',
            python_callable=lambda:  pendulum.now(
                config.pacific_timezone).strftime("%Y-%m-%dT%H:%M:%S")
        )

        if_name_downcase_not_ends_with_txt_3 = rail.IfOperator(
            task_id='if_name_downcase_not_ends_with_txt_3',
            test='''{{ not(result('new_file_sensor') | ends_with('txt')) }}''',
            yes_task="send_mail_4",
            no_task="download_8",
        )

        send_mail_4 = rail.EmailOperator(
            task_id='send_mail_4',
            to=config.tenant_email,
            bcc=config.internal_logs_email,  # config.alert_email on error fixme
            subject='''{{ get_company_key() }} | User import file processing skipped - {{ result('log_tobeusedinsubjectline_2') }} ''',
            html_content='''<p><strong>This is a automated mail, please don't reply&nbsp;</strong></p>
            <p>Hello,</p>
            <p>The User sync into Replicon has not been processed based on filename - {{ result('new_file_sensor') | file_name }} due to incorrect file format. Please replace the file with extension (.txt)</p>
            <p>please contact our support team at https://support.deltek.com for any further assistance.</p>
            <p>Thanks, <br /> Deltek Inc.</p> ''',
            params=None,
        )

        rename_movefilefrom_inputto_archive_5 = rail.SFTPMoveFileOperator(
            task_id='rename_movefilefrom_inputto_archive_5',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            '''/Incorrectformat_{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}''',
        )

        download_8 = rail.SFTPDownloadFileOperator(
            task_id='download_8',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        def find_file_encoding_callable(task_id):
            feed_file = rail.result(task_id)
            with existing_artifact(feed_file) as ff:
                return chardet.detect_all(ff.file.read())

        find_file_encoding_8 = rail.PythonOperator(
            task_id='find_file_encoding_8',
            python_callable=find_file_encoding_callable,
            op_args=[download_8.task_id]
        )

        parse_csv_9 = rail.LoadCSVFileOperator(
            task_id="parse_csv_9",
            document="{{ result('download_8') }}",
            encoding="{{ result('find_file_encoding_8')[0].encoding}}",
            delimiter='	'
        )

        def get_formated_user_row(item):
            user_md5 = hashlib.md5((
                item["employeenumber"] +
                item["firstname"] +
                item['lastname'] +
                item['loginname'] +
                item['hiredate'] +
                item['email'] +
                item['position'] +
                item['positionname'] +
                item['employeetype'] +
                item['payfrequency'] +
                item['employeestatus']).encode()).hexdigest()

            return {
                "employeenumber": item["employeenumber"].strip() if item["employeenumber"] else "",
                "firstname": item["firstname"].strip() if item["firstname"] else "",
                "lastname": item["lastname"].strip() if item["lastname"] else "",
                "loginname": item["loginname"].strip() if item["loginname"] else "",
                "hiredate": item["hiredate"].strip() if item["hiredate"] else "",
                "email": item["email"].strip() if item["email"] else "",
                "position": item["position"].strip() if item["position"] else "",
                "positionname": item["positionname"].strip() if item["positionname"] else "",
                "employeetype": item["employeetype"].strip() if item["employeetype"] else "",
                "payfrequency": item["payfrequency"].strip() if item["payfrequency"] else "",
                "supervisorname": item["supervisorname"].strip() if item["supervisorname"] else "",
                "status": item["employeestatus"].strip() if item["employeestatus"] else "",
                "md5": user_md5
            }.values()

        load_csv_create_list_from_csv_16 = rail.WriteCSVFileOperator(
            task_id='load_csv_create_list_from_csv_16',
            source="{{ result('parse_csv_9') }}",
            header=['employeenumber',
                    'firstname',
                    'lastname',
                    'loginname',
                    'hiredate',
                    'email',
                    'position',
                    'positionname',
                    'employeetype',
                    'payfrequency',
                    'supervisorname',
                    'status',
                    'md5'],
            row=get_formated_user_row,
            encoding='utf-8'
        )

        create_collection_create_list_from_csv_16 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_16',
            source="{{ result('load_csv_create_list_from_csv_16') }}",
            name="input_data_file_refreshed",
            columns={
                'employeenumber': 'employeenumber',
                'firstname': 'firstname',
                'lastname': 'lastname',
                'loginname': 'loginname',
                'hiredate': 'hiredate',
                'email': 'email',
                'position': 'position',
                'positionname': 'positionname',
                'employeetype': 'employeetype',
                'payfrequency': 'payfrequency',
                'supervisorname': 'supervisorname',
                'status': 'status',
                'md5': 'md5'
            }
        )

        if_parse_csv_9_lines_less_than_1_16 = rail.IfOperator(
            task_id='if_parse_csv_9_lines_less_than_1_16',
            test='{{ result("create_collection_create_list_from_csv_16", "length") == 0 }}',
            yes_task="send_mail_16",
            no_task="dir_17",
        )

        send_mail_16 = rail.EmailOperator(
            task_id='send_mail_16',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | User import - No records found - {{ result('log_tobeusedinsubjectline_2') }} ''',
            html_content='''<p><strong>This is a automated mail, please don't reply&nbsp;</strong></p>
            <p>Hello,</p>
            <p>The User sync into Replicon has been processed based on filename - {{ result('new_file_sensor') | file_name }} and there was no data found in the feedfile to be imported.</p>
            <p>please contact our support team at https://support.deltek.com for any further assistance.</p>
            <p>Thanks, <br /> Deltek Inc.</p> ''',
            params=None,
        )

        rename_movefilefrom_inputto_archive_16 = rail.SFTPMoveFileOperator(
            task_id='rename_movefilefrom_inputto_archive_16',
            new_filename=config.archive_filepath +
            '''/No Data_{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}''',
            existing_filename='''{{ result('new_file_sensor') }}''',
        )

        dir_17 = rail.SFTPListFilesOperator(
            task_id='dir_17',
            paths=[config.referance_filepath],
        )

        def has_any_file(result_task_id, input_file_path):
            if not result_task_id or not input_file_path:
                raise Exception(
                    "Task_id" if not result_task_id else "input path" + "is not provided")
            data = rail.result(result_task_id)
            if not data:
                return False
            return len(data[input_file_path]) > 0

        if_first_name_blank_18 = rail.IfOperator(
            task_id="if_first_name_blank_18",
            test=lambda: has_any_file(
                "dir_17", config.referance_filepath),
            yes_task="get_referance_file_name_20",
            no_task="log_to_sumo"
        )

        def get_refrance_file_path():
            referance_info = rail.result('dir_17')
            file_info = referance_info[config.referance_filepath][0]
            return file_info['name']

        get_referance_file_name_20 = rail.PythonOperator(
            task_id='get_referance_file_name_20',
            python_callable=get_refrance_file_path
        )

        download_20 = rail.SFTPDownloadFileOperator(
            task_id='download_20',
            remote_filepath=config.referance_filepath + "/" +
            '''{{ result('get_referance_file_name_20') }}'''
        )

        load_csv_create_list_from_csv_21 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_21",
            document="{{ result('download_20') }}"
        )

        def get_formated_user_referance_row(item):
            return {
                "employeenumber": item["employeenumber"].strip() if item["employeenumber"] else "",
                "firstname": item["firstname"].strip() if item["firstname"] else "",
                "lastname": item["lastname"].strip() if item["lastname"] else "",
                "loginname": item["loginname"].strip() if item["loginname"] else "",
                "hiredate": item["hiredate"].strip() if item["hiredate"] else "",
                "email": item["email"].strip() if item["email"] else "",
                "position": item["position"].strip() if item["position"] else "",
                "positionname": item["positionname"].strip() if item["positionname"] else "",
                "employeetype": item["employeetype"].strip() if item["employeetype"] else "",
                "payfrequency": item["payfrequency"].strip() if item["payfrequency"] else "",
                "supervisorname": item["supervisorname"].strip() if item["supervisorname"] else "",
                "status": item["status"].strip() if item["status"] else "",
                "md5": item["md5"].strip() if item["md5"] else "",
            }.values()

        write_csv_from_csv_21 = rail.WriteCSVFileOperator(
            task_id='write_csv_from_csv_21',
            source="{{ result('load_csv_create_list_from_csv_21') }}",
            header=['employeenumber',
                    'firstname',
                    'lastname',
                    'loginname',
                    'hiredate',
                    'email',
                    'position',
                    'positionname',
                    'employeetype',
                    'payfrequency',
                    'supervisorname',
                    'status',
                    'md5'],
            row=get_formated_user_referance_row
        )

        create_collection_create_list_from_csv_21 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_21',
            source="{{ result('write_csv_from_csv_21') }}",
            name="reference_file",
            columns={
                'employeenumber': 'employeenumber',
                'firstname': 'firstname',
                'lastname': 'lastname',
                'loginname': 'loginname',
                'hiredate': 'hiredate',
                'email': 'email',
                'position': 'position',
                'positionname': 'positionname',
                'employeetype': 'employeetype',
                'payfrequency': 'payfrequency',
                'supervisorname': 'supervisorname',
                'status': 'status',
                'md5': 'md5'
            }
        )

        query_list_new_changed_listof_users_22 = rail.QueryCollectionOperator(
            task_id='query_list_new_changed_listof_users_22',
            query="""SELECT  *  FROM  input_data_file_refreshed  WHERE  input_data_file_refreshed.md5 NOT IN (SELECT DISTINCT  reference_file.md5 FROM  reference_file)""",
        )

        query_list_unchanged_listof_users_24 = rail.QueryCollectionOperator(
            task_id='query_list_unchanged_listof_users_24',
            query="""SELECT  *  FROM  input_data_file_refreshed  WHERE  input_data_file_refreshed.md5 IN (SELECT DISTINCT  reference_file.md5 FROM  reference_file)""",
        )

        if_query_list_new_changed_listof_users_22_rows_greater_than_0_26 = rail.IfOperator(
            task_id='if_query_list_new_changed_listof_users_22_rows_greater_than_0_26',
            test='{{ result("query_list_new_changed_listof_users_22", "length") > 0 }}',
            yes_task="declare_list_dag_runs_27",
            no_task="if_query_list_new_changed_listof_users_22_rows_less_than_1_whennodeltafound_61",
        )

        declare_list_dag_runs_27 = rail.SetVariableOperator(
            task_id='declare_list_dag_runs_27',
            name='user_process_dag_runs',
            value=[]
        )

        rename_movefilefrom_inputto_archive_47 = rail.SFTPMoveFileOperator(
            task_id='rename_movefilefrom_inputto_archive_47',
            new_filename=config.archive_filepath +
            '''/{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}''',
            existing_filename='''{{ result('new_file_sensor') }}''',
        )

        foreach_query_list_new_changed_listof_users_22_28 = rail.ForEachOperator(
            task_id='foreach_query_list_new_changed_listof_users_22_28',
            items="{{ result('query_list_new_changed_listof_users_22') }}",
            start_task='if_foreach_query_list_new_changed_listof_users_22_28_employeenumber_present_29',
            end_task='foreach_query_list_new_changed_listof_users_22_28_end'
        )

        if_foreach_query_list_new_changed_listof_users_22_28_employeenumber_present_29 = rail.IfOperator(
            task_id='if_foreach_query_list_new_changed_listof_users_22_28_employeenumber_present_29',
            test='''{{ result('foreach_query_list_new_changed_listof_users_22_28').employeenumber | is_truthy }}''',
            yes_task="search_users_searchuserbasedonemployeenumber_31",
            no_task="american_integrated_user_import_logs_add_entry_40",
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def compose_user_details(response, loginname):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            users_info = list(filter(lambda x: x['employeeid'] == loginname, map(lambda row: {
                'loginname': row['cells'][1]['textValue'] if 'textValue' in row['cells'][1] else None,
                'employeeid': row['cells'][2]['textValue'] if 'textValue' in row['cells'][2] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))
            return users_info[0] if users_info else None

        search_users_searchuserbasedonemployeenumber_31 = rail.RepliconServicePageOperator(
            task_id='search_users_searchuserbasedonemployeenumber_31',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                "sort": [],
                "filterExpression": {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('foreach_query_list_new_changed_listof_users_22_28')['employeenumber'],
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda response: compose_user_details(
                response, rail.result('foreach_query_list_new_changed_listof_users_22_28')['employeenumber'])
        )

        if_log_gettherequiredusersuri_34_blank_35 = rail.IfOperator(
            task_id='if_log_gettherequiredusersuri_34_blank_35',
            test='''{{ result('search_users_searchuserbasedonemployeenumber_31') | is_falsy }}''',
            yes_task="trigger_dag_run_live_american_integrated_user_add_v1_0_childasync_36",
            no_task="trigger_dag_run_live_american_integrated_user_update_v1_0_childasync_38",
        )

        trigger_dag_run_live_american_integrated_user_add_v1_0_childasync_36 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_american_integrated_user_add_v1_0_childasync_36',
            retries=0,
            items=[-1],
            trigger_dag_id=config.user_import_add_child,
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda: {
                "employeenumber": rail.result('foreach_query_list_new_changed_listof_users_22_28')['employeenumber'],
                "firstname": rail.result('foreach_query_list_new_changed_listof_users_22_28')['firstname'],
                "lastname": rail.result('foreach_query_list_new_changed_listof_users_22_28')['lastname'],
                "hiredate": rail.result('foreach_query_list_new_changed_listof_users_22_28')['hiredate'],
                "email": rail.result('foreach_query_list_new_changed_listof_users_22_28')['email'],
                "position": rail.result('foreach_query_list_new_changed_listof_users_22_28')['position'],
                "positionname": rail.result('foreach_query_list_new_changed_listof_users_22_28')['positionname'],
                "employeetype": rail.result('foreach_query_list_new_changed_listof_users_22_28')['employeetype'],
                "supervisorname": rail.result('foreach_query_list_new_changed_listof_users_22_28')['supervisorname'],
                "payfrequency": rail.result('foreach_query_list_new_changed_listof_users_22_28')['payfrequency'],
                "status": rail.result('foreach_query_list_new_changed_listof_users_22_28')['status']
            }
        )

        trigger_dag_run_live_american_integrated_user_update_v1_0_childasync_38 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_american_integrated_user_update_v1_0_childasync_38',
            retries=0,
            items=[-1],
            trigger_dag_id=config.user_import_update_child,
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda: {
                "employeenumber": rail.result('foreach_query_list_new_changed_listof_users_22_28')['employeenumber'],
                "firstname": rail.result('foreach_query_list_new_changed_listof_users_22_28')['firstname'],
                "lastname": rail.result('foreach_query_list_new_changed_listof_users_22_28')['lastname'],
                "hiredate": rail.result('foreach_query_list_new_changed_listof_users_22_28')['hiredate'],
                "email": rail.result('foreach_query_list_new_changed_listof_users_22_28')['email'],
                "position": rail.result('foreach_query_list_new_changed_listof_users_22_28')['position'],
                "positionname": rail.result('foreach_query_list_new_changed_listof_users_22_28')['positionname'],
                "employeetype": rail.result('foreach_query_list_new_changed_listof_users_22_28')['employeetype'],
                "supervisorname": rail.result('foreach_query_list_new_changed_listof_users_22_28')['supervisorname'],
                "payfrequency": rail.result('foreach_query_list_new_changed_listof_users_22_28')['payfrequency'],
                "status": rail.result('foreach_query_list_new_changed_listof_users_22_28')['status'],
                "useruri": rail.result('search_users_searchuserbasedonemployeenumber_31')['useruri']
            }
        )

        insert_to_user_dag_run_list_38 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_38',
            append=True,
            name='{{ result("declare_list_dag_runs_27").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_american_integrated_user_add_v1_0_childasync_36") or result("trigger_dag_run_live_american_integrated_user_update_v1_0_childasync_38"))[0]}}'
        )

        is_adduser_trigger_runs_avaialbale_38 = rail.IfOperator(
            task_id='is_adduser_trigger_runs_avaialbale_38',
            test='''{{ result('insert_to_user_dag_run_list_38') | is_truthy }}''',
            yes_task="wait_for_completion_trigger_dag_run_live_intercontinentalexchange_38",
            no_task="if_query_list_unchanged_listof_users_24_rows_greater_than_0_41",
        )

        wait_for_completion_trigger_dag_run_live_intercontinentalexchange_38 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_intercontinentalexchange_38',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list_38").value | to_json }}'
        )

        if_foreach_query_list_new_changed_listof_users_22_28_employeenumber_blank_39 = rail.IfOperator(
            task_id='if_foreach_query_list_new_changed_listof_users_22_28_employeenumber_blank_39',
            test='''{{ result('foreach_query_list_new_changed_listof_users_22_28').employeenumber | is_falsy }}''',
            yes_task="american_integrated_user_import_logs_add_entry_40",
            no_task="foreach_query_list_new_changed_listof_users_22_28_end",
        )

        american_integrated_user_import_logs_add_entry_40 = rail.WriteLogOperator(
            task_id='american_integrated_user_import_logs_add_entry_40',
            message="na",
            severity="Exception",
            properties={
                "Username": "{{ result('foreach_query_list_new_changed_listof_users_22_28').firstname }} {{ result('foreach_query_list_new_changed_listof_users_22_28').lastname }}",
                "Status": "Skipped",
                "action": "NA",
                "Details": "Employee number not present"
            }
        )

        foreach_query_list_new_changed_listof_users_22_28_end = rail.EmptyOperator(
            task_id='foreach_query_list_new_changed_listof_users_22_28_end',
        )

        if_query_list_unchanged_listof_users_24_rows_greater_than_0_41 = rail.IfOperator(
            task_id='if_query_list_unchanged_listof_users_24_rows_greater_than_0_41',
            test='{{ result("query_list_unchanged_listof_users_24", "length") > 0 }}',
            yes_task="insert_to_list_42",
            no_task="rename_movefilefrom_old_referencefileto_archive_45",
        )

        insert_to_list_42 = rail.WriteLogOperator(
            task_id='insert_to_list_42',
            message="No change in user records",
            items="{{ result('query_list_unchanged_listof_users_24') }}",
            severity="Skipped",
            properties={
                "Employeeid": "{{ item.employeenumber }}",
                "Username": "{{ item.firstname }} {{ item.lastname }}",
                "Status": "Skipped ",
                "action": "NA",
                "Details": "No Change in input"
            }
        )

        rename_movefilefrom_old_referencefileto_archive_45 = rail.SFTPMoveFileOperator(
            task_id='rename_movefilefrom_old_referencefileto_archive_45',
            new_filename=config.archive_filepath +
            '''/Old_reference_{{ dag_run_ecid() }}_{{ result('get_referance_file_name_20') }}''',
            existing_filename=config.referance_filepath +
            '''/{{ result('get_referance_file_name_20') }}''',
        )

        upload_uploadsreferencefiletosftp_46 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadsreferencefiletosftp_46',
            content='''{{ result('load_csv_create_list_from_csv_16') }}''',
            remote_filepath=config.referance_filepath +
            '''/New_Reference_{{ result('new_file_sensor')| file_name | replace('.txt', '.csv') }}''',
        )

        def do_format_logs():
            context = get_master_log_artifact_name(rail.get_current_context())
            user_import_log = rail.load_all_records(context)
            unique_users = list(
                set(map(lambda item: item['properties'].get(
                    "Employeeid", ''), user_import_log))
            )

            def get_status_details(user_logs):
                return ";".join(list(filter(bool, (set(map(lambda x: x['properties']['Status'], user_logs))))))

            def get_log_details(user_logs):
                return "|".join(list(filter(bool, (set(map(lambda x: x['properties']['Details'], user_logs))))))

            logs = []
            # pylint: disable= cell-var-from-loop
            for employee_id in unique_users:
                if employee_id:
                    user_logs = list(
                        filter(lambda x: x['properties'].get(
                            'Employeeid', '') == employee_id, user_import_log)
                    )

                    if len(user_logs) > 0:
                        first = user_logs[0]
                        logs.append(
                            {
                                "Employee ID": employee_id,
                                "User Name": first['properties'].get('Username', ''),
                                "Action": first['properties'].get('action', ''),
                                "Status": get_status_details(user_logs),
                                "Details": get_log_details(user_logs),
                                "Jobid": first['ecid']
                            }
                        )
                else:
                    user_logs = list(
                        filter(lambda x: x['properties'].get(
                            'Employeeid', '') == '' or x['properties'].get(
                            'Employeeid', '') is None, user_import_log)
                    )
                    for user in user_logs:
                        logs.append(
                            {
                                "Employee ID": user['properties'].get('Employeeid', ''),
                                "User Name": user['properties'].get('Username', ''),
                                "Action": user['properties'].get('action', ''),
                                "Status": user['properties'].get('Status', ''),
                                "Details": user['properties'].get('Details', ''),
                                "Jobid": user['ecid']
                            }
                        )

            return logs

        log_merge_52 = rail.PythonOperator(
            task_id='log_merge_52',
            python_callable=do_format_logs
        )

        create_csv_lines_52 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_52',
            source="{{ result('log_merge_52') | to_json }}",
            header=['Employee ID',
                    'User Name',
                    'Status',
                    'Action',
                    'Details',
                    'Jobid'],
            row=[
                '{{ item | attr_or_default("Employee ID", "") }}',
                '{{ item | attr_or_default("User Name", "") }}',
                '{{ item | attr_or_default("Status", "") }}',
                '{{ item | attr_or_default("Action", "")}}',
                '{{ item | attr_or_default("Details", "") }}',
                '{{ item | attr_or_default("Jobid", "") }}'],
        )

        log_filenamemodified_53 = rail.PythonOperator(
            task_id='log_filenamemodified_53',
            python_callable=lambda:  rail.render_template(
                '''{{ result('new_file_sensor') | file_name | replace('.txt', '.csv') }}''')
        )

        upload_54 = rail.SFTPUploadFileOperator(
            task_id='upload_54',
            content='''{{ result('create_csv_lines_52') }}''',
            remote_filepath=config.log_filepath +
            '''/Logs_{{ dag_run_ecid() }}_{{ result('log_filenamemodified_53') }}''',
        )

        get_logged_errors_55 = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors_55',
            severity='Error',
        )

        get_logged_exception_55 = rail.FilterLogEntriesOperator(
            task_id='get_logged_exception_55',
            severity='Exception',
        )

        def get_subject_line():
            import_completion_message = "completed succesfully"
            has_error_message = rail.render_template(
                '{{result("get_logged_errors_55", key="length") > 0}}')
            has_exception_message = rail.render_template(
                '{{result("get_logged_exception_55", key="length") > 0}}')
            if has_error_message == 'True':
                import_completion_message = "completed with errors"
            elif has_exception_message == 'True':
                import_completion_message = "completed with exceptions"
            return import_completion_message

        email_subject_line_55 = rail.PythonOperator(
            task_id='email_subject_line_55',
            python_callable=get_subject_line
        )

        def get_email_body():
            body = ''
            error_message = rail.render_template('{{result("get_logged_errors_55", key="length") > 0}}')
            if error_message == 'True':
                body = '''<br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>'''
            else:
                body = '''<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>'''
            return body

        email_body_55 = rail.PythonOperator(
            task_id='email_body_55',
            python_callable=get_email_body
        )

        send_mail_59 = rail.EmailOperator(
            task_id='send_mail_59',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors_55', key='length') > 0 -%}\
                "+config.alert_email+"\
            {%- else -%}\
                "+config.internal_logs_email+"\
            {%- endif -%}",
            subject='''{{ get_company_key() }} | User import - {{ result('email_subject_line_55') }} - {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%d %H:%M:%S") }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong></p>

            <p>Hello,</p>

            <p>The user import is {{ result('email_subject_line_55') }} based on the file - {{ result('new_file_sensor') | file_name }}.</p>

            <p>The log file - "Logs_{{ dag_run_ecid() }}_{{ result('log_filenamemodified_53') }}" is available at {{ params.sftp_path }} for reference.</p>

            {{ result('email_body_55') }}''',
            params={
                'sftp_path': config.log_filepath
            }
        )

        american_integrated_user_import_logs_truncate_60 = rail.EmptyOperator(
            task_id='american_integrated_user_import_logs_truncate_60',
        )

        if_query_list_new_changed_listof_users_22_rows_less_than_1_whennodeltafound_61 = rail.IfOperator(
            task_id='if_query_list_new_changed_listof_users_22_rows_less_than_1_whennodeltafound_61',
            test='{{ result("query_list_new_changed_listof_users_22", "length") == 0 }}',
            yes_task="rename_movefilefrom_inputto_archive_62",
            no_task="log_to_sumo",
        )

        rename_movefilefrom_inputto_archive_62 = rail.SFTPMoveFileOperator(
            task_id='rename_movefilefrom_inputto_archive_62',
            new_filename=config.archive_filepath +
            '''/No change_{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}''',
            existing_filename='''{{ result('new_file_sensor') }}''',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> get_time_for_file >> was_new_file_found
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        was_new_file_found >> rail.Label('Yes') >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> log_tobeusedinsubjectline_2
        log_tobeusedinsubjectline_2 >> if_name_downcase_not_ends_with_txt_3
        if_name_downcase_not_ends_with_txt_3 >> rail.Label(
            'Yes') >> send_mail_4 >> rename_movefilefrom_inputto_archive_5 >> log_to_sumo
        if_name_downcase_not_ends_with_txt_3 >> rail.Label(
            'No') >> download_8 >> find_file_encoding_8 >> parse_csv_9 >> \
            load_csv_create_list_from_csv_16 >> create_collection_create_list_from_csv_16 >> if_parse_csv_9_lines_less_than_1_16
        if_parse_csv_9_lines_less_than_1_16 >> rail.Label(
            'Yes') >> send_mail_16 >> rename_movefilefrom_inputto_archive_16 >> log_to_sumo
        if_parse_csv_9_lines_less_than_1_16 >> rail.Label(
            'No') >> dir_17 >> if_first_name_blank_18
        if_first_name_blank_18 >> rail.Label('No') >> log_to_sumo
        if_first_name_blank_18 >> rail.Label(
            'Yes') >> get_referance_file_name_20 >> download_20 >> load_csv_create_list_from_csv_21 >> \
            write_csv_from_csv_21 >> create_collection_create_list_from_csv_21 >> \
            query_list_new_changed_listof_users_22 >> query_list_unchanged_listof_users_24 >> if_query_list_new_changed_listof_users_22_rows_greater_than_0_26
        if_query_list_new_changed_listof_users_22_rows_greater_than_0_26 >> rail.Label(
            'Yes') >> declare_list_dag_runs_27 >> rename_movefilefrom_inputto_archive_47 >> foreach_query_list_new_changed_listof_users_22_28 >> \
            if_foreach_query_list_new_changed_listof_users_22_28_employeenumber_present_29
        if_foreach_query_list_new_changed_listof_users_22_28_employeenumber_present_29 >> rail.Label(
            'Yes') >> search_users_searchuserbasedonemployeenumber_31 >> if_log_gettherequiredusersuri_34_blank_35
        if_log_gettherequiredusersuri_34_blank_35 >> rail.Label(
            'Yes') >> trigger_dag_run_live_american_integrated_user_add_v1_0_childasync_36 >> \
            insert_to_user_dag_run_list_38
        if_log_gettherequiredusersuri_34_blank_35 >> rail.Label(
            'No') >> trigger_dag_run_live_american_integrated_user_update_v1_0_childasync_38 >> \
            insert_to_user_dag_run_list_38 >> if_foreach_query_list_new_changed_listof_users_22_28_employeenumber_blank_39
        if_foreach_query_list_new_changed_listof_users_22_28_employeenumber_present_29 >> rail.Label(
            'No') >> american_integrated_user_import_logs_add_entry_40 >> foreach_query_list_new_changed_listof_users_22_28_end
        if_foreach_query_list_new_changed_listof_users_22_28_employeenumber_blank_39 >> rail.Label(
            'Yes') >> american_integrated_user_import_logs_add_entry_40
        if_foreach_query_list_new_changed_listof_users_22_28_employeenumber_blank_39 >> rail.Label(
            'No') >> foreach_query_list_new_changed_listof_users_22_28_end
        foreach_query_list_new_changed_listof_users_22_28 >> foreach_query_list_new_changed_listof_users_22_28_end >> \
            is_adduser_trigger_runs_avaialbale_38
        is_adduser_trigger_runs_avaialbale_38 >> rail.Label('Yes') >> wait_for_completion_trigger_dag_run_live_intercontinentalexchange_38 >> \
            if_query_list_unchanged_listof_users_24_rows_greater_than_0_41
        is_adduser_trigger_runs_avaialbale_38 >> rail.Label(
            'No') >> if_query_list_unchanged_listof_users_24_rows_greater_than_0_41
        if_query_list_unchanged_listof_users_24_rows_greater_than_0_41 >> rail.Label(
            'Yes') >> insert_to_list_42 >> rename_movefilefrom_old_referencefileto_archive_45
        if_query_list_unchanged_listof_users_24_rows_greater_than_0_41 >> rail.Label(
            'No') >> rename_movefilefrom_old_referencefileto_archive_45 >> upload_uploadsreferencefiletosftp_46 >> \
            log_merge_52 >> create_csv_lines_52 >> log_filenamemodified_53 >> \
            upload_54 >> get_logged_errors_55 >> get_logged_exception_55 >> email_subject_line_55 >> email_body_55 >> send_mail_59 >> \
            american_integrated_user_import_logs_truncate_60 >> if_query_list_new_changed_listof_users_22_rows_less_than_1_whennodeltafound_61
        if_query_list_new_changed_listof_users_22_rows_greater_than_0_26 >> rail.Label(
            'No') >> if_query_list_new_changed_listof_users_22_rows_less_than_1_whennodeltafound_61
        if_query_list_new_changed_listof_users_22_rows_less_than_1_whennodeltafound_61 >> rail.Label(
            'Yes') >> rename_movefilefrom_inputto_archive_62 >> log_to_sumo
        if_query_list_new_changed_listof_users_22_rows_less_than_1_whennodeltafound_61 >> rail.Label(
            'No') >> log_to_sumo
    return dag


rail.for_each_instance(create_dag)
