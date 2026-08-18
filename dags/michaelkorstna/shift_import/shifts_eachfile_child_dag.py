
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


# pylint: disable=too-many-statements line-too-long
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'mk_shifts_master_eachfile_v1_0_{config.instance}',
        description=f'MK_Shifts_Master_eachfile V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='download_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='download_3',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        download_3 = rail.SFTPDownloadFileOperator(
            task_id='download_3',
            remote_filepath=config.input_filepath +
            '/{{ dag_run.conf.filename }}'
        )

        load_csv_create_list_from_csv_6 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_6",
            document="{{ result('download_3') }}"
        )

        create_collection_create_list_from_csv_6 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_6',
            source="{{ result('load_csv_create_list_from_csv_6') }}",
            name="inputdata",
            columns={
                'WD ID': 'loginname',
                'Entry Date': 'entrydate',
                'Start Time': 'starttime',
                'End Time': 'endtime'
            }
        )

        download_7 = rail.SFTPDownloadFileOperator(
            task_id='download_7',
            remote_filepath='{{ dag_run.conf.userdatafilepath }}'
        )

        load_csv_userdata_list_from_csv_7 = rail.LoadCSVFileOperator(
            task_id="load_csv_userdata_list_from_csv_7",
            document="{{ result('download_7') }}"
        )

        create_collection_userdata_from_csv_7 = rail.CreateCollectionOperator(
            task_id='create_collection_userdata_from_csv_7',
            source="{{ result('load_csv_userdata_list_from_csv_7') }}",
            columns={
                'User Name': 'username',
                'Login Name': 'loginname',
                'Country (Current)': 'country',
                'Schedule Name (Current)': 'schedule',
                'UserUri': 'useruri',
                'User Status': 'status'
            }
        )

        create_shift_import_child_log = rail.CreateLogOperator(
            task_id='create_shift_import_child_log'
        )

        create_shift_import_child_for_master_log = rail.CreateLogOperator(
            task_id='create_shift_import_child_for_master_log'
        )

        query_list_unique_user_list_10 = rail.QueryCollectionOperator(
            task_id='query_list_unique_user_list_10',
            query="""SELECT DISTINCT loginname FROM inputdata"""
        )

        def get_userlist_to_process():
            users = rail.load_all_records(
                rail.result('query_list_unique_user_list_10'))
            userdatalist = rail.load_all_records(
                rail.result('create_collection_userdata_from_csv_7'))
            users_to_process = []
            for user in users:
                for userdata in userdatalist:
                    if user["loginname"] == userdata['loginname']:
                        users_to_process.append({
                            "user": user['loginname'],
                            "username": userdata['username'],
                            "country": userdata['country'],
                            "schedule": userdata['schedule'],
                            "useruri": userdata['useruri'],
                            "status": userdata['status'],
                            "process": "Yes" if userdata['status'] == "Enabled" and userdata['schedule'] == "Shift Schedule" else "No"
                        }
                        )
            return users_to_process

        invoke_custom_ruby_code_11 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_11',
            python_callable=get_userlist_to_process
        )

        def get_userlist_to_ignore():
            users = rail.load_all_records(
                rail.result('query_list_unique_user_list_10'))
            userdatalist = rail.load_all_records(
                rail.result('create_collection_userdata_from_csv_7'))
            users_to_ignore = []

            def get_reason(loginname, status, schedule):
                if loginname:
                    if status == "Enabled":
                        if schedule == "Shift Schedule":
                            return "Yes"
                        return "User is assigned with Office schedule"
                    return "User is disabled in Replicon"
                return "User not available in Replicon"
            for user in users:
                for userdata in userdatalist:
                    if user["loginname"] == userdata['loginname']:
                        users_to_ignore.append({
                            "user": user['loginname'],
                            "username": userdata['username'],
                            "country": userdata['country'],
                            "schedule": userdata['schedule'],
                            "useruri": userdata['useruri'],
                            "status": userdata['status'],
                            "process": "Yes" if userdata['status'] == "Enabled" and userdata['schedule'] == "Shift Schedule" else "No",
                            "reason": get_reason(user['loginname'], userdata['status'], userdata['schedule'])
                        }
                        )
            return users_to_ignore
        invoke_custom_ruby_code_12 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_12',
            python_callable=get_userlist_to_ignore
        )

        if_output_userlistoutput_greater_than_0_13 = rail.IfOperator(
            task_id='if_output_userlistoutput_greater_than_0_13',
            test="{{ result('invoke_custom_ruby_code_12') | length > 0 }}",
            yes_task="insert_to_list_skipped_users_14",
            no_task="foreach_output_15"
        )

        insert_to_list_skipped_users_14 = rail.WriteLogOperator(
            task_id='insert_to_list_skipped_users_14',
            log="{{ result('create_shift_import_child_log') }}",
            items="{{ result('invoke_custom_ruby_code_12') | to_json }}",
            message="na",
            severity="Info",
            properties={
                "loginname": "{{ item.user }}",
                "status": "Skipped",
                "reason": "{{ item.reason }}",
            }
        )

        declare_list_dag_runs = rail.SetVariableOperator(
            task_id='declare_list_dag_runs',
            name='user_process_dag_runs',
            value=[]
        )

        foreach_output_15 = rail.ForEachOperator(
            task_id='foreach_output_15',
            items="{{ result('invoke_custom_ruby_code_11') | to_json }}",
            start_task='query_list_userwisedata_17',
            end_task='foreach_output_15_end'
        )

        query_list_userwisedata_17 = rail.QueryCollectionOperator(
            task_id='query_list_userwisedata_17',
            query="""SELECT * FROM inputdata WHERE loginname='{{ result('foreach_output_15').user }}'"""
        )

        trigger_dag_run_live_mk_default_shift_assignment_per_user_v1_019 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_mk_default_shift_assignment_per_user_v1_019',
            retries=0,
            trigger_dag_id=f'mk_default_shift_assignment_per_user_v1_0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "useruri": "{{ result('foreach_output_15').useruri }}",
                "country": "{{ result('foreach_output_15').country }}",
                "username": "{{ result('foreach_output_15').username }}",
                "loginname": "{{ result('foreach_output_15').user }}",
                "data": "{{ result('query_list_userwisedata_17') }}",
                "logger": "{{ result('create_shift_import_child_log')}}"
            }
        )

        insert_to_per_user_dag_run_list = rail.SetVariableOperator(
            task_id='insert_to_per_user_dag_run_list',
            append=True,
            name='{{ result("declare_list_dag_runs").name }}',
            value='{{ result("trigger_dag_run_live_mk_default_shift_assignment_per_user_v1_019") }}'
        )

        foreach_output_15_end = rail.EmptyOperator(
            task_id='foreach_output_15_end'
        )

        dag_run_list_greater_than_0 = rail.IfOperator(
            task_id='dag_run_list_greater_than_0',
            test=lambda: rail.result('insert_to_per_user_dag_run_list') and len(
                rail.result('insert_to_per_user_dag_run_list')['value']) > 0,
            yes_task="wait_for_completion_trigger_dag_run_live_mk_default_shift_assignment_per_user_v1_019",
            no_task='if_output_userlistoutput_greater_than_0_24'
        )

        wait_for_completion_trigger_dag_run_live_mk_default_shift_assignment_per_user_v1_019 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_mk_default_shift_assignment_per_user_v1_019',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{  result("insert_to_per_user_dag_run_list").value | to_json }}'
        )

        if_output_userlistoutput_greater_than_0_24 = rail.IfOperator(
            task_id='if_output_userlistoutput_greater_than_0_24',
            test="{{ result('invoke_custom_ruby_code_11') | length > 0 }}",
            yes_task="insert_to_list_processed_users_25",
            no_task="invoke_custom_ruby_code_26"
        )

        insert_to_list_processed_users_25 = rail.WriteLogOperator(
            task_id='insert_to_list_processed_users_25',
            log="{{ result('create_shift_import_child_log') }}",
            items="{{ result('invoke_custom_ruby_code_11') | to_json }}",
            message="na",
            severity="Success",
            properties=lambda item: {
                "loginname": item['user'],
                "status":  "Success",
                "reason":  "Assigned Successfully",
            }
        )

        invoke_custom_ruby_code_26 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_26',
            python_callable=lambda: {
                "input": "_('data.workato_variable.declare_list_9.list_items').pluck('finallist').flatten"
            }
        )

        create_csv_lines_logs_27 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_logs_27',
            source="{{ result('create_shift_import_child_log')  }}",
            header=['Login Name',
                    'Status',
                    'Reason',
                    'Job ID'],
            row=[
                "{{ item.properties.loginname }}",
                "{{ item.properties.status }}",
                "{{ item.properties.reason }}",
                "{{ item.ecid }}"
            ]
        )

        upload_upload_log_file_33 = rail.SFTPUploadFileOperator(
            task_id='upload_upload_log_file_33',
            content="{{ result('create_csv_lines_logs_27') }}",
            remote_filepath=config.log_filepath +
            "/{{ dag_run.conf.time }}/{{ dag_run_ecid() | replace(':', '-') }}_{{ dag_run.conf.filename }}"
        )

        rename_move_filetoarchive_34 = rail.SFTPMoveFileOperator(
            task_id='rename_move_filetoarchive_34',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ dag_run.conf.filename }}",
            existing_filename=config.input_filepath +
            "/{{ dag_run.conf.filename }}"
        )

        mkshiftfilelogs_add_entry_35 = rail.WriteLogOperator(
            task_id='mkshiftfilelogs_add_entry_35',
            log="{{ result('create_shift_import_child_for_master_log') }}",
            message="na",
            severity="Processed",
            properties={
                "filename": "{{ dag_run.conf.filename }}",
                "log_file_path": config.log_filepath + "/{{ dag_run.conf.time }}/{{ dag_run_ecid() | replace(':', '-') }}_{{ dag_run.conf.filename }}",
                "status": "Processed",
                "reason": "Processed Successfully",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ result('create_shift_import_child_log') }}",
            severity="Error",
            message='{{ result(get_failed_upstream_task_ids() | first_or_default, key="error") or "Unknown error occurred" }}',
            properties={
                "filename": "{{ dag_run.conf.filename }}",
                "status": "Error",
                "childjobid": "{{ dag_run_ecid() }}",
                "log_file_path": "{{ dag_run.conf.logfilepath }}/{{ dag_run.conf.time }}/{{ dag_run_ecid() | replace(':', '-') }}_{{ dag_run.conf.filename }}",
                "reason": "{{ get_error_message() }}"
            }
        )

        rename_movefiletoarchiveonerror_38 = rail.SFTPMoveFileOperator(
            task_id='rename_movefiletoarchiveonerror_38',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ dag_run.conf.filename }}",
            existing_filename=config.input_filepath +
            "/{{ dag_run.conf.filename }}"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> download_3
        download_3 >> load_csv_create_list_from_csv_6 >> create_collection_create_list_from_csv_6 >> download_7 \
            >> load_csv_userdata_list_from_csv_7 >> create_collection_userdata_from_csv_7 \
            >> create_shift_import_child_log >> create_shift_import_child_for_master_log \
            >> query_list_unique_user_list_10 >> invoke_custom_ruby_code_11 >> invoke_custom_ruby_code_12 >> if_output_userlistoutput_greater_than_0_13
        if_output_userlistoutput_greater_than_0_13 >> rail.Label(
            'Yes') >> insert_to_list_skipped_users_14 >> declare_list_dag_runs >> foreach_output_15
        if_output_userlistoutput_greater_than_0_13 >> rail.Label(
            'No') >> foreach_output_15 >> query_list_userwisedata_17 \
            >> trigger_dag_run_live_mk_default_shift_assignment_per_user_v1_019 >> insert_to_per_user_dag_run_list \
            >> foreach_output_15_end
        foreach_output_15 >> foreach_output_15_end >> dag_run_list_greater_than_0
        dag_run_list_greater_than_0 >> rail.Label(
            'Yes') >> wait_for_completion_trigger_dag_run_live_mk_default_shift_assignment_per_user_v1_019 >> if_output_userlistoutput_greater_than_0_24
        dag_run_list_greater_than_0 >> rail.Label(
            'No') >> if_output_userlistoutput_greater_than_0_24
        if_output_userlistoutput_greater_than_0_24 >> rail.Label(
            'Yes') >> insert_to_list_processed_users_25 >> create_csv_lines_logs_27
        if_output_userlistoutput_greater_than_0_24 >> rail.Label(
            'No') >> invoke_custom_ruby_code_26 >> create_csv_lines_logs_27 >> upload_upload_log_file_33 \
            >> rename_move_filetoarchive_34 >> mkshiftfilelogs_add_entry_35 >> catch_and_log_errors

        catch_and_log_errors >> rename_movefiletoarchiveonerror_38 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
