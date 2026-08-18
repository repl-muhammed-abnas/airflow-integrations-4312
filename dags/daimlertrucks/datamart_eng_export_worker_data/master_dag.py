
from datetime import datetime, timedelta
import itertools
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long too-many-branches
    with rail.create_airflow_dag(
        dag_id=f'daimlertrucks_datamart_eng_export_worker_data_master_{config.instance}',
        description=f'DTNA_DataMart_Export_ENG_User Export_V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        log_get_todays_datein_y_y_y_y_m_m_d_d_h_h_m_m_s_sformat_6 = rail.PythonOperator(
            task_id='log_get_todays_datein_y_y_y_y_m_m_d_d_h_h_m_m_s_sformat_6',
            python_callable=lambda:  datetime.utcnow().strftime("%Y%m%d%H%M%S")
        )

        log_get_todays_datein_m_m_d_d_y_y_y_yformat_7 = rail.PythonOperator(
            task_id='log_get_todays_datein_m_m_d_d_y_y_y_yformat_7',
            python_callable=lambda:  datetime.utcnow().strftime("%m/%d/%Y")
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_all_reports = rail.RepliconServiceOperator(
            task_id='get_all_reports',
            endpoint='/services/ReportService1.svc/GetAllReports'
        )

        generate_report_emp_id = rail.RepliconServiceOperator(
            task_id='generate_report_emp_id',
            endpoint='/services/ReportService1.svc/GenerateReport',
            data={
                "reportUri": "{{ result('get_all_reports') | find_first_by_attr_and_get_attr('displayText','***User-Employee ID Check***','uri') }}",
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            },
        )

        load_csv_create_list_from_csv_11 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_11",
            document="{{ result('generate_report_emp_id').payload }}",
        )

        create_collection_create_list_from_csv_11 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_11',
            source="{{ result('load_csv_create_list_from_csv_11') }}",
            name="useremployeeid_list",
            columns={
                'Login Name': 'loginname',
                'Employee ID': 'employeeid',
                'User Status': 'status'
            }
        )

        trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process12 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process12',
            retries=0,
            items=[1],
            trigger_dag_id=f'daimlertrucks_datamart_eng_export_worker_data_costcenter_process_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf={
                "report_name": '***Manager ENG File***',
                "type": "Datamart Export - Manager ENG FILE - User",
                "domain": "DTNA ENG"
            }
        )

        wait_for_completion_trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process12 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process12',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process12") }}'
        )

        gather_report_filter_manager_eng = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_report_filter_manager_eng',
            dagrun_task_id='log_final_list',
            dag_runs='{{ result("trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process12") }}',
            flatten=True
        )

        get_report_filter_manager_eng = rail.PythonOperator(
            task_id='get_report_filter_manager_eng',
            python_callable=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_reports'), 'displayText', '***Manager ENG File***', 'uri'),
                        "filterValues": rail.result('gather_report_filter_manager_eng'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        generate_report_manager_eng = rail.RepliconServiceOperator(
            task_id='generate_report_manager_eng',
            endpoint='/services/ReportService1.svc/GenerateReport',
            data={
                "reportUri": "{{ result('get_all_reports') | find_first_by_attr_and_get_attr('displayText','***Manager ENG File***','uri') }}",
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            },
        )

        parse_csv_17 = rail.LoadCSVFileOperator(
            task_id="parse_csv_17",
            document="{{ result('generate_report_manager_eng').payload }}",
        )

        create_report_manager_eng_collection = rail.CreateCollectionOperator(
            task_id='create_report_manager_eng_collection',
            source='{{ result("parse_csv_17") }}'
        )

        trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process18 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process18',
            retries=0,
            items=[1],
            trigger_dag_id=f'daimlertrucks_datamart_eng_export_worker_data_costcenter_process_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf={
                "report_name": "***ENG_DataMart Worker Export-User Data****",
                "type": "Datamart Export - DTNA ENG - User",
                "domain": "DTNA ENG"
            }
        )

        wait_for_completion_trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process18 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process18',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process18") }}'
        )

        gather_report_filter_eng_data_mart_worker = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_report_filter_eng_data_mart_worker',
            dagrun_task_id='log_final_list',
            dag_runs='{{ result("trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process18") }}',
            flatten=True
        )

        get_report_filter_eng_data_mart_worker = rail.PythonOperator(
            task_id='get_report_filter_eng_data_mart_worker',
            python_callable=lambda: {
                "reportUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_reports'), 'displayText', '***ENG_DataMart Worker Export-User Data****', 'uri'),
                "filterValues": rail.result('gather_report_filter_manager_eng'),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        e_n_g_data_mart_worker_export_user_data_20 = rail.RepliconServiceOperator(
            task_id='e_n_g_data_mart_worker_export_user_data_20',
            endpoint='/services/ReportService1.svc/GenerateReport',
            data="{{ result('get_report_filter_eng_data_mart_worker') | to_json }}"
        )

        parse_csv_23 = rail.LoadCSVFileOperator(
            task_id='parse_csv_23',
            document="{{ result('e_n_g_data_mart_worker_export_user_data_20').payload }}",
        )

        create_collection_create_list_from_csv_25 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_25',
            source="{{ result('parse_csv_23') }}",
            name="input_data_file",
            columns={
                'Replicon Worker ID': 'RepliconWorkerID',
                'Hiring Manager ID': 'HiringManagerID',
                'Cost Center': 'CostCenter',
                'Cost Center Effective Date': 'CostCenterEffectiveDate',
                'Active Date': 'ActiveDate',
                'Termination Date': 'TerminationDate',
                'Login Name': 'LoginName',
                'Client Worker ID': 'ClientWorkerID',
                'Worker Type': 'WorkerType',
                'Worker First Name': 'WorkerFirstName',
                'Worker Last Name': 'WorkerLastName',
                'User Email': 'email',
                'Approver ID': 'ApproverID',
                'User Supervisor Email address': 'UserSupervisorEmailaddress',
                'Initials - ENG': 'InitialsENG',
                'Manager - ENG': 'ManagerENG',
                'user uri': 'useruri'
            }
        )

        query_list_26 = rail.QueryCollectionOperator(
            task_id='query_list_26',
            query="""SELECT DISTINCT  input_data_file.LoginName FROM  input_data_file """,
        )

        parallel_count = 20
        process_user_records = rail.trigger_parallel_dagrun(
            task_id='process_user_records',
            items="{{ result('query_list_26') }}",
            trigger_dag_id=f'daimlertrucks_datamart_eng_export_worker_data_process_user_child_{config.instance}',
            parallel_count=parallel_count,
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                **item, 'report_manager_eng_collection': rail.result('create_report_manager_eng_collection')}
        )

        get_all_child_dag_runs = rail.PythonOperator(
            task_id='get_all_child_dag_runs',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_user_records_{x+1}'), range(parallel_count)))))
        )

        gather_final_result = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_final_result',
            dagrun_task_id='final_result',
            dag_runs='{{ result("get_all_child_dag_runs") }}',
            execution_timeout=timedelta(days=14),
            flatten=True
        )

        declare_list_184 = rail.SetVariableOperator(
            task_id='declare_list_184',
            append=False,
            name='Processed Logs',
            value=[]
        )

        declare_reject_list = rail.SetVariableOperator(
            task_id='declare_reject_list',
            append=False,
            name='reject_list',
            value=[]
        )

        if_gather_final_result_list_items_greater_than_0_187 = rail.IfOperator(
            task_id='if_gather_final_result_list_items_greater_than_0_187',
            test='''{{ result('gather_final_result') | length > 0 }}''',
            yes_task="insert_to_list_190",
            no_task="if_declare_list_184_list_items_greater_than_0_194",
        )

        insert_to_list_190 = rail.SetVariableOperator(
            task_id='insert_to_list_190',
            append=False,
            name='Processed Logs',
            value=lambda: list(filter(lambda item: item['costcenter'] and item['costcentereffectivedate'] and item['status'] and item['loginname'] and item['clientworkerid'] and item['workertype']
                               and item['firstname'] and item['lastname'] and item['repliconworkerid'] and item['hiringmanagerid'] and item['activedate'], rail.result('gather_final_result')))
        )

        def get_validation_message(item):
            logs = []

            if item['repliconworkerid'] and "," in item['repliconworkerid']:
                logs.append("Replicon Worker ID include ',';")

            if item['hiringmanagerid'] and "," in item['hiringmanagerid']:
                logs.append("Hiring Manager ID include ',';")

            if item['costcenter'] and "," in item['costcenter']:
                logs.append("Cost Center include ',';")

            if item['status'] and "," in item['status']:
                logs.append("Status include ',';")

            if item['clientworkerid'] and "," in item['clientworkerid']:
                logs.append("Client Worker ID include ',';")

            if item['workertype'] and "," in item['workertype']:
                logs.append("Worker type include ',';")

            if item['firstname'] and "," in item['firstname']:
                logs.append("Worker First name include ',';")

            if item['lastname'] and "," in item['lastname']:
                logs.append("Worker Last name include ',';")

            if item['repliconworkerid'] and len(item['repliconworkerid']) > 50:
                logs.append(
                    "Replicon Worker ID is greater than 50 characters;")

            if item['hiringmanagerid'] and len(item['hiringmanagerid']) > 50:
                logs.append("Hiring Manager ID is greater than 50 characters;")

            if item['costcenter'] and len(item['costcenter']) > 100:
                logs.append("Cost Center is greater than 100 characters;")

            if item['status'] and len(item['status']) > 30:
                logs.append("Status is greater than 30 characters;")

            if item['loginname'] and len(item['loginname']) > 50:
                logs.append(
                    "Replicon Login Name is greater than 50 characters;")

            if item['clientworkerid'] and len(item['clientworkerid']) > 50:
                logs.append("Client Worker ID is greater than 50 characters;")

            if item['workertype'] and len(item['workertype']) > 30:
                logs.append("worker type is greater than 50 characters;")

            if item['firstname'] and len(item['firstname']) > 50:
                logs.append("Worker first name is greater than 50 characters;")
            if item['lastname'] and len(item['lastname']) > 50:
                logs.append("Worker last name is greater than 50 characters;")
            if item['email'] and len(item['email']) > 150:
                logs.append("email is greater than 150 characters;")
            if item['approverid'] and len(item['approverid']) > 50:
                logs.append("approverid is greater than 50 characters;")

            if item['initialseng'] and len(item['initialseng']) > 10:
                logs.append("Initials -Eng is greater than 10 characters;")
            if item['managereng'] and len(item['managereng']) > 30:
                logs.append("Manager - Eng  is greater than 30 characters;")

            if item['email'] and "," in item['email']:
                logs.append("Worker Email Address include ',';")

            if item['approverid'] and "," in item['approverid']:
                logs.append("Approver ID include ',';")

            if item['initialseng'] and "," in item['initialseng']:
                logs.append("Initials-Eng include ',';")

            if item['managereng'] and "," in item['managereng']:
                logs.append("Manager-Eng include ',';")

            if item['loginname'] and "," in item['loginname']:
                logs.append("Replicon Login Name include ',';")

            return "".join(logs)

        insert_to_list_193 = rail.SetVariableOperator(
            task_id='insert_to_list_193',
            append=False,
            name='reject_list',
            value=lambda: list(map(lambda item: {**item, "reason": get_validation_message(item)}, filter(lambda item: not (item['costcenter'] and item['costcentereffectivedate'] and item['status'] and item['loginname'] and item['clientworkerid'] and item['workertype']
                               and item['firstname'] and item['lastname'] and item['repliconworkerid'] and item['hiringmanagerid'] and item['activedate']), rail.result('gather_final_result')))
                               )
        )

        if_declare_list_184_list_items_greater_than_0_194 = rail.IfOperator(
            task_id='if_declare_list_184_list_items_greater_than_0_194',
            test='''{{ dag_run_var(result('declare_list_184').name) | length > 0 }}''',
            yes_task="create_csv_lines_195",
            no_task="dir_207",
        )

        create_csv_lines_195 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_195',
            source="{{ dag_run_var(result('declare_list_184').name) | to_json }}",
            header=['reference',
                    'repliconworkerid',
                    'hiringmanagerid',
                    'costcenter',
                    'costcentereffectivedate',
                    'activedate',
                    'terminationdate',
                    'status',
                    'repliconloginname',
                    'clientworkerid',
                    'workertype',
                    'workerfirstname',
                    'workerlastname',
                    'workeremailaddress',
                    'approverid',
                    'initialseng',
                    'managereng'],
            row=[
                "{{ item.repliconworkerid }}{{ item.hiringmanagerid }}{{ item.costcenter }}{{ item.costcentereffectivedate }}",
                "{{ item.repliconworkerid }}",
                "{{ item.hiringmanagerid }}",
                "{{ item.costcenter }}",
                "{{ item.costcentereffectivedate }}",
                "{{ item.activedate }}",
                "{{ item.terminationdate }}",
                "{{ item.status }}",
                "{{ item.loginname }}",
                "{{ item.clientworkerid }}",
                "{{ item.workertype }}",
                "{{ item.firstname }}",
                "{{ item.lastname }}",
                "{{ item.email }}",
                "{{ item.approverid }}",
                "{{ item.initialseng }}",
                "{{ item.managereng }}"
            ],
        )

        load_csv_create_list_from_csv_196 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_196",
            document="{{ result('create_csv_lines_195') }}",
        )

        create_collection_create_list_from_csv_196 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_196',
            source="{{ result('load_csv_create_list_from_csv_196') }}",
            name="referencefile",
            columns={
                'reference': 'reference',
                'repliconworkerid': 'repliconworkerid',
                'hiringmanagerid': 'hiringmanagerid',
                'costcenter': 'costcenter',
                'costcentereffectivedate': 'costcentereffectivedate',
                'activedate': 'activedate',
                'terminationdate': 'terminationdate',
                'status': 'status',
                'repliconloginname': 'repliconloginname',
                'clientworkerid': 'clientworkerid',
                'workertype': 'workertype',
                'workerfirstname': 'workerfirstname',
                'workerlastname': 'workerlastname',
                'workeremailaddress': 'workeremailaddress',
                'approverid': 'approverid',
                'initialseng': 'initialseng',
                'managereng': 'managereng'
            }
        )

        process_item_records = rail.trigger_parallel_dagrun(
            task_id='process_item_records',
            items="{{ dag_run_var(result('declare_list_184').name) | to_json }}",
            trigger_dag_id=f'daimlertrucks_datamart_eng_export_worker_data_process_item_child_{config.instance}',
            parallel_count=parallel_count,
            execution_timeout=timedelta(days=14),
        )

        get_all_item_dag_runs = rail.PythonOperator(
            task_id='get_all_item_dag_runs',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_item_records_{x+1}'), range(parallel_count)))))
        )

        gather_final_item_valid_result = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_final_item_valid_result',
            dagrun_task_id='log_final_valid_entry',
            dag_runs='{{ result("get_all_item_dag_runs") }}',
            execution_timeout=timedelta(days=14),
            flatten=True
        )

        gather_final_item_reject_result_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_final_item_reject_result_child',
            dagrun_task_id='log_final_reject_entry',
            dag_runs='{{ result("get_all_item_dag_runs") }}',
            execution_timeout=timedelta(days=14),
            flatten=True
        )

        gather_final_item_reject_result = rail.PythonOperator(
            task_id='gather_final_item_reject_result',
            python_callable=lambda: rail.result(
                'gather_final_item_reject_result_child') + (rail.get_dag_run_var('reject_list') or [])
        )

        dir_207 = rail.SFTPListFilesOperator(
            task_id='dir_207',
            paths=[config.sftp_processedrecords_directory]
        )

        if_first_name_present_208 = rail.IfOperator(
            task_id='if_first_name_present_208',
            test='''{{ result('dir_207').values() | is_truthy }}''',
            yes_task="foreach_dir_207_209",
            no_task="dir_214",
        )

        foreach_dir_207_209 = rail.ForEachOperator(
            task_id='foreach_dir_207_209',
            items=lambda: list(rail.result('dir_207').values())[0],
            start_task='rename_211',
            end_task='foreach_dir_207_209_end'
        )

        rename_211 = rail.SFTPMoveFileOperator(
            task_id='rename_211',
            existing_filename=config.sftp_processedrecords_directory +
            "/" + "{{ result('foreach_dir_207_209').name}}",
            new_filename=config.sftp_archive_directory +
            "/" + "{{ result('foreach_dir_207_209').name}}",
        )

        foreach_dir_207_209_end = rail.EmptyOperator(
            task_id='foreach_dir_207_209_end',
        )

        dir_214 = rail.SFTPListFilesOperator(
            task_id='dir_214',
            paths=[config.sftp_rejectedrecords_directory]
        )

        if_first_name_present_215 = rail.IfOperator(
            task_id='if_first_name_present_215',
            test='''{{ result('dir_214').values() | is_truthy }}''',
            yes_task="foreach_dir_214_216",
            no_task="if_gather_final_item_valid_result_list_items_greater_than_0_222",
        )

        foreach_dir_214_216 = rail.ForEachOperator(
            task_id='foreach_dir_214_216',
            items=lambda: list(rail.result('dir_214').values())[0],
            start_task='if_foreach_44d6de14_216_name_not_contains_costcenter_217',
            end_task='foreach_dir_214_216_end'
        )

        if_foreach_44d6de14_216_name_not_contains_costcenter_217 = rail.IfOperator(
            task_id='if_foreach_44d6de14_216_name_not_contains_costcenter_217',
            test='''{{ not result('foreach_dir_214_216').name | matches('CostCenter') }}''',
            yes_task="rename_219",
            no_task="foreach_dir_214_216_end",
        )

        rename_219 = rail.SFTPMoveFileOperator(
            task_id='rename_219',
            existing_filename=config.sftp_rejectedrecords_directory +
            "/" + "{{ result('foreach_dir_214_216').name}}",
            new_filename=config.sftp_archive_directory +
            "/" + "{{ result('foreach_dir_214_216').name}}",
        )

        foreach_dir_214_216_end = rail.EmptyOperator(
            task_id='foreach_dir_214_216_end',
        )

        if_gather_final_item_valid_result_list_items_greater_than_0_222 = rail.IfOperator(
            task_id='if_gather_final_item_valid_result_list_items_greater_than_0_222',
            test='''{{ result('gather_final_item_valid_result') | length > 0 }}''',
            yes_task="create_csv_lines_223",
            no_task="if_gather_final_item_reject_result__greater_than_0_229",
        )

        create_csv_lines_223 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_223',
            source="{{ result('gather_final_item_valid_result') | to_json }}",
            header=['Replicon Worker ID',
                    'Hiring Manager ID',
                    'Cost Center',
                    'Cost Center Effective Date',
                    'Active Date',
                    'Termination Date',
                    'Status',
                    'Replicon Login Name',
                    'Client Worker ID',
                    'Worker Type',
                    'Worker First Name',
                    'Worker Last Name',
                    'Worker Email address',
                    'Approver ID',
                    'Initials - ENG',
                    'Manager - ENG'],
            row=[
                "{{ item.repliconworkerid.strip() }}",
                "{{ item.hiringmanagerid.strip() }}",
                "{{ item.costcenter.strip() }}",
                "{{ item.costcentereffectivedate.replace('/', '-').strip() }}",
                "{{ item.activedate.replace('/', '-').strip() }}",
                "{{ item.terminationdate.replace('/','-').strip() if item.terminationdate | is_truthy else '' }}",
                "{{ item.status.strip() }}",
                "{{ item.loginname.strip() }}",
                "{{ item.clientworkerid.strip() }}",
                "{{ item.workertype.strip() }}",
                "{{ item.firstname.strip() }}",
                "{{ item.lastname.strip() }}",
                "{{ item.email.strip()  if item.email | is_truthy else '' }}",
                "{{ item.approverid.strip()  if item.approverid | is_truthy else '' }}",
                "{{ item.initialseng.strip() if item.initialseng | is_truthy else '' }}",
                "{{ item.managereng.strip() if item.managereng | is_truthy else '' }}",
            ],
        )

        send_mail_processed_email_224 = rail.EmailOperator(
            task_id='send_mail_processed_email_224',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }}| Datamart worker data export for ENG Department completed (Processed Records File) - {{ current_time() }}''',
            html_content='''<p><strong>This is an automated mail. Please do not reply.</strong></p>
            <p>Hi Team,</p>
            <p>The 'Datamart user export' for processed records in 'DTNA ENG' department is completed successfully on '{{ result('log_get_todays_datein_m_m_d_d_y_y_y_yformat_7') }}'. Please find the file 'Replicon_WorkerEngr_Download_{{ result('log_get_todays_datein_y_y_y_y_m_m_d_d_h_h_m_m_s_sformat_6') }}.csv' at {{ params.process_dir }} on the SFTP server.</p>
            <p>For any issue, Please contact our support team at https://support.deltek.com</p>
            <p>Regards,<br>Deltek Inc.</br></p> ''',
            params={
                'process_dir': config.sftp_processedrecords_directory
            },
        )

        upload_upload_processed_file_226 = rail.SFTPUploadFileOperator(
            task_id='upload_upload_processed_file_226',
            content="{{ result('create_csv_lines_223') }}",
            remote_filepath=config.sftp_processedrecords_directory +
            "/Replicon_WorkerEngr_Download_{{ result('log_get_todays_datein_y_y_y_y_m_m_d_d_h_h_m_m_s_sformat_6') }}.csv"
        )

        if_gather_final_item_reject_result__greater_than_0_229 = rail.IfOperator(
            task_id='if_gather_final_item_reject_result__greater_than_0_229',
            test='''{{ result('gather_final_item_reject_result') | length  > 0 }}''',
            yes_task="create_csv_lines_230",
            no_task="finish",
        )

        create_csv_lines_230 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_230',
            source="{{ result('gather_final_item_reject_result') | to_json }}",
            header=['Replicon Worker ID',
                    'Hiring Manager ID',
                    'Cost Center',
                    'Cost Center Effective Date',
                    'Active Date',
                    'Termination Date',
                    'Status',
                    'Replicon Login Name',
                    'Client Worker ID',
                    'Worker Type',
                    'Worker First Name',
                    'Worker Last Name',
                    'Worker Email address',
                    'Approver ID',
                    'Initials - ENG',
                    'Manager - ENG',
                    'Reason'],
            row=[
                "{{ item.repliconworkerid.strip() if item.repliconworkerid else ''}}",
                "{{ item.hiringmanagerid.strip() if item.hiringmanagerid else ''}}",
                "{{ item.costcenter.strip() if item.costcenter else ''}}",
                "{{ item.costcentereffectivedate.replace('/','-') if item.costcentereffectivedate else ''}}",
                "{{ item.activedate.replace('/','-') if item.activedate else ''}}",
                "{{ item.terminationdate.replace('/','-') if item.terminationdate else ''}}",
                "{{ item.status.strip() if item.status else ''}}",
                "{{ item.loginname.strip() if item.loginname else ''}}",
                "{{ item.clientworkerid.strip() if item.clientworkerid else ''}}",
                "{{ item.workertype.strip() if item.workertype else ''}}",
                "{{ item.firstname.strip() if item.firstname else ''}}",
                "{{ item.lastname.strip() if item.lastname else ''}}",
                "{{ item.email.strip() if item.email else ''}}",
                "{{ item.approverid.strip() if item.approverid else ''}}",
                "{{ item.initialseng.strip() if item.initialseng else ''}}",
                "{{ item.managereng.strip() if item.managereng else ''}}",
                "{{ item.reason }}"
            ],
        )

        send_mail_rejected_email_231 = rail.EmailOperator(
            task_id='send_mail_rejected_email_231',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }}| Datamart worker data export for ENG Department completed (Rejected Records File) - {{ current_time()}}''',
            html_content='''<p><strong>This is an automated mail. Please do not reply.</strong></p>
            <p>Hi Team,</p>
            <p>The 'Datamart user export' for rejected records in 'DTNA ENG' department is completed successfully on '{{ result('log_get_todays_datein_m_m_d_d_y_y_y_yformat_7') }}'. Please find the rejected records file 'Replicon_WorkerEngr_Rejectedrecords_{{ result('log_get_todays_datein_y_y_y_y_m_m_d_d_h_h_m_m_s_sformat_6') }}.csv' at {{ params.sftp_rejectedrecords_directory }} on the SFTP server.</p>
            <p>For any issue, Please contact our support team at https://support.deltek.com</p>
            <p>Regards,<br>Deltek Inc.</br></p> ''',
            params={
                'sftp_rejectedrecords_directory': config.sftp_rejectedrecords_directory
            },
        )

        upload_upload_rejected_file_233 = rail.SFTPUploadFileOperator(
            task_id='upload_upload_rejected_file_233',
            content="{{ result('create_csv_lines_230') }}",
            remote_filepath=config.sftp_rejectedrecords_directory +
            "/Replicon_WorkerEngr_Rejectedrecords_{{ result('log_get_todays_datein_y_y_y_y_m_m_d_d_h_h_m_m_s_sformat_6') }}.csv"
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_get_todays_datein_y_y_y_y_m_m_d_d_h_h_m_m_s_sformat_6 >> log_get_todays_datein_m_m_d_d_y_y_y_yformat_7 >> create_log >> get_all_reports >> generate_report_emp_id >> load_csv_create_list_from_csv_11 >> create_collection_create_list_from_csv_11 >> trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process12 >> wait_for_completion_trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process12 >> gather_report_filter_manager_eng >> get_report_filter_manager_eng >> generate_report_manager_eng >> parse_csv_17 >> create_report_manager_eng_collection >> trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process18 >> wait_for_completion_trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process18 >> gather_report_filter_eng_data_mart_worker >> get_report_filter_eng_data_mart_worker >> e_n_g_data_mart_worker_export_user_data_20 >> parse_csv_23 >> create_collection_create_list_from_csv_25 >> query_list_26 >> process_user_records >> get_all_child_dag_runs >> gather_final_result >> declare_list_184
        declare_list_184 >> declare_reject_list >> if_gather_final_result_list_items_greater_than_0_187
        if_gather_final_result_list_items_greater_than_0_187 >> rail.Label(
            'Yes') >> insert_to_list_190 >> insert_to_list_193 >> if_declare_list_184_list_items_greater_than_0_194
        if_gather_final_result_list_items_greater_than_0_187 >> rail.Label(
            'No') >> if_declare_list_184_list_items_greater_than_0_194
        if_declare_list_184_list_items_greater_than_0_194 >> rail.Label(
            'Yes') >> create_csv_lines_195 >> load_csv_create_list_from_csv_196 >> create_collection_create_list_from_csv_196 >> process_item_records >> get_all_item_dag_runs >> gather_final_item_valid_result >> gather_final_item_reject_result_child >> gather_final_item_reject_result >> dir_207
        if_declare_list_184_list_items_greater_than_0_194 >> rail.Label(
            'No') >> dir_207 >> if_first_name_present_208
        if_first_name_present_208 >> rail.Label(
            'Yes') >> foreach_dir_207_209 >> rename_211 >> foreach_dir_207_209_end
        foreach_dir_207_209 >> foreach_dir_207_209_end >> dir_214
        if_first_name_present_208 >> rail.Label(
            'No') >> dir_214 >> if_first_name_present_215
        if_first_name_present_215 >> rail.Label(
            'Yes') >> foreach_dir_214_216 >> if_foreach_44d6de14_216_name_not_contains_costcenter_217
        if_foreach_44d6de14_216_name_not_contains_costcenter_217 >> rail.Label(
            'Yes') >> rename_219 >> foreach_dir_214_216_end >> if_gather_final_item_valid_result_list_items_greater_than_0_222
        if_foreach_44d6de14_216_name_not_contains_costcenter_217 >> rail.Label(
            'No') >> foreach_dir_214_216_end
        foreach_dir_214_216 >> foreach_dir_214_216_end >> if_gather_final_item_valid_result_list_items_greater_than_0_222
        if_first_name_present_215 >> rail.Label(
            'No') >> if_gather_final_item_valid_result_list_items_greater_than_0_222
        if_gather_final_item_valid_result_list_items_greater_than_0_222 >> rail.Label(
            'Yes') >> create_csv_lines_223 >> send_mail_processed_email_224 >> upload_upload_processed_file_226 >> if_gather_final_item_reject_result__greater_than_0_229
        if_gather_final_item_valid_result_list_items_greater_than_0_222 >> rail.Label(
            'No') >> if_gather_final_item_reject_result__greater_than_0_229
        if_gather_final_item_reject_result__greater_than_0_229 >> rail.Label(
            'Yes') >> create_csv_lines_230 >> send_mail_rejected_email_231 >> upload_upload_rejected_file_233 >> finish
        if_gather_final_item_reject_result__greater_than_0_229 >> rail.Label(
            'No') >> finish

    return dag


rail.for_each_instance(create_dag)
