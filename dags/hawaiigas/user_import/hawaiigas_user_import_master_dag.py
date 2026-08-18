import hashlib
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'hawaiigas_user_import_hawaiigas_user_import_master_{config.instance}',
        description=f'Live|HawaiiGas User Import Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs,
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

        archive_file=rail.SFTPMoveFileOperator(
            task_id='archive_file',
            new_filename=config.archive_filepath + "{{dag_run_ecid()}}_{{ result('new_file_sensor') | file_name }}",
            existing_filename= "{{ result('new_file_sensor') }}",
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
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_file_name_ends_with_csv_2=rail.IfOperator(
            task_id='if_file_name_ends_with_csv_2',
            test='''{{ result('new_file_sensor') | ends_with('csv') }}''',
            yes_task="create_activity_and_payrate_logs_lookuptable",
            no_task="rename_move_inputfileto_archives_86",
        )

        create_activity_and_payrate_logs_lookuptable=rail.CreateLogOperator(
            task_id='create_activity_and_payrate_logs_lookuptable',
        )

        create_userimport_logs_lookuptable=rail.CreateLogOperator(
            task_id='create_userimport_logs_lookuptable',
        )

        create_supervisor_assignment_logs_lookuptable=rail.CreateLogOperator(
            task_id='create_supervisor_assignment_logs_lookuptable',
        )

        log_timetobeusedfor_file_7=rail.PythonOperator(
            task_id='log_timetobeusedfor_file_7',
            python_callable= lambda:  datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
        )

        load_csv_create_list_from_csv_9=rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_9",
            document="{{result('download_file') }}",
        )

        create_collection_create_list_from_csv_9 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_9',
            source = "{{ result('load_csv_create_list_from_csv_9') }}",
            name = "inputfile",
            columns = {
                'Record Type':'recordtype', 
                'Employee':'employee', 
                'First Name':'firstname', 
                'Last Name':'lastname', 
                'Class ID':'classid', 
                'Employment Type':'employmenttype', 
                'Division':'division', 
                'Department':'department', 
                'Job Code':'jobcode', 
                'Supervisor':'supervisor', 
                'Vacation Balance':'vacationbalance', 
                'Sick Balance':'sickbalance', 
                'Employee Status':'employeestatus', 
                'Hire Date':'hiredate', 
                'Status':'status', 
                'Termination Date':'terminationdate', 
                'Termination Reason':'terminationreason'
            }
        )

        if_create_list_from_csv_9_row_count_equals_to_0_11=rail.IfOperator(
            task_id='if_create_list_from_csv_9_row_count_equals_to_0_11',
            test='''{{ result('create_collection_create_list_from_csv_9','length') == 0 }}''',
            yes_task="send_mail_13",
            no_task="query_list_paycoderecords_16",
        )

        send_mail_13=rail.EmailOperator(
            task_id='send_mail_13',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''HawaiiGas | User Import - no records in file  {{ current_time() }} ''',
            html_content= '''templates/no_records_in_file_mail.html''',
        )

        query_list_paycoderecords_16=rail.QueryCollectionOperator(
            task_id='query_list_paycoderecords_16',
            query="""SELECT * FROM Inputfile WHERE Inputfile.recordtype = 'PAYCODE'""",
        )

        query_list_employee_records_17=rail.QueryCollectionOperator(
            task_id='query_list_employee_records_17',
            query="""select * from  inputfile where recordtype='EMPLOYEE'""",
        )

        if_query_list_employee_records_17_rows_greater_than_0_18=rail.IfOperator(
            task_id='if_query_list_employee_records_17_rows_greater_than_0_18',
            test='''{{ result('query_list_employee_records_17','length') > 0 }}''',
            yes_task="create_csv_lines_employee_records_encoded_19",
            no_task="if_query_list_paycoderecords_16_rows_greater_than_0_58",
        )

        create_csv_lines_employee_records_encoded_19=rail.WriteCSVFileOperator(
            task_id='create_csv_lines_employee_records_encoded_19',
            source="{{ result('query_list_employee_records_17') }}",
            header=['recordtype',
                    'employee',
                    'firstname',
                    'lastname',
                    'classid',
                    'employmenttype',
                    'division',
                    'department',
                    'jobcode',
                    'supervisor',
                    'vacationbalance',
                    'sickbalance',
                    'employeestatus',
                    'hiredate',
                    'status',
                    'terminationdate',
                    'terminationreason',
                    'md5'],
            row= lambda item:[
                item['recordtype'],
                item['employee'],
                item['firstname'],
                item['lastname'],
                item['classid'],
                item['employmenttype'],
                item['division'],
                item['department'],
                item['jobcode'],
                item['supervisor'],
                item['vacationbalance'],
                item['sickbalance'],
                item['employeestatus'],
                item['hiredate'],
                item['status'],
                item['terminationdate'],
                item['terminationreason'],
                hashlib.md5((str(str(item['recordtype']) + "," + str(item['employee']) + "," + str(item['firstname']) + "," + str(item['lastname']) + "," +
                    str(item['classid']) + "," + str(item['employmenttype']) + "," + str(item['division']) + "," + str(item['department']) + "," +
                    str(item['jobcode']) + "," + str(item['supervisor']) + "," + str(item['vacationbalance']) + "," + str(item['sickbalance']) + "," +
                    str(item['employeestatus']) + "," + str(item['hiredate']) + "," + str(item['status']) + "," + str(item['terminationdate']) + "," +
                    str(item['terminationreason']))).encode('utf-8')).hexdigest()
            ],
        )

        list_reference_folder=rail.SFTPListFilesOperator(
            task_id='list_reference_folder',
            paths=[config.reference_filepath],
        )

        get_reference_file_name = rail.PythonOperator(
            task_id = 'get_reference_file_name',
            python_callable=lambda: config.reference_filepath + rail.result('list_reference_folder')[config.reference_filepath][0]['name']
        )

        download_23=rail.SFTPDownloadFileOperator(
            task_id='download_23',
            remote_filepath="{{result('get_reference_file_name')}}"

        )

        create_collection_create_list_from_csv_24 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_24',
            source = "{{ result('create_csv_lines_employee_records_encoded_19') }}",
            name = "source_data_with_md5",
            columns = {
                'recordtype':'recordtype', 
                'employee':'employee', 
                'firstname':'firstname', 
                'lastname':'lastname', 
                'classid':'classid', 
                'employmenttype':'employmenttype', 
                'division':'division', 
                'department':'department', 
                'jobcode':'jobcode', 
                'supervisor':'supervisor', 
                'vacationbalance':'vacationbalance', 
                'sickbalance':'sickbalance', 
                'employeestatus':'employeestatus', 
                'hiredate':'hiredate', 
                'status':'status', 
                'terminationdate':'terminationdate', 
                'terminationreason':'terminationreason', 
                'md5':'md5'
            }
        )

        load_csv_create_list_from_csv_25=rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_25",
            document="{{result('download_23') }}",
        )

        create_collection_create_list_from_csv_25 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_25',
            source = "{{ result('load_csv_create_list_from_csv_25') }}",
            name = "reference_list",
            columns = {
                'recordtype':'recordtype', 
                'employee':'employee', 
                'firstname':'firstname', 
                'lastname':'lastname', 
                'classid':'classid', 
                'employmenttype':'employmenttype', 
                'division':'division', 
                'department':'department', 
                'jobcode':'jobcode', 
                'supervisor':'supervisor', 
                'vacationbalance':'vacationbalance', 
                'sickbalance':'sickbalance', 
                'employeestatus':'employeestatus', 
                'hiredate':'hiredate', 
                'status':'status', 
                'terminationdate':'terminationdate', 
                'terminationreason':'terminationreason', 
                'md5':'md5'
            }
        )

        query_list_delta_values_26=rail.QueryCollectionOperator(
            task_id='query_list_delta_values_26',
            query="""SELECT * FROM  source_data_with_md5 WHERE source_data_with_md5.md5 NOT IN (SELECT DISTINCT reference_list.md5 FROM reference_list)""",
        )

        query_list_unchangedusers_27=rail.QueryCollectionOperator(
            task_id='query_list_unchangedusers_27',
            query="""SELECT  *  FROM  source_data_with_md5  WHERE  source_data_with_md5.md5 IN (SELECT DISTINCT  reference_list.md5 FROM   reference_list)""",
        )

        log_unchanged_records = rail.WriteLogOperator(
            task_id = 'log_unchanged_records',
            log="{{result('create_userimport_logs_lookuptable')}}",
            items="{{result('query_list_unchangedusers_27')}}",
            message="na",
            severity="Skipped",
            properties={
                "employeeid": "{{item.employee}}",
                "username": "{{item.firstname}} {{item.lastname}}",
                "action": "Validation",
                "status": "Skipped",
                "details": "No change in user record",
                "jobid": "{{dag_run_ecid()}}"
            }
        )

        def get_userlist(response):
            users = response['rows']
            return [{
                'loginname': user['cells'][0].get('textValue'),
                'uri': user['cells'][0].get('uri'),
                'employeeid': user['cells'][1].get('textValue'),
                'status': user['cells'][2].get('textValue'),
                'statusvalue': "Active" if 'True' in user['cells'][2].get('textValue') else "Inactive"
            } for user in users]

        get_user_data_34=rail.RepliconServiceOperator(
            task_id='get_user_data_34',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_userlist
        )

        query_list_getsickandvacationbalancefrompreviousday_38=rail.QueryCollectionOperator(
            task_id='query_list_getsickandvacationbalancefrompreviousday_38',
            query="""SELECT  reference_list.employee,  reference_list.sickbalance,  reference_list.vacationbalance FROM  reference_list""",
        )

        load_getsickandvacationbalancefrompreviousday_38 = rail.PythonOperator(
            task_id = 'load_getsickandvacationbalancefrompreviousday_38',
            python_callable=lambda: rail.load_all_records(rail.result('query_list_getsickandvacationbalancefrompreviousday_38'))
        )

        create_dags_to_wait_list = rail.SetVariableOperator(
            task_id = 'create_dags_to_wait_list',
            name='dagstowait',
            append=False,
            value=[]
        )

        foreach_query_list_delta_values_26_39=rail.ForEachOperator(
            task_id='foreach_query_list_delta_values_26_39',
            items="{{ result('query_list_delta_values_26') }}",
            start_task = 'if_foreach_query_list_delta_values_26_39_employee_present_40',
            end_task = 'foreach_query_list_delta_values_26_39_end'
        )

        if_foreach_query_list_delta_values_26_39_employee_present_40=rail.IfOperator(
            task_id='if_foreach_query_list_delta_values_26_39_employee_present_40',
            test='''{{ result('foreach_query_list_delta_values_26_39').employee | is_truthy }}''',
            yes_task="if_log_useruri_41_present_42",
            no_task="hawaiigas_userimport_logs_prod_add_entry_47",
        )

        if_log_useruri_41_present_42=rail.IfOperator(
            task_id='if_log_useruri_41_present_42',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result('get_user_data_34'),'employeeid',rail.result(
                'foreach_query_list_delta_values_26_39')['employee'],'uri','')),
            yes_task="trigger_dag_update_usersasync_43",
            no_task="trigger_dag_new_usersasync_45",
        )

        trigger_dag_update_usersasync_43=rail.TriggerDagRunOperator(
            task_id='trigger_dag_update_usersasync_43',
            retries=0,
            trigger_dag_id=f'hawaiigas_user_import_update_users_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "recordtype": rail.result('foreach_query_list_delta_values_26_39')['recordtype'],
                "employee": rail.result('foreach_query_list_delta_values_26_39')['employee'],
                "firstname": rail.result('foreach_query_list_delta_values_26_39')['firstname'],
                "lastname": rail.result('foreach_query_list_delta_values_26_39')['lastname'],
                "classid": rail.result('foreach_query_list_delta_values_26_39')['classid'],
                "employmenttype": rail.result('foreach_query_list_delta_values_26_39')['employmenttype'],
                "division": rail.result('foreach_query_list_delta_values_26_39')['division'],
                "department": rail.result('foreach_query_list_delta_values_26_39')['department'],
                "jobcode": rail.result('foreach_query_list_delta_values_26_39')['jobcode'],
                "supervisor": rail.result('foreach_query_list_delta_values_26_39')['supervisor'],
                "vacationbalance": rail.result('foreach_query_list_delta_values_26_39')['vacationbalance'],
                "sickbalance": rail.result('foreach_query_list_delta_values_26_39')['sickbalance'],
                "employeestatus": rail.result('foreach_query_list_delta_values_26_39')['employeestatus'],
                "hiredate": rail.result('foreach_query_list_delta_values_26_39')['hiredate'],
                "status": rail.result('foreach_query_list_delta_values_26_39')['status'],
                "terminationdate": rail.result('foreach_query_list_delta_values_26_39')['terminationdate'],
                "terminationreason": rail.result('foreach_query_list_delta_values_26_39')['terminationreason'],
                "usercurrentstatus": rail.find_first_by_attr_and_get_attr(rail.result('get_user_data_34'),'employeeid',rail.result(
                    'foreach_query_list_delta_values_26_39')['employee'],'status',''),
                "useruri": rail.find_first_by_attr_and_get_attr(rail.result('get_user_data_34'),'employeeid',rail.result(
                    'foreach_query_list_delta_values_26_39')['employee'],'uri',''),
                "usercurrentstatus_converted": rail.find_first_by_attr_and_get_attr(rail.result('get_user_data_34'),'employeeid',rail.result(
                    'foreach_query_list_delta_values_26_39')['employee'],'statusvalue',''),
                "vacationbalancepreviousday": rail.find_first_by_attr_and_get_attr(rail.result(
                    'load_getsickandvacationbalancefrompreviousday_38'),'employee',rail.result(
                    'foreach_query_list_delta_values_26_39')['employee'],'vacationbalancepreviousday',null),
                "sickbalancepreviousday": rail.find_first_by_attr_and_get_attr(rail.result(
                    'load_getsickandvacationbalancefrompreviousday_38'),'employee',rail.result(
                        'foreach_query_list_delta_values_26_39')['employee'],'sickbalancepreviousday',null),
                "logslookuptable": rail.result('create_userimport_logs_lookuptable'),
                "supervisorlookuptable": rail.result('create_supervisor_assignment_logs_lookuptable'),
                "callerjobid": rail.render_template('{{dag_run_ecid()}}'),
            }
        )

        trigger_dag_new_usersasync_45=rail.TriggerDagRunOperator(
            task_id='trigger_dag_new_usersasync_45',
            retries=0,
            trigger_dag_id=f'hawaiigas_user_import_hawaiigas_new_users_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "recordtype": "{{ result('foreach_query_list_delta_values_26_39').recordtype }}",
                "employee": "{{ result('foreach_query_list_delta_values_26_39').employee }}",
                "firstname": "{{ result('foreach_query_list_delta_values_26_39').firstname }}",
                "lastname": "{{ result('foreach_query_list_delta_values_26_39').lastname }}",
                "classid": "{{ result('foreach_query_list_delta_values_26_39').classid }}",
                "employmenttype": "{{ result('foreach_query_list_delta_values_26_39').employmenttype }}",
                "division": "{{ result('foreach_query_list_delta_values_26_39').division }}",
                "department": "{{ result('foreach_query_list_delta_values_26_39').department }}",
                "jobcode": "{{ result('foreach_query_list_delta_values_26_39').jobcode }}",
                "supervisor": "{{ result('foreach_query_list_delta_values_26_39').supervisor }}",
                "vacationbalance": "{{ result('foreach_query_list_delta_values_26_39').vacationbalance }}",
                "sickbalance": "{{ result('foreach_query_list_delta_values_26_39').sickbalance }}",
                "employeestatus": "{{ result('foreach_query_list_delta_values_26_39').employeestatus }}",
                "hiredate": "{{ result('foreach_query_list_delta_values_26_39').hiredate }}",
                "status": "{{ result('foreach_query_list_delta_values_26_39').status }}",
                "terminationdate": "{{ result('foreach_query_list_delta_values_26_39').terminationdate }}",
                "terminationreason": "{{ result('foreach_query_list_delta_values_26_39').terminationreason }}",
                "logslookuptable": "{{result('create_userimport_logs_lookuptable')}}",
                "callerjobid": "{{dag_run_ecid()}}",
                "supervisorlookuptable": "{{result('create_supervisor_assignment_logs_lookuptable')}}"
            }
        )

        if_child_triggered = rail.IfOperator(
            task_id = 'if_child_triggered',
            test="{{ result('trigger_dag_update_usersasync_43') | is_truthy or result('trigger_dag_new_usersasync_45') | is_truthy}}",
            yes_task="insert_dag_id_to_wait",
            no_task="foreach_query_list_delta_values_26_39_end"
        )

        insert_dag_id_to_wait = rail.SetVariableOperator(
            task_id = 'insert_dag_id_to_wait',
            append=True,
            name='dagstowait',
            value="{{ result('trigger_dag_update_usersasync_43') or result('trigger_dag_new_usersasync_45')}}"
        )

        hawaiigas_userimport_logs_prod_add_entry_47=rail.WriteLogOperator(
            task_id='hawaiigas_userimport_logs_prod_add_entry_47',
            log="{{ result('create_userimport_logs_lookuptable') }}",
            message="na",
            severity="Exception",
            properties={
                "employeeid": "{{result('foreach_query_list_delta_values_26_39').firstname}} {{result('foreach_query_list_delta_values_26_39').lastname}}",
                "action": "Invalid",
                "status": "Exception",
                "details": "User's employeeid missing",
                "jobid": "{{ dag_run_ecid() }}",
            }
        )

        foreach_query_list_delta_values_26_39_end=rail.EmptyOperator(
            task_id='foreach_query_list_delta_values_26_39_end',
        )

        if_dags_to_wait_available = rail.IfOperator(
            task_id = 'if_dags_to_wait_available',
            test="{{result('insert_dag_id_to_wait') | is_truthy}}",
            yes_task="wait_for_child_dags",
            no_task="hawaii_gas_supervisor_lookup_prod_search_entries_51"

        )

        wait_for_child_dags = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dags',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('insert_dag_id_to_wait').value | to_json }}"
        )

        hawaii_gas_supervisor_lookup_prod_search_entries_51=rail.FilterLogEntriesOperator(
            task_id='hawaii_gas_supervisor_lookup_prod_search_entries_51',
            log="{{result('create_supervisor_assignment_logs_lookuptable')}}",
            properties={
                "jobid": "{{dag_run_ecid()}}"
            }
        )

        trigger_dag_supervisor_assignmentasync_53=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_supervisor_assignmentasync_53',
            retries=0,
            items="{{ result('hawaii_gas_supervisor_lookup_prod_search_entries_51') }}",
            trigger_dag_id=f'hawaiigas_user_import_supervisor_assignment_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ item.properties.jobid }}",
                "supervisorid": "{{ item.properties.supervisorloginname }}",
                "useruri": "{{ item.properties.enduseruri }}",
                "loginname": "{{ item.properties.userloginname }}",
                "logslookuptable": "{{result('create_userimport_logs_lookuptable')}}",
                "callerjobid": "{{dag_run_ecid()}}"
            }
        )

        wait_for_completion_supervisor_assignmentasync_53 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_supervisor_assignmentasync_53',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_supervisor_assignmentasync_53") }}'
        )

        log_for_oldreferencefilename_54=rail.PythonOperator(
            task_id='log_for_oldreferencefilename_54',
            python_callable= lambda:  datetime.now().strftime("%Y_%m_%dT%H_%M_%S")
        )

        if_query_list_paycoderecords_16_rows_greater_than_0_58=rail.IfOperator(
            task_id='if_query_list_paycoderecords_16_rows_greater_than_0_58',
            test='''{{ result('query_list_paycoderecords_16','length') > 0 }}''',
            yes_task="create_csv_lines_59",
            no_task="search_user_import_logs",
        )

        create_csv_lines_59=rail.WriteCSVFileOperator(
            task_id='create_csv_lines_59',
            source="{{ result('query_list_paycoderecords_16') }}",
            header=['recordtype',
                    'employee',
                    'paycode',
                    'rate',
                    'status',
                    'encoded'],
            row=lambda item: [
                item['recordtype'],
                item['employee'],
                item['firstname'],
                item['lastname'],
                item['classid'],
                hashlib.md5((str(str(item['recordtype']) + ',' + str(item['employee']) + ',' + str(item['firstname']) + ',' + str(item['lastname']) +
                ',' + str(item['classid']))).encode('utf-8')).hexdigest()
            ],
        )

        create_collection_create_list_from_csv_60 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_60',
            source = "{{ result('create_csv_lines_59') }}",
            name = "activity_and_payrates_input",
            columns = {
                'recordtype':'recordtype', 
                'employee':'employee', 
                'paycode':'paycode', 
                'rate':'rate', 
                'status':'status', 
                'encoded':'encoded'
            }
        )

        list_activity_reference_folder=rail.SFTPListFilesOperator(
            task_id='list_activity_reference_folder',
            paths=[config.activity_reference_filepath],
        )

        get_activity_reference_filename = rail.PythonOperator(
            task_id = 'get_activity_reference_filename',
            python_callable=lambda: config.activity_reference_filepath +
                rail.result('list_activity_reference_folder')[config.activity_reference_filepath][0]['name']
        )

        download_64=rail.SFTPDownloadFileOperator(
            task_id='download_64',
            remote_filepath="{{result('get_activity_reference_filename')}}"
        )

        load_csv_create_list_from_csv_65=rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_65",
            document="{{result('download_64') }}",
        )

        create_collection_create_list_from_csv_65 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_65',
            source = "{{ result('load_csv_create_list_from_csv_65') }}",
            name = "reference_activity_and_payrates_input",
            columns = {
                'recordtype':'recordtype', 
                'employee':'employee', 
                'paycode':'paycode', 
                'rate':'rate', 
                'status':'status', 
                'encoded':'encoded'
            }
        )

        query_list_get_delta_66=rail.QueryCollectionOperator(
            task_id='query_list_get_delta_66',
            name="activity_and_payrates_list",
            query="""SELECT * FROM  activity_and_payrates_input WHERE
                activity_and_payrates_input.encoded NOT IN (SELECT DISTINCT  reference_activity_and_payrates_input.encoded FROM
                reference_activity_and_payrates_input)""",
        )

        query_list_get_unchanged_records_67=rail.QueryCollectionOperator(
            task_id='query_list_get_unchanged_records_67',
            query="""SELECT * FROM  activity_and_payrates_input WHERE
                activity_and_payrates_input.encoded IN (SELECT DISTINCT reference_activity_and_payrates_input.encoded FROM
                reference_activity_and_payrates_input)""",
        )

        log_unchanged_activity_records = rail.WriteLogOperator(
            task_id = 'log_unchanged_activity_records',
            items="{{result('query_list_get_unchanged_records_67')}}",
            log="{{result('create_activity_and_payrate_logs_lookuptable')}}",
            message="na",
            severity="Skipped",
            properties={
                "jobid": "{{dag_run_ecid()}}",
                "activityname": "{{item.paycode}}",
                "payrateamount": "{{item.rate}}",
                "childjobid": "",
                "employeeid": "{{item.employee}}",
                "status": "Validation",
                "reason": "No change in payrates",
                "username": "{{item.employee}}"
            }
        )

        query_list_select_distinctemployeesfromthepayrateandactivitylist_71=rail.QueryCollectionOperator(
            task_id='query_list_select_distinctemployeesfromthepayrateandactivitylist_71',
            query="""SELECT DISTINCT employee FROM  activity_and_payrates_list""",
        )

        trigger_dag_activity_and_pay_rate_assignmentsasync_75=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_activity_and_pay_rate_assignmentsasync_75',
            retries=0,
            items="{{ result('query_list_select_distinctemployeesfromthepayrateandactivitylist_71') }}",
            trigger_dag_id=f'hawaiigas_user_import_activity_and_pay_rate_assignments_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "employee": "{{item.employee}}",
                "activitypayratelogs": "{{result('create_activity_and_payrate_logs_lookuptable')}}",
                "callerjobid": "{{dag_run_ecid()}}"
            }
        )

        wait_for_completion_activity_and_pay_rate_assignmentsasync_75 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_activity_and_pay_rate_assignmentsasync_75',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_activity_and_pay_rate_assignmentsasync_75") }}'
        )

        search_user_import_logs = rail.FilterLogEntriesOperator(
            task_id = 'search_user_import_logs',
            log="{{result('create_userimport_logs_lookuptable')}}",
            properties={
                "jobid": "{{dag_run_ecid()}}"
            }
        )

        if_userimport_logs_present=rail.IfOperator(
            task_id='if_userimport_logs_present',
            test='''{{ result('search_user_import_logs','length') > 0 }}''',
            yes_task="compose_csv_userimport_logs",
            no_task="search_activity_payrate_logs",
        )

        compose_csv_userimport_logs=rail.WriteCSVFileOperator(
            task_id='compose_csv_userimport_logs',
            source="{{ result('search_user_import_logs') }}",
            header=['Employeeid',
                    'User Name',
                    'Action',
                    'Status',
                    'Details',
                    'JobID'],
            row=lambda item: [
                (item['properties']['employeeid'].split('|'))[0],
                item['properties']['username'] if item['properties']['action'] == 'Validation' else (item['properties']['employeeid'].split('|'))[-1],
                item['properties']['action'],
                item['properties']['status'],
                (item['properties']['details'].split('|'))[0],
                item['properties']['jobid'] if item['properties']['action'] == 'Validation' else item['properties']['jobid'] + "|" +
                (item['properties']['details'].split('|'))[-1]
            ],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name='{{result("compose_csv_userimport_logs")}}',
            output_file_name="{{ result('log_timetobeusedfor_file_7') }}_userlogs.csv",
            expires_in_seconds=7*24*60*60
        )

        search_activity_payrate_logs = rail.FilterLogEntriesOperator(
            task_id = 'search_activity_payrate_logs',
            log="{{result('create_activity_and_payrate_logs_lookuptable')}}",
            properties={
                "jobid": "{{dag_run_ecid()}}"
            }
        )

        if_activity_payrate_logs_present=rail.IfOperator(
            task_id='if_activity_payrate_logs_present',
            test='''{{ result('search_activity_payrate_logs','length') > 0 }}''',
            yes_task="compose_csv_activity_payrate_logs",
            no_task="check_if_errors_or_exceptions_present",
        )

        compose_csv_activity_payrate_logs=rail.WriteCSVFileOperator(
            task_id='compose_csv_activity_payrate_logs',
            source="{{ result('search_activity_payrate_logs') }}",
            header=['Jobid',
                    'activity name',
                    'pay rate amount',
                    'child job id',
                    'employeeid',
                    'user name',
                    'status',
                    'reason'],
            row= [
                "{{ item.properties.jobid }}",
                "{{ item.properties.activityname }}",
                "{{ item.properties.payrateamount }}",
                "{{ item.properties.childjobid }}",
                "{{ item.properties.employeeid }}",
                "{{ item.properties.username }}",
                "{{ item.properties.status }}",
                "{{ item.properties.reason }}"
            ],
        )

        generate_downloadlink = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadlink",
            artifact_name='{{result("compose_csv_activity_payrate_logs")}}',
            output_file_name="{{ result('log_timetobeusedfor_file_7') }}_activityandpayratelogs.csv",
            expires_in_seconds=7*24*60*60
        )

        def check_error_and_exceptions():
            userlogs = rail.load_all_records(rail.result('search_user_import_logs'))
            activitypayratelogs = rail.load_all_records(rail.result('search_activity_payrate_logs'))
            error_in_userlogs = rail.find_first_by_attr_and_get_attr(userlogs,'properties.status','Error','properties.status','')
            exception_in_userlogs = rail.find_first_by_attr_and_get_attr(userlogs,'properties.status','Exception','properties.status','')
            error_in_activity_payrate_logs = rail.find_first_by_attr_and_get_attr(activitypayratelogs,'properties.status','Error','properties.status','')
            exception_in_activity_payrate_logs = rail.find_first_by_attr_and_get_attr(activitypayratelogs,'properties.status',
                'Exception','properties.status','')
            return {
                'errorbody': error_in_userlogs,
                'error': error_in_userlogs or error_in_activity_payrate_logs,
                'exception': exception_in_userlogs or exception_in_activity_payrate_logs
            }

        check_if_errors_or_exceptions_present = rail.PythonOperator(
            task_id = 'check_if_errors_or_exceptions_present',
            python_callable= check_error_and_exceptions
        )

        get_subject_and_body = rail.PythonOperator(
            task_id = 'get_subject_and_body',
            python_callable=lambda: {
                "subject": "completed with errors" if rail.result('check_if_errors_or_exceptions_present')['error'] else ( "completed with exceptions" if
                    rail.result('check_if_errors_or_exceptions_present')['exception'] else "completed successfully"),
                #pylint: disable = line-too-long
                "body": "<br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>" if rail.result('check_if_errors_or_exceptions_present')['errorbody'] else "<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>"
            }
        )

        send_success_mail=rail.EmailOperator(
            task_id='send_success_mail',
            to=config.tenant_email,
            bcc="{%- if result('check_if_errors_or_exceptions_present')['error'] -%}\
                    "+config.alert_email+"\
                {%- else -%}\
                    "+config.internal_logs_email+"\
                {%- endif -%}",
            subject='''{{ get_company_key() }}| User import - {{ result('get_subject_and_body').subject }} - {{ current_time("%m/%d/%YT%H:%M:%S") }} ''',
            html_content= '''templates/success_mail.html''',
        )

        rename_move_inputfileto_archives_79=rail.SFTPMoveFileOperator(
            task_id='rename_move_inputfileto_archives_79',
            new_filename=config.archive_filepath + '''Processed_{{dag_run_ecid()}}_{{ result('new_file_sensor') | file_name }}''',
            existing_filename=config.archive_filepath + "{{dag_run_ecid()}}_{{ result('new_file_sensor') | file_name }}",
        )

        rename_moveexistingreferencefileto_archives_80=rail.SFTPMoveFileOperator(
            task_id='rename_moveexistingreferencefileto_archives_80',
            new_filename=config.archive_filepath + '''{{ result('log_for_oldreferencefilename_54') }}_reference.csv''',
            existing_filename= "{{result('get_reference_file_name')}}",
        )

        upload_uploadnewreferencefile_81=rail.SFTPUploadFileOperator(
            task_id='upload_uploadnewreferencefile_81',
            content='''{{ result('create_csv_lines_employee_records_encoded_19') }}''',
            remote_filepath= config.reference_filepath + "{{ dag_run_ecid() }}_newreference.csv",
        )

        if_query_list_get_delta_66_rows_greater_than_0_82=rail.IfOperator(
            task_id='if_query_list_get_delta_66_rows_greater_than_0_82',
            test='''{{ result('query_list_get_delta_66','length') > 0 }}''',
            yes_task="rename_moveactivityexistingreferencefileto_archives_83",
            no_task="log_to_sumo",
        )

        rename_moveactivityexistingreferencefileto_archives_83=rail.SFTPMoveFileOperator(
            task_id='rename_moveactivityexistingreferencefileto_archives_83',
            new_filename=config.archive_filepath + "{{ result('log_for_oldreferencefilename_54') }}_activityreference.csv",
            existing_filename="{{result('get_activity_reference_filename')}}",
        )

        upload_uploadnew_activityreferencefile_84=rail.SFTPUploadFileOperator(
            task_id='upload_uploadnew_activityreferencefile_84',
            content='''{{ result('create_csv_lines_59') }}''',
            remote_filepath=config.activity_reference_filepath +  '''{{ dag_run_ecid() }}_newreference.csv''',
        )

        rename_move_inputfileto_archives_86=rail.SFTPMoveFileOperator(
            task_id='rename_move_inputfileto_archives_86',
            new_filename=config.archive_filepath + '''Skipped_{{ dag_run_ecid()}}_{{ result('new_file_sensor') | file_name }}''',
            existing_filename=config.archive_filepath + "{{dag_run_ecid()}}_{{ result('new_file_sensor') | file_name }}",
        )

        send_mail_87=rail.EmailOperator(
            task_id='send_mail_87',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''HawaiiGas | User Import - Incorrect file format received - {{ current_time() }} ''',
            html_content= '''templates/incorrect_file_format_mail.html''',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> download_file >> rail.Label("Always") >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_file >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> if_file_name_ends_with_csv_2
        if_file_name_ends_with_csv_2 >> rail.Label('Yes') >> create_activity_and_payrate_logs_lookuptable >> create_userimport_logs_lookuptable
        create_userimport_logs_lookuptable >> create_supervisor_assignment_logs_lookuptable >> log_timetobeusedfor_file_7 >> load_csv_create_list_from_csv_9
        load_csv_create_list_from_csv_9 >> create_collection_create_list_from_csv_9 >> if_create_list_from_csv_9_row_count_equals_to_0_11
        if_create_list_from_csv_9_row_count_equals_to_0_11 >> rail.Label('Yes') >> send_mail_13 >> log_to_sumo
        if_create_list_from_csv_9_row_count_equals_to_0_11 >> rail.Label(
            'No') >> query_list_paycoderecords_16 >> query_list_employee_records_17 >> if_query_list_employee_records_17_rows_greater_than_0_18
        if_query_list_employee_records_17_rows_greater_than_0_18 >> rail.Label(
            'Yes') >> create_csv_lines_employee_records_encoded_19 >> list_reference_folder >> get_reference_file_name >> download_23
        download_23 >> create_collection_create_list_from_csv_24 >> load_csv_create_list_from_csv_25 >> create_collection_create_list_from_csv_25
        create_collection_create_list_from_csv_25 >> query_list_delta_values_26 >> query_list_unchangedusers_27 >> log_unchanged_records >> get_user_data_34
        get_user_data_34 >> query_list_getsickandvacationbalancefrompreviousday_38 >> load_getsickandvacationbalancefrompreviousday_38
        load_getsickandvacationbalancefrompreviousday_38 >> create_dags_to_wait_list >> foreach_query_list_delta_values_26_39
        foreach_query_list_delta_values_26_39 >> if_foreach_query_list_delta_values_26_39_employee_present_40
        if_foreach_query_list_delta_values_26_39_employee_present_40 >> rail.Label('Yes') >> if_log_useruri_41_present_42
        if_log_useruri_41_present_42 >> rail.Label('Yes')  >> trigger_dag_update_usersasync_43 >> if_child_triggered
        if_log_useruri_41_present_42 >> rail.Label('No') >> trigger_dag_new_usersasync_45 >> if_child_triggered
        if_child_triggered >> rail.Label('Yes') >> insert_dag_id_to_wait >> foreach_query_list_delta_values_26_39_end
        if_child_triggered >> rail.Label('No') >> foreach_query_list_delta_values_26_39_end
        if_foreach_query_list_delta_values_26_39_employee_present_40 >> rail.Label(
            'No') >> hawaiigas_userimport_logs_prod_add_entry_47 >> foreach_query_list_delta_values_26_39_end
        foreach_query_list_delta_values_26_39 >> foreach_query_list_delta_values_26_39_end >> if_dags_to_wait_available >> rail.Label(
            'Yes') >> wait_for_child_dags >> hawaii_gas_supervisor_lookup_prod_search_entries_51
        if_dags_to_wait_available >> rail.Label(
            'No') >> hawaii_gas_supervisor_lookup_prod_search_entries_51 >> trigger_dag_supervisor_assignmentasync_53
        trigger_dag_supervisor_assignmentasync_53 >> wait_for_completion_supervisor_assignmentasync_53
        wait_for_completion_supervisor_assignmentasync_53 >> log_for_oldreferencefilename_54 >> if_query_list_paycoderecords_16_rows_greater_than_0_58
        if_query_list_employee_records_17_rows_greater_than_0_18 >> rail.Label('No') >> if_query_list_paycoderecords_16_rows_greater_than_0_58
        if_query_list_paycoderecords_16_rows_greater_than_0_58 >> rail.Label(
            'Yes') >> create_csv_lines_59 >> create_collection_create_list_from_csv_60 >> list_activity_reference_folder >> get_activity_reference_filename
        get_activity_reference_filename >> download_64 >> load_csv_create_list_from_csv_65 >> create_collection_create_list_from_csv_65
        create_collection_create_list_from_csv_65 >> query_list_get_delta_66 >> query_list_get_unchanged_records_67 >> log_unchanged_activity_records
        log_unchanged_activity_records >> query_list_select_distinctemployeesfromthepayrateandactivitylist_71
        query_list_select_distinctemployeesfromthepayrateandactivitylist_71 >> trigger_dag_activity_and_pay_rate_assignmentsasync_75
        trigger_dag_activity_and_pay_rate_assignmentsasync_75 >> wait_for_completion_activity_and_pay_rate_assignmentsasync_75
        if_query_list_paycoderecords_16_rows_greater_than_0_58 >> rail.Label('No') >> search_user_import_logs
        wait_for_completion_activity_and_pay_rate_assignmentsasync_75 >> search_user_import_logs >> if_userimport_logs_present
        if_userimport_logs_present >> rail.Label('Yes') >> compose_csv_userimport_logs >> generate_download_link >> search_activity_payrate_logs
        if_userimport_logs_present >> rail.Label('No') >> search_activity_payrate_logs >> if_activity_payrate_logs_present
        if_activity_payrate_logs_present >> rail.Label(
            'Yes') >> compose_csv_activity_payrate_logs >> generate_downloadlink >> check_if_errors_or_exceptions_present
        if_activity_payrate_logs_present >> rail.Label(
            'No') >> check_if_errors_or_exceptions_present >> get_subject_and_body >> send_success_mail >> rename_move_inputfileto_archives_79
        rename_move_inputfileto_archives_79 >> rename_moveexistingreferencefileto_archives_80 >> upload_uploadnewreferencefile_81
        upload_uploadnewreferencefile_81 >> if_query_list_get_delta_66_rows_greater_than_0_82
        if_query_list_get_delta_66_rows_greater_than_0_82 >> rail.Label(
            'Yes') >> rename_moveactivityexistingreferencefileto_archives_83 >> upload_uploadnew_activityreferencefile_84 >> log_to_sumo
        if_query_list_get_delta_66_rows_greater_than_0_82 >> rail.Label('No') >> log_to_sumo
        if_file_name_ends_with_csv_2 >> rail.Label('No') >> rename_move_inputfileto_archives_86 >> send_mail_87 >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
