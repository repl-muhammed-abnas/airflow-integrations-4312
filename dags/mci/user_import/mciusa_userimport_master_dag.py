import hashlib
from datetime import timedelta, datetime
from rail.lib.ecid import get_dagrun_ecid
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'mci_user_import_master_{config.instance}',
        description=f'MCIUSA_UserImport_Master {config.instance}',
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

        archive_file=rail.SFTPMoveFileOperator(
            task_id='archive_file',
            new_filename=config.archive_filepath + "{{current_time('%d%m%YT%H%M%S')}}_{{ result('new_file_sensor') | file_name }}",
            existing_filename= "{{ result('new_file_sensor') }}",
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        if_filename_ends_with_csv=rail.IfOperator(
            task_id='if_filename_ends_with_csv',
            test="{{result('new_file_sensor') | ends_with('csv') }}",
            yes_task="parse_csv",
            no_task="send_mail_file_not_in_csv_format",
        )

        send_mail_file_not_in_csv_format=rail.EmailOperator(
            task_id='send_mail_file_not_in_csv_format',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject= "{{ get_company_key()}}" + " | Replicon user import for workday - Incorrect File Format - {{ current_time() }}",
            html_content='templates/file_not_csv_mail.html',
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            encoding='utf-8-sig',
            document="{{result('download_file')}}",
            delimiter=',',
        )

        compose_csv_input_data=rail.WriteCSVFileOperator(
            task_id='compose_csv_input_data',
            source="{{ result('parse_csv') }}",
            header=['workemail',
                    'legalfirstname',
                    'legallastname',
                    'employeecode',
                    'payclass',
                    'terminalgroup',
                    'department',
                    'replicondivision',
                    'employeestatus',
                    'hiredate',
                    'rehiredate',
                    'terminationdate',
                    'supervisorprimary',
                    'supervisorprimarycode',
                    'accrualleave',
                    'worklocation',
                    'md5'],
            row=lambda item:
            [
                item['Work_Email'],
                item['Legal_Firstname'],
                item['Legal_Lastname'],
                item['Employee_Code'],
                item['Pay_Class'],
                item['Terminal_Group'],
                item['Department'],
                item['RepliconDivision'],
                item['Employee_Status'],
                item['Hire_Date'],
                item['Rehire_Date'],
                item['Termination_Date'],
                item['Supervisor_Primary'],
                item['Supervisor_Primary_Code'],
                item['AccrualLevel'],
                item['Work_Location'],
                hashlib.md5((
                    str(item['Work_Email']) + "," + str(item['Legal_Firstname']) + "," + str(item['Legal_Lastname']) + "," + str(item['Employee_Code']) + "," +
                    str(item['Pay_Class']) + "," + str(item['Terminal_Group']) + "," + str(item['Department']) + "," + str(item['RepliconDivision']) + "," +
                    str(item['Employee_Status']) + "," + str(item['Hire_Date']) + "," + str(item['Rehire_Date']) + "," + str(item['Supervisor_Primary']) +
                    "," + str(item['Supervisor_Primary_Code']) + "," + str(item['AccrualLevel']) + "," + str(item['Work_Location'])
                ).encode('utf-8')).hexdigest()
],
        )

        create_rawdata_collection = rail.CreateCollectionOperator(
            task_id='create_rawdata_collection',
            source = "{{ result('compose_csv_input_data') }}",
            name = "rawdata",
            columns = {
                'workemail':'workemail', 
                'legalfirstname':'legalfirstname', 
                'legallastname':'legallastname', 
                'employeecode':'employeecode', 
                'payclass':'payclass', 
                'terminalgroup':'terminalgroup', 
                'department':'department', 
                'replicondivision':'replicondivision', 
                'employeestatus':'employeestatus', 
                'hiredate':'hiredate', 
                'rehiredate':'rehiredate', 
                'terminationdate':'terminationdate', 
                'supervisorprimary':'supervisorprimary', 
                'supervisorprimarycode':'supervisorprimarycode', 
                'accrualleave':'accrualleave', 
                'worklocation':'worklocation', 
                'md5':'md5'
            }
        )

        get_user_detail_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_detail_report_details',
            report_name=config.user_detail_report
        )

        run_user_detail_report = rail.run_report2(
            group_id='run_user_detail_report',
            target='artifact',
            report_params= lambda: {
                "reportParameters": [
                    {
                    "reportUri": rail.result('get_user_detail_report_details')['uri'],
                    "filterValues": [],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        parse_csv_from_report_payload = rail.LoadCSVFileOperator(
            task_id = 'parse_csv_from_report_payload',
            document="{{ (result('run_user_detail_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}",

        )

        download_reference_file=rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_filepath + "/user_reference.csv"
        )

        load_csv_from_reference_file = rail.LoadCSVFileOperator(
            task_id = 'load_csv_from_reference_file',
            document="{{result('download_reference_file')}}"
        )

        create_reference_collection = rail.CreateCollectionOperator(
            task_id='create_reference_collection',
            source = lambda: rail.result('load_csv_from_reference_file'),
            name = "reference",
            columns = {
                'workemail':'workemail', 
                'legalfirstname':'legalfirstname', 
                'legallastname':'legallastname', 
                'employeecode':'employeecode', 
                'payclass':'payclass', 
                'terminalgroup':'terminalgroup', 
                'department':'department', 
                'replicondivision':'replicondivision', 
                'employeestatus':'employeestatus', 
                'hiredate':'hiredate', 
                'rehiredate':'rehiredate', 
                'terminationdate':'terminationdate', 
                'supervisorprimary':'supervisorprimary', 
                'supervisorprimarycode':'supervisorprimarycode', 
                'accrualleave':'accrualleave', 
                'worklocation':'worklocation', 
                'md5':'md5'
            }
        )

        query_delta_records=rail.QueryCollectionOperator(
            task_id='query_delta_records',
            query="""SELECT * FROM  rawdata WHERE  rawdata.md5 NOT IN (SELECT DISTINCT reference.md5 FROM  reference)""",
        )

        if_query_delta_records_has_data=rail.IfOperator(
            task_id='if_query_delta_records_has_data',
            test="{{ result('query_delta_records','length') > 0 }}",
            yes_task="get_all_time_off_types",
            no_task="finish",
        )

        get_all_time_off_types=rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            target='artifact'
        )

        def getdepartments(response):
            return [ {
                "code": row['cells'][0].get('textValue'),
                "uri": row['cells'][1].get('uri')
            } for row in response['rows']]

        get_department_group_data=rail.RepliconServiceOperator(
            task_id='get_department_group_data',
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
            "page": "1",
            "pagesize": "1000",
            "columnUris": [
                "urn:replicon:department-group-list-column:code",
                "urn:replicon:department-group-list-column:department-group"
            ],
            "sort": [],
            "filterExpression": null
            },
            data_handler=getdepartments
        )

        create_lookup_table= rail.CreateLogOperator(
            task_id = 'create_lookup_table',
        )

        create_report_data_collection = rail.CreateCollectionOperator(
            task_id = 'create_report_data_collection',
            source= "{{result('parse_csv_from_report_payload')}}",
            name= 'reportdata'

        )

        query_required_users = rail.QueryCollectionOperator(
            task_id = 'query_required_users',
            query="""SELECT * FROM  reportdata JOIN query_delta_records ON reportdata.Employee_ID = query_delta_records.employeecode
                    OR reportdata.Employee_ID = query_delta_records.supervisorprimarycode"""
        )

        load_users_present = rail.PythonOperator(
            task_id = 'load_users_present',
            python_callable=lambda: rail.load_all_records(rail.result('query_required_users'))
        )

        for_each_delta_record=rail.ForEachOperator(
            task_id='for_each_delta_record',
            items="{{ result('query_delta_records') }}",
            start_task = 'if_employeecode_present',
            end_task = 'for_each_delta_record_end'
        )

        if_employeecode_present=rail.IfOperator(
            task_id='if_employeecode_present',
            test= lambda: bool(rail.find_first_by_attr_and_get_attr(
                            rail.result('load_users_present'),'Employee_ID',
                            rail.result('for_each_delta_record')['employeecode'],'Employee_ID','')),
            yes_task="trigger_child_update_user",
            no_task="trigger_child_add_user",
        )

        def get_update_user_payload():
            record = rail.result('for_each_delta_record')
            hire_date_obj = datetime.strptime(record['hiredate'],'%m/%d/%Y') if record['hiredate'] else ''
            terminate_date_obj = datetime.strptime(
                record['terminationdate'],'%m/%d/%Y') if record['terminationdate'] != '00/00/0000' and record['terminationdate'] else ''
            report_data = rail.result('load_users_present')
            return {
            "workemail": record['workemail'],
            "legalfirstname": record['legalfirstname'],
            "legallastname": record['legallastname'],
            "employeecode": record['employeecode'],
            "payclass": record['payclass'],
            "terminalgroup": record['terminalgroup'],
            "department": record['department'],
            "replicondivision": record['replicondivision'],
            "employeestatus": record['employeestatus'],
            "hiredate": record['hiredate'],
            "rehiredate": record['rehiredate'],
            "terminationdate": record['terminationdate'],
            "supervisorprimary": record['supervisorprimary'],
            "supervisorprimarycode": record['supervisorprimarycode'],
            "accrualleave": record['accrualleave'],
            "worklocation": record['worklocation'],
            "useruri": rail.find_first_by_attr_and_get_attr(report_data,'Employee_ID',record['employeecode'],'useruri',''),
            "supervisoruri": rail.find_first_by_attr_and_get_attr(report_data,'Employee_ID',record['supervisorprimarycode'],'useruri',''),
            "departmenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_department_group_data'),'code',record['department'],'uri',null),
            "startdate": {
                "day": hire_date_obj.strftime('%d'),
                "month": hire_date_obj.strftime('%m'),
                "year": hire_date_obj.strftime('%Y')
            } if hire_date_obj else {
                "day": null,
                "month": null,
                "year": null
            },
            "enddate": {
                "day": terminate_date_obj.strftime('%d'),
                "month": terminate_date_obj.strftime('%m'),
                "year": terminate_date_obj.strftime('%Y')
            } if terminate_date_obj and not(record['terminationdate'] == "00/00/0000") else {
                "day": null,
                "month": null,
                "year": null
            },
            "lookuptable": rail.result('create_lookup_table'),
            'callerjobid': get_dagrun_ecid(rail.get_current_context()['dag_run']),
        }

        trigger_child_update_user=rail.TriggerDagRunOperator(
            task_id='trigger_child_update_user',
            retries = 0,
            trigger_dag_id= f'mci_user_import_update_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf = get_update_user_payload
        )

        def get_add_user_payload():
            record = rail.result('for_each_delta_record')
            report_data = rail.result('load_users_present')
            supervisoruri = rail.find_first_by_attr_and_get_attr(report_data,'Employee_ID',record['supervisorprimarycode'],'useruri','')
            departmenturi= rail.find_first_by_attr_and_get_attr(rail.result('get_department_group_data'),'code',record['department'],'uri',null)
            terminate_date_obj = datetime.strptime(
              record['terminationdate'],'%m/%d/%Y') if record['terminationdate'] and record['terminationdate'] != '00/00/0000' else ''
            hire_date_obj = datetime.strptime(record['hiredate'],'%m/%d/%Y') if record['hiredate'] else ''
            timesheettemplate = list(filter(lambda item: item['field'] == 'timesheet template' and
                                            item['identifier'] == record['terminalgroup'],config.mapper)) if record['terminalgroup'] else null
            schedule = list(filter(lambda item: item['field'] == 'schedule' and
                                            item['identifier'] == (record['accrualleave'].split(" ")[0]),config.mapper)) if record['accrualleave'] else null
            return {
            "workemail": record['workemail'],
            "legalfirstname": record['legalfirstname'],
            "legallastname": record['legallastname'],
            "employeecode": record['employeecode'],
            "payclass": record['payclass'],
            "terminalgroup": record['terminalgroup'],
            "department": record['department'],
            "replicondivision": record['replicondivision'],
            "employeestatus": record['employeestatus'],
            "hiredate": record['hiredate'],
            "rehiredate": record['rehiredate'],
            "terminationdate": record['terminationdate'],
            "supervisorprimary": record['supervisorprimary'],
            "supervisorprimarycode": record['supervisorprimarycode'],
            "accrualleave": record['accrualleave'],
            "worklocation": record['worklocation'],
            "supervisoruri": supervisoruri,
            "departmenturi": departmenturi,
            "timeoff": {
                "d": rail.result('get_all_time_off_types')
            },
            "mapper":{
                "Add": {
                    "firstname": record['legalfirstname'],
                    "lastname": record['legallastname'],
                    "displayname": record['legalfirstname'] + " " + record['legallastname'],
                    "email": record['workemail'],
                    "employeecode": record['employeecode'],
                    "loginname": record['workemail'],
                    "supervisorname": record['supervisorprimary'] if record['supervisorprimary'] else null,
                    "supervisoruri": supervisoruri if supervisoruri else null,
                    "department": departmenturi if departmenturi else null,
                    "location": record['worklocation'] if record['worklocation'] else null,
                    "timesheettemplate": timesheettemplate[0]['value'] if timesheettemplate else null,
                    "schedule": ( "8 hours/day; Mon-Fri"
                                          if record['accrualleave'] == "0 percent (ineligible)"
                                          else schedule[0]['value'] if schedule else "8 hours/day; Mon-Fri")
                                          if record['accrualleave'] else '8 hours/day; Mon-Fri',
                    "enddate": {
                      "day": terminate_date_obj.strftime('%d'),
                      "month": terminate_date_obj.strftime('%m'),
                      "year": terminate_date_obj.strftime('%Y')
                    } if terminate_date_obj and not(record['terminationdate'] == "00/00/0000") else {
                        "day": null,
                        "month": null,
                        "year": null
                    },
                    "startdate": {
                        "day": hire_date_obj.strftime('%d'),
                        "month": hire_date_obj.strftime('%m'),
                        "year": hire_date_obj.strftime('%Y')
                    } if hire_date_obj else {
                        "day": null,
                        "month": null,
                        "year": null
                    }
                }
            },
            "lookuptable": rail.result('create_lookup_table'),
            "callerjobid": get_dagrun_ecid(rail.get_current_context()['dag_run']),
        }

        trigger_child_add_user=rail.TriggerDagRunOperator(
            task_id='trigger_child_add_user',
            retries = 0,
            trigger_dag_id= f'mci_user_import_add_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf = get_add_user_payload
        )

        for_each_delta_record_end=rail.EmptyOperator(
            task_id='for_each_delta_record_end',
        )

        if_update_child_triggered = rail.IfOperator(
            task_id = 'if_update_child_triggered',
            test= "{{result('trigger_child_update_user') | is_truthy}}",
            yes_task='wait_for_update_child',
            no_task='if_add_child_triggered'
        )

        wait_for_update_child = rail.WaitForDagRunsSensor(
            task_id ='wait_for_update_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('trigger_child_update_user')}}"
        )

        if_add_child_triggered = rail.IfOperator(
            task_id = 'if_add_child_triggered',
            test="{{ result('trigger_child_add_user') | is_truthy}}",
            yes_task='wait_for_add_child',
            no_task='if_entries_present'
        )

        wait_for_add_child = rail.WaitForDagRunsSensor(
            task_id ='wait_for_add_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ result('trigger_child_add_user')}}"
        )

        if_entries_present=rail.IfOperator(
            task_id='if_entries_present',
            test="{{ result('create_lookup_table') | load_all_records() | length > 0}}",
            yes_task="compose_logs",
            no_task="upload_reference_file",
        )

        compose_logs=rail.WriteCSVFileOperator(
            task_id='compose_logs',
            source="{{ result('create_lookup_table') }}",
            header=['Job ID',
                    'User Name',
                    'Login Name',
                    'Action',
                    'Status',
                    'Details'],
            row=[
                    "{{ item.properties.jobid}}",
                    "{{ item.properties.username}}",
                    "{{ item.properties.loginname}}",
                    "{{ item.properties.action}}",
                    "{{ item.properties.status}}",
                    "{{ item.properties.details}}"
            ],
        )

        upload_logs=rail.SFTPUploadFileOperator(
            task_id='upload_logs',
            content="{{ result('compose_logs') }}",
            remote_filepath= config.log_filepath + "logs_{{ result('new_file_sensor') | file_name }}",
        )

        upload_reference_file=rail.SFTPUploadFileOperator(
            task_id='upload_reference_file',
            content='''{{ result('compose_csv_input_data') }}''',
            remote_filepath= config.reference_filepath + "/user_reference.csv",
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )


        new_file_sensor >> download_file >> rail.Label("Always") >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_file >> if_filename_ends_with_csv
        if_filename_ends_with_csv >> rail.Label('No')  >> send_mail_file_not_in_csv_format >> finish
        if_filename_ends_with_csv >> rail.Label('Yes') >> parse_csv >> compose_csv_input_data >> create_rawdata_collection >> get_user_detail_report_details
        get_user_detail_report_details >> run_user_detail_report >> parse_csv_from_report_payload >> download_reference_file >> load_csv_from_reference_file
        load_csv_from_reference_file >> create_reference_collection >> query_delta_records >> if_query_delta_records_has_data
        if_query_delta_records_has_data >> rail.Label('Yes')  >> get_all_time_off_types >> get_department_group_data >> create_lookup_table
        create_lookup_table >> create_report_data_collection >> query_required_users >> load_users_present >> for_each_delta_record >> if_employeecode_present
        if_employeecode_present >> rail.Label('Yes')  >> trigger_child_update_user >> for_each_delta_record_end
        if_employeecode_present >> rail.Label('No') >> trigger_child_add_user >> for_each_delta_record_end
        for_each_delta_record >> for_each_delta_record_end >> if_update_child_triggered >> rail.Label('Yes') >> wait_for_update_child >> if_add_child_triggered
        if_update_child_triggered >> rail.Label('No') >> if_add_child_triggered >> rail.Label('Yes') >> wait_for_add_child >> if_entries_present
        if_add_child_triggered >> rail.Label('No') >> if_entries_present
        if_entries_present >> rail.Label('Yes')  >> compose_logs >> upload_logs >> upload_reference_file
        if_entries_present >> rail.Label('No') >> upload_reference_file >> finish
        if_query_delta_records_has_data >> rail.Label('No') >> finish

    return dag

rail.for_each_instance(create_dag)
