
from datetime import timedelta, datetime
from pendulum import datetime as dt
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'broadridge_project_main_version2_0_master_{config.instance}',
        description=f'Broadridge_project_main_version2_0_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=dt(2023, 1, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_todaysdate'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_todaysdate',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_todaysdate = rail.PythonOperator(
            task_id='log_todaysdate',
            python_callable=lambda: datetime.now().strftime("%m/%d/%Y")
        )

        broadridge_lookup_table = rail.CreateLogOperator(
            task_id='broadridge_lookup_table'
        )

        list_dir = rail.SFTPListFilesOperator(
            task_id='list_dir',
            paths=[config.input_filepath],
        )

        if_first_name_blank_5 = rail.IfOperator(
            task_id='if_first_name_blank_5',
            test=lambda: not bool(rail.result('list_dir')[config.input_filepath][0]['name'] if rail.result(
                'list_dir') and rail.result(
                'list_dir')[config.input_filepath][0] else null),
            yes_task="log_to_sumo",
            no_task="if_first_name_present",
        )

        if_first_name_present = rail.IfOperator(
            task_id='if_first_name_present',
            test=lambda: bool(rail.result('list_dir')[config.input_filepath][0]['name'] if rail.result(
                'list_dir') and rail.result(
                'list_dir')[config.input_filepath][0] else null),
            yes_task="log_filename",
            no_task="log_to_sumo",
        )

        log_filename = rail.PythonOperator(
            task_id='log_filename',
            python_callable=lambda: config.input_filepath + '/' +
            rail.result('list_dir')[config.input_filepath][0]['name']
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{result('log_filename')}}"
        )

        parse_csv_data = rail.LoadCSVFileOperator(
            task_id='parse_csv_data',
            document="{{ result('download_file') }}",
        )

        def get_data():
            result = rail.read_artifact(rail.result('parse_csv_data'))
            return result

        load_file = rail.PythonOperator(
            task_id='load_file',
            python_callable=get_data
        )

        load_csv_create_list_from_csv = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv",
            document="{{result('download_file')}}"
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('load_csv_create_list_from_csv') }}",
            name="inputfile",
            columns={
                'Project Name': 'projectname',
                'Project Code': 'projectcode',
                'Start Date': 'startdate',
                'End Date': 'enddate',
                'Project Manager': 'projectmanager',
                'Client Code': 'clientcode',
                'Task Name': 'taskname',
                'Task Team Assignment': 'taskteam',
                'Task Start Date': 'taskstartdate',
                'Task End Date': 'taskenddate',
                'TaskOutlinelevel': 'taskoutlinelevel',
                'TaskOutlineNumber': 'taskoutlinenumber',
                'Metis_ProjectUID': 'metisprojectuid',
                'Metis_TaskUID': 'metistaskuid'
            }
        )

        query_list_csv = rail.QueryCollectionOperator(
            task_id='query_list_csv',
            query="""SELECT DISTINCT  inputfile.projectname, inputfile.metisprojectuid FROM  inputfile""",
        )

        if_query_list_csv_less_than_1 = rail.IfOperator(
            task_id='if_query_list_csv_less_than_1',
            test='''{{ result('query_list_csv','length') < 1 }}''',
            yes_task="send_no_data_mail",
            no_task="get_allprojectcustomfields",
        )

        send_no_data_mail = rail.EmailOperator(
            task_id='send_no_data_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }}: Project Task import completed on {{ current_time() }} ''',
            html_content='templates/emails/no_data_mail.html'
        )

        get_allprojectcustomfields = rail.RepliconServiceOperator(
            task_id='get_allprojectcustomfields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:project"
            }
        )

        log_urifor_project_codecustomfield_15 = rail.PythonOperator(
            task_id='log_urifor_project_codecustomfield_15',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_allprojectcustomfields'), 'displayText', "Project_Code", 'uri', null) if rail.result('get_allprojectcustomfields')[0]['displayText'] else null
        )

        log_urifor_metis_project_u_i_d_16 = rail.PythonOperator(
            task_id='log_urifor_metis_project_u_i_d_16',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_allprojectcustomfields'), 'displayText', "Metis_ProjectUID", 'uri', null) if rail.result('get_allprojectcustomfields')[0]['displayText'] else null
        )

        process_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_child',
            retries=0,
            items="{{result('query_list_csv')}}",
            trigger_dag_id=f'broadridge_project_main_version2_0_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "batch_items": item,
                "customfield": rail.result('log_urifor_project_codecustomfield_15'),
                "metis_uid": rail.result('log_urifor_metis_project_u_i_d_16'),
                "input_file": rail.result('load_file'),
                "jobid": rail.render_template("{{ dag_run_ecid() }}"),
                "load_csv": rail.result('load_csv_create_list_from_csv'),
                "lookup_table": rail.result('broadridge_lookup_table'),
                "file_list": rail.result('list_dir')[config.input_filepath],
                "file_path": config.input_filepath + '/' +
                rail.result('list_dir')[config.input_filepath][0]['name'],
                "file_data": rail.result('parse_csv_data')
            }
        )

        wait_for_process_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_child") }}'
        )

        rename_to_archive = rail.SFTPMoveFileOperator(
            task_id='rename_to_archive',
            new_filename=config.archive_filepath +
            "{{dag_run_ecid() | replace(':', '-')}}" +
            "{{result('log_filename') | file_name}}",
            existing_filename=config.input_filepath
        )

        search_entries_in_broadridge_lookup_table = rail.FilterLogEntriesOperator(
            task_id='search_entries_in_broadridge_lookup_table',
            log="{{ result('broadridge_lookup_table') }}",
            properties={
                'jobid': "{{dag_run_ecid()}}",
            }
        )

        get_data = rail.PythonOperator(
            task_id='get_data',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv_data'))
        )

        get_entries_data = rail.PythonOperator(
            task_id='get_entries_data',
            python_callable=lambda: rail.load_all_records(rail.result(
                'search_entries_in_broadridge_lookup_table')) if rail.result(
                'search_entries_in_broadridge_lookup_table') else null
        )

        def get_details(projectname, taskname):
            data = rail.result('get_entries_data') if rail.result(
                'get_entries_data') else ''
            for d in data:
                if d['properties']['projectname'] == projectname and d['properties']['taskname'] == taskname:
                    return d['properties']['failure/reason']
            return ''

        def get_status(projectname, taskname):
            data = rail.result('get_entries_data') if rail.result(
                'get_entries_data') else ''
            for d in data:
                if d['properties']['projectname'] == projectname and d['properties']['taskname'] == taskname:
                    return d['properties']['status']
            return ''

        def get_jobid(projectname, taskname):
            data = rail.result('get_entries_data') if rail.result(
                'get_entries_data') else ''
            for d in data:
                if d['properties']['projectname'] == projectname and d['properties']['taskname'] == taskname:
                    return d['properties']['jobid']
            return ''

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            log="{{result('broadridge_lookup_table')}}",
            severity='Failed'
        )

        if_entry_is_present = rail.IfOperator(
            task_id='if_entry_is_present',
            test='''{{ result('get_logged_errors','length') > 0 }}''',
            yes_task="send_error_mail",
            no_task="send_success_mail",
        )

        create_csv_data = rail.WriteCSVFileOperator(
            task_id='create_csv_data',
            source="{{ result('get_data') | to_json}}",
            header=['Project Name',
                    'Project Code',
                    'Start Date',
                    'End Date',
                    'Project Manager',
                    'Client Code',
                    'Task Name',
                    'Task Team Assignment',
                    'Task Start Date',
                    'Task End Date',
                    'TaskOutlinelevel',
                    'TaskOutlineNumber',
                    'Metis_ProjectUID',
                    'Metis_TaskUID',
                    'Jobid',
                    'Status',
                    'details'],
            row=lambda item: [
                item['Project Name'],
                item['Project Code'],
                item['Start Date'],
                item['End Date'],
                item['Project Manager'],
                item['Client Code'],
                item['Task Name'],
                item['Task Team Assignment'],
                item['Task Start Date'],
                item['Task End Date'],
                item['TaskOutlinelevel'],
                item['TaskOutlineNumber'],
                item['Metis_ProjectUID'],
                item['Metis_TaskUID'],
                get_jobid(item['Project Name'], item['Task Name']) + "|" +
                get_status(item['Project Name'], item['Task Name']),
                get_status(item['Project Name'], item['Task Name']),
                get_details(item['Project Name'], item['Task Name']),

            ],
        )

        generate_downloadlink = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_downloadlink',
            artifact_name="{{ result('create_csv_data')}}",
            output_file_name='logs_{{ current_time() }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_success_mail = rail.EmailOperator(
            task_id='send_success_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''Broadridge | Project import  - Completed Successfully on  {{ current_time() }} ''',
            html_content="templates/emails/success_mail.html"
        )

        send_error_mail = rail.EmailOperator(
            task_id='send_error_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''Broadridge | Project import  - Completed with errors on {{ current_time() }} ''',
            html_content="templates/emails/mail_with_error.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> log_todaysdate
        log_todaysdate >> broadridge_lookup_table >> list_dir >> if_first_name_blank_5
        if_first_name_blank_5 >> rail.Label(
            'Yes') >> log_to_sumo
        if_first_name_blank_5 >> rail.Label('No') >> if_first_name_present
        if_first_name_present >> rail.Label(
            'Yes') >> log_filename >> download_file >> parse_csv_data >> load_file
        load_file >> load_csv_create_list_from_csv >> create_collection_from_csv
        create_collection_from_csv >> query_list_csv >> if_query_list_csv_less_than_1
        if_query_list_csv_less_than_1 >> rail.Label(
            'Yes') >> send_no_data_mail >> rename_to_archive >> log_to_sumo
        if_query_list_csv_less_than_1 >> rail.Label(
            'No') >> get_allprojectcustomfields
        get_allprojectcustomfields >> log_urifor_project_codecustomfield_15 >> log_urifor_metis_project_u_i_d_16
        log_urifor_metis_project_u_i_d_16 >> process_child >> wait_for_process_child
        wait_for_process_child >> search_entries_in_broadridge_lookup_table
        search_entries_in_broadridge_lookup_table >> get_data >> get_entries_data >> create_csv_data >> generate_downloadlink
        generate_downloadlink >> get_logged_errors >> if_entry_is_present
        if_entry_is_present >> rail.Label(
            'Yes') >> send_error_mail >> rename_to_archive
        if_entry_is_present >> rail.Label(
            'No') >> send_success_mail >> rename_to_archive >> log_to_sumo

        if_first_name_present >> rail.Label(
            'No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
