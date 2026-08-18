
from datetime import timedelta
from zaloragroup.zendesk_task_import.tasks.send_logs import get_send_logs
import pendulum
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'zaloragroup_create_new_task_in_replicon_master_{config.instance}',
        description=f'ZaloraGroup New tickets in Zendesk will create new Task in Replicon {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval = timedelta(seconds=config.schedule_interval),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10),
        )

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=lambda: {
                "dag_run_start_time": pendulum.now().strftime("%m_%d_%Y_T%H_%M_%S")
            }
        )

        is_csv_file = rail.IfOperator(
            task_id='is_csv_file',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Task import skipped on {{ current_time("%m/%d/%Y") }}',
            html_content='templates/emails/bad_file_format.html'

        )

        archive_invalid_file = rail.SFTPMoveFileOperator(
            task_id='archive_invalid_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/Skipped_{{ result('get_logging_details').dag_run_start_time }}_{{ result('new_file_sensor') | file_name }}"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_input_file',
            no_task='delete_this_dagrun',
        )

        archive_input_file = rail.SFTPMoveFileOperator(
            task_id='archive_input_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ result('new_file_sensor') | file_base }}_{{ result('get_logging_details').dag_run_start_time }}.csv"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_task_data = rail.LoadCSVFileOperator(
            task_id='load_task_data',
            document="{{ result('download_file') }}"
        )

        create_ticket_data_collection = rail.CreateCollectionOperator(
            task_id='create_ticket_data_collection',
            source="{{ result('load_task_data') }}",
            columns={
                'Project Name': 'projectname',
                'Ticket Number': 'ticketnumber',
                'Ticket Description': 'ticketdescription',
                'Task Description': 'taskdescription',
                'Task Start Date': 'taskstartdate',
                'Task End Date': 'taskenddate',
                'Is Time Entry Allowed': 'istimeentryallowed',
                'Users': 'users'
            },
            name="input_ticket_data"
        )

        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{ result('create_ticket_data_collection', 'length') > 0 }}",
            yes_task='get_report_details',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Task import completed on {{ current_time("%m/%d/%Y") }}',
            html_content="templates/emails/blank_payload.html"
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.task_import_report_name,
        )

        run_my_report_entry, run_my_report_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('get_report_details').uri}}"
                    }
                ]
            },
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test='{{result("run_report.get_report_result").reportGenerationResults[0].error | is_truthy}}',
            yes_task="fail_report_generation",
            no_task="report_has_data"
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="{{result('run_report.get_report_result').reportGenerationResults[0].error}}"
        )

        report_has_data = rail.IfOperator(
            task_id = "report_has_data",
            test= "{{ result('run_report.get_report_result','has_data')}}",
            yes_task='load_task_report_data'
        )

        load_task_report_data = rail.LoadCSVFileOperator(
            task_id='load_task_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        task_report_data_collection = rail.CreateCollectionOperator(
            task_id='task_report_data_collection',
            source="{{ result('load_task_report_data') }}",
            name='taskdetails'
        )

        get_valid_tickets = rail.QueryCollectionOperator(
            task_id='get_valid_tickets',
            query="SELECT * FROM input_ticket_data WHERE NULLIF(ticketdescription, '') IS NOT NULL",
            name='valid_tickets'
        )

        get_tasks_to_create = rail.QueryCollectionOperator(
            task_id='get_tasks_to_create',
            query='''SELECT * FROM valid_tickets WHERE ticketnumber NOT IN (SELECT DISTINCT Task_Code FROM taskdetails)
                AND ticketdescription NOT IN (SELECT DISTINCT Task_Name FROM taskdetails)'''
        )

        get_existed_task_codes = rail.QueryCollectionOperator(
            task_id='get_existed_task_codes',
            query='''SELECT * FROM valid_tickets WHERE ticketnumber IN (SELECT DISTINCT Task_Code FROM taskdetails)''',
            name='existedtaskcodesdata'
        )

        log_task_code_exist = rail.WriteLogOperator(
            task_id='log_task_code_exist',
            items='{{ result("get_existed_task_codes") }}',
            severity='Skipped',
            message='Task Code already exists',
            properties=lambda item: {
                "Projectname": item["projectname"],
                "Ticketnumber": item["ticketnumber"],
                "Ticketdescription": item["ticketdescription"],
                "Status": 'Skipped',
                "Reason": 'Task Code already exists'
            }
        )

        get_existed_task_name = rail.QueryCollectionOperator(
            task_id='get_existed_task_name',
            query='''SELECT * FROM valid_tickets WHERE ticketnumber NOT IN (SELECT DISTINCT ticketnumber FROM existedtaskcodesdata)
                        AND ticketdescription IN (SELECT DISTINCT Task_Name FROM taskdetails)'''
        )

        log_task_name_exist = rail.WriteLogOperator(
            task_id='log_task_name_exist',
            items='{{ result("get_existed_task_name") }}',
            severity='Skipped',
            message='Task Code already exists',
            properties=lambda item: {
                "Projectname": item["projectname"],
                "Ticketnumber": item["ticketnumber"],
                "Ticketdescription": item["ticketdescription"],
                "Status": 'Skipped',
                "Reason": 'Task Name already exists'
            }
        )

        get_first_project_record = rail.QueryCollectionOperator(
            task_id='get_first_project_record',
            query='''SELECT Project_Name, projecturi FROM taskdetails WHERE NULLIF(Project_Name, '')
                    IS NOT NULL AND NULLIF(projecturi, '') IS NOT NULL LIMIT 1'''
        )

        load_project_details = rail.PythonOperator(
            task_id='load_project_details',
            python_callable=lambda: rail.load_all_records(rail.result("get_first_project_record"))[0]
        )

        create_new_tasks = rail.TriggerDagRunForEachItemOperator(
            task_id='create_new_tasks',
            items='{{ result("get_tasks_to_create") }}',
            trigger_dag_id=f'zaloragroup_create_new_task_in_replicon_child_{config.instance}',
            conf=lambda item: {
                "parenturi": rail.result("load_project_details")["projecturi"],
                "projectname": rail.result("load_project_details")["Project_Name"],
                "taskname": item["ticketdescription"],
                "taskcode": item["ticketnumber"],
                "taskdescription": item["taskdescription"],
                "ticketdescription": item["ticketdescription"]
            }
        )

        wait_for_create_new_tasks = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_new_tasks',
            dag_runs='{{ result("create_new_tasks") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        send_logs_enter, send_logs_end = get_send_logs(config)

        new_file_sensor >> get_logging_details >> is_csv_file
        is_csv_file >> rail.Label("Yes") >> download_file >> was_new_file_found
        was_new_file_found >> rail.Label("Yes") >> archive_input_file
        download_file >> load_task_data >> create_ticket_data_collection \
            >> has_any_records
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        has_any_records >> rail.Label("Yes") >> get_report_details >> run_my_report_entry
        run_my_report_exit >> is_report_failed
        is_report_failed >> rail.Label("Yes") >> fail_report_generation
        is_report_failed >> rail.Label("No") >> report_has_data
        report_has_data >> rail.Label("Yes") >> load_task_report_data >> task_report_data_collection \
            >> get_valid_tickets >> get_tasks_to_create >> get_existed_task_codes >> log_task_code_exist \
                >> get_existed_task_name >> log_task_name_exist >> get_first_project_record >> load_project_details \
                    >> create_new_tasks >> wait_for_create_new_tasks >> send_logs_enter
        send_logs_end
        has_any_records >> rail.Label("No") >> send_blank_payload_email
        is_csv_file >> rail.Label("No") >> send_bad_file_format_email >> archive_invalid_file

    return dag

rail.for_each_instance(create_dag)
