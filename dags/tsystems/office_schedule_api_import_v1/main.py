from datetime import timedelta
import rail
from tsystems.office_schedule_api_import_v1.utils import custom_methods
from pendulum import now


null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'{config.company_key} office schedule api import Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        job_started_time = rail.PythonOperator(
            task_id='job_started_time',
            python_callable=lambda: now(config.timezone).strftime("%y-%m-%dT%H:%M:%S%z"),
        )

        is_data_available = rail.IfOperator(
            task_id='is_data_available',
            test=lambda dag_run: bool(dag_run.conf['payload']),
            yes_task="create_log",
            no_task="send_mail_blank_data"
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        create_schedule_list_collection = rail.CreateCollectionOperator(
            task_id='create_schedule_list_collection',
            source="{{ dag_run.conf['payload']['Schedule WorkTime'] | to_json }}",
            name='schedule_list',
            columns={
                "CID": "employee_id",
                "Valid From": "valid_from",
                "Schedule Name": "schedule_name"
            }
        )

        if_schedule_list_collection_has_records = rail.IfOperator(
            task_id='if_schedule_list_collection_has_records',
            test="{{ result('create_schedule_list_collection', 'length') > 0 }}",
            yes_task='query_invalid_records',
            no_task='send_mail_blank_data'
        )

        send_mail_blank_data = rail.EmailOperator(
            task_id='send_mail_blank_data',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Replicon Office Schedule Import - No records | {{current_time_in_specified_tz()}} ''',
            html_content='''templates/emails/no_records_in_payload_mail.html''',
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id='query_invalid_records',
            query='''SELECT *
                FROM schedule_list
                WHERE 
                NULLIF(employee_id, '') IS NULL
                OR NULLIF(valid_from, '') IS NULL
                OR NULLIF(schedule_name, '') IS NULL
                OR LENGTH(schedule_name) - LENGTH(REPLACE(schedule_name, ',', '')) != 1
                OR TRIM(SUBSTR(schedule_name, 1, INSTR(schedule_name, ',') - 1)) = ''
                OR TRIM(SUBSTR(schedule_name, INSTR(schedule_name, ',') + 1)) = ''
                '''
        )

        if_invalid_records = rail.IfOperator(
            task_id='if_invalid_records',
            test="{{ result('query_invalid_records', 'length') > 0 }}",
            yes_task='log_add_invalid_records',
            no_task='query_valid_records'
        )

        log_add_invalid_records = rail.WriteLogOperator(
            task_id='log_add_invalid_records',
            log="{{ result('create_log') }}",
            items="{{result('query_invalid_records')}}",
            message='One or more mandatory field is missing.',
            severity='Info',
            properties=lambda item: {
                "employee_id": item['employee_id'],
                "schedule_name": item['schedule_name'],
                "action": "Validation",
                "status": "Skipped",
                "details": custom_methods.get_missing_field_message(item)
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            query='''SELECT *
            FROM schedule_list
            WHERE 
            NULLIF(employee_id, '') IS NOT NULL
            AND NULLIF(valid_from, '') IS NOT NULL
            AND NULLIF(schedule_name, '') IS NOT NULL
            AND LENGTH(schedule_name) - LENGTH(REPLACE(schedule_name, ',', '')) = 1
            AND TRIM(SUBSTR(schedule_name, 1, INSTR(schedule_name, ',') - 1)) != ''
            AND TRIM(SUBSTR(schedule_name, INSTR(schedule_name, ',') + 1)) != ''
            '''
        )

        if_valid_records = rail.IfOperator(
            task_id='if_valid_records',
            test="{{ result('query_valid_records', 'length') > 0 }}",
            yes_task='get_all_office_schedules',
            no_task='format_logs'
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules',
            endpoint='/services/OfficeScheduleService1.svc/GetAllOfficeSchedules',
        )

        def get_create_new_schedules_not_inreplicon_by_name():
            valid_records = rail.load_all_records(rail.result('query_valid_records'))
            replicon_schedule_names = [schedule['displayText'] for schedule in rail.result('get_all_office_schedules')]
            new_schedules = []
            seen = set()
            for record in valid_records:
                schedule_name = record['schedule_name'].split(',')[0].strip()
                if  schedule_name not in replicon_schedule_names and schedule_name not in seen:
                    new_schedules.append({
                        "schedule_name": schedule_name,
                        "schedule_type": record['schedule_name'].split(',')[1].strip(),
                    })
                    seen.add(schedule_name)
            return new_schedules
        
        get_create_new_schedules = rail.PythonOperator(
            task_id='get_create_new_schedules',
            python_callable=get_create_new_schedules_not_inreplicon_by_name
        )

        trigger_create_new_schedules = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_create_new_schedules',
            retries=0,
            trigger_dag_id=config.schedule_add_dag_id,
            items="{{ result('get_create_new_schedules') | to_json }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'schedulename': "{{ item.schedule_name }}",
                'scheduletype': "{{ item.schedule_type }}"
            },
        )
        wait_for_create_new_schedules = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_new_schedules',
            dag_runs='{{ result("trigger_create_new_schedules") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_all_updated_office_schedules = rail.RepliconServiceOperator(
            task_id='get_all_updated_office_schedules',
            endpoint='/services/OfficeScheduleService1.svc/GetAllOfficeSchedules'
        )

        trigger_assign_schedule_to_employee = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_assign_schedule_to_employee',
            retries=0,
            trigger_dag_id=config.assign_schedule_dag_id,
            items="{{ result('query_valid_records') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'employee_id': item['employee_id'],
                'valid_from': item['valid_from'],
                'schedule_name': item['schedule_name'],
                'schedule_uri': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_updated_office_schedules'),
                    'displayText',
                    item['schedule_name'].split(',')[0].strip(),
                    'uri'
                )
            }
        )

        wait_for_assign_schedule_to_employee = rail.WaitForDagRunsSensor(
            task_id='wait_for_assign_schedule_to_employee',
            dag_runs='{{ result("trigger_assign_schedule_to_employee") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_assignment_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_assignment_logs',
            dag_runs='{{ result("trigger_assign_schedule_to_employee") }}',
            dagrun_task_id='create_assignment_log',
            execution_timeout=timedelta(
                hours=config.gather_assignment_timeout_hours),
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=custom_methods.do_format_logs,
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                "CID",
                "Schedule Name",
                "Action",
                "Status",
                "Details",
                "JobID | Run ID",
            ],
            row=[
                "{{ item.employee_id }}",
                "{{ item.schedule_name }}",
                "{{ item.action }}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.ecid }}",
            ]
        )

        get_email_details = rail.PythonOperator(
            task_id = "get_email_details",
            python_callable=lambda dag_run: custom_methods.get_email_details_callable(dag_run, config.timezone)
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name="{{ result('get_email_details').log_file_name }}",
            expires_in_seconds=7*24*60*60,
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/'+"{{ result('get_email_details').log_file_name }}",
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon Office Schedule Import is " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/completion_mail.html"
        )

        job_started_time >> is_data_available
        is_data_available >> rail.Label('Yes') >> create_log
        is_data_available >> rail.Label('No') >> send_mail_blank_data
        create_log >> create_schedule_list_collection >> if_schedule_list_collection_has_records
        if_schedule_list_collection_has_records >> rail.Label('Yes') >> query_invalid_records >> if_invalid_records
        if_schedule_list_collection_has_records >> rail.Label('No') >> send_mail_blank_data
        if_invalid_records >> rail.Label('Yes') >> log_add_invalid_records >> query_valid_records
        if_invalid_records >> rail.Label('No') >> query_valid_records >> if_valid_records
        if_valid_records >> rail.Label('Yes') >> get_all_office_schedules >> get_create_new_schedules >> trigger_create_new_schedules >>\
        wait_for_create_new_schedules >> get_all_updated_office_schedules >> trigger_assign_schedule_to_employee >> wait_for_assign_schedule_to_employee >> gather_assignment_logs >>\
        format_logs 
        if_valid_records >> rail.Label('No') >> format_logs
        format_logs >> render_logs_csv >> get_email_details >> generate_download_link >> upload_log_to_sftp >> send_import_complete_email

    return dag


rail.for_each_instance(create_dag)

        