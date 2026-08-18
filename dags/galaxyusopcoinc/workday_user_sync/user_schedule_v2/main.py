from datetime import timedelta
import os
import rail
from galaxyusopcoinc.workday_user_sync.user_schedule_v2.utils import request_payload
from galaxyusopcoinc.workday_user_sync.user_schedule_v2.task.run_report_task import run_schedule_base_report
from galaxyusopcoinc.workday_user_sync.user_schedule_v2.utils.python_callable_method import get_work_duration_for_validation

# pylint: disable=too-many-statements

def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.main_dag,
        description=f'VialtoPartners_User Schedule Master V1.0 - SFTP {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon User Schedule Sync - Incorrect Format - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email/bad_file_format.html"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        has_decrypted_file = rail.IfOperator(
            task_id='has_decrypted_file',
            test=lambda: rail.result('decrypt_file'),
            yes_task='has_file_content',
            no_task='fail_decryption_file'
        )

        fail_decryption_file = rail.FailOperator(
            task_id="fail_decryption_file",
            message="File Decryption Failed",
        )

        def do_has_file_content():
            with rail.existing_artifact(rail.result('decrypt_file')) as artifact:
                return os.path.getsize(artifact.local_filename) > 0
        
        has_file_content = rail.IfOperator(
            task_id='has_file_content',
            test=do_has_file_content,
            yes_task='load_user_schedule_data',
            no_task='send_blank_payload_email'
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            # yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            # trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_user_schedule_data = rail.LoadCSVFileOperator(
            task_id='load_user_schedule_data',
            document="{{ result('decrypt_file') }}",
            encoding="utf-8-sig",
            delimiter="|"
        )

        create_user_schedule_data_collection = rail.CreateCollectionOperator(
            task_id='create_user_schedule_data_collection',
            source="{{ result('load_user_schedule_data') }}",
            name="userscheduledata",
        )

        has_user_schedule_data = rail.IfOperator(
            task_id='has_user_schedule_data',
            test="{{ result('create_user_schedule_data_collection','length') > 0 }}",
            yes_task='query_user_schedule_data',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon User Schedule Sync - Blank File - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email/blank_payload.html"
        )

        query_user_schedule_data = rail.QueryCollectionOperator(
            task_id='query_user_schedule_data',
            query="""SELECT * FROM userscheduledata
                    WHERE  NULLIF(EmployeeId, '') IS NOT NULL""",
            name="queryuserscheduledata"
        )


        add_replicon_schedule_name_data = rail.QueryCollectionOperator(
            task_id='add_replicon_schedule_name_data',
            query="""SELECT *, (e.MondayHours || "|" || e.TuesdayHours || "|" || e.WednesdayHours || "|" || e.ThursdayHours || "|" || \
                e.FridayHours || "|" || e.SaturdayHours || "|" || e.SundayHours) AS replicon_schedule_name FROM queryuserscheduledata e""",
            name="addrepliconschedulenamedata"
        )

        run_schedule_base_report_task_start, run_schedule_base_report_task_end = run_schedule_base_report(
            group_id = "run_schedule_base_report",
            config= config
        )

        query_users_not_available_in_replicon = rail.QueryCollectionOperator(
            task_id = "query_users_not_available_in_replicon",
            query="""SELECT * FROM addrepliconschedulenamedata
                     WHERE EmployeeId NOT IN (SELECT DISTINCT EmployeeId FROM schedule_base_report_data)"""
        )

        log_user_not_found = rail.WriteLogOperator(
            task_id = "log_user_not_found",
            severity= "Skipped",
            items= "{{ result('query_users_not_available_in_replicon')}}",
            message="User Disabled/Not available in Replicon",
            properties={
                'schedulename': "{{ item.replicon_schedule_name}}",
                'employeeid': "{{ item.EmployeeId}}",
                'status': 'Skipped',
                'message': "User Disabled/Not available in Replicon"
            }
        )

        query_users_available_in_replicon = rail.QueryCollectionOperator(
            task_id = "query_users_available_in_replicon",
            name="users_available_in_replicon",
            query="""SELECT * FROM addrepliconschedulenamedata
                     WHERE EmployeeId IN (SELECT DISTINCT EmployeeId FROM schedule_base_report_data)"""
        )
        query_users_to_process_from_report = rail.QueryCollectionOperator(
            task_id = "query_users_to_process_from_report",
            query="""SELECT * FROM schedule_base_report_data
                     WHERE EmployeeId IN (SELECT DISTINCT EmployeeId FROM users_available_in_replicon)""",
            name= "data_to_process"
        )

        query_unchanged_records = rail.QueryCollectionOperator(
            task_id = "query_unchanged_records",
            query="""SELECT * FROM users_available_in_replicon as feed_data
                     WHERE feed_data.replicon_schedule_name == (
                     SELECT report_data.current_schedule_name FROM data_to_process as report_data
                     WHERE report_data.EmployeeID == feed_data.EmployeeID)"""
        )

        log_no_change_in_records = rail.WriteLogOperator(
            task_id = "log_no_change_in_records",
            severity= "Skipped",
            items= "{{ result('query_unchanged_records')}}",
            message="No change in user's schedule",
            properties={
                'schedulename': "{{ item.replicon_schedule_name}}",
                'employeeid': "{{ item.EmployeeId}}",
                'status': 'Skipped',
                'message': "No change in user's schedule"
            }
        )

        query_changed_records = rail.QueryCollectionOperator(
            task_id = "query_changed_records",
            query="""SELECT * FROM users_available_in_replicon as feed_data
                     WHERE feed_data.replicon_schedule_name != (
                     SELECT report_data.current_schedule_name FROM data_to_process as report_data
                     WHERE report_data.EmployeeID == feed_data.EmployeeID)""",
            name="final_data_to_process"
        )

        has_any_changed_records = rail.IfOperator(
            task_id = "has_any_changed_records",
            test="{{ result('query_changed_records', 'length') > 0}}",
            yes_task= "query_distinct_schedule",
            no_task="generate_output_log"
        )

        query_distinct_schedule = rail.QueryCollectionOperator(
            task_id='query_distinct_schedule',
            query="SELECT DISTINCT replicon_schedule_name FROM final_data_to_process WHERE replicon_schedule_name IS NOT NULL"
        )

        get_all_office_schedule = rail.RepliconServiceOperator(
            task_id='get_all_office_schedule',
            endpoint='/services/OfficeScheduleService1.svc/GetAllOfficeSchedules',
        )

        def get_required_schedule_details_callable():
            feed_file_schedules = rail.load_all_records(rail.result(query_distinct_schedule.task_id))
            replicon_schedules = rail.result(get_all_office_schedule.task_id)
            # Removing `None` for Schedule where it is not found in Replicon
            return list(filter(None,
                                map(lambda item: rail.find_first_by_attr_and_get_attr(
                                        replicon_schedules, "displayText", item['replicon_schedule_name'])
            ,feed_file_schedules)))

        get_required_schedule_details = rail.PythonOperator(
            task_id = "get_required_schedule_details",
            python_callable=get_required_schedule_details_callable
        )

        create_office_schedule_collection = rail.CreateCollectionOperator(
            task_id="create_office_schedule_collection",
            name="replicon_office_schedule",
            source="{{ result('get_all_office_schedule') | to_json }}"
        )

        query_new_schedules = rail.QueryCollectionOperator(
            task_id='query_new_schedules',
            query='''SELECT * FROM query_distinct_schedule
                    WHERE replicon_schedule_name IS NOT NULL AND replicon_schedule_name NOT IN
                    (SELECT DISTINCT Displaytext FROM replicon_office_schedule)'''
        )

        process_new_schedules = rail.TriggerDagRunForEachItemOperator(
            task_id='process_new_schedules',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            items=lambda: rail.result('query_new_schedules'),
            trigger_dag_id=config.process_new_schedule_creation,
            conf={
                'scheduletype': '{{ item.replicon_schedule_name }}'
            }
        )

        wait_for_process_new_schedules = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_new_schedules',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_new_schedules") }}',
        )

        def get_pattern_for_schedule(schedule_name):
            return {
                    "startDayOfWeekUri": "urn:replicon:day-of-week:sunday",
                    "day1WorkDuration": get_work_duration_for_validation(schedule_name, 'sunday'),
                    "day2WorkDuration": get_work_duration_for_validation(schedule_name, 'monday'),
                    "day3WorkDuration": get_work_duration_for_validation(schedule_name, 'tuesday'),
                    "day4WorkDuration": get_work_duration_for_validation(schedule_name, 'wednesday'),
                    "day5WorkDuration": get_work_duration_for_validation(schedule_name, 'thursday'),
                    "day6WorkDuration": get_work_duration_for_validation(schedule_name, 'friday'),
                    "day7WorkDuration": get_work_duration_for_validation(schedule_name, 'saturday'),
                }

        # Logic for updating the schedule
        def get_all_schedule_details_for_update_data_handler(response, service_call_input):
            schedules_for_update = []
            response_details_for_schedules_for_update = []
            # Adding the response to XCOM
            current_response = rail.result("get_all_schedule_details_for_update")
            rail.set_result(key="response", val= response + (current_response if current_response else []))
            for item in service_call_input:
                # Generating the Pattern as per what Integration will create while creating a new schedule
                schedule_name = item['displayText']
                generate_pattern_by_name = get_pattern_for_schedule(schedule_name)

                # Getting the simplePattern Using the URI
                get_replicon_pattern = rail.find_first_by_attr_and_get_attr(
                                    response,'officeScheduleUri', item['uri'] ,"officeSchedule.simplePattern")

                # DICT comparing for what will Integration add as pattern and what it is present in Replicon
                if generate_pattern_by_name != get_replicon_pattern:
                    response_details_for_schedules_for_update.append({
                        schedule_name: {
                            "uri": item['uri'],
                            "replicon_pattern": get_replicon_pattern,
                            "pattern_generate_by_name": generate_pattern_by_name
                        }}
                    )
                    schedules_for_update.append(item)

            existing_response_details_for_schedules_for_update = rail.result("get_all_schedule_details_for_update", 'response_details_for_schedules_for_update')
            if not existing_response_details_for_schedules_for_update:
                existing_response_details_for_schedules_for_update = []
            rail.set_result(key="response_details_for_schedules_for_update", val=response_details_for_schedules_for_update + existing_response_details_for_schedules_for_update)
            return schedules_for_update

        get_all_schedule_details_for_update = rail.RepliconServiceCallForEachItemOperator(
            task_id = "get_all_schedule_details_for_update",
            endpoint="/services/OfficeScheduleService1.svc/BulkGetOfficeScheduleDetails",
            items=lambda: rail.result(get_required_schedule_details.task_id),
            batch_size= 100,
            data= lambda items: {
                "officeScheduleUris": list(map(lambda x: x['uri'], items))
            },
            flatten=True,
            # sending Input also so to get only those Schedule where the hours pattern is miss-match
            data_handler= lambda response, **context: get_all_schedule_details_for_update_data_handler(response, context['items'])
        )

        process_schedule_correction = rail.trigger_parallel_dagrun(
            task_id="process_schedule_correction",
            parallel_count=25,
            items=lambda: rail.result(get_all_schedule_details_for_update.task_id),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_user_correction_child_dag,
            conf={
                'file_name': "{{ result('new_file_sensor') | file_name}}",
                'scheduletype': '{{ item.displayText }}',
                'schedule_uri': '{{ item.uri }}'
            }
        )

        dummy_process_user_schedule_records = rail.EmptyOperator(
            task_id = "dummy_process_user_schedule_records"
        )

        process_each_user_schedule_records = rail.trigger_parallel_dagrun(
            task_id='process_each_user_schedule_records',
            parallel_count=25,
            items="{{ result('query_changed_records') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_user_schedule_child_dag,
            conf=request_payload.get_process_each_user_schedule_records_conf
        )

        generate_output_log = rail.EmptyOperator(task_id='generate_output_log')

        get_successful_logs = rail.FilterLogEntriesOperator(
            task_id='get_successful_logs',
            properties={'status': 'Success'}
        )

        get_errored_logs = rail.FilterLogEntriesOperator(
            task_id='get_errored_logs',
            properties={'status': 'Error'}
        )

        get_exception_logs = rail.FilterLogEntriesOperator(
            task_id='get_exception_logs',
            properties={'status': 'Exception'}
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                'Schedule',
                'Employee ID',
                'Status',
                'Details',
                'Job ID'],
            row=[
                '{{ item.properties.schedulename }}',
                '{{ item.properties.employeeid }}',
                '{{ item.properties.status }}',
                '{{ item.properties.message }}',
                '{{ item.ecid }}'],
            footer=[
                'Number of Records Processed Successfully: {{result("get_successful_logs", key="length")}}',
                'Number of Records with Error: {{ result("get_errored_logs", key="length") }}',
                'Number of Records with Exception: {{ result("get_exception_logs", key="length") }}',
                '',
                ''],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv')

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='''{{ get_company_key() + " | Replicon User Schedule Sync - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    Completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs", key="length") > 0 -%} \
                        Completed with exceptions  \
                    {%- else -%} \
                        Completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " " + current_time() }}''',
            html_content="templates/email/import_complete.html",
            params={
                'log_filepath': config.log_filepath
            }
        )

        new_file_sensor >> is_csv
        is_csv >> rail.Label("No") >> send_bad_file_format_email
        is_csv >> rail.Label("Yes") >> download_file
        download_file >> archive_file >> decrypt_file >> has_decrypted_file >> rail.Label("Yes") >> has_file_content
        has_decrypted_file >> rail.Label("No") >> fail_decryption_file

        has_file_content >> rail.Label(
             "Yes") >> load_user_schedule_data
        has_file_content >> rail.Label("No") >> send_blank_payload_email
        load_user_schedule_data >> create_user_schedule_data_collection >> has_user_schedule_data
        has_user_schedule_data >> rail.Label("No") >> send_blank_payload_email
        has_user_schedule_data >> rail.Label("Yes") >> query_user_schedule_data
        query_user_schedule_data >> add_replicon_schedule_name_data >> run_schedule_base_report_task_start
        run_schedule_base_report_task_end >> query_users_not_available_in_replicon \
            >> log_user_not_found >> query_users_available_in_replicon >> query_users_to_process_from_report >> query_unchanged_records\
                >> log_no_change_in_records >> query_changed_records >> has_any_changed_records >> rail.Label("Yes") >> query_distinct_schedule
        has_any_changed_records >> rail.Label("No") >> generate_output_log
        query_distinct_schedule >> get_all_office_schedule >> get_required_schedule_details >> create_office_schedule_collection
        create_office_schedule_collection >> query_new_schedules
        query_new_schedules >> process_new_schedules >> wait_for_process_new_schedules
        wait_for_process_new_schedules >> get_all_schedule_details_for_update >> process_schedule_correction\
            >> dummy_process_user_schedule_records >> process_each_user_schedule_records >> generate_output_log

        download_file >> was_new_file_found >> rail.Label("No") >> delete_this_dagrun
        generate_output_log >> [
            get_successful_logs, get_errored_logs, get_exception_logs] >> render_logs_csv
        render_logs_csv >> upload_log_to_sftp >> send_import_complete_email

        return dag


rail.for_each_instance(create_main_dag)