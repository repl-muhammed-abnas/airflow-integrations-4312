from datetime import timedelta
from airflow.models import Variable
from pendulum import datetime, now
from incyte_biosciences_international_sarl.time_off_sync.utils import custom_methods
import rail

# pylint: disable=too-many-statements
def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"incyte_biosciences_international_sarl_time_off_sync_master_{config.instance}",
        description="incyte time off sync add and delete time off",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 11, 16, tz=config.time_zone),
        max_active_runs=config.max_active_run_master,
        schedule_interval=timedelta(seconds=30),
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id="new_file_sensor",
            path=config.sftp_import_path,
            soft_fail_timeout=timedelta(minutes=10)
        )

        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            trigger_rule="all_done",
            test='{{get_task_state("new_file_sensor") == "success"}}',
            yes_task="archive_file",
            no_task="delete_dagrun"
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dagrun"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id="download_file",
            remote_filepath='{{result("new_file_sensor")}}'
        )

        can_decrypt_file = rail.IfOperator(
            task_id ="can_decrypt_file",
            test=lambda: Variable.get(config.can_decrypt_file_var_name, default_var='true').lower() == 'true',
            yes_task='decrypt_file',
            no_task='dummy_load_data'
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id="decrypt_file",
            pgp_conn_id=config.pgp_conn_id,
            source='{{result("download_file")}}'
        )

        process_start_time = rail.PythonOperator(
            task_id="process_start_time",
            python_callable=lambda: now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        )

        create_incyte_log = rail.CreateLogOperator(
            task_id="create_incyte_log"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id="archive_file",
            trigger_rule="all_done",
            new_filename=config.sftp_archive_path +
            'archive_{{ dag_run_ecid() }}_{{result("new_file_sensor")|file_name}}',
            existing_filename='{{result("new_file_sensor")}}'
        )

        dummy_load_data = rail.PythonOperator(
            task_id= "dummy_load_data",
            python_callable= lambda: rail.result('decrypt_file') if Variable.get(
                config.can_decrypt_file_var_name, default_var='true').lower()== 'true' else  rail.result('download_file'),
            show_return_value_in_logs= False
        )

        load_import_csv = rail.LoadCSVFileOperator(
            task_id='load_import_csv',
            document="{{ result('dummy_load_data') }}",
            encoding='utf-8-sig'
        )

        if_file_has_records = rail.IfOperator(
            task_id="if_file_has_records",
            test='{{result("load_import_csv") | load_all_records |length > 0}}',
            yes_task="create_time_off_import_collection",
            no_task="send_no_records_mail"
        )

        send_no_records_mail = rail.EmailOperator(
            task_id="send_no_records_mail",
            to=config.tenant_mail,
            subject='{{get_company_key()}}'+"| Time off sync to Replicon has no records " +
            '{{result("process_start_time")}}',
            html_content="templates/no_records.html"
        )

        create_time_off_import_collection = rail.CreateCollectionOperator(
            task_id="create_time_off_import_collection",
            source='{{result("load_import_csv")}}',
            name="time_off_sync",
            columns={
                "UserID": "userid",
                "EE ID": "employee_id",
                "FirstName": "first_name",
                "LastName": "last_name",
                "Absence Type": "time_off_type",
                "Begin Date": "start_date",
                "Return Date": "end_date",
                "Duration Type": "duration_type",
                "Duration": "duration",
                "Status": "status",
                "Days": "noofdays",
                "Hours": "hours",
                "Shift": "start_hours",
                "Unique ID": "peoplesoft_unique_id"
            }
        )

        query_valid_time_off_records = rail.QueryCollectionOperator(
            task_id="query_valid_time_off_records",
            query="""SELECT * FROM time_off_sync WHERE NULLIF("employee_id","") IS NOT NULL
                        AND NULLIF("time_off_type","") IS NOT NULL
                        AND NULLIF("start_date","") IS NOT NULL
                        AND NULLIF("end_date","") IS NOT NULL
                        AND NULLIF("duration","") IS NOT NULL
                        AND NULLIF("duration_type", "") IS NOT NULL
                        AND NULLIF("status","") IS NOT NULL
                        AND NULLIF("peoplesoft_unique_id","") IS NOT NULL"""
        )

        query_invalid_time_off_records = rail.QueryCollectionOperator(
            task_id="query_invalid_time_off_records",
            query="""SELECT * FROM time_off_sync WHERE NULLIF("employee_id","") IS NULL
                        OR NULLIF("time_off_type","") IS NULL
                        OR NULLIF("start_date","") IS NULL
                        OR NULLIF("end_date","") IS NULL
                        OR NULLIF("duration","") IS NULL
                        OR NULLIF("duration_type", "") IS NULL
                        OR NULLIF("status","") IS NULL
                        OR NULLIF("peoplesoft_unique_id","") IS NULL"""
        )

        if_invalid_time_off_records = rail.IfOperator(
            task_id="if_invalid_time_off_records",
            test='{{result("query_invalid_time_off_records","length") > 0}}',
            yes_task="write_skipped_records_log",
            no_task="if_valid_time_off_records"
        )

        write_skipped_records_log = rail.WriteLogOperator(
            task_id="write_skipped_records_log",
            items='{{result("query_invalid_time_off_records")}}',
            log='{{result("create_incyte_log")}}',
            message="Exception",
            severity="Exception",
            properties=lambda item: {
                "employee_id": item["employee_id"],
                "time_off_type": item["time_off_type"],
                "start_date": item["start_date"],
                "end_date": item["end_date"],
                "unique_id": item["peoplesoft_unique_id"],
                "time_off_status": item["status"],
                "status": "Exception",
                "details": custom_methods.get_skipped_log_details(item)
            }
        )

        if_valid_time_off_records = rail.IfOperator(
            task_id="if_valid_time_off_records",
            test='{{result("query_valid_time_off_records","length") > 0}}',
            yes_task="get_all_object_extension_fields",
            no_task="if_log_records"
        )

        get_all_object_extension_fields = rail.RepliconServiceOperator(
            task_id="get_all_object_extension_fields",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:time-off"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response,
                "name",
                "People Soft UniqueID",
                "uri"
            )
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id="get_all_time_off_types",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        process_valid_time_off_records = rail.trigger_parallel_dagrun(
            task_id="process_valid_time_off_records",
            items='{{result("query_valid_time_off_records")}}',
            trigger_dag_id=f"incyte_biosciences_international_sarl_time_off_sync_child_{config.instance}",
            parallel_count=config.max_active_run_child,
            execution_timeout=timedelta(days=config.execution_timeout),
            conf=lambda item: {
                **item,
                "people_soft_unique_id_uri": rail.result("get_all_object_extension_fields"),
                "time_off_type_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result("get_all_time_off_types"),
                    "displayText",
                    item["time_off_type"],
                    "uri"
                ),
                "lookuptable": rail.result("create_incyte_log")

            }
        )

        if_log_records = rail.IfOperator(
            task_id="if_log_records",
            test='{{result("create_incyte_log") | load_all_records | length > 0}}',
            yes_task="filter_for_error_logs",
            no_task="log_to_sumo"
        )

        filter_for_error_logs = rail.FilterLogEntriesOperator(
            task_id="filter_for_error_logs",
            severity="Error",
            log='{{result("create_incyte_log")}}'
        )

        filter_for_exception_logs = rail.FilterLogEntriesOperator(
            task_id="filter_for_exception_logs",
            properties={"status": "Exception"},
            log='{{result("create_incyte_log")}}'
        )

        filter_timesheet_reopened_logs = rail.FilterLogEntriesOperator(
            task_id="filter_timesheet_reopened_logs",
            properties={"details": "Time sheet reopened for timeoff booking"},
            log='{{result("create_incyte_log")}}'
        )

        if_filter_timesheet_reopened_logs = rail.IfOperator(
            task_id="if_filter_timesheet_reopened_logs",
            test='{{result("filter_timesheet_reopened_logs", "length") > 0}}',
            yes_task="create_data_for_each_user",
            no_task="write_logs_csv"
        )


        create_data_for_each_user = rail.DataAdaptorOperator(
            task_id="create_data_for_each_user",
            source='{{result("filter_timesheet_reopened_logs")}}',
            columns=["first_name","employee_id", "time_sheet_period","time_off_type", "start_date",
                    "end_date", "unique_id", "status", "details", "email"],
            data=custom_methods.create_data
        )

        create_collection_for_each_user = rail.CreateCollectionOperator(
            task_id="create_collection_for_each_user",
            source='{{result("create_data_for_each_user")}}',
            name="timesheetreopenedusers",
        )

        query_distinct_users = rail.QueryCollectionOperator(
            task_id="query_distinct_users",
            query="""SELECT DISTINCT first_name,employee_id,email FROM timesheetreopenedusers"""
        )

        for_each_user = rail.ForEachOperator(
            task_id="for_each_user",
            items='{{result("query_distinct_users")}}',
            start_task="query_all_reopened_timesheet_periods",
            end_task="end_each_user"
        )

        query_all_reopened_timesheet_periods = rail.QueryCollectionOperator(
            task_id="query_all_reopened_timesheet_periods",
            query="""SELECT DISTINCT time_sheet_period FROM timesheetreopenedusers WHERE employee_id={{result("for_each_user").employee_id}}"""
        )

        send_time_sheet_reopened_mail = rail.EmailOperator(
            task_id="send_time_sheet_reopened_mail",
            to='{{result("for_each_user").email}}',
            subject='{{get_company_key()}}'+" | Timesheet(s) was reopened in Replicon",
            html_content="templates/time_sheet_reopened.html"
        )

        end_each_user = rail.EmptyOperator(task_id="end_each_user")

        write_logs_csv = rail.WriteCSVFileOperator(
            task_id="write_logs_csv",
            source='{{result("create_incyte_log")}}',
            header=["Employee id", "Time off type", "Start date",
                    "End date", "Unique ID", "Time of status", "Status", "Details"],
            row=[
                '{{item.properties | attr_or_default("employee_id","")}}',
                '{{item.properties | attr_or_default("time_off_type","")}}',
                '{{item.properties | attr_or_default("start_date","")}}',
                '{{item.properties | attr_or_default("end_date","")}}',
                '{{item.properties | attr_or_default("unique_id","")}}',
                '{{item.properties | attr_or_default("time_off_status","")}}',
                '{{item.properties | attr_or_default("status","")}}',
                '{{item.properties | attr_or_default("details")}}'
            ]
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_logs_to_sftp",
            content='{{result("write_logs_csv")}}',
            remote_filepath=config.sftp_log_path +
            'logs_{{dag_run_ecid()}}_{{ result("new_file_sensor") | file_name}}'
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{result("write_logs_csv")}}',
            output_file_name='logs_{{dag_run_ecid()}}_{{ result("new_file_sensor") | file_name}}',
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_mail = rail.EmailOperator(
            task_id="send_import_complete_mail",
            to=config.tenant_mail,
            bcc="{%- if result('filter_for_error_logs', 'length') > 0 -%}\
                    "+config.alert_mail+"\
                {%- else -%}\
                    "+config.internal_logs_mail+"\
                {%- endif -%}",
            subject='{{get_company_key()}}'+" | Timeoff Import to Replicon has completed " +
            '{% if result("filter_for_error_logs", "length") > 0%}with errors {{result("process_start_time")}}\
            {% elif result("filter_for_exception_logs", "length") > 0%}with exceptions {{result("process_start_time")}}\
            {% else %}succesfully {{result("process_start_time")}} {%endif%}',
            html_content="templates/import_complete.html",
            params={
                "filepath": config.sftp_log_path
            }

        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{get_error_message()|is_truthy}}',
            yes_task="fail_dagrun"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{get_error_message()}}'
        )

        new_file_sensor >> download_file >>\
            was_new_file_found >> rail.Label("Yes") >>\
            archive_file
        was_new_file_found >> rail.Label("No") >>\
            delete_dagrun
        download_file >> process_start_time >> create_incyte_log >>\
        can_decrypt_file >> rail.Label('No') >>  dummy_load_data
        can_decrypt_file >> rail.Label('Yes') >> decrypt_file >> dummy_load_data >>\
        load_import_csv >>\
            if_file_has_records >> rail.Label("No") >>\
            send_no_records_mail >> log_to_sumo
        if_file_has_records >> rail.Label("Yes") >>\
            create_time_off_import_collection >>\
            query_valid_time_off_records >>\
            query_invalid_time_off_records >>\
            if_invalid_time_off_records >> rail.Label(
                "Yes") >> write_skipped_records_log >> if_valid_time_off_records
        if_invalid_time_off_records >> rail.Label("No") >>\
            if_valid_time_off_records >> rail.Label("Yes") >>\
            get_all_object_extension_fields >> get_all_time_off_types >>\
            process_valid_time_off_records >>\
            if_log_records >> rail.Label("Yes") >>\
            filter_for_error_logs >> filter_for_exception_logs >>\
            filter_timesheet_reopened_logs >>\
            if_filter_timesheet_reopened_logs >> rail.Label("No") >> write_logs_csv
        if_filter_timesheet_reopened_logs >> rail.Label("Yes") >>\
        create_data_for_each_user>>\
        create_collection_for_each_user >> query_distinct_users >>\
        for_each_user >> end_each_user
        for_each_user >> query_all_reopened_timesheet_periods >> send_time_sheet_reopened_mail >> end_each_user >>\
            write_logs_csv >>\
            upload_logs_to_sftp >> generate_download_link >> send_import_complete_mail >> log_to_sumo >>\
            can_fail_dag >> fail_dagrun
        if_valid_time_off_records >> rail.Label("No") >>\
            if_log_records >> rail.Label("No") >> log_to_sumo
        return dag


rail.for_each_instance(create_master_dag)
