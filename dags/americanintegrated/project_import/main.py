from datetime import timedelta
from hashlib import md5
import itertools
import chardet
from pendulum import datetime
from rail.lib.artifact import existing_artifact
from americanintegrated.project_import.tasks.create_input_entries_collection import create_entry_collection
import rail
null = None

# pylint: disable=too-many-statements


def create_airflow_master_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.master_dag_id,
        description="americanintegrated project import master",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 9, 12),
        schedule_interval=timedelta(seconds=30),
        max_active_runs=config.max_active_runs_master,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:
        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id="new_file_sensor",
            path=config.sftp_import_file_path,
            soft_fail_timeout=timedelta(minutes=10)
        )

        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            trigger_rule="all_done",
            test='{{get_task_state("new_file_sensor") == "success" }}',
            yes_task="archive_file",
            no_task="delete_dagrun"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id="archive_file",
            new_filename=config.sftp_archive_file_path +
            "{{ecid()| replace(':', '-')}}_{{result('new_file_sensor')|file_name}}",
            existing_filename="{{result('new_file_sensor')}}",
            trigger_rule="all_done"
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dagrun"
        )

        if_new_file_ends_with_txt = rail.IfOperator(
            task_id="if_new_file_ends_with_txt",
            test='{{result("new_file_sensor")| file_name |lower|ends_with(".txt")}}',
            yes_task="american_integrated_lookup_table",
            no_task="send_incorrect_file_format_mail"
        )

        send_incorrect_file_format_mail = rail.EmailOperator(
            task_id="send_incorrect_file_format_mail",
            to=config.tenant_email,
            subject='{{get_company_key()}}'+" | Project Import - Incorrect file format received - " +
            '{{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/incorrect_mail.html"
        )

        american_integrated_lookup_table = rail.CreateLogOperator(
            task_id="american_integrated_lookup_table"
        )
        download_file = rail.SFTPDownloadFileOperator(
            task_id="download_file",
            remote_filepath='{{result("new_file_sensor")}}'
        )

        def find_file_encoding_callable(task_id):
            feed_file = rail.result(task_id)
            with existing_artifact(feed_file) as ff:
                return chardet.detect_all(ff.file.read())

        find_file_encoding = rail.PythonOperator(
            task_id = "find_file_encoding",
            python_callable=find_file_encoding_callable,
            op_args=[download_file.task_id]
        )

        parse_project_import_csv = rail.LoadCSVFileOperator(
            task_id="parse_project_import_csv",
            document='{{result("download_file")}}',
            encoding="{{ result('find_file_encoding')[0].encoding}}",
            delimiter="\t"
        )

        write_project_import_csv = rail.WriteCSVFileOperator(
            task_id="write_project_import_csv",
            source='{{result("parse_project_import_csv")}}',
            header=["clientnumber",
                    "name",
                    "status",
                    "jobnumber",
                    "jobname",
                    "projectmanager",
                    "pmname",
                    "certifiedpayroll",
                    "md5_reference",
                    "co_manager"],
            row=lambda item: [
                item["clientnumber"] or null, item["name"] or null, item["status"] or null,
                item["jobnumber"] or null, item["jobname"] or null, item["projectmanager"] or null,
                item["pmname"] or null, item["certifiedpayroll"] or null,
                md5("".join([item["clientnumber"], item["name"], item["status"],
                             item["jobnumber"], item["jobname"], item["projectmanager"],
                             item["pmname"], item["certifiedpayroll"]]).encode()).hexdigest(),
                item["co_manager"] or null
            ]
        )

        if_projects_for_import = rail.IfOperator(
            task_id="if_projects_for_import",
            test="{{result('write_project_import_csv') | load_all_records | length > 0}}",
            yes_task="list_reference_sftp_files",
            no_task="send_completed_blank_file_received_mail"
        )

        send_completed_blank_file_received_mail = rail.EmailOperator(
            task_id="send_completed_blank_file_received_mail",
            to=config.tenant_email,
            subject='{{get_company_key()}}'+" | Project Import - Blank File received - " +
            '{{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/blank_file.html"
        )

        list_reference_sftp_files = rail.SFTPListFilesOperator(
            task_id="list_reference_sftp_files",
            paths=[config.sftp_reference_file_path],
        )

        if_reference_files_are_present = rail.IfOperator(
            task_id="if_reference_files_are_present",
            test='{{result("list_reference_sftp_files") |length >0}}',
            yes_task="for_each_reference_file",
            no_task="fail_no_reference_dagrun"
        )

        fail_no_reference_dagrun = rail.FailOperator(
            task_id="fail_no_reference_dagrun",
            message="No Reference File Present"
        )

        for_each_reference_file = rail.ForEachOperator(
            task_id="for_each_reference_file",
            items=lambda: rail.result('list_reference_sftp_files').get(
                config.sftp_reference_file_path),
            start_task="download_reference_file",
            end_task="end_for_reference"
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id="download_reference_file",
            remote_filepath=config.sftp_reference_file_path +
            "{{result('for_each_reference_file').name}}"
        )

        parse_project_reference_csv = rail.LoadCSVFileOperator(
            task_id="parse_project_reference_csv",
            document='{{result("download_reference_file")}}',
            delimiter='\t'
        )

        create_reference_collection = rail.CreateCollectionOperator(
            task_id="create_reference_collection",
            source='{{result("parse_project_reference_csv")}}',
            columns=["clientcode",
                     "clientname",
                     "projectstatus",
                     "projectcode",
                     "projectname",
                     "projectleadercode",
                     "projectleadername",
                     "prevailingwages",
                     "md5_reference",
                     "projectcomanagercode"],
            name="project_reference"
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id="archive_reference_file",
            existing_filename=config.sftp_reference_file_path +
            "{{result('for_each_reference_file').name}}",
            new_filename=config.sftp_archive_file_path +
            "{{result('for_each_reference_file').name}}"
        )

        end_for_reference = rail.EmptyOperator(task_id="end_for_reference")

        create_project_import_collection = rail.CreateCollectionOperator(
            task_id="create_project_import_collection",
            source='{{result("write_project_import_csv")}}',
            columns={
                'clientnumber': 'clientcode',
                'name': 'clientname',
                'status': 'projectstatus',
                'jobnumber': 'projectcode',
                'jobname': 'projectname',
                'projectmanager': 'projectleadercode',
                'pmname': 'projectleadername',
                'certifiedpayroll': 'prevailingwages',
                'md5_reference': 'md5_reference',
                "co_manager": "projectcomanagercode"
            },
            name="project_import",
        )

        query_delta_values_not_in_reference = rail.QueryCollectionOperator(
            task_id="query_delta_values_not_in_reference",
            query="""SELECT * FROM project_import WHERE md5_reference NOT IN(
                    SELECT DISTINCT md5_reference FROM project_reference
                    )
                    """
        )

        if_delta_values = rail.IfOperator(
            task_id="if_delta_values",
            test='{{result("query_delta_values_not_in_reference")| load_all_records() |length > 0}}',
            no_task="empty_reference_task",
            yes_task="start_basic_task"
        )

        empty_reference_task = rail.EmptyOperator(task_id ='empty_reference_task')
        start_basic_task = rail.EmptyOperator(task_id="start_basic_task")
        create_list_of_basic_task = create_entry_collection(
            config.sftp_task_file_path, "basic_task", "costcodes")

        end_basic_task = rail.EmptyOperator(task_id="end_basic_task")

        create_list_of_prevailing_wages = create_entry_collection(
            config.sftp_wages_file_path, "prevailing_wage", "paygroup")

        get_all_custom_fields_for_project = rail.RepliconServiceOperator(
            task_id="get_all_custom_fields_for_project",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data=lambda: {
                    "objectUri": "urn:replicon:object-type:project"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response,
                "displayText", "Prevailing Wage", "uri")
        )

        get_all_custom_fields_for_task = rail.RepliconServiceOperator(
            task_id="get_all_custom_fields_for_task",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data=lambda: {
                    "objectUri": "urn:replicon:object-type:task"
            },
            data_handler=lambda response: {
                "prevailing_wage_dt": rail.find_first_by_attr_and_get_attr(
                    response,
                    "displayText",
                    "Prevailing wages DT",
                    "uri"),
                "prevailing_wage_ot":rail.find_first_by_attr_and_get_attr(
                    response,
                    "displayText",
                    "Prevailing wages OT",
                    "uri"),
                "prevailing_wage_rt": rail.find_first_by_attr_and_get_attr(
                    response,
                    "displayText",
                    "Prevailing wages RT",
                    "uri")
            }
        )

        query_for_unchanged_records = rail.QueryCollectionOperator(
            task_id="query_for_unchanged_records",
            query="""SELECT * FROM project_import WHERE md5_reference IN(
                    SELECT DISTINCT md5_reference FROM project_reference
                    )
                    """
        )

        if_unchanged_records = rail.IfOperator(
            task_id="if_unchanged_records",
            test='{{result("query_for_unchanged_records") | load_all_records() | length > 0}}',
            yes_task="write_log_unchanged_project_record",
            no_task="query_unique_clients"
        )

        write_log_unchanged_project_record = rail.WriteLogOperator(
            task_id="write_log_unchanged_project_record",
            log='{{result("american_integrated_lookup_table")}}',
            items='{{result("query_for_unchanged_records")}}',
            message="Project Skipped",
            severity="Skipped",
            properties=lambda item: {
                "Job ID": rail.render_template('{{ecid()}}'),
                "Project Name": item["projectname"],
                "Project Code": item["projectcode"],
                "Status": "Ignored",
                "Reason": "No change in record",
                "Child Job ID": rail.render_template('{{ecid()}}'),
            }
        )

        query_unique_clients = rail.QueryCollectionOperator(
            task_id="query_unique_clients",
            query="""SELECT DISTINCT clientname, clientcode FROM query_delta_values_not_in_reference"""
        )

        process_clients = rail.trigger_parallel_dagrun(
            task_id="process_clients",
            items='{{result("query_unique_clients")}}',
            trigger_dag_id= config.process_client_dag_id,
            parallel_count=config.max_active_runs_child,
            execution_timeout=timedelta(days=config.execution_timeout),
            conf=lambda item: {
                **item,
                "lookuptable": rail.result("american_integrated_lookup_table"),
                "parent_ecid": rail.render_template('{{ecid()}}')
            }
        )

        get_process_clients_dag_ids =rail.PythonOperator(
            task_id= 'get_process_clients_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_clients_{x+1}'), range(config.max_active_runs_child))))),
            show_return_value_in_logs= False
        )

        gather_client_details = rail.GatherResultsFromDagRunsOperator(
            task_id = 'gather_client_details',
            dag_runs= '{{ result("get_process_clients_dag_ids") }}',
            dagrun_task_id= 'log_client_details',
            flatten= True
        )

        process_projects = rail.trigger_parallel_dagrun(
            task_id="process_projects",
            items='{{result("query_delta_values_not_in_reference")}}',
            trigger_dag_id= config.process_project_dag_id,
            parallel_count=config.max_active_runs_child,
            execution_timeout=timedelta(days=config.execution_timeout),
            conf=lambda item: {
                **item,
                "prevailingwageuri": rail.result("get_all_custom_fields_for_project"),
                "lookuptable": rail.result("american_integrated_lookup_table"),
                "parent_ecid": rail.render_template('{{ecid()}}'),
                "Prevailing wages DT uri": rail.result("get_all_custom_fields_for_task")["prevailing_wage_dt"],
                "Prevailing wages OT uri": rail.result("get_all_custom_fields_for_task")["prevailing_wage_ot"],
                "Prevailing wages RT uri": rail.result("get_all_custom_fields_for_task")["prevailing_wage_rt"],
                "prevailing_wage_artifact": rail.result("append_content_prevailing_wage"),
                "basic_task_artifact": rail.result("append_content_basic_task"),
                "client_uri": rail.find_first_by_attr_and_get_attr(rail.result(
                    "gather_client_details"),'client_code',item['clientcode'],'client_uri')
            }
        )

        parse_reference_csv = rail.WriteCSVFileOperator(
            task_id="parse_reference_csv",
            source='{{result("write_project_import_csv")}}',
            delimiter='\t'
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id="upload_new_reference_file",
            remote_filepath=config.sftp_reference_file_path + "newreference_" +
            "{{ecid()| replace(':', '-')}}_{{result('new_file_sensor')|file_name}}",
            content='{{result("parse_reference_csv")}}'
        )

        compose_logs_csv = rail.WriteCSVFileOperator(
            task_id="compose_logs_csv",
            source='{{result("american_integrated_lookup_table")}}',
            header=["Project Name", "Project Code",
                    "Status", "Reason", "Job ID"],
            row=[
                '{{ item.properties | attr_or_default("Project Name", "") }}',
                '{{ item.properties | attr_or_default("Project Code", "") }}',
                '{{ item.properties | attr_or_default("Status", "") }}',
                '{{ item.properties | attr_or_default("Reason", "") }}',
                '{{ item.ecid }}'
            ]
        )

        sftp_upload_logs = rail.SFTPUploadFileOperator(
            task_id="sftp_upload_logs",
            content='{{result("compose_logs_csv")}}',
            remote_filepath=config.sft_logs_file_path + "logs_" +
            "{{dag_run_ecid()| replace(':', '-')}}_{{result('new_file_sensor')|file_name}}"
        )

        filter_error_logs = rail.FilterLogEntriesOperator(
            task_id="filter_error_logs",
            log='{{result("american_integrated_lookup_table")}}',
            severity='Error'
        )

        send_project_import_mail = rail.EmailOperator(
            task_id="send_project_import_mail",
            to=config.tenant_email,
            bcc="{%- if result('filter_error_logs', 'length') > 0 -%}\
                "+config.alert_mail+"\
            {%- else -%}\
                "+config.internal_logs_email+"\
            {%- endif -%}",
            subject='{{ get_company_key() + " | Project Import - " }} \
                {%- if result("filter_error_logs")| load_all_records() | length > 0 -%} \
                   completed with error \
                {%- else -%} \
                    completed successfully \
                {%- endif -%} \
                {{ " - " + current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/import_mail.html"
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

        new_file_sensor >> \
            if_new_file_ends_with_txt >> rail.Label("Yes") >>\
            send_incorrect_file_format_mail >> log_to_sumo
        if_new_file_ends_with_txt >> rail.Label("NO") >> american_integrated_lookup_table >>\
            download_file >> find_file_encoding >> parse_project_import_csv >> write_project_import_csv >>\
            if_projects_for_import >> rail.Label('No') >>\
            send_completed_blank_file_received_mail >> log_to_sumo
        download_file >> was_new_file_found >> rail.Label(
            "No") >> delete_dagrun
        download_file >> was_new_file_found >> rail.Label(
            "Yes") >> archive_file
        if_projects_for_import >> rail.Label("Yes") >>\
            list_reference_sftp_files >>\
            if_reference_files_are_present >> rail.Label("Yes") >>\
            for_each_reference_file >> end_for_reference
        for_each_reference_file >> download_reference_file >>\
            parse_project_reference_csv >> create_reference_collection >>\
            archive_reference_file >> end_for_reference >>\
            create_project_import_collection >> query_delta_values_not_in_reference >>\
            if_delta_values >> rail.Label(
                "No") >> empty_reference_task >> parse_reference_csv
        if_delta_values >> rail.Label("Yes") >>\
            start_basic_task >> create_list_of_basic_task >>\
            end_basic_task >> create_list_of_prevailing_wages >>\
            get_all_custom_fields_for_project >> get_all_custom_fields_for_task >>\
            query_for_unchanged_records >>\
            if_unchanged_records >> rail.Label(
                "Yes") >> write_log_unchanged_project_record >> query_unique_clients
        if_unchanged_records >> rail.Label("No") >>\
        query_unique_clients >> process_clients
        compose_logs_csv >> parse_reference_csv >> upload_new_reference_file >> log_to_sumo
        process_clients >> get_process_clients_dag_ids >> gather_client_details >> process_projects >>\
            compose_logs_csv >> sftp_upload_logs >> filter_error_logs >>\
            send_project_import_mail >> log_to_sumo
        if_reference_files_are_present >> rail.Label("No") >> fail_no_reference_dagrun
        parse_reference_csv >> upload_new_reference_file >>\
            log_to_sumo >> can_fail_dag >> fail_dagrun

        return dag


rail.for_each_instance(create_airflow_master_dag)
