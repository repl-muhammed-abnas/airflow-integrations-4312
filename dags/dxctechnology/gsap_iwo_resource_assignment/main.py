from datetime import timedelta
import itertools
import rail
from dxctechnology.gsap_iwo_resource_assignment.utils import request_payload
from dxctechnology.gsap_iwo_resource_assignment.utils import response_filter
from dxctechnology.gsap_iwo_resource_assignment.utils import python_callable_method


null = None

# pylint: disable=too-many-statements


def create_attribute_1_master_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_iwo_resource_assignment_master_{config.dag_id_postfix}',
        description=f'DXC GSAP IWO Resource Assignment Master V1.0 {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10),
            # We do the timeout with a soft fail here to yield to potential other waiting executions of this DAG
            # Since max_active_runs is set to 1, if this sensor ran indefinitiely then someone manually wanting to
            # retry failed tasks in a past run would also be waiting indefinitely. This way it'll give them a window
            # every 10 minutes to run their tasks.
        )

        is_xml = rail.IfOperator(
            task_id='is_xml',
            test='{{ result("new_file_sensor") | file_ext | lower == "xml" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email',
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Replicon GSAP IWO Resource Assignment Master - Incorrect file format {{ current_time() }}',
            html_content='templates/email/bad_file_format.html',
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_xml = rail.LoadXMLFileOperator(
            task_id='parse_xml',
            document='{{ result("download_file") }}',
            xsd_document='./dags/dxctechnology/gsap_iwo_resource_assignment/xsdschema/input_schema.xsd'
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Records") | length > 0 }}',
            yes_task='create_log',
            no_task='send_blank_payload_email',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon GSAP IWO Resource Assignment - Blank Payload  {{ current_time() }}',
            html_content="templates/email/blank_payload.html",
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_all_divisions = rail.RepliconServiceOperator(
            task_id='get_all_divisions',
            endpoint='/services/DivisionListService1.svc/GetData',
            data=request_payload.get_all_division_payload,
            response_filter=response_filter.map_all_divisions
        )

        get_c1_leanstaffing_import_base_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_c1_leanstaffing_import_base_report_details',
            report_name=config.extract_report_name,
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='c1_leanstaffing_import_base_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_c1_leanstaffing_import_base_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('c1_leanstaffing_import_base_report.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='fail_no_report_data',
        )

        fail_no_report_data = rail.FailOperator(
            task_id="fail_no_report_data",
            message="Report \"**C1 Lean staffing Import base report\" execution failed",
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('c1_leanstaffing_import_base_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        get_active_user = rail.PythonOperator(
            task_id="get_active_user",
            python_callable=python_callable_method.active_user,
            op_args=['load_report_data']
        )

        get_wbs_records_from_xml = rail.XMLAdaptorOperator(
            task_id='get_wbs_records_from_xml',
            source='{{  result("parse_xml") }}',
            target='result',
            adaptor=[
                'Records',
                {
                    'wbs': 'WBS_Name/text()',
                    'empid': 'Employee_ID/text()',
                    'tasklevel1': 'Task_level_1/text()',
                    'assignmentStartDate': 'User_Assignment_Start_date/text()',
                    'assignmentEndDate': 'User_Assignment_End_Date/text()',
                }
            ],
        )

        filter_valid_wbs_records = rail.PythonOperator(
            task_id='filter_valid_wbs_records',
            python_callable=python_callable_method.get_valid_wbs_records
        )

        filter_blank_wbs_records = rail.PythonOperator(
            task_id='filter_blank_wbs_records',
            python_callable=python_callable_method.get_blank_wbs_records
        )

        check_all_valid_wbs = rail.IfOperator(
            task_id='check_all_valid_wbs',
            test=lambda: len(rail.result('filter_valid_wbs_records')) > 0,
            yes_task='dummy_process_each_wbs_attribute',
            no_task='log_no_record_to_process'
        )

        log_no_record_to_process = rail.WriteLogOperator(
            task_id='log_no_record_to_process',
            log='{{ result("create_log") }}',
            message='All records are invalid in file to process',
            items='{{ result("filter_blank_wbs_records") | to_json }}',
            properties={
                'wbs': 'na',
                'empid': '',
                'taskcode': '',
                'action': null,
                'status': 'skipped',
                'details': 'All records are invalid in file to process',
            }
        )

        dummy_process_each_wbs_attribute = rail.EmptyOperator(
            task_id='dummy_process_each_wbs_attribute'
        )

        process_each_wbs_attribute = rail.trigger_parallel_dagrun(
            task_id='process_each_wbs_attribute',
            items='{{ result("filter_valid_wbs_records") | to_json }}',
            parallel_count=config.trigger_parallel_dagrun_process_each_wbs_attribute,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_gsap_iwo_resource_process_wbs_{config.dag_id_postfix}',
            conf=request_payload.get_process_each_wbs
        )

        get_process_each_wbs_attribute_dagrun_ids =rail.PythonOperator(
            task_id='get_process_each_wbs_attribute_dagrun_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'process_each_wbs_attribute_{x+1}') if rail.result(
                    f'process_each_wbs_attribute_{x+1}') else []), range(config.trigger_parallel_dagrun_process_each_wbs_attribute))))),
            show_return_value_in_logs=False
        )

        gather_process_each_wbs_attribute_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_process_each_wbs_attribute_logs',
            dag_runs='{{ result("get_process_each_wbs_attribute_dagrun_ids") }}',
            dagrun_task_id='create_log',
            execution_timeout=timedelta(
                hours=config.gather_logs_timeout_hours),
            flatten=True
        )

        format_log_records = rail.CreateCollectionOperator(
            task_id='format_log_records',
            source=python_callable_method.do_format_logs,
            columns=[
                'wbs',
                'empid',
                'status',
                'details',
                'ecid'],
            name='final_log_records'
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('format_log_records') }}",
            header=[
                '{{ current_time("%d/%m/%YT%H:%M:%S") }}',
                'Number of Rows: {{ result("get_wbs_records_from_xml") | length }}',
                'GSAP IWO Lean Staffing',
                '',
                ''],
            row=[
                '{{ item | attr_or_default("wbs", "") }}',
                '{{ item | attr_or_default("empid", "") }}',
                '{{ item.status }}',
                '{{ item.details }}',
                '{{ item.ecid }}'],
            footer=[
                # pylint: disable=line-too-long
                'Number of Records Processed Successfully: {{ result("format_log_records", key="get_success_logs") }}',
                'Number of Records with Error: {{ result("format_log_records", key="get_errored_logs") }}',
                'Number of Records with Exception: {{ result("format_log_records", key="get_exception_logs") }}',
                '',
                ''],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content='{{ result("render_logs_csv") }}',
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv'
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_log_records', key='get_errored_logs') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon GSAP IWO Resource Assignment sync -  " }} \
                {%- if result("format_log_records", key="get_errored_logs") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_log_records", key="get_exception_logs") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email/import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule="all_done",
            test=lambda:  rail.get_current_context()['dag_run'].get_task_instance(
                delete_this_dagrun.task_id).current_state().lower() != "success" and
                rail.get_current_context()['dag_run'].get_task_instance(
                download_file.task_id).current_state().lower() == "success",
            yes_task="log_to_sumo",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "file_name": "{{result('new_file_sensor')}}",
                "archive_file": "{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}",
                "log_file_name": 'log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv'
            }
        )

        new_file_sensor >> is_xml

        is_xml >> rail.Label(
            'Yes') >> download_file >> parse_xml >> has_data
        is_xml >> rail.Label('No') >> send_bad_file_format_email

        has_data >> rail.Label(
            'Yes') >> create_log >> get_all_divisions >> get_c1_leanstaffing_import_base_report_details >> report_group_entry
        report_group_exit >> report_has_data >> rail.Label(
            "YES") >> load_report_data >> get_active_user
        report_has_data >> rail.Label('NO') >> fail_no_report_data
        get_active_user >> \
            get_wbs_records_from_xml >> filter_valid_wbs_records >> filter_blank_wbs_records >> check_all_valid_wbs

        check_all_valid_wbs >> rail.Label(
            'Yes') >> dummy_process_each_wbs_attribute >> process_each_wbs_attribute \
                >> get_process_each_wbs_attribute_dagrun_ids >> gather_process_each_wbs_attribute_logs \
                    >> format_log_records
        check_all_valid_wbs >> rail.Label(
            'No') >> log_no_record_to_process >> format_log_records

        format_log_records >> render_logs_csv \
            >> upload_log_to_sftp >> send_import_complete_email >> can_log_to_sumo >> rail.Label("Yes") >> log_to_sumo

        has_data >> rail.Label('No') >> send_blank_payload_email
        # was_new_file_found has trigger_rule = 'all_done', so it will execute whenever download_file is done, regardless of whether it
        # succeeded, failed, or was skipped
        download_file >> rail.Label(
            'Always') >> was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun

    return dag


rail.for_each_instance(create_attribute_1_master_dag)
