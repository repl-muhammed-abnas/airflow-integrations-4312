import datetime
import itertools
from airflow.models import Variable
import rail

from pwcglobal.absense_data_pre_population.utils import response_filter
from pwcglobal.absense_data_pre_population.utils import request_payload
from pwcglobal.absense_data_pre_population.utils import python_callable_method
from pwcglobal.absense_data_pre_population.utils import custom_method


def create_main_absense_data_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'PWC Absense Data Pre-population Master V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var),
        start_date=datetime.datetime(2022, 1, 1),
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config',
            extra_config=config)

        can_redirect_to_workato = rail.IfOperator(
            task_id='can_redirect_to_workato',
            test=lambda: Variable.get(
                config.can_redirect_to_workato_var_name, default_var='').lower() == 'true',
            yes_task='post_to_workato',
            no_task='can_run_batch_task',
        )

        post_to_workato = rail.SimpleHttpOperator(
            task_id='post_to_workato',
            method='POST',
            http_conn_id=config.workato_api_endpoint,
            headers={
                "Content-Type": 'application/json; charset=utf-8',
                "API-TOKEN": "{{ var.value." + config.workato_api_token_var_name + " }}"
            },
            data="{{ dag_run.conf.webhook.data | to_json }}",
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='').lower() == 'true',
            yes_task='batch_task',
            no_task='was_triggered_by_pwc'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='was_triggered_by_pwc',
            execution_timeout=datetime.timedelta(
                days=config.execution_timeout_days),
            end_task='has_valid_time_entries',
        )

        was_triggered_by_pwc = rail.EmptyOperator(
            task_id='was_triggered_by_pwc')

        should_process_absense_data = rail.IfOperator(
            task_id='should_process_absense_data',
            test=lambda dag_run: bool((dag_run.conf['webhook']).get('data')
                                      and (dag_run.conf['webhook']['data']).get('PersonResourceActualTime')),
            yes_task='create_log',
            no_task='log_empty_payload_to_sumo'
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log',
        )

        get_all_object_extension_field_bindings = rail.RepliconServiceOperator(
            task_id='get_all_object_extension_field_bindings',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings',
            data={"bindingContextUri": "urn:replicon:object-type:time-entry"},
            response_filter=lambda response: response_filter.get_timesheet_oef(response, config.WORKTYPE_MAPPER)
        )

        get_time_entry_all_columns = rail.RepliconServiceOperator(
            task_id='get_time_entry_all_columns',
            endpoint='/services/TimeEntryRevisionGroupListService1.svc/GetAllColumns',
            response_filter=response_filter.get_timeentry_column_uri
        )

        get_time_entry_all_filter_definitions = rail.RepliconServiceOperator(
            task_id='get_time_entry_all_filter_definitions',
            endpoint='/services/TimeEntryRevisionGroupListService1.svc/GetAllFilterDefinitions',
            response_filter=response_filter.get_timeentry_filter_definition_uri
        )

        has_valid_column_filter_definition = rail.IfOperator(
            task_id='has_valid_column_filter_definition',
            test=lambda: bool(rail.result('get_time_entry_all_columns') and
                              rail.result('get_time_entry_all_filter_definitions')),
            yes_task='filter_records_of_time_entry_data',
            no_task='log_column_filter_definition_missing'
        )

        log_column_filter_definition_missing = rail.WriteLogOperator(
            task_id='log_column_filter_definition_missing',
            message='Time entry column or Time entry filter definition isn\'t configured',
            severity='Error'
        )

        filter_records_of_time_entry_data = rail.PythonOperator(
            task_id='filter_records_of_time_entry_data',
            python_callable=python_callable_method.filter_records_of_time_entry_data
        )

        has_valid_time_entries = rail.IfOperator(
            task_id='has_valid_time_entries',
            test='{{ result("filter_records_of_time_entry_data") | attr_or_default("valid_time_entry_data") | length > 0 }}',
            yes_task='start_processing_childs_empty',
            no_task='has_invalid_time_entries'
        )

        has_invalid_time_entries = rail.IfOperator(
            task_id='has_invalid_time_entries',
            test='{{ result("filter_records_of_time_entry_data") | attr_or_default("invalid_time_entry_data") | length > 0 }}',
            yes_task='log_invalid_time_entry',
            no_task='finish'
        )

        start_processing_childs_empty = rail.EmptyOperator(
            task_id = 'start_processing_childs_empty'
        )

        def get_process_timeentry_trigger_id(item):
            modulo = int(item['record_id']) % config.TIME_ENTRY_BATCH_COUNT
            if modulo == 0:
                return f'pwc_timesheetprepopulation_child_{config.instance}'
            return f"pwc_timesheetprepopulation_child_{config.instance}_batch_{str(modulo)}"

        process_valid_time_entry_data = rail.trigger_parallel_dagrun(
            task_id='process_valid_time_entry_data',
            items='{{ result("filter_records_of_time_entry_data") | attr_or_default("valid_time_entry_data") | to_json }}',
            execution_timeout=datetime.timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id= get_process_timeentry_trigger_id,
            conf=lambda item: request_payload.get_process_prepopulation_data(item, config.WORKTYPE_MAPPER),
            parallel_count= config.parallel_count
        )

        log_invalid_time_entry = rail.WriteLogOperator(
            task_id='log_invalid_time_entry',
            log='{{ result("create_log") }}',
            message=custom_method.build_message,
            items='{{ result("filter_records_of_time_entry_data") | attr_or_default("invalid_time_entry_data") | to_json }}',
            severity='Exception',
            properties=lambda item: custom_method.get_invalid_log_properties(
                item, action='Pre-check', status='Exception')
        )

        can_run_batch_task1 = rail.IfOperator(
            task_id='can_run_batch_task1',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task1',
            no_task='get_process_child_dag_ids'
        )

        batch_task1 = rail.BatchTaskRunOperator(
            task_id='batch_task1',
            start_task='get_process_child_dag_ids',
            execution_timeout=datetime.timedelta(
                days=config.execution_timeout_days),
            end_task='log_to_sumo_dummy',
        )

        log_to_sumo_dummy = rail.EmptyOperator(
            task_id = "log_to_sumo_dummy"
        )

        get_process_child_dag_ids =rail.PythonOperator(
            task_id= 'get_process_child_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_valid_time_entry_data_{x+1}'), range(config.parallel_count))))),
            show_return_value_in_logs= False
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs='{{ result("get_process_child_dag_ids") }}',
            dagrun_task_id='create_child_log',
            execution_timeout=datetime.timedelta(hours=2),
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=datetime.timedelta(days=config.execution_timeout_days),
            python_callable=custom_method.do_format_logs,
            show_return_value_in_logs=False
        )

        write_xml_file = rail.RenderTemplateOperator(
            task_id='write_xml_file',
            target='artifact',
            template_file='templates/output/template.xml',
            dataset=lambda: rail.result('format_logs')
        )

        def file_upload_failed(context):
            subject = '{{ get_company_key() }} | Time pre-population -  Failed while uploading logs to SFTP - {{ current_time("%Y%m%d%H%M%S") }}'
            email = rail.EmailOperator(
                task_id='send_sftp_failure_payload_email',
                to=config.tenant_email,
                bcc=config.internal_logs_email,
                subject=subject,
                html_content='templates/email/sftp_failure_payload.html',
                files=[('{{ result("write_xml_file") }}')]
            )
            email.render_template_fields(context)
            email.execute(context)

        upload_xml_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_xml_to_sftp',
            content='{{ result("write_xml_file") }}',
            remote_filepath=config.log_filepath +
            # pylint: disable=line-too-long
            '/{{get_company_key() | lower}}_time_prepopulation_{{current_time("%Y%m%d%H%M%S")|replace(":","")|replace("-","")|replace(" ","")}}_{{dag_run_ecid()|replace(":","-")}}_logs.xml',
            on_failure_callback=file_upload_failed
        )

        if config.secondary_sftp:
            upload_xml_to_secondary_sftp = rail.SFTPUploadFileOperator(
                task_id='upload_xml_to_secondary_sftp',
                sftp_conn_id=config.secondary_sftp_conn_id,
                content='{{ result("write_xml_file") }}',
                remote_filepath=config.secondary_log_filepath +
                # pylint: disable=line-too-long
                '/{{get_company_key() | lower}}_time_prepopulation_{{current_time("%Y%m%d%H%M%S")|replace(":","")|replace("-","")|replace(" ","")}}_{{dag_run_ecid()|replace(":","-")}}_logs.xml',
            )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("write_xml_file")}}',
            # pylint: disable=line-too-long
            output_file_name='{{get_company_key() | lower}}_time_prepopulation_{{current_time("%Y%m%d%H%M%S")|replace(":","")|replace("-","")|replace(" ","")}}_{{dag_run_ecid()|replace(":","-")}}_logs.xml',
            expires_in_seconds=7 * 24 * 60 * 60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Time pre-population - "  }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " " + current_time() }}',
            html_content="templates/email/import_complete.html",
            params={
                'log_filepath': config.log_filepath
            }
        )

        log_empty_payload_to_sumo = rail.SendToSumoOperator(
            task_id='log_empty_payload_to_sumo',
            data={
                'payloaddatetime': '{{ dag_run.conf.webhook.received_at }}',
                'payloadrecordcount': '0',
            },
            sumo_conn_id=config.sumo_conn_id
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_redirect_to_workato >> rail.Label('Yes') >> post_to_workato
        can_redirect_to_workato >> rail.Label('No') >> can_run_batch_task

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> has_valid_time_entries
        can_run_batch_task >> rail.Label('No') >> was_triggered_by_pwc

        was_triggered_by_pwc >> should_process_absense_data

        should_process_absense_data >> rail.Label(
            'Yes') >> create_log >> get_all_object_extension_field_bindings >> get_time_entry_all_columns >> \
            get_time_entry_all_filter_definitions >> has_valid_column_filter_definition
        should_process_absense_data >> rail.Label(
            'No') >> log_empty_payload_to_sumo

        has_valid_column_filter_definition >> rail.Label(
            'Yes') >> filter_records_of_time_entry_data >> has_valid_time_entries
        has_valid_column_filter_definition >> rail.Label(
            'No') >> log_column_filter_definition_missing

        has_valid_time_entries >> rail.Label(
            'Yes') >> start_processing_childs_empty >> process_valid_time_entry_data >> finish
        has_valid_time_entries >> rail.Label(
            'No') >> has_invalid_time_entries

        has_invalid_time_entries >> rail.Label(
            'Yes') >> log_invalid_time_entry >> finish
        has_invalid_time_entries >> rail.Label(
            'No') >> finish >> can_run_batch_task1

        can_run_batch_task1 >> rail.Label(
            'Yes') >> batch_task1 >> log_to_sumo_dummy
        
        can_run_batch_task1 >> rail.Label(
            'No') >> get_process_child_dag_ids

        get_process_child_dag_ids >> gather_child_logs >> format_logs >> \
            write_xml_file >> upload_xml_to_sftp

        if config.secondary_sftp:
            upload_xml_to_sftp >> upload_xml_to_secondary_sftp >> generate_download_link
        else:
            upload_xml_to_sftp >> generate_download_link

        generate_download_link >> send_import_complete_email >> log_to_sumo_dummy

    return dag


rail.for_each_instance(create_main_absense_data_dag)
